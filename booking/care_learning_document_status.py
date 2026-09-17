"""
booking/care_learning_document_status.py
─────────────────────────────────────────
Calls XN Portal outreach API → Gemini Vision → stores in care_learning_document.

Routes
------
  GET /booking/care-learning/document-status
      ?email=<email>     → single user (force re-check, skips already-saved docs)
      ?xn_user_id=<id>   → single user by xn_user_id
      ?batch=<1|2|3>     → batch mode: picks next un-flagged user in that batch
      (no param)         → batch mode: picks next un-flagged user across all batches
      All modes return 202 immediately; Gemini runs in a background thread.

  GET /booking/care-learning/document-status/rescan
      Reset care_check for all users with incomplete docs

  GET /booking/care-learning/document-status/debug?email=<email>
      Show API doc names vs allowlist for a user

care_learning_found values
--------------------------
  'yes'   — HSeLanD / hseland.ie branding explicitly detected  (pass 1)
  'no'    — HSeLanD branding not found                         (pass 1)
  'maybe' — pass 1 = 'no', recheck found a generic HSE keyword (pass 2)
            recheck pass 2 = 'no' → stays 'no'
  'no_url'— document record has no URL to check
  'error' — download or Gemini API failure

Flow
----
  Fresh doc  (not yet in DB)  → pass 1 only  → 'yes' | 'no' | 'error'
  Recheck    (saved as 'no')  → pass 2 only  → 'maybe' | 'no'
"""

import os
import re
import base64
import logging
import threading
from datetime import datetime

import requests
from flask import jsonify, request

from database import db
from . import bp

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Env config
# ──────────────────────────────────────────────────────────────────────────────

XN_PORTAL_BASE_URL = os.getenv("XN_PORTAL_BASE_URL", "").rstrip("/")
XN_PORTAL_API_KEY  = os.getenv("XN_PORTAL_API_KEY", "")
XN_APP_COUNTRY     = os.getenv("XN_APP_COUNTRY", "ie")
GEMINI_API_KEY     = os.getenv("GEMINI_API_KEY", "")

OUTREACH_URL = "https://user.xpresshealthapp.com/api/ai/recruitments/user-document-list"
GEMINI_URL   = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.5-flash:generateContent"
)
GEMINI_BATCH = 2

# ──────────────────────────────────────────────────────────────────────────────
# Allowlist
# ──────────────────────────────────────────────────────────────────────────────

ALLOWED_DOCUMENT_TYPES = {
    "Infection Prevention Control Certificate",
    "Ppe",
    "Hand Hygiene",
    "Children First",
    "Safeguarding Adults At Risk",
    "Cpr/Bls",
    "Manual And People Handling Documents",
    "The Open Disclosure",
    "Cpi/ Mapa/Pmav",
    "Cyber Security",
    "Gdpr",
    "Dignity At Work",
    "Fire Safety",
    "QQI Level 5 or equivalent in Health Service Skills or Healthcare Support",
    "Managing Feeding, Eating, Drinking And Swallowing In People With An Intellectual Disability",
    "Enhanced Declaration Of Risk Assessment",
    "Applying A Human Rights-Based Approach In Health And Social Care",
    "Supporting Decision Making In Health & Social Care",
    "Hse National Consent Policy V1.2",
    "Sepsis Management",
    "Hse National Consent Policy V1.1",
    "Occupational Health",
    "Hse Effective Complaints Handling",
    "Medication Administration",
    "Neurogenic Bowel Dysfunction",
    "Neurogenic Bowel Dysfunction Training (Practical)",
    "Management Of Blood & Body Substance Spills",
    "Haccp/Food Safety",
}

ALLOWED_DOCUMENT_TYPES_LOWER = {v.strip().lower() for v in ALLOWED_DOCUMENT_TYPES}

# ──────────────────────────────────────────────────────────────────────────────
# HSE keyword list for secondary Gemini pass
# ──────────────────────────────────────────────────────────────────────────────

HSE_KEYWORDS = [
    "hse",
    "health service executive",
    "hse.ie",
    "hseland",
    "hselands",
]

# ──────────────────────────────────────────────────────────────────────────────
# Outreach API
# ──────────────────────────────────────────────────────────────────────────────

def _outreach_headers() -> dict:
    return {
        "Api-Key":       XN_PORTAL_API_KEY,
        "X-App-Country": XN_APP_COUNTRY,
        "Content-Type":  "application/json",
    }


def _fetch_document_list(email: str) -> dict:
    resp = requests.get(
        OUTREACH_URL,
        json={"email": email},
        headers=_outreach_headers(),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


# ──────────────────────────────────────────────────────────────────────────────
# Gemini Vision
# ──────────────────────────────────────────────────────────────────────────────

def _download_as_base64(url: str) -> tuple:
    """Download a document URL and return (base64_string, mime_type)."""
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")
    path = url.split("?")[0].lower()
    if "pdf" in content_type or path.endswith(".pdf"):
        mime_type = "application/pdf"
    elif "png" in content_type or path.endswith(".png"):
        mime_type = "image/png"
    elif "jpg" in content_type or "jpeg" in content_type or path.endswith((".jpg", ".jpeg")):
        mime_type = "image/jpeg"
    else:
        mime_type = content_type.split(";")[0].strip() or "application/octet-stream"

    return base64.b64encode(resp.content).decode("utf-8"), mime_type


def _gemini_call(b64_data: str, mime_type: str, prompt: str) -> str:
    """
    Shared Gemini API caller.
    Sends the already-encoded document + prompt and returns the raw AI text.
    Raises on HTTP / parsing errors so callers can handle them.
    """
    payload = {
        "contents": [{
            "parts": [
                {"inline_data": {"mime_type": mime_type, "data": b64_data}},
                {"text": prompt},
            ]
        }]
    }

    resp = requests.post(
        GEMINI_URL,
        params={"key": GEMINI_API_KEY},
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    return (
        data.get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [{}])[0]
        .get("text", "")
        .strip()
    )


def _gemini_hseland_pass(b64_data: str, mime_type: str) -> tuple:
    """
    Primary pass: strict check for HSeLanD / hseland.ie branding.

    Returns:
        (found: bool, ai_text: str)
        found=True  → 'yes'
        found=False → continue to secondary pass
    """
    prompt = (
        "You are a strict document verification assistant. "
        "Examine this document carefully and answer ONLY about whether it was "
        "issued by or belongs to the organisation 'HSeLanD' (hseland.ie), "
        "which is the HSE's online learning and development platform in Ireland. "
        "\n\n"
        "Answer YES only if the document explicitly contains ANY of: "
        "1. The text 'hseland.ie' (the website/domain name), OR "
        "2. The text 'HSeLanD' or 'HSELanD' as a brand or organisation name, OR "
        "3. A logo that is specifically identified as the HSeLanD / hseland.ie logo. "
        "\n\n"
        "Do NOT answer YES for documents from any other provider, including: "
        "Care Learning, carelearning.org.uk, care-learning.com, AMRIC, NMBI, RCPI, "
        "or any other training organisation that is not HSeLanD / hseland.ie. "
        "\n\n"
        "Answer with YES or NO on the very first line only. "
        "Then on the next lines state exactly what text, branding, or logo you found "
        "and why you answered YES or NO."
    )

    try:
        ai_text    = _gemini_call(b64_data, mime_type, prompt)
        first_line = ai_text.splitlines()[0].upper() if ai_text else ""
        return ("YES" in first_line), ai_text
    except Exception as exc:
        raise RuntimeError(f"Primary Gemini pass failed: {exc}") from exc


def _gemini_hse_keyword_pass(b64_data: str, mime_type: str) -> tuple:
    """
    Secondary pass: look for ANY generic HSE keyword in the document.
    Called only when the primary HSeLanD check returned False (no branding found).

    Returns:
        (found: bool, ai_text: str)
        found=True  → upgrade result to 'maybe'
        found=False → final result is 'no'
    """
    keyword_list = ", ".join(f'"{k}"' for k in HSE_KEYWORDS)

    prompt = (
        "You are a document text extraction assistant. "
        "Carefully read ALL visible text in this document — including headers, "
        "footers, logos, watermarks, and small print. "
        "\n\n"
        f"Does the document contain ANY of the following keywords or phrases: {keyword_list}? "
        "These keywords are NOT case-sensitive (e.g. 'HSE', 'hse', 'Hse' all count). "
        "\n\n"
        "Answer YES if ANY of those keywords appear ANYWHERE in the document, "
        "even once, even in a footer or small print. "
        "Answer NO only if none of them appear at all. "
        "\n\n"
        "Answer with YES or NO on the very first line only. "
        "On the next lines, quote the exact text snippet where you found the keyword, "
        "or explain why you answered NO."
    )

    try:
        ai_text    = _gemini_call(b64_data, mime_type, prompt)
        first_line = ai_text.splitlines()[0].upper() if ai_text else ""
        return ("YES" in first_line), ai_text
    except Exception as exc:
        logger.warning("Secondary HSE keyword Gemini pass failed: %s", exc)
        # Non-fatal: return False so we fall back to 'no' rather than 'error'
        return False, f"HSE keyword check error: {exc}"


def _gemini_check(url: str) -> tuple:
    """
    Pass 1 — strict HSeLanD / hseland.ie branding check for a FRESH document.

    Used when the document is not yet saved in the DB.
    Does NOT run the HSE keyword scan — that is done only during a recheck
    of documents already stored as 'no' (see _gemini_recheck).

    Returns (care_learning_found, ai_response):
        'yes'   — HSeLanD / hseland.ie branding explicitly detected
        'no'    — HSeLanD branding not found
        'error' — document download or Gemini API failure
    """
    if not GEMINI_API_KEY:
        return "error", "GEMINI_API_KEY not configured"

    try:
        b64_data, mime_type = _download_as_base64(url)
    except Exception as exc:
        logger.warning("Download failed for Gemini: %s — %s", url[:80], exc)
        return "error", f"Document download failed: {exc}"

    try:
        hseland_found, ai_text = _gemini_hseland_pass(b64_data, mime_type)
    except Exception as exc:
        logger.warning("Gemini pass 1 error for %s: %s", url[:80], exc)
        return "error", str(exc)

    if hseland_found:
        logger.debug("URL %s → yes (HSeLanD branding found)", url[:80])
        return "yes", ai_text

    logger.debug("URL %s → no (HSeLanD branding not found)", url[:80])
    return "no", ai_text


def _gemini_recheck(url: str) -> tuple:
    """
    Pass 2 — HSE keyword scan for documents already stored as 'no'.

    Called only during a recheck of a saved 'no' document.
    Downloads the document fresh and runs the HSE keyword scan.

    Returns (care_learning_found, ai_response):
        'maybe' — a generic HSE keyword was found  → upgrade from 'no'
        'no'    — no HSE keyword found             → stays 'no'
        'error' — document download or Gemini API failure
    """
    if not GEMINI_API_KEY:
        return "error", "GEMINI_API_KEY not configured"

    try:
        b64_data, mime_type = _download_as_base64(url)
    except Exception as exc:
        logger.warning("Download failed for Gemini recheck: %s — %s", url[:80], exc)
        return "error", f"Document download failed: {exc}"

    hse_found, ai_text = _gemini_hse_keyword_pass(b64_data, mime_type)

    if hse_found:
        logger.debug("URL %s → maybe (HSE keyword found on recheck)", url[:80])
        return "maybe", ai_text

    logger.debug("URL %s → no (no HSE keyword found on recheck)", url[:80])
    return "no", ai_text


# ──────────────────────────────────────────────────────────────────────────────
# Persistence
# ──────────────────────────────────────────────────────────────────────────────

def _save_document_result(user_id, xn_user_id, doc, hseland_found, ai_response):
    """
    Upsert keyed on (user_id, document_type_name).

    care_learning_found values stored:
        'yes'   — HSeLanD / hseland.ie detected            (pass 1)
        'no'    — HSeLanD branding not found               (pass 1, or recheck pass 2 = no keyword)
        'maybe' — HSE keyword found on recheck             (pass 2)
        'no_url'— no URL available to check
        'error' — Gemini / download failure
    """
    db.care_learning_document.update_one(
        {
            "user_id":            user_id,
            "document_type_name": doc.get("document_type_name"),
        },
        {
            "$set": {
                "user_id":             user_id,
                "xn_user_id":          xn_user_id,
                "document_type_name":  doc.get("document_type_name"),
                "status":              doc.get("status"),
                "url":                 doc.get("url"),
                "care_learning_found": hseland_found,
                "ai_response":         ai_response,
                "ai_checked_at":       datetime.utcnow(),
            }
        },
        upsert=True,
    )


# ──────────────────────────────────────────────────────────────────────────────
# User flag helpers
# ──────────────────────────────────────────────────────────────────────────────

def _mark_done(user_oid, status: str, error=None):
    db.care_learning_users.update_one(
        {"_id": user_oid},
        {
            "$set": {
                "care_check":            1,
                "care_check_status":     status,
                "care_check_error":      error,
                "care_check_updated_at": datetime.utcnow(),
            }
        },
    )


def _user_query_single(xn_user_id: str) -> dict:
    return {"xn_user_id": xn_user_id}


def _user_query_batch(batch: int = None) -> dict:
    """
    MongoDB filter for the next un-flagged user.
    If batch is given (1, 2, or 3), restrict to that batch only.
    """
    conditions = [
        {
            "$or": [
                {"care_check": {"$exists": False}},
                {"care_check": {"$ne": 1}},
            ]
        },
        # Exclude users currently being processed by another worker
        {"care_check_status": {"$ne": "processing"}},
    ]

    if batch is not None:
        conditions.append({"batch": batch})

    return {"$and": conditions}


# ──────────────────────────────────────────────────────────────────────────────
# Core: process one user  (runs in background thread)
# ──────────────────────────────────────────────────────────────────────────────

def _process_user(user: dict) -> None:
    """
    Called in a background thread — no return value used.

    1. Call outreach API for the user's documents.
    2. Filter to allowed types (normalised name match).
    3. Bucket docs into:
         - new (not yet saved)         → pass 1 HSeLanD branding check
         - saved as 'no' with URL      → pass 2 HSE keyword recheck → 'maybe' or 'no'
         - saved as anything else      → skipped (already final)
         - no URL (new)                → save 'no_url' immediately
    4. Stamp care_check = 1 only when this call processed zero new/recheck docs.
    """
    email      = (user.get("email") or "").strip()
    xn_user_id = user.get("xn_user_id") or ""
    user_oid   = user["_id"]
    user_id    = str(user_oid)

    if not email:
        _mark_done(user_oid, status="no_email", error="No email on user record")
        return

    if not XN_PORTAL_BASE_URL or not XN_PORTAL_API_KEY:
        logger.error("XN_PORTAL_BASE_URL or XN_PORTAL_API_KEY not configured")
        # Don't stamp done — server config problem, not a user problem
        db.care_learning_users.update_one(
            {"_id": user_oid},
            {"$unset": {"care_check": "", "care_check_status": ""}},
        )
        return

    try:
        api_data = _fetch_document_list(email)
        all_docs = api_data.get("data", {}).get("documents", [])

        # ── Filter to allowed types ────────────────────────────────────
        allowed_docs = [
            d for d in all_docs
            if (d.get("document_type_name") or "").strip().lower()
            in ALLOWED_DOCUMENT_TYPES_LOWER
        ]

        # ── Load saved records keyed by normalised name ────────────────
        # Maps normalised_name → care_learning_found value (or None if unsaved)
        saved_records = {
            (rec.get("document_type_name") or "").strip().lower(): rec.get("care_learning_found")
            for rec in db.care_learning_document.find(
                {"user_id": user_id},
                {"document_type_name": 1, "care_learning_found": 1},
            )
        }

        def _saved_status(doc):
            return saved_records.get(
                (doc.get("document_type_name") or "").strip().lower()
            )

        # ── Split into buckets ─────────────────────────────────────────
        # New docs — not yet in DB at all
        docs_new_with_url    = [d for d in allowed_docs if d.get("url")     and _saved_status(d) is None]
        docs_new_without_url = [d for d in allowed_docs if not d.get("url") and _saved_status(d) is None]
        # Docs already saved as 'no' with a URL → eligible for pass 2 recheck
        docs_recheck         = [d for d in allowed_docs if d.get("url")     and _saved_status(d) == "no"]
        # Everything else (yes / maybe / no_url / error) → skip
        docs_skipped         = [
            d for d in allowed_docs
            if _saved_status(d) not in (None, "no") or (not d.get("url") and _saved_status(d) is not None)
        ]

        logger.info(
            "User %s — allowed:%d  new_url:%d  new_no_url:%d  recheck:%d  skipped:%d",
            email, len(allowed_docs),
            len(docs_new_with_url), len(docs_new_without_url),
            len(docs_recheck), len(docs_skipped),
        )

        # ── New docs without URL → save immediately ────────────────────
        for doc in docs_new_without_url:
            _save_document_result(user_id, xn_user_id, doc, "no_url", None)

        # ── New docs with URL → pass 1 HSeLanD branding check ─────────
        for i in range(0, len(docs_new_with_url), GEMINI_BATCH):
            for doc in docs_new_with_url[i: i + GEMINI_BATCH]:
                found, ai_resp = _gemini_check(doc.get("url", ""))
                _save_document_result(user_id, xn_user_id, doc, found, ai_resp)
                logger.info(
                    "User %s | [pass 1] doc '%s' → %s",
                    email, doc.get("document_type_name", "?"), found,
                )

        # ── Saved 'no' docs → pass 2 HSE keyword recheck ──────────────
        for i in range(0, len(docs_recheck), GEMINI_BATCH):
            for doc in docs_recheck[i: i + GEMINI_BATCH]:
                found, ai_resp = _gemini_recheck(doc.get("url", ""))
                _save_document_result(user_id, xn_user_id, doc, found, ai_resp)
                logger.info(
                    "User %s | [pass 2 recheck] doc '%s' → %s",
                    email, doc.get("document_type_name", "?"), found,
                )

        # ── Completion check ───────────────────────────────────────────
        docs_processed = (
            len(docs_new_with_url) + len(docs_new_without_url) + len(docs_recheck)
        )

        if docs_processed == 0:
            # Nothing new or to recheck — fully done
            _mark_done(user_oid, status="ok")
            logger.info(
                "User %s fully done — %d allowed docs total in DB",
                email, len(docs_skipped),
            )
        else:
            # Processed something — re-queue so next poll confirms nothing is left
            db.care_learning_users.update_one(
                {"_id": user_oid},
                {"$unset": {"care_check": "", "care_check_status": ""}},
            )
            logger.info(
                "User %s processed %d docs — re-queued for next run",
                email, docs_processed,
            )

    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response else None
        logger.warning("Outreach HTTP %s error for %s: %s", code, email, exc)
        _mark_done(user_oid, status="api_error", error=f"HTTP {code}: {exc}")

    except requests.RequestException as exc:
        logger.warning("Outreach network error for %s: %s", email, exc)
        _mark_done(user_oid, status="network_error", error=f"Request failed: {exc}")

    except Exception as exc:
        logger.exception("Unexpected error for %s", email)
        _mark_done(user_oid, status="unexpected_error", error=f"Unexpected error: {exc}")


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@bp.route("/care-learning/document-status")
def care_learning_document_status():
    """
    ?email=<email>     → force re-process this user (bypasses care_check)
    ?xn_user_id=<id>   → same by xn_user_id
    ?batch=<1|2|3>     → batch mode scoped to that batch number
    (no param)         → batch mode across all users

    Returns 202 immediately — Gemini processing runs in a background thread.
    """
    email      = request.args.get("email", "").strip().lower()
    xn_user_id = request.args.get("xn_user_id", "").strip()
    batch_raw  = request.args.get("batch", "").strip()

    # Parse optional batch number
    batch = None
    if batch_raw:
        try:
            batch = int(batch_raw)
        except ValueError:
            return jsonify({
                "success": False,
                "error":   f"Invalid batch value '{batch_raw}' — must be an integer.",
            }), 400

    fields = {
        "email": 1, "xn_user_id": 1,
        "first_name": 1, "last_name": 1, "care_check": 1, "batch": 1,
    }

    if email or xn_user_id:
        # ── Single-user mode ───────────────────────────────────────────
        q = (
            {"email": {"$regex": f"^{re.escape(email)}$", "$options": "i"}}
            if email else _user_query_single(xn_user_id)
        )
        user = db.care_learning_users.find_one(q, fields)
        if not user:
            return jsonify({
                "success": False,
                "error":   f"No user found for '{email or xn_user_id}'",
            }), 404

        # Clean stale no_url records that actually have a URL
        db.care_learning_document.delete_many({
            "user_id":             str(user["_id"]),
            "care_learning_found": "no_url",
            "url":                 {"$ne": None},
        })

        # Lock immediately so batch poller skips this user
        db.care_learning_users.update_one(
            {"_id": user["_id"]},
            {"$set": {
                "care_check_status":     "processing",
                "care_check_updated_at": datetime.utcnow(),
            }},
        )

        threading.Thread(target=_process_user, args=(user,), daemon=True).start()

        return jsonify({
            "success":    True,
            "mode":       "single",
            "accepted":   True,
            "email":      user.get("email"),
            "xn_user_id": user.get("xn_user_id"),
            "message":    f"Processing started for {user.get('email')}",
        }), 202

    else:
        # ── Batch mode ─────────────────────────────────────────────────
        query     = _user_query_batch(batch)
        remaining = db.care_learning_users.count_documents(query)
        user      = db.care_learning_users.find_one(query, fields, sort=[("_id", 1)])

        if not user:
            batch_label = f"batch {batch}" if batch else "all batches"
            return jsonify({
                "success":   True,
                "mode":      "batch",
                "batch":     batch,
                "remaining": 0,
                "found":     0,
                "message":   f"All users in {batch_label} already processed.",
            })

        # Lock immediately
        db.care_learning_users.update_one(
            {"_id": user["_id"]},
            {"$set": {
                "care_check_status":     "processing",
                "care_check_updated_at": datetime.utcnow(),
            }},
        )

        threading.Thread(target=_process_user, args=(user,), daemon=True).start()

        return jsonify({
            "success":    True,
            "mode":       "batch",
            "batch":      batch,
            "remaining":  remaining,
            "found":      1,
            "accepted":   True,
            "email":      user.get("email", "—"),
            "xn_user_id": user.get("xn_user_id", "—"),
            "message":    "Processing started in background.",
        }), 202


@bp.route("/care-learning/document-status/user/<xn_user_id>")
def care_learning_document_status_user(xn_user_id: str):
    """Clean-URL alias for single-user mode."""
    return care_learning_document_status()


@bp.route("/care-learning/document-status/rescan")
def care_learning_document_status_rescan():
    """Reset care_check for all users whose saved doc count < allowlist size."""
    flagged = list(db.care_learning_users.find(
        {"care_check": 1},
        {"email": 1, "xn_user_id": 1},
    ))

    reset_count = 0
    for user in flagged:
        uid   = str(user["_id"])
        saved = db.care_learning_document.count_documents({"user_id": uid})
        if saved < len(ALLOWED_DOCUMENT_TYPES):
            db.care_learning_users.update_one(
                {"_id": user["_id"]},
                {"$unset": {
                    "care_check":        "",
                    "care_check_status": "",
                    "care_check_error":  "",
                }},
            )
            reset_count += 1

    return jsonify({
        "success":       True,
        "users_checked": len(flagged),
        "users_reset":   reset_count,
        "message":       f"{reset_count} users reset and re-queued.",
    })


@bp.route("/care-learning/document-status/debug")
def care_learning_document_status_debug():
    """Show API doc names vs allowlist for a user."""
    xn_user_id = request.args.get("xn_user_id", "").strip()
    email      = request.args.get("email", "").strip()

    if not xn_user_id and not email:
        return jsonify({"error": "xn_user_id or email param required"}), 400

    q = (
        _user_query_single(xn_user_id) if xn_user_id
        else {"email": {"$regex": f"^{re.escape(email)}$", "$options": "i"}}
    )
    user = db.care_learning_users.find_one(q, {"email": 1, "xn_user_id": 1})
    if not user:
        return jsonify({"error": "User not found"}), 404

    try:
        api_data = _fetch_document_list(user.get("email", ""))
        all_docs = api_data.get("data", {}).get("documents", [])
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    uid = str(user["_id"])
    saved_records = {
        (r.get("document_type_name") or "").strip().lower(): r.get("care_learning_found")
        for r in db.care_learning_document.find(
            {"user_id": uid}, {"document_type_name": 1, "care_learning_found": 1}
        )
    }

    rows = []
    for d in all_docs:
        name       = d.get("document_type_name") or ""
        normalised = name.strip().lower()
        rows.append({
            "document_type_name":     name,
            "document_category_type": d.get("document_category_type"),
            "in_allowlist":           normalised in ALLOWED_DOCUMENT_TYPES_LOWER,
            "has_url":                bool(d.get("url")),
            "already_saved":          normalised in saved_records,
            "care_learning_found":    saved_records.get(normalised),
            "will_process":           normalised in ALLOWED_DOCUMENT_TYPES_LOWER
                                      and normalised not in saved_records,
        })

    matched   = [r for r in rows if r["in_allowlist"]]
    unmatched = [r for r in rows if not r["in_allowlist"]]

    # Breakdown of saved results by value
    result_breakdown = {}
    for v in saved_records.values():
        result_breakdown[v] = result_breakdown.get(v, 0) + 1

    return jsonify({
        "success":          True,
        "email":            user.get("email"),
        "total_from_api":   len(all_docs),
        "matched":          len(matched),
        "unmatched_count":  len(unmatched),
        "unmatched_names":  [r["document_type_name"] for r in unmatched],
        "result_breakdown": result_breakdown,   # e.g. {"yes": 3, "maybe": 2, "no": 1}
        "rows":             rows,
    })
