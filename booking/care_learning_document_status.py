"""
booking/care_learning_document_status.py
─────────────────────────────────────────
Calls XN Portal outreach API → Gemini Vision → stores in care_learning_document.

Routes
------
  GET /booking/care-learning/document-status
      ?email=<email>     → single user (force re-check, skips already-saved docs)
      ?xn_user_id=<id>   → single user by xn_user_id
      (no param)         → batch: picks next un-flagged user

  GET /booking/care-learning/document-status/rescan
      Reset care_check for all users with incomplete docs

  GET /booking/care-learning/document-status/debug?xn_user_id=<id>
      Show API doc names vs allowlist for a user
"""

import os
import re
import base64
import logging
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

OUTREACH_URL = f"{XN_PORTAL_BASE_URL}/ai/recruitments/user-document-list"
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


def _gemini_check(url: str) -> tuple:
    """Returns (care_learning_found, ai_response)."""
    if not GEMINI_API_KEY:
        return "error", "GEMINI_API_KEY not configured"

    try:
        b64_data, mime_type = _download_as_base64(url)
    except Exception as exc:
        logger.warning("Download failed for Gemini: %s — %s", url[:80], exc)
        return "error", f"Document download failed: {exc}"

    prompt = (
        "You are a strict document verification assistant. "
        "Examine this document carefully and answer ONLY about the specific "
        "organisation named 'Care Learning'. "
        "\n\n"
        "'Care Learning' is a specific UK/Ireland-based training provider. "
        "It is NOT the same as any of the following — do NOT answer YES for these: "
        "HSE (Health Service Executive), HSeLanD, hseland.ie, AMRIC, NMBI, RCPI, "
        "HSELanD, Health Service Executive, or any other Irish health organisation. "
        "\n\n"
        "Answer YES only if the document explicitly contains: "
        "1. The exact text 'Care Learning' as a brand/organisation name, OR "
        "2. The exact URL 'carelearning.org.uk' or 'care-learning.com', OR "
        "3. A logo that is specifically identified as the Care Learning logo. "
        "\n\n"
        "If the document is from HSeLanD, HSE, hseland.ie or any other provider, "
        "answer NO. "
        "\n\n"
        "Answer with YES or NO on the very first line only. "
        "Then on the next lines state exactly what organisation or branding you found "
        "and why you answered YES or NO."
    )

    payload = {
        "contents": [{
            "parts": [
                {"inline_data": {"mime_type": mime_type, "data": b64_data}},
                {"text": prompt},
            ]
        }]
    }

    try:
        resp = requests.post(
            GEMINI_URL,
            params={"key": GEMINI_API_KEY},
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        ai_text = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [{}])[0]
            .get("text", "")
            .strip()
        )
        first_line = ai_text.splitlines()[0].upper() if ai_text else ""
        found = "yes" if "YES" in first_line else "no"
        return found, ai_text

    except Exception as exc:
        logger.warning("Gemini API error for %s: %s", url[:80], exc)
        return "error", f"Gemini API error: {exc}"


# ──────────────────────────────────────────────────────────────────────────────
# Persistence
# ──────────────────────────────────────────────────────────────────────────────

def _save_document_result(user_id, xn_user_id, doc, care_learning_found, ai_response):
    """
    Upsert keyed on (user_id, document_type_name).
    Simple and collision-free — document_type_name is unique within the allowlist.
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
                "care_learning_found": care_learning_found,
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


def _user_query_batch() -> dict:
    return {
        "$and": [
            {
                "$or": [
                    {"care_check": {"$exists": False}},
                    {"care_check": {"$ne": 1}},
                ]
            },
            {"care_check_status": {"$ne": "processing"}},
        ]
    }


# ──────────────────────────────────────────────────────────────────────────────
# Core: process one user
# ──────────────────────────────────────────────────────────────────────────────

def _process_user(user: dict) -> dict:
    """
    1. Call outreach API for the user's documents.
    2. Filter to allowed types (normalised name match).
    3. Skip docs whose document_type_name is already in care_learning_document.
    4. no_url docs → save "no_url" immediately.
    5. URL docs → Gemini Vision check → save result.
    6. Stamp care_check = 1 only when all allowed docs are confirmed saved.
    """
    email      = (user.get("email") or "").strip()
    xn_user_id = user.get("xn_user_id") or ""
    user_oid   = user["_id"]
    user_id    = str(user_oid)

    result = {
        "user_id":         user_id,
        "xn_user_id":      xn_user_id,
        "email":           email,
        "api_status":      None,
        "success":         False,
        "care_check":      None,
        "error":           None,
        "documents_saved": [],
        "documents_skipped": 0,
    }

    if not email:
        result["error"]      = "No email on user record"
        result["care_check"] = 1
        _mark_done(user_oid, status="no_email", error=result["error"])
        return result

    if not XN_PORTAL_BASE_URL or not XN_PORTAL_API_KEY:
        result["error"] = "XN_PORTAL_BASE_URL or XN_PORTAL_API_KEY not configured"
        logger.error(result["error"])
        return result

    try:
        api_data = _fetch_document_list(email)
        result["api_status"] = 200
        result["success"]    = api_data.get("success", False)

        all_docs = api_data.get("data", {}).get("documents", [])

        # ── Filter to allowed types (normalised name match) ────────────
        allowed_docs = [
            d for d in all_docs
            if (d.get("document_type_name") or "").strip().lower()
            in ALLOWED_DOCUMENT_TYPES_LOWER
        ]

        # ── Already-saved names for this user (keyed on normalised name) ─
        # Using ONLY document_type_name — avoids all cross-category collisions.
        already_saved_names = {
            (rec.get("document_type_name") or "").strip().lower()
            for rec in db.care_learning_document.find(
                {"user_id": user_id},
                {"document_type_name": 1},
            )
        }

        def _is_saved(doc):
            return (doc.get("document_type_name") or "").strip().lower() in already_saved_names

        # ── Split ──────────────────────────────────────────────────────
        docs_with_url    = [d for d in allowed_docs if d.get("url")     and not _is_saved(d)]
        docs_without_url = [d for d in allowed_docs if not d.get("url") and not _is_saved(d)]
        docs_skipped     = [d for d in allowed_docs if _is_saved(d)]

        logger.info(
            "User %s — allowed:%d  with_url:%d  no_url:%d  skipped:%d",
            email, len(allowed_docs), len(docs_with_url),
            len(docs_without_url), len(docs_skipped),
        )

        # ── No URL → save immediately ──────────────────────────────────
        for doc in docs_without_url:
            _save_document_result(user_id, xn_user_id, doc, "no_url", None)
            result["documents_saved"].append({
                "document_type_name":  doc.get("document_type_name"),
                "care_learning_found": "no_url",
            })

        # ── Has URL → Gemini Vision ────────────────────────────────────
        for i in range(0, len(docs_with_url), GEMINI_BATCH):
            for doc in docs_with_url[i: i + GEMINI_BATCH]:
                found, ai_resp = _gemini_check(doc.get("url", ""))
                _save_document_result(user_id, xn_user_id, doc, found, ai_resp)
                result["documents_saved"].append({
                    "document_type_name":  doc.get("document_type_name"),
                    "care_learning_found": found,
                    "ai_response":         ai_resp,
                })

        result["documents_skipped"] = len(docs_skipped)

        # ── Completion check ───────────────────────────────────────────
        # docs_missing = allowed docs from this API call not yet saved.
        # If zero missing this call → this batch is complete → stamp done.
        # If still missing → re-queue so next call processes them.
        docs_still_missing = len(docs_with_url) + len(docs_without_url)
        # Note: docs_with_url / docs_without_url were already filtered
        # to exclude already-saved ones, so if both are 0 after processing
        # it means everything the API returned this time is now saved.

        if docs_still_missing == 0:
            _mark_done(user_oid,
                       status="ok" if result["success"] else "api_returned_failure",
                       error=None if result["success"] else api_data.get("message"))
            result["care_check"] = 1
            logger.info("User %s done — all %d allowed docs from API saved",
                        email, len(allowed_docs))
        else:
            # Still had docs to process — re-queue for next run
            db.care_learning_users.update_one(
                {"_id": user_oid},
                {"$unset": {"care_check": "", "care_check_status": ""}},
            )
            result["care_check"] = 0
            logger.warning("User %s re-queued — %d docs still pending",
                           email, docs_still_missing)

    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response else None
        result["api_status"] = code
        result["error"]      = f"HTTP {code}: {exc}"
        logger.warning("Outreach HTTP error for %s: %s", email, exc)
        _mark_done(user_oid, status="api_error", error=result["error"])
        result["care_check"] = 1

    except requests.RequestException as exc:
        result["error"] = f"Request failed: {exc}"
        logger.warning("Outreach network error for %s: %s", email, exc)
        _mark_done(user_oid, status="network_error", error=result["error"])
        result["care_check"] = 1

    except Exception as exc:
        result["error"] = f"Unexpected error: {exc}"
        logger.exception("Unexpected error for %s", email)
        _mark_done(user_oid, status="unexpected_error", error=result["error"])
        result["care_check"] = 1

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@bp.route("/care-learning/document-status")
def care_learning_document_status():
    """
    ?email=<email>     → force re-process this user (bypasses care_check)
    ?xn_user_id=<id>   → same by xn_user_id
    (no param)         → batch: next un-flagged user
    """
    email      = request.args.get("email", "").strip().lower()
    xn_user_id = request.args.get("xn_user_id", "").strip()

    fields = {"email": 1, "xn_user_id": 1, "first_name": 1, "last_name": 1, "care_check": 1}

    if email or xn_user_id:
        # ── Single-user mode ───────────────────────────────────────────
        if email:
            q = {"email": {"$regex": f"^{re.escape(email)}$", "$options": "i"}}
        else:
            q = _user_query_single(xn_user_id)

        user = db.care_learning_users.find_one(q, fields)
        if not user:
            return jsonify({
                "success": False,
                "error":   f"No user found for '{email or xn_user_id}'",
            }), 404

        # Reset stale no_url records so they are re-evaluated with correct data
        db.care_learning_document.delete_many({
            "user_id":             str(user["_id"]),
            "care_learning_found": "no_url",
            "url":                 {"$ne": None},   # has URL but was saved as no_url
        })

        result = _process_user(user)

        return jsonify({
            "success":    True,
            "mode":       "single",
            "email":      user.get("email"),
            "xn_user_id": user.get("xn_user_id"),
            "result":     result,
        })

    else:
        # ── Batch mode ─────────────────────────────────────────────────
        query     = _user_query_batch()
        remaining = db.care_learning_users.count_documents(query)
        user      = db.care_learning_users.find_one(query, fields, sort=[("_id", 1)])

        if not user:
            return jsonify({
                "success":   True,
                "mode":      "batch",
                "remaining": 0,
                "found":     0,
                "message":   "All users already processed.",
            })

        result = _process_user(user)

        return jsonify({
            "success":    True,
            "mode":       "batch",
            "remaining":  remaining,
            "found":      1,
            "email":      user.get("email", "—"),
            "xn_user_id": user.get("xn_user_id", "—"),
            "result":     result,
        })


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
        uid = str(user["_id"])
        saved = db.care_learning_document.count_documents({"user_id": uid})
        if saved < len(ALLOWED_DOCUMENT_TYPES):
            db.care_learning_users.update_one(
                {"_id": user["_id"]},
                {"$unset": {"care_check": "", "care_check_status": "", "care_check_error": ""}},
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

    q    = _user_query_single(xn_user_id) if xn_user_id else \
           {"email": {"$regex": f"^{re.escape(email)}$", "$options": "i"}}
    user = db.care_learning_users.find_one(q, {"email": 1, "xn_user_id": 1})
    if not user:
        return jsonify({"error": "User not found"}), 404

    try:
        api_data = _fetch_document_list(user.get("email", ""))
        all_docs = api_data.get("data", {}).get("documents", [])
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    uid = str(user["_id"])
    saved_names = {
        (r.get("document_type_name") or "").strip().lower()
        for r in db.care_learning_document.find({"user_id": uid}, {"document_type_name": 1})
    }

    rows = []
    for d in all_docs:
        name       = d.get("document_type_name") or ""
        normalised = name.strip().lower()
        rows.append({
            "document_type_name":  name,
            "document_category_type": d.get("document_category_type"),
            "in_allowlist":        normalised in ALLOWED_DOCUMENT_TYPES_LOWER,
            "has_url":             bool(d.get("url")),
            "already_saved":       normalised in saved_names,
            "will_process":        normalised in ALLOWED_DOCUMENT_TYPES_LOWER
                                   and normalised not in saved_names,
        })

    matched   = [r for r in rows if r["in_allowlist"]]
    unmatched = [r for r in rows if not r["in_allowlist"]]

    return jsonify({
        "success":         True,
        "email":           user.get("email"),
        "total_from_api":  len(all_docs),
        "matched":         len(matched),
        "unmatched_count": len(unmatched),
        "unmatched_names": [r["document_type_name"] for r in unmatched],
        "rows":            rows,
    })
