"""
booking/care_learning_document_status.py
─────────────────────────────────────────
1.  Calls XN Portal outreach API  →  GET user document list by email
2.  Filters to ALLOWED_DOCUMENT_TYPES only
3.  For documents that have a URL, takes 2 at a time and sends each to
    Gemini Vision to check whether the document contains any text or logo
    related to "Care Learning"
4.  Stores results in  care_learning_document  collection
5.  Stamps  care_check = 1  on the user so they are never re-processed

Routes
------
  GET /booking/care-learning/document-status
      ?xn_user_id=<id>   →  single-user mode
      (no param)         →  batch: picks next un-flagged user

  GET /booking/care-learning/document-status/user/<xn_user_id>
      Clean-URL single-user alias

care_learning_document schema
------------------------------
  {
    user_id            : str (users._id as string),
    xn_user_id         : str,
    document_id        : str | None,
    document_type_name : str,
    status             : str,          # from API  (pending / approved / expired …)
    url                : str | None,
    care_learning_found: "yes" | "no" | "no_url" | "error",
    ai_response        : str | None,   # raw Gemini explanation
    ai_checked_at      : datetime,
  }

Flag behaviour
--------------
  care_check = 1 is stamped on EVERY outcome (see _mark_done).
  Only exception: env-var misconfiguration (server problem, not user).
  Reset manually:
    db.care_learning_users.update_one({"xn_user_id":"<id>"},{"$unset":{"care_check":""}})
"""

import os
import base64
import logging
from datetime import datetime

import threading
import requests
from bson import ObjectId
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

OUTREACH_URL   = f"{XN_PORTAL_BASE_URL}/ai/recruitments/user-document-list"
GEMINI_URL     = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.5-flash:generateContent"
)

# Process this many URL-bearing documents per user call through Gemini
GEMINI_BATCH   = 2

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
    "Neurogenic Bowel Dysfunction Training (Practical)",
    "Management Of Blood & Body Substance Spills",
    "Haccp/Food Safety",
}

# ──────────────────────────────────────────────────────────────────────────────
# Outreach API helpers
# ──────────────────────────────────────────────────────────────────────────────

def _outreach_headers() -> dict:
    return {
        "Api-Key":       XN_PORTAL_API_KEY,
        "X-App-Country": XN_APP_COUNTRY,
        "Content-Type":  "application/json",
    }


def _fetch_document_list(email: str) -> dict:
    """GET the outreach API. Raises requests.HTTPError on 4xx/5xx."""
    resp = requests.get(
        OUTREACH_URL,
        json={"email": email},
        headers=_outreach_headers(),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


# ──────────────────────────────────────────────────────────────────────────────
# Gemini Vision helpers
# ──────────────────────────────────────────────────────────────────────────────

def _download_as_base64(url: str) -> tuple[str, str]:
    """
    Download the file at url and return (base64_data, mime_type).
    Detects PDF vs image from the URL path or Content-Type header.
    Raises on network / HTTP errors.
    """
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()

    content_type = resp.headers.get("Content-Type", "")
    if "pdf" in content_type or url.split("?")[0].lower().endswith(".pdf"):
        mime_type = "application/pdf"
    elif "png" in content_type or url.split("?")[0].lower().endswith(".png"):
        mime_type = "image/png"
    elif "jpg" in content_type or "jpeg" in content_type or url.split("?")[0].lower().endswith((".jpg", ".jpeg")):
        mime_type = "image/jpeg"
    else:
        mime_type = content_type.split(";")[0].strip() or "application/octet-stream"

    b64 = base64.b64encode(resp.content).decode("utf-8")
    return b64, mime_type


def _gemini_check(url: str) -> tuple[str, str]:
    """
    Send the document at `url` to Gemini Vision and ask whether it
    contains any text or logo related to 'Care Learning'.

    Returns:
        care_learning_found : "yes" | "no" | "error"
        ai_response         : Gemini's explanation string
    """
    if not GEMINI_API_KEY:
        return "error", "GEMINI_API_KEY not configured"

    try:
        b64_data, mime_type = _download_as_base64(url)
    except Exception as exc:
        logger.warning("Failed to download document for Gemini: %s — %s", url[:80], exc)
        return "error", f"Document download failed: {exc}"

    prompt = (
        "Examine this document carefully. "
        "Does it contain any text, heading, logo, watermark, or branding "
        "related to 'Care Learning' (including variations such as "
        "'carelearning', 'care-learning', 'Care Learning Ireland', or any "
        "recognisable Care Learning logo)? "
        "Answer with YES or NO on the very first line, then on the next "
        "lines briefly explain what you found (or did not find)."
    )

    payload = {
        "contents": [
            {
                "parts": [
                    {
                        "inline_data": {
                            "mime_type": mime_type,
                            "data":      b64_data,
                        }
                    },
                    {"text": prompt},
                ]
            }
        ]
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

        # Extract text from Gemini response
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
        logger.warning("Gemini API error for url %s: %s", url[:80], exc)
        return "error", f"Gemini API error: {exc}"


# ──────────────────────────────────────────────────────────────────────────────
# Persistence
# ──────────────────────────────────────────────────────────────────────────────

def _save_document_result(
    user_id: str,
    xn_user_id: str,
    doc: dict,
    care_learning_found: str,
    ai_response: str,
):
    """
    Upsert into care_learning_document keyed on (user_id, document_id).
    If document_id is None (category-3 docs), key on (user_id, document_type_name).
    """
    document_id = doc.get("document_id")

    match_filter = {
        "user_id":    user_id,
        **(
            {"document_id": document_id}
            if document_id
            else {"document_type_name": doc.get("document_type_name")}
        ),
    }

    db.care_learning_document.update_one(
        match_filter,
        {
            "$set": {
                "user_id":             user_id,
                "xn_user_id":          xn_user_id,
                "document_id":         document_id,
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
    """
    Stamp care_check = 1 so this user is excluded from every future batch run.
    """
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
    """
    Users where care_check is absent OR not equal to 1, AND not
    currently being processed by another thread/terminal.
    """
    return {
        "$and": [
            {
                "$or": [
                    {"care_check": {"$exists": False}},
                    {"care_check": {"$ne": 1}},
                ]
            },
            # Exclude users being processed right now by another worker
            {"care_check_status": {"$ne": "processing"}},
        ]
    }


# ──────────────────────────────────────────────────────────────────────────────
# Core: process one user
# ──────────────────────────────────────────────────────────────────────────────

def _process_user(user: dict) -> dict:
    """
    1. Call outreach API to get the user's document list.
    2. Filter to ALLOWED_DOCUMENT_TYPES.
    3. For docs WITH a url: take 2 at a time through Gemini Vision.
    4. For docs WITHOUT a url: record care_learning_found = "no_url".
    5. Persist every result to care_learning_document.
    6. Stamp care_check = 1 on the user.
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
    }

    # ── Guard: missing email ───────────────────────────────────────────
    if not email:
        result["error"]      = "No email on user record"
        result["care_check"] = 1
        _mark_done(user_oid, status="no_email", error=result["error"])
        return result

    # ── Guard: env not configured (server problem — don't stamp user) ──
    if not XN_PORTAL_BASE_URL or not XN_PORTAL_API_KEY:
        result["error"] = "XN_PORTAL_BASE_URL or XN_PORTAL_API_KEY not configured"
        logger.error(result["error"])
        return result

    # ── Call outreach API ──────────────────────────────────────────────
    try:
        api_data = _fetch_document_list(email)
        result["api_status"] = 200
        result["success"]    = api_data.get("success", False)

        all_docs = api_data.get("data", {}).get("documents", [])

        # Filter to allowed types only
        allowed_docs = [
            d for d in all_docs
            if d.get("document_type_name") in ALLOWED_DOCUMENT_TYPES
        ]

        # ── Split docs into url-bearing and url-less ───────────────────
        docs_with_url    = [d for d in allowed_docs if d.get("url")]
        docs_without_url = [d for d in allowed_docs if not d.get("url")]

        # ── Docs without URL — save immediately, no Gemini call ────────
        for doc in docs_without_url:
            _save_document_result(
                user_id=user_id,
                xn_user_id=xn_user_id,
                doc=doc,
                care_learning_found="no_url",
                ai_response=None,
            )
            result["documents_saved"].append({
                "document_id":         doc.get("document_id"),
                "document_type_name":  doc.get("document_type_name"),
                "care_learning_found": "no_url",
            })

        # ── Docs WITH URL — process in batches of GEMINI_BATCH ─────────
        for i in range(0, len(docs_with_url), GEMINI_BATCH):
            batch = docs_with_url[i: i + GEMINI_BATCH]

            for doc in batch:
                url = doc.get("url", "")
                care_learning_found, ai_response = _gemini_check(url)

                _save_document_result(
                    user_id=user_id,
                    xn_user_id=xn_user_id,
                    doc=doc,
                    care_learning_found=care_learning_found,
                    ai_response=ai_response,
                )
                result["documents_saved"].append({
                    "document_id":         doc.get("document_id"),
                    "document_type_name":  doc.get("document_type_name"),
                    "care_learning_found": care_learning_found,
                    "ai_response":         ai_response,
                })

        # Stamp done — whether API returned success or failure
        api_status = "ok" if result["success"] else "api_returned_failure"
        api_error  = None if result["success"] else api_data.get("message")
        _mark_done(user_oid, status=api_status, error=api_error)
        result["care_check"] = 1

    except requests.HTTPError as exc:
        status_code          = exc.response.status_code if exc.response else None
        result["api_status"] = status_code
        result["error"]      = f"HTTP {status_code}: {str(exc)}"
        logger.warning("Outreach API HTTP error for %s: %s", email, exc)
        _mark_done(user_oid, status="api_error", error=result["error"])
        result["care_check"] = 1

    except requests.RequestException as exc:
        result["error"] = f"Request failed: {str(exc)}"
        logger.warning("Outreach API network error for %s: %s", email, exc)
        _mark_done(user_oid, status="network_error", error=result["error"])
        result["care_check"] = 1

    except Exception as exc:
        result["error"] = f"Unexpected error: {str(exc)}"
        logger.exception("Unexpected error processing user %s", email)
        _mark_done(user_oid, status="unexpected_error", error=result["error"])
        result["care_check"] = 1

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Core runner
# ──────────────────────────────────────────────────────────────────────────────

def _run(xn_user_id: str = "") -> dict:
    """
    Single-user mode : xn_user_id provided → process that one user.
    Batch mode       : no xn_user_id → pick next un-flagged user (one per call).
    """
    fields = {
        "email": 1, "xn_user_id": 1,
        "first_name": 1, "last_name": 1, "care_check": 1,
    }

    if xn_user_id:
        user = db.care_learning_users.find_one(_user_query_single(xn_user_id), fields)
        if not user:
            return {
                "mode":    "single",
                "found":   0,
                "results": [],
                "error":   f"No user found with xn_user_id '{xn_user_id}'",
            }
        return {
            "mode":    "single",
            "found":   1,
            "results": [_process_user(user)],
        }

    # Batch — pick exactly ONE oldest un-flagged user per call
    query     = _user_query_batch()
    remaining = db.care_learning_users.count_documents(query)
    user      = db.care_learning_users.find_one(query, fields, sort=[("_id", 1)])

    if not user:
        return {
            "mode":      "batch",
            "remaining": 0,
            "found":     0,
            "results":   [],
            "message":   "All users already processed.",
        }

    return {
        "mode":      "batch",
        "remaining": remaining,
        "found":     1,
        "results":   [_process_user(user)],
    }


# ──────────────────────────────────────────────────────────────────────────────
# Background worker
# ──────────────────────────────────────────────────────────────────────────────

def _background_process(user: dict):
    """
    Runs _process_user in a daemon thread so the HTTP request returns
    immediately — avoiding gateway timeouts on slow Gemini calls.
    """
    try:
        _process_user(user)
    except Exception as exc:
        logger.exception("Background processing failed for user %s: %s",
                         user.get("xn_user_id"), exc)


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@bp.route("/care-learning/document-status")
def care_learning_document_status():
    """
    Batch mode  (no xn_user_id):
        1. Picks the next un-flagged user instantly.
        2. Marks them as care_check_status = "processing" to prevent
           other workers from picking the same user.
        3. Returns 202 immediately with remaining count.
        4. Processes the user (Gemini calls) in a background thread.

    Single mode (?xn_user_id=<id>):
        Same async behaviour for one specific user.
    """
    xn_user_id = request.args.get("xn_user_id", "").strip()

    fields = {
        "email": 1, "xn_user_id": 1,
        "first_name": 1, "last_name": 1, "care_check": 1,
    }

    if xn_user_id:
        # ── Single-user mode ──────────────────────────────────────────
        user = db.care_learning_users.find_one(
            _user_query_single(xn_user_id), fields
        )
        if not user:
            return jsonify({
                "success": False,
                "error":   f"No user found with xn_user_id '{xn_user_id}'",
            }), 404

        # Lock immediately
        db.care_learning_users.update_one(
            {"_id": user["_id"]},
            {"$set": {"care_check_status": "processing",
                      "care_check_updated_at": datetime.utcnow()}},
        )
        t = threading.Thread(target=_background_process, args=(user,), daemon=True)
        t.start()

        return jsonify({
            "success":   True,
            "mode":      "single",
            "found":     1,
            "accepted":  True,
            "message":   f"Processing started for {user.get('email', xn_user_id)}",
        }), 202

    else:
        # ── Batch mode ────────────────────────────────────────────────
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

        # Lock immediately so parallel terminals skip this user
        db.care_learning_users.update_one(
            {"_id": user["_id"]},
            {"$set": {"care_check_status": "processing",
                      "care_check_updated_at": datetime.utcnow()}},
        )

        t = threading.Thread(target=_background_process, args=(user,), daemon=True)
        t.start()

        return jsonify({
            "success":   True,
            "mode":      "batch",
            "remaining": remaining,
            "found":     1,
            "accepted":  True,
            "email":     user.get("email", "—"),
            "xn_user_id": user.get("xn_user_id", "—"),
            "message":   "Processing started in background.",
        }), 202


@bp.route("/care-learning/document-status/user/<xn_user_id>")
def care_learning_document_status_user(xn_user_id: str):
    """Clean-URL alias for single-user async mode."""
    return care_learning_document_status()
