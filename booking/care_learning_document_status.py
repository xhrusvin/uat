"""
booking/care_learning_document_status.py
─────────────────────────────────────────
Calls the XN Portal outreach API to fetch the document list for users
in the `users` collection.

Routes
------
  GET /booking/care-learning/document-status
      - No xn_user_id param  → batch: runs over users where care_check != 1
      - ?xn_user_id=<id>     → single: runs for that user only

  GET /booking/care-learning/document-status/user/<xn_user_id>
      - Clean-URL version of the single-user mode

Flag behaviour
--------------
  care_check is set to 1 on EVERY outcome so the user is NEVER
  picked up again by the batch runner, regardless of whether the
  API call succeeded or failed.

  care_check_status records WHY:
    "ok"                  — API returned success: true
    "api_returned_failure"— API returned success: false
    "api_error"           — HTTP 4xx / 5xx from the API
    "network_error"       — timeout / connection refused
    "no_email"            — user has no email field
    "unexpected_error"    — unhandled exception

  The only case where care_check is NOT stamped is when the env vars
  (XN_PORTAL_BASE_URL / XN_PORTAL_API_KEY) are missing — that is a
  server config problem, not a per-user problem, so those users stay
  eligible and will retry once config is fixed.

  To manually re-run a user:
      db.users.update_one({"xn_user_id": "<id>"}, {"$unset": {"care_check": ""}})

API called
----------
  GET {XN_PORTAL_BASE_URL}/ai/recruitments/user-document-list
  Headers : Api-Key:       {XN_PORTAL_API_KEY}
            X-App-Country: {XN_APP_COUNTRY}
  Body    : { "email": <users.email> }   (JSON body on GET request)
"""

import os
import logging
from datetime import datetime

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

OUTREACH_URL = f"{XN_PORTAL_BASE_URL}/ai/recruitments/user-document-list"

PER_PAGE = 20


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────

def _api_headers() -> dict:
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
        headers=_api_headers(),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _mark_done(user_oid, status: str, error=None):
    """
    Stamp care_check = 1 unconditionally so this user is excluded
    from every future batch run.  Also records the outcome for audit.
    """
    db.users.update_one(
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
    """Users where care_check is absent OR not equal to 1."""
    return {
        "$or": [
            {"care_check": {"$exists": False}},
            {"care_check": {"$ne": 1}},
        ]
    }


# ──────────────────────────────────────────────────────────────────────────────
# Core: process one user
# ──────────────────────────────────────────────────────────────────────────────

def _process_user(user: dict) -> dict:
    """
    Call the outreach API for one user and ALWAYS stamp care_check = 1
    afterwards (except on server config errors) so the user is skipped
    on every subsequent batch run.
    """
    email      = (user.get("email") or "").strip()
    xn_user_id = user.get("xn_user_id") or ""
    user_oid   = user["_id"]

    result = {
        "user_id":    str(user_oid),
        "xn_user_id": xn_user_id,
        "email":      email,
        "api_status": None,
        "success":    False,
        "care_check": None,
        "error":      None,
        "documents":  [],
    }

    # ── Guard: missing email ───────────────────────────────────────────
    if not email:
        result["error"]      = "No email on user record"
        result["care_check"] = 1
        _mark_done(user_oid, status="no_email", error=result["error"])
        return result

    # ── Guard: env not configured ──────────────────────────────────────
    # Do NOT mark done — this is a server problem, not a per-user one.
    if not XN_PORTAL_BASE_URL or not XN_PORTAL_API_KEY:
        result["error"] = "XN_PORTAL_BASE_URL or XN_PORTAL_API_KEY not configured"
        logger.error(result["error"])
        return result

    # ── Call the API ───────────────────────────────────────────────────
    try:
        api_data = _fetch_document_list(email)
        result["api_status"] = 200
        result["success"]    = api_data.get("success", False)
        result["documents"]  = api_data.get("data", {}).get("documents", [])

        # Stamp care_check = 1 — both on success AND on api_returned_failure.
        # Either way the API has responded; there is nothing to retry.
        status = "ok" if result["success"] else "api_returned_failure"
        error  = None if result["success"] else api_data.get("message")
        _mark_done(user_oid, status=status, error=error)
        result["care_check"] = 1

    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response else None
        result["api_status"] = status_code
        result["error"]      = f"HTTP {status_code}: {str(exc)}"
        logger.warning("Outreach API HTTP error for %s: %s", email, exc)
        # 4xx → API rejected the request; retrying won't help → mark done.
        # 5xx → server error; mark done to prevent hammering.
        _mark_done(user_oid, status="api_error", error=result["error"])
        result["care_check"] = 1

    except requests.RequestException as exc:
        result["error"] = f"Request failed: {str(exc)}"
        logger.warning("Outreach API network error for %s: %s", email, exc)
        # Network/timeout — mark done; operator can reset manually if needed.
        _mark_done(user_oid, status="network_error", error=result["error"])
        result["care_check"] = 1

    except Exception as exc:
        result["error"] = f"Unexpected error: {str(exc)}"
        logger.exception("Unexpected error processing user %s", email)
        _mark_done(user_oid, status="unexpected_error", error=result["error"])
        result["care_check"] = 1

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Core runner (shared by all routes)
# ──────────────────────────────────────────────────────────────────────────────

def _run(xn_user_id: str = "", page: int = 1) -> dict:
    """
    Single-user mode : xn_user_id provided → process that one user.
    Batch mode       : no xn_user_id → page through un-flagged users.
    """
    fields = {"email": 1, "xn_user_id": 1,
               "first_name": 1, "last_name": 1, "care_check": 1}

    if xn_user_id:
        user = db.users.find_one(_user_query_single(xn_user_id), fields)
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

    # Batch
    query = _user_query_batch()
    total = db.users.count_documents(query)
    pages = max((total + PER_PAGE - 1) // PER_PAGE, 1)

    raw_users = list(
        db.users
        .find(query, fields)
        .sort("_id", 1)
        .skip((page - 1) * PER_PAGE)
        .limit(PER_PAGE)
    )

    results = [_process_user(u) for u in raw_users]

    return {
        "mode":    "batch",
        "total":   total,
        "page":    page,
        "pages":   pages,
        "found":   len(raw_users),
        "results": results,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@bp.route("/care-learning/document-status")
def care_learning_document_status():
    """
    Query params
    ------------
    xn_user_id  (optional) — single-user mode
    page        (optional) — batch pagination, default 1
    """
    xn_user_id = request.args.get("xn_user_id", "").strip()
    page       = max(int(request.args.get("page", 1)), 1)

    payload = _run(xn_user_id=xn_user_id, page=page)
    return jsonify({"success": True, **payload})


@bp.route("/care-learning/document-status/user/<xn_user_id>")
def care_learning_document_status_user(xn_user_id: str):
    """Clean-URL alias: /booking/care-learning/document-status/user/<xn_user_id>"""
    payload = _run(xn_user_id=xn_user_id.strip(), page=1)
    return jsonify({"success": True, **payload})
