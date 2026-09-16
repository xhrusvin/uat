"""
booking/care_learning_document_status.py
─────────────────────────────────────────
Calls the XN Portal outreach API to fetch the document list for users
in the `users` collection.

Routes
------
  GET /booking/care-learning/document-status
      - No xn_user_id param  → runs over users where care_check != 1
      - ?xn_user_id=<id>     → runs for that single user only

  GET /booking/care-learning/document-status/data
      - Same logic, lightweight JSON (no page render)

API called
----------
  GET {XN_PORTAL_BASE_URL}/ai/recruitments/user-document-list
  Headers : Api-Key: {XN_PORTAL_API_KEY}
            X-App-Country: {XN_APP_COUNTRY}
  Payload : { "email": <users.email> }   (sent as JSON body on GET)
"""

import os
import logging
from datetime import datetime

import requests
from bson import ObjectId
from flask import jsonify, render_template, request, current_app

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
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _api_headers() -> dict:
    return {
        "Api-Key":       XN_PORTAL_API_KEY,
        "X-App-Country": XN_APP_COUNTRY,
        "Content-Type":  "application/json",
    }


def _fetch_document_list(email: str) -> dict:
    """
    Call the outreach API for one user.
    Returns the parsed JSON response or raises on failure.
    """
    resp = requests.get(
        OUTREACH_URL,
        json={"email": email},          # GET with JSON body as per spec
        headers=_api_headers(),
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _user_query_single(xn_user_id: str) -> dict:
    """Filter for one specific user."""
    return {"xn_user_id": xn_user_id}


def _user_query_batch() -> dict:
    """
    Filter for users where care_check doesn't exist OR is not 1.
    This is the bulk / background mode.
    """
    return {
        "$or": [
            {"care_check": {"$exists": False}},
            {"care_check": {"$ne": 1}},
        ]
    }


def _serialize(v):
    """Make a value JSON-safe."""
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def _process_user(user: dict) -> dict:
    """
    Call the outreach API for one user, parse the response,
    and update the user's care_check flag in the DB.

    Returns a result dict suitable for the JSON response.
    """
    email      = user.get("email") or ""
    xn_user_id = user.get("xn_user_id") or ""
    user_oid   = user["_id"]

    result = {
        "user_id":    str(user_oid),
        "xn_user_id": xn_user_id,
        "email":      email,
        "api_status": None,
        "success":    False,
        "error":      None,
        "documents":  [],
    }

    if not email:
        result["error"] = "No email on user record"
        return result

    if not XN_PORTAL_BASE_URL or not XN_PORTAL_API_KEY:
        result["error"] = "XN_PORTAL_BASE_URL or XN_PORTAL_API_KEY not configured"
        logger.error(result["error"])
        return result

    try:
        api_data = _fetch_document_list(email)
        result["api_status"] = 200
        result["success"]    = api_data.get("success", False)

        documents = api_data.get("data", {}).get("documents", [])
        result["documents"] = documents

        # ── Mark user as care_check = 1 after a successful API call ──
        if result["success"]:
            db.users.update_one(
                {"_id": user_oid},
                {
                    "$set": {
                        "care_check":          1,
                        "care_check_updated_at": datetime.utcnow(),
                    }
                },
            )

    except requests.HTTPError as exc:
        result["api_status"] = exc.response.status_code if exc.response else None
        result["error"]      = f"HTTP {result['api_status']}: {str(exc)}"
        logger.warning("Outreach API HTTP error for %s: %s", email, exc)

    except requests.RequestException as exc:
        result["error"] = f"Request failed: {str(exc)}"
        logger.warning("Outreach API request error for %s: %s", email, exc)

    except Exception as exc:
        result["error"] = f"Unexpected error: {str(exc)}"
        logger.exception("Unexpected error processing user %s", email)

    return result


# ──────────────────────────────────────────────────────────────────────────────
# Core processing logic (shared by both routes)
# ──────────────────────────────────────────────────────────────────────────────

def _run(xn_user_id: str = "", page: int = 1) -> dict:
    """
    Single-user mode  : xn_user_id is set → find that user, call API, return result.
    Batch mode        : xn_user_id is empty → page through users where care_check != 1.
    """
    if xn_user_id:
        # ── Single-user mode ──────────────────────────────────────────
        user = db.users.find_one(
            _user_query_single(xn_user_id),
            {"email": 1, "xn_user_id": 1, "first_name": 1,
             "last_name": 1, "care_check": 1},
        )
        if not user:
            return {
                "mode":    "single",
                "found":   0,
                "results": [],
                "error":   f"No user with xn_user_id '{xn_user_id}'",
            }

        return {
            "mode":    "single",
            "found":   1,
            "results": [_process_user(user)],
        }

    else:
        # ── Batch mode ────────────────────────────────────────────────
        query = _user_query_batch()
        total = db.users.count_documents(query)
        pages = max((total + PER_PAGE - 1) // PER_PAGE, 1)

        raw_users = list(
            db.users
            .find(query, {"email": 1, "xn_user_id": 1,
                          "first_name": 1, "last_name": 1, "care_check": 1})
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
    JSON endpoint — triggers outreach API call(s) and returns structured results.

    Query params
    ------------
    xn_user_id  (optional) : run for a single user
    page        (optional) : pagination for batch mode (default 1)
    """
    xn_user_id = request.args.get("xn_user_id", "").strip()
    page       = max(int(request.args.get("page", 1)), 1)

    payload = _run(xn_user_id=xn_user_id, page=page)

    # Top-level success = True as long as the endpoint itself worked
    return jsonify({
        "success": True,
        **payload,
    })


@bp.route("/care-learning/document-status/user/<xn_user_id>")
def care_learning_document_status_user(xn_user_id: str):
    """
    Convenience route: GET /booking/care-learning/document-status/user/<xn_user_id>
    Identical to ?xn_user_id= but as a clean URL.
    """
    payload = _run(xn_user_id=xn_user_id.strip(), page=1)
    return jsonify({
        "success": True,
        **payload,
    })
