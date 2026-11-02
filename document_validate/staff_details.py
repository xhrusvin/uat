from flask import jsonify, request
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import hmac
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import requests

from . import bp

load_dotenv()
logger = logging.getLogger(__name__)

# ==================== CONFIG ====================
MONGO_URI             = os.getenv('MONGO_URI')
DB_NAME               = os.getenv('DB_NAME')
USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_WEBHOOK_KEY')
APP_COUNTRY           = os.getenv('XN_APP_COUNTRY', 'ie')

# Curser app
CURSER_API_BASE     = os.getenv('CURSER_API_BASE')
CURSER_API_KEY      = os.getenv('CURSER_API_KEY')
CURSER_WEBHOOK_PATH = "automations/webhook/a12a1c6b-d86e-5713-a20c-c8d84c00a06e"

XN_API_BASE = os.getenv('XN_API_BASE', 'https://uat.expresshealth.ie/xnapi')
XN_API_KEY  = os.getenv('XN_API_KEY')  # no hardcoded fallback

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db           = mongo_client[DB_NAME]
table_name   = db['staff_updated']

# Background worker pool (non-blocking for the request cycle)
_bg_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="curser-webhook")


# ==================== BACKGROUND TASK ====================
def _notify_curser(user_id: str) -> None:
    """Fire-and-forget call to Curser automation webhook.
    Never raises: any failure is only logged."""
    if not CURSER_API_BASE or not CURSER_API_KEY:
        logger.warning("[curser] CURSER_API_BASE / CURSER_API_KEY not set, skipping")
        return

    url = f"{CURSER_API_BASE.rstrip('/')}/{CURSER_WEBHOOK_PATH}"
    try:
        resp = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {CURSER_API_KEY}",
                "Content-Type": "application/json",
            },
            json={"id": user_id, "type": "staff"},
            timeout=30,
        )
        logger.info("[curser] user_id=%s status=%s body=%s",
                    user_id, resp.status_code, resp.text[:200])
    except Exception as e:
        logger.error("[curser] call failed for user_id=%s: %s", user_id, e)


# ==================== ROUTE ====================
@bp.route("/staff-details", methods=["POST"])
def staff_details_webhook():
    """
    Webhook endpoint triggered to fetch and store staff details.

    Expected Headers:
        Api-Key: <XN_PORTAL_WEBHOOK_KEY>
        X-App-Country: ie

    Expected JSON Body:
        { "user_id": "695541458810dcdf8b0d4c51" }
    """
    try:
        # 1. Validate Headers
        api_key     = request.headers.get("Api-Key") or ""
        app_country = request.headers.get("X-App-Country")

        if not USER_EXTERNAL_API_KEY or not hmac.compare_digest(api_key, USER_EXTERNAL_API_KEY):
            return jsonify({"status": "error", "message": "Invalid or missing Api-Key"}), 401

        # 2. Get JSON payload
        data    = request.get_json(silent=True) or {}
        user_id = data.get("user_id")

        if not user_id:
            return jsonify({"status": "error", "message": "Missing required field: user_id"}), 400

        user_id = str(user_id).strip()

        # 3. Call XN API recruitments/detail to fetch and sync staff
        staff_api_url    = f"{XN_API_BASE.rstrip('/')}/recruitments/detail"
        staff_api_status = None
        staff_api_body   = None

        try:
            staff_response = requests.post(
                staff_api_url,
                headers={
                    "Authorization": f"Bearer {XN_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={"_id": user_id},
                timeout=30,
            )
            staff_api_status = staff_response.status_code
            staff_api_body   = staff_response.text
            logger.info("[recruitments/detail] status=%s body=%s",
                        staff_api_status, staff_api_body[:200])

        except Exception as e:
            staff_api_status = "failed"
            staff_api_body   = str(e)
            logger.error("[recruitments/detail] call failed: %s", e)

        # 4. Prepare and insert record
        record = {
            "user_id":            user_id,
            "uploaded_at":        datetime.now(timezone.utc),
            "country":            app_country,
            "status":             "1",
            "staff_api_status":   str(staff_api_status),
            "staff_api_response": staff_api_body,
        }
        result = table_name.insert_one(record)

        # 5. On success -> trigger Curser webhook in background (never blocks / breaks response)
        staff_ok = isinstance(staff_api_status, int) and 200 <= staff_api_status < 300
        if staff_ok:
            try:
                _bg_executor.submit(_notify_curser, user_id)
            except Exception as e:
                logger.error("[curser] could not schedule background call: %s", e)

        return jsonify({
            "status":             "success",
            "message":            "Staff details recorded successfully",
            "record_id":          str(result.inserted_id),
            "user_id":            user_id,
            "staff_api_status":   staff_api_status,
            "staff_api_response": staff_api_body,
            "timestamp":          record["uploaded_at"].isoformat(),
        }), 201

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500