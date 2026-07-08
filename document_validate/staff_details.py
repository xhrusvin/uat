from flask import jsonify, request
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
import os
from datetime import datetime
import requests

from . import bp

load_dotenv()

# ==================== CONFIG ====================
MONGO_URI             = os.getenv('MONGO_URI')
DB_NAME               = os.getenv('DB_NAME')
USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_WEBHOOK_KEY')
APP_COUNTRY           = os.getenv('XN_APP_COUNTRY', 'ie')

XN_API_BASE = os.getenv('XN_API_BASE', 'https://uat.expresshealth.ie/xnapi')
XN_API_KEY  = os.getenv('XN_API_KEY', 'xh-uat-9f4a2c8b1d6e3f7a0b5c9d2e4f8a1b3c')

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client      = MongoClient(MONGO_URI)
db                = mongo_client[DB_NAME]
table_name        = db['staff_updated']
leads_collection  = db['users']


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
        api_key     = request.headers.get("Api-Key")
        app_country = request.headers.get("X-App-Country")

        if api_key != USER_EXTERNAL_API_KEY:
            return jsonify({"status": "error", "message": "Invalid or missing Api-Key"}), 401

        # 2. Get JSON payload
        data    = request.get_json(silent=True) or {}
        user_id = data.get("user_id")

        if not user_id:
            return jsonify({"status": "error", "message": "Missing required field: user_id"}), 400

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
                json={"user_id": user_id},
                timeout=30,
            )
            staff_api_status = staff_response.status_code
            staff_api_body   = staff_response.text
            print(f"[recruitments/detail] status={staff_api_status} body={staff_api_body[:200]}")

        except Exception as e:
            staff_api_status = "failed"
            staff_api_body   = str(e)
            print(f"[recruitments/detail] call failed: {e}")

        # 4. Prepare and insert record
        record = {
            "user_id":             str(user_id).strip(),
            "uploaded_at":         datetime.utcnow(),
            "country":             app_country,
            "status":              "1",
            "staff_api_status":    str(staff_api_status),
            "staff_api_response":  staff_api_body,
        }
        result = table_name.insert_one(record)

        return jsonify({
            "status":            "success",
            "message":           "Staff details recorded successfully",
            "record_id":         str(result.inserted_id),
            "user_id":           user_id,
            "staff_api_status":  staff_api_status,
            "staff_api_response": staff_api_body,
            "timestamp":         record["uploaded_at"].isoformat(),
        }), 201

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500