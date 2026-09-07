from flask import jsonify, request
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime
import requests

from . import bp

load_dotenv()

# ==================== CONFIG ====================
MONGO_URI            = os.getenv('MONGO_URI')
DB_NAME              = os.getenv('DB_NAME')
USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_WEBHOOK_KEY')
APP_COUNTRY          = os.getenv('XN_APP_COUNTRY', 'ie')

XN_API_BASE     = os.getenv('XN_API_BASE', 'https://uat.expresshealth.ie/xnapi')
XN_API_KEY      = os.getenv('XN_API_KEY', 'xh-uat-9f4a2c8b1d6e3f7a0b5c9d2e4f8a1b3c')

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db           = mongo_client[DB_NAME]
table_name   = db['document_status']


# ==================== ROUTE ====================
@bp.route("/document-status", methods=["POST"])
def document_status_webhook():
    try:
        # 1. Validate Headers
        api_key     = request.headers.get("Api-Key")
        app_country = request.headers.get("X-App-Country")

        if api_key != USER_EXTERNAL_API_KEY:
            return jsonify({"status": "error", "message": "Invalid or missing Api-Key"}), 401

        # 2. Get JSON payload
        data        = request.get_json(silent=True) or {}
        document_id = data.get("document_id")
        user_id     = data.get("user_id")

        if not document_id:
            return jsonify({"status": "error", "message": "Missing required field: document_id"}), 400

       

        # 4. Call validate_document_noai
        WEB_URL              = os.getenv('WEB_URL', '').rstrip('/')
        validate_url         = f"{WEB_URL}/admin/validate_document_noai"
        validate_status      = None
        validate_body        = None

        try:
            validate_response = requests.get(
                validate_url,
                params={"limit": 1, "xn_user_id": user_id},
                timeout=90,
            )
            validate_status = validate_response.status_code
            validate_body   = validate_response.text
            print(f"[validate_document_noai] status={validate_status} body={validate_body[:200]}")

        except Exception as e:
            validate_status = "failed"
            validate_body   = str(e)
            print(f"[validate_document_noai] call failed: {e}")

        # 5. Prepare and insert record
        record = {
            "document_id":              str(document_id).strip(),
            "user_id":                  str(user_id).strip(),
            "uploaded_at":              datetime.utcnow(),
            "country":                  app_country,
            "status":                   "1"
        }
        result = table_name.insert_one(record)

        return jsonify({
            "status":                   "success",
            "message":                  "Document status synced and recorded successfully",
            "record_id":                str(result.inserted_id),
            "document_id":              document_id,
            "user_id":                  user_id,
            "sync_api_status":          validate_status,   # ← was sync_status
            "validate_api_response":    validate_body,
            "validate_api_url":         validate_url,
            "timestamp":                record["uploaded_at"].isoformat(),
        }), 201

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500