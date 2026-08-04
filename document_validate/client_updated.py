from flask import jsonify, request
from pymongo import MongoClient
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
XN_API_KEY  = os.getenv('XN_API_KEY',  'xh-uat-9f4a2c8b1d6e3f7a0b5c9d2e4f8a1b3c')

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db           = mongo_client[DB_NAME]
table_name   = db['client_updated']


# ==================== ROUTE ====================
@bp.route("/client-updated", methods=["POST"])
def client_updated_webhook():
    """
    Webhook endpoint triggered when a client is updated.

    Expected Headers:
        Api-Key: <XN_PORTAL_WEBHOOK_KEY>
        X-App-Country: ie

    Expected JSON Body:
        { "client_id": "695541458810dcd1ert120d4c45" }
    """
    try:
        # 1. Validate Headers
        api_key     = request.headers.get("Api-Key")
        app_country = request.headers.get("X-App-Country")

        if api_key != USER_EXTERNAL_API_KEY:
            return jsonify({"status": "error", "message": "Invalid or missing Api-Key"}), 401

        # 2. Get JSON payload
        data      = request.get_json(silent=True) or {}
        client_id = data.get("client_id")

        if not client_id:
            return jsonify({"status": "error", "message": "Missing required field: client_id"}), 400

        # 3. Call XN API client-detail to upsert client into clients collection
        sync_url    = f"{XN_API_BASE.rstrip('/')}/common/client-detail"
        sync_status = None
        sync_body   = None

        try:
            sync_response = requests.post(
                sync_url,
                headers={
                    "Authorization": f"Bearer {XN_API_KEY}",
                    "Content-Type":  "application/json",
                },
                json={"client_id": client_id},
                timeout=30,
            )
            sync_status = sync_response.status_code
            sync_body   = sync_response.text
            print(f"[client-detail] status={sync_status} body={sync_body[:200]}")

        except Exception as e:
            sync_status = "failed"
            sync_body   = str(e)
            print(f"[client-detail] call failed: {e}")

        # 4. Prepare and insert record
        record = {
            "client_id":         str(client_id).strip(),
            "uploaded_at":       datetime.utcnow(),
            "country":           app_country,
            "status":            "1",
            "sync_api_status":   str(sync_status),
            "sync_api_response": sync_body,
        }
        result = table_name.insert_one(record)

        return jsonify({
            "status":            "success",
            "message":           "Client synced and recorded successfully",
            "record_id":         str(result.inserted_id),
            "client_id":         client_id,
            "sync_api_status":   sync_status,
            "sync_api_response": sync_body,
            "timestamp":         record["uploaded_at"].isoformat(),
        }), 201

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500