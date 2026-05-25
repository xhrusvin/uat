from flask import jsonify, request
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime
import requests

from . import bp

load_dotenv()

# ==================== CONFIG ====================
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')

USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_WEBHOOK_KEY')
APP_COUNTRY = os.getenv('XN_APP_COUNTRY', 'ie')

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]

# Collection for shift updates
table_name = db['shift_updated']


# ==================== ROUTE ====================
@bp.route("/staff-deatils", methods=["POST"])
def shift_updated_webhook():
    """
    Webhook endpoint triggered when a shift is updated.

    Expected Headers:
        Api-Key: sk-8f3a9c1b7d4e6f0a2b9c8d7e7
        X-App-Country: ie

    Expected JSON Body:
        {
            "shift_id": "695541458810dcd1ert120d4c45"
        }

    This route calls the external shift-updated API and saves the record
    to the "shift_updated" collection.
    """
    try:
        # 1. Validate Headers
        api_key = request.headers.get("Api-Key")
        app_country = request.headers.get("X-App-Country")

        if api_key != USER_EXTERNAL_API_KEY:
            return jsonify({
                "status": "error",
                "message": "Invalid or missing Api-Key"
            }), 401

        # 2. Get JSON payload
        data = request.get_json(silent=True) or {}

        shift_id = data.get("shift_id")

        if not shift_id:
            return jsonify({
                "status": "error",
                "message": "Missing required fields: shift_id"
            }), 400

        # 3. Call external shift-updated API
        # if app_country and app_country.lower() == "ie":
        #     shift_updated_url = "https://expresshealth.ie/document-validate/shift-updated"
        # else:
        #     shift_updated_url = "https://uat.expresshealth.ie/document-validate/shift-updated"

        # shift_api_status = None
        # shift_api_body = None

        # try:
        #     shift_response = requests.post(
        #         shift_updated_url,
        #         headers={
        #             "Api-Key": "sk-8f3a9c1b7d4e6f0a2b9c8d7e7",
        #             "X-App-Country": app_country or APP_COUNTRY,
        #             "Content-Type": "application/json"
        #         },
        #         json={"shift_id": shift_id},
        #         timeout=10
        #     )
        #     shift_api_status = shift_response.status_code
        #     shift_api_body = shift_response.text
        #     print("Shift updated response status:", shift_api_status)
        #     print("Shift updated response body:", shift_api_body)

        # except Exception as e:
        #     shift_api_status = "failed"
        #     shift_api_body = str(e)
        #     print("Shift updated API call failed:", str(e))

        # 4. Prepare record for shift_updated collection
        record = {
            "shift_id": str(shift_id).strip(),
            "uploaded_at": datetime.utcnow(),
            "country": app_country,
            "status": "1",
            #"shift_api_status": str(shift_api_status),
            #"shift_api_response": shift_api_body
        }

        # 5. Insert into shift_updated collection
        result = table_name.insert_one(record)

        return jsonify({
            "status": "success",
            "message": "Shift updated recorded successfully",
            "record_id": str(result.inserted_id),
            "shift_id": shift_id,
            # "shift_api_status": shift_api_status,
            # "shift_api_response": shift_api_body,
            "timestamp": record["uploaded_at"].isoformat()
        }), 201

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Server error: {str(e)}"
        }), 500