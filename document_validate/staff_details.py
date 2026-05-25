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
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')

USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_WEBHOOK_KEY')
APP_COUNTRY = os.getenv('XN_APP_COUNTRY', 'ie')

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]

# Collection for staff details updates
table_name = db['staff_updated']

# Optional: reference to users/leads if needed
leads_collection = db['users']


# ==================== ROUTE ====================
@bp.route("/staff-details", methods=["POST"])
def staff_details_webhook():
    """
    Webhook endpoint triggered to fetch and store staff details.

    Expected Headers:
        Api-Key: sk-8f3a9c1b7d4e6f0a2b9c8d7e6f5e4f3a2b1c0d9e8f7
        X-App-Country: ie

    Expected JSON Body:
        {
            "user_id": "695541458810dcdf8b0d4c51"
        }

    This route calls the external staff-details API and saves the record
    to the "staff_updated" collection.
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

        user_id = data.get("user_id")

        if not user_id:
            return jsonify({
                "status": "error",
                "message": "Missing required fields: user_id"
            }), 400

        # 3. Call external staff-details API
        if app_country and app_country.lower() == "ie":
            staff_details_url = "https://expresshealth.ie/document-validate/staff-details"
        else:
            staff_details_url = "https://uat.expresshealth.ie/document-validate/staff-details"

        staff_api_status = None
        staff_api_body = None

        try:
            staff_response = requests.post(
                staff_details_url,
                headers={
                    "Api-Key": USER_EXTERNAL_API_KEY,
                    "X-App-Country": app_country or APP_COUNTRY,
                    "Content-Type": "application/json"
                },
                json={"user_id": user_id},
                timeout=10
            )
            staff_api_status = staff_response.status_code
            staff_api_body = staff_response.text
            print("Staff details response status:", staff_api_status)
            print("Staff details response body:", staff_api_body)

        except Exception as e:
            staff_api_status = "failed"
            staff_api_body = str(e)
            print("Staff details API call failed:", str(e))

        # 4. Prepare record for staff_updated collection
        record = {
            "user_id": str(user_id).strip(),
            "uploaded_at": datetime.utcnow(),
            "country": app_country,
            "status": "1",
            "staff_api_status": str(staff_api_status),
            "staff_api_response": staff_api_body
        }

        # 5. Insert into staff_updated collection
        result = table_name.insert_one(record)

        return jsonify({
            "status": "success",
            "message": "Staff details recorded successfully",
            "record_id": str(result.inserted_id),
            "user_id": user_id,
            "staff_api_status": staff_api_status,
            "staff_api_response": staff_api_body,
            "timestamp": record["uploaded_at"].isoformat()
        }), 201

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Server error: {str(e)}"
        }), 500