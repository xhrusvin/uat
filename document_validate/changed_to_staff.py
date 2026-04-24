from flask import jsonify, request
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
import os
from datetime import datetime

from . import bp

load_dotenv()

# ==================== CONFIG ====================
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')

# Use the same API key and country as in validate_nmbi for consistency
USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_WEBHOOK_KEY')
APP_COUNTRY = os.getenv('XN_APP_COUNTRY', 'ie')

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]

# New collection for uploaded documents
table_name = db['changed_to_staff']

# Optional: reference to leads if you want to link back
leads_collection = db['users']


# ==================== ROUTE ====================
@bp.route("/changed-to-staff", methods=["POST"])
def changed_to_staff_webhook():
    """
    Webhook endpoint triggered when a user uploads a document.
    
    Expected Headers:
        Api-Key: sk-8f3a9c1b7d4e6f0a2b9c8d7e6f5a4c3b2d1e0f9a8b7c6d5e4f3a2b1c0d9e8f7
        X-App-Country: ie
    
    Expected JSON Body:
        {
            "user_id": "695541458810dcdf8b0d4c51"
        }
    
    This route saves the upload record to the "changed_to_staff" collection.
    """
    try:
        # 1. Validate Headers (optional but recommended for security)
        api_key = request.headers.get("Api-Key")
        app_country = request.headers.get("X-App-Country")

        if api_key != USER_EXTERNAL_API_KEY:
            return jsonify({
                "status": "error",
                "message": "Invalid or missing Api-Key"
            }), 401

        # if app_country and app_country.lower() != APP_COUNTRY.lower():
        #     return jsonify({
        #         "status": "error",
        #         "message": "Invalid X-App-Country"
        #     }), 400

        # 2. Get JSON payload
        data = request.get_json(silent=True) or {}

        user_id = data.get("user_id")

        if not user_id:
            return jsonify({
                "status": "error",
                "message": "Missing required fields: user_id"
            }), 400

        # Optional: Validate if user exists in leads collection
        # lead = leads_collection.find_one({"xn_user_id": user_id})
        # if not lead:
        #     return jsonify({
        #         "status": "warning",
        #         "message": "User not found in leads collection"
        #     }), 200

        # 3. Prepare record for uploaded_documents collection
        record = {
            "user_id": str(user_id).strip(),           # xn_user_id from external system
            "uploaded_at": datetime.utcnow(),
            "cuntry": app_country,
            "status": "1"
        }

        # 4. Insert into uploaded_documents (use insert_one for webhook style)
        result = table_name.insert_one(record)

        # 5. Return success response
        return jsonify({
            "status": "success",
            "message": "Changed to staff recorded successfully",
            "uploaded_id": str(result.inserted_id),
            "user_id": user_id,
            "timestamp": record["uploaded_at"].isoformat()
        }), 201

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Server error: {str(e)}"
        }), 500