from flask import jsonify, request
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime
from bson import ObjectId

from . import bp

load_dotenv()

# ==================== CONFIG ====================
MONGO_URI             = os.getenv('MONGO_URI')
DB_NAME               = os.getenv('DB_NAME')
USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_WEBHOOK_KEY')
APP_COUNTRY           = os.getenv('XN_APP_COUNTRY', 'ie')

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db           = mongo_client[DB_NAME]
users_col    = db['users']


# ==================== ROUTE ====================
@bp.route("/most-matching-shifts", methods=["POST"])
def most_matching_shifts():
    """
    Accepts user_id and returns user details from the users collection
    (looked up via xn_user_id / _id / user_id).

    Expected Headers:
        Api-Key: <XN_PORTAL_WEBHOOK_KEY>
        X-App-Country: ie   (optional)

    Expected JSON Body:
        { "user_id": "64f1a2b3c4d5e6f7a8b9c0d1" }
    """
    try:
        # 1. Validate Headers
        api_key     = request.headers.get("Api-Key")
        app_country = request.headers.get("X-App-Country", APP_COUNTRY)

        if api_key != USER_EXTERNAL_API_KEY:
            return jsonify({
                "status": "error",
                "message": "Invalid or missing Api-Key"
            }), 401

        # 2. Get JSON payload
        data    = request.get_json(silent=True) or {}
        user_id = data.get("user_id")

        if not user_id:
            return jsonify({
                "status": "error",
                "message": "Missing required field: user_id"
            }), 400

        user_id = str(user_id).strip()

        # 3. Look up user in DB
        query = None
        try:
            query = {"_id": ObjectId(user_id)}
        except Exception:
            query = {
                "$or": [
                    {"_id": user_id},
                    {"user_id": user_id},
                    {"xn_user_id": user_id},
                ]
            }

        user_doc = users_col.find_one(query)

        if not user_doc:
            return jsonify({
                "status": "error",
                "message": f"User not found for user_id: {user_id}"
            }), 404

        xn_user_id = user_doc.get("xn_user_id")
        if not xn_user_id:
            return jsonify({
                "status": "error",
                "message": "User found but xn_user_id is missing"
            }), 400

        # 4. Prepare user details
        user_details = {
            "user_id":    str(user_doc.get("_id", user_id)),
            "xn_user_id": xn_user_id,
            "name":       user_doc.get("name") or user_doc.get("full_name"),
            "email":      user_doc.get("email"),
            "phone":      user_doc.get("phone") or user_doc.get("mobile"),
            "country":    user_doc.get("country") or app_country,
            # add any extra fields you need from the document
        }

        return jsonify({
            "status":    "success",
            "message":   "User details retrieved successfully",
            "user":      user_details,
            "timestamp": datetime.utcnow().isoformat(),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Server error: {str(e)}"
        }), 500