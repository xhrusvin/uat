from flask import jsonify, request
from pymongo import MongoClient
from dotenv import load_dotenv
import os
from datetime import datetime
from bson import ObjectId
from bson.errors import InvalidId

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
    Accepts user_id (which is actually xn_user_id) and returns
    the full user document details from the users collection.

    Expected Headers:
        Api-Key: <XN_PORTAL_WEBHOOK_KEY>
        X-App-Country: ie   (optional)

    Expected JSON Body:
        { "user_id": "67ef75c2c9ede4cc5506bc1b" }   # = xn_user_id
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

        # 3. Look up user – priority: xn_user_id (what the client sends)
        #    Also try _id / user_id as fallback
        or_conditions = [
            {"xn_user_id": user_id},
            {"user_id": user_id},
        ]

        # Only add ObjectId(_id) if the string is a valid ObjectId
        try:
            or_conditions.append({"_id": ObjectId(user_id)})
        except (InvalidId, TypeError):
            pass

        # Also allow plain string _id match (rare)
        or_conditions.append({"_id": user_id})

        user_doc = users_col.find_one({"$or": or_conditions})

        if not user_doc:
            return jsonify({
                "status": "error",
                "message": f"User not found for user_id: {user_id}"
            }), 404

        # 4. Build response with the important fields
        user_details = {
            "user_id":          str(user_doc.get("_id")),
            "xn_user_id":       user_doc.get("xn_user_id"),
            "first_name":       user_doc.get("first_name"),
            "last_name":        user_doc.get("last_name"),
            "email":            user_doc.get("email"),
            "phone":            user_doc.get("phone"),
            "designation":      user_doc.get("designation"),
            "job_title":        user_doc.get("job_title"),
            "company_name":     user_doc.get("company_name"),
            "address":          user_doc.get("address"),
            "dob":              user_doc.get("dob"),
            "gender_id":        user_doc.get("gender_id"),
            "country_id":       user_doc.get("country_id"),
            "county_id":        user_doc.get("county_id"),
            "experience_year":  user_doc.get("experience_year"),
            "experience_month": user_doc.get("experience_month"),
            "rating":           user_doc.get("rating"),
            "status":           user_doc.get("status"),
            "is_active":        user_doc.get("is_active"),
            "onboarded":        user_doc.get("onboarded"),
            "user_sub_type_ids": user_doc.get("user_sub_type_ids"),
            "tags":             user_doc.get("tags"),
            "location":         user_doc.get("location"),
            "created_at":       user_doc.get("created_at").isoformat() if user_doc.get("created_at") else None,
            "updated_at":       user_doc.get("updated_at").isoformat() if user_doc.get("updated_at") else None,
            # add / remove fields as needed
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