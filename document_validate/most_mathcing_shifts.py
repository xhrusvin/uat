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

DEFAULT_PAGE          = 1
DEFAULT_PAGE_SIZE     = 10
MAX_PAGE_SIZE         = 100

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db           = mongo_client[DB_NAME]
users_col    = db['users']
shifts_col   = db['shifts']


# ==================== HELPERS ====================
def _serialize_shift(doc):
    """Convert a shift document to a JSON-safe dict."""
    def _dt(val):
        if isinstance(val, datetime):
            return val.isoformat()
        return val

    return {
        "shift_id":            str(doc.get("_id")),
        "name":                doc.get("name"),
        "shift_xn_id":        doc.get("shift_xn_id"),
        "shift_code":         doc.get("shift_code"),
        "description":        doc.get("description"),
        "date":               _dt(doc.get("date")),
        "start_time":         doc.get("start_time"),
        "end_time":           doc.get("end_time"),
        "shift_timing":       doc.get("shift_timing"),
        "status":             doc.get("status"),
        "rate":               doc.get("rate"),
        "pay_rate":           doc.get("pay_rate"),
        "is_active":          doc.get("is_active"),
        "is_premium":         doc.get("is_premium"),
        "user_type":          doc.get("user_type"),
        "user_type_id":       str(doc.get("user_type_id")) if doc.get("user_type_id") else None,
        "unit":               doc.get("unit"),
        "location":           doc.get("location"),
        "postal_code":        doc.get("postal_code"),
        "client_id":          doc.get("client_id"),
        "client_type":        doc.get("client_type"),
        "client_name":        doc.get("client_name"),
        "client_county":      doc.get("client_county"),
        "booking_type":       doc.get("booking_type"),
        "radius":             doc.get("radius"),
        "slots":              doc.get("slots", []),
        "shift_preferences":  doc.get("shift_preferences", []),
        "created_at":         _dt(doc.get("created_at")),
        "updated_at":         _dt(doc.get("updated_at")),
    }


# ==================== ROUTE ====================
@bp.route("/most-matching-shifts", methods=["POST"])
def most_matching_shifts():
    """
    Accepts user_id (which is actually xn_user_id) and returns
    the full user document details plus matching "To Be Filled"
    shifts whose user_type_id matches the user's user_type_id.

    Expected Headers:
        Api-Key: <XN_PORTAL_WEBHOOK_KEY>
        X-App-Country: ie   (optional)

    Expected JSON Body:
        {
            "user_id": "67ef75c2c9ede4cc5506bc1b",
            "page": 1,          # optional, default 1
            "page_size": 10     # optional, default 10, max 100
        }
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

        # Pagination params
        page      = max(int(data.get("page", DEFAULT_PAGE)), 1)
        page_size = min(int(data.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE)
        skip      = (page - 1) * page_size

        # 3. Look up user
        or_conditions = [
            {"xn_user_id": user_id},
            {"user_id": user_id},
        ]
        try:
            or_conditions.append({"_id": ObjectId(user_id)})
        except (InvalidId, TypeError):
            pass
        or_conditions.append({"_id": user_id})

        user_doc = users_col.find_one({"$or": or_conditions})

        if not user_doc:
            return jsonify({
                "status": "error",
                "message": f"User not found for user_id: {user_id}"
            }), 404

                # 4. Build user response
        user_details = {
            "user_id":           str(user_doc.get("_id")),
            "xn_user_id":        user_doc.get("xn_user_id"),
            "first_name":        user_doc.get("first_name"),
            "last_name":         user_doc.get("last_name"),
            "email":             user_doc.get("email"),
            "phone":             user_doc.get("phone"),
            "designation":       user_doc.get("designation"),
            "job_title":         user_doc.get("job_title"),
            "rating":            user_doc.get("rating"),
            "status":            user_doc.get("status"),
            "is_active":         user_doc.get("is_active"),
            "user_type_id":      str(user_doc.get("user_type_id")) if user_doc.get("user_type_id") else None,
            "user_sub_type_ids": user_doc.get("user_sub_type_ids"),
            "tags":              user_doc.get("tags"),
        }

        # 5. Fetch matching shifts
        user_type_id = user_doc.get("user_type_id")

        if user_type_id:
            # Ensure we match both ObjectId and string forms
            type_id_variants = [user_type_id]
            if isinstance(user_type_id, ObjectId):
                type_id_variants.append(str(user_type_id))
            else:
                try:
                    type_id_variants.append(ObjectId(user_type_id))
                except (InvalidId, TypeError):
                    pass

            shift_filter = {
                "user_type_id": {"$in": type_id_variants},
                "status":       "To Be Filled",
            }

            total_shifts = shifts_col.count_documents(shift_filter)
            shift_cursor = (
                shifts_col.find(shift_filter)
                .sort("date", 1)          # soonest shifts first
                .skip(skip)
                .limit(page_size)
            )
            shifts = [_serialize_shift(s) for s in shift_cursor]
        else:
            total_shifts = 0
            shifts       = []

        import math
        total_pages = math.ceil(total_shifts / page_size) if page_size else 0

        return jsonify({
            "status":    "success",
            "message":   "User details and matching shifts retrieved successfully",
            "user":      user_details,
            "shifts":    shifts,
            "pagination": {
                "page":        page,
                "page_size":   page_size,
                "total_items": total_shifts,
                "total_pages": total_pages,
            },
            "timestamp": datetime.utcnow().isoformat(),
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Server error: {str(e)}"
        }), 500