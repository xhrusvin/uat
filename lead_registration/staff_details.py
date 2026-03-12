# staff_details.py

import os
import re
from flask import jsonify, request
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId, json_util
from database import db, references_collection
import requests

from . import bp

# ────────────────────────────────────────────────
#               Same DB connection as original
# ────────────────────────────────────────────────

load_dotenv()

XN_PORTAL_BASE_URL = os.getenv('XN_PORTAL_BASE_URL')


def prepare_staff_data(doc):
    if not doc:
        return None

    # Most reliable way: dump → load once
    serialized = json_util.dumps(doc)
    data = json_util.loads(serialized)

    # Explicitly handle _id if it somehow survived
    result = {
        "id": str(doc["_id"]) if "_id" in doc else None,   # fallback raw → str
        "xn_user_id":                 data.get("xn_user_id"),
        "name":                       data.get("name"),
        "first_name":                 data.get("first_name"),
        "last_name":                  data.get("last_name"),
        "email":                      data.get("email"),
        "phone":                      data.get("phone"),
        "designation":                data.get("designation"),
        "country":                    data.get("country"),
    }

    return result


@bp.route("/staff", methods=["POST"])
def get_staff_details():
    """
    Get staff/user details by one of:
      • MongoDB _id        (24 hex characters)
      • xn_user_id
      • email
      • phone number

    You can help disambiguate with ?by= param:
      ?by=email
      ?by=phone

    Examples:
      GET /staff/698c23ca185af776fcea52a2
      GET /staff/698c23a59d47d89c7a0d1078
      GET /staff/nelsonbuenafe7788@gmail.com?by=email
      GET /staff/+353831430494
      GET /staff/0831430494?by=phone
      GET /staff/353831430494?by=phone
    """
    data = request.json
    phone_number = data.get("phone_number")
    identifier = phone_number

    try:
        user_doc = None
        by = request.args.get("by", "").lower()

        # 1. By MongoDB _id
        if identifier and ObjectId.is_valid(identifier):
            user_doc = references_collection.find_one({"_id": ObjectId(identifier)})

        # 2. By xn_user_id
        if not user_doc:
            user_doc = references_collection.find_one({"xn_user_id": identifier})

        # 3. By email
        if not user_doc and (by == "email" or "@" in identifier):
            user_doc = references_collection.find_one({
                "email": re.compile(f"^{re.escape(identifier)}$", re.IGNORECASE)
            })

        # 4. By phone (with flexible matching)
        if not user_doc and (by == "phone" or re.match(r'^[\+\d\s\-\(\)]{7,18}$', identifier)):
            clean = re.sub(r'[^0-9+]', '', identifier)

            candidates = [
                clean,
                f"+{clean.lstrip('+')}",
                clean.lstrip('+'),
            ]

            # Extra Irish number variations (very common in your data)
            if clean.startswith('353'):
                base = clean[3:]
                candidates.extend([
                    base,
                    f"+{base}",
                    f"+353{base}",
                    f"353{base}",
                ])

            user_doc = references_collection.find_one({
                "phone": {"$in": candidates}
            })

        if not user_doc:
            return jsonify({
                "status": "success",
                "message": "No staff record found for this identifier"
            }), 200

        result = prepare_staff_data(user_doc)


        if user_doc.get("xn_user_id"):
            details_url = f"{XN_PORTAL_BASE_URL}ai/recruitments/detail"
            details_payload = {
                "_id": str(user_doc.get("xn_user_id"))
            }

            details_headers = {
                "Api-Key": os.getenv("XN_PORTAL_API_KEY"),         # Set this in env
                "X-App-Country": os.getenv("XN_APP_COUNTRY"),       # Set this in env
                "Content-Type": "application/json"
            }

            details_resp = requests.get(details_url, json=details_payload, headers=details_headers, timeout=25)

            details_resp.raise_for_status()

            

        return jsonify({
            "status": "success",
            "data": details_resp.json()
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Server error: {str(e)}"
        }), 500


# ────────────────────────────────────────────────
#  Optional: simple list / filter endpoint
# ────────────────────────────────────────────────

@bp.route("/staff-list", methods=["GET"])
def list_staff():
    """
    List staff with basic filtering & pagination

    Query params examples:
      ?page=1&limit=20
      ?designation=Healthcare Assistant
      ?needs_reference=true
      ?country=Ireland
    """
    try:
        page  = max(1, int(request.args.get("page", 1)))
        limit = max(1, min(100, int(request.args.get("limit", 20))))
        skip  = (page - 1) * limit

        query = {}

        if val := request.args.get("designation"):
            query["designation"] = val.strip()
        if val := request.args.get("country"):
            query["country"] = val.strip()
        if request.args.get("needs_reference") == "true":
            query["$or"] = [
                {"reference_email_sent": {"$ne": 1}},
                {"reference_email_sent": {"$exists": False}}
            ]

        total = references_collection.count_documents(query)

        cursor = (
            references_collection
            .find(query)
            .sort("created_at", -1)
            .skip(skip)
            .limit(limit)
        )

        items = [prepare_staff_data(doc) for doc in cursor]

        return jsonify({
            "status": "success",
            "data": items,
            "pagination": {
                "page": page,
                "limit": limit,
                "total_records": total,
                "total_pages": (total + limit - 1) // limit if limit else 1
            }
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500