from flask import jsonify, request
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
import os
import requests

from datetime import datetime

from . import bp

load_dotenv()

# ==================== CONFIG ====================
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')
USER_EXTERNAL_API_URL = os.getenv('XN_PORTAL_BASE_URL')          # e.g. https://api.example.com
USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_API_KEY')
APP_COUNTRY = os.getenv('XN_APP_COUNTRY', 'ie')                       # fallback to Ireland

if not all([MONGO_URI, DB_NAME, USER_EXTERNAL_API_URL, USER_EXTERNAL_API_KEY]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME, USER_EXTERNAL_API_URL, USER_EXTERNAL_API_KEY)")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
leads_collection     = db['users']               # rename for clarity
user_documents_coll  = db['users_documents']

# ==================== ROUTE ====================
@bp.route("/validate-nmbi", methods=["GET"])
def validate_nmbi():
    """
    Logic:
    1. If ?id=... → try to process exactly that lead
    2. Otherwise → take the oldest eligible lead that has NOT had documents fetched yet
    3. Call external /ai/recruitments/user-document-list API
    4. Save document name + url (if present) to user_documents collection
    5. Set documents_fetched = true on the lead
    6. Return lead info + list of documents
    """
    lead_id_param = request.args.get("id")

    try:
        # Required fields that must exist and not be empty
        required_fields_query = {
            "email":       {"$exists": True, "$nin": [None, ""]},
            "name":        {"$exists": True, "$nin": [None, ""]},
            "phone":       {"$exists": True, "$nin": [None, ""]},
            "xn_user_id":  {"$exists": True, "$nin": [None, ""]}
        }

        lead = None

        # ── 1. Specific lead requested ───────────────────────────────
        if lead_id_param:
            if not ObjectId.is_valid(lead_id_param):
                return jsonify({"status": "error", "message": "Invalid lead ID format"}), 400

            lead = leads_collection.find_one({
                "_id": ObjectId(lead_id_param),
                **required_fields_query,
                # Only allow if documents not already fetched (unless forced via ?id)
                # You may remove this line if ?id should bypass the check
                "documents_fetched": {"$ne": True}
            })

            if not lead:
                return jsonify({
                    "status": "no_lead",
                    "message": "Lead not found, missing fields, or documents already fetched"
                }), 200

        # ── 2. Auto-select oldest eligible & not-yet-fetched lead ─────
        else:
            query = {
                **required_fields_query,
                "documents_fetched": {"$ne": True},  # key filter — only not-yet-processed
                # You can keep your original nmbi_verified logic if still needed:
                # "$or": [
                #     {"nmbi_verified": {"$ne": 0}},
                #     {"nmbi_verified": {"$exists": False}}
                # ]
            }

            lead = leads_collection.find_one(
                query,
                sort=[("created_at", 1)]   # oldest first (use 1 for ascending)
            )

            if not lead:
                return jsonify({
                    "status": "no_lead",
                    "message": "No eligible lead found (missing fields or already fetched)"
                }), 200

        # ── We have a lead → extract needed fields ────────────────────
        lead_id     = str(lead["_id"])
        xn_user_id  = str(lead["xn_user_id"]).strip()
        email       = str(lead.get("email", "")).strip().lower()
        designation = str(lead.get("designation", "")).strip()

        # ── Call external API ─────────────────────────────────────────
        api_url = f"{USER_EXTERNAL_API_URL.rstrip('/')}/ai/recruitments/user-document-list"
        headers = {
            "Api-Key": USER_EXTERNAL_API_KEY,
            "X-App-Country": APP_COUNTRY,
            "Content-Type": "application/json"
        }

        payload = {
            "_id": xn_user_id
        }

        resp = requests.get(api_url, headers=headers, json=payload, timeout=12)

        if not resp.ok:
            return jsonify({
                "status": "error",
                "message": f"External API failed: {resp.status_code} - {resp.text[:180]}"
            }), 502


        api_data = resp.json()

        if not api_data.get("success"):
            return jsonify({
                "status": "error",
                "message": "External API returned non-success response"
            }), 502

        documents = api_data.get("data", [])

        # ── Save documents to user_documents collection ──────────────
        saved_docs = []
        for doc in documents:
            doc_name = doc.get("document_type_name", "").strip()
            doc_url  = doc.get("url", None)

            if not doc_name:
                continue  # skip nameless entries

            record = {
                "lead_id": lead_id,
                "xn_user_id": xn_user_id,
                "document_type_name": doc_name,
                "url": doc_url,                    # null / None if missing
                "fetched_at": datetime.utcnow(),
                # optional: store more fields if useful later
                # "document_id": doc.get("document_id"),
                # "status": doc.get("status"),
            }

            # Upsert — prevent duplicates if re-run
            user_documents_coll.update_one(
                {
                    "lead_id": lead_id,
                    "document_type_name": doc_name
                },
                {"$set": record},
                upsert=True
            )

            saved_docs.append({
                "document_type_name": doc_name,
                "url": doc_url
            })

        # ── Mark lead as fetched ──────────────────────────────────────
        leads_collection.update_one(
            {"_id": ObjectId(lead_id)},
            {"$set": {
                "documents_fetched": True,
                "documents_fetched_at": datetime.utcnow()
            }}
        )

        # ── Final response ────────────────────────────────────────────
        all_urls_null = all(d["url"] is None for d in saved_docs)

        return jsonify({
            "status": "success",
            "lead_id": lead_id,
            "xn_user_id": xn_user_id,
            "email": email,
            "designation": designation,
            "documents_fetched": True,
            "all_urls_null": all_urls_null,
            "document_list": saved_docs,
            "api_data": api_data
            # ← list of {name, url}
        }), 200

    except requests.RequestException as e:
        return jsonify({"status": "error", "message": f"External API connection error: {str(e)}"}), 502
    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500