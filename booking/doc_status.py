# booking/doc_status.py
from flask import request, jsonify
from bson import ObjectId
from datetime import datetime
import requests
import os

from database import db
from . import bp


XN_PORTAL_BASE_URL = os.getenv("XN_PORTAL_BASE_URL", "").rstrip("/")
XN_PORTAL_API_KEY = os.getenv("XN_PORTAL_API_KEY", "")
XN_APP_COUNTRY = os.getenv("XN_APP_COUNTRY", "")


def resolve_level(doc):
    """
    If level exists and is truthy, return it.
    Otherwise look up prompts collection by document_type_name == prompts.document_type_code,
    save the found level back to documents_new, and return it.
    """
    level = doc.get("level")

    if level:
        return level

    # Look up from prompts collection
    prompt = db.prompts.find_one({
        "document_type_code": doc.get("document_type_name")
    })

    if prompt and prompt.get("level"):
        found_level = prompt["level"]

        # Save level back to documents_new
        db.documents_new.update_one(
            {"_id": doc["_id"]},
            {"$set": {"level": found_level}}
        )

        doc["level"] = found_level
        return found_level

    return None


def call_outreach_api(doc):
    """
    Call the external outreach API (GET) and save status + expiry_date back to documents_new.
    """
    params = {
        "_id": doc.get("xn_user_id"),
        "document_id": doc.get("document_id")
    }
    headers = {
        "Api-Key": XN_PORTAL_API_KEY,
        "X-App-Country": XN_APP_COUNTRY
    }

    resp = requests.get(
        f"{XN_PORTAL_BASE_URL}/ai/recruitments/user-document-list",
        params=params,
        headers=headers,
        timeout=30
    )

    result = {
        "document_id": doc.get("document_id"),
        "xn_user_id": doc.get("xn_user_id"),
        "api_status": resp.status_code,
        "api_response": resp.json() if resp.ok else resp.text
    }

    # Save status and expiry_date from response back to documents_new
    if resp.ok:
        resp_data = resp.json()
        documents = (
            resp_data.get("data", {})
            .get("documents", [])
        )

        # Find matching document from response by document_id
        for api_doc in documents:
            if api_doc.get("document_id") == doc.get("document_id"):
                update_fields = {}

                if "status" in api_doc:
                    update_fields["status"] = api_doc["status"]

                if "expiry_date" in api_doc:
                    update_fields["expiry_date"] = api_doc["expiry_date"]

                if update_fields:
                    db.documents_new.update_one(
                        {"_id": doc["_id"]},
                        {"$set": update_fields}
                    )
                break

    return result


@bp.route('/doc-statuses')
def doc_statuses():
    try:
        search = request.args.get('search', '').strip()

        query = {
            "url_status": 1,
            "document_id": {
                "$exists": True,
                "$nin": [None, ""]
            },
            "$or": [
                {"synced": 0},
                {"synced": {"$exists": False}}
            ]
        }

        if search:
            query["$or"] = [
                {"title": {"$regex": search, "$options": "i"}},
                {"url": {"$regex": search, "$options": "i"}},
                {"document_type_name": {"$regex": search, "$options": "i"}},
                {"xn_user_id": {"$regex": search, "$options": "i"}}
            ]

        docs = list(
            db.documents_new.find(query)
            .sort("synced_at", -1).limit(1)
        )

        # Mark fetched documents as synced
        if docs:
            doc_ids = [d["_id"] for d in docs]
            db.documents_new.update_many(
                {"_id": {"$in": doc_ids}},
                {"$set": {"synced": 1}}
            )

        # Resolve level and call outreach API only if level == 1
        api_responses = []
        for doc in docs:
            try:
                level = resolve_level(doc)

                if level == 1:
                    result = call_outreach_api(doc)
                    api_responses.append(result)
                else:
                    api_responses.append({
                        "document_id": doc.get("document_id"),
                        "xn_user_id": doc.get("xn_user_id"),
                        "skipped": True,
                        "reason": f"level is {level}, outreach only for level 1"
                    })

            except Exception as api_err:
                api_responses.append({
                    "document_id": doc.get("document_id"),
                    "xn_user_id": doc.get("xn_user_id"),
                    "api_status": None,
                    "api_error": str(api_err)
                })

        # Convert ObjectId and datetime for JSON serialization
        for d in docs:
            for key, val in d.items():
                if isinstance(val, ObjectId):
                    d[key] = str(val)
                elif isinstance(val, datetime):
                    d[key] = val.isoformat()

        return jsonify({
            "doc_statuses": docs,
            "api_responses": api_responses
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500