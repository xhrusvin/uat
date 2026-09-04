from flask import (
    redirect, url_for, flash, current_app, jsonify,
    request, session, render_template, Response
)
from functools import wraps
from . import admin_bp
from datetime import datetime
import pytz
from bson import ObjectId
import os
import requests
import json
import re
from bson.json_util import dumps

ALLOWED_IPS = ["34.52.131.152", "103.146.175.179", "10.0.0.5"]

def get_remote_ip():
    """
    Extracts the real client IP, accounting for proxies/load balancers
    that set X-Forwarded-For or X-Real-IP headers.
    """
    if request.headers.get('X-Forwarded-For'):
        return request.headers.get('X-Forwarded-For').split(',')[0].strip()
    elif request.headers.get('X-Real-IP'):
        return request.headers.get('X-Real-IP').strip()
    return request.remote_addr

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session or not session.get('is_admin'):
            return redirect(url_for('admin.admin_login'))
        return f(*args, **kwargs)
    return decorated

@admin_bp.route('/validate_document_noai')
def validate_document_noai():
    client_ip = get_remote_ip()

    # if client_ip not in ALLOWED_IPS:
    #     return jsonify({
    #         "status": "error",
    #         "message": f"Access denied: IP {client_ip} is not whitelisted"
    #     }), 403

    # 1. Get URL Parameters
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('limit', 10))
    search = request.args.get('search', '').strip()
    email_filter = request.args.get('email', '').strip()
    user_id_filter = request.args.get('user_id', '').strip()
    xn_user_id_filter = request.args.get('xn_user_id', '').strip()
    document_id_filter = request.args.get('document_id', '').strip()

    # 2. Build Query
    query = {"is_admin": {"$ne": True}, "source": "staff"}

    if email_filter:
        query["email"] = email_filter
    elif user_id_filter:
        query["_id"] = ObjectId(user_id_filter)
    elif xn_user_id_filter:
        query["xn_user_id"] = xn_user_id_filter
    else:
        query["$or"] = [
            {"document_fetched": 0},
            {"document_fetched": {"$exists": False}}
        ]
        query["xn_user_id"] = {"$exists": True, "$ne": ""}

    if search:
        query["email"] = {"$regex": search, "$options": "i"}

    # 3. Fetch Users
    users_list = list(
        current_app.db.users.find(query)
        .sort("created_at", 1)
        .skip((page - 1) * per_page)
        .limit(per_page)
    )

    BASE_URL = os.getenv('XN_PORTAL_BASE_URL')
    headers = {
        "Api-Key": os.getenv('XN_PORTAL_API_KEY'),
        "X-App-Country": os.getenv('XN_APP_COUNTRY'),
        "Content-Type": "application/json"
    }

    processed_results = []

    for u in users_list:
        local_id = u['_id']
        xn_user_id = u.get('xn_user_id')

        try:
            api_url = f"{BASE_URL}/ai/recruitments/user-document-list"
            payload = {"staff_id": xn_user_id}
            if document_id_filter:
                payload["document_id"] = document_id_filter

            response = requests.get(api_url, headers=headers, json=payload, timeout=15)

            if response.status_code != 200:
                continue

            resp_data = response.json().get('data', {}) or {}
            if isinstance(resp_data, dict):
                docs_array = resp_data.get('documents', []) or []
                api_user_name = (resp_data.get('name') or '').strip()
            else:
                docs_array = resp_data
                api_user_name = ''

            # ── Filter docs ────────────────────────────────────────────────
            if document_id_filter:
                docs_to_process = [
                    doc for doc in docs_array
                    if str(doc.get('document_id')) == document_id_filter
                ]
            else:
                already_checked_ids = set(
                    d['document_id']
                    for d in current_app.db.documents_new.find(
                        {
                            "user_id": local_id,
                            "ai_attempted": True
                        },
                        {"document_id": 1}
                    )
                    if d.get('document_id') is not None
                )
                pending_docs = [
                    doc for doc in docs_array
                    if doc.get('document_id') not in already_checked_ids
                ]
                # HARD CAP: only process 2 docs per request
                docs_to_process = pending_docs[:200000000]

            checked_count = 0

            for doc in docs_to_process:
                doc_url = doc.get('url')
                doc_name = doc.get('document_type_name', 'Unknown')
                url_flag = 1 if doc_url else 0

                # ── Get level from prompts collection ──────────────────────
                level = None
                try:
                    prompt_record = current_app.db.prompts.find_one({
                        "document_type_code": {"$regex": re.escape(doc_name), "$options": "i"}
                    })
                    if prompt_record:
                        level = prompt_record.get('level')
                except Exception:
                    level = None

                # Keep original fields from API + our own fields
                doc_to_save = {
                    "document_id": doc.get('document_id'),
                    "document_category_type": doc.get('document_category_type'),
                    "document_type_name": doc.get('document_type_name'),
                    "sub_type_id": doc.get('sub_type_id'),
                    "sub_type_name": doc.get('sub_type_name'),
                    "expiry_date": doc.get('expiry_date'),
                    "status": doc.get('status'),
                    "updated_at": doc.get('updated_at'),
                    "user_id": local_id,
                    "xn_user_id": xn_user_id,
                    "url_status": url_flag,
                    "ai_status": None,
                    "ai_reason": "AI checking disabled",
                    "level": level,                     # ← level now saved
                    "ai_attempted": True,
                    "ai_raw_response": "",
                    "synced_at": datetime.now(pytz.UTC)
                }

                # Unique key for upsert
                filter_query = {"user_id": local_id}
                if doc.get('document_id') is not None:
                    filter_query["document_id"] = doc.get('document_id')
                else:
                    # Fallback for docs that have null document_id
                    filter_query["document_type_name"] = doc.get('document_type_name')

                current_app.db.documents_new.update_one(
                    filter_query,
                    {"$set": doc_to_save},
                    upsert=True
                )
                checked_count += 1

            # ── Mark user as fully done if all docs processed ──────────────
            total_docs = len(docs_array)
            total_saved = current_app.db.documents_new.count_documents({
                "user_id": local_id,
                "ai_attempted": True
            })

            if total_saved >= total_docs:
                current_app.db.users.update_one(
                    {"_id": local_id},
                    {"$set": {"document_fetched": 1}}
                )
                fully_done = True
            else:
                fully_done = False

            processed_results.append({
                "email": u.get('email'),
                "total_docs": total_docs,
                "checked_this_request": checked_count,
                "total_checked_so_far": total_saved,
                "user_fully_done": fully_done
            })

        except Exception as e:
            current_app.logger.error(f"Sync error for {u.get('email')}: {e}")
            return jsonify({"status": "error", "message": str(e)})

    return jsonify({
        "status": "Batch processed",
        "count": len(processed_results),
        "processed_users": processed_results
    })


@admin_bp.route('/get_user_documents_noai/<user_id>')
@admin_required
def get_user_documents_noai(user_id):
    try:
        target_id = ObjectId(user_id)
        user_docs = list(current_app.db.documents_new.find({"user_id": target_id}))

        for doc in user_docs:
            doc['_id'] = str(doc['_id'])
            doc['user_id'] = str(doc['user_id'])
            if 'synced_at' in doc and isinstance(doc['synced_at'], datetime):
                doc['synced_at'] = doc['synced_at'].isoformat()

        return jsonify({"success": True, "data": user_docs})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500