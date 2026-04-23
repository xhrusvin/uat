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
import google.generativeai as genai
import json
import re

# Configure Gemini
# Using the stable 1.5 Flash model which is widely supported for generateContent with multimodal data
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
model = genai.GenerativeModel('gemini-2.5-flash')

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session or not session.get('is_admin'):
            return redirect(url_for('admin.admin_login'))
        return f(*args, **kwargs)
    return decorated

@admin_bp.route('/validate_document')
@admin_required
def validate_document():
    # 1. Get URL Parameters
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('limit', 10))
    search = request.args.get('search', '').strip()
    email_filter = request.args.get('email', '').strip()
    user_id_filter = request.args.get('user_id', '').strip()

    # 2. Build Query
    query = {"is_admin": {"$ne": True}}

    if email_filter:
        query["email"] = email_filter
    elif user_id_filter:
        query["_id"] = ObjectId(user_id_filter)
    else:
        query["document_fetched"] = {"$ne": 1}
        query["xn_user_id"] = {"$exists": True, "$ne": ""}

    if search:
        query["email"] = {"$regex": search, "$options": "i"}
        

    # 3. Fetch Users
    users_list = list(
        current_app.db.users.find(query)
        .sort("created_at", -1)
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
            response = requests.get(api_url, headers=headers, json={"_id": xn_user_id}, timeout=15)

            if response.status_code != 200:
                continue

            docs_array = response.json().get('data', [])

            # ── Find docs not yet AI-checked in documents_new ──────────────
            already_checked_ids = set(
                d['document_id']
                for d in current_app.db.documents_new.find(
                    {
                        "user_id": local_id,
                        "ai_attempted": True   # has been processed
                    },
                    {"document_id": 1}
                )
            )

            # Docs still pending AI check
            pending_docs = [
                doc for doc in docs_array
                if doc.get('document_id') not in already_checked_ids
            ]

            # ── HARD CAP: only process 2 docs per request ──────────────────
            docs_to_process = pending_docs[:2]
            ai_checked_count = 0

            for doc in docs_to_process:
                doc_url = doc.get('url')
                doc_name = doc.get('document_type_name', 'Unknown')
                prompt = ""
                ai_status = False
                ai_reason = "No URL"
                ai_raw_response = ""

                if doc_url:
                    try:
                        prompt_record = current_app.db.prompts.find_one({
                            "document_type_code": {"$regex": doc_name, "$options": "i"}
                        })

                        prompt = (
                            prompt_record['prompt_text']
                            if prompt_record and prompt_record.get('prompt_text')
                            else "Analise and find document name"
                        )

                        file_resp = requests.get(doc_url, timeout=10)
                        if file_resp.status_code == 200:
                            if ".pdf" in doc_url.lower():
                                mime_type = "application/pdf"
                            elif ".png" in doc_url.lower():
                                mime_type = "image/png"
                            else:
                                mime_type = "image/jpeg"

                            response_ai = model.generate_content([
                                prompt,
                                {'mime_type': mime_type, 'data': file_resp.content}
                            ])

                            ai_raw_response = response_ai.text
                            clean_text = ai_raw_response.replace('```json', '').replace('```', '').strip()
                            res_json = json.loads(clean_text)

                            ai_status = (
                                res_json.get('is_valid')
                                if 'is_valid' in res_json
                                else res_json.get('verified', False)
                            )
                            ai_reason = (
                                res_json.get('failed_reason')
                                if 'failed_reason' in res_json
                                else res_json.get('reason', 'Processed')
                            )
                        else:
                            ai_reason = f"Download failed ({file_resp.status_code})"

                    except Exception as e:
                        ai_reason = f"AI processing error: {str(e)}"
                        ai_raw_response = str(e)

                # ── Save doc result ────────────────────────────────────────
                url = doc.get('url')
                url_flag = 1 if url else 0
                doc.pop('url', None)
                doc.update({
                    "user_id": local_id,
                    "xn_user_id": xn_user_id,
                    "ai_status": ai_status,
                    "ai_reason": ai_reason,
                    "prompt": prompt,
                    "url_status": url_flag,
                    "ai_attempted": True,
                    "ai_raw_response": ai_raw_response,
                    "synced_at": datetime.now(pytz.UTC)
                })

                current_app.db.documents_new.update_one(
                    {"document_id": doc.get('document_id'), "user_id": local_id},
                    {"$set": doc},
                    upsert=True
                )
                ai_checked_count += 1

            # ── Check if ALL docs for this user are now complete ───────────
            # Total docs from API vs total saved in documents_new for this user
            total_docs = len(docs_array)
            total_saved = current_app.db.documents_new.count_documents({
                "user_id": local_id,
                "ai_attempted": True
            })

            if total_saved >= total_docs:
                # Every doc has been AI-checked across all requests — mark done
                current_app.db.users.update_one(
                    {"_id": local_id},
                    {"$set": {"document_fetched": 1}}
                )
                fully_done = True
            else:
                # Still has pending docs — will be picked up in next request
                fully_done = False

            processed_results.append({
                "email": u.get('email'),
                "total_docs": total_docs,
                "checked_this_request": ai_checked_count,
                "total_checked_so_far": total_saved,
                "user_fully_done": fully_done
            })

        except Exception as e:
            current_app.logger.error(f"Sync error for {u.get('email')}: {e}")

    return jsonify({
        "status": "Batch processed",
        "count": len(processed_results),
        "processed_users": processed_results
    })

@admin_bp.route('/get_user_documents/<user_id>')
@admin_required
def get_user_documents(user_id):
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