# admin/user_documents.py
from flask import (
    render_template, request, redirect, url_for, flash, jsonify, current_app
)
from functools import wraps
from bson import ObjectId
from datetime import datetime
import pytz
import requests
import os
from dotenv import load_dotenv
import google.generativeai as genai
import base64
import uuid
import tempfile
import subprocess
from pathlib import Path
from PyPDF2 import PdfReader
from io import BytesIO
import magic 
import re 

import json

import hashlib


load_dotenv()

from .views import admin_required

from . import admin_bp

ist_tz = pytz.timezone('Europe/London')
current_ist = datetime.now(ist_tz)
date_str = current_ist.strftime("%A, %B %d, %Y %I:%M %p IST")

GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
if not GEMINI_API_KEY:
    print("Warning: GEMINI_API_KEY missing in .env")

genai.configure(api_key=GEMINI_API_KEY)

# Supported MIME types for Gemini inline_data (images + pdf)
SUPPORTED_MIME_TYPES = {
    'application/pdf',
    'image/jpeg',
    'image/png',
    'image/webp',
    # Add 'image/heic', 'image/heif' if your model supports them
}

DOC_TYPE_MAP = {
    "INFECTION_PREVENTION_CONTROL_CERTIFICATE": "IPC",
    "SAFEGUARDING_ADULTS_AT_RISK": "SAFEGUARDING_ADULTS",
    "QQI_LEVEL_5_OR_EQUIVALENT": "QQI_LEVEL_5",
    "CPR/BLS": "CPR_BLS",
    "MANUAL_AND_PEOPLE_HANDLING_DOCUMENTS": "MANUAL_HANDLING",
    "GARDA_VETTING_DOCUMENT": "GARDA_VETTING",
    "EMPLOYMENT_CONTRACT_SIGNED": "EMPLOYMENT_CONTRACT",
    "THE_OPEN_DISCLOSURE": "OPEN_DISCLOSURE",
    "CPI/_MAPA/PMAV": "CPI_MAPA_PMAV",
    "Nmbi Qualification": "NBMI",
    "Covid Certificate": "COVID",
    "Medication Management": "MEDICATION",
    "Fire Safety": "FIRE_SAFETY",
    "IPC": "IPC",
    "Hand Hygiene": "HAND_HYGIENE",
    "Children First": "CHILDREN_FIRST",
    "Safeguarding Adults": "SAFEGUARDING_ADULTS",
    "CV": "CV",
    "CPR/BLS": "CPR_BLS",
    "Manual Handling": "MANUAL_HANDLING",
    "Garda Vetting": "GARDA_VETTING",
    "PPE": "PPE",
    "Employment Contract": "EMPLOYMENT_CONTRACT",
    "Open Disclosure": "OPEN_DISCLOSURE",
    "CPI/MAPA/PMAV": "CPI_MAPA_PMAV",
    "Cyber Security": "CYBER_SECURITY",
    "GDPR": "GDPR"
}


def safe_json_parse(text):
    if not text:
        return {"is_valid": False, "status": "invalid", "failed_reason": "Empty AI response"}
    
    # Strip markdown code blocks
    text = re.sub(r'```(?:json)?\s*', '', text)
    text = re.sub(r'```\s*$', '', text)
    text = text.strip()
    
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        current_app.logger.warning(f"JSON parse failed: {e} | Raw: {text[:500]}...")
        return {
            "is_valid": False,
            "status": "invalid",
            "failed_reason": f"AI returned malformed JSON: {str(e)[:100]}"
        }

def generate_file_hash(file_bytes):
    return hashlib.sha256(file_bytes).hexdigest()




@admin_bp.route('/user-documents')
@admin_required
def user_documents():
    # ── unchanged ─────────────────────────────────────────────────────────────
    page = int(request.args.get('page', 1))
    per_page = 12
    search = request.args.get('search', '').strip()

    query = {
        "is_admin": {"$ne": True},
        "status": {"$ne": "Enabled"}            
    }

    if search:
        regex = {"$regex": search, "$options": "i"}
        query["$or"] = [
            {"email": regex},
            {"phone": regex},
            {"first_name": regex},
            {"last_name": regex},
            {"country": regex},
            {"designation": regex},
        ]

    total = current_app.db.users.count_documents(query)

    users_cursor = (
        current_app.db.users.find(query)
        .sort("created_at", -1)
        .skip((page - 1) * per_page)
        .limit(per_page)
    )

    users_list = []
    for user in users_cursor:
        user_dict = dict(user)
        user_dict['id_str'] = str(user_dict['_id'])

        nmbi_doc = current_app.db.users_documents.find_one({
            "lead_id": str(user_dict['_id']),
            "document_type_name": "Nmbi Qualification"
        })

        has_valid_nmbi = False
        nmbi_url = None

        if nmbi_doc:
            url_value = nmbi_doc.get('url')
            if url_value and isinstance(url_value, str) and url_value.strip():
                has_valid_nmbi = True
                nmbi_url = url_value.strip()

        user_dict['has_nmbi_qualification'] = has_valid_nmbi
        user_dict['nmbi_url'] = nmbi_url

        user_dict['garda_vetted'] = user_dict.get('garda_vetted', False)
        vetted_at = user_dict.get('garda_vetted_at')
        if vetted_at and isinstance(vetted_at, datetime):
            user_dict['garda_vetted_at_formatted'] = vetted_at.astimezone(pytz.UTC).strftime('%d %b %Y at %H:%M')
        else:
            user_dict['garda_vetted_at_formatted'] = '—'

        name = f"{user_dict.get('first_name', '')} {user_dict.get('last_name', '')}".strip()
        user_dict['display_name'] = name or user_dict.get('email', 'Unknown')

        created = user_dict.get('created_at')
        if isinstance(created, datetime):
            user_dict['created_at_formatted'] = created.strftime('%d %b %Y')
        elif isinstance(created, str):
            try:
                dt = datetime.fromisoformat(created.replace('Z', '+00:00'))
                user_dict['created_at_formatted'] = dt.strftime('%d %b %Y')
            except:
                user_dict['created_at_formatted'] = '—'
        else:
            user_dict['created_at_formatted'] = '—'

        users_list.append(user_dict)

    return render_template(
        'admin/user_documents.html',
        users=users_list,
        page=page,
        total=total,
        per_page=per_page,
        search=search
    )


@admin_bp.route('/api/user-documents/list', methods=['GET'])
@admin_required
def api_list_user_documents():
    # unchanged
    lead_id = request.args.get('lead_id')

    if not lead_id or not ObjectId.is_valid(lead_id):
        return jsonify({"status": "error", "message": "Invalid or missing lead_id"}), 400

    user = current_app.db.users.find_one(
        {"_id": ObjectId(lead_id)},
        {"xn_user_id": 1, "email": 1, "first_name": 1, "last_name": 1}
    )

    if not user or "xn_user_id" not in user:
        return jsonify({"status": "error", "message": "User not found or missing xn_user_id"}), 404

    xn_user_id = str(user["xn_user_id"]).strip()
    user_name = (
        f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
        or user.get("email", "User")
    )

    USER_EXTERNAL_API_URL = os.getenv('XN_PORTAL_BASE_URL')
    USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_API_KEY')
    APP_COUNTRY           = os.getenv('XN_APP_COUNTRY', 'ie')

    if not USER_EXTERNAL_API_URL or not USER_EXTERNAL_API_KEY:
        current_app.logger.error("Missing XN_PORTAL_BASE_URL or XN_PORTAL_API_KEY")
        return jsonify({"status": "error", "message": "Server configuration error"}), 500

    api_url = f"{USER_EXTERNAL_API_URL.rstrip('/')}/ai/recruitments/user-document-list"

    headers = {
        "Api-Key": USER_EXTERNAL_API_KEY,
        "X-App-Country": APP_COUNTRY,
        "Content-Type": "application/json"
    }

    try:
        resp = requests.get(api_url, headers=headers, json={"_id": xn_user_id}, timeout=10)
        resp.raise_for_status()
        api_data = resp.json()

        if not api_data.get("success"):
            current_app.logger.warning(f"External API non-success: {api_data}")
            return jsonify({"status": "error", "message": "External API non-success"}), 502

        fresh_documents = api_data.get("data", []) or []

        # documents = []
        # for doc in fresh_documents:
        #     doc_name = (doc.get("document_type_name") or "").strip()
        #     if not doc_name:
        #         continue
        #     documents.append({
        #         "document_type_name": doc_name,
        #         "url": doc.get("url"),
        #         "fetched_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M")
        #     })

        # documents = []
        # for doc in fresh_documents:
        #     doc_name = (doc.get("document_type_name") or "").strip()
        #     doc_code = DOC_TYPE_MAP.get(doc_name, doc_name.upper().replace(" ", "_"))
        #     if not doc_name:
        #         continue

        #     doc_url = doc.get("url")

        #     # 🔹 Fetch latest validation
        #     validation = current_app.db.validations.find_one(
        #         {
        #             "user_id": ObjectId(lead_id),
        #             "document_type_code": doc_name,
        #             "document_type_code": doc_code
        #         },
        #         sort=[("validated_at", -1)]
        #     )

        #     verification_data = None
        #     if validation:
        #         verification_data = {
        #             "status": validation.get("status"),
        #             "result": validation.get("result"),
        #             "failed_reason": validation.get("failed_reason"),
        #             "validation_id": str(validation["_id"])
        #         }

        #     documents.append({
        #         "document_type_name": doc_name,
        #         "document_type_code": doc_code,
        #         "url": doc_url,
        #         "fetched_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
        #         "verification": verification_data
        #     })

        documents = []

        for doc in fresh_documents:
            doc_name = (doc.get("document_type_name") or "").strip()
            if not doc_name:
                continue

            doc_code = DOC_TYPE_MAP.get(doc_name, doc_name.upper().replace(" ", "_"))
            doc_url = doc.get("url")

            
            current_app.db.documents.update_one(
                {
                    "user_id": ObjectId(lead_id),
                    "document_type_code": doc_code
                },
                {
                    "$set": {
                        "status": "pending",
                        "updated_at": datetime.utcnow()
                    },
                    "$setOnInsert": {
                        "created_at": datetime.utcnow()
                    }
                },
                upsert=True
            )

            # 🔹 Existing validation fetch (keep this)
            validation = current_app.db.validations.find_one(
                {
                    "user_id": ObjectId(lead_id),
                    "document_type_code": doc_code
                },
                sort=[("validated_at", -1)]
            )

            verification_data = None
            if validation:
                verification_data = {
                    "status": validation.get("status"),
                    "result": validation.get("result"),
                    "failed_reason": validation.get("failed_reason"),
                    "validation_id": str(validation["_id"])
                }

            documents.append({
                "document_type_name": doc_name,
                "document_type_code": doc_code,
                "url": doc_url,
                "fetched_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
                "verification": verification_data
            })

        return jsonify({
            "status": "success",
            "lead_id": lead_id,
            "user_name": user_name,
            "designation": user.get("designation", "—"),
            "documents": documents,
            "count": len(documents)
        })

    except requests.RequestException as e:
        current_app.logger.error(f"External API request failed: {e}")
        return jsonify({"status": "error", "message": f"External service unreachable: {str(e)}"}), 502
    except ValueError as e:
        current_app.logger.error(f"Invalid JSON from external API: {e}")
        return jsonify({"status": "error", "message": "Invalid response format"}), 502
    except Exception as e:
        current_app.logger.exception("Unexpected error in list documents")
        return jsonify({"status": "error", "message": "Internal error"}), 500
    

@admin_bp.route('/api/prompts/types')
@admin_required
def get_prompt_types():
    types = list(current_app.db.document_types.find({}, {"code": 1}))
    return jsonify([t["code"] for t in types])


@admin_bp.route('/api/prompts/get', methods=['GET'])
@admin_required
def get_prompt():
    doc_type = request.args.get("document_type")

    prompt = current_app.db.prompts.find_one(
        {"document_type_code": doc_type, "is_active": True},
        sort=[("version", -1)]
    )

    if not prompt:
        return jsonify({"status": "error", "message": "Not found"}), 404

    return jsonify({
        "status": "success",
        "prompt": prompt["prompt_text"],
        "version": prompt["version"]
    })

@admin_bp.route('/api/prompts/update', methods=['POST'])
@admin_required
def update_prompt():
    data = request.get_json()

    doc_type = data.get("document_type")
    new_prompt = data.get("prompt")

    latest = current_app.db.prompts.find_one(
        {"document_type_code": doc_type},
        sort=[("version", -1)]
    )

    new_version = (latest["version"] + 1) if latest else 1

    current_app.db.prompts.insert_one({
        "document_type_code": doc_type,
        "version": new_version,
        "prompt_text": new_prompt,
        "is_active": True,
        "created_at": datetime.utcnow()
    })

    return jsonify({"status": "success"})


@admin_bp.route('/api/verify-document', methods=['POST'])
@admin_required
def api_verify_document():
    data = request.get_json()
    doc_url = data.get('url')
    document_type = data.get('document_type')

    # prompt_doc = current_app.db.prompts.find_one(
    #     {"document_type_code": document_type, "is_active": True},
    #     sort=[("version", -1)]
    # )

    # if not prompt_doc:
    #     return jsonify({"status": "error", "message": "Prompt not found"}), 404

    # user_prompt = prompt_doc["prompt_text"]

    # Get prompt from DB
    prompt_doc = current_app.db.prompts.find_one(
        {"document_type_code": document_type, "is_active": True},
        sort=[("version", -1)]
    )

    # 🔥 FALLBACK LOGIC
    if prompt_doc and prompt_doc.get("prompt_text"):
        user_prompt = prompt_doc["prompt_text"]
    else:
        user_prompt = data.get("prompt", "").strip()

        if not user_prompt:
            return jsonify({
                "status": "error",
                "message": "No prompt available (DB or input)"
            }), 400

    print("Using prompt:", user_prompt[:100])


    document_type = data.get('document_type', 'document')

    if not doc_url or not user_prompt:
        return jsonify({"status": "error", "message": "Missing URL or prompt"}), 400

#     full_prompt = f"""Current date and time in IST: {date_str}
# Answer any date-related questions using this exact current time — do NOT guess or use outdated knowledge.

# {user_prompt}"""

    full_prompt = f"""Current date and time: {date_str}

You are a strict document validation AI. 

CRITICAL RULES:
- Respond with **ONLY** a valid JSON object. No explanations, no markdown, no ```json blocks, no extra text.
- Do not add any words before or after the JSON.

Required output format (exactly):

{{
  "is_valid": true or false,
  "status": "valid" or "invalid",
  "failed_reason": "brief reason why invalid (empty string if valid)",
  "expiry_date": "YYYY-MM-DD" or null
}}

Now analyze the provided document according to these instructions:

{user_prompt}
"""

    model = genai.GenerativeModel('gemini-2.5-flash')

    try:
        # Download file
        resp = requests.get(doc_url, timeout=25, allow_redirects=True)
        resp.raise_for_status()
        file_bytes = resp.content

        file_hash = generate_file_hash(file_bytes)

        file_size = len(file_bytes)
        if file_size < 300:
            raise ValueError(f"Downloaded file too small ({file_size} bytes)")

        # Detect real MIME type using content sniffing (better than headers)
        mime = magic.Magic(mime=True)
        detected_mime = mime.from_buffer(file_bytes)

        current_app.logger.info(
            f"Downloaded {file_size} bytes | "
            f"Detected MIME: {detected_mime} | "
            f"Header Content-Type: {resp.headers.get('Content-Type')} | "
            f"Header Content-Encoding: {resp.headers.get('Content-Encoding')}"
        )

        if detected_mime not in SUPPORTED_MIME_TYPES:
            return jsonify({
                "status": "error",
                "message": f"Unsupported file format (detected: {detected_mime}). Only PDF and common images (JPEG/PNG/WebP) are supported."
            }), 415

        # Debug save
        debug_dir = Path("/tmp/gemini_debug")
        debug_dir.mkdir(exist_ok=True)
        debug_path = debug_dir / f"file_{uuid.uuid4()}_{detected_mime.replace('/', '_')}"
        with open(debug_path, "wb") as f:
            f.write(file_bytes)
        current_app.logger.info(f"Debug file saved: {debug_path}")

        # For PDF: optional repair + fallback text extraction
        extracted_text = ""
        if detected_mime == 'application/pdf':
            # qpdf repair (optional but recommended)
            repaired_bytes = file_bytes
            try:
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_in, \
                     tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_out:
                    tmp_in.write(file_bytes)
                    tmp_in.flush()
                    in_path, out_path = tmp_in.name, tmp_out.name

                subprocess.run(["qpdf", "--linearize", in_path, out_path], check=True, timeout=12)
                with open(out_path, "rb") as f:
                    repaired_bytes = f.read()
                if len(repaired_bytes) > 200:
                    file_bytes = repaired_bytes
                    current_app.logger.info("qpdf repair applied to PDF")
            except Exception as q_err:
                current_app.logger.warning(f"qpdf skipped/failed: {q_err}")

            # Try text extraction as fallback
            try:
                reader = PdfReader(BytesIO(file_bytes))
                for page in reader.pages:
                    page_text = page.extract_text() or ""
                    extracted_text += page_text + "\n\n"
                extracted_text = extracted_text.strip()
            except Exception as pdf_err:
                current_app.logger.warning(f"PDF text extraction failed: {pdf_err}")

        # Prepare content for Gemini
        file_b64 = base64.b64encode(file_bytes).decode('utf-8')

        content_parts = [full_prompt]

        if extracted_text and detected_mime == 'application/pdf':
            # Prefer text if available (more reliable when PDF is damaged)
            content_parts.append(f"\nExtracted document text:\n{extracted_text[:15000]}")
            current_app.logger.info("Using text fallback for damaged PDF")
        else:
            # Use binary (PDF or image)
            content_parts.append({
                "inline_data": {
                    "mime_type": detected_mime,
                    "data": file_b64
                }
            })

        response = model.generate_content(
    content_parts,
    generation_config=genai.GenerationConfig(
        temperature=0.1,           # Lower = more consistent
        max_output_tokens=4096,
        response_mime_type="application/json",   # ← THIS IS KEY
        response_schema={                        # Optional but powerful (Gemini 2.5+)
            "type": "object",
            "properties": {
                "is_valid": {"type": "boolean"},
                "status": {"type": "string", "enum": ["valid", "invalid"]},
                "failed_reason": {"type": "string"},
                "expiry_date": {"type": ["string", "null"]}
            },
            "required": ["is_valid", "status", "failed_reason"]
        }
    )
)

        # result = response.text.strip() or "[No content extracted]"
        result_text = response.text.strip()
        parsed_result = safe_json_parse(result_text)

        # return jsonify({
        #     "status": "success",
        #     "result": parsed_result,
        #     "mime_type_used": detected_mime,
        #     "source": "text fallback" if extracted_text else "direct file",
        #     "document_type": document_type
        # })


        
        user_id = data.get("user_id")  # send from frontend if possible

        validation_id = None

        try:
            validation_doc = {
                "user_id": ObjectId(user_id) if user_id and ObjectId.is_valid(user_id) else None,
                "document_type_code": document_type,
                "status": "success" if parsed_result.get("is_valid") else "failed",
                "result": parsed_result,
                "failed_reason": parsed_result.get("failed_reason"),
                "raw_response": parsed_result,
                "validated_at": datetime.utcnow()
            }

            inserted = current_app.db.validations.insert_one(validation_doc)
            validation_id = str(inserted.inserted_id)


            # 1. Update document
            current_app.db.documents.update_one(
                {
                    "user_id": ObjectId(user_id),
                    "document_type_code": document_type
                },
                {
                    "$set": {
                        "file_url": doc_url,
                        "status": "completed" if parsed_result.get("is_valid") else "failed",
                        "last_validation_id": validation_id,
                        "updated_at": datetime.utcnow()
                    }
                }
            )

            # 2. Update user_document_status
            status_value = "valid" if parsed_result.get("is_valid") else "invalid"

            if parsed_result.get("expired") == "expired":
                status_value = "expired"

            current_app.db.user_document_status.update_one(
                {
                    "user_id": ObjectId(user_id),
                    "document_type_code": document_type
                },
                {
                    "$set": {
                        "status": status_value,
                        "latest_validation_id": validation_id,
                        "source": "ai",
                        "last_updated": datetime.utcnow()
                    }
                },
                upsert=True
            )

        except Exception as db_err:
            current_app.logger.warning(f"Validation save failed: {db_err}")

        return jsonify({
            "status": "success",
            "result": parsed_result,
            "validation_id": validation_id,
            "mime_type_used": detected_mime,
            "source": "text fallback" if extracted_text else "direct file",
            "document_type": document_type
        })

    except requests.RequestException as e:
        current_app.logger.error(f"Download failed: {e}")
        return jsonify({"status": "error", "message": f"Failed to download file: {str(e)}"}), 502

    except Exception as e:
        current_app.logger.exception(f"Verification failed for {doc_url}")
        return jsonify({
            "status": "error",
            "message": f"Processing failed: {str(e)}. Check logs and /tmp/gemini_debug files."
        }), 500
    


@admin_bp.route('/api/user-documents/accepted', methods=['GET'])
@admin_required
def api_user_documents_accepted():
    lead_id = request.args.get('lead_id')

    if not lead_id or not ObjectId.is_valid(lead_id):
        return jsonify({"status": "error", "message": "Invalid lead_id"}), 400

    # 🔹 Get xn_user_id (required for API)
    user = current_app.db.users.find_one(
        {"_id": ObjectId(lead_id)},
        {"xn_user_id": 1}
    )

    if not user or "xn_user_id" not in user:
        return jsonify({"status": "error", "message": "User not found"}), 404

    xn_user_id = str(user["xn_user_id"]).strip()

    # 🔹 External API config
    USER_EXTERNAL_API_URL = os.getenv('XN_PORTAL_BASE_URL')
    USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_API_KEY')
    APP_COUNTRY = os.getenv('XN_APP_COUNTRY', 'ie')

    api_url = f"{USER_EXTERNAL_API_URL.rstrip('/')}/ai/recruitments/user-document-list"

    headers = {
        "Api-Key": USER_EXTERNAL_API_KEY,
        "X-App-Country": APP_COUNTRY,
        "Content-Type": "application/json"
    }

    try:
        
        resp = requests.get(api_url, headers=headers, json={"_id": xn_user_id}, timeout=10)
        resp.raise_for_status()

        fresh_documents = resp.json().get("data", []) or []

       
        url_map = {}
        for doc in fresh_documents:
            doc_name = (doc.get("document_type_name") or "").strip()
            if not doc_name:
                continue

            doc_code = DOC_TYPE_MAP.get(doc_name, doc_name.upper().replace(" ", "_"))
            url_map[doc_code] = doc.get("url")

        
        docs = list(current_app.db.documents.find(
            {"user_id": ObjectId(lead_id)},
            {
                "document_type_code": 1,
                "status": 1,
                "updated_at": 1
            }
        ).sort("updated_at", -1))

        result = []

        for d in docs:
            updated = d.get("updated_at")
            if isinstance(updated, datetime):
                updated = updated.strftime("%d %b %Y %H:%M")

            code = d.get("document_type_code")

            result.append({
                "document_type": code,
                "status": d.get("status", "pending"),
                "updated_at": updated or "—",
                "url": url_map.get(code), 
                "reason": d.get("reason", "")
            })

        return jsonify({
            "status": "success",
            "documents": result
        })

    except Exception as e:
        current_app.logger.exception("Error fetching accepted documents")
        return jsonify({"status": "error", "message": str(e)}), 500