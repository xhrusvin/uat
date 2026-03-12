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
import magic  # for real MIME detection

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

        documents = []
        for doc in fresh_documents:
            doc_name = (doc.get("document_type_name") or "").strip()
            if not doc_name:
                continue
            documents.append({
                "document_type_name": doc_name,
                "url": doc.get("url"),
                "fetched_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            })

        return jsonify({
            "status": "success",
            "lead_id": lead_id,
            "user_name": user_name,
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


@admin_bp.route('/api/verify-document', methods=['POST'])
@admin_required
def api_verify_document():
    data = request.get_json()
    doc_url = data.get('url')
    user_prompt = data.get('prompt', '').strip()
    document_type = data.get('document_type', 'document')

    if not doc_url or not user_prompt:
        return jsonify({"status": "error", "message": "Missing URL or prompt"}), 400

    full_prompt = f"""Current date and time in IST: {date_str}
Answer any date-related questions using this exact current time — do NOT guess or use outdated knowledge.

{user_prompt}"""

    model = genai.GenerativeModel('gemini-2.5-flash')

    try:
        # Download file
        resp = requests.get(doc_url, timeout=25, allow_redirects=True)
        resp.raise_for_status()
        file_bytes = resp.content

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
                temperature=0.2,
                max_output_tokens=4096,
            )
        )

        result = response.text.strip() or "[No content extracted]"

        return jsonify({
            "status": "success",
            "result": result,
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