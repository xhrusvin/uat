from flask import render_template, request, jsonify, Response
from bson import ObjectId
from datetime import datetime
import json
import csv
import io
import re
import os
import threading

from database import db
from . import admin_bp
from admin.views import admin_required


# ── Helpers ──────────────────────────────────────────────────────────

# ── Google Cloud Storage helpers ──────────────────────────────────────

def _gcs_client():
    """Return an authenticated GCS client.
    Priority:
      1. GCS_CREDENTIALS_JSON env var (full JSON as string) — recommended
      2. GCS_KEY_FILE env var (path to JSON key file)
      3. Application Default Credentials (ADC) — if on Google Cloud VM
    """
    from google.cloud import storage as _gcs
    import json as _json

    creds_json = os.environ.get('GCS_CREDENTIALS_JSON', '').strip()
    if creds_json:
        from google.oauth2 import service_account
        info  = _json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return _gcs.Client(credentials=creds, project=info.get('project_id'))

    key_path = os.environ.get('GCS_KEY_FILE', '').strip()
    if key_path and os.path.exists(key_path):
        return _gcs.Client.from_service_account_json(key_path)

    # Fallback — Application Default Credentials (works on GCE/Cloud Run)
    return _gcs.Client()


def _gcs_bucket():
    return _gcs_client().bucket(os.environ.get('GCS_BUCKET_NAME', ''))


def _gcs_upload(blob_name, data_bytes, content_type='application/octet-stream'):
    """Upload bytes to GCS and return the blob name."""
    bucket = _gcs_bucket()
    blob   = bucket.blob(blob_name)
    blob.upload_from_string(data_bytes, content_type=content_type)
    return blob_name


def _gcs_download(blob_name):
    """Download bytes from GCS blob."""
    bucket = _gcs_bucket()
    blob   = bucket.blob(blob_name)
    return blob.download_as_bytes()


def _gcs_signed_url(blob_name, expiry_minutes=60):
    """
    Generate a signed URL for a GCS blob (time-limited, no public access needed).
    Requires the service account to have roles/iam.serviceAccountTokenCreator.
    Falls back to a direct Flask download URL if signing fails.
    """
    import datetime as _dt
    try:
        bucket = _gcs_bucket()
        blob   = bucket.blob(blob_name)
        url    = blob.generate_signed_url(
            expiration=_dt.timedelta(minutes=expiry_minutes),
            method='GET',
            version='v4',
        )
        return url
    except Exception:
        # Return None — caller will use internal download route instead
        return None



# ── Doc webhook collection ────────────────────────────────────────────

def _doc_webhook_col():
    return db.doc_webhook


# ── DOCX → PDF converter ──────────────────────────────────────────────

def _docx_to_pdf_bytes(docx_bytes):
    """
    Convert DOCX bytes to PDF bytes.
    Uses LibreOffice (soffice) via subprocess — available on the server.
    Falls back to docx2pdf if soffice not found.
    """
    import subprocess, tempfile, pathlib

    with tempfile.TemporaryDirectory() as tmp:
        docx_path = pathlib.Path(tmp) / 'input.docx'
        pdf_path  = pathlib.Path(tmp) / 'input.pdf'
        docx_path.write_bytes(docx_bytes)

        # Try LibreOffice first (preferred)
        for soffice in ['soffice', 'libreoffice',
                        '/usr/bin/soffice', '/usr/bin/libreoffice']:
            try:
                result = subprocess.run(
                    [soffice, '--headless', '--convert-to', 'pdf',
                     '--outdir', tmp, str(docx_path)],
                    capture_output=True, timeout=60
                )
                if result.returncode == 0 and pdf_path.exists():
                    return pdf_path.read_bytes()
            except (FileNotFoundError, subprocess.TimeoutExpired):
                continue

        raise RuntimeError(
            'LibreOffice not found. Install with: sudo apt install libreoffice'
        )


# ── Background HSE document upload ───────────────────────────────────

HSE_DOC_TYPES = {
    'cv':          'hse_cv',
    'interview':   'interview_notes',
    'appform':     'application_form',
}


def _resolve_xn_staff_id(staff_mongo_id, email):
    """
    Get the XN Portal staff_id to use in the HSE document upload API.

    Priority:
      1. live_staffs.staff_id field (already stored) — use directly
      2. live_staffs.xn_staff_id field (previously fetched) — use directly
      3. Call XN Portal user-document-list API with email → get data.id
         → store in live_staffs.xn_staff_id for future use

    Returns the resolved staff_id string, or None if not found.
    """
    import requests as _req

    col = _staffs_col()

    # 1. Check live_staffs.staff_id field first (primary source)
    doc = col.find_one(
        {"_id": ObjectId(staff_mongo_id)},
        {"staff_id": 1, "xn_staff_id": 1, "email": 1}
    )
    if doc and doc.get('staff_id'):
        return str(doc['staff_id'])

    # 2. Check previously resolved xn_staff_id
    if doc and doc.get('xn_staff_id'):
        return str(doc['xn_staff_id'])

    # 3. Fetch from XN Portal API
    if not email:
        return None

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')

    if not base_url:
        return None

    try:
        resp = _req.post(
            f"{base_url}/ai/recruitments/user-document-list",
            json={"email": email},
            headers={
                "Api-Key":       api_key,
                "X-App-Country": app_country,
                "Content-Type":  "application/json",
                "Accept":        "application/json",
            },
            timeout=30,
        )
        # Handle 405 — retry with GET
        if resp.status_code == 405:
            resp = _req.get(
                f"{base_url}/ai/recruitments/user-document-list",
                params={"email": email},
                headers={
                    "Api-Key":       api_key,
                    "X-App-Country": app_country,
                    "Accept":        "application/json",
                },
                timeout=30,
            )

        data     = resp.json()
        api_data = data.get('data') or {}

        # Handle both list and dict response
        if isinstance(api_data, list):
            api_data = api_data[0] if api_data else {}

        xn_id = str(api_data.get('id', '')).strip()
        if xn_id:
            # Store as xn_staff_id and also as staff_id for future use
            col.update_one(
                {"_id": ObjectId(staff_mongo_id)},
                {"$set": {
                    "xn_staff_id": xn_id,
                    "staff_id":    xn_id,
                }}
            )
            return xn_id

    except Exception:
        pass

    return None


def _push_hse_document_background(staff_id_str, doc_type_key,
                                   docx_bytes, staff_name='',
                                   mongo_id=None, email=''):
    """
    Fire-and-forget background task.
    Converts DOCX → PDF, POSTs to HSE document upload API,
    saves full response to doc_webhook collection.
    """
    def _run():
        import requests as _req

        base_url    = os.environ.get('DOC_BASE_URL', '').rstrip('/')
        api_key     = os.environ.get('DOC_API_KEY', '')
        app_country = os.environ.get('XN_APP_COUNTRY', '')

        if not base_url:
            _doc_webhook_col().insert_one({
                "staff_id":     staff_id_str,
                "staff_name":   staff_name,
                "doc_type":     doc_type_key,
                "status":       "error",
                "error":        "DOC_BASE_URL not set in environment",
                "triggered_at": datetime.utcnow(),
            })
            return

        # Resolve xn_staff_id — fetch from XN Portal if missing
        resolved_staff_id = staff_id_str
        if mongo_id and email:
            xn_id = _resolve_xn_staff_id(mongo_id, email)
            if xn_id:
                resolved_staff_id = xn_id

        endpoint      = f"{base_url}/api/admin/staff/hse-document-upload"
        hse_type      = HSE_DOC_TYPES.get(doc_type_key, doc_type_key)
        pdf_bytes     = None
        convert_error = None

        # Convert DOCX → PDF
        try:
            pdf_bytes = _docx_to_pdf_bytes(docx_bytes)
        except Exception as e:
            convert_error = str(e)

        if not pdf_bytes:
            _doc_webhook_col().insert_one({
                "staff_id":     staff_id_str,
                "staff_name":   staff_name,
                "doc_type":     doc_type_key,
                "hse_type":     hse_type,
                "status":       "error",
                "error":        f"PDF conversion failed: {convert_error}",
                "triggered_at": datetime.utcnow(),
            })
            return

        # POST to HSE API
        try:
            resp = _req.post(
                endpoint,
                data={
                    "staff_id":          resolved_staff_id,
                    "hse_document_type": hse_type,
                },
                files={
                    "file": (f"{hse_type}.pdf", pdf_bytes, "application/pdf"),
                },
                headers={
                    "Api-Key":       api_key,
                    "X-App-Country": app_country,
                    "Accept":        "application/json",
                },
                timeout=60,
            )

            # Try to parse JSON response
            try:
                resp_json = resp.json()
            except Exception:
                resp_json = {"raw": resp.text[:500]}

            _doc_webhook_col().insert_one({
                "staff_id":           staff_id_str,
                "xn_staff_id":        resolved_staff_id,
                "staff_name":         staff_name,
                "doc_type":           doc_type_key,
                "hse_type":           hse_type,
                "endpoint":           endpoint,
                "status":             "success" if resp.status_code < 300 else "api_error",
                "http_status":        resp.status_code,
                "response":           resp_json,
                "triggered_at":       datetime.utcnow(),
            })

        except Exception as e:
            _doc_webhook_col().insert_one({
                "staff_id":     staff_id_str,
                "staff_name":   staff_name,
                "doc_type":     doc_type_key,
                "hse_type":     hse_type,
                "endpoint":     endpoint,
                "status":       "error",
                "error":        str(e),
                "triggered_at": datetime.utcnow(),
            })

    # Launch in background thread — does not block the HTTP response
    t = threading.Thread(target=_run, daemon=True)
    t.start()


def _staffs_col():
    return db.live_staffs


def _serialize(doc):
    """Recursively convert ObjectId / datetime to JSON-safe types."""
    if isinstance(doc, list):
        return [_serialize(i) for i in doc]
    if isinstance(doc, dict):
        return {k: _serialize(v) for k, v in doc.items()}
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, datetime):
        return doc.isoformat()
    return doc


def _get_all(search, page, per_page):
    match = {}
    if search:
        pattern = re.compile(re.escape(search), re.IGNORECASE)
        match = {"$or": [
            {"section_1_personal_details.full_name": pattern},
            {"email": pattern},
            {"employee_code": pattern},
            {"section_1_personal_details.nationality": pattern},
            {"user_type": pattern},
        ]}
    col   = _staffs_col()
    total = col.count_documents(match)

    # Aggregation: add a sort key so records with extracted_cv come first
    pipeline = []
    if match:
        pipeline.append({"$match": match})
    pipeline += [
        {"$addFields": {
            "_cv_filled": {
                "$cond": {
                    "if": {
                        "$and": [
                            {"$ifNull": ["$extracted_cv", False]},
                            {"$ne": ["$extracted_cv", ""]},
                            {"$ne": ["$extracted_cv", None]},
                        ]
                    },
                    "then": 0,   # has CV — sort first
                    "else": 1    # no CV — sort after
                }
            }
        }},
        {"$sort": {
            "_cv_filled": 1,
            "section_1_personal_details.full_name": 1
        }},
        {"$skip":  (page - 1) * per_page},
        {"$limit": per_page},
    ]

    items = list(col.aggregate(pipeline))
    # Serialize BEFORE passing to template so tojson never sees ObjectId
    return [_serialize(doc) for doc in items], total


def _parse_json_content(content):
    """
    Handle all JSON variants that can come from the export pipeline:
      1. Standard JSON array  [ {...}, ... ]
      2. Standard JSON object { "records": [ ... ] }
      3. Bare fragment        "records": [ ... ]   ← missing outer braces
      4. JSONL                {...}\n{...}\n
      5. Concatenated objects {...}{...}
    """
    content = content.strip()

    # 1 & 2 — standard JSON
    try:
        raw = json.loads(content)
        return raw if isinstance(raw, list) else raw.get('records', [raw])
    except json.JSONDecodeError:
        pass

    # 3 — bare fragment (missing outer braces)
    try:
        raw = json.loads('{' + content + '}')
        if 'records' in raw:
            return raw['records']
    except json.JSONDecodeError:
        pass

    # 4 — JSONL
    try:
        lines = [l for l in content.splitlines() if l.strip()]
        records = [json.loads(l) for l in lines]
        if records:
            return records
    except json.JSONDecodeError:
        pass

    # 5 — concatenated objects
    try:
        records = []
        decoder = json.JSONDecoder()
        idx = 0
        while idx < len(content):
            while idx < len(content) and content[idx] in ' \t\r\n,':
                idx += 1
            if idx >= len(content):
                break
            obj, end = decoder.raw_decode(content, idx)
            records.append(obj)
            idx = end
        if records:
            return records
    except json.JSONDecodeError:
        pass

    raise ValueError("Could not parse JSON — unrecognised format.")


# ── Routes ───────────────────────────────────────────────────────────

@admin_bp.route('/live-staffs')
@admin_required
def live_staffs():
    page     = int(request.args.get('page', 1))
    search   = request.args.get('search', '').strip()
    per_page = 20

    items, total = _get_all(search, page, per_page)

    return render_template(
        'admin/live_staffs.html',
        staffs=items,
        page=page,
        total=total,
        per_page=per_page,
        search=search,
    )


@admin_bp.route('/live-staffs/get')
@admin_required
def live_staff_get():
    """Return a single staff record as JSON — used by view/edit modals."""
    staff_id = (request.args.get('id') or '').strip()
    if not staff_id:
        return jsonify({"success": False, "error": "Missing id"}), 400
    try:
        doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
        if not doc:
            return jsonify({"success": False, "error": "Record not found"}), 404
        return jsonify({"success": True, "record": _serialize(doc)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── AI CV Collection helper ───────────────────────────────────────────

def _ai_cvs_col():
    return db.live_staff_ai_cvs

def _ai_interviews_col():
    return db.live_staff_ai_interviews


def _ai_appforms_col():
    return db.live_staff_ai_appforms


# ── Generate AI Application Form ──────────────────────────────────────

@admin_bp.route('/live-staffs/ai-appform/generate', methods=['POST'])
@admin_required
def live_staff_ai_appform_generate():
    """
    Build a filled Xpress Health Application Form .docx for a staff member.
    Uses actual DB data — no AI hallucination, no Gemini needed.
    Uploads to GCS and saves metadata to MongoDB.
    """
    data     = request.get_json()
    staff_id = (data.get('staff_id') or '').strip()
    if not staff_id:
        return jsonify({"success": False, "error": "Missing staff_id"}), 400

    try:
        doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
        if not doc:
            return jsonify({"success": False, "error": "Staff record not found"}), 404

        docx_bytes = _build_appform_docx(doc)

        s1        = doc.get('section_1_personal_details') or {}
        full_name = _v(s1.get('full_name') or 'staff')
        email     = _v(doc.get('email'))
        emp_code  = _v(doc.get('employee_code') or '')
        safe_name = full_name.replace(' ', '_').replace('/', '_')
        filename  = f"AppForm_{safe_name}.docx"
        gcs_blob  = f"appforms/{filename}"

        _gcs_upload(
            gcs_blob, docx_bytes,
            content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        )

        col      = _ai_appforms_col()
        existing = col.find_one({"staff_id": str(doc['_id'])})
        rec = {
            "staff_id":      str(doc['_id']),
            "staff_name":    full_name,
            "employee_code": emp_code,
            "filename":      filename,
            "gcs_blob":      gcs_blob,
            "generated_at":  datetime.utcnow(),
        }
        if existing:
            col.update_one({"_id": existing["_id"]}, {"$set": rec})
            rec_id = str(existing["_id"])
        else:
            result = col.insert_one(rec)
            rec_id = str(result.inserted_id)

        # Background: push to HSE document API
        _push_hse_document_background(
            staff_id_str=staff_id,
            doc_type_key='appform',
            docx_bytes=docx_bytes,
            staff_name=full_name,
            mongo_id=staff_id,
            email=email,
        )

        return jsonify({
            "success":      True,
            "appform_id":   rec_id,
            "staff_name":   full_name,
            "message":      f"Application form generated for {full_name}",
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/ai-appform/download/<appform_id>')
@admin_required
def live_staff_ai_appform_download(appform_id):
    """Serve saved application form DOCX from Google Cloud Storage."""
    try:
        rec = _ai_appforms_col().find_one({"_id": ObjectId(appform_id)})
        if not rec:
            return "Application form not found", 404
        gcs_blob = rec.get('gcs_blob', '')
        if not gcs_blob:
            return "File not found in storage — please regenerate", 404
        name       = (rec.get('staff_name') or 'staff').replace(' ', '_')
        docx_bytes = _gcs_download(gcs_blob)
        return Response(
            docx_bytes,
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            headers={"Content-Disposition": f'attachment; filename="AppForm_{name}.docx"'}
        )
    except Exception as e:
        return str(e), 500


@admin_bp.route('/live-staffs/ai-appform/saved/<staff_id>')
@admin_required
def live_staff_ai_appform_saved(staff_id):
    """Check if a saved application form exists for this staff member."""
    try:
        rec = _ai_appforms_col().find_one({"staff_id": staff_id})
        if not rec:
            return jsonify({"success": True, "found": False})
        return jsonify({
            "success":      True,
            "found":        True,
            "appform_id":   str(rec["_id"]),
            "generated_at": rec["generated_at"].strftime("%d %b %Y %H:%M"),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/ai-appform/upload/<staff_id>', methods=['POST'])
@admin_required
def live_staff_ai_appform_upload(staff_id):
    """Replace the saved application form with an edited .docx upload."""
    if 'file' not in request.files:
        return jsonify({"success": False, "error": "No file provided"}), 400
    file = request.files['file']
    if not file.filename.lower().endswith('.docx'):
        return jsonify({"success": False, "error": "Only .docx files are accepted"}), 400
    try:
        col = _ai_appforms_col()
        rec = col.find_one({"staff_id": staff_id})
        if not rec:
            return jsonify({"success": False,
                            "error": "No saved application form found for this staff member"}), 404
        gcs_blob = rec.get('gcs_blob', '')
        if not gcs_blob:
            doc2 = _staffs_col().find_one({"_id": ObjectId(staff_id)})
            s1   = (doc2.get('section_1_personal_details') or {}) if doc2 else {}
            name = _v(s1.get('full_name') or 'staff').replace(' ', '_').replace('/', '_')
            gcs_blob = f"appforms/AppForm_{name}.docx"
        data_bytes = file.read()
        _gcs_upload(
            gcs_blob, data_bytes,
            content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        )
        col.update_one(
            {"_id": rec["_id"]},
            {"$set": {
                "gcs_blob":      gcs_blob,
                "filename":      os.path.basename(gcs_blob),
                "last_uploaded": datetime.utcnow(),
                "uploaded_by":   "admin",
            }}
        )
        # Background: push updated form to HSE document API
        try:
            _push_hse_document_background(
                staff_id_str=staff_id,
                doc_type_key='appform',
                docx_bytes=data_bytes,
                staff_name=(rec.get('staff_name') or ''),
                mongo_id=staff_id,
                email=_v((_staffs_col().find_one({"_id": ObjectId(staff_id)}) or {}).get('email') or ''),
            )
        except Exception:
            pass

        return jsonify({
            "success":  True,
            "message":  "Application form replaced successfully",
            "filename": os.path.basename(gcs_blob),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── Build Application Form DOCX ───────────────────────────────────────

def _build_appform_docx(doc):
    """
    Build a filled Xpress Health Application Form matching the uploaded template exactly.
    Sections: Personal Details, Identity Verification, Qualification and Experience,
              Declaration, Signature.
    All data pulled from live_staffs MongoDB document — no hallucination.
    """
    from docx import Document as DocxDocument
    from docx.shared import Pt, RGBColor, Inches, Cm, RGBColor
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement
    import io as _io

    BLACK  = RGBColor(0x00, 0x00, 0x00)
    NAVY   = RGBColor(0x1B, 0x3A, 0x6B)
    GREEN  = RGBColor(0x2E, 0x9E, 0x44)
    GRAY   = RGBColor(0x55, 0x55, 0x55)
    WHITE  = RGBColor(0xFF, 0xFF, 0xFF)

    # ── Extract data from doc ──────────────────────────────────────────
    s1   = doc.get('section_1_personal_details') or {}
    s2   = doc.get('section_2_identity_verification') or {}
    s3   = doc.get('section_3_professional_registration') or {}
    s4   = doc.get('section_4_qualifications') or {}
    s5   = doc.get('section_5_employment_history') or {}
    visa = s1.get('work_permit_visa_status') or {}
    docs = s2.get('documents_submitted') or {}

    full_name   = _v(s1.get('full_name'))
    email       = _v(doc.get('email'))
    user_type   = _v(doc.get('user_type'))
    address     = _v(s1.get('address'))
    eircode     = _v(s1.get('eircode_postcode'))
    mobile      = _v(s1.get('mobile_number'))
    pps         = _v(s1.get('pps_number'))
    perm_work   = _v(visa.get('permission_to_work'))
    total_exp   = _v(s5.get('total_experience'))
    nmbi_pin    = _v(s3.get('registration_number_pin'))
    divisions   = s3.get('divisions_registered_in') or []

    is_nurse  = 'nurse' in user_type.lower() if user_type else bool(divisions or nmbi_pin)
    is_hca    = not is_nurse

    d = DocxDocument()
    for sec in d.sections:
        sec.top_margin    = Cm(1.8)
        sec.bottom_margin = Cm(1.8)
        sec.left_margin   = Cm(2.2)
        sec.right_margin  = Cm(2.2)

    normal = d.styles['Normal']
    normal.font.name = 'Calibri'
    normal.font.size = Pt(11)

    def add_border_bottom(para, color='1B3A6B', size=12):
        pPr  = para._p.get_or_add_pPr()
        pBdr = OxmlElement('w:pBdr')
        bot  = OxmlElement('w:bottom')
        bot.set(qn('w:val'),   'single')
        bot.set(qn('w:sz'),    str(size))
        bot.set(qn('w:space'), '1')
        bot.set(qn('w:color'), color)
        pBdr.append(bot)
        pPr.append(pBdr)

    def add_doc_title():
        p = d.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after  = Pt(14)
        r = p.add_run('Xpress Health Application Form')
        r.bold = True
        r.font.size = Pt(18)
        r.font.name = 'Calibri'
        r.font.color.rgb = NAVY

    def add_section_heading(title):
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(14)
        p.paragraph_format.space_after  = Pt(6)
        r = p.add_run(title)
        r.bold = True
        r.font.size = Pt(12)
        r.font.name = 'Calibri'
        r.font.color.rgb = NAVY
        add_border_bottom(p, color='2E9E44', size=8)

    def add_field(label, value):
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(2)
        p.paragraph_format.space_after  = Pt(2)
        r1 = p.add_run(label + '  ')
        r1.bold = True
        r1.font.name = 'Calibri'
        r1.font.color.rgb = NAVY
        r2 = p.add_run(value or '')
        r2.font.name = 'Calibri'
        r2.font.color.rgb = BLACK

    def add_checkbox_line(label, checked):
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(2)
        p.paragraph_format.space_after  = Pt(2)
        tick = '☑' if checked else '☐'
        r = p.add_run(f'{tick}  {label}')
        r.font.name = 'Calibri'
        r.font.size = Pt(11)
        r.font.color.rgb = BLACK

    def add_checkbox_row(items):
        """Multiple checkboxes on one line: [(label, checked), ...]"""
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(3)
        p.paragraph_format.space_after  = Pt(3)
        for i, (label, checked) in enumerate(items):
            tick = '☑' if checked else '☐'
            run  = p.add_run(f'{tick}  {label}')
            run.font.name = 'Calibri'
            run.font.size = Pt(11)
            run.font.color.rgb = BLACK
            if i < len(items) - 1:
                spacer = p.add_run('       ')
                spacer.font.name = 'Calibri'

    def add_spacer(pts=6):
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after  = Pt(0)
        p.paragraph_format.line_spacing = Pt(pts)

    # ── Build document ─────────────────────────────────────────────────

    add_doc_title()

    # ── Section 1: Personal Details ────────────────────────────────────
    add_section_heading('Personal Details')
    add_field('Full Name:', full_name)
    add_field('Email:', email)
    add_field('Role:', user_type)
    add_field('Address:', address)
    add_field('Eircode/Postcode:', eircode)
    add_field('Mobile Number:', mobile)
    add_field('Work Permit / Visa Status:', perm_work or ('Yes' if visa.get('visa_type') else ''))
    add_field('PPS Number (if applicable)', pps)

    # ── Section 2: Identity Verification ─────────────────────────────
    add_section_heading('Identity Verification')
    p_id = d.add_paragraph()
    p_id.paragraph_format.space_before = Pt(4)
    p_id.paragraph_format.space_after  = Pt(4)
    r_id = p_id.add_run('ID Proof:')
    r_id.bold = True
    r_id.font.name = 'Calibri'
    r_id.font.color.rgb = NAVY

    add_checkbox_row([
        ('Passport',            bool(docs.get('passport'))),
        ('Birth Certificate',   bool(docs.get('birth_certificate'))),
        ('Driving Licence',     bool(docs.get('driving_licence'))),
        ('Proof of Address',    bool(docs.get('proof_of_address'))),
    ])

    # ── Section 3: Qualification and Experience ───────────────────────
    add_section_heading('Qualification and Experience')

    add_checkbox_row([
        ('Nurse (NMBI):', is_nurse),
        ('HCA (QQI L5):', is_hca),
    ])

    add_spacer(4)
    add_field('Total years of experience:', total_exp)

    # NMBI PIN if nurse
    if is_nurse and nmbi_pin:
        add_field('NMBI PIN:', nmbi_pin)
    if divisions:
        add_field('Divisions:', ', '.join(divisions))

    # ── Declaration ────────────────────────────────────────────────────
    add_section_heading('Declaration')
    p_decl = d.add_paragraph()
    p_decl.paragraph_format.space_before = Pt(4)
    p_decl.paragraph_format.space_after  = Pt(12)
    r_decl = p_decl.add_run(
        'I declare that the information provided in this application form is true and accurate '
        'to the best of my knowledge. I understand that any false or misleading information may '
        'result in the withdrawal of an offer of employment or termination of employment.'
    )
    r_decl.font.name = 'Calibri'
    r_decl.font.size = Pt(10)
    r_decl.font.color.rgb = GRAY
    r_decl.italic = True

    # ── Signature line ─────────────────────────────────────────────────
    p_sig = d.add_paragraph()
    p_sig.paragraph_format.space_before = Pt(6)
    p_sig.paragraph_format.space_after  = Pt(0)
    r_sig = p_sig.add_run('Applicant Signature:')
    r_sig.bold = True
    r_sig.font.name = 'Calibri'
    r_sig.font.color.rgb = NAVY
    p_sig.add_run('   _______________________________')

    buf = _io.BytesIO()
    d.save(buf)
    return buf.getvalue()




# ── Generate AI CV ────────────────────────────────────────────────────

@admin_bp.route('/live-staffs/ai-cv/generate', methods=['POST'])
@admin_required
def live_staff_ai_cv_generate():
    """Call Gemini to write a personalised CV, render to DOCX, upload to Google Cloud Storage."""
    data     = request.get_json()
    staff_id = (data.get('staff_id') or '').strip()
    if not staff_id:
        return jsonify({"success": False, "error": "Missing staff_id"}), 400

    try:
        doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
        if not doc:
            return jsonify({"success": False, "error": "Staff record not found"}), 404

        s1   = doc.get('section_1_personal_details') or {}
        s3   = doc.get('section_3_professional_registration') or {}
        s4   = doc.get('section_4_qualifications') or {}
        s5   = doc.get('section_5_employment_history') or {}
        s8   = doc.get('section_8_garda_vetting_police_clearance') or {}
        s9   = doc.get('section_9_occupational_health') or {}
        s10  = doc.get('section_10_mandatory_training') or {}
        visa = s1.get('work_permit_visa_status') or {}

        def _vv(val):
            if val is None: return ''
            return str(val).strip()

        full_name   = _vv(s1.get('full_name'))
        user_type   = _vv(doc.get('user_type'))
        address     = _vv(s1.get('address'))
        mobile      = _vv(s1.get('mobile_number'))
        email       = _vv(doc.get('email'))
        dob         = _vv(s1.get('date_of_birth'))
        nationality = _vv(s1.get('nationality'))
        emp_code    = _vv(doc.get('employee_code'))
        total_exp   = _vv(s5.get('total_experience'))
        divisions   = ', '.join(s3.get('divisions_registered_in') or [])
        reg_pin     = _vv(s3.get('registration_number_pin'))
        reg_exp     = _vv(s3.get('registration_expiry_date'))
        nmbi        = 'Yes' if s3.get('nmbi_active_declaration') else 'No'
        visa_type   = _vv(visa.get('visa_type'))
        perm_work   = _vv(visa.get('permission_to_work'))
        garda       = 'Yes' if s8.get('garda_vetting_submitted') else 'No'
        fit         = 'Yes' if s9.get('fit_for_nursing_duties') else 'No'

        qual_lines = []
        for qk in ['nursing_degree', 'postgraduate_qualification', 'other_qualification']:
            q = s4.get(qk) or {}
            if q.get('qualification') or q.get('institution'):
                qual_lines.append(
                    f"  - {_vv(q.get('qualification'))} | "
                    f"{_vv(q.get('institution'))} | "
                    f"{_vv(q.get('year_completed'))}"
                )

        entries = [e for e in (s5.get('entries') or [])
                   if e.get('employer') or e.get('position')]
        exp_lines = []
        for e in entries:
            exp_lines.append(
                f"  - {_vv(e.get('position'))} at {_vv(e.get('employer'))} "
                f"({_vv(e.get('from'))} - {_vv(e.get('to') or 'Present')})"
            )

        TLABELS = {
            'manual_handling': 'Manual Handling',
            'cpr_bls': 'CPR / BLS',
            'fire_safety': 'Fire Safety',
            'infection_prevention_control': 'Infection Prevention & Control',
            'hand_hygiene': 'Hand Hygiene',
            'safeguarding': 'Safeguarding',
            'children_first': 'Children First',
            'cyber_security': 'Cyber Security',
            'dignity_at_work': 'Dignity at Work',
            'open_disclosure': 'Open Disclosure',
            'mapa_pmav': 'MAPA / PMAV',
        }
        certs = [label for k, label in TLABELS.items() if s10.get(k)][:6]

        # Pull extracted_cv text from DB if available
        extracted_cv = _v(doc.get('extracted_cv') or '')
        has_extracted_cv = (
            extracted_cv and
            not extracted_cv.startswith('[') and
            extracted_cv != '[no CV document found]'
        )

        data_summary = f"""
Candidate: {full_name}
Role / User Type: {user_type}
Employee Code: {emp_code}
Address: {address}
Mobile: {mobile}
Email: {email}
Nationality: {nationality}
Total Experience: {total_exp}
Divisions / Speciality: {divisions}
Registration PIN: {reg_pin}
Registration Expiry: {reg_exp}
NMBI Active Declaration: {nmbi}
Permission to Work: {perm_work}
Visa / Stamp Type: {visa_type}
Garda Vetted: {garda}
Fit for Nursing Duties: {fit}

Qualifications:
{chr(10).join(qual_lines) if qual_lines else '  None recorded'}

Employment History (from profile):
{chr(10).join(exp_lines) if exp_lines else '  None recorded'}

Training & Certifications (on file):
{chr(10).join('  - ' + c for c in certs) if certs else '  None recorded'}
""".strip()

        # Build extracted CV section for prompt
        extracted_cv_section = f"""

EXTRACTED CV TEXT (use this as the PRIMARY source for PROFESSIONAL EXPERIENCE, TRAINING & CERTIFICATIONS and KEY SKILLS — copy the actual duties, skills and certifications directly from this text, preserving the candidate's own words):
{extracted_cv[:8000]}
""" if has_extracted_cv else ""

        prompt = f"""You are an expert professional CV writer specialising in Irish healthcare staffing.

STRICT RULE — NO HALLUCINATION:
You MUST use ONLY the exact facts provided in the CANDIDATE DATA and EXTRACTED CV TEXT below.
Do NOT invent, assume, or add any information that is not explicitly stated.
If a field is empty or says "None recorded", skip it.

SECTION SOURCE RULES:
- PERSONAL DETAILS, PROFESSIONAL PROFILE, EDUCATION & QUALIFICATIONS: use CANDIDATE DATA.
- PROFESSIONAL EXPERIENCE: {"Extract directly from EXTRACTED CV TEXT — copy the actual job titles, employers, dates, and duties word-for-word as written by the candidate. Do not rewrite or invent." if has_extracted_cv else "Use Employment History from CANDIDATE DATA. Write 5-6 appropriate duties per role."}
- TRAINING & CERTIFICATIONS: {"Extract directly from EXTRACTED CV TEXT — list only the certifications the candidate actually listed." if has_extracted_cv else "Use Training & Certifications from CANDIDATE DATA only."}
- KEY SKILLS: {"Extract directly from EXTRACTED CV TEXT — use the candidate's own skills list exactly as written." if has_extracted_cv else "Write 8-10 bullet points from their role and certifications in the data."}

Structure the CV exactly as follows (EXACT section headings in UPPERCASE on their own line):

PERSONAL DETAILS
PROFESSIONAL PROFILE
EDUCATION & QUALIFICATIONS
PROFESSIONAL EXPERIENCE
TRAINING & CERTIFICATIONS
KEY SKILLS
ADDITIONAL INFORMATION

Section format rules:
- PERSONAL DETAILS: "Label: Value" per line. Skip blank fields.
- PROFESSIONAL PROFILE: 2 paragraphs, FIRST PERSON ("I am", "I have", "I bring"). Genuine personal statement.
- EDUCATION & QUALIFICATIONS: Qualification Name | Institution | Year
- PROFESSIONAL EXPERIENCE: One block per role:
    Job Title: [title]
    Employer: [employer]
    Dates: [from] - [to]
    Duties:
    - [duty]
- TRAINING & CERTIFICATIONS: Bullet list of certifications only.
- KEY SKILLS: 8-10 bullet points.
- ADDITIONAL INFORMATION: Put each item on its own line. Include: Driving Licence: No
Own Transport: No

---
CANDIDATE DATA:
{data_summary}
{extracted_cv_section}
---

Output CV text only. No preamble, no explanation, no markdown symbols like ** or ##. Plain text with section headings and dash bullet points.
"""

        gemini_key = os.environ.get('GEMINI_API_KEY', '')
        if not gemini_key:
            return jsonify({"success": False, "error": "GEMINI_API_KEY not set in environment"}), 500

        from google import genai as google_genai
        client   = google_genai.Client(api_key=gemini_key)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        cv_text = response.text.strip()

        docx_bytes = _build_ai_cv_docx(doc, cv_text)

        safe_name   = (full_name or 'staff').replace(' ', '_').replace('/', '_')
        cv_filename = f"{safe_name}.docx"
        gcs_blob    = f"cv/{cv_filename}"
        _gcs_upload(gcs_blob, docx_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        col      = _ai_cvs_col()
        existing = col.find_one({"staff_id": str(doc['_id'])})
        ai_doc = {
            "staff_id":      str(doc['_id']),
            "staff_name":    full_name,
            "employee_code": emp_code,
            "cv_text":       cv_text,
            "cv_filename":   cv_filename,
            "gcs_blob":      gcs_blob,
            "generated_at":  datetime.utcnow(),
        }
        if existing:
            col.update_one({"_id": existing["_id"]}, {"$set": ai_doc})
            ai_id = str(existing["_id"])
        else:
            result = col.insert_one(ai_doc)
            ai_id  = str(result.inserted_id)

        # Background: push to HSE document API
        _push_hse_document_background(
            staff_id_str=staff_id,
            doc_type_key='cv',
            docx_bytes=docx_bytes,
            staff_name=full_name,
            mongo_id=staff_id,
            email=email,
        )

        return jsonify({
            "success":     True,
            "ai_cv_id":    ai_id,
            "cv_filename": cv_filename,
            "staff_name":  full_name,
            "message":     f"AI CV generated for {full_name}"
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/ai-cv/download/<ai_cv_id>')
@admin_required
def live_staff_ai_cv_download(ai_cv_id):
    """Serve the saved AI CV DOCX from Google Cloud Storage."""
    try:
        rec = _ai_cvs_col().find_one({"_id": ObjectId(ai_cv_id)})
        if not rec:
            return "AI CV not found", 404
        gcs_blob = rec.get('gcs_blob', '')
        if not gcs_blob:
            return "CV not found in storage — please regenerate", 404
        name       = (rec.get('staff_name') or 'staff').replace(' ', '_')
        filename   = f"{name}.docx"
        docx_bytes = _gcs_download(gcs_blob)
        return Response(
            docx_bytes,
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        return str(e), 500


@admin_bp.route('/live-staffs/ai-cv/saved/<staff_id>')
@admin_required
def live_staff_ai_cv_saved(staff_id):
    """Check if a saved AI CV exists for this staff member."""
    try:
        rec = _ai_cvs_col().find_one(
            {"staff_id": staff_id},
            {"cv_text": 0}
        )
        if not rec:
            return jsonify({"success": True, "found": False})
        return jsonify({
            "success":      True,
            "found":        True,
            "ai_cv_id":     str(rec["_id"]),
            "cv_filename":  rec.get("cv_filename", ""),
            "generated_at": rec["generated_at"].strftime("%d %b %Y %H:%M"),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/ai-cv/upload/<staff_id>', methods=['POST'])
@admin_required
def live_staff_ai_cv_upload(staff_id):
    """
    Replace the saved AI CV file with an edited version uploaded by the user.
    Accepts a .docx file upload and overwrites the existing file on disk.
    """
    if 'file' not in request.files:
        return jsonify({"success": False, "error": "No file provided"}), 400

    file = request.files['file']
    if not file.filename.lower().endswith('.docx'):
        return jsonify({"success": False, "error": "Only .docx files are accepted"}), 400

    try:
        col = _ai_cvs_col()
        rec = col.find_one({"staff_id": staff_id})

        if not rec:
            return jsonify({"success": False, "error": "No saved CV found for this staff member"}), 404

        # Determine GCS blob name
        gcs_blob = rec.get('gcs_blob', '')
        if not gcs_blob:
            doc2 = _staffs_col().find_one({"_id": ObjectId(staff_id)})
            s1   = (doc2.get('section_1_personal_details') or {}) if doc2 else {}
            name = _v(s1.get('full_name') or 'staff').replace(' ', '_').replace('/', '_')
            gcs_blob = f"cv/{name}.docx"

        # Upload to GCS — overwrites existing blob
        data_bytes = file.read()
        _gcs_upload(gcs_blob, data_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        col.update_one(
            {"_id": rec["_id"]},
            {"$set": {
                "gcs_blob":      gcs_blob,
                "cv_filename":   os.path.basename(gcs_blob),
                "last_uploaded": datetime.utcnow(),
                "uploaded_by":   "admin",
            }}
        )

        # Background: push updated CV to HSE document API
        try:
            _push_hse_document_background(
                staff_id_str=staff_id,
                doc_type_key='cv',
                docx_bytes=data_bytes,
                staff_name=(rec.get('staff_name') or ''),
                mongo_id=staff_id,
                email=_v((_staffs_col().find_one({"_id": ObjectId(staff_id)}) or {}).get('email') or ''),
            )
        except Exception:
            pass

        return jsonify({
            "success":  True,
            "message":  "CV replaced successfully with uploaded version",
            "filename": os.path.basename(gcs_blob),
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── Generate AI Interview Notes ───────────────────────────────────────

@admin_bp.route('/live-staffs/ai-interview/generate', methods=['POST'])
@admin_required
def live_staff_ai_interview_generate():
    """
    Call Gemini to write realistic interview notes for a staff member
    using the exact structure of the Nurse Interview Template.
    Saves DOCX to Google Cloud Storage and metadata to MongoDB.
    """
    data     = request.get_json()
    staff_id = (data.get('staff_id') or '').strip()
    if not staff_id:
        return jsonify({"success": False, "error": "Missing staff_id"}), 400

    try:
        doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
        if not doc:
            return jsonify({"success": False, "error": "Staff record not found"}), 404

        s1   = doc.get('section_1_personal_details') or {}
        s3   = doc.get('section_3_professional_registration') or {}
        s5   = doc.get('section_5_employment_history') or {}
        s8   = doc.get('section_8_garda_vetting_police_clearance') or {}
        s9   = doc.get('section_9_occupational_health') or {}
        s10  = doc.get('section_10_mandatory_training') or {}
        visa = s1.get('work_permit_visa_status') or {}

        full_name   = _v(s1.get('full_name'))
        email       = _v(doc.get('email'))
        user_type   = _v(doc.get('user_type'))
        address     = _v(s1.get('address'))
        nationality = _v(s1.get('nationality'))
        reg_pin     = _v(s3.get('registration_number_pin'))
        visa_type   = _v(visa.get('visa_type'))
        divisions   = ', '.join(s3.get('divisions_registered_in') or [])
        total_exp   = _v(s5.get('total_experience'))
        entries     = [e for e in (s5.get('entries') or [])
                       if e.get('employer') or e.get('position')]
        nmbi        = 'Yes' if s3.get('nmbi_active_declaration') else 'No'
        garda       = 'Yes' if s8.get('garda_vetting_submitted') else 'No'
        bls         = 'Yes' if s10.get('cpr_bls') else 'No'
        manual      = 'Yes' if s10.get('manual_handling') else 'No'
        fit         = 'Yes' if s9.get('fit_for_nursing_duties') else 'No'

        # Preferred county from address
        county = ''
        if address:
            parts = [p.strip() for p in address.replace(',', ' ').split()]
            for p in parts:
                if p.lower().startswith('co.') or p.lower() == 'county':
                    idx = parts.index(p)
                    if idx + 1 < len(parts):
                        county = parts[idx + 1]
                    break
            if not county:
                county = parts[-1] if parts else ''

        # Build experience summary for prompt
        exp_lines = []
        for e in entries[:5]:
            pos = _v(e.get('position')); emp = _v(e.get('employer'))
            d_from = _v(e.get('from')); d_to = _v(e.get('to'))
            if pos or emp:
                exp_lines.append(
                    f"  - {pos} at {emp} ({d_from} – {d_to or 'Present'})"
                )

        TLABELS = {
            'manual_handling': 'Manual Handling',
            'cpr_bls': 'BLS/CPR',
            'safeguarding': 'Safeguarding',
            'fire_safety': 'Fire Safety',
            'infection_prevention_control': 'Infection Prevention & Control',
        }
        certs = [label for k, label in TLABELS.items() if s10.get(k)]

        data_summary = f"""
Name: {full_name}
Role / User Type: {user_type}
Address / Location: {address}
Nationality: {nationality}
Visa / Stamp Type: {visa_type}
NMBI Registration PIN: {reg_pin}
NMBI Registration Active: {nmbi}
Divisions / Speciality: {divisions}
Total Experience: {total_exp}
Garda Vetted: {garda}
BLS/CPR on file: {bls}
Manual Handling on file: {manual}
Fit for Duties: {fit}

Employment History:
{chr(10).join(exp_lines) if exp_lines else '  None recorded'}

Certifications on file: {', '.join(certs) if certs else 'None recorded'}
""".strip()

        prompt = f"""You are an experienced nursing recruitment consultant at Xpress Health, Ireland.

Using ONLY the verified candidate data below, complete a realistic, professional nurse interview notes template.
Answers must be written as if the candidate themselves just answered each question in a live phone/video interview.
Write naturally — conversational but professional. First person where appropriate ("I have", "I work", "I currently").

STRICT RULES — NO HALLUCINATION:
- Use ONLY the facts provided in CANDIDATE DATA. Do not invent employers, dates, locations, or qualifications.
- If data is missing for a field, write a realistic professional answer appropriate to their role and experience level without inventing specific names.
- Clinical question answers must be clinically appropriate for a {user_type}.
- Assessment scores: pick a random realistic score for each between 3.5 and 5.0 in 0.5 increments (e.g. 3.5/5, 4/5, 4.5/5, 5/5). Vary the three scores — do not give the same score to all three.
- Do NOT add any text outside the template structure below.

Output ONLY the completed template below — no preamble, no explanations, no markdown symbols:

---
Completed {user_type} Interview

Name: [full name]
Location: [county/city from address]
NMBI PIN: [registration pin or N/A]
Visa Status: [visa type]

Experience

1. Tell me about your nursing experience.
[Write a 4–6 sentence answer in first person describing their experience, speciality, and current/most recent role. Use only the data provided.]

2. How many years in Ireland?
[Write a realistic answer based on employment history dates. If Ireland-based work is evident, state it clearly.]

3. Acute, Nursing Home, Community, or Mental Health?
[Based on employment history, state the most relevant care setting.]

Clinical Questions

1. How would you manage a deteriorating patient?
[Write a clinically accurate 4–5 sentence answer appropriate for a {user_type}. Use recognised frameworks (ABCDE, NEWS2, ISBAR) where appropriate.]

2. What would you do if you witnessed a medication error?
[Write a clinically accurate 4–5 sentence answer covering patient safety, reporting, documentation, and prevention.]

Compliance
NMBI Registration: [Yes/No based on data]
BLS/CPR: [Yes/No based on data]
Manual Handling: [Yes/No based on data]
Garda Vetting: [Yes/No based on data]
References: Yes

Availability
Preferred counties: [county from address, or nearest city]
Day/Night/Both: Both
Earliest start date: Immediate

Assessment
Communication: [3.5/5 or 4/5 or 4.5/5 or 5/5 — vary randomly]
Clinical Knowledge: [3.5/5 or 4/5 or 4.5/5 or 5/5 — vary randomly]
Experience: [3.5/5 or 4/5 or 4.5/5 or 5/5 — vary randomly]
Suitable: Yes
---

CANDIDATE DATA (use ONLY this):
{data_summary}
"""

        gemini_key = os.environ.get('GEMINI_API_KEY', '')
        if not gemini_key:
            return jsonify({"success": False,
                            "error": "GEMINI_API_KEY not set"}), 500

        from google import genai as google_genai
        client   = google_genai.Client(api_key=gemini_key)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        interview_text = response.text.strip()
        # Strip leading/trailing --- if Gemini included them
        interview_text = interview_text.strip('-').strip()

        # Build DOCX
        docx_bytes = _build_interview_docx(doc, interview_text)

        # Upload to Google Cloud Storage
        safe_name    = (full_name or 'staff').replace(' ', '_').replace('/', '_')
        filename     = f"Interview_{safe_name}_{staff_id}.docx"
        gcs_blob  = f"interviews/{filename}"
        _gcs_upload(gcs_blob, docx_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        # Save metadata to MongoDB
        col      = _ai_interviews_col()
        existing = col.find_one({"staff_id": str(doc['_id'])})
        rec = {
            "staff_id":       str(doc['_id']),
            "staff_name":     full_name,
            "employee_code":  _v(doc.get('employee_code')),
            "interview_text": interview_text,
            "filename":       filename,
            "gcs_blob":       gcs_blob,
            "generated_at":   datetime.utcnow(),
        }
        if existing:
            col.update_one({"_id": existing["_id"]}, {"$set": rec})
            rec_id = str(existing["_id"])
        else:
            result = col.insert_one(rec)
            rec_id = str(result.inserted_id)

        # Background: push to HSE document API
        _push_hse_document_background(
            staff_id_str=staff_id,
            doc_type_key='interview',
            docx_bytes=docx_bytes,
            staff_name=full_name,
            mongo_id=staff_id,
            email=email,
        )

        return jsonify({
            "success":      True,
            "interview_id": rec_id,
            "staff_name":   full_name,
            "message":      f"Interview notes generated for {full_name}"
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/ai-interview/download/<interview_id>')
@admin_required
def live_staff_ai_interview_download(interview_id):
    """Serve saved interview DOCX from Google Cloud Storage."""
    try:
        rec = _ai_interviews_col().find_one({"_id": ObjectId(interview_id)})
        if not rec:
            return "Interview notes not found", 404
        gcs_blob = rec.get('gcs_blob', '')
        if not gcs_blob:
            return "File not found in storage — please regenerate", 404
        name       = (rec.get('staff_name') or 'staff').replace(' ', '_')
        docx_bytes = _gcs_download(gcs_blob)
        return Response(
            docx_bytes,
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            headers={"Content-Disposition":
                     f'attachment; filename="Interview_{name}.docx"'}
        )
    except Exception as e:
        return str(e), 500


@admin_bp.route('/live-staffs/ai-interview/saved/<staff_id>')
@admin_required
def live_staff_ai_interview_saved(staff_id):
    """Check if saved interview notes exist for this staff member."""
    try:
        rec = _ai_interviews_col().find_one(
            {"staff_id": staff_id},
            {"interview_text": 0}
        )
        if not rec:
            return jsonify({"success": True, "found": False})
        return jsonify({
            "success":      True,
            "found":        True,
            "interview_id": str(rec["_id"]),
            "generated_at": rec["generated_at"].strftime("%d %b %Y %H:%M"),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/ai-interview/upload/<staff_id>', methods=['POST'])
@admin_required
def live_staff_ai_interview_upload(staff_id):
    """
    Replace the saved interview notes file with an edited version uploaded by the user.
    Accepts a .docx file and overwrites the existing file on disk.
    """
    if 'file' not in request.files:
        return jsonify({"success": False, "error": "No file provided"}), 400

    file = request.files['file']
    if not file.filename.lower().endswith('.docx'):
        return jsonify({"success": False, "error": "Only .docx files are accepted"}), 400

    try:
        col = _ai_interviews_col()
        rec = col.find_one({"staff_id": staff_id})

        if not rec:
            return jsonify({"success": False,
                            "error": "No saved interview notes found for this staff member"}), 404

        gcs_blob = rec.get('gcs_blob', '')
        if not gcs_blob:
            doc2 = _staffs_col().find_one({"_id": ObjectId(staff_id)})
            s1   = (doc2.get('section_1_personal_details') or {}) if doc2 else {}
            name = _v(s1.get('full_name') or 'staff').replace(' ', '_').replace('/', '_')
            gcs_blob = f"interviews/Interview_{name}_{staff_id}.docx"

        data_bytes = file.read()
        _gcs_upload(gcs_blob, data_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        col.update_one(
            {"_id": rec["_id"]},
            {"$set": {
                "gcs_blob":      gcs_blob,
                "filename":      os.path.basename(gcs_blob),
                "last_uploaded": datetime.utcnow(),
                "uploaded_by":   "admin",
            }}
        )

        # Background: push updated interview to HSE document API
        try:
            _push_hse_document_background(
                staff_id_str=staff_id,
                doc_type_key='interview',
                docx_bytes=data_bytes,
                staff_name=(rec.get('staff_name') or ''),
                mongo_id=staff_id,
                email=_v((_staffs_col().find_one({"_id": ObjectId(staff_id)}) or {}).get('email') or ''),
            )
        except Exception:
            pass

        return jsonify({
            "success":  True,
            "message":  "Interview notes replaced successfully with uploaded version",
            "filename": os.path.basename(gcs_blob),
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── Build Interview Notes PDF ─────────────────────────────────────────



def _build_interview_docx(doc, interview_text):
    """
    Render AI interview notes as a Word doc matching the original
    Completed Nurse Interview PDF design:
    - Plain white background throughout
    - Bold black section headings with a simple bottom border line
    - Bold label + plain value for fields
    - Numbered questions in bold, answers as plain indented paragraphs
    - No coloured boxes, no navy/green fills
    """
    from docx import Document as DocxDocument
    from docx.shared import Pt, RGBColor, Inches, Cm, Twips
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement
    import io as _io, re as _re

    BLACK  = RGBColor(0x00, 0x00, 0x00)
    DKGRAY = RGBColor(0x22, 0x22, 0x22)

    s1_d      = doc.get('section_1_personal_details') or {}
    full_name = _v(s1_d.get('full_name')) or 'Candidate'
    user_type = _v(doc.get('user_type')) or 'Nurse'

    d = DocxDocument()

    # ── Margins — match original template ────────────────────────────
    for sec in d.sections:
        sec.top_margin    = Cm(2.54)
        sec.bottom_margin = Cm(2.54)
        sec.left_margin   = Cm(2.54)
        sec.right_margin  = Cm(2.54)

    # ── Default Normal style ──────────────────────────────────────────
    normal = d.styles['Normal']
    normal.font.name  = 'Calibri'
    normal.font.size  = Pt(11)
    normal.font.color.rgb = BLACK

    # ── Helpers ───────────────────────────────────────────────────────

    def add_para_border_bottom(para):
        """Add a thin black bottom border to a paragraph (section divider)."""
        pPr  = para._p.get_or_add_pPr()
        pBdr = OxmlElement('w:pBdr')
        bot  = OxmlElement('w:bottom')
        bot.set(qn('w:val'),   'single')
        bot.set(qn('w:sz'),    '6')       # 0.75pt
        bot.set(qn('w:space'), '1')
        bot.set(qn('w:color'), '000000')
        pBdr.append(bot)
        pPr.append(pBdr)

    def add_section_heading(title):
        """Bold heading + bottom border line — plain black on white."""
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(14)
        p.paragraph_format.space_after  = Pt(4)
        run = p.add_run(title)
        run.bold = True
        run.font.size = Pt(13)
        run.font.color.rgb = BLACK
        run.font.name = 'Calibri'
        add_para_border_bottom(p)

    def add_field(label, value):
        """Bold label followed by plain value on the same line."""
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(1)
        p.paragraph_format.space_after  = Pt(1)
        r1 = p.add_run(label + ' ')
        r1.bold = True
        r1.font.name = 'Calibri'
        r1.font.color.rgb = BLACK
        r2 = p.add_run(value or '')
        r2.font.name = 'Calibri'
        r2.font.color.rgb = BLACK

    def add_numbered_question(text):
        """Bold numbered question."""
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(8)
        p.paragraph_format.space_after  = Pt(2)
        run = p.add_run(text)
        run.bold = True
        run.font.size = Pt(11)
        run.font.name = 'Calibri'
        run.font.color.rgb = BLACK

    def add_answer_text(text):
        """Plain answer paragraph — indented slightly."""
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after  = Pt(6)
        p.paragraph_format.left_indent  = Inches(0.2)
        run = p.add_run(text or '')
        run.font.name = 'Calibri'
        run.font.size = Pt(11)
        run.font.color.rgb = BLACK

    def add_compliance_field(label, value):
        """Bold label + bold value (no colour change — plain black)."""
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(1)
        p.paragraph_format.space_after  = Pt(1)
        r1 = p.add_run(label + ' ')
        r1.bold = True
        r1.font.name = 'Calibri'
        r1.font.color.rgb = BLACK
        r2 = p.add_run(value or '')
        r2.bold = True
        r2.font.name = 'Calibri'
        r2.font.color.rgb = BLACK

    def add_score_field(label, value):
        """Bold label + bold score value."""
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(1)
        p.paragraph_format.space_after  = Pt(1)
        r1 = p.add_run(label + ' ')
        r1.bold = True
        r1.font.name = 'Calibri'
        r1.font.color.rgb = BLACK
        r2 = p.add_run(value or '')
        r2.bold = True
        r2.font.name = 'Calibri'
        r2.font.color.rgb = BLACK

    # ── Parse interview text ──────────────────────────────────────────
    parsed = {'header': {}, 'experience': {}, 'clinical': {},
              'compliance': {}, 'availability': {}, 'assessment': {}}
    current = 'header'
    cur_q   = None
    cur_ans = []

    def flush():
        nonlocal cur_q, cur_ans
        if cur_q and cur_ans:
            parsed[current][cur_q] = ' '.join(cur_ans).strip()
            cur_q = None
            cur_ans = []

    for line in interview_text.splitlines():
        sl = line.strip()
        if sl == 'Experience':
            flush(); current = 'experience'; continue
        elif sl == 'Clinical Questions':
            flush(); current = 'clinical'; continue
        elif sl == 'Compliance':
            flush(); current = 'compliance'; continue
        elif sl == 'Availability':
            flush(); current = 'availability'; continue
        elif sl == 'Assessment':
            flush(); current = 'assessment'; continue

        if current == 'header':
            if ':' in sl:
                k, v = sl.split(':', 1)
                parsed['header'][k.strip()] = v.strip()
        elif current in ('experience', 'clinical'):
            if _re.match(r'^[0-9]+\.\s+.+$', sl):
                flush(); cur_q = sl; cur_ans = []
            elif cur_q and sl:
                cur_ans.append(sl)
        elif current in ('compliance', 'availability', 'assessment'):
            if ':' in sl:
                k, v = sl.split(':', 1)
                parsed[current][k.strip()] = v.strip()
    flush()

    # ── Build document ────────────────────────────────────────────────

    # Document title — centred bold
    title_p = d.add_paragraph()
    title_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    title_p.paragraph_format.space_before = Pt(0)
    title_p.paragraph_format.space_after  = Pt(16)
    t_run = title_p.add_run(f'Completed {user_type} Interview')
    t_run.bold = True
    t_run.font.size = Pt(16)
    t_run.font.name = 'Calibri'
    t_run.font.color.rgb = BLACK

    # Header fields (Name / Location / NMBI PIN / Visa Status)
    hdr = parsed['header']
    for key in ['Name', 'Location', 'NMBI PIN', 'Visa Status']:
        add_field(f'{key}:', hdr.get(key, ''))

    # ── Experience ────────────────────────────────────────────────────
    add_section_heading('Experience')
    for q_text, answer in parsed['experience'].items():
        add_numbered_question(q_text)
        add_answer_text(answer)

    # ── Clinical Questions ────────────────────────────────────────────
    add_section_heading('Clinical Questions')
    for q_text, answer in parsed['clinical'].items():
        add_numbered_question(q_text)
        add_answer_text(answer)

    # ── Compliance ────────────────────────────────────────────────────
    add_section_heading('Compliance')
    for field in ['NMBI Registration', 'BLS/CPR', 'Manual Handling',
                  'Garda Vetting', 'References']:
        add_compliance_field(
            f'{field}:', parsed['compliance'].get(field, '')
        )

    # ── Availability ──────────────────────────────────────────────────
    add_section_heading('Availability')
    for field in ['Preferred counties', 'Day/Night/Both', 'Earliest start date']:
        add_field(f'{field}:', parsed['availability'].get(field, ''))

    # ── Assessment ────────────────────────────────────────────────────
    add_section_heading('Assessment')
    for field in ['Communication', 'Clinical Knowledge', 'Experience']:
        add_score_field(f'{field}:', parsed['assessment'].get(field, ''))
    add_compliance_field('Suitable:', parsed['assessment'].get('Suitable', 'Yes'))

    buf = _io.BytesIO()
    d.save(buf)
    return buf.getvalue()



@admin_bp.route('/live-staffs/cv/<staff_id>')
@admin_required
def live_staff_cv(staff_id):
    """Generate and download a filled HSE CV PDF for a staff member."""
    try:
        doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
        if not doc:
            return "Staff record not found", 404
        pdf_bytes = _build_cv_pdf(doc)
        s1   = (doc.get('section_1_personal_details') or {})
        name = (s1.get('full_name') or 'staff').replace(' ', '_')
        filename = f"CV_{name}.pdf"
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        return str(e), 500


def _v(val):
    """Return value as string, or empty string if None/empty."""
    if val is None:
        return ''
    return str(val).strip()






def _build_ai_cv_docx(doc, cv_text):
    """
    Render AI-generated CV text as a clean Word document (.docx).
    Plain black text, Calibri font, section headings with bottom border —
    matches the clean professional style of the interview docx.
    """
    from docx import Document as DocxDocument
    from docx.shared import Pt, RGBColor, Inches, Cm
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement
    import io as _io, re as _re

    BLACK = RGBColor(0x00, 0x00, 0x00)
    GRAY  = RGBColor(0x44, 0x44, 0x44)

    s1_d      = doc.get('section_1_personal_details') or {}
    full_name = _v(s1_d.get('full_name')) or 'Candidate'
    mobile    = _v(s1_d.get('mobile_number'))
    email     = _v(doc.get('email'))
    address   = _v(s1_d.get('address'))

    d = DocxDocument()
    for sec in d.sections:
        sec.top_margin    = Cm(2.54)
        sec.bottom_margin = Cm(2.54)
        sec.left_margin   = Cm(2.54)
        sec.right_margin  = Cm(2.54)

    normal = d.styles['Normal']
    normal.font.name  = 'Calibri'
    normal.font.size  = Pt(11)

    def add_border_bottom(para):
        pPr  = para._p.get_or_add_pPr()
        pBdr = OxmlElement('w:pBdr')
        bot  = OxmlElement('w:bottom')
        bot.set(qn('w:val'),   'single')
        bot.set(qn('w:sz'),    '6')
        bot.set(qn('w:space'), '1')
        bot.set(qn('w:color'), '000000')
        pBdr.append(bot)
        pPr.append(pBdr)

    def add_section_heading(title):
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(14)
        p.paragraph_format.space_after  = Pt(4)
        r = p.add_run(title.upper())
        r.bold = True
        r.font.size = Pt(13)
        r.font.name = 'Calibri'
        r.font.color.rgb = BLACK
        add_border_bottom(p)

    def add_name_header():
        # Name — large centred
        p = d.add_paragraph()
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        p.paragraph_format.space_after = Pt(4)
        r = p.add_run(full_name)
        r.bold = True
        r.font.size = Pt(18)
        r.font.name = 'Calibri'
        r.font.color.rgb = BLACK
        # Contact line
        contact = '   |   '.join(x for x in [mobile, email, address] if x)
        if contact:
            cp = d.add_paragraph()
            cp.alignment = WD_ALIGN_PARAGRAPH.CENTER
            cp.paragraph_format.space_after = Pt(12)
            cr = cp.add_run(contact)
            cr.font.size = Pt(9)
            cr.font.name = 'Calibri'
            cr.font.color.rgb = GRAY

    def add_body(text):
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after  = Pt(6)
        r = p.add_run(text)
        r.font.name = 'Calibri'
        r.font.size = Pt(11)
        r.font.color.rgb = BLACK

    def add_field(label, value):
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(1)
        p.paragraph_format.space_after  = Pt(1)
        r1 = p.add_run(label + '  ')
        r1.bold = True
        r1.font.name = 'Calibri'
        r1.font.color.rgb = BLACK
        r2 = p.add_run(value or '')
        r2.font.name = 'Calibri'
        r2.font.color.rgb = BLACK

    def add_role_heading(title, dates=''):
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(8)
        p.paragraph_format.space_after  = Pt(1)
        r1 = p.add_run(title)
        r1.bold = True
        r1.font.size = Pt(11)
        r1.font.name = 'Calibri'
        r1.font.color.rgb = BLACK
        if dates:
            r2 = p.add_run(f'   {dates}')
            r2.font.size = Pt(9)
            r2.font.name = 'Calibri'
            r2.font.color.rgb = GRAY

    def add_sub(text):
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after  = Pt(2)
        r = p.add_run(text)
        r.italic = True
        r.font.size = Pt(10)
        r.font.name = 'Calibri'
        r.font.color.rgb = GRAY

    def add_bullet(text):
        clean = text.lstrip('- •	').strip()
        if not clean:
            return
        p = d.add_paragraph(style='List Bullet')
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after  = Pt(2)
        p.paragraph_format.left_indent  = Inches(0.25)
        r = p.add_run(clean)
        r.font.name = 'Calibri'
        r.font.size = Pt(11)
        r.font.color.rgb = BLACK

    # ── Parse sections ────────────────────────────────────────────────
    HEADINGS = [
        'PERSONAL DETAILS', 'PROFESSIONAL PROFILE',
        'EDUCATION & QUALIFICATIONS', 'PROFESSIONAL EXPERIENCE',
        'TRAINING & CERTIFICATIONS', 'KEY SKILLS', 'ADDITIONAL INFORMATION',
    ]
    sections = {}
    current  = '__pre__'
    sections[current] = []
    for line in cv_text.splitlines():
        matched = next((h for h in HEADINGS if line.strip().upper() == h), None)
        if matched:
            current = matched
            sections[current] = []
        else:
            sections.setdefault(current, []).append(line)

    # ── Build document ────────────────────────────────────────────────
    add_name_header()

    for heading in HEADINGS:
        lines = [l for l in sections.get(heading, []) if l.strip()]
        if not lines:
            continue

        add_section_heading(heading)

        if heading == 'PERSONAL DETAILS':
            for line in lines:
                if ':' in line:
                    parts = line.split(':', 1)
                    lbl = parts[0].strip() + ':'
                    val = parts[1].strip()
                    if val:
                        add_field(lbl, val)

        elif heading == 'PROFESSIONAL PROFILE':
            para_buf = []
            for line in lines:
                if line.strip() == '':
                    if para_buf:
                        add_body(' '.join(para_buf))
                        para_buf = []
                else:
                    para_buf.append(line.strip())
            if para_buf:
                add_body(' '.join(para_buf))

        elif heading == 'EDUCATION & QUALIFICATIONS':
            for line in lines:
                s = line.strip().lstrip('- ').strip()
                if not s:
                    continue
                parts = [p.strip() for p in s.split('|')]
                qual  = parts[0] if parts else s
                inst  = parts[1] if len(parts) > 1 else ''
                year  = parts[2] if len(parts) > 2 else ''
                p = d.add_paragraph()
                p.paragraph_format.space_before = Pt(4)
                p.paragraph_format.space_after  = Pt(1)
                r = p.add_run(qual + (f'  ({year})' if year else ''))
                r.bold = True
                r.font.name = 'Calibri'
                r.font.color.rgb = BLACK
                if inst:
                    add_sub(inst)

        elif heading == 'PROFESSIONAL EXPERIENCE':
            roles = []
            cur   = []
            for line in lines:
                if line.strip().lower().startswith('job title:') and cur:
                    roles.append(cur); cur = [line]
                else:
                    cur.append(line)
            if cur:
                roles.append(cur)

            for role_lines in roles:
                jt = en = ds = ''
                duties = []
                for rl in role_lines:
                    sl = rl.strip(); sll = sl.lower()
                    if not sl: continue
                    if sll.startswith('job title:'):  jt = sl.split(':',1)[1].strip()
                    elif sll.startswith('employer:'): en = sl.split(':',1)[1].strip()
                    elif sll.startswith('dates:'):    ds = sl.split(':',1)[1].strip()
                    elif sll.startswith('duties'):    pass
                    elif sl.startswith('-') or sl.startswith('•'):
                        duties.append(sl.lstrip('- •').strip())
                if not jt and not en:
                    continue
                add_role_heading(jt or 'Role', ds)
                if en:
                    add_sub(en)
                for duty in duties:
                    if duty:
                        add_bullet(duty)
                d.add_paragraph()

        elif heading in ('TRAINING & CERTIFICATIONS', 'KEY SKILLS'):
            for line in lines:
                s = line.strip()
                if s:
                    add_bullet(s)

        elif heading == 'ADDITIONAL INFORMATION':
            for line in lines:
                s = line.strip()
                if not s or s.lower().startswith('reference'):
                    continue
                if ':' in s:
                    parts = s.split(':', 1)
                    lbl   = parts[0].strip() + ':'
                    val   = parts[1].strip()
                    # Each item on its own line, no bold — plain text
                    p = d.add_paragraph()
                    p.paragraph_format.space_before = Pt(1)
                    p.paragraph_format.space_after  = Pt(1)
                    r = p.add_run(f'{lbl}  {val}')
                    r.font.name  = BF
                    r.font.size  = Pt(L['body_size'])
                    r.font.color.rgb = NEAR_BLK
                else:
                    add_body(s)

    buf = _io.BytesIO()
    d.save(buf)
    return buf.getvalue()


def _build_ai_cv_pdf(doc, cv_text):
    """
    4 visually distinct ATS-friendly CV designs, all black text.
    Theme chosen by md5(staff_id) % 4 — same staff always gets same design.
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, HRFlowable,
        Image as RLImage, Table, TableStyle
    )
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY, TA_RIGHT
    import io as _io, hashlib

    BLACK     = colors.HexColor('#000000')
    NEAR_BLK  = colors.HexColor('#111111')
    DARK_GRAY = colors.HexColor('#333333')
    LT_GRAY   = colors.HexColor('#CCCCCC')
    WHITE     = colors.white

    W, H = A4
    id_str  = str(doc.get('_id', ''))
    theme_n = int(hashlib.md5(id_str.encode()).hexdigest(), 16) % 4

    LAYOUTS = [
        {'lm':22*mm,'rm':22*mm,'tm':18*mm,'bm':15*mm,'bf':'Helvetica','bfb':'Helvetica-Bold','bfi':'Helvetica-Oblique','name_size':22,'name_align':TA_LEFT,'sec_size':10,'body_size':10,'contact_align':TA_LEFT},
        {'lm':25*mm,'rm':25*mm,'tm':20*mm,'bm':15*mm,'bf':'Times-Roman','bfb':'Times-Bold','bfi':'Times-Italic','name_size':24,'name_align':TA_CENTER,'sec_size':11,'body_size':10,'contact_align':TA_CENTER},
        {'lm':18*mm,'rm':18*mm,'tm':15*mm,'bm':12*mm,'bf':'Helvetica','bfb':'Helvetica-Bold','bfi':'Helvetica-Oblique','name_size':20,'name_align':TA_LEFT,'sec_size':9,'body_size':9.5,'contact_align':TA_LEFT},
        {'lm':28*mm,'rm':28*mm,'tm':22*mm,'bm':18*mm,'bf':'Times-Roman','bfb':'Times-Bold','bfi':'Times-Italic','name_size':26,'name_align':TA_CENTER,'sec_size':11,'body_size':10.5,'contact_align':TA_CENTER},
    ]
    L      = LAYOUTS[theme_n]
    PAGE_W = W - L['lm'] - L['rm']
    BF     = L['bf']
    BFB    = L['bfb']
    BFI    = L['bfi']

    def ps(name, **kw):
        d = dict(fontName=BF, fontSize=L['body_size'], textColor=NEAR_BLK,
                 spaceAfter=2, leading=L['body_size'] * 1.5)
        d.update(kw)
        return ParagraphStyle(name, **d)

    S = {
        'name'    : ps('name',    fontName=BFB, fontSize=L['name_size'],
                       textColor=BLACK, alignment=L['name_align'],
                       spaceAfter=3, leading=L['name_size'] * 1.3),
        'contact' : ps('contact', fontSize=L['body_size'] - 1,
                       textColor=DARK_GRAY, alignment=L['contact_align'],
                       spaceAfter=0, leading=14),
        'sec'     : ps('sec',     fontName=BFB, fontSize=L['sec_size'],
                       textColor=BLACK, spaceAfter=0,
                       leading=L['sec_size'] * 1.4, tracking=30),
        'body'    : ps('body',    alignment=TA_JUSTIFY, spaceAfter=4,
                       leading=L['body_size'] * 1.55),
        'val'     : ps('val',     spaceAfter=1),
        'role'    : ps('role',    fontName=BFB, fontSize=L['body_size'] + 1,
                       textColor=BLACK, spaceAfter=1,
                       leading=(L['body_size'] + 1) * 1.4),
        'employer': ps('employer',fontName=BFI, fontSize=L['body_size'],
                       textColor=DARK_GRAY, spaceAfter=1, leading=14),
        'dates'   : ps('dates',   fontSize=L['body_size'] - 1,
                       textColor=DARK_GRAY, spaceAfter=2, leading=13),
        'bullet'  : ps('bullet',  leftIndent=10, spaceAfter=3,
                       leading=L['body_size'] * 1.5),
        'qual_q'  : ps('qual_q',  fontName=BFB, fontSize=L['body_size'],
                       textColor=BLACK, spaceAfter=1),
        'qual_i'  : ps('qual_i',  fontName=BFI, fontSize=L['body_size'] - 1,
                       textColor=DARK_GRAY, spaceAfter=0, leading=13),
    }

    sp   = lambda n=3: Spacer(1, n * mm)
    thin = lambda: HRFlowable(width=PAGE_W, color=LT_GRAY, thickness=0.5, spaceAfter=2)

    def sec_heading(title):
        if theme_n == 0:
            return [Paragraph(title.upper(), S['sec']),
                    HRFlowable(width=PAGE_W, color=BLACK, thickness=0.8, spaceAfter=3)]
        elif theme_n == 1:
            return [HRFlowable(width=PAGE_W, color=BLACK, thickness=0.4, spaceAfter=2),
                    Paragraph(title.upper(), S['sec']),
                    HRFlowable(width=PAGE_W, color=BLACK, thickness=1.2, spaceAfter=4)]
        elif theme_n == 2:
            t = Table([[Paragraph(f'  {title.upper()}', S['sec'])]], colWidths=[PAGE_W])
            t.setStyle(TableStyle([
                ('LINEBEFORE',(0,0),(0,-1),3.5,BLACK),('LINEBELOW',(0,0),(-1,-1),0.4,LT_GRAY),
                ('TOPPADDING',(0,0),(-1,-1),3),('BOTTOMPADDING',(0,0),(-1,-1),4),
                ('LEFTPADDING',(0,0),(-1,-1),6)]))
            return [t]
        else:
            t = Table([[Paragraph(f'  {title.upper()}  ', S['sec'])]], colWidths=[PAGE_W])
            t.setStyle(TableStyle([
                ('BOX',(0,0),(-1,-1),0.8,BLACK),('TOPPADDING',(0,0),(-1,-1),4),
                ('BOTTOMPADDING',(0,0),(-1,-1),4),('LEFTPADDING',(0,0),(-1,-1),8)]))
            return [t]

    def bullet_p(text):
        clean = text.lstrip('- \u2022\t').strip()
        return Paragraph(f'\u2022\u2003{clean}', S['bullet']) if clean else None

    HEADINGS = [
        'PERSONAL DETAILS','PROFESSIONAL PROFILE','EDUCATION & QUALIFICATIONS',
        'PROFESSIONAL EXPERIENCE','TRAINING & CERTIFICATIONS','KEY SKILLS',
        'ADDITIONAL INFORMATION',
    ]
    sections = {}
    current  = '__pre__'
    sections[current] = []
    for line in cv_text.splitlines():
        matched = next((h for h in HEADINGS if line.strip().upper() == h), None)
        if matched:
            current = matched
            sections[current] = []
        else:
            sections.setdefault(current, []).append(line)

    s1_d      = doc.get('section_1_personal_details') or {}
    full_name = _v(s1_d.get('full_name')) or 'Candidate'
    mobile    = _v(s1_d.get('mobile_number'))
    email     = _v(doc.get('email'))
    address   = _v(s1_d.get('address'))

    logo_path = None
    for c in [
        os.path.join(os.path.dirname(__file__), '..', 'static', 'images', 'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'img', 'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'logo.png'),
        'static/images/logo.png', 'static/img/logo.png', 'static/logo.png',
    ]:
        if os.path.exists(c):
            logo_path = c
            break

    buf   = _io.BytesIO()
    story = []

    # Header
    if logo_path:
        logo_w   = 30*mm
        logo_img = RLImage(logo_path, width=logo_w, height=logo_w*94/316)
        if L['name_align'] == TA_CENTER:
            story.append(logo_img); story.append(sp(2))
        else:
            hdr_t = Table([[Paragraph('', S['body']), logo_img]],
                          colWidths=[PAGE_W - logo_w - 2*mm, logo_w + 2*mm])
            hdr_t.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),
                                       ('TOPPADDING',(0,0),(-1,-1),0),
                                       ('BOTTOMPADDING',(0,0),(-1,-1),0),
                                       ('LEFTPADDING',(0,0),(-1,-1),0),
                                       ('RIGHTPADDING',(0,0),(-1,-1),0)]))
            story.append(hdr_t)

    story.append(Paragraph(full_name, S['name']))
    contact_parts = [p for p in [mobile, email, address] if p]
    if contact_parts:
        sep = '   |   ' if L['contact_align'] == TA_CENTER else '  •  '
        story.append(Paragraph(sep.join(contact_parts), S['contact']))

    story.append(sp(3))
    if theme_n == 0:
        story.append(HRFlowable(width=PAGE_W,color=BLACK,thickness=1.5,spaceAfter=0))
        story.append(HRFlowable(width=PAGE_W,color=BLACK,thickness=0.4,spaceAfter=4))
    elif theme_n == 1:
        story.append(HRFlowable(width=PAGE_W,color=BLACK,thickness=1.2,spaceAfter=4))
    elif theme_n == 2:
        story.append(HRFlowable(width=PAGE_W,color=LT_GRAY,thickness=0.5,spaceAfter=4))
    else:
        story.append(HRFlowable(width=PAGE_W,color=BLACK,thickness=0.6,spaceAfter=2))
        story.append(HRFlowable(width=PAGE_W,color=BLACK,thickness=0.6,spaceAfter=4))
    story.append(sp(2))

    for heading in HEADINGS:
        lines = [l for l in sections.get(heading, []) if l.strip()]
        if not lines:
            continue
        story += sec_heading(heading)
        story.append(sp(2))

        if heading == 'PERSONAL DETAILS':
            for line in lines:
                if ':' in line:
                    parts = line.split(':', 1)
                    lbl_t = parts[0].strip() + ':'
                    val_t = parts[1].strip()
                    if val_t:
                        story.append(Paragraph(f'<b>{lbl_t}</b> {val_t}', S['val']))
                        story.append(sp(1))
            story.append(sp(3))

        elif heading == 'PROFESSIONAL PROFILE':
            pb = []
            for line in lines:
                if line.strip() == '':
                    if pb:
                        story.append(Paragraph(' '.join(pb), S['body']))
                        story.append(sp(2))
                        pb = []
                else:
                    pb.append(line.strip())
            if pb:
                story.append(Paragraph(' '.join(pb), S['body']))
            story.append(sp(4))

        elif heading == 'EDUCATION & QUALIFICATIONS':
            for line in lines:
                s = line.strip().lstrip('- ').strip()
                if not s:
                    continue
                parts = [p.strip() for p in s.split('|')]
                qual  = parts[0] if parts else s
                inst  = parts[1] if len(parts) > 1 else ''
                year  = parts[2] if len(parts) > 2 else ''
                yr_txt = f' ({year})' if year else ''
                story.append(Paragraph(f'<b>{qual}</b>{yr_txt}', S['qual_q']))
                if inst:
                    story.append(Paragraph(inst, S['qual_i']))
                story += [sp(2), thin(), sp(2)]
            story.append(sp(3))

        elif heading == 'PROFESSIONAL EXPERIENCE':
            roles = []
            cur   = []
            for line in lines:
                if line.strip().lower().startswith('job title:') and cur:
                    roles.append(cur); cur = [line]
                else:
                    cur.append(line)
            if cur:
                roles.append(cur)

            for ri, role_lines in enumerate(roles):
                job_title = emp_name = dates_str = ''
                duties    = []
                for rl in role_lines:
                    sl    = rl.strip()
                    sl_lo = sl.lower()
                    if not sl: continue
                    if sl_lo.startswith('job title:'):   job_title = sl.split(':',1)[1].strip()
                    elif sl_lo.startswith('employer:'):  emp_name  = sl.split(':',1)[1].strip()
                    elif sl_lo.startswith('dates:'):     dates_str = sl.split(':',1)[1].strip()
                    elif sl_lo.startswith('duties'):     pass
                    elif sl.startswith('-') or sl.startswith('\u2022'):
                        duties.append(sl.lstrip('- \u2022').strip())

                if not job_title and not emp_name:
                    continue

                if theme_n in (1, 3) and dates_str:
                    rt = Table([[Paragraph(f'<b>{job_title}</b>', S['role']),
                                 Paragraph(dates_str, S['dates'])]],
                               colWidths=[PAGE_W*0.65, PAGE_W*0.35])
                    rt.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'BOTTOM'),
                                            ('TOPPADDING',(0,0),(-1,-1),0),
                                            ('BOTTOMPADDING',(0,0),(-1,-1),2),
                                            ('LEFTPADDING',(0,0),(-1,-1),0),
                                            ('RIGHTPADDING',(0,0),(-1,-1),0),
                                            ('ALIGN',(1,0),(1,-1),'RIGHT')]))
                    story.append(rt)
                else:
                    story.append(Paragraph(job_title or 'Role', S['role']))
                    if dates_str:
                        story.append(Paragraph(dates_str, S['dates']))

                if emp_name:
                    story.append(Paragraph(emp_name, S['employer']))
                story.append(sp(2))
                for d in duties:
                    if d:
                        bp = bullet_p(d)
                        if bp: story.append(bp)
                story.append(sp(3))
                if ri < len(roles) - 1:
                    story.append(thin()); story.append(sp(2))
            story.append(sp(2))

        elif heading in ('TRAINING & CERTIFICATIONS', 'KEY SKILLS'):
            if theme_n in (0, 2):
                bitems = [bullet_p(l.strip()) for l in lines if bullet_p(l.strip())]
                pairs  = []
                for i in range(0, len(bitems), 2):
                    left  = bitems[i]
                    right = bitems[i+1] if i+1 < len(bitems) else Paragraph('', S['body'])
                    pairs.append([left, right])
                if pairs:
                    col_w = PAGE_W / 2 - 3*mm
                    bt = Table(pairs, colWidths=[col_w, col_w])
                    bt.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'),
                                            ('TOPPADDING',(0,0),(-1,-1),1),
                                            ('BOTTOMPADDING',(0,0),(-1,-1),1),
                                            ('LEFTPADDING',(0,0),(-1,-1),0),
                                            ('RIGHTPADDING',(0,0),(-1,-1),4)]))
                    story.append(bt)
            else:
                for line in lines:
                    bp = bullet_p(line.strip())
                    if bp: story.append(bp)
            story.append(sp(4))

        elif heading == 'ADDITIONAL INFORMATION':
            for line in lines:
                s = line.strip()
                if not s or s.lower().startswith('reference'): continue
                if ':' in s:
                    parts = s.split(':', 1)
                    story.append(Paragraph(
                        f'<b>{parts[0].strip()}:</b> {parts[1].strip()}', S['val']))
                    story.append(sp(1))
                else:
                    story.append(Paragraph(s, S['body']))
            story.append(sp(4))

    pdf_doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=L['lm'], rightMargin=L['rm'],
        topMargin=L['tm'],  bottomMargin=L['bm'],
    )
    pdf_doc.build(story)
    return buf.getvalue()



def _build_cv_pdf(doc):
    """
    Build a rich individual Xpress Health CV PDF.
    Mirrors the Abidemi Aluko CV structure:
      1. Personal Details
      2. Professional Profile  (auto-generated flowing paragraph)
      3. Education & Qualifications  (entry per qual)
      4. Professional Experience  (one card per role with full duties)
      5. Training & Certifications  (bullet list, max 6)
      6. Key Skills  (bullet list)
      7. Additional Information  (Driving / Transport / References / Date)
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table,
        TableStyle, HRFlowable, Image as RLImage, ListFlowable, ListItem
    )
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
    import io as _io, os

    # ── Brand palette ─────────────────────────────────────────────────
    NAVY      = colors.HexColor('#1B3A6B')
    XH_GREEN  = colors.HexColor('#2E9E44')
    LIGHT_BG  = colors.HexColor('#EFF6FF')
    STRIPE    = colors.HexColor('#F0FDF4')
    MID_GRAY  = colors.HexColor('#CBD5E1')
    TEXT_DARK = colors.HexColor('#1E293B')
    TEXT_GRAY = colors.HexColor('#475569')
    WHITE     = colors.white

    W, H   = A4
    PAGE_W = W - 30 * mm   # 15 mm margins each side

    # ── Styles ────────────────────────────────────────────────────────
    def ps(name, **kw):
        d = dict(fontName='Helvetica', fontSize=10, textColor=TEXT_GRAY,
                 spaceAfter=2, leading=15)
        d.update(kw)
        return ParagraphStyle(name, **d)

    S = {
        'cv_title'  : ps('cv_title',   fontName='Helvetica-Bold', fontSize=20,
                         textColor=WHITE, alignment=TA_CENTER, spaceAfter=0, leading=24),
        'cv_name'   : ps('cv_name',    fontName='Helvetica-Bold', fontSize=13,
                         textColor=NAVY, alignment=TA_CENTER, spaceAfter=0, leading=18),
        'sec_head'  : ps('sec_head',   fontName='Helvetica-Bold', fontSize=10,
                         textColor=WHITE, spaceAfter=0, leading=14),
        'lbl'       : ps('lbl',        fontName='Helvetica-Bold', fontSize=10,
                         textColor=NAVY, spaceAfter=0, leading=14),
        'val'       : ps('val',        fontSize=10, textColor=TEXT_GRAY,
                         spaceAfter=0, leading=14),
        'body'      : ps('body',       fontSize=10, textColor=TEXT_GRAY,
                         alignment=TA_JUSTIFY, spaceAfter=4, leading=15),
        'exp_title' : ps('exp_title',  fontName='Helvetica-Bold', fontSize=11,
                         textColor=NAVY, spaceAfter=0, leading=15),
        'exp_sub'   : ps('exp_sub',    fontName='Helvetica-Oblique', fontSize=10,
                         textColor=XH_GREEN, spaceAfter=0, leading=14),
        'exp_date'  : ps('exp_date',   fontName='Helvetica-Bold', fontSize=9,
                         textColor=WHITE, alignment=TA_CENTER, spaceAfter=0, leading=12),
        'duty'      : ps('duty',       fontSize=10, textColor=TEXT_GRAY,
                         leftIndent=8, spaceAfter=3, leading=15),
        'bullet'    : ps('bullet',     fontSize=10, textColor=TEXT_GRAY,
                         leftIndent=8, spaceAfter=3, leading=15),
        'footer'    : ps('footer',     fontSize=7,  textColor=MID_GRAY,
                         alignment=TA_CENTER, spaceAfter=0),
        'qual_title': ps('qual_title', fontName='Helvetica-Bold', fontSize=10,
                         textColor=NAVY, spaceAfter=1, leading=14),
        'qual_sub'  : ps('qual_sub',   fontName='Helvetica-Oblique', fontSize=9,
                         textColor=TEXT_GRAY, spaceAfter=0, leading=13),
    }

    sp = lambda n=3: Spacer(1, n * mm)

    # ── Helpers ───────────────────────────────────────────────────────
    def sec(title):
        t = Table([[Paragraph(title, S['sec_head'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), NAVY),
            ('TOPPADDING',    (0,0), (-1,-1), 6),
            ('BOTTOMPADDING', (0,0), (-1,-1), 6),
            ('LEFTPADDING',   (0,0), (-1,-1), 10),
            ('LINEBELOW',     (0,0), (-1,-1), 2, XH_GREEN),
        ]))
        return t

    def lv(label, value, lw=55*mm):
        val_text = value if value else '—'
        t = Table(
            [[Paragraph(label, S['lbl']), Paragraph(val_text, S['val'])]],
            colWidths=[lw, PAGE_W - lw]
        )
        t.setStyle(TableStyle([
            ('TOPPADDING',    (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ('LEFTPADDING',   (0,0), (0,0),   6),
            ('LEFTPADDING',   (1,0), (1,0),   4),
            ('LINEBELOW',     (0,0), (-1,-1), 0.3, MID_GRAY),
        ]))
        return t

    def date_badge(text):
        """Green pill badge for date range."""
        t = Table([[Paragraph(text, S['exp_date'])]], colWidths=[None])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), XH_GREEN),
            ('TOPPADDING',    (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ('LEFTPADDING',   (0,0), (-1,-1), 8),
            ('RIGHTPADDING',  (0,0), (-1,-1), 8),
            ('ROUNDEDCORNERS',(0,0), (-1,-1), 4),
        ]))
        return t

    def bullet_item(text):
        return Paragraph(f'\u2022\u2003{text}', S['bullet'])

    def duty_item(text):
        return Paragraph(f'\u2022\u2003{text}', S['duty'])

    def profile_box(text):
        t = Table([[Paragraph(text, S['body'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), LIGHT_BG),
            ('BOX',           (0,0), (-1,-1), 0.5, MID_GRAY),
            ('LINEBEFORE',    (0,0), (0,-1),  4,   XH_GREEN),
            ('TOPPADDING',    (0,0), (-1,-1), 10),
            ('BOTTOMPADDING', (0,0), (-1,-1), 10),
            ('LEFTPADDING',   (0,0), (-1,-1), 12),
            ('RIGHTPADDING',  (0,0), (-1,-1), 12),
        ]))
        return t

    # ── Data ─────────────────────────────────────────────────────────
    s1   = doc.get('section_1_personal_details') or {}
    s2   = doc.get('section_2_identity_verification') or {}
    s3   = doc.get('section_3_professional_registration') or {}
    s4   = doc.get('section_4_qualifications') or {}
    s5   = doc.get('section_5_employment_history') or {}
    s7   = doc.get('section_7_references') or {}
    s8   = doc.get('section_8_garda_vetting_police_clearance') or {}
    s9   = doc.get('section_9_occupational_health') or {}
    s10  = doc.get('section_10_mandatory_training') or {}
    s12  = doc.get('section_12_declaration') or {}
    visa     = s1.get('work_permit_visa_status') or {}
    docs_sub = s2.get('documents_submitted') or {}

    full_name  = _v(s1.get('full_name'))
    emp_code   = _v(doc.get('employee_code'))
    user_type  = _v(doc.get('user_type'))
    address    = _v(s1.get('address'))
    mobile     = _v(s1.get('mobile_number'))
    email      = _v(doc.get('email'))
    dob        = _v(s1.get('date_of_birth'))
    nationality= _v(s1.get('nationality'))
    reg_pin    = _v(s3.get('registration_number_pin'))
    reg_exp    = _v(s3.get('registration_expiry_date'))
    divisions  = ', '.join(s3.get('divisions_registered_in') or [])
    nmbi       = s3.get('nmbi_active_declaration')
    perm_work  = _v(visa.get('permission_to_work'))
    visa_type  = _v(visa.get('visa_type'))
    total_exp  = _v(s5.get('total_experience'))
    entries    = [e for e in (s5.get('entries') or [])
                  if e.get('employer') or e.get('position')]

    # ── Logo ─────────────────────────────────────────────────────────
    logo_path = None
    for c in [
        os.path.join(os.path.dirname(__file__), '..', 'static', 'images', 'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'img', 'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'logo.png'),
        'static/images/logo.png', 'static/img/logo.png', 'static/logo.png',
    ]:
        if os.path.exists(c):
            logo_path = c
            break

    # ── Build story ───────────────────────────────────────────────────
    buf   = _io.BytesIO()
    story = []

    # ════════════════════════════════════════════════════════════════
    # HEADER — Logo + "CURRICULUM VITAE" + candidate name
    # ════════════════════════════════════════════════════════════════
    title_rows = [
        [Paragraph('CURRICULUM VITAE', S['cv_title'])],
        [Paragraph(full_name or 'Candidate', S['cv_name'])],   # name under title
    ]
    title_w   = PAGE_W - (55*mm if logo_path else 0)
    title_tbl = Table(title_rows, colWidths=[title_w])
    title_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), NAVY),
        ('TOPPADDING',    (0,0), (-1,-1), 12),
        ('BOTTOMPADDING', (0,0), (-1,-1), 12),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        # name row slightly lighter bg
        ('BACKGROUND',    (0,1), (-1,1),  colors.HexColor('#162F58')),
        ('TOPPADDING',    (0,1), (-1,1),  6),
        ('BOTTOMPADDING', (0,1), (-1,1),  8),
    ]))

    if logo_path:
        logo_img  = RLImage(logo_path, width=45*mm, height=45*mm*94/316)
        logo_cell = Table([[logo_img]], colWidths=[55*mm])
        logo_cell.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), WHITE),
            ('TOPPADDING',    (0,0), (-1,-1), 6),
            ('BOTTOMPADDING', (0,0), (-1,-1), 6),
            ('LEFTPADDING',   (0,0), (-1,-1), 6),
            ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        ]))
        banner = Table([[logo_cell, title_tbl]],
                       colWidths=[55*mm, PAGE_W - 55*mm])
    else:
        banner = Table([[title_tbl]], colWidths=[PAGE_W])

    banner.setStyle(TableStyle([
        ('TOPPADDING',    (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
        ('LEFTPADDING',   (0,0), (-1,-1), 0),
        ('RIGHTPADDING',  (0,0), (-1,-1), 0),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        ('LINEBELOW',     (0,0), (-1,-1), 3, XH_GREEN),
    ]))
    story += [banner, sp(5)]

    # ════════════════════════════════════════════════════════════════
    # 1. PERSONAL DETAILS
    # ════════════════════════════════════════════════════════════════
    story += [sec('PERSONAL DETAILS'), sp(3)]
    for label, value in [
        ('Full Name:',     full_name),
        ('Address:',       address),
        ('Mobile Number:', mobile),
        ('Email Address:', email),
        ('Nationality:',   nationality),
    ]:
        if value:
            story += [lv(label, value), sp(1)]
    story.append(sp(4))

    # ════════════════════════════════════════════════════════════════
    # 2. PROFESSIONAL PROFILE — rich individual paragraph
    # ════════════════════════════════════════════════════════════════
    story += [sec('PROFESSIONAL PROFILE'), sp(3)]

    # Build a natural multi-sentence profile from the data
    para_sentences = []

    # Opening — who they are + experience + speciality
    if full_name and user_type:
        opener = f"{full_name} is a compassionate and dedicated {user_type}"
        if divisions:
            opener += f" specialising in {divisions}"
        if total_exp:
            opener += f", with {total_exp} of professional healthcare experience"
        para_sentences.append(opener)

    # Most recent role
    if entries:
        latest = entries[0]
        pos_l  = _v(latest.get('position'))
        emp_l  = _v(latest.get('employer'))
        d_from = _v(latest.get('from'))
        d_to   = _v(latest.get('to'))
        if pos_l and emp_l:
            role_s = f"Most recently working as {pos_l} at {emp_l}"
            if d_from:
                role_s += f" from {d_from}"
                role_s += f" to {d_to}" if d_to else " to present"
            para_sentences.append(role_s)

    # Registration / professional status
    if reg_pin or reg_exp or nmbi:
        reg_s = "Professionally registered"
        if reg_pin:
            reg_s += f" (PIN: {reg_pin})"
        if reg_exp:
            reg_s += f" with registration valid until {reg_exp}"
        if nmbi:
            reg_s += ", holding an active NMBI declaration"
        para_sentences.append(reg_s)

    # Work authorisation
    if perm_work == 'Yes' and visa_type:
        para_sentences.append(
            f"Fully authorised to work in Ireland ({visa_type})"
        )
    elif perm_work == 'Yes':
        para_sentences.append("Fully authorised to work in Ireland")

    # Occupational health
    if s9.get('fit_for_nursing_duties'):
        para_sentences.append(
            "Confirmed fit for nursing duties with up-to-date occupational health clearance"
        )

    # Garda vetting
    if s8.get('garda_vetting_submitted'):
        para_sentences.append("Garda vetted and cleared to work with vulnerable adults")

    # Qualities closing line
    qualities = (
        "Known for excellent communication, a caring and professional manner, "
        "and a genuine commitment to promoting client dignity, independence, and wellbeing"
    )
    para_sentences.append(qualities)

    profile_text = '. '.join(para_sentences) + '.'

    story.append(profile_box(profile_text))
    story.append(sp(4))

    # ════════════════════════════════════════════════════════════════
    # 3. EDUCATION & QUALIFICATIONS
    # ════════════════════════════════════════════════════════════════
    story += [sec('EDUCATION & QUALIFICATIONS'), sp(3)]

    qual_keys = ['nursing_degree', 'postgraduate_qualification', 'other_qualification']
    qual_found = False
    for qk in qual_keys:
        q     = s4.get(qk) or {}
        qname = _v(q.get('qualification'))
        qinst = _v(q.get('institution'))
        qyear = _v(q.get('year_completed'))
        if not (qname or qinst):
            continue
        qual_found = True

        # Heading row: qual name left, year right
        h_left  = Paragraph(f'<b>{qname}</b>' if qname else '<b>Qualification</b>',
                             S['qual_title'])
        h_right = Paragraph(qyear, S['exp_date'])
        yr_cell = Table([[h_right]], colWidths=[30*mm])
        yr_cell.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), XH_GREEN),
            ('TOPPADDING',    (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ('LEFTPADDING',   (0,0), (-1,-1), 6),
            ('RIGHTPADDING',  (0,0), (-1,-1), 6),
        ]))
        head_row = Table([[h_left, yr_cell]],
                         colWidths=[PAGE_W - 34*mm, 34*mm])
        head_row.setStyle(TableStyle([
            ('TOPPADDING',    (0,0), (-1,-1), 0),
            ('BOTTOMPADDING', (0,0), (-1,-1), 2),
            ('LEFTPADDING',   (0,0), (-1,-1), 0),
            ('RIGHTPADDING',  (0,0), (-1,-1), 0),
            ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        ]))
        story.append(head_row)
        if qinst:
            story.append(Paragraph(qinst, S['qual_sub']))
        story += [sp(2), HRFlowable(width=PAGE_W, color=MID_GRAY, thickness=0.4), sp(3)]

    if not qual_found:
        story.append(Paragraph('No qualifications recorded.', S['body']))
        story.append(sp(3))

    story.append(sp(1))

    # ════════════════════════════════════════════════════════════════
    # 4. PROFESSIONAL EXPERIENCE — one card per role with full duties
    # ════════════════════════════════════════════════════════════════
    story += [sec('PROFESSIONAL EXPERIENCE'), sp(3)]

    if entries:
        for i, e in enumerate(entries):
            pos     = _v(e.get('position'))
            emp     = _v(e.get('employer'))
            loc     = _v(e.get('location', ''))   # location field if present
            d_from  = _v(e.get('from'))
            d_to    = _v(e.get('to'))
            leaving = _v(e.get('reason_for_leaving'))

            # Date range string
            if d_from and d_to:
                date_str = f"{d_from} \u2013 {d_to}"
            elif d_from:
                date_str = f"{d_from} \u2013 Present"
            elif d_to:
                date_str = f"Until {d_to}"
            else:
                date_str = ''

            # ── Role heading: title left, date badge right ────────
            t_para  = Paragraph(f'<b>{pos}</b>' if pos else '<b>Role</b>', S['exp_title'])
            if date_str:
                d_badge = Table([[Paragraph(date_str, S['exp_date'])]],
                                colWidths=[None])
                d_badge.setStyle(TableStyle([
                    ('BACKGROUND',    (0,0), (-1,-1), XH_GREEN),
                    ('TOPPADDING',    (0,0), (-1,-1), 4),
                    ('BOTTOMPADDING', (0,0), (-1,-1), 4),
                    ('LEFTPADDING',   (0,0), (-1,-1), 10),
                    ('RIGHTPADDING',  (0,0), (-1,-1), 10),
                ]))
                head_t = Table([[t_para, d_badge]],
                               colWidths=[PAGE_W * 0.60, PAGE_W * 0.40])
            else:
                head_t = Table([[t_para]], colWidths=[PAGE_W])

            head_t.setStyle(TableStyle([
                ('TOPPADDING',    (0,0), (-1,-1), 0),
                ('BOTTOMPADDING', (0,0), (-1,-1), 2),
                ('LEFTPADDING',   (0,0), (-1,-1), 0),
                ('RIGHTPADDING',  (0,0), (-1,-1), 0),
                ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
            ]))
            story.append(head_t)

            # Employer + location sub-line
            sub_parts = []
            if emp: sub_parts.append(emp)
            if loc: sub_parts.append(loc)
            if sub_parts:
                story.append(Paragraph(' \u2022 '.join(sub_parts), S['exp_sub']))

            story.append(sp(2))

            # ── Description paragraph ─────────────────────────────
            # Build a rich descriptive paragraph for this role
            desc_parts = []
            if pos and emp:
                desc = f"Worked as <b>{pos}</b> at {emp}"
                if loc:
                    desc += f", based in {loc}"
                if d_from and d_to:
                    desc += f", from {d_from} to {d_to}"
                elif d_from:
                    desc += f" from {d_from} to present"
                desc_parts.append(desc)

            # Add responsibilities based on user_type keywords
            ut_lower = user_type.lower() if user_type else ''
            if 'nurse' in ut_lower or 'nursing' in ut_lower:
                role_duties = [
                    "Assisted residents and clients with all aspects of personal care including personal hygiene, dressing, and grooming",
                    "Supported safe mobility and transfers, assisting with walking, wheelchair use, and repositioning",
                    "Observed and reported changes in residents' condition — including skin integrity, pain, and behaviour — to the nursing team",
                    "Assisted with medication administration under the direct supervision of qualified nursing staff",
                    "Maintained accurate records and contributed to care planning in line with individual care plans",
                    "Built positive and respectful therapeutic relationships with residents and their families",
                    "Worked effectively within multidisciplinary teams, supporting a safe and caring environment",
                ]
            elif 'healthcare' in ut_lower or 'hca' in ut_lower or 'assistant' in ut_lower:
                role_duties = [
                    "Provided high-quality, person-centred care and support tailored to each individual client's needs",
                    "Assisted clients with all activities of daily living including personal care, meal preparation, and mobility support",
                    "Observed and reported changes in clients' physical or emotional wellbeing to the supervising care team",
                    "Promoted client independence, dignity, and choice throughout all aspects of care delivery",
                    "Maintained comprehensive and accurate care records in line with organisational policies",
                    "Collaborated effectively with colleagues, families, and multidisciplinary teams to ensure continuity of care",
                    "Followed safe working practices, infection control procedures, and moving and handling guidelines at all times",
                ]
            else:
                role_duties = [
                    "Delivered high standards of professional care and support in line with organisational policies and procedures",
                    "Maintained clear and accurate records and communicated effectively with the wider team",
                    "Promoted the dignity, independence, and wellbeing of all clients and residents at all times",
                ]

            if desc_parts:
                story.append(Paragraph('. '.join(desc_parts) + '.', S['body']))
                story.append(sp(2))

            # Duties heading
            story.append(Paragraph('<b>Duties &amp; Responsibilities</b>', S['lbl']))
            story.append(sp(1))
            for duty in role_duties:
                story.append(duty_item(duty))
            story.append(sp(2))

            if leaving:
                story.append(Paragraph(
                    f'<i>Reason for leaving: {leaving}</i>', S['qual_sub']
                ))
                story.append(sp(2))

            if i < len(entries) - 1:
                story.append(HRFlowable(width=PAGE_W, color=MID_GRAY, thickness=0.5))
                story.append(sp(3))

    else:
        story.append(Paragraph('No employment history recorded.', S['body']))
        story.append(sp(3))

    if total_exp:
        story += [sp(2), lv('Total Experience:', total_exp, lw=55*mm), sp(1)]
    story.append(sp(4))

    # ════════════════════════════════════════════════════════════════
    # 5. TRAINING & CERTIFICATIONS — bullet list, max 6
    # ════════════════════════════════════════════════════════════════
    story += [sec('TRAINING & CERTIFICATIONS'), sp(3)]
    TLABELS = {
        'manual_handling':              'Manual Handling',
        'cpr_bls':                      'CPR / Basic Life Support',
        'fire_safety':                  'Fire Safety',
        'infection_prevention_control': 'Infection Prevention & Control',
        'hand_hygiene':                 'Hand Hygiene',
        'safeguarding':                 'Safeguarding Vulnerable Adults',
        'children_first':               'Children First',
        'cyber_security':               'Cyber Security Awareness',
        'dignity_at_work':              'Dignity at Work',
        'open_disclosure':              'Open Disclosure',
        'mapa_pmav':                    'MAPA / PMAV (De-escalation)',
    }
    cert_labels = []
    for key, label in TLABELS.items():
        if s10.get(key):
            cert_labels.append(label)
        if len(cert_labels) == 6:
            break

    if cert_labels:
        for label in cert_labels:
            story.append(bullet_item(label))
        story.append(sp(4))
    else:
        story.append(Paragraph('No training certifications recorded.', S['body']))
        story.append(sp(4))

    # ════════════════════════════════════════════════════════════════
    # 6. KEY SKILLS — bullets from health/registration data
    # ════════════════════════════════════════════════════════════════
    story += [sec('KEY SKILLS'), sp(3)]
    ut_lower = user_type.lower() if user_type else ''
    if 'nurse' in ut_lower or 'nursing' in ut_lower:
        skills = [
            'Medication administration (under nursing supervision)',
            'Patient assessment and observation',
            'Personal and person-centred care',
            'Patient moving and handling / safe mobility support',
            'Communication and interpersonal skills',
            'Observation, monitoring, and reporting of patient condition',
            'Record keeping and report writing',
            'Teamwork and collaboration with multidisciplinary teams',
            'Compassion, empathy, and patience',
            'Promoting patient dignity and independence',
        ]
    else:
        skills = [
            'Person-centred care and support',
            'Assistance with all activities of daily living',
            'Patient moving and handling / safe mobility support',
            'Communication and interpersonal skills',
            'Observation, monitoring, and reporting of client condition',
            'Record keeping and report writing',
            'Teamwork and collaboration',
            'Compassion, empathy, and patience',
            'Promoting client dignity and independence',
        ]

    for skill in skills:
        story.append(bullet_item(skill))
    story.append(sp(4))

    # ════════════════════════════════════════════════════════════════
    # 7. ADDITIONAL INFORMATION
    # ════════════════════════════════════════════════════════════════
    story += [sec('ADDITIONAL INFORMATION'), sp(3)]
    # Generate a random date between 01 Jan 2024 and 31 Dec 2026
    import random as _rand
    from datetime import date as _date, timedelta as _td
    _d_start = _date(2024, 1, 1); _d_end = _date(2026, 12, 31)
    _rand_date = _d_start + _td(days=_rand.randint(0, (_d_end - _d_start).days))
    _cv_date   = _rand_date.strftime('%d %B %Y')
    for label, value in [
        ('Driving Licence:', 'No'),
        ('Own Transport:',   'No'),
    ]:
        story += [lv(label, value), sp(1)]
    story.append(sp(4))

    # ── Render ────────────────────────────────────────────────────────
    pdf_doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=10*mm,  bottomMargin=15*mm,
    )
    pdf_doc.build(story)
    return buf.getvalue()




@admin_bp.route('/live-staffs/add', methods=['POST'])
@admin_required
def live_staff_add():
    data = request.get_json()

    email = (data.get('email') or '').strip().lower()
    if not email:
        return jsonify({"success": False, "error": "Email is required"}), 400

    if _staffs_col().count_documents({"email": email}) > 0:
        return jsonify({"success": False, "error": f'Email "{email}" already exists'}), 400

    doc = _build_doc(data)
    doc["created_at"] = datetime.utcnow()

    try:
        _staffs_col().insert_one(doc)
        return jsonify({"success": True, "message": "Staff record created"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/edit', methods=['POST'])
@admin_required
def live_staff_edit():
    data     = request.get_json()
    staff_id = (data.get('staff_id') or '').strip()

    if not staff_id:
        return jsonify({"success": False, "error": "Missing staff_id"}), 400

    email = (data.get('email') or '').strip().lower()
    if not email:
        return jsonify({"success": False, "error": "Email is required"}), 400

    if _staffs_col().count_documents({"email": email, "_id": {"$ne": ObjectId(staff_id)}}) > 0:
        return jsonify({"success": False, "error": f'Email "{email}" already exists'}), 400

    try:
        col     = _staffs_col()
        current = col.find_one({"_id": ObjectId(staff_id)})
        if not current:
            return jsonify({"success": False, "error": "Staff record not found"}), 404

        doc = _build_doc(data)
        col.update_one({"_id": ObjectId(staff_id)}, {"$set": doc})
        return jsonify({"success": True, "message": "Staff record updated"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/delete', methods=['POST'])
@admin_required
def live_staff_delete():
    data     = request.get_json()
    staff_id = (data.get('staff_id') or '').strip()

    if not staff_id:
        return jsonify({"success": False, "error": "Missing staff_id"}), 400
    try:
        _staffs_col().delete_one({"_id": ObjectId(staff_id)})
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── Import ────────────────────────────────────────────────────────────

@admin_bp.route('/live-staffs/import', methods=['POST'])
@admin_required
def live_staff_import():
    """Accept a JSON file upload; upsert on email. Handles all JSON variants."""
    if 'file' not in request.files:
        return jsonify({"success": False, "error": "No file provided"}), 400

    file = request.files['file']
    if not file.filename.endswith('.json'):
        return jsonify({"success": False, "error": "Only .json files are accepted"}), 400

    try:
        content = file.read().decode('utf-8')
        records = _parse_json_content(content)
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not parse file: {e}"}), 400

    inserted = updated = skipped = 0
    errors   = []

    for idx, rec in enumerate(records):
        email = (rec.get('email') or '').strip().lower()
        if not email:
            skipped += 1
            continue
        try:
            doc = _map_import_record(rec)
            result = _staffs_col().update_one(
                {"email": email},
                {"$set": doc, "$setOnInsert": {"created_at": datetime.utcnow()}},
                upsert=True
            )
            if result.upserted_id:
                inserted += 1
            else:
                updated += 1
        except Exception as e:
            errors.append(f"Row {idx + 1} ({email}): {e}")

    return jsonify({
        "success": True,
        "inserted": inserted,
        "updated":  updated,
        "skipped":  skipped,
        "errors":   errors,
        "message":  f"Import complete — {inserted} added, {updated} updated, {skipped} skipped"
    })


# ── Export ────────────────────────────────────────────────────────────

@admin_bp.route('/live-staffs/export')
@admin_required
def live_staff_export():
    fmt   = request.args.get('format', 'json').lower()
    items = list(_staffs_col().find({}))

    if fmt == 'csv':
        return _export_csv(items)
    return _export_json(items)



# ── Cron: Sync document list from XN Portal ───────────────────────────

@admin_bp.route('/live-staffs/cron/sync-documents', methods=['GET', 'POST'])
def live_staff_cron_sync_documents():
    """
    Cron job — processes ONE staff member per call.

    Logic:
      1. Find the first live_staffs record where extracted_cv is missing/empty.
      2. Call the XN Portal API to get their document list.
      3. If a document_type_name == "Cv" has a URL, download + extract text via Gemini.
      4. Save documents[] and extracted_cv back to MongoDB.
      5. Return result with how many staff still need processing (remaining_count).

    Call every N minutes via cron — it will work through all staff automatically,
    one at a time, until everyone has an extracted_cv.

    Protect with ?cron_key=<CRON_SECRET> env var.
    """
    import requests as _req

    # ── Auth ──────────────────────────────────────────────────────────
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    # ── Env ───────────────────────────────────────────────────────────
    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')

    if not base_url:
        return jsonify({"success": False,
                        "error": "LIVE_STAFF_URL not set in environment"}), 500

    endpoint = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    col = _staffs_col()

    # ── Find next staff without extracted_cv ──────────────────────────
    staff = col.find_one(
        {"$or": [
            {"extracted_cv": {"$exists": False}},
            {"extracted_cv": None},
            {"extracted_cv": ""},
        ]},
        {"email": 1, "section_1_personal_details": 1}
    )

    # Count how many still need processing (including this one)
    remaining_total = col.count_documents(
        {"$or": [
            {"extracted_cv": {"$exists": False}},
            {"extracted_cv": None},
            {"extracted_cv": ""},
        ]}
    )

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff already have extracted_cv — nothing to do.",
            "remaining_count": 0,
            "processed":       None,
        })

    email = _v(
        (staff.get('section_1_personal_details') or {}).get('email_address') or
        staff.get('email') or ''
    )

    if not email:
        # Mark as attempted so it doesn't block the queue forever
        col.update_one(
            {"_id": staff["_id"]},
            {"$set": {"extracted_cv": "[skipped — no email]",
                      "extracted_cv_at": datetime.utcnow()}}
        )
        return jsonify({
            "success":         True,
            "message":         "Skipped — staff record has no email address.",
            "remaining_count": remaining_total - 1,
            "processed":       str(staff.get("_id")),
        })

    # ── Call XN Portal API ────────────────────────────────────────────
    status_code   = None
    response_text = ''
    try:
        resp = _req.post(
            endpoint,
            json={"email": email},
            headers=api_headers,
            timeout=30
        )
        # If POST not allowed, retry with GET + query param
        if resp.status_code == 405:
            resp = _req.get(
                endpoint,
                params={"email": email},
                headers=api_headers,
                timeout=30
            )
        status_code   = resp.status_code
        response_text = resp.text[:500] if resp.text else ''
        resp.raise_for_status()
        data = resp.json()
    except _req.exceptions.ConnectionError as api_err:
        return jsonify({
            "success":         False,
            "email":           email,
            "error":           f"Connection error — cannot reach XN Portal",
            "detail":          str(api_err)[:300],
            "endpoint":        endpoint,
            "check":           "Verify LIVE_STAFF_URL env var is correct and server is reachable",
            "remaining_count": remaining_total,
        })
    except _req.exceptions.Timeout:
        return jsonify({
            "success":         False,
            "email":           email,
            "error":           "Timeout — XN Portal did not respond within 30s",
            "endpoint":        endpoint,
            "remaining_count": remaining_total,
        })
    except _req.exceptions.HTTPError as api_err:
        return jsonify({
            "success":         False,
            "email":           email,
            "error":           f"HTTP {status_code} error from XN Portal",
            "response_body":   response_text,
            "endpoint":        endpoint,
            "remaining_count": remaining_total,
        })
    except Exception as api_err:
        return jsonify({
            "success":         False,
            "email":           email,
            "error":           f"{type(api_err).__name__}: {api_err}",
            "endpoint":        endpoint,
            "remaining_count": remaining_total,
        })

    if not data.get('success'):
        # Mark attempted so cron moves on next time
        col.update_one(
            {"_id": staff["_id"]},
            {"$set": {"extracted_cv": f"[API error: {data.get('message', 'unknown')}]",
                      "extracted_cv_at": datetime.utcnow()}}
        )
        return jsonify({
            "success":         False,
            "email":           email,
            "error":           data.get('message', 'API returned success=false'),
            "api_raw":         data,
            "remaining_count": remaining_total - 1,
        })

    api_data = data.get('data')

    # API may return data as a list (empty []) or a dict with documents key
    if isinstance(api_data, list):
        documents = api_data          # data itself is the document list
    elif isinstance(api_data, dict):
        documents = api_data.get('documents') or []
    else:
        documents = []

    # No documents found — save "No doc found" and move on
    if not documents:
        col.update_one(
            {"_id": staff["_id"]},
            {"$set": {
                "extracted_cv":    "No doc found",
                "extracted_cv_at": datetime.utcnow(),
            }}
        )
        return jsonify({
            "success":         True,
            "email":           email,
            "documents_found": 0,
            "cv_extracted":    False,
            "extracted_cv":    "No doc found",
            "remaining_count": max(0, remaining_total - 1),
            "message":         f"No documents returned by API for {email} — marked as 'No doc found'.",
        })

    # ── Process documents ─────────────────────────────────────────────
    extracted_cv_text = None
    cv_error          = None
    cv_url_found      = None

    for doc_item in documents:
        # Extract only when document_type_name is exactly "Cv"
        doc_name = (doc_item.get('document_type_name') or '').strip()
        doc_url  = doc_item.get('url') or ''

        if doc_name == 'Cv' and doc_url and not extracted_cv_text:
            cv_url_found = doc_url
            try:
                extracted_cv_text = _extract_text_from_url(doc_url, api_headers)
            except Exception as cv_err:
                cv_error = str(cv_err)
                extracted_cv_text = f"[CV extraction failed: {cv_err}]"

    # ── Save only extracted_cv to MongoDB ─────────────────────────────
    col.update_one(
        {"_id": staff["_id"]},
        {"$set": {
            "extracted_cv":      extracted_cv_text or "[no CV document found]",
            "extracted_cv_at":   datetime.utcnow(),
        }}
    )

    return jsonify({
        "success":         True,
        "email":           email,
        "cv_extracted":    bool(extracted_cv_text and not cv_error),
        "cv_url_found":    cv_url_found,
        "cv_error":        cv_error,
        "remaining_count": max(0, remaining_total - 1),
        "message": (
            f"Processed {email} — "
            f"{max(0, remaining_total - 1)} staff still need extraction."
        ),
        "synced_at": datetime.utcnow().isoformat(),
    })


def _extract_text_from_url(url, headers=None):
    """
    Download a CV document from URL, then use Gemini AI to extract
    and structure the full text content.

    Strategy:
      1. Download the file (PDF or DOCX).
      2. Get raw text via pdfplumber / python-docx as a pre-extraction step.
      3. Send that raw text to Gemini 2.5 Flash to clean, structure,
         and return a well-formatted plain-text CV extraction.
      4. If Gemini is unavailable, fall back to raw text only.
    """
    import requests as _req
    import io as _io

    # ── Download file ─────────────────────────────────────────────────
    dl_headers = dict(headers or {})
    dl_headers.pop('Content-Type', None)

    resp = _req.get(url, headers=dl_headers, timeout=60)
    resp.raise_for_status()

    content_type = resp.headers.get('Content-Type', '').lower()
    raw          = resp.content
    url_lower    = url.lower().split('?')[0]

    # ── Step 1: raw text extraction ───────────────────────────────────
    raw_text = ''

    # PDF
    if 'pdf' in content_type or url_lower.endswith('.pdf'):
        try:
            import pdfplumber
            with pdfplumber.open(_io.BytesIO(raw)) as pdf:
                raw_text = chr(10).join( page.extract_text() or '' for page in pdf.pages ).strip()
        except Exception:
            try:
                import PyPDF2
                reader   = PyPDF2.PdfReader(_io.BytesIO(raw))
                raw_text = chr(10).join( page.extract_text() or '' for page in reader.pages ).strip()
            except Exception:
                pass

    # DOCX
    elif ('wordprocessingml' in content_type or
          url_lower.endswith('.docx') or url_lower.endswith('.doc')):
        try:
            from docx import Document as _DocxDoc
            d        = _DocxDoc(_io.BytesIO(raw))
            raw_text = chr(10).join(p.text for p in d.paragraphs).strip()
        except Exception:
            pass

    # Plain text
    elif 'text' in content_type:
        raw_text = raw.decode('utf-8', errors='replace').strip()

    # Last resort — try PDF
    if not raw_text:
        try:
            import pdfplumber
            with pdfplumber.open(_io.BytesIO(raw)) as pdf:
                raw_text = chr(10).join( page.extract_text() or '' for page in pdf.pages ).strip()
        except Exception:
            pass

    if not raw_text:
        raise RuntimeError(
            f"Could not extract any text from document (content-type: {content_type})"
        )

    # ── Step 2: Gemini AI extraction & structuring ────────────────────
    gemini_key = os.environ.get('GEMINI_API_KEY', '')
    if not gemini_key:
        # No Gemini key — return raw text as-is
        return raw_text

    try:
        from google import genai as _genai
        client = _genai.Client(api_key=gemini_key)

        prompt = f"""You are a professional CV parser.

The text below was extracted from a candidate's CV document (PDF or DOCX).
The text may be messy, have formatting issues, or be partially garbled from extraction.

Your task:
1. Read the raw extracted text carefully.
2. Identify and structure all CV content into clean, readable plain text.
3. Preserve ALL factual information exactly as stated — do NOT add, invent, or change any facts.
4. Format it with clear section headings (PERSONAL DETAILS, PROFESSIONAL PROFILE, EDUCATION & QUALIFICATIONS, PROFESSIONAL EXPERIENCE, TRAINING & CERTIFICATIONS, KEY SKILLS, ADDITIONAL INFORMATION) where the content exists.
5. If a section's content is not present in the raw text, omit that section entirely.
6. Return ONLY the clean structured CV text — no preamble, no commentary.

RAW EXTRACTED TEXT:
{raw_text[:12000]}
"""

        response   = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        gemini_out = (response.text or '').strip()

        # Return Gemini-structured text; fall back to raw if empty
        return gemini_out if gemini_out else raw_text

    except Exception as gemini_err:
        # Gemini failed — return raw text with a note
        return f"[Gemini extraction failed: {gemini_err}]\n\n{raw_text}"


def _export_json(items):
    serialized = _serialize(items)
    payload    = json.dumps({"records": serialized}, indent=2, ensure_ascii=False)
    return Response(
        payload,
        mimetype='application/json',
        headers={"Content-Disposition": f'attachment; filename="live_staffs_{_now_slug()}.json"'}
    )


def _export_csv(items):
    flat_rows = [_flatten(_serialize(doc)) for doc in items]
    all_keys  = _ordered_keys(flat_rows)

    buf    = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=all_keys, extrasaction='ignore')
    writer.writeheader()
    for row in flat_rows:
        writer.writerow({k: row.get(k, '') for k in all_keys})

    return Response(
        buf.getvalue(),
        mimetype='text/csv',
        headers={"Content-Disposition": f'attachment; filename="live_staffs_{_now_slug()}.csv"'}
    )


# ── Internal builders ─────────────────────────────────────────────────

def _build_doc(data):
    """Build a MongoDB document from flat form POST data."""
    return {
        "recruitment_id": data.get('recruitment_id'),
        "email":          (data.get('email') or '').strip().lower(),
        "employee_code":  (data.get('employee_code') or '').strip(),
        "user_type":      (data.get('user_type') or '').strip(),
        "status":         (data.get('status') or 'active').strip(),

        "section_1_personal_details": {
            "full_name":      data.get('full_name', ''),
            "previous_names": data.get('previous_names', ''),
            "date_of_birth":  data.get('date_of_birth', ''),
            "address":        data.get('address', ''),
            "eircode_postcode": data.get('eircode_postcode', ''),
            "mobile_number":  data.get('mobile_number', ''),
            "email_address":  (data.get('email') or '').strip().lower(),
            "pps_number":     data.get('pps_number', ''),
            "nationality":    data.get('nationality', ''),
            "work_permit_visa_status": {
                "permission_to_work": data.get('permission_to_work', ''),
                "visa_type":          data.get('visa_type', ''),
            },
            "nmbi_pin_number": data.get('nmbi_pin_number', ''),
        },

        "section_2_identity_verification": {
            "passport_number":        data.get('passport_number', ''),
            "expiry_date":            data.get('passport_expiry', ''),
            "driving_licence_number": data.get('driving_licence_number', ''),
            "documents_submitted": {
                "passport":          bool(data.get('doc_passport')),
                "birth_certificate": bool(data.get('doc_birth_cert')),
                "driving_licence":   bool(data.get('doc_driving')),
                "proof_of_address":  bool(data.get('doc_address')),
            },
            "verified_by":       data.get('verified_by', ''),
            "verification_date": data.get('verification_date', ''),
        },

        "section_3_professional_registration": {
            "registration_number_pin":  data.get('registration_number_pin', ''),
            "divisions_registered_in":  data.get('divisions_registered_in', []),
            "registration_expiry_date": data.get('registration_expiry_date', ''),
            "nmbi_active_declaration":  bool(data.get('nmbi_active_declaration')),
        },

        "section_8_garda_vetting_police_clearance": {
            "garda_vetting_submitted":    bool(data.get('garda_vetting_submitted')),
            "police_clearance_submitted": bool(data.get('police_clearance_submitted')),
        },

        "section_9_occupational_health": {
            "occupational_health_screening": bool(data.get('occupational_health_screening')),
            "immunisation_records_provided": bool(data.get('immunisation_records_provided')),
            "fit_for_nursing_duties":        bool(data.get('fit_for_nursing_duties')),
            "covid_19_vaccine":              data.get('covid_19_vaccine', ''),
            "tuberculosis_vaccine":          data.get('tuberculosis_vaccine', ''),
            "hepatitis_antibody":            data.get('hepatitis_antibody', ''),
            "mmr_vaccine":                   data.get('mmr_vaccine', ''),
        },

        "section_10_mandatory_training": {
            "manual_handling":              data.get('manual_handling', ''),
            "cpr_bls":                      data.get('cpr_bls', ''),
            "fire_safety":                  data.get('fire_safety', ''),
            "infection_prevention_control": data.get('infection_prevention_control', ''),
            "hand_hygiene":                 data.get('hand_hygiene', ''),
            "safeguarding":                 data.get('safeguarding', ''),
            "children_first":               data.get('children_first', ''),
            "cyber_security":               data.get('cyber_security', ''),
            "dignity_at_work":              data.get('dignity_at_work', ''),
            "open_disclosure":              data.get('open_disclosure', ''),
            "mapa_pmav":                    data.get('mapa_pmav', ''),
        },
    }


def _map_import_record(rec):
    """Map a full JSON record into a clean MongoDB doc."""
    s1  = rec.get('section_1_personal_details', {})
    s2  = rec.get('section_2_identity_verification', {})
    s3  = rec.get('section_3_professional_registration', {})
    s4  = rec.get('section_4_qualifications', {})
    s5  = rec.get('section_5_employment_history', {})
    s6  = rec.get('section_6_employment_gaps', [])
    s7  = rec.get('section_7_references', {})
    s8  = rec.get('section_8_garda_vetting_police_clearance', {})
    s9  = rec.get('section_9_occupational_health', {})
    s10 = rec.get('section_10_mandatory_training', {})
    s11 = rec.get('section_11_criminal_convictions_declaration', {})
    s12 = rec.get('section_12_declaration', {})

    return {
        "recruitment_id": rec.get('recruitment_id'),
        "email":          (rec.get('email') or '').strip().lower(),
        "employee_code":  rec.get('employee_code', ''),
        "user_type":      rec.get('user_type', ''),
        "status":         rec.get('status', 'found'),

        "section_1_personal_details":                  s1,
        "section_2_identity_verification":             s2,
        "section_3_professional_registration":         s3,
        "section_4_qualifications":                    s4,
        "section_5_employment_history":                s5,
        "section_6_employment_gaps":                   s6,
        "section_7_references":                        s7,
        "section_8_garda_vetting_police_clearance":    s8,
        "section_9_occupational_health":               s9,
        "section_10_mandatory_training":               s10,
        "section_11_criminal_convictions_declaration": s11,
        "section_12_declaration":                      s12,
    }


# ── Flatten helpers for CSV export ───────────────────────────────────

def _flatten(doc, prefix='', result=None):
    if result is None:
        result = {}
    for k, v in doc.items():
        key = f"{prefix}{k}" if prefix else k
        if isinstance(v, dict):
            _flatten(v, key + '.', result)
        elif isinstance(v, list):
            result[key] = '; '.join(
                json.dumps(i) if isinstance(i, dict) else str(i)
                for i in v
            )
        else:
            result[key] = v if v is not None else ''
    return result


_PREFERRED_KEY_ORDER = [
    '_id', 'recruitment_id', 'employee_code', 'email', 'user_type', 'status', 'created_at'
]


def _ordered_keys(rows):
    seen    = set()
    ordered = []
    for k in _PREFERRED_KEY_ORDER:
        for row in rows:
            if k in row and k not in seen:
                ordered.append(k)
                seen.add(k)
                break
    for row in rows:
        for k in row:
            if k not in seen:
                ordered.append(k)
                seen.add(k)
    return ordered


def _now_slug():
    return datetime.utcnow().strftime('%Y%m%d_%H%M%S')
