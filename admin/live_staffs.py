from flask import render_template, request, jsonify, Response
from bson import ObjectId
from datetime import datetime
import json
import json as _cjson
import json as _cjson2
import base64
import csv
import io
import re
import re as _re
import os
import threading

# ── Module-level helpers ─────────────────────────────────────────────
def _v(val):
    """Strip and stringify — safe for None."""
    if val is None: return ''
    return str(val).strip()


# ── PCC constants ─────────────────────────────────────────────────────
_PCC_REVIEWERS = [
    'Letty Mathew',
    'Valencia Da Silva',
    'Ann Maria',
    'Audrey Maguire',
    'Liberata Gama',
]
_PCC_COMPLIANCE_OFFICER = 'Betsy Daniel'


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


@admin_bp.route('/live-staffs/edit-extracted-cv', methods=['POST'])
@admin_required
def live_staff_edit_extracted_cv():
    """
    Save edited extracted_cv text for a staff member.
    Also resets experience_analysed_at so AI analysis re-runs on next cron call.

    POST /admin/live-staffs/edit-extracted-cv
    Body: {"staff_id": "...", "extracted_cv": "..."}
    """
    data      = request.get_json() or {}
    staff_id  = (data.get('staff_id') or '').strip()
    new_text  = data.get('extracted_cv', '')
    user_type = (data.get('user_type') or '').strip()

    if not staff_id:
        return jsonify({"success": False, "error": "Missing staff_id"}), 400
    try:
        update_fields = {
            "extracted_cv":               new_text,
            "extracted_cv_at":            datetime.utcnow(),
            "extracted_cv_edited":        True,
            # Reset AI analysis so it re-runs with updated text/type
            "experience_analysed_at":     None,
            "experience_list_at":         None,
            "experience_list_processing": False,
        }
        if user_type:
            update_fields["user_type"] = user_type

        result = _staffs_col().update_one(
            {"_id": ObjectId(staff_id)},
            {"$set": update_fields}
        )
        if result.matched_count == 0:
            return jsonify({"success": False, "error": "Staff not found"}), 404
        return jsonify({"success": True, "message": "Changes saved successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@admin_bp.route('/live-staffs/points')
@admin_required
def live_staff_points():
    """
    Get points for a staff member by email address.

    GET /admin/live-staffs/points?email=someone@example.com

    Returns:
      {
        "success": true,
        "email": "someone@example.com",
        "name": "Sherin Augustine",
        "points": 8
      }
    """
    email = (request.args.get('email') or '').strip().lower()
    if not email:
        return jsonify({"success": False, "error": "Missing email parameter"}), 400
    try:
        doc = _staffs_col().find_one(
            {"email": email},
            {"points": 1, "section_1_personal_details": 1, "email": 1}
        )
        if not doc:
            return jsonify({"success": False, "error": f"No staff found with email: {email}"}), 404

        s1   = doc.get('section_1_personal_details') or {}
        name = _v(s1.get('full_name') or '')

        return jsonify({
            "success": True,
            "email":   email,
            "name":    name,
            "points":  doc.get('points'),
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@admin_bp.route('/live-staffs/experience', methods=['GET', 'POST'])
def live_staff_experience():
    """
    Analyse a staff member's extracted_cv using Gemini AI and return
    total years and months of experience as digits.

    Accepts X-API-Key header (no session required) OR admin session cookie.

    GET  /admin/live-staffs/experience?email=someone@example.com
    POST /admin/live-staffs/experience  body: {"staff_id": "68abc..."} or {"email": "..."}

    Response:
      {
        "success": true,
        "staff_name": "Jane Smith",
        "email": "jane@example.com",
        "years": 4,
        "months": 6,
        "total_months": 54,
        "summary": "4 years and 6 months",
        "source": "extracted_cv"   // or "section_5" if CV text unavailable
      }
    """
    # Accept either API key or admin session
    api_key_provided = request.headers.get('X-API-Key', '').strip()
    if api_key_provided:
        ok, err = _validate_api_key()
        if not ok:
            return jsonify({"success": False, "error": err}), 401
    else:
        from admin.views import admin_required as _admin_required
        from flask import session, redirect, url_for
        if not session.get('admin_logged_in'):
            return jsonify({"success": False,
                            "error": "Unauthorised — provide X-API-Key header or login"}), 401

    gemini_key = os.environ.get('GEMINI_API_KEY', '')
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set on server"}), 500

    # ── Resolve staff record ──────────────────────────────────────────
    if request.method == 'POST':
        data     = request.get_json(silent=True) or {}
        staff_id = (data.get('staff_id') or '').strip()
        email    = (data.get('email') or '').strip().lower()
    else:
        staff_id = (request.args.get('id') or '').strip()
        email    = (request.args.get('email') or '').strip().lower()

    if staff_id:
        # 1. Try live_staffs.staff_id field (XN Portal ID)
        doc = _staffs_col().find_one({"staff_id": staff_id})
        # 2. Try live_staffs.xn_staff_id field
        if not doc:
            doc = _staffs_col().find_one({"xn_staff_id": staff_id})
        # 3. Try MongoDB _id
        if not doc:
            try:
                doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
            except Exception:
                pass
    elif email:
        # Search email in top-level field and section_1 sub-field
        doc = _staffs_col().find_one({"$or": [
            {"email": email},
            {"section_1_personal_details.email_address": email},
        ]})
    else:
        return jsonify({"success": False,
                        "error": "Provide staff_id or email (query param or JSON body)"}), 400

    if not doc:
        return jsonify({"success": False, "error": "Staff record not found"}), 404

    s1        = doc.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email_out = _v(doc.get('email') or s1.get('email_address') or email)
    user_type = _v(doc.get('user_type') or '')

    # ── Return cached result if already analysed ─────────────────────
    if doc.get('experience_analysed_at') and doc.get('experience_years') is not None:
        years        = int(doc.get('experience_years') or 0)
        months       = int(doc.get('experience_months') or 0)
        total_months = int(doc.get('experience_total_months') or 0)
        if total_months == 0:
            total_months = years * 12 + months
        parts   = []
        if years:  parts.append(f"{years} year{'s' if years != 1 else ''}")
        if months: parts.append(f"{months} month{'s' if months != 1 else ''}")
        summary = ' and '.join(parts) if parts else '0 months'
        return jsonify({
            "success":      True,
            "staff_name":   full_name,
            "email":        email_out,
            "years":        years,
            "months":       months,
            "total_months": total_months,
            "summary":      summary,
            "note":         _v(doc.get('experience_note') or ''),
            "source":       _v(doc.get('experience_source') or 'cached'),
            "cached":       True,
        })

    # ── Determine text to analyse ─────────────────────────────────────
    extracted_cv = _v(doc.get('extracted_cv') or '')
    has_cv_text  = (
        extracted_cv and
        not extracted_cv.startswith('[') and
        extracted_cv != 'No doc found'
    )

    # Fallback: use section_5 employment history total_experience string
    s5           = doc.get('section_5_employment_history') or {}
    total_exp_db = _v(s5.get('total_experience') or '')

    if not has_cv_text and not total_exp_db:
        return jsonify({
            "success":      True,
            "staff_name":   full_name,
            "email":        email_out,
            "years":        None,
            "months":       None,
            "total_months": None,
            "summary":      "No experience data available",
            "source":       "none",
        })

    # ── Call Gemini ───────────────────────────────────────────────────
    try:
        from google import genai as google_genai

        # Determine role filter based on user_type
        ut_lower = (user_type or '').lower()
        if 'nurse' in ut_lower or 'nursing' in ut_lower:
            role_rule = (
                "IMPORTANT — Count ONLY nursing-related work experience, regardless of the country where it was gained. "
                "This includes: Registered Nurse, Staff Nurse, Clinical Nurse, ICU Nurse, Theatre Nurse, "
                "Community Nurse, Mental Health Nurse, Nursing Home Nurse, or any role with Nurse or Nursing in the title. "
                "DO NOT count non-nursing roles such as healthcare assistant, carer, support worker, admin, or any other role."
            )
        elif 'hca' in ut_lower or 'healthcare assistant' in ut_lower or 'health care assistant' in ut_lower:
            role_rule = (
                "IMPORTANT — Count ONLY Healthcare Assistant (HCA) work experience, regardless of the country where it was gained. "
                "This includes: Healthcare Assistant, HCA, Care Assistant, Care Worker, Support Worker in a clinical/care setting, "
                "or any role with Healthcare Assistant or HCA in the title. "
                "DO NOT count nursing roles (Registered Nurse, Staff Nurse, etc.) or non-care roles such as admin, retail, or hospitality."
            )
        else:
            role_rule = (
                "Count only direct healthcare or care-related work experience, regardless of country. "
                "Exclude non-healthcare roles such as admin, retail, hospitality, or general support roles "
                "unless they are clearly in a clinical or care setting."
            )

        if has_cv_text:
            source = 'extracted_cv'
            prompt = f"""You are a professional CV analyser specialising in Irish healthcare staffing.

Candidate role: {user_type}

Read the CV text below and calculate the candidate's TOTAL relevant work experience.

{role_rule}

Calculation Rules:
- Only count roles that match the role filter above.
- Experience gained in ANY country counts — not just Ireland.
- If a matching role has no end date, assume it is still ongoing (use today's date to calculate).
- If two matching roles overlap in time, count the overlapping period only once.
- Ignore all non-matching roles entirely — do not add them.
- Return ONLY a JSON object with these exact keys — nothing else, no markdown, no explanation:
  {{"years": <integer>, "months": <integer 0-11>, "total_months": <integer>, "note": "<one sentence summary of which roles were counted and why>"}}

CV TEXT:
{extracted_cv[:10000]}
"""
        else:
            source = 'section_5'
            prompt = f"""You are a professional CV analyser specialising in Irish healthcare staffing.

Candidate role: {user_type}

The candidate's total experience is described as: "{total_exp_db}"

{role_rule}

Extract the relevant years and months from this description, applying the role filter above.
Return ONLY a JSON object — nothing else, no markdown:
{{"years": <integer>, "months": <integer 0-11>, "total_months": <integer>, "note": "<one sentence summary>"}}
"""

        client   = google_genai.Client(api_key=gemini_key)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        raw = (response.text or '').strip()

        # Strip markdown code fences if Gemini wraps it
        raw = _re.sub(r'^```(?:json)?\s*', '', raw, flags=_re.MULTILINE)
        raw = _re.sub(r'```\s*$', '', raw, flags=_re.MULTILINE).strip()

        result = _json.loads(raw)

        years        = int(result.get('years', 0) or 0)
        months       = int(result.get('months', 0) or 0)
        total_months = int(result.get('total_months', 0) or 0)
        note         = _v(result.get('note', ''))

        # Recalculate total_months as sanity check
        if total_months == 0:
            total_months = years * 12 + months

        # Build human-readable summary
        parts = []
        if years:  parts.append(f"{years} year{'s' if years != 1 else ''}")
        if months: parts.append(f"{months} month{'s' if months != 1 else ''}")
        summary = ' and '.join(parts) if parts else '0 months'

        # Save to DB
        _staffs_col().update_one(
            {"_id": doc['_id']},
            {"$set": {
                "experience_years":        years,
                "experience_months":       months,
                "experience_total_months": total_months,
                "experience_note":         note,
                "experience_source":       source,
                "experience_analysed_at":  datetime.utcnow(),
            }}
        )

        return jsonify({
            "success":      True,
            "staff_name":   full_name,
            "email":        email_out,
            "years":        years,
            "months":       months,
            "total_months": total_months,
            "summary":      summary,
            "note":         note,
            "source":       source,
            "cached":       False,
        })

    except _cjson.JSONDecodeError:
        return jsonify({
            "success": False,
            "error":   "Gemini returned non-JSON response",
            "raw":     raw[:300],
        }), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/api/experience', methods=['POST'])
def api_experience():
    """
    External API — analyse experience from extracted_cv via Gemini.

    Headers:
      X-API-Key: <LIVE_STAFF_API_KEY>
      Content-Type: application/json

    Body:
      {"staff_id": "68abc123..."} or {"email": "jane@example.com"}
    """
    ok, err = _validate_api_key()
    if not ok:
        return jsonify({"success": False, "error": err}), 401

    data     = request.get_json(silent=True) or {}
    staff_id = (data.get('staff_id') or '').strip()
    email    = (data.get('email') or '').strip().lower()

    gemini_key = os.environ.get('GEMINI_API_KEY', '')
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set on server"}), 500

    if staff_id:
        # 1. Try live_staffs.staff_id field (XN Portal ID)
        doc = _staffs_col().find_one({"staff_id": staff_id})
        # 2. Try live_staffs.xn_staff_id field
        if not doc:
            doc = _staffs_col().find_one({"xn_staff_id": staff_id})
        # 3. Try MongoDB _id
        if not doc:
            try:
                doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
            except Exception:
                pass
    elif email:
        # Search email in top-level field and section_1 sub-field
        doc = _staffs_col().find_one({"$or": [
            {"email": email},
            {"section_1_personal_details.email_address": email},
        ]})
    else:
        return jsonify({"success": False,
                        "error": "Provide staff_id or email in JSON body"}), 400

    if not doc:
        return jsonify({"success": False, "error": "Staff record not found"}), 404

    s1        = doc.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email_out = _v(doc.get('email') or s1.get('email_address') or email)
    user_type = _v(doc.get('user_type') or '')

    extracted_cv = _v(doc.get('extracted_cv') or '')
    has_cv_text  = (
        extracted_cv and
        not extracted_cv.startswith('[') and
        extracted_cv != 'No doc found'
    )

    s5           = doc.get('section_5_employment_history') or {}
    total_exp_db = _v(s5.get('total_experience') or '')

    if not has_cv_text and not total_exp_db:
        return jsonify({
            "success":      True,
            "staff_name":   full_name,
            "email":        email_out,
            "years":        None,
            "months":       None,
            "total_months": None,
            "summary":      "No experience data available",
            "source":       "none",
        })

    # ── Return cached result if already analysed ──────────────────────
    if doc.get('experience_analysed_at') and doc.get('experience_years') is not None:
        years        = int(doc.get('experience_years') or 0)
        months       = int(doc.get('experience_months') or 0)
        total_months = int(doc.get('experience_total_months') or 0)
        if total_months == 0:
            total_months = years * 12 + months
        parts   = []
        if years:  parts.append(f"{years} year{'s' if years != 1 else ''}")
        if months: parts.append(f"{months} month{'s' if months != 1 else ''}")
        summary = ' and '.join(parts) if parts else '0 months'
        return jsonify({
            "success":      True,
            "staff_name":   full_name,
            "email":        email_out,
            "years":        years,
            "months":       months,
            "total_months": total_months,
            "summary":      summary,
            "note":         _v(doc.get('experience_note') or ''),
            "source":       _v(doc.get('experience_source') or 'cached'),
            "cached":       True,
        })

    try:
        from google import genai as google_genai

        # Determine role filter based on user_type
        ut_lower = (user_type or '').lower()
        if 'nurse' in ut_lower or 'nursing' in ut_lower:
            role_rule = (
                "IMPORTANT — Count ONLY nursing-related work experience, regardless of the country where it was gained. "
                "This includes: Registered Nurse, Staff Nurse, Clinical Nurse, ICU Nurse, Theatre Nurse, "
                "Community Nurse, Mental Health Nurse, Nursing Home Nurse, or any role with Nurse or Nursing in the title. "
                "DO NOT count non-nursing roles such as healthcare assistant, carer, support worker, admin, or any other role."
            )
        elif 'hca' in ut_lower or 'healthcare assistant' in ut_lower or 'health care assistant' in ut_lower:
            role_rule = (
                "IMPORTANT — Count ONLY Healthcare Assistant (HCA) work experience, regardless of the country where it was gained. "
                "This includes: Healthcare Assistant, HCA, Care Assistant, Care Worker, Support Worker in a clinical/care setting, "
                "or any role with Healthcare Assistant or HCA in the title. "
                "DO NOT count nursing roles (Registered Nurse, Staff Nurse, etc.) or non-care roles such as admin, retail, or hospitality."
            )
        else:
            role_rule = (
                "Count only direct healthcare or care-related work experience, regardless of country. "
                "Exclude non-healthcare roles such as admin, retail, hospitality, or general support roles "
                "unless they are clearly in a clinical or care setting."
            )

        if has_cv_text:
            source = 'extracted_cv'
            prompt = f"""You are a professional CV analyser specialising in Irish healthcare staffing.

Candidate role: {user_type}

Read the CV text below and calculate the candidate's TOTAL relevant work experience.

{role_rule}

Calculation Rules:
- Only count roles that match the role filter above.
- Experience gained in ANY country counts — not just Ireland.
- If a matching role has no end date, assume it is still ongoing (use today's date to calculate).
- If two matching roles overlap in time, count the overlapping period only once.
- Ignore all non-matching roles entirely — do not add them.
- Return ONLY a JSON object with these exact keys — nothing else, no markdown, no explanation:
  {{"years": <integer>, "months": <integer 0-11>, "total_months": <integer>, "note": "<one sentence summary of which roles were counted>"}}

CV TEXT:
{extracted_cv[:10000]}
"""
        else:
            source = 'section_5'
            prompt = f"""You are a professional CV analyser specialising in Irish healthcare staffing.

Candidate role: {user_type}

The candidate's total experience is described as: "{total_exp_db}"

{role_rule}

Extract the relevant years and months from this description, applying the role filter above.
Return ONLY a JSON object — nothing else, no markdown:
{{"years": <integer>, "months": <integer 0-11>, "total_months": <integer>, "note": "<one sentence summary>"}}
"""


        client   = google_genai.Client(api_key=gemini_key)
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        raw      = (response.text or '').strip()
        raw      = _re.sub(r'^```(?:json)?\s*', '', raw, flags=_re.MULTILINE)
        raw      = _re.sub(r'```\s*$', '', raw, flags=_re.MULTILINE).strip()

        result       = _json.loads(raw)
        years        = int(result.get('years', 0) or 0)
        months       = int(result.get('months', 0) or 0)
        total_months = int(result.get('total_months', 0) or 0)
        note         = _v(result.get('note', ''))
        if total_months == 0:
            total_months = years * 12 + months

        parts   = []
        if years:  parts.append(f"{years} year{'s' if years != 1 else ''}")
        if months: parts.append(f"{months} month{'s' if months != 1 else ''}")
        summary = ' and '.join(parts) if parts else '0 months'

        _staffs_col().update_one(
            {"_id": doc['_id']},
            {"$set": {
                "experience_years":        years,
                "experience_months":       months,
                "experience_total_months": total_months,
                "experience_analysed_at":  datetime.utcnow(),
            }}
        )

        return jsonify({
            "success":      True,
            "staff_name":   full_name,
            "email":        email_out,
            "years":        years,
            "months":       months,
            "total_months": total_months,
            "summary":      summary,
            "note":         note,
            "source":       source,
        })

    except _cjson.JSONDecodeError:
        return jsonify({"success": False, "error": "Gemini returned non-JSON response", "raw": raw[:300]}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/import-last-point-scale', methods=['POST'])
@admin_required
def live_staff_import_last_point_scale():
    """
    Seed last_point_scale for a staff member by email.
    Also resets points_checked so the cron will re-check them.

    POST /admin/live-staffs/import-last-point-scale
    Body: {"email": "...", "last_point_scale": 9}
    """
    data     = request.get_json() or {}
    email    = (data.get('email') or '').strip().lower()
    last_ps  = data.get('last_point_scale')

    if not email:
        return jsonify({"success": False, "error": "Missing email"}), 400
    if last_ps is None:
        return jsonify({"success": False, "error": "Missing last_point_scale"}), 400

    try:
        result = _staffs_col().update_one(
            {"email": email},
            {"$set": {
                "last_point_scale": last_ps,
                "points_checked":   False,   # reset so cron re-checks
            }}
        )
        if result.matched_count == 0:
            return jsonify({"success": False,
                            "error": f"No staff found with email: {email}"}), 404
        return jsonify({
            "success":  True,
            "email":    email,
            "last_point_scale": last_ps,
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── AI CV Collection helper ───────────────────────────────────────────

def _ai_cvs_col():
    return db.live_staff_ai_cvs

def _ai_interviews_col():
    return db.live_staff_ai_interviews


def _ai_appforms_col():
    return db.live_staff_ai_appforms


@admin_bp.route('/live-staffs/ai-appform/reset-all', methods=['POST'])
@admin_required
def live_staff_ai_appform_reset_all():
    """
    Delete all saved application forms from MongoDB so the cron
    will regenerate them with the latest template.
    POST /admin/live-staffs/ai-appform/reset-all
    """
    try:
        result = _ai_appforms_col().delete_many({})
        return jsonify({
            "success": True,
            "deleted": result.deleted_count,
            "message": f"Cleared {result.deleted_count} application forms — cron will regenerate them.",
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


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

        s1        = doc.get('section_1_personal_details') or {}
        full_name = _v(s1.get('full_name') or 'staff')
        email     = _v(doc.get('email'))
        emp_code  = _v(doc.get('employee_code') or '')

        # Download signature from GCS if available
        signature_bytes = None
        sig_blob = _v(doc.get('signature_gcs_blob') or '')
        if sig_blob:
            try:
                signature_bytes = _gcs_download(sig_blob)
            except Exception:
                signature_bytes = None

        docx_bytes = _build_appform_docx(doc, signature_bytes=signature_bytes)
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

def _build_appform_docx(doc, signature_bytes=None):
    """
    Build a filled Xpress Health Application Form matching the uploaded template exactly.
    Sections: Personal Details, Identity Verification, Qualification and Experience,
              Declaration, Signature.
    All data pulled from live_staffs MongoDB document — no hallucination.
    If signature_bytes is provided, embeds the actual signature image.
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

    def _add_tick_run(para, checked):
        """
        Add a tick/box using Unicode ballot box characters with DejaVu Sans font.
        U+2611 ☑ = ballot box with check (ticked)
        U+2610 ☐ = ballot box (empty)
        DejaVu Sans is bundled with LibreOffice and renders these correctly in PDF.
        """
        r = para.add_run('☑' if checked else '☐')
        r.font.name = 'DejaVu Sans'
        r.font.size = Pt(12)
        r.font.color.rgb = BLACK
        return r

    def add_checkbox_line(label, checked):
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(2)
        p.paragraph_format.space_after  = Pt(2)
        _add_tick_run(p, checked)
        r2 = p.add_run(f'  {label}')
        r2.font.name = 'Calibri'
        r2.font.size = Pt(11)
        r2.font.color.rgb = BLACK

    def add_checkbox_row(items):
        """Multiple checkboxes on one line: [(label, checked), ...]"""
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(3)
        p.paragraph_format.space_after  = Pt(3)
        for i, (label, checked) in enumerate(items):
            _add_tick_run(p, checked)
            run = p.add_run(f'  {label}')
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

    # ── Signature ──────────────────────────────────────────────────────
    p_sig_lbl = d.add_paragraph()
    p_sig_lbl.paragraph_format.space_before = Pt(6)
    p_sig_lbl.paragraph_format.space_after  = Pt(2)
    r_sig = p_sig_lbl.add_run('Applicant Signature:')
    r_sig.bold = True
    r_sig.font.name = 'Calibri'
    r_sig.font.color.rgb = NAVY

    if signature_bytes:
        # Embed the actual signature image
        try:
            import tempfile as _tmp, pathlib as _pl
            with _tmp.NamedTemporaryFile(suffix='.png', delete=False) as tf:
                tf.write(signature_bytes)
                tf_path = tf.name
            from docx.shared import Inches as _Inches
            p_img = d.add_paragraph()
            p_img.paragraph_format.space_before = Pt(0)
            p_img.paragraph_format.space_after  = Pt(4)
            run_img = p_img.add_run()
            run_img.add_picture(tf_path, width=_Inches(2.0))
            import os as _os
            _os.unlink(tf_path)
        except Exception:
            # Fall back to blank line if image embedding fails
            p_blank = d.add_paragraph()
            p_blank.add_run('   _______________________________')
    else:
        p_blank = d.add_paragraph()
        p_blank.paragraph_format.space_before = Pt(0)
        p_blank.add_run('   _______________________________')

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
        # Also include NMBI/QQI numbers as qualification context
        nmbi_num = _vv(doc.get('nmbi_number') or s3.get('registration_number_pin') or '')
        qqi_num  = _vv(doc.get('qqi_number') or '')
        if nmbi_num and not any('nmbi' in l.lower() or 'registration' in l.lower() for l in qual_lines):
            qual_lines.append(f"  - NMBI Registration PIN: {nmbi_num}")
        if qqi_num and not any('qqi' in l.lower() for l in qual_lines):
            qual_lines.append(f"  - QQI Level 5 Certificate No: {qqi_num}")

        # ── Guaranteed fallback so EDUCATION section is NEVER empty ──
        if not qual_lines:
            _role_lower = user_type.lower()
            if any(t in _role_lower for t in ('nurse', 'rgn', 'midwife', 'rnm', 'rn')):
                _inferred_q = 'Bachelor of Nursing Science (or equivalent)'
                _inferred_i = 'University College (based on nationality)'
            elif any(t in _role_lower for t in ('hca', 'healthcare assistant', 'health care assistant',
                                                 'support worker', 'care assistant', 'care worker')):
                _inferred_q = 'QQI Level 5 in Healthcare Support'
                _inferred_i = 'College of Further Education'
            elif any(t in _role_lower for t in ('physio', 'occupational', 'speech', 'radiograph')):
                _inferred_q = 'BSc in Allied Health Sciences (or equivalent)'
                _inferred_i = 'Health Sciences University (based on nationality)'
            else:
                _inferred_q = 'Relevant Healthcare Qualification (see profile)'
                _inferred_i = 'Healthcare Training Institute'
            qual_lines.append(f"  - {_inferred_q} | {_inferred_i} | [year estimated from experience]")

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

        # ── Always re-extract CV from XN Portal on every generate ────────
        extracted_cv = _v(doc.get('extracted_cv') or '')

        if True:  # always attempt fresh extraction
            _base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
            _api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
            _app_country = os.environ.get('XN_APP_COUNTRY', '')
            _gemini_key  = os.environ.get('GEMINI_API_KEY', '')
            _staff_email = _v(doc.get('email') or
                              (doc.get('section_1_personal_details') or {}).get('email_address') or '')

            if _base_url and _staff_email:
                try:
                    import requests as _req_cv
                    _endpoint   = f"{_base_url}/ai/recruitments/user-document-list"
                    _api_hdrs   = {
                        "Api-Key":       _api_key,
                        "X-App-Country": _app_country,
                        "Content-Type":  "application/json",
                        "Accept":        "application/json",
                    }
                    _r = _req_cv.post(_endpoint, json={"email": _staff_email},
                                      headers=_api_hdrs, timeout=30)
                    if _r.status_code == 405:
                        _r = _req_cv.get(_endpoint, params={"email": _staff_email},
                                         headers=_api_hdrs, timeout=30)
                    _r.raise_for_status()
                    _portal_data = _r.json()

                    _api_data  = _portal_data.get('data')
                    _docs      = _api_data if isinstance(_api_data, list) else \
                                 (_api_data.get('documents') or []
                                  if isinstance(_api_data, dict) else [])

                    _cv_url = None
                    for _d in _docs:
                        _dn = (_d.get('document_type_name') or '').strip()
                        if _dn == 'Cv' and _d.get('url'):
                            _cv_url = _d['url']
                            break

                    if _cv_url and _gemini_key:
                        # Download and extract CV text using Gemini
                        import io as _cv_io
                        _dl_hdrs = {k: v for k, v in _api_hdrs.items()
                                    if k != 'Content-Type'}
                        _dl = _req_cv.get(_cv_url, headers=_dl_hdrs, timeout=60)
                        _dl.raise_for_status()
                        _raw    = _dl.content
                        _ct     = _dl.headers.get('Content-Type', '').lower()
                        _ul     = _cv_url.lower().split('?')[0]
                        _rtxt   = ''

                        # Extract raw text
                        if 'pdf' in _ct or _ul.endswith('.pdf'):
                            try:
                                import pdfplumber as _plmb
                                with _plmb.open(_cv_io.BytesIO(_raw)) as _pdf:
                                    _rtxt = '\n'.join(p.extract_text() or ''
                                                      for p in _pdf.pages).strip()
                            except Exception:
                                pass
                        if not _rtxt and ('wordprocessingml' in _ct or
                                          _ul.endswith('.docx') or _ul.endswith('.doc')):
                            try:
                                from docx import Document as _DDoc
                                _ddoc = _DDoc(_cv_io.BytesIO(_raw))
                                _rtxt = '\n'.join(p.text for p in _ddoc.paragraphs).strip()
                            except Exception:
                                pass
                        if not _rtxt:
                            try:
                                import pdfplumber as _plmb2
                                with _plmb2.open(_cv_io.BytesIO(_raw)) as _pdf2:
                                    _rtxt = '\n'.join(p.extract_text() or ''
                                                      for p in _pdf2.pages).strip()
                            except Exception:
                                pass
                        if not _rtxt:
                            _rtxt = _raw.decode('utf-8', errors='replace').strip()

                        if _rtxt:
                            # Gemini clean & structure
                            from google import genai as _gai_cv
                            _gclient = _gai_cv.Client(api_key=_gemini_key)
                            _prompt  = f"""You are a professional CV parser.

Extract and structure all CV content into clean, readable plain text.
Preserve ALL factual information exactly — do NOT add, invent, or change any facts.
Format with clear section headings where content exists.
Return ONLY the clean structured CV text — no preamble, no commentary.

RAW EXTRACTED TEXT:
{_rtxt[:12000]}
"""
                            _gr = _gclient.models.generate_content(
                                model='gemini-2.5-flash',
                                contents=_prompt
                            )
                            _extracted = (_gr.text or '').strip()
                            if _extracted:
                                extracted_cv = _extracted
                                # Save to DB so future generations skip this step
                                _staffs_col().update_one(
                                    {"_id": doc['_id']},
                                    {"$set": {
                                        "extracted_cv":    _extracted,
                                        "extracted_cv_at": datetime.utcnow(),
                                        "extracted_cv_source": "auto_on_cv_generate",
                                    }}
                                )
                except Exception as _cv_ex:
                    # Non-fatal — CV generation continues without extracted text
                    pass

        has_extracted_cv = (
            extracted_cv and
            not extracted_cv.startswith('[') and
            extracted_cv not in ('[no CV document found]', 'No doc found', '')
        )

        data_summary = f"""
Name: {full_name}
Role / User Type: {user_type}
Employee Code: {emp_code}
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

        prompt = f"""You are a professional CV writer for Irish healthcare staffing.

Rewrite the candidate's CV below into a clean, professional CV using this exact structure:

EMPLOYMENT ELIGIBILITY
PROFESSIONAL PROFILE
EDUCATION & QUALIFICATIONS
PROFESSIONAL EXPERIENCE
TRAINING & CERTIFICATIONS
KEY SKILLS
ADDITIONAL INFORMATION

Rules:
- Use ONLY the information provided — do not invent anything.
- EMPLOYMENT ELIGIBILITY: use CANDIDATE DATA. Label: Value per line. Do NOT include name, address, mobile or email.
- PROFESSIONAL PROFILE: 2 short paragraphs, first person, based on the candidate's background.
- EDUCATION & QUALIFICATIONS: copy ALL education from the CV exactly as written — every school, college, course, degree. Do not filter or skip any entry.
- PROFESSIONAL EXPERIENCE: copy ALL jobs from the CV exactly — every employer, job title, dates and duties.
- TRAINING & CERTIFICATIONS: list all certificates and training from the CV.
- KEY SKILLS: list skills from the CV.
- ADDITIONAL INFORMATION: write only these two lines:
  Driving Licence: No
  Own Transport: No

CANDIDATE DATA:
{data_summary}

CANDIDATE'S ORIGINAL CV:
{extracted_cv[:15000] if has_extracted_cv else "No CV available — build from CANDIDATE DATA above."}

Output the structured CV text only. No markdown, no asterisks, no preamble.
"""

        from google import genai as google_genai
        client   = google_genai.Client(api_key=gemini_key)
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        cv_text  = response.text.strip()

        docx_bytes  = _build_ai_cv_docx(doc, cv_text)
        safe_name   = (full_name or 'staff').replace(' ', '_').replace('/', '_')
        cv_filename = f"{safe_name}.docx"
        gcs_blob    = f"cv/{cv_filename}"
        _gcs_upload(gcs_blob, docx_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        col      = _ai_cvs_col()
        existing = col.find_one({"staff_id": str(doc['_id'])})
        ai_doc   = {
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
            ai_id = str(col.insert_one(ai_doc).inserted_id)

        _push_hse_document_background(
            staff_id_str=staff_id, doc_type_key='cv',
            docx_bytes=docx_bytes, staff_name=full_name,
            mongo_id=staff_id, email=email,
        )

        download_url = _gcs_signed_url(gcs_blob) or ''
        return jsonify({
            "success":      True,
            "staff_id":     staff_id,
            "staff_name":   full_name,
            "cv_id":        ai_id,
            "cv_filename":  cv_filename,
            "gcs_blob":     gcs_blob,
            "download_url": download_url,
            "generated_at": datetime.utcnow().isoformat(),
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/api/generate-cv', methods=['POST'])
def live_staff_api_generate_cv():
    """
    External API — generate AI CV for a staff member.
    Headers: X-API-Key: <LIVE_STAFF_API_KEY>
    Body:    {"staff_id": "..."}  or  {"email": "..."}
    """
    api_key = os.environ.get('LIVE_STAFF_API_KEY', '')
    if api_key:
        provided = (request.headers.get('X-API-Key') or
                    request.headers.get('X-Api-Key') or '')
        if provided != api_key:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    body     = request.get_json(silent=True) or {}
    staff_id = _v(body.get('staff_id') or '')
    email    = _v(body.get('email') or '').lower()

    if not staff_id and not email:
        return jsonify({"success": False, "error": "staff_id or email required"}), 400

    try:
        col = _staffs_col()
        if staff_id:
            doc = col.find_one({"_id": ObjectId(staff_id)})
        else:
            doc = col.find_one({"$or": [
                {"email": email},
                {"section_1_personal_details.email_address": email},
            ]})

        if not doc:
            return jsonify({"success": False, "error": "Staff not found"}), 404

        staff_id  = str(doc['_id'])
        s1        = doc.get('section_1_personal_details') or {}
        full_name = _v(s1.get('full_name') or '')
        email     = _v(doc.get('email') or s1.get('email_address') or '')
        emp_code  = _v(doc.get('employee_code') or '')
        user_type = _v(doc.get('user_type') or '')

        gemini_key = os.environ.get('GEMINI_API_KEY', '')
        if not gemini_key:
            return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

        # Build candidate data summary
        s3 = doc.get('section_3_professional_registration') or {}
        s4 = doc.get('section_4_qualifications') or {}
        s5 = doc.get('section_5_employment_history') or {}
        def _vv(v): return '' if v is None else str(v).strip()
        nationality = _vv(s1.get('nationality'))
        reg_pin     = _vv(s3.get('registration_number_pin'))
        total_exp   = _vv(s5.get('total_experience'))
        nmbi_num    = _vv(doc.get('nmbi_number') or reg_pin or '')
        qqi_num     = _vv(doc.get('qqi_number') or '')

        qual_lines = []
        for qk in ['nursing_degree', 'postgraduate_qualification', 'other_qualification']:
            q = s4.get(qk) or {}
            if q.get('qualification') or q.get('institution'):
                qual_lines.append(f"  - {_vv(q.get('qualification'))} | {_vv(q.get('institution'))} | {_vv(q.get('year_completed'))}")
        if nmbi_num:
            qual_lines.append(f"  - NMBI Registration PIN: {nmbi_num}")
        if qqi_num:
            qual_lines.append(f"  - QQI Level 5 Certificate No: {qqi_num}")

        # Fallback qualification
        if not qual_lines:
            _rl = user_type.lower()
            if any(t in _rl for t in ('nurse','rgn','midwife')):
                qual_lines.append('  - Bachelor of Nursing Science (or equivalent) | University College | [year estimated]')
            elif any(t in _rl for t in ('hca','healthcare assistant','care worker','support worker')):
                qual_lines.append('  - QQI Level 5 in Healthcare Support | College of Further Education | [year estimated]')
            else:
                qual_lines.append(f'  - Relevant Professional Qualification | Training Institute | [year estimated]')

        entries = [e for e in (s5.get('entries') or []) if e.get('employer') or e.get('position')]
        exp_lines = [f"  - {_vv(e.get('position'))} at {_vv(e.get('employer'))} ({_vv(e.get('from'))} - {_vv(e.get('to') or 'Present')})" for e in entries]

        extracted_cv = _v(doc.get('extracted_cv') or '')
        has_cv = extracted_cv and not extracted_cv.startswith('[') and extracted_cv not in ('No doc found','')

        data_summary = f"""Name: {full_name}
Role: {user_type}
Nationality: {nationality}
Total Experience: {total_exp}
Registration PIN: {reg_pin}
Qualifications:
{chr(10).join(qual_lines) if qual_lines else '  None recorded'}
Employment History:
{chr(10).join(exp_lines) if exp_lines else '  None recorded'}""".strip()

        prompt = f"""You are a professional CV writer for Irish healthcare staffing.

Rewrite the candidate's CV below into a clean, professional CV using this exact structure:

EMPLOYMENT ELIGIBILITY
PROFESSIONAL PROFILE
EDUCATION & QUALIFICATIONS
PROFESSIONAL EXPERIENCE
TRAINING & CERTIFICATIONS
KEY SKILLS
ADDITIONAL INFORMATION

Rules:
- Use ONLY the information provided — do not invent anything.
- EMPLOYMENT ELIGIBILITY: Label: Value per line. Do NOT include name, address, mobile or email.
- EDUCATION & QUALIFICATIONS: copy ALL education exactly as written.
- PROFESSIONAL EXPERIENCE: copy ALL jobs exactly — employer, title, dates, duties.
- ADDITIONAL INFORMATION: write only: Driving Licence: No / Own Transport: No

CANDIDATE DATA:
{data_summary}

CANDIDATE'S ORIGINAL CV:
{extracted_cv[:15000] if has_cv else "No CV available — build from CANDIDATE DATA above."}

Output the structured CV text only. No markdown, no preamble.
"""

        from google import genai as google_genai
        client   = google_genai.Client(api_key=gemini_key)
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        cv_text  = response.text.strip()

        docx_bytes  = _build_ai_cv_docx(doc, cv_text)
        safe_name   = (full_name or 'staff').replace(' ', '_').replace('/', '_')
        cv_filename = f"{safe_name}.docx"
        gcs_blob    = f"cv/{cv_filename}"
        _gcs_upload(gcs_blob, docx_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        col2     = _ai_cvs_col()
        existing = col2.find_one({"staff_id": staff_id})
        ai_doc   = {
            "staff_id":      staff_id,
            "staff_name":    full_name,
            "employee_code": emp_code,
            "cv_text":       cv_text,
            "cv_filename":   cv_filename,
            "gcs_blob":      gcs_blob,
            "generated_at":  datetime.utcnow(),
        }
        if existing:
            col2.update_one({"_id": existing["_id"]}, {"$set": ai_doc})
            ai_id = str(existing["_id"])
        else:
            ai_id = str(col2.insert_one(ai_doc).inserted_id)

        _push_hse_document_background(
            staff_id_str=staff_id, doc_type_key='cv',
            docx_bytes=docx_bytes, staff_name=full_name,
            mongo_id=staff_id, email=email,
        )

        return jsonify({
            "success":      True,
            "staff_id":     staff_id,
            "staff_name":   full_name,
            "cv_id":        ai_id,
            "cv_filename":  cv_filename,
            "gcs_blob":     gcs_blob,
            "generated_at": datetime.utcnow().isoformat(),
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/api/generate-interview', methods=['POST'])
def api_generate_interview():
    """
    External API — generate AI Interview Notes for a staff member.

    Headers:
      X-API-Key: <LIVE_STAFF_API_KEY>
      Content-Type: application/json

    Body:
      {"staff_id": "68abc123..."}
    """
    ok, err = _validate_api_key()
    if not ok:
        return jsonify({"success": False, "error": err}), 401

    data     = request.get_json(silent=True) or {}
    staff_id = (data.get('staff_id') or '').strip()
    email    = (data.get('email') or '').strip().lower()

    if not staff_id and not email:
        return jsonify({"success": False,
                        "error": "Provide staff_id or email in request body"}), 400

    gemini_key = os.environ.get('GEMINI_API_KEY', '')
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set on server"}), 500

    try:
        doc = None
        if staff_id:
            # Search by staff_id (XN Portal ID), xn_staff_id, then MongoDB _id
            doc = _staffs_col().find_one({"staff_id": staff_id})
            if not doc:
                doc = _staffs_col().find_one({"xn_staff_id": staff_id})
            if not doc:
                try:
                    doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
                except Exception:
                    pass
        if not doc and email:
            doc = _staffs_col().find_one({"$or": [
                {"email": email},
                {"section_1_personal_details.email_address": email},
            ]})
        if not doc:
            identifier = staff_id or email
            return jsonify({"success": False,
                            "error": f"Staff not found: {identifier}"}), 404

        s1        = doc.get('section_1_personal_details') or {}
        s3        = doc.get('section_3_professional_registration') or {}
        s5        = doc.get('section_5_employment_history') or {}
        s8        = doc.get('section_8_garda_vetting_police_clearance') or {}
        s9        = doc.get('section_9_occupational_health') or {}
        s10       = doc.get('section_10_mandatory_training') or {}
        visa      = s1.get('work_permit_visa_status') or {}

        def _vv(v): return '' if v is None else str(v).strip()

        full_name   = _vv(s1.get('full_name'))
        email       = _vv(doc.get('email'))
        user_type   = _vv(doc.get('user_type') or 'Nurse')
        emp_code    = _vv(doc.get('employee_code'))
        address     = _vv(s1.get('address'))
        nationality = _vv(s1.get('nationality'))
        reg_pin     = _vv(s3.get('registration_number_pin'))
        visa_type   = _vv(visa.get('visa_type'))
        divisions   = ', '.join(s3.get('divisions_registered_in') or [])
        total_exp   = _vv(s5.get('total_experience'))
        nmbi        = 'Yes' if s3.get('nmbi_active_declaration') else 'No'
        garda       = 'Yes' if s8.get('garda_vetting_submitted') else 'No'
        bls         = 'Yes' if s10.get('cpr_bls') else 'No'
        manual      = 'Yes' if s10.get('manual_handling') else 'No'
        fit         = 'Yes' if s9.get('fit_for_nursing_duties') else 'No'

        entries   = [e for e in (s5.get('entries') or []) if e.get('employer') or e.get('position')]
        exp_lines = [
            f"  - {_vv(e.get('position'))} at {_vv(e.get('employer'))} "
            f"({_vv(e.get('from'))} – {_vv(e.get('to') or 'Present')})"
            for e in entries[:5]
        ]

        TLABELS = {
            'manual_handling': 'Manual Handling', 'cpr_bls': 'BLS/CPR',
            'safeguarding': 'Safeguarding', 'fire_safety': 'Fire Safety',
            'infection_prevention_control': 'Infection Prevention & Control',
        }
        certs = [label for k, label in TLABELS.items() if s10.get(k)]

        county = ''
        if address:
            parts = [p.strip() for p in address.replace(',', ' ').split()]
            for p in parts:
                if p.lower().startswith('co.') or p.lower() == 'county':
                    idx = parts.index(p)
                    county = parts[idx + 1] if idx + 1 < len(parts) else ''
                    break
            if not county and parts:
                county = parts[-1]

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
{chr(10).join(exp_lines) if exp_lines else "  None recorded"}

Certifications on file: {", ".join(certs) if certs else "None recorded"}
""".strip()

        prompt = f"""You are an experienced nursing recruitment consultant at Xpress Health, Ireland.

Complete a realistic professional interview notes template using ONLY the candidate data below.
Write answers in first person, conversational but professional. NO HALLUCINATION.
Assessment scores: pick varied random scores 3.5–5/5 in 0.5 increments.

Output ONLY the completed template — no preamble, no markdown:

---
Completed {user_type} Interview

Name: [full name]
Location: [county/city]
NMBI PIN: [pin or N/A]
Visa Status: [visa type]

Experience

1. Tell me about your nursing experience.
[4–6 sentence first-person answer using only data provided]

2. How many years in Ireland?
[realistic answer from employment history]

3. Acute, Nursing Home, Community, or Mental Health?
[most relevant care setting from employment history]

Clinical Questions

1. How would you manage a deteriorating patient?
[4–5 sentence clinically accurate answer for a {user_type}, using ABCDE/NEWS2/ISBAR]

2. What would you do if you witnessed a medication error?
[4–5 sentence answer covering patient safety, reporting, documentation, prevention]

Compliance
NMBI Registration: [Yes/No]
BLS/CPR: [Yes/No]
Manual Handling: [Yes/No]
Garda Vetting: [Yes/No]
References: Yes

Availability
Preferred counties: [county from address]
Day/Night/Both: Both
Earliest start date: Immediate

Assessment
Communication: [score/5]
Clinical Knowledge: [score/5]
Experience: [score/5]
Suitable: Yes
---

CANDIDATE DATA:
{data_summary}
"""

        from google import genai as google_genai
        client         = google_genai.Client(api_key=gemini_key)
        response       = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        interview_text = response.text.strip().strip('-').strip()

        docx_bytes = _build_interview_docx(doc, interview_text)
        safe_name  = (full_name or 'staff').replace(' ', '_').replace('/', '_')
        filename   = f"Interview_{safe_name}_{staff_id}.docx"
        gcs_blob   = f"interviews/{filename}"
        _gcs_upload(gcs_blob, docx_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        col      = _ai_interviews_col()
        existing = col.find_one({"staff_id": str(doc['_id'])})
        rec = {
            "staff_id":       str(doc['_id']),
            "staff_name":     full_name,
            "employee_code":  emp_code,
            "interview_text": interview_text,
            "filename":       filename,
            "gcs_blob":       gcs_blob,
            "generated_at":   datetime.utcnow(),
        }
        if existing:
            col.update_one({"_id": existing["_id"]}, {"$set": rec})
            rec_id = str(existing["_id"])
        else:
            rec_id = str(col.insert_one(rec).inserted_id)

        _push_hse_document_background(
            staff_id_str=staff_id, doc_type_key='interview',
            docx_bytes=docx_bytes, staff_name=full_name,
            mongo_id=staff_id, email=email,
        )

        download_url = _gcs_signed_url(gcs_blob) or ''
        return jsonify({
            "success":       True,
            "staff_id":      staff_id,
            "staff_name":    full_name,
            "interview_id":  rec_id,
            "filename":      filename,
            "gcs_blob":      gcs_blob,
            "download_url":  download_url,
            "generated_at":  datetime.utcnow().isoformat(),
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/api/generate-appform', methods=['POST'])
def api_generate_appform():
    """
    External API — generate Application Form for a staff member.

    Headers:
      X-API-Key: <LIVE_STAFF_API_KEY>
      Content-Type: application/json

    Body:
      {"staff_id": "68abc123..."}
    """
    ok, err = _validate_api_key()
    if not ok:
        return jsonify({"success": False, "error": err}), 401

    data     = request.get_json(silent=True) or {}
    staff_id = (data.get('staff_id') or '').strip()
    email    = (data.get('email') or '').strip().lower()

    if not staff_id and not email:
        return jsonify({"success": False,
                        "error": "Provide staff_id or email in request body"}), 400

    try:
        doc = None
        if staff_id:
            doc = _staffs_col().find_one({"staff_id": staff_id})
            if not doc:
                doc = _staffs_col().find_one({"xn_staff_id": staff_id})
            if not doc:
                try:
                    doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
                except Exception:
                    pass
        if not doc and email:
            doc = _staffs_col().find_one({"$or": [
                {"email": email},
                {"section_1_personal_details.email_address": email},
            ]})
        if not doc:
            identifier = staff_id or email
            return jsonify({"success": False,
                            "error": f"Staff not found: {identifier}"}), 404

        s1        = doc.get('section_1_personal_details') or {}
        full_name = _v(s1.get('full_name') or 'staff')
        email     = _v(doc.get('email'))
        emp_code  = _v(doc.get('employee_code') or '')

        # Download signature from GCS if available
        signature_bytes = None
        sig_blob = _v(doc.get('signature_gcs_blob') or '')
        if sig_blob:
            try:
                signature_bytes = _gcs_download(sig_blob)
            except Exception:
                signature_bytes = None

        docx_bytes = _build_appform_docx(doc, signature_bytes=signature_bytes)
        safe_name  = (full_name or 'staff').replace(' ', '_').replace('/', '_')
        filename   = f"AppForm_{safe_name}.docx"
        gcs_blob   = f"appforms/{filename}"
        _gcs_upload(gcs_blob, docx_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        col      = _ai_appforms_col()
        existing = col.find_one({"staff_id": str(doc['_id'])})
        rec = {
            "staff_id":      str(doc['_id']),
            "staff_name":    full_name,
            "employee_code": emp_code,
            "filename":      filename,
            "gcs_blob":      gcs_blob,
            "has_signature": bool(signature_bytes),
            "generated_at":  datetime.utcnow(),
        }
        if existing:
            col.update_one({"_id": existing["_id"]}, {"$set": rec})
            rec_id = str(existing["_id"])
        else:
            rec_id = str(col.insert_one(rec).inserted_id)

        _push_hse_document_background(
            staff_id_str=staff_id, doc_type_key='appform',
            docx_bytes=docx_bytes, staff_name=full_name,
            mongo_id=staff_id, email=email,
        )

        # Convert DOCX → PDF and upload to GCS for direct download link
        pdf_blob     = gcs_blob.replace('.docx', '.pdf')
        download_url = ''
        try:
            pdf_bytes_dl = _docx_to_pdf_bytes(docx_bytes)
            _gcs_upload(pdf_blob, pdf_bytes_dl, content_type='application/pdf')
            download_url = _gcs_signed_url(pdf_blob) or ''
        except Exception:
            # Fall back to DOCX signed URL if PDF conversion fails
            download_url = _gcs_signed_url(gcs_blob) or ''
            pdf_blob     = ''

        return jsonify({
            "success":       True,
            "staff_id":      staff_id,
            "staff_name":    full_name,
            "appform_id":    rec_id,
            "filename":      filename,
            "gcs_blob":      gcs_blob,
            "pdf_blob":      pdf_blob,
            "has_signature": bool(signature_bytes),
            "download_url":  download_url,
            "generated_at":  datetime.utcnow().isoformat(),
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/export')
@admin_required
def live_staff_export():
    """Export live_staffs as JSON or CSV (format=json or format=csv)."""
    fmt  = request.args.get('format', 'json').lower()
    docs = list(_staffs_col().find({}))

    def _ser(obj):
        if isinstance(obj, dict):  return {k: _ser(v) for k, v in obj.items()}
        if isinstance(obj, list):  return [_ser(i) for i in obj]
        if hasattr(obj, 'isoformat'): return obj.isoformat()
        if type(obj).__name__ in ('ObjectId', 'Decimal128'): return str(obj)
        return obj

    if fmt == 'csv':
        import csv, io as _io
        out  = _io.StringIO()
        flat = []
        for d in docs:
            s = _ser(d)
            s1 = s.get('section_1_personal_details') or {}
            flat.append({
                '_id':           str(d.get('_id','')),
                'full_name':     s1.get('full_name',''),
                'email':         s.get('email',''),
                'user_type':     s.get('user_type',''),
                'employee_code': s.get('employee_code',''),
                'nationality':   s1.get('nationality',''),
                'nmbi_number':   s.get('nmbi_number',''),
                'qqi_number':    s.get('qqi_number',''),
            })
        if flat:
            writer = csv.DictWriter(out, fieldnames=flat[0].keys())
            writer.writeheader()
            writer.writerows(flat)
        return Response(
            out.getvalue(), mimetype='text/csv',
            headers={"Content-Disposition": "attachment; filename=live_staffs.csv"}
        )

    data = [dict(_ser(d), **{'_id': str(d['_id'])}) for d in docs]
    return Response(
        __import__('json').dumps(data, default=str, ensure_ascii=False),
        mimetype='application/json',
        headers={"Content-Disposition": "attachment; filename=live_staffs.json"}
    )



# ── Cron: Sync document list from XN Portal ───────────────────────────
