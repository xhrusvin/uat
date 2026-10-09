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

def _validate_api_key():
    """Validate X-API-Key header against LIVE_STAFF_API_KEY env var.
    Returns (True, None) if valid or no key configured, else (False, response)."""
    api_key = os.environ.get('LIVE_STAFF_API_KEY', '')
    if not api_key:
        return True, None
    provided = (request.headers.get('X-API-Key') or
                request.headers.get('X-Api-Key') or '')
    if provided != api_key:
        return False, (jsonify({"success": False, "error": "Unauthorised"}), 401)
    return True, None


def _extract_missing_fields(nationality, total_exp, extracted_cv, entries):
    """
    Fill missing nationality and total_exp by analysing extracted_cv and employment entries.
    Returns (nationality, total_exp) with best available values.
    """
    import re as _re_mf
    from datetime import datetime as _dt_mf

    cv = extracted_cv or ''

    # ── Nationality from CV text ──────────────────────────────────────
    if not nationality and cv:
        _m = _re_mf.search(
            r'(?i)\bnationality\s*[:\-]\s*([A-Za-z ]{2,40})',
            cv
        )
        if _m:
            nationality = _m.group(1).strip().title()

    # ── Total Experience: calculate from employment entries ───────────
    if not total_exp and entries:
        try:
            _now = _dt_mf.now()
            _earliest = None
            for _e in entries:
                _from_str = str(_e.get('from') or '').strip()
                for _fmt in ('%B %Y', '%b %Y', '%m/%Y', '%Y-%m',
                              '%d/%m/%Y', '%Y-%m-%d', '%Y'):
                    try:
                        _d = _dt_mf.strptime(_from_str, _fmt)
                        if _earliest is None or _d < _earliest:
                            _earliest = _d
                        break
                    except Exception:
                        pass
            if _earliest:
                _months = (_now.year - _earliest.year) * 12 +                           (_now.month - _earliest.month)
                _yrs    = _months // 12
                _mos    = _months % 12
                total_exp = (
                    f"{_yrs} year{'s' if _yrs != 1 else ''}" +
                    (f" {_mos} month{'s' if _mos != 1 else ''}" if _mos else "")
                )
        except Exception:
            pass

    # ── Total Experience: scan CV text for "X years experience" ──────
    if not total_exp and cv:
        _m2 = _re_mf.search(
            r'(?i)(\d+\.?\d*)\s*\+?\s*years?\s*(?:of\s+)?(?:experience|nursing|working)',
            cv
        )
        if _m2:
            total_exp = f"{_m2.group(1)} years"

    return nationality, total_exp


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
    'pcc':         'others_2',
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
{extracted_cv}
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
{extracted_cv}
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

def _ai_pcc_col():
    return db.live_staff_ai_pcc

def _screening_col():
    return db.live_staff_screening


# ── AI CV DOCX builder ────────────────────────────────────────────────

def _build_ai_cv_docx(doc, cv_text):
    """
    Build an ATS-friendly, clean CV DOCX from AI-generated text.
    No colours, no tables, no images — plain text with simple formatting
    that passes all ATS parsers.
    """
    import io as _io
    from docx import Document as _Doc
    from docx.shared import Pt, Cm
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    SECTION_HEADINGS = {
        'EMPLOYMENT ELIGIBILITY', 'PROFESSIONAL PROFILE',
        'EDUCATION & QUALIFICATIONS', 'PROFESSIONAL EXPERIENCE',
        'TRAINING & CERTIFICATIONS', 'KEY SKILLS', 'ADDITIONAL INFORMATION',
    }

    document = _Doc()
    for sec in document.sections:
        sec.top_margin    = Cm(2.0)
        sec.bottom_margin = Cm(2.0)
        sec.left_margin   = Cm(2.5)
        sec.right_margin  = Cm(2.5)

    # Use Calibri — best ATS font
    style = document.styles['Normal']
    style.font.name = 'Calibri'
    style.font.size = Pt(11)

    s1        = doc.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    user_type = _v(doc.get('user_type') or '')
    email     = _v(doc.get('email') or s1.get('email_address') or '')
    phone     = _v(s1.get('mobile_number') or s1.get('phone_number') or '')

    # ── Name header — large, bold, left-aligned ───────────────────────
    p = document.add_paragraph()
    p.paragraph_format.space_before = Pt(0)
    p.paragraph_format.space_after  = Pt(0)
    run = p.add_run(full_name if full_name else 'Curriculum Vitae')
    run.bold = True
    run.font.name = 'Calibri'
    run.font.size = Pt(22)

    # Role title
    if user_type:
        p2 = document.add_paragraph()
        p2.paragraph_format.space_before = Pt(2)
        p2.paragraph_format.space_after  = Pt(2)
        r2 = p2.add_run(user_type)
        r2.font.name = 'Calibri'
        r2.font.size = Pt(12)
        r2.italic    = True

    # Contact line — phone + email
    contact_parts = [x for x in [phone, email] if x]
    if contact_parts:
        pc = document.add_paragraph()
        pc.paragraph_format.space_before = Pt(2)
        pc.paragraph_format.space_after  = Pt(6)
        rc = pc.add_run('  |  '.join(contact_parts))
        rc.font.name = 'Calibri'
        rc.font.size = Pt(10)

    # ── Section heading — bold uppercase with bottom border ───────────
    def _heading(text):
        p = document.add_paragraph()
        p.paragraph_format.space_before = Pt(12)
        p.paragraph_format.space_after  = Pt(4)
        run = p.add_run(text.upper())
        run.bold      = True
        run.font.name = 'Calibri'
        run.font.size = Pt(11)
        # Simple bottom border — ATS safe
        pPr = p._p.get_or_add_pPr()
        pBdr = OxmlElement('w:pBdr')
        bot = OxmlElement('w:bottom')
        bot.set(qn('w:val'), 'single')
        bot.set(qn('w:sz'), '6')
        bot.set(qn('w:color'), '000000')
        pBdr.append(bot)
        pPr.append(pBdr)

    # ── Body line ─────────────────────────────────────────────────────
    def _body(text, bold=False, indent=False):
        p = document.add_paragraph()
        p.paragraph_format.space_before = Pt(1)
        p.paragraph_format.space_after  = Pt(1)
        if indent:
            p.paragraph_format.left_indent = Cm(0.5)
        run = p.add_run(text)
        run.bold      = bold
        run.font.name = 'Calibri'
        run.font.size = Pt(11)

    # ── Strip duplicate header lines Gemini repeats ──────────────────
    # The DB header already has: Name (large) / Role (italic) / phone+email
    # Gemini often repeats the name and role at top of cv_text — strip those.
    # Keep contact lines (phone | address) — they come from the CV and are fine.
    import re as _re_cv

    def _is_duplicate_header(line):
        s  = line.strip()
        sl = s.lower()
        if not s:
            return False
        # Exact name match
        if full_name and sl == full_name.lower():
            return True
        # Exact role match
        if user_type and sl == user_type.lower():
            return True
        # Name + role on one line
        if full_name and user_type and full_name.lower() in sl and user_type.lower() in sl:
            return True
        return False

    _lines     = cv_text.split('\n')
    _first_sec = next((i for i, l in enumerate(_lines)
                       if l.strip().upper() in SECTION_HEADINGS), None)
    _clean_lines = []
    for idx, line in enumerate(_lines):
        if _first_sec is not None and idx < _first_sec:
            if _is_duplicate_header(line):
                continue
        _clean_lines.append(line)
    cv_text = '\n'.join(_clean_lines)

    # ── Parse and render cv_text ──────────────────────────────────────
    for line in cv_text.split('\n'):
        stripped = line.strip()
        if not stripped:
            # Small gap between entries
            p = document.add_paragraph()
            p.paragraph_format.space_before = Pt(2)
            p.paragraph_format.space_after  = Pt(2)
            continue

        if stripped.upper() in SECTION_HEADINGS:
            _heading(stripped)
        elif stripped.startswith('- ') or stripped.startswith('• '):
            # Bullet point — use simple dash for ATS
            _body('- ' + stripped[2:].strip(), indent=True)
        elif ':' in stripped and stripped.split(':')[0].isupper() and len(stripped.split(':')[0]) < 30:
            # Label: Value line (e.g. "Job Title: Staff Nurse")
            label, _, value = stripped.partition(':')
            p = document.add_paragraph()
            p.paragraph_format.space_before = Pt(1)
            p.paragraph_format.space_after  = Pt(1)
            r1 = p.add_run(label.strip() + ': ')
            r1.bold = True; r1.font.name = 'Calibri'; r1.font.size = Pt(11)
            r2 = p.add_run(value.strip())
            r2.font.name = 'Calibri'; r2.font.size = Pt(11)
        else:
            _body(stripped)

    buf = _io.BytesIO()
    document.save(buf)
    return buf.getvalue()


# ── AI Interview DOCX builder ─────────────────────────────────────────

def _build_ai_interview_docx(doc, interview_text):
    """
    Build interview notes DOCX matching the exact design from sample:
    - Title: bold 16pt
    - Header fields (Name/Location/NMBI PIN/Visa Status): bold label + normal value
    - Section headings (Experience/Clinical Questions/Compliance/Availability/Assessment): bold 13pt
    - Questions (numbered): bold 11pt
    - Answers: normal 11pt
    - Compliance/Assessment/Availability fields: bold label + bold value
    """
    import io as _io
    from docx import Document as _Doc
    from docx.shared import Pt, Cm

    document = _Doc()
    for sec in document.sections:
        sec.top_margin    = Cm(2.0)
        sec.bottom_margin = Cm(2.0)
        sec.left_margin   = Cm(2.5)
        sec.right_margin  = Cm(2.5)
    document.styles['Normal'].font.name = 'Calibri'
    document.styles['Normal'].font.size = Pt(11)

    # Section headings from the template
    SECTION_HEADINGS = {
        'experience', 'clinical questions', 'compliance',
        'availability', 'assessment',
    }

    # Fields where both label AND value are bold
    BOLD_VALUE_PREFIXES = (
        'nmbi registration:', 'bls/cpr:', 'manual handling:',
        'garda vetting:', 'references:', 'communication:',
        'clinical knowledge:', 'experience:', 'suitable:',
        'day/night/both:',
    )

    # Fields where label is bold, value is normal
    BOLD_LABEL_PREFIXES = (
        'name:', 'location:', 'nmbi pin:', 'visa status:',
        'preferred counties:', 'earliest start date:',
    )

    def _add_label_value(text, bold_value=False):
        """Add a paragraph with bold label: normal/bold value."""
        if ':' not in text:
            p = document.add_paragraph()
            p.paragraph_format.space_before = Pt(1)
            p.paragraph_format.space_after  = Pt(1)
            run = p.add_run(text)
            run.bold = True
            run.font.name = 'Calibri'
            return
        label, _, value = text.partition(':')
        p = document.add_paragraph()
        p.paragraph_format.space_before = Pt(1)
        p.paragraph_format.space_after  = Pt(1)
        r1 = p.add_run(label + ': ')
        r1.bold = True
        r1.font.name = 'Calibri'
        r2 = p.add_run(value.strip())
        r2.bold = bold_value
        r2.font.name = 'Calibri'

    lines = interview_text.split('\n')
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        i += 1

        if not stripped or stripped == '---':
            if stripped == '---':
                pass  # skip separator lines
            else:
                p = document.add_paragraph()
                p.paragraph_format.space_before = Pt(2)
                p.paragraph_format.space_after  = Pt(2)
            continue

        stripped_lower = stripped.lower()

        # Title line — "Completed X Interview"
        if stripped_lower.startswith('completed ') and 'interview' in stripped_lower:
            p = document.add_paragraph()
            p.paragraph_format.space_before = Pt(0)
            p.paragraph_format.space_after  = Pt(6)
            run = p.add_run(stripped)
            run.bold = True
            run.font.size = Pt(16)
            run.font.name = 'Calibri'
            continue

        # Section headings
        if stripped_lower in SECTION_HEADINGS:
            p = document.add_paragraph()
            p.paragraph_format.space_before = Pt(10)
            p.paragraph_format.space_after  = Pt(4)
            run = p.add_run(stripped)
            run.bold = True
            run.font.size = Pt(13)
            run.font.name = 'Calibri'
            continue

        # Numbered questions e.g. "1. Tell me about..."
        if len(stripped) > 2 and stripped[0].isdigit() and stripped[1] in '.':
            p = document.add_paragraph()
            p.paragraph_format.space_before = Pt(6)
            p.paragraph_format.space_after  = Pt(2)
            run = p.add_run(stripped)
            run.bold = True
            run.font.size = Pt(11)
            run.font.name = 'Calibri'
            continue

        # Bold label + bold value fields (Compliance/Assessment)
        if any(stripped_lower.startswith(pfx) for pfx in BOLD_VALUE_PREFIXES):
            _add_label_value(stripped, bold_value=True)
            continue

        # Bold label + normal value fields (header fields / Availability)
        if any(stripped_lower.startswith(pfx) for pfx in BOLD_LABEL_PREFIXES):
            _add_label_value(stripped, bold_value=False)
            continue

        # Normal answer text
        p = document.add_paragraph()
        p.paragraph_format.space_before = Pt(1)
        p.paragraph_format.space_after  = Pt(1)
        run = p.add_run(stripped)
        run.font.size = Pt(11)
        run.font.name = 'Calibri'

    buf = _io.BytesIO()
    document.save(buf)
    return buf.getvalue()


# ── AI Appform DOCX builder ───────────────────────────────────────────

def _build_ai_appform_docx(doc, appform_text):
    """Build application form DOCX from AI-generated text."""
    # Reuse same pattern as interview docx
    return _build_ai_interview_docx(doc, appform_text)


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
        Add a tick/box using Unicode ballot box characters.
        Segoe UI Symbol renders ☑/☐ correctly in both Word and LibreOffice.
        """
        r = para.add_run('☑' if checked else '☐')
        r.font.name = 'Segoe UI Symbol'
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
        # ── Fill missing nationality / total_exp from extracted_cv ────
        extracted_cv_temp = _v(doc.get('extracted_cv') or '')
        entries_temp = [e for e in (s5.get('entries') or []) if e.get('employer') or e.get('position')]
        nationality, total_exp = _extract_missing_fields(nationality, total_exp, extracted_cv_temp, entries_temp)
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
{extracted_cv}
""" if has_extracted_cv else ""

        prompt = f"""You are a professional CV writer specializing in Irish healthcare recruitment.
Your task is to rewrite the candidate's CV into a clean, ATS-friendly, professional CV using ONLY the information provided.

STRICT RULES
* NEVER invent, assume, enhance, or rewrite information that is not present.
* Use ONLY information from:
   1. CANDIDATE DATA
   2. CANDIDATE'S ORIGINAL CV
* Preserve employer names, job titles, dates, education, duties, certificates and skills exactly as provided.
* Use professional formatting and grammar while keeping the original meaning.
* Omit any section if no information is available.

CV FORMAT

Candidate Name
* Display the candidate's FULL NAME at the very top.
* Centre align the name.
* Use a larger heading than the rest of the document.
Immediately below the name (centre aligned), display:
Mobile: | Address:
(Do NOT include Email — it is shown separately above)
Only display fields that are available in the provided data.

EMPLOYMENT ELIGIBILITY
Display each item as: Label: Value
Example fields: Employment Type | Visa Status | Nationality | Current Location | Notice Period | Driving Licence | Own Transport | Healthcare Registration | Years of Experience
Rules:
* Use the values from CANDIDATE DATA whenever available.
* If Nationality is blank in CANDIDATE DATA:
   - Look for "Nationality:" in the original CV.
   - Otherwise infer from education and employment history only if strongly supported.
* If Years of Experience is blank:
   - Calculate from the earliest employment start date up to today. Ignore education dates.
   - Format as: X years Y months
* Skip any field that cannot be determined.

PROFESSIONAL PROFILE
Write a concise professional summary using ONLY the information contained in the CV.
Do not invent skills, experience or achievements.

EDUCATION & QUALIFICATIONS
Copy ALL education exactly as written. For each qualification include:
- Qualification
- Institution
- Location (if available)
- Dates

PROFESSIONAL EXPERIENCE
List ALL employment in reverse chronological order. For every role include:
- Job Title
- Employer
- Location (if available)
- Employment Dates
- Bullet-point responsibilities exactly as stated or lightly formatted for readability.
Do not remove any employment. Do not create new responsibilities.

TRAINING & CERTIFICATIONS
List every certificate, mandatory training course, licence and professional training mentioned in the CV.

KEY SKILLS
List all skills explicitly mentioned in the CV. Use bullet points. Do not invent additional skills.

ADDITIONAL INFORMATION
Display:
Driving Licence: No
Own Transport: No
If the original CV explicitly states different values, use those instead.

---
CANDIDATE DATA:
{data_summary}

CANDIDATE'S ORIGINAL CV:
{extracted_cv if has_extracted_cv else "No CV available — build from CANDIDATE DATA above."}
---

Output the structured CV text only. No markdown, no preamble.
"""

        from google import genai as google_genai
        client   = google_genai.Client(api_key=_gemini_key)
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
            "ai_cv_id":     ai_id,
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
            # 1. live_staffs.staff_id (XN Portal ID) — primary
            doc = _staffs_col().find_one({"staff_id": staff_id})
            # 2. xn_staff_id
            if not doc:
                doc = _staffs_col().find_one({"xn_staff_id": staff_id})
            # 3. MongoDB _id
            if not doc:
                try:
                    doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
                except Exception:
                    pass
            # 4. employee_code
            if not doc:
                doc = _staffs_col().find_one({"employee_code": staff_id})
        if not doc and email:
            doc = _staffs_col().find_one({"$or": [
                {"email": email},
                {"section_1_personal_details.email_address": email},
            ]})
        if not doc:
            identifier = staff_id or email
            return jsonify({"success": False,
                            "error": f"Staff not found (staff_id={staff_id or 'n/a'}, email={email or 'n/a'})"}), 404

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
        # ── Fill missing nationality / total_exp from extracted_cv ────
        extracted_cv_temp = _v(doc.get('extracted_cv') or '')
        entries_temp = [e for e in (s5.get('entries') or []) if e.get('employer') or e.get('position')]
        nationality, total_exp = _extract_missing_fields(nationality, total_exp, extracted_cv_temp, entries_temp)
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

        docx_bytes = _build_ai_interview_docx(doc, interview_text)
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



# ── API: Return document URLs for a staff member ──────────────────────

@admin_bp.route('/live-staffs/api/staff-documents', methods=['POST'])
def live_staff_api_staff_documents():
    """
    Return all document URLs for one staff member from XN Portal.
    Auth: cron_key param or X-Cron-Key header (same as cron endpoints).
    """
    import requests as _req

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', '') or
                    (request.get_json(silent=True) or {}).get('cron_key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    body     = request.get_json(silent=True) or {}
    staff_id = _v(body.get('staff_id') or '')
    email    = _v(body.get('email') or '').lower()

    if not staff_id and not email:
        return jsonify({"success": False, "error": "email or staff_id required"}), 400

    # ── Lookup staff ──────────────────────────────────────────────────
    col = _staffs_col()
    doc = None
    if staff_id:
        try:
            doc = col.find_one({"_id": ObjectId(staff_id)})
        except Exception:
            pass
    if not doc and email:
        doc = col.find_one({"$or": [
            {"email": email},
            {"section_1_personal_details.email_address": email},
        ]})
    if not doc:
        return jsonify({"success": False, "error": "Staff not found"}), 404

    s1         = doc.get('section_1_personal_details') or {}
    staff_name = _v(s1.get('full_name') or '')
    email      = email or _v(doc.get('email') or s1.get('email_address') or '')

    if not email:
        return jsonify({"success": False, "error": "Staff has no email"}), 400

    # ── Call XN Portal ────────────────────────────────────────────────
    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')

    if not base_url or not api_key:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL or XN_PORTAL_API_KEY not configured"}), 500

    hdrs = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        endpoint = f"{base_url}/ai/recruitments/user-document-list"
        r = _req.post(endpoint, json={"email": email}, headers=hdrs, timeout=30)
        if r.status_code == 405:
            r = _req.get(endpoint, params={"email": email}, headers=hdrs, timeout=30)
        r.raise_for_status()
        data     = r.json()
        api_data = data.get('data')
        raw_docs = api_data if isinstance(api_data, list) else                    (api_data.get('documents') or [] if isinstance(api_data, dict) else [])
    except Exception as e:
        return jsonify({"success": False, "error": f"Portal API error: {e}"}), 502

    # ── Build response with filename hints ────────────────────────────
    import re as _re2

    def _guess_ext(url):
        u = url.lower().split('?')[0]
        for ext in ('.pdf', '.docx', '.doc', '.jpg', '.jpeg', '.png', '.webp', '.heic'):
            if u.endswith(ext):
                return ext
        return '.pdf'  # default assumption

    documents = []
    for d in raw_docs:
        doc_type = _v(d.get('document_type_name') or 'Unknown')
        url      = _v(d.get('url') or '')
        if not url:
            continue
        ext      = _guess_ext(url)
        safe     = _re2.sub(r'[<>:"/\\|?*\s]+', '_', doc_type)[:60]
        documents.append({
            "document_type_name": doc_type,
            "url":                url,
            "filename":           safe + ext,
            "extension":          ext,
            "document_id":        d.get('id') or d.get('_id') or '',
        })

    return jsonify({
        "success":    True,
        "email":      email,
        "staff_name": staff_name,
        "staff_id":   str(doc['_id']),
        "count":      len(documents),
        "documents":  documents,
    })


@admin_bp.route('/live-staffs/api/all-staff-emails', methods=['GET'])
def live_staff_api_all_emails():
    """
    Return ONE unprocessed HCA staff at a time.
    Marks the staff as point_scale_doc_downloaded=True atomically.
    Local script calls this in a loop.
    Auth: cron_key
    """
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    doc = _staffs_col().find_one_and_update(
        {
            "user_type": {"$regex": "healthcare assistant|health care assistant|\\bhca\\b|care assistant",
                          "$options": "i"},
            "point_scale_doc_downloaded": {"$ne": True},
            "$or": [
                {"email": {"$exists": True, "$ne": ""}},
                {"section_1_personal_details.email_address": {"$exists": True, "$ne": ""}},
            ]
        },
        {"$set": {"point_scale_doc_downloaded": True}},
        projection={"email": 1, "section_1_personal_details": 1,
                    "employee_code": 1, "user_type": 1}
    )

    if not doc:
        return jsonify({"success": True, "done": True,
                        "message": "All HCA staff processed"})

    s1 = doc.get('section_1_personal_details') or {}
    return jsonify({
        "success":   True,
        "done":      False,
        "staff_id":  str(doc['_id']),
        "email":     _v(doc.get('email') or s1.get('email_address') or ''),
        "name":      _v(s1.get('full_name') or ''),
        "user_type": _v(doc.get('user_type') or ''),
    })


# ── API: Generate CV ──────────────────────────────────────────────────

@admin_bp.route('/live-staffs/api/generate-cv', methods=['POST'])
def live_staff_api_generate_cv():
    """External API — generate AI CV. Auth: X-API-Key header."""
    ok, err = _validate_api_key()
    if not ok:
        return err

    body     = request.get_json(silent=True) or {}
    staff_id = _v(body.get('staff_id') or '')
    email    = _v(body.get('email') or '').lower()

    if not staff_id and not email:
        return jsonify({"success": False, "error": "staff_id or email required"}), 400

    try:
        col = _staffs_col()
        doc = None
        if staff_id:
            # 1. Match live_staffs.staff_id (XN Portal staff ID) — primary
            doc = col.find_one({"staff_id": staff_id})
            # 2. Match live_staffs.xn_staff_id
            if not doc:
                doc = col.find_one({"xn_staff_id": staff_id})
            # 3. Match MongoDB _id
            if not doc:
                try:
                    doc = col.find_one({"_id": ObjectId(staff_id)})
                except Exception:
                    pass
            # 4. Match employee_code
            if not doc:
                doc = col.find_one({"employee_code": staff_id})

        if not doc and email:
            doc = col.find_one({"$or": [
                {"email": email},
                {"section_1_personal_details.email_address": email},
            ]})

        if not doc:
            return jsonify({
                "success": False,
                "error": f"Staff not found (staff_id={staff_id or 'n/a'}, email={email or 'n/a'})"
            }), 404

        staff_id  = str(doc['_id'])
        s1        = doc.get('section_1_personal_details') or {}
        full_name = _v(s1.get('full_name') or '')
        email     = email or _v(doc.get('email') or s1.get('email_address') or '')
        emp_code  = _v(doc.get('employee_code') or '')
        user_type = _v(doc.get('user_type') or '')

        gemini_key = os.environ.get('GEMINI_API_KEY', '')
        if not gemini_key:
            return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

        s3 = doc.get('section_3_professional_registration') or {}
        s4 = doc.get('section_4_qualifications') or {}
        s5 = doc.get('section_5_employment_history') or {}
        visa = s1.get('work_permit_visa_status') or {}
        def _vv(v): return '' if v is None else str(v).strip()

        nationality = _vv(s1.get('nationality'))
        total_exp   = _vv(s5.get('total_experience'))
        reg_pin     = _vv(s3.get('registration_number_pin'))
        visa_type   = _vv(visa.get('visa_type'))
        perm_work   = _vv(visa.get('permission_to_work'))
        divisions   = ', '.join(s3.get('divisions_registered_in') or [])
        nmbi        = 'Yes' if s3.get('nmbi_active_declaration') else 'No'

        entries   = [e for e in (s5.get('entries') or []) if e.get('employer') or e.get('position')]
        exp_lines = [f"  - {_vv(e.get('position'))} at {_vv(e.get('employer'))} ({_vv(e.get('from'))} - {_vv(e.get('to') or 'Present')})" for e in entries]

        extracted_cv = _v(doc.get('extracted_cv') or '')

        # ── Always fetch Hse Cv from XN Portal (primary source) ──────
        _base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
        _api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
        _app_country = os.environ.get('XN_APP_COUNTRY', '')
        if _base_url and email:
            try:
                import requests as _rq
                _hdrs = {"Api-Key": _api_key, "X-App-Country": _app_country,
                         "Content-Type": "application/json", "Accept": "application/json"}
                _r = _rq.post(f"{_base_url}/ai/recruitments/user-document-list",
                              json={"email": email}, headers=_hdrs, timeout=30)
                if _r.status_code == 405:
                    _r = _rq.get(f"{_base_url}/ai/recruitments/user-document-list",
                                 params={"email": email}, headers=_hdrs, timeout=30)
                if _r.status_code == 200:
                    _pd   = _r.json().get('data')
                    _dls  = _pd if isinstance(_pd, list) else (_pd.get('documents') or [] if isinstance(_pd, dict) else [])
                    _cv_url = next(
                        (_d['url'] for _d in _dls
                         if (_d.get('document_type_name') or '').strip().lower() in ('hse cv','hse_cv','hsecv')
                         and _d.get('url')), None
                    )
                    if _cv_url:
                        import io as _cv_io
                        _dl_hdrs = {k: v for k, v in _hdrs.items() if k != 'Content-Type'}
                        _dl = _rq.get(_cv_url, headers=_dl_hdrs, timeout=60)
                        _dl.raise_for_status()
                        _raw = _dl.content
                        _ct  = _dl.headers.get('Content-Type','').lower()
                        _ul  = _cv_url.lower().split('?')[0]
                        _rtxt = ''
                        # PDF
                        if 'pdf' in _ct or _ul.endswith('.pdf'):
                            try:
                                import pdfplumber as _plmb
                                with _plmb.open(_cv_io.BytesIO(_raw)) as _pdf:
                                    _rtxt = '\n'.join(p.extract_text() or '' for p in _pdf.pages).strip()
                            except Exception: pass
                        # DOCX
                        if not _rtxt and ('wordprocessingml' in _ct or _ul.endswith('.docx')):
                            try:
                                from docx import Document as _DDoc
                                _ddoc = _DDoc(_cv_io.BytesIO(_raw))
                                _rtxt = '\n'.join(p.text for p in _ddoc.paragraphs).strip()
                            except Exception: pass
                        # Gemini vision fallback
                        if not _rtxt:
                            try:
                                import base64 as _b64
                                from google import genai as _gai_hse
                                _gc = _gai_hse.Client(api_key=gemini_key)
                                _gr = _gc.models.generate_content(
                                    model='gemini-2.5-flash',
                                    contents=[{"parts": [
                                        {"inline_data": {"mime_type": "application/pdf",
                                                         "data": _b64.b64encode(_raw).decode()}},
                                        {"text": "Extract all text from this CV. Return plain text only."}
                                    ]}]
                                )
                                _rtxt = (_gr.text or '').strip()
                            except Exception: pass
                        if _rtxt:
                            extracted_cv = _rtxt
                            _staffs_col().update_one(
                                {"_id": doc['_id']},
                                {"$set": {"extracted_cv": _rtxt,
                                          "extracted_cv_at": datetime.utcnow(),
                                          "extracted_cv_source": "hse_cv_api_generate"}}
                            )
            except Exception:
                pass  # non-fatal — continue with existing extracted_cv

        has_cv = bool(extracted_cv and not extracted_cv.startswith('[') and extracted_cv not in ('No doc found',''))
        nationality, total_exp = _extract_missing_fields(nationality, total_exp, extracted_cv, entries)

        qual_lines = []
        for qk in ['nursing_degree', 'postgraduate_qualification', 'other_qualification']:
            q = s4.get(qk) or {}
            if q.get('qualification') or q.get('institution'):
                qual_lines.append(f"  - {_vv(q.get('qualification'))} | {_vv(q.get('institution'))} | {_vv(q.get('year_completed'))}")
        if not qual_lines:
            _rl = user_type.lower()
            if any(t in _rl for t in ('nurse','rgn','midwife')):
                qual_lines.append('  - Bachelor of Nursing Science (or equivalent) | University College | [year estimated]')
            elif any(t in _rl for t in ('hca','healthcare assistant','care worker','support worker')):
                qual_lines.append('  - QQI Level 5 in Healthcare Support | College of Further Education | [year estimated]')

        data_summary = f"""Name: {full_name}
Role / User Type: {user_type}
Nationality: {nationality}
Visa / Stamp Type: {visa_type}
Permission to Work: {perm_work}
NMBI Registration PIN: {reg_pin}
Divisions / Speciality: {divisions}
Total Experience: {total_exp}
NMBI Active: {nmbi}
Qualifications:
{chr(10).join(qual_lines) if qual_lines else '  None recorded'}
Employment History:
{chr(10).join(exp_lines) if exp_lines else '  None recorded'}""".strip()

        prompt = f"""You are a professional CV writer specializing in Irish healthcare recruitment.
Your task is to rewrite the candidate's CV into a clean, ATS-friendly, professional CV using ONLY the information provided.

STRICT RULES
* NEVER invent, assume, enhance, or rewrite information that is not present.
* Use ONLY information from CANDIDATE DATA and CANDIDATE'S ORIGINAL CV.
* Preserve employer names, job titles, dates, education, duties, certificates and skills exactly as provided.

CV FORMAT
Candidate Name (centred, large heading)
Mobile | Address (centred, only available fields — do NOT include Email)

EMPLOYMENT ELIGIBILITY
Label: Value per line. Do NOT include name, address, mobile or email.
For blank fields: Nationality — check CV; Years of Experience — calculate from earliest employment date to today (X years Y months).

PROFESSIONAL PROFILE — concise summary from CV only.

EDUCATION & QUALIFICATIONS — copy ALL education exactly (Qualification, Institution, Location, Dates).

PROFESSIONAL EXPERIENCE — ALL jobs in reverse chronological order (Job Title, Employer, Location, Dates, bullet responsibilities).

TRAINING & CERTIFICATIONS — every certificate and training from the CV.

KEY SKILLS — all skills explicitly mentioned. No invented skills.

ADDITIONAL INFORMATION
Driving Licence: No
Own Transport: No

---
CANDIDATE DATA:
{data_summary}

CANDIDATE'S ORIGINAL CV:
{extracted_cv if has_cv else "No CV available — build from CANDIDATE DATA above."}
---
Output the structured CV text only. No markdown, no preamble.
"""

        from google import genai as _gai
        client   = _gai.Client(api_key=gemini_key)
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        cv_text  = response.text.strip()

        # Re-extract CV from portal before generating
        _base_url = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
        _api_key  = os.environ.get('XN_PORTAL_API_KEY', '')
        _country  = os.environ.get('XN_APP_COUNTRY', '')
        if _base_url and email:
            try:
                import requests as _rq
                _hdrs = {"Api-Key": _api_key, "X-App-Country": _country,
                         "Content-Type": "application/json", "Accept": "application/json"}
                _r = _rq.post(f"{_base_url}/ai/recruitments/user-document-list",
                              json={"email": email}, headers=_hdrs, timeout=15)
                if _r.status_code == 200:
                    _pd  = _r.json().get('data')
                    _dls = _pd if isinstance(_pd, list) else (_pd.get('documents') or [] if isinstance(_pd, dict) else [])
                    _cv_url = next((_d['url'] for _d in _dls if (_d.get('document_type_name') or '').strip() == 'Cv' and _d.get('url')), None)
                    if _cv_url:
                        from admin.live_staffs_crons import _extract_text_from_url
                        _et = _extract_text_from_url(_cv_url, {k: v for k, v in _hdrs.items() if k != 'Content-Type'})
                        if _et and not _et.startswith('['):
                            extracted_cv = _et
                            has_cv = True
                            _staffs_col().update_one({"_id": doc['_id']}, {"$set": {"extracted_cv": _et, "extracted_cv_at": datetime.utcnow()}})
            except Exception:
                pass

        docx_bytes  = _build_ai_cv_docx(doc, cv_text)
        safe_name   = (full_name or 'staff').replace(' ', '_').replace('/', '_')
        cv_filename = f"{safe_name}.docx"
        gcs_blob    = f"cv/{cv_filename}"
        _gcs_upload(gcs_blob, docx_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        col2     = _ai_cvs_col()
        existing = col2.find_one({"staff_id": staff_id})
        ai_doc   = {"staff_id": staff_id, "staff_name": full_name,
                    "employee_code": emp_code, "cv_text": cv_text,
                    "cv_filename": cv_filename, "gcs_blob": gcs_blob,
                    "generated_at": datetime.utcnow()}
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
            "success": True, "staff_id": staff_id,
            "cv_id": ai_id, "ai_cv_id": ai_id,
            "cv_filename": cv_filename, "gcs_blob": gcs_blob,
            "generated_at": datetime.utcnow().isoformat(),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



@admin_bp.route('/live-staffs/api/nurse-list')
def live_staff_api_nurse_list():
    """
    Return all nurse-type staff with name, email, user_type.
    Auth: cron_key — used by the local NMBI register checker script.
    """
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    nurse_regex = (
        "nurse|rgn|rnm|midwife|nchd|staff nurse|clinical nurse|"
        "registered nurse|community nurse|public health nurse|"
        "theatre nurse|icu nurse|phn|cns|cnm|rnid"
    )
    docs = list(_staffs_col().find(
        {"user_type": {"$regex": nurse_regex, "$options": "i"}},
        {"section_1_personal_details": 1, "email": 1, "user_type": 1}
    ))

    staff = []
    for d in docs:
        s1 = d.get('section_1_personal_details') or {}
        staff.append({
            "staff_name": _v(s1.get('full_name') or ''),
            "email":      _v(d.get('email') or s1.get('email_address') or ''),
            "user_type":  _v(d.get('user_type') or ''),
        })

    return jsonify({"success": True, "count": len(staff), "staff": staff})



@admin_required
def live_staff_export_special_flag_xlsx():
    """
    Export ALL staff with: Staff Name | Email | Special (1 or 0).

    GET /admin/live-staffs/export/special-flag-xlsx
    """
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1, "special": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY  = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT   = 'EFF6FF'

        h_font  = Font(name='Calibri', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Calibri', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        g_bot   = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'Special Flag'

        headers    = ['Sno', 'Staff Name', 'Email', 'Special']
        col_widths = [5, 32, 38, 10]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = g_bot
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:D{len(docs)+1}'

        for ri, doc in enumerate(docs, start=2):
            s1    = doc.get('section_1_personal_details') or {}
            name  = _v(s1.get('full_name') or '')
            email = _v(doc.get('email') or s1.get('email_address') or '')
            special_val = doc.get('special')
            special     = 1 if special_val == 1 or special_val == '1' or special_val is True else 0

            row_vals = [ri - 1, name, email, special]
            aligns   = [c_align, l_align, l_align, c_align]
            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT) if ri % 2 == 0 else PatternFill()

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border; cell.fill = alt_fill
            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Calibri', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="special_flag_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
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





# ── AI CV saved/download/upload routes ───────────────────────────────

@admin_bp.route('/live-staffs/ai-cv/saved/<staff_id>')
@admin_required
def live_staff_ai_cv_saved(staff_id):
    rec = _ai_cvs_col().find_one({"staff_id": staff_id})
    if rec:
        return jsonify({
            "success":      True,
            "found":        True,
            "ai_cv_id":     str(rec["_id"]),
            "cv_id":        str(rec["_id"]),
            "cv_filename":  rec.get("cv_filename", ""),
            "gcs_blob":     rec.get("gcs_blob", ""),
            "generated_at": rec["generated_at"].strftime("%d %b %Y %H:%M") if rec.get("generated_at") else "",
        })
    return jsonify({"success": True, "found": False})


@admin_bp.route('/live-staffs/ai-cv/download/<ai_cv_id>')
@admin_required
def live_staff_ai_cv_download(ai_cv_id):
    try:
        rec = _ai_cvs_col().find_one({"_id": ObjectId(ai_cv_id)})
        if not rec or not rec.get('gcs_blob'):
            return jsonify({"success": False, "error": "CV not found"}), 404
        docx_bytes = _gcs_download(rec['gcs_blob'])
        filename   = rec.get('cv_filename') or 'cv.docx'
        return Response(
            docx_bytes,
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/ai-cv/upload/<staff_id>', methods=['POST'])
@admin_required
def live_staff_ai_cv_upload(staff_id):
    f = request.files.get('file')
    if not f:
        return jsonify({"success": False, "error": "No file uploaded"}), 400
    try:
        doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
        if not doc:
            return jsonify({"success": False, "error": "Staff not found"}), 404
        s1        = doc.get('section_1_personal_details') or {}
        full_name = _v(s1.get('full_name') or 'staff')
        safe_name = full_name.replace(' ', '_').replace('/', '_')
        filename  = f"CV_{safe_name}.docx"
        gcs_blob  = f"cv/{filename}"
        docx_bytes = f.read()
        _gcs_upload(gcs_blob, docx_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
        _ai_cvs_col().update_one(
            {"staff_id": staff_id},
            {"$set": {"gcs_blob": gcs_blob, "cv_filename": filename, "generated_at": datetime.utcnow()}},
            upsert=True
        )
        return jsonify({"success": True, "gcs_blob": gcs_blob, "cv_filename": filename})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── AI Interview routes ───────────────────────────────────────────────

@admin_bp.route('/live-staffs/ai-interview/saved/<staff_id>')
@admin_required
def live_staff_ai_interview_saved(staff_id):
    rec = _ai_interviews_col().find_one({"staff_id": staff_id})
    if rec:
        return jsonify({
            "success":      True,
            "found":        True,
            "ai_id":        str(rec["_id"]),
            "interview_id": str(rec["_id"]),
            "filename":     rec.get("filename", ""),
            "gcs_blob":     rec.get("gcs_blob", ""),
            "generated_at": rec["generated_at"].strftime("%d %b %Y %H:%M") if rec.get("generated_at") else "",
        })
    return jsonify({"success": True, "found": False})


@admin_bp.route('/live-staffs/ai-interview/download/<ai_id>')
@admin_required
def live_staff_ai_interview_download(ai_id):
    try:
        rec = _ai_interviews_col().find_one({"_id": ObjectId(ai_id)})
        if not rec or not rec.get('gcs_blob'):
            return jsonify({"success": False, "error": "Interview notes not found"}), 404
        docx_bytes = _gcs_download(rec['gcs_blob'])
        return Response(
            docx_bytes,
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            headers={"Content-Disposition": f'attachment; filename="{rec.get("filename", "interview.docx")}"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/ai-interview/upload/<staff_id>', methods=['POST'])
@admin_required
def live_staff_ai_interview_upload(staff_id):
    f = request.files.get('file')
    if not f:
        return jsonify({"success": False, "error": "No file"}), 400
    try:
        doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
        if not doc:
            return jsonify({"success": False, "error": "Staff not found"}), 404
        s1        = (doc.get('section_1_personal_details') or {})
        full_name = _v(s1.get('full_name') or 'staff')
        safe_name = full_name.replace(' ', '_').replace('/', '_')
        filename  = f"Interview_{safe_name}.docx"
        gcs_blob  = f"interview/{filename}"
        _gcs_upload(gcs_blob, f.read(),
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')
        _ai_interviews_col().update_one(
            {"staff_id": staff_id},
            {"$set": {"gcs_blob": gcs_blob, "filename": filename, "generated_at": datetime.utcnow()}},
            upsert=True
        )
        return jsonify({"success": True, "gcs_blob": gcs_blob})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── AI Appform saved/download/upload ──────────────────────────────────


# ══════════════════════════════════════════════════════════════════════
# SCREENING INTERVIEW RECORD
# ══════════════════════════════════════════════════════════════════════

# One of these is picked at random for each generated record — used for
# both "Screening Interview Conducted By" / "Title" (header) and
# "Interviewer" (bottom assessment row).
_SCREENING_INTERVIEWERS = [
    {"name": "Victor Cornel",      "title": "Head of Nursing & Clinical Manager"},
    {"name": "Pomin Ponselvan",    "title": "Nursing Manager"},
    {"name": "Namitha Ponselvan",  "title": "Nursing Supervisor"},
    {"name": "Jijo Jerlin",        "title": "Clinical Officer"},
]

# Correct option letter for each of the 10 Knowledge Check MCQ questions,
# in document order — matches the HCA Questionnaire Answer Key.
_SCREENING_MCQ_CORRECT = ['B', 'B', 'D', 'C', 'A', 'B', 'B', 'B', 'A', 'B']
_SCREENING_MCQ_LETTERS = ['A', 'B', 'C', 'D']

# Checkbox occurrence indices (0-based, in document order among all 44
# <w:sdt> checkbox controls) — fixed by the template's layout:
#   0-39  : the 40 MCQ option checkboxes (10 questions × A,B,C,D)
#   40,41 : Pass, Fail
#   42,43 : Suitable for Placement — Yes, No
_SCREENING_PASS_CHECKBOX_IDX = 40
_SCREENING_FAIL_CHECKBOX_IDX = 41
_SCREENING_SUITABLE_YES_CHECKBOX_IDX = 42
_SCREENING_SUITABLE_NO_CHECKBOX_IDX  = 43

# How many of the 10 MCQ questions are marked as answered correctly —
# the rest get a random wrong option checked instead.
_SCREENING_CORRECT_COUNT = 7

# Pool of reusable "Compliance Actions from Screening Interview" notes —
# from Xpress_Health_Screening_Compliance_Notes_50_Options.docx. 3-4 are
# picked at random (rarely all 5) to fill the template's 5 numbered rows.
_SCREENING_COMPLIANCE_NOTES = [
    "International Police Clearance Certificate (PCC) pending from country of previous residence.",
    "Garda Vetting application form pending submission.",
    "Garda Vetting process initiated; awaiting clearance outcome.",
    "Proof of address required to proceed with Garda Vetting.",
    "Name discrepancy noted between identification and vetting documents; clarification required.",
    "Candidate reports difficulty obtaining PCC; supporting explanation required for compliance review.",
    "Garda Vetting documentation received; status to be verified.",
    "Practical Manual Handling training required; certificate pending.",
    "Practical CPR/BLS training required.",
    "PPE training certificate pending verification.",
    "Fire Safety training evidence required.",
    "CPI/MAPA/PMAV training status to be verified against the placement requirements.",
    "HSEland mandatory training certificates require review.",
    "Pre-placement Occupational Health assessment pending.",
    "Occupational Health clearance required before allocation to the relevant setting.",
    "EPP/non-EPP fitness-to-work status to be confirmed by Occupational Health.",
    "Immunisation history and supporting evidence pending review.",
    "QQI Level 5 certificate required; confirm the accepted modules against the applicable checklist.",
    "Candidate has ongoing QQI studies; eligibility to be confirmed with the relevant client.",
    "NMBI registration details require verification before nurse allocation.",
    "Employment reference pending from the most recent employer.",
    "Employment dates on CV require clarification.",
    "Candidate to provide an updated CV with complete employment history.",
    "Candidate is available for weekends only.",
    "Candidate prefers morning shifts only.",
    "Candidate is available for day shifts only.",
    "Candidate prefers night shifts; availability to be matched with open shifts.",
    "Candidates have limited availability due to existing employment commitments.",
    "Candidate is willing to travel within the agreed service area.",
    "Candidate requires clarification on shift patterns and expected hours.",
    "Candidate meets the initial screening criteria; outstanding compliance items remain.",
]

# Pool of plausible "Reason for leaving" phrases for the Employment table
# — a random, different one is picked per past role (never for the
# candidate's current/ongoing role).
_SCREENING_LEAVING_REASONS = [
    "Limited career progression",
    "Long commute distance",
    "Poor work-life balance",
    "Unsuitable shift patterns",
    "Inadequate salary",
    "Limited specialty experience",
    "Narrow clinical exposure",
    "Unstable rostering",
    "Strained team environment",
    "Insufficient management support",
    "Inadequate staffing levels",
    "Limited training access",
    "Excessive workload pressure",
    "Lack of new challenges",
    "Unsupportive leadership culture",
    "Inadequate pay and benefits",
    "Unsuitable care setting",
    "Inflexible working hours",
    "Unclear promotion pathways",
    "Limited shift availability",
    "Delayed payment turnaround",
    "Poor communication support",
    "Inconsistent shift offers",
    "Placements too far away",
    "Unclear pay structure",
    "Late payroll processing",
    "Difficult onboarding experience",
    "Unresponsive coordinator support",
]


def _ongoing_role(to_value):
    """True if an employment entry's 'to' date implies the candidate is
    still working there (so it shouldn't get a fabricated leaving reason)."""
    t = (to_value or '').strip().lower()
    return t in ('', 'present', 'current', 'ongoing', 'till date', 'to date', 'now', 'date')


def _pick_compliance_actions(_random_mod):
    """3-4 compliance notes most of the time, rarely all 5."""
    count = _random_mod.choices([3, 4, 5], weights=[45, 45, 10])[0]
    count = min(count, len(_SCREENING_COMPLIANCE_NOTES))
    return _random_mod.sample(_SCREENING_COMPLIANCE_NOTES, count)


def _extract_education_and_employment(extracted_cv, gemini_key,
                                       max_education=5, max_employment=7):
    """
    Single Gemini call that pulls BOTH education and employment history
    out of a staff member's extracted_cv text — strictly from what's
    written there, no invented details.

    Returns a tuple (education_entries, employment_entries):
      education_entries: list of up to max_education dicts
        {"from": "...", "to": "...", "course": "..."}
      employment_entries: list of up to max_employment dicts
        {"from": "...", "to": "...", "employer": "...", "reason": "..."}

    Returns ([], []) if there's no usable CV text, no API key, or on error.
    """
    has_cv_text = bool(
        extracted_cv and
        not str(extracted_cv).startswith('[') and
        extracted_cv not in ('No doc found', '')
    )
    if not has_cv_text or not gemini_key:
        return [], []

    try:
        from google import genai as _gai_edu
        import re as _re_edu
        import json as _json_edu

        prompt = f"""You are analysing a candidate's CV text to extract their EDUCATION and EMPLOYMENT history.

Read the CV text below and extract:

1. EDUCATION — every degree, diploma, or certificate from a school, college,
   or university explicitly mentioned.
2. EMPLOYMENT — every job/role explicitly mentioned, in reverse chronological
   order (most recent first).

Rules:
- Use ONLY information explicitly present in the CV text below. Do NOT invent,
  guess, or infer any detail that is not written there.
- If a specific field is not stated for an entry, leave it as an empty string "".
- Order both lists from MOST RECENT to OLDEST.
- Return at most {max_education} education entries and {max_employment} employment entries.
- For employment "reason", only fill it if the CV explicitly states a reason
  for leaving that role — otherwise leave it as "".
- Return ONLY a single JSON object — nothing else, no markdown, no explanation —
  with exactly this shape:

{{
  "education": [
    {{"from": "<start year/date or empty>", "to": "<end year/date or empty>", "course": "<qualification, institution and/or course name>"}}
  ],
  "employment": [
    {{"from": "<start year/date or empty>", "to": "<end year/date or empty>", "employer": "<employer, role, location>", "reason": "<reason for leaving, or empty>"}}
  ]
}}

- If the CV contains no education information, "education" must be [].
- If the CV contains no employment information, "employment" must be [].

CV TEXT:
{extracted_cv}
"""
        client   = _gai_edu.Client(api_key=gemini_key)
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        raw      = (response.text or '').strip()
        raw      = _re_edu.sub(r'^```(?:json)?\s*', '', raw, flags=_re_edu.MULTILINE)
        raw      = _re_edu.sub(r'```\s*$', '', raw, flags=_re_edu.MULTILINE).strip()

        result = _json_edu.loads(raw)
        if not isinstance(result, dict):
            return [], []

        raw_education  = result.get('education') or []
        raw_employment = result.get('employment') or []

        education_entries = []
        for e in raw_education[:max_education]:
            if not isinstance(e, dict):
                continue
            education_entries.append({
                "from":   str(e.get("from") or "").strip(),
                "to":     str(e.get("to") or "").strip(),
                "course": str(e.get("course") or "").strip(),
            })

        employment_entries = []
        for e in raw_employment[:max_employment]:
            if not isinstance(e, dict):
                continue
            employment_entries.append({
                "from":     str(e.get("from") or "").strip(),
                "to":       str(e.get("to") or "").strip(),
                "employer": str(e.get("employer") or "").strip(),
                "reason":   str(e.get("reason") or "").strip(),
            })

        return education_entries, employment_entries
    except Exception:
        return [], []


def _build_screening_docx(first_shift_date=None, candidate_name='',
                           location='', candidate_id='', extracted_cv=''):
    """
    Load the Screening Interview Record template from GCS and inject:
      - Screening Interview Conducted By = a randomly chosen interviewer
      - Title                            = that interviewer's job title
      - Candidate Name                   = candidate_name (from live_staffs)
      - Candidate ID                     = candidate_id (live_staffs.employee_code)
      - Location                         = location (live_staffs.county)
      - Date                             = first_shift_date − 15 days  (or blank)
      - Candidate 1st Contact Date       = first_shift_date − 20 days  (or blank)
      - Interviewer (bottom assessment)  = same randomly chosen interviewer
      - Education table                 = up to 5 entries extracted via
                                            Gemini from live_staffs.extracted_cv

    All other template content and styling is preserved byte-for-byte.

    Template must be uploaded to GCS at the path set in env var
    SCREENING_TEMPLATE_GCS_BLOB (default: templates/Screening_Interview_Record.docx).
    Or set SCREENING_TEMPLATE_LOCAL_PATH to an absolute file path on disk.

    Returns bytes of the .docx file.
    """
    import io as _sio
    import zipfile as _szip
    import random as _random
    from datetime import timedelta as _std

    gemini_key = os.environ.get('GEMINI_API_KEY', '')

    # ── Load template ─────────────────────────────────────────────────
    template_blob  = os.environ.get(
        'SCREENING_TEMPLATE_GCS_BLOB',
        'templates/Screening_Interview_Record.docx'
    )
    template_local = os.environ.get('SCREENING_TEMPLATE_LOCAL_PATH', '')

    if template_local and os.path.exists(template_local):
        with open(template_local, 'rb') as _tf:
            template_bytes = _tf.read()
    else:
        template_bytes = _gcs_download(template_blob)

    # ── Parse first_shift_date ────────────────────────────────────────
    if isinstance(first_shift_date, str):
        try:
            first_shift_date = datetime.strptime(first_shift_date[:10], '%Y-%m-%d')
        except ValueError:
            first_shift_date = None

    interview_date = (
        (first_shift_date - _std(days=15)).strftime('%d-%m-%Y')
        if first_shift_date else ''
    )
    contact_date = (
        (first_shift_date - _std(days=20)).strftime('%d-%m-%Y')
        if first_shift_date else ''
    )

    # ── Pick a random interviewer (same person used in both places) ───
    interviewer = _random.choice(_SCREENING_INTERVIEWERS)

    # ── Read document.xml ─────────────────────────────────────────────
    with _szip.ZipFile(_sio.BytesIO(template_bytes), 'r') as _z:
        xml       = _z.read('word/document.xml').decode('utf-8')
        all_files = {name: _z.read(name) for name in _z.namelist()}

    # ── Inject value into the cell immediately after the label cell ───
    # start_pos lets a caller scope the search past an earlier point in
    # the document — needed because some labels (e.g. "Experience") are
    # reused in more than one section (Agency Work vs Assessment scores).
    def _inject(xml_str, label, value, start_pos=0):
        label_tag = f'<w:t xml:space="preserve">{label}</w:t>'
        pos = xml_str.find(label_tag, start_pos)
        if pos == -1:
            return xml_str
        tc_end = xml_str.find('</w:tc>', pos)
        if tc_end == -1:
            return xml_str
        tc_end += len('</w:tc>')

        # Find the exact <w:t ...> run tag — NOT <w:tcW>, <w:tcPr>,
        # <w:tcBorders>, <w:tcMar> etc, which all also start with the
        # substring '<w:t'. A genuine text-run tag is followed by a
        # space, '>', or '/'.
        t_start = -1
        search_from = tc_end
        while True:
            cand = xml_str.find('<w:t', search_from)
            if cand == -1:
                break
            next_char = xml_str[cand + 4]
            if next_char in (' ', '>', '/'):
                t_start = cand
                break
            search_from = cand + 4
        if t_start == -1:
            return xml_str

        # Find the end of the OPENING tag (the first '>' after t_start)
        open_tag_end = xml_str.find('>', t_start)
        if open_tag_end == -1:
            return xml_str

        is_self_closing = xml_str[open_tag_end - 1] == '/'
        # XML-escape the value — unescaped '&', '<', '>' (e.g. in job
        # titles like "Nursing & Clinical Manager") would otherwise
        # break the document.xml and corrupt the .docx on open.
        escaped_value = (
            str(value)
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
        )
        new_t = f'<w:t xml:space="preserve">{escaped_value}</w:t>'

        if is_self_closing:
            return xml_str[:t_start] + new_t + xml_str[open_tag_end + 1:]
        else:
            close_pos = xml_str.find('</w:t>', open_tag_end)
            if close_pos == -1:
                return xml_str
            close_pos += len('</w:t>')
            return xml_str[:t_start] + new_t + xml_str[close_pos:]

    # ── Toggle a checkbox content control by its 0-based occurrence
    # index among all <w:sdt> blocks in document order. Flips both the
    # w14:checked value and the displayed <w:sym> char so it renders
    # correctly in Word, LibreOffice, and PDF conversion alike.
    def _set_checkbox(xml_str, occurrence_index, checked=True):
        search_from = 0
        count = 0
        target_start = None
        while True:
            pos = xml_str.find('<w:sdt>', search_from)
            if pos == -1:
                break
            if count == occurrence_index:
                target_start = pos
                break
            count += 1
            search_from = pos + len('<w:sdt>')
        if target_start is None:
            return xml_str
        end = xml_str.find('</w:sdt>', target_start)
        if end == -1:
            return xml_str
        end += len('</w:sdt>')
        block = xml_str[target_start:end]
        if checked:
            block = block.replace('w14:val="0"', 'w14:val="1"', 1)
            block = block.replace('w:char="2610"', 'w:char="2612"', 1)
        else:
            block = block.replace('w14:val="1"', 'w14:val="0"', 1)
            block = block.replace('w:char="2612"', 'w:char="2610"', 1)
        return xml_str[:target_start] + block + xml_str[end:]

    # ── Fill a plain (unlabelled) table row's cells — used for the
    # Education and Employment tables. Most rows have only empty
    # <w:t> runs to fill in order; some rows (Employment's "Reason for
    # leaving") have a non-empty LABEL cell first that must be skipped —
    # skip_cells controls how many leading <w:t> runs to pass over
    # before starting to capture the ones to actually fill.
    def _find_nth_row_cells(xml_str, anchor_pos, row_index, num_cells=3, skip_cells=0):
        pos = anchor_pos
        for _ in range(row_index):
            tr_end = xml_str.find('</w:tr>', pos)
            if tr_end == -1:
                return None
            pos = tr_end + len('</w:tr>')
        row_end = xml_str.find('</w:tr>', pos)
        if row_end == -1:
            return None
        cells = []
        search_from = pos
        total_to_scan = skip_cells + num_cells
        scanned = 0
        while scanned < total_to_scan:
            t_start = -1
            while True:
                cand = xml_str.find('<w:t', search_from)
                if cand == -1 or cand > row_end:
                    break
                nxt = xml_str[cand + 4]
                if nxt in (' ', '>', '/'):
                    t_start = cand
                    break
                search_from = cand + 4
            if t_start == -1:
                return None
            open_end = xml_str.find('>', t_start)
            scanned += 1
            if scanned > skip_cells:
                cells.append((t_start, open_end))
            search_from = open_end + 1
        return cells

    def _fill_row(xml_str, anchor_pos, row_index, values, skip_cells=0):
        cells = _find_nth_row_cells(
            xml_str, anchor_pos, row_index,
            num_cells=len(values), skip_cells=skip_cells
        )
        if not cells:
            return xml_str
        # Process from the last cell backward so earlier offsets stay valid.
        for (t_start, open_end), value in zip(reversed(cells), reversed(values)):
            is_self_closing = xml_str[open_end - 1] == '/'
            escaped = (
                str(value)
                .replace('&', '&amp;')
                .replace('<', '&lt;')
                .replace('>', '&gt;')
            )
            new_t = f'<w:t xml:space="preserve">{escaped}</w:t>'
            if is_self_closing:
                xml_str = xml_str[:t_start] + new_t + xml_str[open_end + 1:]
            else:
                close_pos = xml_str.find('</w:t>', open_end)
                if close_pos == -1:
                    continue
                close_pos += len('</w:t>')
                xml_str = xml_str[:t_start] + new_t + xml_str[close_pos:]
        return xml_str

    xml = _inject(xml, 'Screening Interview Conducted By', interviewer['name'])
    xml = _inject(xml, 'Title', interviewer['title'])
    xml = _inject(xml, 'Candidate Name', candidate_name or '')
    xml = _inject(xml, 'Candidate ID', candidate_id or '')
    xml = _inject(xml, 'Date', interview_date)
    xml = _inject(xml, 'Candidate 1st Contact Date', contact_date)
    xml = _inject(xml, 'Location', location or '')
    xml = _inject(xml, 'Interviewer', interviewer['name'])

    # ── Knowledge Check MCQ — mark 7 questions correct, 3 incorrect ───
    wrong_question_indices = set(
        _random.sample(range(len(_SCREENING_MCQ_CORRECT)),
                        len(_SCREENING_MCQ_CORRECT) - _SCREENING_CORRECT_COUNT)
    )
    correct_marked = 0
    for q_idx, correct_letter in enumerate(_SCREENING_MCQ_CORRECT):
        if q_idx in wrong_question_indices:
            wrong_options = [l for l in _SCREENING_MCQ_LETTERS if l != correct_letter]
            chosen_letter = _random.choice(wrong_options)
        else:
            chosen_letter = correct_letter
            correct_marked += 1
        letter_idx = _SCREENING_MCQ_LETTERS.index(chosen_letter)
        checkbox_occurrence = q_idx * 4 + letter_idx
        xml = _set_checkbox(xml, checkbox_occurrence, checked=True)

    # ── Pass / Fail — always Pass, since correct_marked is fixed at 7/10 ──
    xml = _set_checkbox(xml, _SCREENING_PASS_CHECKBOX_IDX, checked=True)

    # ── Suitable for Placement — always Yes, consistent with a Pass ──
    xml = _set_checkbox(xml, _SCREENING_SUITABLE_YES_CHECKBOX_IDX, checked=True)

    # ── Test Score, and Communication / Clinical Knowledge / Experience /
    # Overall — random score between 3.5 and 5 (0.5 steps) for each.
    # Scoped to start AFTER the "Interviewer Assessment" heading, since
    # "Experience" is also a label in the earlier Agency Work section.
    assessment_anchor = xml.find('<w:t xml:space="preserve">Interviewer Assessment</w:t>')
    if assessment_anchor == -1:
        assessment_anchor = 0

    def _fmt_score(v):
        return str(int(v)) if float(v) == int(v) else str(v)

    # Pad the score to a fixed character width before "/ N" so the slash
    # lines up in the same column regardless of whether the score is a
    # short ("5") or longer ("4.5") string — avoids the ragged alignment
    # a plain single-space join produces when values differ in length.
    def _score_cell(score_str, out_of):
        return f"{score_str:<4}/ {out_of}"

    score_choices = [3.5, 4, 4.5, 5]
    xml = _inject(xml, 'Test Score',
                  _score_cell(str(correct_marked), 10),
                  start_pos=assessment_anchor)
    xml = _inject(xml, 'Communication',
                  _score_cell(_fmt_score(_random.choice(score_choices)), 5),
                  start_pos=assessment_anchor)
    xml = _inject(xml, 'Clinical Knowledge',
                  _score_cell(_fmt_score(_random.choice(score_choices)), 5),
                  start_pos=assessment_anchor)
    xml = _inject(xml, 'Experience',
                  _score_cell(_fmt_score(_random.choice(score_choices)), 5),
                  start_pos=assessment_anchor)
    xml = _inject(xml, 'Overall',
                  _score_cell(_fmt_score(_random.choice(score_choices)), 5),
                  start_pos=assessment_anchor)

    # ── Education + Employment tables — both filled from
    # live_staffs.extracted_cv via a single combined Gemini call ──────
    education_entries, employment_entries = _extract_education_and_employment(
        extracted_cv, gemini_key, max_education=5, max_employment=7
    )

    if education_entries:
        edu_header_pos = xml.find('School / College / Course')
        if edu_header_pos != -1:
            edu_anchor = xml.find('</w:tr>', edu_header_pos) + len('</w:tr>')
            for row_idx, entry in enumerate(education_entries):
                xml = _fill_row(
                    xml, edu_anchor, row_idx,
                    [entry.get('from', ''), entry.get('to', ''), entry.get('course', '')]
                )

    if employment_entries:
        emp_header_pos = xml.find('Employer, Role, Location')
        if emp_header_pos != -1:
            emp_anchor = xml.find('</w:tr>', emp_header_pos) + len('</w:tr>')
            for i, entry in enumerate(employment_entries):
                data_row_idx   = i * 2
                reason_row_idx = i * 2 + 1
                xml = _fill_row(
                    xml, emp_anchor, data_row_idx,
                    [entry.get('from', ''), entry.get('to', ''), entry.get('employer', '')]
                )
                # Prefer a reason actually stated in the CV; otherwise
                # backfill with a random plausible one — but never for
                # the role the candidate is still currently in.
                reason = entry.get('reason', '')
                if not reason and not _ongoing_role(entry.get('to', '')):
                    reason = _random.choice(_SCREENING_LEAVING_REASONS)
                if reason:
                    xml = _fill_row(
                        xml, emp_anchor, reason_row_idx,
                        [reason], skip_cells=1
                    )

    # ── Compliance Actions from Screening Interview — 3-4 random notes,
    # rarely all 5, picked from the reusable compliance notes pool.
    compliance_pos = xml.find('Compliance Actions from Screening Interview')
    if compliance_pos != -1:
        compliance_anchor = xml.find('</w:tr>', compliance_pos) + len('</w:tr>')
        chosen_notes = _pick_compliance_actions(_random)
        for row_idx, note in enumerate(chosen_notes):
            xml = _fill_row(
                xml, compliance_anchor, row_idx,
                [note], skip_cells=1
            )

    # ── Rebuild zip ───────────────────────────────────────────────────
    out = _sio.BytesIO()
    with _szip.ZipFile(out, 'w', _szip.ZIP_DEFLATED) as _zout:
        for name, data in all_files.items():
            if name == 'word/document.xml':
                _zout.writestr(name, xml.encode('utf-8'))
            else:
                _zout.writestr(name, data)
    return out.getvalue()


@admin_bp.route('/live-staffs/screening/generate', methods=['POST'])
@admin_required
def live_staff_screening_generate():
    """
    Generate a Screening Interview Record .docx for a staff member.

    POST /admin/live-staffs/screening/generate
    Body: {
        "staff_id":        "<mongo_id>",
        "first_shift_date": "YYYY-MM-DD"   # optional — sets Date (−15d) and Contact Date (−20d)
    }
    """
    data            = request.get_json() or {}
    staff_id        = (data.get('staff_id') or '').strip()
    first_shift_str = (data.get('first_shift_date') or '').strip()

    if not staff_id:
        return jsonify({"success": False, "error": "Missing staff_id"}), 400

    try:
        doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
        if not doc:
            return jsonify({"success": False, "error": "Staff record not found"}), 404

        def _first(*keys, default=''):
            """Return the first non-empty value found among several
            possible key spellings — live_staffs documents exist in more
            than one schema shape (old nested vs new flat with spaced/
            capitalised keys like 'Employee Code')."""
            for k in keys:
                val = doc.get(k)
                if val not in (None, ''):
                    return _v(val)
            return default

        s1           = doc.get('section_1_personal_details') or {}
        full_name    = _first('Name') or _v(s1.get('full_name') or '') or 'staff'
        emp_code     = _first('Employee Code', 'employee_code')
        county       = _first('County', 'county', 'Location', 'location')
        email        = _v(doc.get('email') or '')
        extracted_cv = _v(doc.get('extracted_cv') or '')

        def _parse_any_date(value):
            """Accepts a native datetime (pymongo returns BSON dates this
            way) or a string in any of several common formats. Returns a
            datetime, or None if unparseable/empty."""
            if value in (None, ''):
                return None
            if isinstance(value, datetime):
                return value
            s = str(value).strip()
            for fmt in (
                '%d-%m-%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d',
                '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S',
            ):
                try:
                    return datetime.strptime(s, fmt)
                except ValueError:
                    continue
            return None

        # First shift date: an explicit value passed in the request takes
        # priority; otherwise fall back to the staff's own work_start_date
        # (live_staffs.work_start_date) — Date is set to 15 days before it,
        # Candidate 1st Contact Date to 20 days before it.
        first_shift_date = _parse_any_date(first_shift_str)
        if first_shift_date is None:
            first_shift_date = _parse_any_date(doc.get('work_start_date'))

        docx_bytes = _build_screening_docx(
            first_shift_date=first_shift_date,
            candidate_name=full_name,
            location=county,
            candidate_id=emp_code,
            extracted_cv=extracted_cv,
        )

        safe_name = full_name.replace(' ', '_').replace('/', '_')
        filename  = f"Screening_{safe_name}.docx"
        gcs_blob  = f"screening/{filename}"

        _gcs_upload(
            gcs_blob, docx_bytes,
            content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        )

        # Upsert into live_staff_screening collection
        col      = _screening_col()
        existing = col.find_one({"staff_id": staff_id})
        rec = {
            "staff_id":         staff_id,
            "staff_name":       full_name,
            "employee_code":    emp_code,
            "filename":         filename,
            "gcs_blob":         gcs_blob,
            "first_shift_date": first_shift_str or None,
            "generated_at":     datetime.utcnow(),
        }
        if existing:
            col.update_one({"_id": existing["_id"]}, {"$set": rec})
        else:
            col.insert_one(rec)

        return jsonify({
            "success":    True,
            "staff_name": full_name,
            "filename":   filename,
            "gcs_blob":   gcs_blob,
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/screening/saved/<staff_id>')
@admin_required
def live_staff_screening_saved(staff_id):
    """
    Check whether a saved Screening Interview Record exists for this staff member.

    GET /admin/live-staffs/screening/saved/<staff_id>
    Returns: { "success": true, "found": true/false, "generated_at": "...", "filename": "..." }
    """
    try:
        rec = _screening_col().find_one({"staff_id": staff_id})
        if not rec:
            return jsonify({"success": True, "found": False})
        return jsonify({
            "success":      True,
            "found":        True,
            "filename":     rec.get("filename", ""),
            "gcs_blob":     rec.get("gcs_blob", ""),
            "generated_at": rec["generated_at"].strftime("%d %b %Y %H:%M")
                            if rec.get("generated_at") else "",
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/screening/download/<staff_id>')
@admin_required
def live_staff_screening_download(staff_id):
    """
    Stream the saved Screening Interview Record .docx from GCS.

    GET /admin/live-staffs/screening/download/<staff_id>
    """
    try:
        rec = _screening_col().find_one({"staff_id": staff_id})
        if not rec or not rec.get('gcs_blob'):
            return "Screening record not found — please regenerate", 404

        docx_bytes = _gcs_download(rec['gcs_blob'])
        name       = (rec.get('staff_name') or 'staff').replace(' ', '_')
        return Response(
            docx_bytes,
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            headers={
                "Content-Disposition":
                    f'attachment; filename="Screening_{name}.docx"'
            }
        )
    except Exception as e:
        return str(e), 500


@admin_bp.route('/live-staffs/screening/upload/<staff_id>', methods=['POST'])
@admin_required
def live_staff_screening_upload(staff_id):
    """
    Replace the saved Screening Interview Record with an edited .docx upload.

    POST /admin/live-staffs/screening/upload/<staff_id>
    Form-data: file=<.docx>
    """
    if 'file' not in request.files:
        return jsonify({"success": False, "error": "No file provided"}), 400
    file = request.files['file']
    if not file.filename.lower().endswith('.docx'):
        return jsonify({"success": False, "error": "Only .docx files accepted"}), 400

    try:
        col = _screening_col()
        rec = col.find_one({"staff_id": staff_id})
        if not rec:
            # Auto-create a record so the upload is not lost
            doc2 = _staffs_col().find_one({"_id": ObjectId(staff_id)})
            s1   = (doc2.get('section_1_personal_details') or {}) if doc2 else {}
            name = _v(s1.get('full_name') or 'staff').replace(' ', '_').replace('/', '_')
            gcs_blob = f"screening/Screening_{name}.docx"
        else:
            gcs_blob = rec.get('gcs_blob') or f"screening/Screening_{staff_id}.docx"

        data_bytes = file.read()
        _gcs_upload(
            gcs_blob, data_bytes,
            content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        )

        col.update_one(
            {"staff_id": staff_id},
            {"$set": {
                "gcs_blob":      gcs_blob,
                "filename":      os.path.basename(gcs_blob),
                "last_uploaded": datetime.utcnow(),
                "uploaded_by":   "admin",
            }},
            upsert=True
        )
        return jsonify({
            "success":  True,
            "message":  "Screening record replaced successfully",
            "filename": os.path.basename(gcs_blob),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500