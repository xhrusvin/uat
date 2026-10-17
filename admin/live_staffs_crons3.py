from flask import request, jsonify, Response
from bson import ObjectId
from datetime import datetime
import json
import json as _cjson
import json as _cjson2
import base64
import csv
import io
import re as _re
import re
import os
import threading

from database import db
from . import admin_bp
from admin.views import admin_required

# ── Helpers — lazy wrappers to avoid circular imports ─────────────────

def _v(val):
    if val is None: return ''
    return str(val).strip()

def _staffs_col():
    return db.live_staffs

def _gcs_upload(blob_name, data_bytes, content_type='application/octet-stream'):
    from admin.live_staffs import _gcs_upload as _f
    return _f(blob_name, data_bytes, content_type)

def _gcs_download(blob_name):
    from admin.live_staffs import _gcs_download as _f
    return _f(blob_name)

def _gcs_signed_url(blob_name, expiry_minutes=60):
    from admin.live_staffs import _gcs_signed_url as _f
    return _f(blob_name, expiry_minutes)

def _ai_pcc_col():
    return db.live_staff_ai_pcc

def _ai_cvs_col():
    return db.live_staff_ai_cvs

def _ai_interviews_col():
    return db.live_staff_ai_interviews

def _ai_appforms_col():
    return db.live_staff_ai_appforms

def _build_pcc_docx(doc, reviewer_index=0):
    from admin.live_staffs import _build_pcc_docx as _f
    return _f(doc, reviewer_index)

def _build_ai_cv_docx(doc, cv_text):
    from admin.live_staffs import _build_ai_cv_docx as _f
    return _f(doc, cv_text)

def _build_ai_interview_docx(doc, interview_text):
    from admin.live_staffs import _build_ai_interview_docx as _f
    return _f(doc, interview_text)

def _build_ai_appform_docx(doc, appform_text):
    from admin.live_staffs import _build_ai_appform_docx as _f
    return _f(doc, appform_text)

def _build_appform_docx(doc, signature_bytes=None):
    from admin.live_staffs import _build_appform_docx as _f
    return _f(doc, signature_bytes=signature_bytes)

def _build_interview_docx(doc, interview_text):
    from admin.live_staffs import _build_ai_interview_docx as _f
    return _f(doc, interview_text)

def _build_doc(template, context):
    from admin.live_staffs import _build_doc as _f
    return _f(template, context)

def _build_qual_xlsx(doc):
    from admin.live_staffs import _build_qual_xlsx as _f
    return _f(doc)

def _extract_text_from_url(url, headers=None):
    from admin.live_staffs_crons import _extract_text_from_url as _etf
    return _etf(url, headers)

def _get_pcc_reviewers():
    from admin.live_staffs import _PCC_REVIEWERS
    return _PCC_REVIEWERS

def _get_compliance_officer():
    from admin.live_staffs import _PCC_COMPLIANCE_OFFICER
    return _PCC_COMPLIANCE_OFFICER

def _push_hse_document_background(staff_id_str, doc_type_key,
                                   docx_bytes, staff_name='',
                                   mongo_id=None, email=''):
    from admin.live_staffs import _push_hse_document_background as _f
    return _f(staff_id_str, doc_type_key, docx_bytes, staff_name, mongo_id, email)

def _serialize(obj):
    from admin.live_staffs import _serialize as _f
    return _f(obj)


@admin_bp.route('/live-staffs/cron/sync-fire-safety', methods=['GET', 'POST'])
def live_staff_cron_sync_fire_safety():
    """
    Cron job — processes ONE staff member per call.
    Finds "Fire Safety" document, extracts details via Gemini AI.
    Saves: fs_certificate_name, fs_staff_name, fs_expiry_date,
           fs_issue_date, fs_issuing_body, fs_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"fs_fetched": {"$exists": False}},
            {"fs_fetched": False},
            {"fs_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff Fire Safety certificates already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["fs_fetched"]    = True
        fields["fs_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"fs_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"fs_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"fs_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"fs_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    fs_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'fire safety', 'fire safety certificate',
            'fire safety training', 'fire prevention',
            'fire awareness', 'fire warden',
        )) and d.get('url'):
            fs_doc = d
            break

    if not fs_doc:
        _mark_done({"fs_note": "no Fire Safety document found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No Fire Safety certificate found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (fs_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"fs_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — Fire Safety doc has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"fs_note": "document URL 404 — skipped", "fs_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — Fire Safety doc URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a certificate data extractor.

Extract the following details from this Fire Safety certificate or training record:
1. Certificate name (e.g. "Fire Safety", "Fire Safety Awareness", "Fire Warden Training Certificate")
2. Staff name as printed on the certificate
3. Expiry date or renewal date (if shown)
4. Issue / completion date
5. Issuing body or training provider

Return ONLY a JSON object — no markdown, no explanation:
{
  "certificate_name": "<exact certificate title as printed>",
  "staff_name_on_cert": "<name as printed on certificate>",
  "expiry_date": "<expiry or renewal date as printed, e.g. 01/06/2027 or June 2027>",
  "issue_date": "<issue or completion date as printed>",
  "issuing_body": "<organization that issued the certificate>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nCERTIFICATE TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result       = _cjson.loads(raw_out)
        cert_name    = _v(result.get('certificate_name') or '')
        cert_staff   = _v(result.get('staff_name_on_cert') or '')
        expiry_date  = _v(result.get('expiry_date') or '')
        issue_date   = _v(result.get('issue_date') or '')
        issuing_body = _v(result.get('issuing_body') or '')

        _mark_done({
            "fs_certificate_name": cert_name,
            "fs_staff_name":       cert_staff,
            "fs_expiry_date":      expiry_date,
            "fs_issue_date":       issue_date,
            "fs_issuing_body":     issuing_body,
            "fs_doc_url":          doc_url,
            "fs_doc_type":         fs_doc.get('document_type_name', ''),
            "fs_note":             "extracted successfully",
        })

        return jsonify({
            "success":            True,
            "email":              email,
            "staff_name":         full_name,
            "doc_found":          True,
            "certificate_name":   cert_name,
            "staff_name_on_cert": cert_staff,
            "expiry_date":        expiry_date,
            "issue_date":         issue_date,
            "issuing_body":       issuing_body,
            "remaining_count":    max(0, remaining_total - 1),
            "message": (
                f"Fire Safety cert extracted for {full_name} "
                f"(expires: {expiry_date or 'unknown'}) — "
                f"{max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"fs_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"fs_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: Fire Safety certificates to Excel ─────────────────────────

@admin_bp.route('/live-staffs/export/fire-safety-xlsx')
@admin_required
def live_staff_export_fire_safety_xlsx():
    """Export Fire Safety certificate details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "fs_certificate_name": 1, "fs_staff_name": 1,
             "fs_expiry_date": 1, "fs_issue_date": 1,
             "fs_issuing_body": 1, "fs_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'Fire Safety Certificates'

        headers    = ['Sno', 'Staff Name', 'Email', 'Certificate Name',
                      'Name on Cert', 'Expiry Date', 'Issue Date', 'Issuing Body', 'Status']
        col_widths = [5, 28, 36, 32, 28, 16, 16, 28, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:I{len(docs)+1}'

        from datetime import date as _date
        today = _date.today()

        def _is_expired(expiry_str):
            if not expiry_str:
                return None
            for fmt in ('%d/%m/%Y','%m/%Y','%Y-%m-%d','%d-%m-%Y','%B %Y','%b %Y'):
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(expiry_str.strip(), fmt).date()
                    return d < today
                except Exception:
                    continue
            return None

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            cert_n   = _v(doc.get('fs_certificate_name') or '')
            cert_s   = _v(doc.get('fs_staff_name') or '')
            expiry   = _v(doc.get('fs_expiry_date') or '')
            issue    = _v(doc.get('fs_issue_date') or '')
            issuer   = _v(doc.get('fs_issuing_body') or '')
            fetched  = doc.get('fs_fetched', False)
            expired  = _is_expired(expiry)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not cert_n:
                status   = 'No Cert Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is True:
                status   = 'EXPIRED'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is False:
                status   = 'Valid'
                row_fill = None
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, cert_n, cert_s, expiry, issue, issuer, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        c_align, c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="fire_safety_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract QQI Level 5 Or Equivalent Certificate ──────────────

@admin_bp.route('/live-staffs/cron/sync-qqi-level5', methods=['GET', 'POST'])
def live_staff_cron_sync_qqi_level5():
    """
    Cron job — processes ONE staff member per call.
    Finds "Qqi Level 5 Or Equivalent" document, extracts details via Gemini AI.
    Saves: qqi5_certificate_name, qqi5_staff_name, qqi5_expiry_date,
           qqi5_issue_date, qqi5_issuing_body, qqi5_award_level, qqi5_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"qqi5_fetched": {"$exists": False}},
            {"qqi5_fetched": False},
            {"qqi5_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff QQI Level 5 certificates already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["qqi5_fetched"]    = True
        fields["qqi5_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"qqi5_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"qqi5_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"qqi5_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"qqi5_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    qqi5_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'qqi level 5', 'qqi level5', 'qqi 5',
            'level 5', 'level5',
            'qqi level 5 or equivalent',
            'fetac level 5',
            'care skills', 'healthcare support',
        )) and d.get('url'):
            qqi5_doc = d
            break

    if not qqi5_doc:
        _mark_done({"qqi5_note": "no QQI Level 5 document found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No QQI Level 5 certificate found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (qqi5_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"qqi5_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — QQI Level 5 doc has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"qqi5_note": "document URL 404 — skipped", "qqi5_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — QQI Level 5 doc URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a certificate data extractor.

Extract the following details from this QQI Level 5 (or equivalent) qualification certificate:
1. Certificate / award name (e.g. "QQI Level 5 Healthcare Support", "FETAC Level 5 Care Skills", "NVQ Level 3 Health and Social Care")
2. Staff / student name as printed on the certificate
3. Award level (e.g. "Level 5", "Level 3", "QQI Level 5")
4. Date of issue or award date
5. Expiry date (if shown — most QQI certs do not expire)
6. Issuing body (e.g. "QQI", "FETAC", "City & Guilds", "BTEC")

Return ONLY a JSON object — no markdown, no explanation:
{
  "certificate_name": "<exact certificate title as printed>",
  "staff_name_on_cert": "<name as printed on certificate>",
  "award_level": "<award level, e.g. Level 5>",
  "issue_date": "<date of issue or award as printed>",
  "expiry_date": "<expiry date if shown, otherwise null>",
  "issuing_body": "<organization that issued the certificate>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nCERTIFICATE TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result       = _cjson.loads(raw_out)
        cert_name    = _v(result.get('certificate_name') or '')
        cert_staff   = _v(result.get('staff_name_on_cert') or '')
        award_level  = _v(result.get('award_level') or '')
        issue_date   = _v(result.get('issue_date') or '')
        expiry_date  = _v(result.get('expiry_date') or '')
        issuing_body = _v(result.get('issuing_body') or '')

        _mark_done({
            "qqi5_certificate_name": cert_name,
            "qqi5_staff_name":       cert_staff,
            "qqi5_award_level":      award_level,
            "qqi5_issue_date":       issue_date,
            "qqi5_expiry_date":      expiry_date,
            "qqi5_issuing_body":     issuing_body,
            "qqi5_doc_url":          doc_url,
            "qqi5_doc_type":         qqi5_doc.get('document_type_name', ''),
            "qqi5_note":             "extracted successfully",
        })

        return jsonify({
            "success":            True,
            "email":              email,
            "staff_name":         full_name,
            "doc_found":          True,
            "certificate_name":   cert_name,
            "staff_name_on_cert": cert_staff,
            "award_level":        award_level,
            "issue_date":         issue_date,
            "expiry_date":        expiry_date,
            "issuing_body":       issuing_body,
            "remaining_count":    max(0, remaining_total - 1),
            "message": (
                f"QQI Level 5 cert extracted for {full_name} "
                f"({award_level or 'Level 5'}) — "
                f"{max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"qqi5_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"qqi5_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: QQI Level 5 certificates to Excel ─────────────────────────

@admin_bp.route('/live-staffs/export/qqi-level5-xlsx')
@admin_required
def live_staff_export_qqi_level5_xlsx():
    """Export QQI Level 5 Or Equivalent certificate details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "qqi5_certificate_name": 1, "qqi5_staff_name": 1,
             "qqi5_award_level": 1, "qqi5_issue_date": 1,
             "qqi5_expiry_date": 1, "qqi5_issuing_body": 1, "qqi5_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'QQI Level 5'

        headers    = ['Sno', 'Staff Name', 'Email', 'Certificate Name',
                      'Name on Cert', 'Award Level', 'Issue Date',
                      'Expiry Date', 'Issuing Body', 'Status']
        col_widths = [5, 28, 36, 38, 28, 12, 14, 14, 22, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:J{len(docs)+1}'

        from datetime import date as _date
        today = _date.today()

        def _is_expired(expiry_str):
            if not expiry_str:
                return None
            for fmt in ('%d/%m/%Y','%m/%Y','%Y-%m-%d','%d-%m-%Y','%B %Y','%b %Y'):
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(expiry_str.strip(), fmt).date()
                    return d < today
                except Exception:
                    continue
            return None

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            cert_n   = _v(doc.get('qqi5_certificate_name') or '')
            cert_s   = _v(doc.get('qqi5_staff_name') or '')
            level    = _v(doc.get('qqi5_award_level') or '')
            issue    = _v(doc.get('qqi5_issue_date') or '')
            expiry   = _v(doc.get('qqi5_expiry_date') or '')
            issuer   = _v(doc.get('qqi5_issuing_body') or '')
            fetched  = doc.get('qqi5_fetched', False)
            expired  = _is_expired(expiry)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not cert_n:
                status   = 'No Cert Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is True:
                status   = 'EXPIRED'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is False:
                status   = 'Valid'
                row_fill = None
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, cert_n, cert_s,
                        level, issue, expiry, issuer, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        c_align, c_align, c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="qqi_level5_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract HSE Clearance Pass ─────────────────────────────────

@admin_bp.route('/live-staffs/cron/sync-hse-clearance', methods=['GET', 'POST'])
def live_staff_cron_sync_hse_clearance():
    """
    Cron job — processes ONE staff member per call.
    Finds "Hse Clearance Pass" document, extracts details via Gemini AI.
    Saves: hcp_certificate_name, hcp_staff_name, hcp_issue_date,
           hcp_expiry_date, hcp_issuing_body, hcp_pass_number, hcp_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"hcp_fetched": {"$exists": False}},
            {"hcp_fetched": False},
            {"hcp_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff HSE Clearance Passes already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["hcp_fetched"]    = True
        fields["hcp_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"hcp_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"hcp_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"hcp_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"hcp_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    hcp_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'hse clearance pass', 'hse clearance', 'hse pass',
            'health service executive clearance',
            'hse occupational health', 'hse pre-employment',
            'hse pre employment',
        )) and d.get('url'):
            hcp_doc = d
            break

    if not hcp_doc:
        _mark_done({"hcp_note": "no HSE Clearance Pass found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No HSE Clearance Pass found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (hcp_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"hcp_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — HSE Clearance doc has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"hcp_note": "document URL 404 — skipped", "hcp_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — HSE Clearance doc URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a document data extractor.

Extract the following details from this HSE Clearance Pass or occupational health clearance document:
1. Document / certificate name (e.g. "HSE Clearance Pass", "Occupational Health Clearance", "Pre-Employment Health Assessment")
2. Staff / employee name as printed on the document
3. Pass / reference number (if shown)
4. Issue date or clearance date
5. Expiry date (if shown)
6. Issuing body (e.g. "HSE", "Health Service Executive", "Occupational Health Dept")

Return ONLY a JSON object — no markdown, no explanation:
{
  "certificate_name": "<exact document title as printed>",
  "staff_name_on_cert": "<name as printed on document>",
  "pass_number": "<pass or reference number if visible>",
  "issue_date": "<date of issue or clearance as printed>",
  "expiry_date": "<expiry date if shown>",
  "issuing_body": "<organization that issued the clearance>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nDOCUMENT TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result       = _cjson.loads(raw_out)
        cert_name    = _v(result.get('certificate_name') or '')
        cert_staff   = _v(result.get('staff_name_on_cert') or '')
        pass_number  = _v(result.get('pass_number') or '')
        issue_date   = _v(result.get('issue_date') or '')
        expiry_date  = _v(result.get('expiry_date') or '')
        issuing_body = _v(result.get('issuing_body') or '')

        _mark_done({
            "hcp_certificate_name": cert_name,
            "hcp_staff_name":       cert_staff,
            "hcp_pass_number":      pass_number,
            "hcp_issue_date":       issue_date,
            "hcp_expiry_date":      expiry_date,
            "hcp_issuing_body":     issuing_body,
            "hcp_doc_url":          doc_url,
            "hcp_doc_type":         hcp_doc.get('document_type_name', ''),
            "hcp_note":             "extracted successfully",
        })

        return jsonify({
            "success":            True,
            "email":              email,
            "staff_name":         full_name,
            "doc_found":          True,
            "certificate_name":   cert_name,
            "staff_name_on_cert": cert_staff,
            "pass_number":        pass_number,
            "issue_date":         issue_date,
            "expiry_date":        expiry_date,
            "issuing_body":       issuing_body,
            "remaining_count":    max(0, remaining_total - 1),
            "message": (
                f"HSE Clearance Pass extracted for {full_name} ({email}) "
                f"— {max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"hcp_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"hcp_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: HSE Clearance Pass to Excel ──────────────────────────────

@admin_bp.route('/live-staffs/export/hse-clearance-xlsx')
@admin_required
def live_staff_export_hse_clearance_xlsx():
    """Export HSE Clearance Pass details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "hcp_certificate_name": 1, "hcp_staff_name": 1,
             "hcp_pass_number": 1, "hcp_issue_date": 1,
             "hcp_expiry_date": 1, "hcp_issuing_body": 1, "hcp_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'HSE Clearance Pass'

        headers    = ['Sno', 'Staff Name', 'Email', 'Document Name',
                      'Name on Doc', 'Pass Number', 'Issue Date',
                      'Expiry Date', 'Issuing Body', 'Status']
        col_widths = [5, 28, 36, 32, 28, 18, 14, 14, 24, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:J{len(docs)+1}'

        from datetime import date as _date
        today = _date.today()

        def _is_expired(expiry_str):
            if not expiry_str:
                return None
            for fmt in ('%d/%m/%Y','%m/%Y','%Y-%m-%d','%d-%m-%Y','%B %Y','%b %Y'):
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(expiry_str.strip(), fmt).date()
                    return d < today
                except Exception:
                    continue
            return None

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            cert_n   = _v(doc.get('hcp_certificate_name') or '')
            cert_s   = _v(doc.get('hcp_staff_name') or '')
            pass_n   = _v(doc.get('hcp_pass_number') or '')
            issue    = _v(doc.get('hcp_issue_date') or '')
            expiry   = _v(doc.get('hcp_expiry_date') or '')
            issuer   = _v(doc.get('hcp_issuing_body') or '')
            fetched  = doc.get('hcp_fetched', False)
            expired  = _is_expired(expiry)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not cert_n:
                status   = 'No Doc Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is True:
                status   = 'EXPIRED'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is False:
                status   = 'Valid'
                row_fill = None
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, cert_n, cert_s,
                        pass_n, issue, expiry, issuer, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        c_align, c_align, c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="hse_clearance_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract Driving Licence ────────────────────────────────────

@admin_bp.route('/live-staffs/cron/sync-driving-licence', methods=['GET', 'POST'])
def live_staff_cron_sync_driving_licence():
    """
    Cron job — processes ONE staff member per call.
    Finds "Driving Licence" document, extracts details via Gemini AI.
    Saves: dl_staff_name, dl_licence_number, dl_expiry_date,
           dl_issue_date, dl_country, dl_categories, dl_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"dl_fetched": {"$exists": False}},
            {"dl_fetched": False},
            {"dl_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff Driving Licences already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["dl_fetched"]    = True
        fields["dl_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"dl_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"dl_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"dl_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"dl_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    dl_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'driving licence', 'driving license',
            'driver licence', 'driver license',
            "driver's licence", "driver's license",
        )) and d.get('url'):
            dl_doc = d
            break

    if not dl_doc:
        _mark_done({"dl_note": "no Driving Licence found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No Driving Licence found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (dl_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"dl_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — Driving Licence has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"dl_note": "document URL 404 — skipped", "dl_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — Driving Licence URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a document data extractor.

Extract the following details from this Driving Licence:
1. Full name of the licence holder as printed
2. Licence number (field 5 on EU licences)
3. Date of birth (field 3)
4. Issue date (field 4a)
5. Expiry date (field 4b)
6. Issuing country or authority (field 4c or issuing body)
7. Licence categories (field 9/10/11 or entitlement codes, e.g. "B", "AM, B", "C1, D1")

Return ONLY a JSON object — no markdown, no explanation:
{
  "staff_name_on_doc": "<full name as printed on licence>",
  "licence_number": "<licence number>",
  "date_of_birth": "<date of birth as printed>",
  "issue_date": "<date of issue as printed>",
  "expiry_date": "<expiry date as printed>",
  "issuing_country": "<issuing country or authority>",
  "categories": "<licence categories or entitlement codes>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nDOCUMENT TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result         = _cjson.loads(raw_out)
        staff_on_doc   = _v(result.get('staff_name_on_doc') or '')
        licence_number = _v(result.get('licence_number') or '')
        dob            = _v(result.get('date_of_birth') or '')
        issue_date     = _v(result.get('issue_date') or '')
        expiry_date    = _v(result.get('expiry_date') or '')
        country        = _v(result.get('issuing_country') or '')
        categories     = _v(result.get('categories') or '')

        _mark_done({
            "dl_staff_name":     staff_on_doc,
            "dl_licence_number": licence_number,
            "dl_dob":            dob,
            "dl_issue_date":     issue_date,
            "dl_expiry_date":    expiry_date,
            "dl_country":        country,
            "dl_categories":     categories,
            "dl_doc_url":        doc_url,
            "dl_doc_type":       dl_doc.get('document_type_name', ''),
            "dl_note":           "extracted successfully",
        })

        return jsonify({
            "success":          True,
            "email":            email,
            "staff_name":       full_name,
            "doc_found":        True,
            "staff_name_on_doc": staff_on_doc,
            "licence_number":   licence_number,
            "expiry_date":      expiry_date,
            "issue_date":       issue_date,
            "country":          country,
            "categories":       categories,
            "remaining_count":  max(0, remaining_total - 1),
            "message": (
                f"Driving Licence extracted for {full_name} ({email}) "
                f"— {max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"dl_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"dl_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: Driving Licences to Excel ────────────────────────────────

@admin_bp.route('/live-staffs/export/driving-licence-xlsx')
@admin_required
def live_staff_export_driving_licence_xlsx():
    """Export Driving Licence details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "dl_staff_name": 1, "dl_licence_number": 1, "dl_dob": 1,
             "dl_issue_date": 1, "dl_expiry_date": 1,
             "dl_country": 1, "dl_categories": 1, "dl_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'Driving Licences'

        headers    = ['Sno', 'Staff Name', 'Email', 'Name on Licence',
                      'Licence Number', 'Date of Birth', 'Issue Date',
                      'Expiry Date', 'Country', 'Categories', 'Status']
        col_widths = [5, 28, 36, 28, 18, 14, 14, 14, 16, 16, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:K{len(docs)+1}'

        from datetime import date as _date
        today = _date.today()

        def _is_expired(expiry_str):
            if not expiry_str:
                return None
            for fmt in ('%d/%m/%Y','%m/%Y','%Y-%m-%d','%d-%m-%Y','%B %Y','%b %Y'):
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(expiry_str.strip(), fmt).date()
                    return d < today
                except Exception:
                    continue
            return None

        for ri, doc in enumerate(docs, start=2):
            s1        = doc.get('section_1_personal_details') or {}
            name      = _v(s1.get('full_name') or '')
            email     = _v(doc.get('email') or '')
            dl_name   = _v(doc.get('dl_staff_name') or '')
            dl_num    = _v(doc.get('dl_licence_number') or '')
            dob       = _v(doc.get('dl_dob') or '')
            issue     = _v(doc.get('dl_issue_date') or '')
            expiry    = _v(doc.get('dl_expiry_date') or '')
            country   = _v(doc.get('dl_country') or '')
            cats      = _v(doc.get('dl_categories') or '')
            fetched   = doc.get('dl_fetched', False)
            expired   = _is_expired(expiry)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not dl_num:
                status   = 'No Licence Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is True:
                status   = 'EXPIRED'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is False:
                status   = 'Valid'
                row_fill = None
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, dl_name, dl_num,
                        dob, issue, expiry, country, cats, status]
            aligns   = [c_align, l_align, l_align, l_align, c_align,
                        c_align, c_align, c_align, l_align, c_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="driving_licences_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract Point Scale Document ───────────────────────────────

@admin_bp.route('/live-staffs/cron/sync-point-scale', methods=['GET', 'POST'])
def live_staff_cron_sync_point_scale():
    """
    Cron job — processes ONE staff member per call.
    Finds "Point Scale Document" document, extracts details via Gemini AI.
    Saves: psd_document_name, psd_staff_name, psd_signed_date,
           psd_point_scale, psd_issuing_body, psd_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"psd_fetched": {"$exists": False}},
            {"psd_fetched": False},
            {"psd_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff Point Scale Documents already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["psd_fetched"]    = True
        fields["psd_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"psd_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"psd_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"psd_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"psd_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    psd_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'point scale document', 'point scale',
            'points scale document', 'points scale',
            'salary scale', 'pay scale', 'incremental scale',
        )) and d.get('url'):
            psd_doc = d
            break

    if not psd_doc:
        _mark_done({"psd_note": "no Point Scale Document found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No Point Scale Document found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (psd_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"psd_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — Point Scale doc has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"psd_note": "document URL 404 — skipped", "psd_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — Point Scale doc URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a document data extractor.

Extract the following details from this Point Scale Document (salary/pay scale document):
1. Document name (e.g. "Point Scale Document", "Salary Scale", "Pay Scale Agreement")
2. Staff / employee name as printed on the document
3. Point scale or salary scale value (e.g. "Point 1", "Scale 5", "€32,000")
4. Date the document was signed or issued
5. Issuing body or employer name

Return ONLY a JSON object — no markdown, no explanation:
{
  "document_name": "<exact document title as printed>",
  "staff_name_on_doc": "<name as printed on document>",
  "point_scale": "<point scale, salary scale or pay value as shown>",
  "signed_date": "<date signed or issued as printed>",
  "issuing_body": "<employer or issuing organisation name>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nDOCUMENT TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result       = _cjson.loads(raw_out)
        doc_name     = _v(result.get('document_name') or '')
        staff_on_doc = _v(result.get('staff_name_on_doc') or '')
        point_scale  = _v(result.get('point_scale') or '')
        signed_date  = _v(result.get('signed_date') or '')
        issuing_body = _v(result.get('issuing_body') or '')

        _mark_done({
            "psd_document_name": doc_name,
            "psd_staff_name":    staff_on_doc,
            "psd_point_scale":   point_scale,
            "psd_signed_date":   signed_date,
            "psd_issuing_body":  issuing_body,
            "psd_doc_url":       doc_url,
            "psd_doc_type":      psd_doc.get('document_type_name', ''),
            "psd_note":          "extracted successfully",
        })

        return jsonify({
            "success":          True,
            "email":            email,
            "staff_name":       full_name,
            "doc_found":        True,
            "document_name":    doc_name,
            "staff_name_on_doc": staff_on_doc,
            "point_scale":      point_scale,
            "signed_date":      signed_date,
            "issuing_body":     issuing_body,
            "remaining_count":  max(0, remaining_total - 1),
            "message": (
                f"Point Scale Document extracted for {full_name} ({email}) "
                f"— {max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"psd_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"psd_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: Point Scale Documents to Excel ───────────────────────────

@admin_bp.route('/live-staffs/export/point-scale-xlsx')
@admin_required
def live_staff_export_point_scale_xlsx():
    """Export Point Scale Document details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "psd_document_name": 1, "psd_staff_name": 1,
             "psd_point_scale": 1, "psd_signed_date": 1,
             "psd_issuing_body": 1, "psd_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'Point Scale Documents'

        headers    = ['Sno', 'Staff Name', 'Email', 'Document Name',
                      'Name on Doc', 'Point Scale', 'Signed Date', 'Issuing Body', 'Status']
        col_widths = [5, 28, 36, 30, 28, 16, 16, 28, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:I{len(docs)+1}'

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            doc_n    = _v(doc.get('psd_document_name') or '')
            doc_s    = _v(doc.get('psd_staff_name') or '')
            scale    = _v(doc.get('psd_point_scale') or '')
            signed   = _v(doc.get('psd_signed_date') or '')
            issuer   = _v(doc.get('psd_issuing_body') or '')
            fetched  = doc.get('psd_fetched', False)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not doc_n:
                status   = 'No Doc Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, doc_n, doc_s, scale, signed, issuer, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        c_align, c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="point_scale_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract Application Form ───────────────────────────────────

@admin_bp.route('/live-staffs/cron/sync-application-form', methods=['GET', 'POST'])
def live_staff_cron_sync_application_form():
    """
    Cron job — processes ONE staff member per call.
    Finds "Application Form" document, extracts details via Gemini AI.
    Saves: af_document_name, af_staff_name, af_signed_date,
           af_position_applied, af_issuing_body, af_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"af_fetched": {"$exists": False}},
            {"af_fetched": False},
            {"af_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff Application Forms already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["af_fetched"]    = True
        fields["af_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"af_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"af_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"af_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"af_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    af_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'application form', 'job application',
            'employment application', 'application for employment',
        )) and d.get('url'):
            af_doc = d
            break

    if not af_doc:
        _mark_done({"af_note": "no Application Form found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No Application Form found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (af_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"af_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — Application Form has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"af_note": "document URL 404 — skipped", "af_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — Application Form URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a document data extractor.

Extract the following details from this Application Form (job / employment application):
1. Document name (e.g. "Application Form", "Job Application Form", "Application for Employment")
2. Applicant / staff name as printed on the form
3. Position or role applied for (if shown)
4. Date the form was signed or submitted
5. Organisation or employer the application is addressed to

Return ONLY a JSON object — no markdown, no explanation:
{
  "document_name": "<exact document title as printed>",
  "staff_name_on_doc": "<applicant name as printed on form>",
  "position_applied": "<position or role applied for>",
  "signed_date": "<date signed or submitted as printed>",
  "issuing_body": "<organisation or employer the form is addressed to>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nDOCUMENT TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result           = _cjson.loads(raw_out)
        doc_name         = _v(result.get('document_name') or '')
        staff_on_doc     = _v(result.get('staff_name_on_doc') or '')
        position_applied = _v(result.get('position_applied') or '')
        signed_date      = _v(result.get('signed_date') or '')
        issuing_body     = _v(result.get('issuing_body') or '')

        _mark_done({
            "af_document_name":    doc_name,
            "af_staff_name":       staff_on_doc,
            "af_position_applied": position_applied,
            "af_signed_date":      signed_date,
            "af_issuing_body":     issuing_body,
            "af_doc_url":          doc_url,
            "af_doc_type":         af_doc.get('document_type_name', ''),
            "af_note":             "extracted successfully",
        })

        return jsonify({
            "success":            True,
            "email":              email,
            "staff_name":         full_name,
            "doc_found":          True,
            "document_name":      doc_name,
            "staff_name_on_doc":  staff_on_doc,
            "position_applied":   position_applied,
            "signed_date":        signed_date,
            "issuing_body":       issuing_body,
            "remaining_count":    max(0, remaining_total - 1),
            "message": (
                f"Application Form extracted for {full_name} ({email}) "
                f"— {max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"af_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"af_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: Application Forms to Excel ───────────────────────────────

@admin_bp.route('/live-staffs/export/application-form-xlsx')
@admin_required
def live_staff_export_application_form_xlsx():
    """Export Application Form details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "af_document_name": 1, "af_staff_name": 1,
             "af_position_applied": 1, "af_signed_date": 1,
             "af_issuing_body": 1, "af_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'Application Forms'

        headers    = ['Sno', 'Staff Name', 'Email', 'Document Name',
                      'Name on Form', 'Position Applied', 'Signed Date',
                      'Organisation', 'Status']
        col_widths = [5, 28, 36, 28, 28, 28, 16, 28, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:I{len(docs)+1}'

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            doc_n    = _v(doc.get('af_document_name') or '')
            doc_s    = _v(doc.get('af_staff_name') or '')
            pos      = _v(doc.get('af_position_applied') or '')
            signed   = _v(doc.get('af_signed_date') or '')
            org      = _v(doc.get('af_issuing_body') or '')
            fetched  = doc.get('af_fetched', False)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not doc_n:
                status   = 'No Form Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, doc_n, doc_s, pos, signed, org, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        l_align, c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="application_forms_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract Interview Notes ────────────────────────────────────

@admin_bp.route('/live-staffs/cron/sync-interview-notes', methods=['GET', 'POST'])
def live_staff_cron_sync_interview_notes():
    """
    Cron job — processes ONE staff member per call.
    Finds "Interview Notes" document, extracts details via Gemini AI.
    Saves: in_document_name, in_staff_name, in_interview_date,
           in_interviewer, in_outcome, in_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"in_fetched": {"$exists": False}},
            {"in_fetched": False},
            {"in_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff Interview Notes already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["in_fetched"]    = True
        fields["in_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"in_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"in_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"in_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"in_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    in_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'interview notes', 'interview note',
            'interview record', 'interview assessment',
            'interview feedback', 'interview evaluation',
        )) and d.get('url'):
            in_doc = d
            break

    if not in_doc:
        _mark_done({"in_note": "no Interview Notes document found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No Interview Notes found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (in_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"in_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — Interview Notes has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"in_note": "document URL 404 — skipped", "in_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — Interview Notes URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a document data extractor.

Extract the following details from this Interview Notes document:
1. Document name (e.g. "Interview Notes", "Interview Assessment", "Interview Record")
2. Candidate / staff name as printed on the document
3. Date of the interview
4. Interviewer name(s) (if shown)
5. Interview outcome or recommendation (e.g. "Recommended", "Successful", "Not Recommended", "Pending")

Return ONLY a JSON object — no markdown, no explanation:
{
  "document_name": "<exact document title as printed>",
  "staff_name_on_doc": "<candidate name as printed>",
  "interview_date": "<date of interview as printed>",
  "interviewer": "<interviewer name(s) if visible>",
  "outcome": "<interview outcome or recommendation if visible>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nDOCUMENT TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result         = _cjson.loads(raw_out)
        doc_name       = _v(result.get('document_name') or '')
        staff_on_doc   = _v(result.get('staff_name_on_doc') or '')
        interview_date = _v(result.get('interview_date') or '')
        interviewer    = _v(result.get('interviewer') or '')
        outcome        = _v(result.get('outcome') or '')

        _mark_done({
            "in_document_name":  doc_name,
            "in_staff_name":     staff_on_doc,
            "in_interview_date": interview_date,
            "in_interviewer":    interviewer,
            "in_outcome":        outcome,
            "in_doc_url":        doc_url,
            "in_doc_type":       in_doc.get('document_type_name', ''),
            "in_note":           "extracted successfully",
        })

        return jsonify({
            "success":          True,
            "email":            email,
            "staff_name":       full_name,
            "doc_found":        True,
            "document_name":    doc_name,
            "staff_name_on_doc": staff_on_doc,
            "interview_date":   interview_date,
            "interviewer":      interviewer,
            "outcome":          outcome,
            "remaining_count":  max(0, remaining_total - 1),
            "message": (
                f"Interview Notes extracted for {full_name} ({email}) "
                f"— {max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"in_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"in_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: Interview Notes to Excel ─────────────────────────────────

@admin_bp.route('/live-staffs/export/interview-notes-xlsx')
@admin_required
def live_staff_export_interview_notes_xlsx():
    """Export Interview Notes details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "in_document_name": 1, "in_staff_name": 1,
             "in_interview_date": 1, "in_interviewer": 1,
             "in_outcome": 1, "in_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'Interview Notes'

        headers    = ['Sno', 'Staff Name', 'Email', 'Document Name',
                      'Name on Doc', 'Interview Date', 'Interviewer',
                      'Outcome', 'Status']
        col_widths = [5, 28, 36, 28, 28, 16, 28, 20, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:I{len(docs)+1}'

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            doc_n    = _v(doc.get('in_document_name') or '')
            doc_s    = _v(doc.get('in_staff_name') or '')
            int_date = _v(doc.get('in_interview_date') or '')
            intrvwr  = _v(doc.get('in_interviewer') or '')
            outcome  = _v(doc.get('in_outcome') or '')
            fetched  = doc.get('in_fetched', False)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not doc_n:
                status   = 'No Notes Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, doc_n, doc_s,
                        int_date, intrvwr, outcome, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        c_align, l_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="interview_notes_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract Visa Document ──────────────────────────────────────

@admin_bp.route('/live-staffs/cron/sync-visa', methods=['GET', 'POST'])
def live_staff_cron_sync_visa():
    """
    Cron job — processes ONE staff member per call.
    Finds "Visa" document, extracts details via Gemini AI.
    Saves: visa_document_name, visa_staff_name, visa_type,
           visa_issue_date, visa_expiry_date, visa_country, visa_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"visa_fetched": {"$exists": False}},
            {"visa_fetched": False},
            {"visa_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff Visa documents already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["visa_fetched"]    = True
        fields["visa_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"visa_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"visa_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"visa_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"visa_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    visa_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'visa', 'work visa', 'work permit',
            'residence permit', 'stamp 4', 'stamp 1',
            'gnib card', 'irish residence permit', 'irp',
        )) and d.get('url'):
            visa_doc = d
            break

    if not visa_doc:
        _mark_done({"visa_note": "no Visa document found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No Visa document found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (visa_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"visa_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — Visa doc has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"visa_note": "document URL 404 — skipped", "visa_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — Visa doc URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a document data extractor.

Extract the following details from this Visa or residence permit document:
1. Document name (e.g. "Visa", "Work Permit", "Irish Residence Permit", "Stamp 4", "GNIB Card")
2. Holder name as printed on the document
3. Visa / permit type or category (e.g. "Work Permit", "Stamp 4", "Type D", "IRP")
4. Issue date
5. Expiry date
6. Issuing country or authority

Return ONLY a JSON object — no markdown, no explanation:
{
  "document_name": "<exact document title as printed>",
  "staff_name_on_doc": "<holder name as printed>",
  "visa_type": "<visa or permit type / category>",
  "issue_date": "<issue date as printed>",
  "expiry_date": "<expiry date as printed>",
  "issuing_country": "<issuing country or authority>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nDOCUMENT TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result          = _cjson.loads(raw_out)
        doc_name        = _v(result.get('document_name') or '')
        staff_on_doc    = _v(result.get('staff_name_on_doc') or '')
        visa_type       = _v(result.get('visa_type') or '')
        issue_date      = _v(result.get('issue_date') or '')
        expiry_date     = _v(result.get('expiry_date') or '')
        issuing_country = _v(result.get('issuing_country') or '')

        _mark_done({
            "visa_document_name":  doc_name,
            "visa_staff_name":     staff_on_doc,
            "visa_type":           visa_type,
            "visa_issue_date":     issue_date,
            "visa_expiry_date":    expiry_date,
            "visa_issuing_country": issuing_country,
            "visa_doc_url":        doc_url,
            "visa_doc_type":       visa_doc.get('document_type_name', ''),
            "visa_note":           "extracted successfully",
        })

        return jsonify({
            "success":           True,
            "email":             email,
            "staff_name":        full_name,
            "doc_found":         True,
            "document_name":     doc_name,
            "staff_name_on_doc": staff_on_doc,
            "visa_type":         visa_type,
            "issue_date":        issue_date,
            "expiry_date":       expiry_date,
            "issuing_country":   issuing_country,
            "remaining_count":   max(0, remaining_total - 1),
            "message": (
                f"Visa extracted for {full_name} ({email}) "
                f"(expires: {expiry_date or 'unknown'}) — "
                f"{max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"visa_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"visa_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: Visa documents to Excel ──────────────────────────────────

@admin_bp.route('/live-staffs/export/visa-xlsx')
@admin_required
def live_staff_export_visa_xlsx():
    """Export Visa document details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "visa_document_name": 1, "visa_staff_name": 1,
             "visa_type": 1, "visa_issue_date": 1,
             "visa_expiry_date": 1, "visa_issuing_country": 1, "visa_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'Visa Documents'

        headers    = ['Sno', 'Staff Name', 'Email', 'Document Name',
                      'Name on Doc', 'Visa Type', 'Issue Date',
                      'Expiry Date', 'Issuing Country', 'Status']
        col_widths = [5, 28, 36, 26, 28, 18, 14, 14, 20, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:J{len(docs)+1}'

        from datetime import date as _date
        today = _date.today()

        def _is_expired(expiry_str):
            if not expiry_str:
                return None
            for fmt in ('%d/%m/%Y','%m/%Y','%Y-%m-%d','%d-%m-%Y','%B %Y','%b %Y'):
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(expiry_str.strip(), fmt).date()
                    return d < today
                except Exception:
                    continue
            return None

        for ri, doc in enumerate(docs, start=2):
            s1        = doc.get('section_1_personal_details') or {}
            name      = _v(s1.get('full_name') or '')
            email     = _v(doc.get('email') or '')
            doc_n     = _v(doc.get('visa_document_name') or '')
            doc_s     = _v(doc.get('visa_staff_name') or '')
            vtype     = _v(doc.get('visa_type') or '')
            issue     = _v(doc.get('visa_issue_date') or '')
            expiry    = _v(doc.get('visa_expiry_date') or '')
            country   = _v(doc.get('visa_issuing_country') or '')
            fetched   = doc.get('visa_fetched', False)
            expired   = _is_expired(expiry)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not doc_n:
                status   = 'No Visa Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is True:
                status   = 'EXPIRED'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is False:
                status   = 'Valid'
                row_fill = None
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, doc_n, doc_s,
                        vtype, issue, expiry, country, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        l_align, c_align, c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="visa_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract Address Proof Document ─────────────────────────────

@admin_bp.route('/live-staffs/cron/sync-address-proof', methods=['GET', 'POST'])
def live_staff_cron_sync_address_proof():
    """
    Cron job — processes ONE staff member per call.
    Finds "address_proof" / "Address proof" document, extracts details via Gemini AI.
    Saves: ap_document_name, ap_staff_name, ap_address, ap_issue_date,
           ap_issuing_body, ap_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"ap_fetched": {"$exists": False}},
            {"ap_fetched": False},
            {"ap_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff Address Proof documents already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["ap_fetched"]    = True
        fields["ap_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"ap_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"ap_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"ap_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"ap_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    ap_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'address proof', 'address_proof', 'proof of address',
            'utility bill', 'bank statement', 'council tax',
            'proof of residence', 'residence proof',
        )) and d.get('url'):
            ap_doc = d
            break

    if not ap_doc:
        _mark_done({"ap_note": "no Address Proof document found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No Address Proof found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (ap_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"ap_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — Address Proof has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"ap_note": "document URL 404 — skipped", "ap_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — Address Proof URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a document data extractor.

Extract the following details from this address proof document (e.g. utility bill, bank statement, council tax letter):
1. Document type / name (e.g. "Utility Bill", "Bank Statement", "Proof of Address", "Council Tax Letter")
2. Name of the person the document is addressed to
3. Full address as printed on the document
4. Date of the document (issue date or statement date)
5. Issuing organisation (e.g. "AIB Bank", "Electric Ireland", "Dublin City Council")

Return ONLY a JSON object — no markdown, no explanation:
{
  "document_name": "<document type or title>",
  "staff_name_on_doc": "<name as addressed on document>",
  "address": "<full address as printed>",
  "issue_date": "<date of document as printed>",
  "issuing_body": "<organisation that issued the document>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nDOCUMENT TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result       = _cjson.loads(raw_out)
        doc_name     = _v(result.get('document_name') or '')
        staff_on_doc = _v(result.get('staff_name_on_doc') or '')
        address      = _v(result.get('address') or '')
        issue_date   = _v(result.get('issue_date') or '')
        issuing_body = _v(result.get('issuing_body') or '')

        _mark_done({
            "ap_document_name": doc_name,
            "ap_staff_name":    staff_on_doc,
            "ap_address":       address,
            "ap_issue_date":    issue_date,
            "ap_issuing_body":  issuing_body,
            "ap_doc_url":       doc_url,
            "ap_doc_type":      ap_doc.get('document_type_name', ''),
            "ap_note":          "extracted successfully",
        })

        return jsonify({
            "success":           True,
            "email":             email,
            "staff_name":        full_name,
            "doc_found":         True,
            "document_name":     doc_name,
            "staff_name_on_doc": staff_on_doc,
            "address":           address,
            "issue_date":        issue_date,
            "issuing_body":      issuing_body,
            "remaining_count":   max(0, remaining_total - 1),
            "message": (
                f"Address Proof extracted for {full_name} ({email}) "
                f"— {max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"ap_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"ap_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: Address Proof to Excel ───────────────────────────────────

@admin_bp.route('/live-staffs/export/address-proof-xlsx')
@admin_required
def live_staff_export_address_proof_xlsx():
    """Export Address Proof document details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "ap_document_name": 1, "ap_staff_name": 1,
             "ap_address": 1, "ap_issue_date": 1,
             "ap_issuing_body": 1, "ap_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'Address Proof'

        headers    = ['Sno', 'Staff Name', 'Email', 'Document Name',
                      'Name on Doc', 'Address', 'Issue Date', 'Issuing Body', 'Status']
        col_widths = [5, 28, 36, 24, 28, 40, 14, 24, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:I{len(docs)+1}'

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            doc_n    = _v(doc.get('ap_document_name') or '')
            doc_s    = _v(doc.get('ap_staff_name') or '')
            address  = _v(doc.get('ap_address') or '')
            issue    = _v(doc.get('ap_issue_date') or '')
            issuer   = _v(doc.get('ap_issuing_body') or '')
            fetched  = doc.get('ap_fetched', False)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not doc_n:
                status   = 'No Doc Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, doc_n, doc_s, address, issue, issuer, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        l_align, c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                if ci == 6:  # Address — allow wrap
                    cell.alignment = Alignment(horizontal='left', vertical='top', wrap_text=True)
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="address_proof_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract PPE Document ────────────────────────────────────────

@admin_bp.route('/live-staffs/cron/sync-ppe', methods=['GET', 'POST'])
def live_staff_cron_sync_ppe():
    """
    Cron job — processes ONE staff member per call.
    Finds "Ppe" document, extracts details via Gemini AI.
    Saves: ppe_document_name, ppe_staff_name, ppe_signed_date,
           ppe_issuing_body, ppe_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"ppe_fetched": {"$exists": False}},
            {"ppe_fetched": False},
            {"ppe_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff PPE documents already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["ppe_fetched"]    = True
        fields["ppe_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"ppe_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"ppe_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"ppe_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"ppe_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    ppe_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'ppe', 'personal protective equipment',
            'ppe acknowledgement', 'ppe form', 'ppe policy',
            'ppe agreement', 'ppe checklist',
        )) and d.get('url'):
            ppe_doc = d
            break

    if not ppe_doc:
        _mark_done({"ppe_note": "no PPE document found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No PPE document found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (ppe_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"ppe_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — PPE doc has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"ppe_note": "document URL 404 — skipped", "ppe_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — PPE doc URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a document data extractor.

Extract the following details from this PPE (Personal Protective Equipment) document, acknowledgement or policy form:
1. Document name (e.g. "PPE Acknowledgement", "Personal Protective Equipment Form", "PPE Policy Agreement")
2. Staff / employee name as printed on the document
3. Date the document was signed or issued
4. Issuing body or employer name

Return ONLY a JSON object — no markdown, no explanation:
{
  "document_name": "<exact document title as printed>",
  "staff_name_on_doc": "<name as printed on document>",
  "signed_date": "<date signed or issued as printed>",
  "issuing_body": "<employer or issuing organisation name>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nDOCUMENT TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result       = _cjson.loads(raw_out)
        doc_name     = _v(result.get('document_name') or '')
        staff_on_doc = _v(result.get('staff_name_on_doc') or '')
        signed_date  = _v(result.get('signed_date') or '')
        issuing_body = _v(result.get('issuing_body') or '')

        _mark_done({
            "ppe_document_name": doc_name,
            "ppe_staff_name":    staff_on_doc,
            "ppe_signed_date":   signed_date,
            "ppe_issuing_body":  issuing_body,
            "ppe_doc_url":       doc_url,
            "ppe_doc_type":      ppe_doc.get('document_type_name', ''),
            "ppe_note":          "extracted successfully",
        })

        return jsonify({
            "success":           True,
            "email":             email,
            "staff_name":        full_name,
            "doc_found":         True,
            "document_name":     doc_name,
            "staff_name_on_doc": staff_on_doc,
            "signed_date":       signed_date,
            "issuing_body":      issuing_body,
            "remaining_count":   max(0, remaining_total - 1),
            "message": (
                f"PPE document extracted for {full_name} ({email}) "
                f"— {max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"ppe_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"ppe_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: PPE documents to Excel ───────────────────────────────────

@admin_bp.route('/live-staffs/export/ppe-xlsx')
@admin_required
def live_staff_export_ppe_xlsx():
    """Export PPE document details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "ppe_document_name": 1, "ppe_staff_name": 1,
             "ppe_signed_date": 1, "ppe_issuing_body": 1, "ppe_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'PPE Documents'

        headers    = ['Sno', 'Staff Name', 'Email', 'Document Name',
                      'Name on Doc', 'Signed Date', 'Issuing Body', 'Status']
        col_widths = [5, 28, 36, 30, 28, 16, 28, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:H{len(docs)+1}'

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            doc_n    = _v(doc.get('ppe_document_name') or '')
            doc_s    = _v(doc.get('ppe_staff_name') or '')
            signed   = _v(doc.get('ppe_signed_date') or '')
            issuer   = _v(doc.get('ppe_issuing_body') or '')
            fetched  = doc.get('ppe_fetched', False)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not doc_n:
                status   = 'No Doc Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, doc_n, doc_s, signed, issuer, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="ppe_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract Manual And People Handling Document ─────────────────

@admin_bp.route('/live-staffs/cron/sync-manual-handling', methods=['GET', 'POST'])
def live_staff_cron_sync_manual_handling():
    """
    Cron job — processes ONE staff member per call.
    Finds "Manual And People Handling Documents" document.
    Saves: mph_certificate_name, mph_staff_name, mph_expiry_date,
           mph_issue_date, mph_issuing_body, mph_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"mph_fetched": {"$exists": False}},
            {"mph_fetched": False},
            {"mph_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff Manual Handling documents already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["mph_fetched"]    = True
        fields["mph_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"mph_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"mph_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"mph_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"mph_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    mph_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'manual and people handling',
            'manual handling',
            'people handling',
            'manual handling certificate',
            'manual handling training',
            'patient handling',
            'moving and handling',
        )) and d.get('url'):
            mph_doc = d
            break

    if not mph_doc:
        _mark_done({"mph_note": "no Manual Handling document found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No Manual Handling document found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (mph_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"mph_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — Manual Handling doc has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"mph_note": "document URL 404 — skipped", "mph_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — Manual Handling doc URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a certificate data extractor.

Extract the following details from this Manual and People Handling training certificate or document:
1. Certificate name (e.g. "Manual Handling", "People Handling", "Manual & People Handling Training Certificate", "Moving and Handling")
2. Staff name as printed on the certificate
3. Expiry date or renewal date (if shown)
4. Issue / completion date
5. Issuing body or training provider

Return ONLY a JSON object — no markdown, no explanation:
{
  "certificate_name": "<exact certificate title as printed>",
  "staff_name_on_cert": "<name as printed on certificate>",
  "expiry_date": "<expiry or renewal date as printed, e.g. 01/06/2027 or June 2027>",
  "issue_date": "<issue or completion date as printed>",
  "issuing_body": "<organization that issued the certificate>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nCERTIFICATE TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result       = _cjson.loads(raw_out)
        cert_name    = _v(result.get('certificate_name') or '')
        cert_staff   = _v(result.get('staff_name_on_cert') or '')
        expiry_date  = _v(result.get('expiry_date') or '')
        issue_date   = _v(result.get('issue_date') or '')
        issuing_body = _v(result.get('issuing_body') or '')

        _mark_done({
            "mph_certificate_name": cert_name,
            "mph_staff_name":       cert_staff,
            "mph_expiry_date":      expiry_date,
            "mph_issue_date":       issue_date,
            "mph_issuing_body":     issuing_body,
            "mph_doc_url":          doc_url,
            "mph_doc_type":         mph_doc.get('document_type_name', ''),
            "mph_note":             "extracted successfully",
        })

        return jsonify({
            "success":            True,
            "email":              email,
            "staff_name":         full_name,
            "doc_found":          True,
            "certificate_name":   cert_name,
            "staff_name_on_cert": cert_staff,
            "expiry_date":        expiry_date,
            "issue_date":         issue_date,
            "issuing_body":       issuing_body,
            "remaining_count":    max(0, remaining_total - 1),
            "message": (
                f"Manual Handling cert extracted for {full_name} "
                f"(expires: {expiry_date or 'unknown'}) — "
                f"{max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"mph_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"mph_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: Manual Handling certificates to Excel ─────────────────────

@admin_bp.route('/live-staffs/export/manual-handling-xlsx')
@admin_required
def live_staff_export_manual_handling_xlsx():
    """Export Manual and People Handling certificate details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "mph_certificate_name": 1, "mph_staff_name": 1,
             "mph_expiry_date": 1, "mph_issue_date": 1,
             "mph_issuing_body": 1, "mph_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'Manual Handling'

        headers    = ['Sno', 'Staff Name', 'Email', 'Certificate Name',
                      'Name on Cert', 'Expiry Date', 'Issue Date', 'Issuing Body', 'Status']
        col_widths = [5, 28, 36, 36, 28, 16, 16, 28, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:I{len(docs)+1}'

        from datetime import date as _date
        today = _date.today()

        def _is_expired(expiry_str):
            if not expiry_str:
                return None
            for fmt in ('%d/%m/%Y','%m/%Y','%Y-%m-%d','%d-%m-%Y','%B %Y','%b %Y'):
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(expiry_str.strip(), fmt).date()
                    return d < today
                except Exception:
                    continue
            return None

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            cert_n   = _v(doc.get('mph_certificate_name') or '')
            cert_s   = _v(doc.get('mph_staff_name') or '')
            expiry   = _v(doc.get('mph_expiry_date') or '')
            issue    = _v(doc.get('mph_issue_date') or '')
            issuer   = _v(doc.get('mph_issuing_body') or '')
            fetched  = doc.get('mph_fetched', False)
            expired  = _is_expired(expiry)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not cert_n:
                status   = 'No Cert Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is True:
                status   = 'EXPIRED'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is False:
                status   = 'Valid'
                row_fill = None
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, cert_n, cert_s, expiry, issue, issuer, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        c_align, c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="manual_handling_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract NMBI Qualification Document ─────────────────────────

@admin_bp.route('/live-staffs/cron/sync-nmbi-qualification', methods=['GET', 'POST'])
def live_staff_cron_sync_nmbi_qualification():
    """
    Cron job — processes ONE staff member per call.
    Finds "Nmbi Qualification" document, extracts details via Gemini AI.
    Saves: nmbi_q_certificate_name, nmbi_q_staff_name, nmbi_q_reg_number,
           nmbi_q_expiry_date, nmbi_q_issue_date, nmbi_q_issuing_body, nmbi_q_fetched = True
    Note: nmbi_q_ prefix to avoid conflict with nmbi_number field from sync-qualification cron.
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"nmbi_q_fetched": {"$exists": False}},
            {"nmbi_q_fetched": False},
            {"nmbi_q_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff NMBI Qualification documents already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["nmbi_q_fetched"]    = True
        fields["nmbi_q_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"nmbi_q_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"nmbi_q_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"nmbi_q_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"nmbi_q_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    nmbi_q_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'nmbi qualification', 'nmbi', 'nursing and midwifery board',
            'nursing midwifery board', 'nmbi registration',
            'nmbi certificate', 'nmbi pin',
        )) and d.get('url'):
            nmbi_q_doc = d
            break

    if not nmbi_q_doc:
        _mark_done({"nmbi_q_note": "no NMBI Qualification document found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No NMBI Qualification document found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (nmbi_q_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"nmbi_q_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — NMBI doc has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"nmbi_q_note": "document URL 404 — skipped", "nmbi_q_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — NMBI doc URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a document data extractor specialising in Irish nursing qualifications.

Extract the following details from this NMBI (Nursing and Midwifery Board of Ireland) qualification certificate or registration document:
1. Certificate / document name (e.g. "NMBI Registration Certificate", "NMBI Qualification", "Certificate of Registration")
2. Nurse / midwife name as printed on the document
3. NMBI PIN or registration number
4. Expiry date or renewal date (if shown)
5. Issue / registration date
6. Issuing body (should be "NMBI" or "Nursing and Midwifery Board of Ireland")

Return ONLY a JSON object — no markdown, no explanation:
{
  "certificate_name": "<exact document title as printed>",
  "staff_name_on_cert": "<name as printed on document>",
  "registration_number": "<NMBI PIN or registration number>",
  "expiry_date": "<expiry or renewal date as printed>",
  "issue_date": "<issue or registration date as printed>",
  "issuing_body": "<issuing organisation>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nDOCUMENT TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result      = _cjson.loads(raw_out)
        cert_name   = _v(result.get('certificate_name') or '')
        cert_staff  = _v(result.get('staff_name_on_cert') or '')
        reg_number  = _v(result.get('registration_number') or '')
        expiry_date = _v(result.get('expiry_date') or '')
        issue_date  = _v(result.get('issue_date') or '')
        issuing_body= _v(result.get('issuing_body') or '')

        _mark_done({
            "nmbi_q_certificate_name": cert_name,
            "nmbi_q_staff_name":       cert_staff,
            "nmbi_q_reg_number":       reg_number,
            "nmbi_q_expiry_date":      expiry_date,
            "nmbi_q_issue_date":       issue_date,
            "nmbi_q_issuing_body":     issuing_body,
            "nmbi_q_doc_url":          doc_url,
            "nmbi_q_doc_type":         nmbi_q_doc.get('document_type_name', ''),
            "nmbi_q_note":             "extracted successfully",
        })

        return jsonify({
            "success":            True,
            "email":              email,
            "staff_name":         full_name,
            "doc_found":          True,
            "certificate_name":   cert_name,
            "staff_name_on_cert": cert_staff,
            "registration_number": reg_number,
            "expiry_date":        expiry_date,
            "issue_date":         issue_date,
            "issuing_body":       issuing_body,
            "remaining_count":    max(0, remaining_total - 1),
            "message": (
                f"NMBI Qualification extracted for {full_name} "
                f"(PIN: {reg_number or 'unknown'}) — "
                f"{max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"nmbi_q_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"nmbi_q_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: NMBI Qualification to Excel ──────────────────────────────

@admin_bp.route('/live-staffs/export/nmbi-qualification-xlsx')
@admin_required
def live_staff_export_nmbi_qualification_xlsx():
    """Export NMBI Qualification document details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "nmbi_q_certificate_name": 1, "nmbi_q_staff_name": 1,
             "nmbi_q_reg_number": 1, "nmbi_q_expiry_date": 1,
             "nmbi_q_issue_date": 1, "nmbi_q_issuing_body": 1, "nmbi_q_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'NMBI Qualification'

        headers    = ['Sno', 'Staff Name', 'Email', 'Certificate Name',
                      'Name on Cert', 'NMBI PIN / Reg No', 'Expiry Date',
                      'Issue Date', 'Issuing Body', 'Status']
        col_widths = [5, 28, 36, 32, 28, 18, 14, 14, 28, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:J{len(docs)+1}'

        from datetime import date as _date
        today = _date.today()

        def _is_expired(expiry_str):
            if not expiry_str:
                return None
            for fmt in ('%d/%m/%Y','%m/%Y','%Y-%m-%d','%d-%m-%Y','%B %Y','%b %Y'):
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(expiry_str.strip(), fmt).date()
                    return d < today
                except Exception:
                    continue
            return None

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            cert_n   = _v(doc.get('nmbi_q_certificate_name') or '')
            cert_s   = _v(doc.get('nmbi_q_staff_name') or '')
            reg_n    = _v(doc.get('nmbi_q_reg_number') or '')
            expiry   = _v(doc.get('nmbi_q_expiry_date') or '')
            issue    = _v(doc.get('nmbi_q_issue_date') or '')
            issuer   = _v(doc.get('nmbi_q_issuing_body') or '')
            fetched  = doc.get('nmbi_q_fetched', False)
            expired  = _is_expired(expiry)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not cert_n:
                status   = 'No Doc Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is True:
                status   = 'EXPIRED'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is False:
                status   = 'Valid'
                row_fill = None
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, cert_n, cert_s,
                        reg_n, expiry, issue, issuer, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        c_align, c_align, c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="nmbi_qualification_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract Medication Management Certificate ───────────────────

@admin_bp.route('/live-staffs/cron/sync-medication-management', methods=['GET', 'POST'])
def live_staff_cron_sync_medication_management():
    """
    Cron job — processes ONE staff member per call.
    Finds "Medication Management" document, extracts details via Gemini AI.
    Saves: mm_certificate_name, mm_staff_name, mm_expiry_date,
           mm_issue_date, mm_issuing_body, mm_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"mm_fetched": {"$exists": False}},
            {"mm_fetched": False},
            {"mm_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff Medication Management certificates already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["mm_fetched"]    = True
        fields["mm_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"mm_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"mm_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"mm_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"mm_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    mm_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'medication management', 'medication administration',
            'medicines management', 'drug administration',
            'safe administration of medication',
            'medication management certificate',
            'medication management training',
        )) and d.get('url'):
            mm_doc = d
            break

    if not mm_doc:
        _mark_done({"mm_note": "no Medication Management document found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No Medication Management certificate found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (mm_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"mm_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — Medication Management doc has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"mm_note": "document URL 404 — skipped", "mm_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — Medication Management doc URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a certificate data extractor.

Extract the following details from this Medication Management or Medication Administration training certificate:
1. Certificate name (e.g. "Medication Management", "Safe Administration of Medication", "Medicines Management Certificate")
2. Staff name as printed on the certificate
3. Expiry date or renewal date (if shown)
4. Issue / completion date
5. Issuing body or training provider

Return ONLY a JSON object — no markdown, no explanation:
{
  "certificate_name": "<exact certificate title as printed>",
  "staff_name_on_cert": "<name as printed on certificate>",
  "expiry_date": "<expiry or renewal date as printed, e.g. 01/06/2027 or June 2027>",
  "issue_date": "<issue or completion date as printed>",
  "issuing_body": "<organization that issued the certificate>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nCERTIFICATE TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result       = _cjson.loads(raw_out)
        cert_name    = _v(result.get('certificate_name') or '')
        cert_staff   = _v(result.get('staff_name_on_cert') or '')
        expiry_date  = _v(result.get('expiry_date') or '')
        issue_date   = _v(result.get('issue_date') or '')
        issuing_body = _v(result.get('issuing_body') or '')

        _mark_done({
            "mm_certificate_name": cert_name,
            "mm_staff_name":       cert_staff,
            "mm_expiry_date":      expiry_date,
            "mm_issue_date":       issue_date,
            "mm_issuing_body":     issuing_body,
            "mm_doc_url":          doc_url,
            "mm_doc_type":         mm_doc.get('document_type_name', ''),
            "mm_note":             "extracted successfully",
        })

        return jsonify({
            "success":            True,
            "email":              email,
            "staff_name":         full_name,
            "doc_found":          True,
            "certificate_name":   cert_name,
            "staff_name_on_cert": cert_staff,
            "expiry_date":        expiry_date,
            "issue_date":         issue_date,
            "issuing_body":       issuing_body,
            "remaining_count":    max(0, remaining_total - 1),
            "message": (
                f"Medication Management cert extracted for {full_name} "
                f"(expires: {expiry_date or 'unknown'}) — "
                f"{max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"mm_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"mm_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: Medication Management certificates to Excel ───────────────

@admin_bp.route('/live-staffs/export/medication-management-xlsx')
@admin_required
def live_staff_export_medication_management_xlsx():
    """Export Medication Management certificate details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "mm_certificate_name": 1, "mm_staff_name": 1,
             "mm_expiry_date": 1, "mm_issue_date": 1,
             "mm_issuing_body": 1, "mm_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'Medication Management'

        headers    = ['Sno', 'Staff Name', 'Email', 'Certificate Name',
                      'Name on Cert', 'Expiry Date', 'Issue Date', 'Issuing Body', 'Status']
        col_widths = [5, 28, 36, 36, 28, 16, 16, 28, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:I{len(docs)+1}'

        from datetime import date as _date
        today = _date.today()

        def _is_expired(expiry_str):
            if not expiry_str:
                return None
            for fmt in ('%d/%m/%Y','%m/%Y','%Y-%m-%d','%d-%m-%Y','%B %Y','%b %Y'):
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(expiry_str.strip(), fmt).date()
                    return d < today
                except Exception:
                    continue
            return None

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            cert_n   = _v(doc.get('mm_certificate_name') or '')
            cert_s   = _v(doc.get('mm_staff_name') or '')
            expiry   = _v(doc.get('mm_expiry_date') or '')
            issue    = _v(doc.get('mm_issue_date') or '')
            issuer   = _v(doc.get('mm_issuing_body') or '')
            fetched  = doc.get('mm_fetched', False)
            expired  = _is_expired(expiry)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not cert_n:
                status   = 'No Cert Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is True:
                status   = 'EXPIRED'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is False:
                status   = 'Valid'
                row_fill = None
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, cert_n, cert_s, expiry, issue, issuer, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        c_align, c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="medication_management_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract Visual Display Equipment Document ───────────────────

@admin_bp.route('/live-staffs/cron/sync-vde', methods=['GET', 'POST'])
def live_staff_cron_sync_vde():
    """
    Cron job — processes ONE staff member per call.
    Finds "Visual Display Equipment" document, extracts details via Gemini AI.
    Saves: vde_certificate_name, vde_staff_name, vde_expiry_date,
           vde_issue_date, vde_issuing_body, vde_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"vde_fetched": {"$exists": False}},
            {"vde_fetched": False},
            {"vde_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff VDE documents already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["vde_fetched"]    = True
        fields["vde_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"vde_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"vde_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"vde_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"vde_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    vde_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'visual display equipment', 'vde',
            'display screen equipment', 'dse',
            'visual display unit', 'vdu',
            'screen equipment assessment',
        )) and d.get('url'):
            vde_doc = d
            break

    if not vde_doc:
        _mark_done({"vde_note": "no VDE document found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No Visual Display Equipment document found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (vde_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"vde_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — VDE doc has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"vde_note": "document URL 404 — skipped", "vde_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — VDE doc URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a certificate data extractor.

Extract the following details from this Visual Display Equipment (VDE) / Display Screen Equipment (DSE) training certificate or assessment document:
1. Certificate / document name (e.g. "Visual Display Equipment", "DSE Assessment", "VDE Training Certificate", "Display Screen Equipment")
2. Staff name as printed on the document
3. Expiry date or review date (if shown)
4. Issue / completion date
5. Issuing body or training provider

Return ONLY a JSON object — no markdown, no explanation:
{
  "certificate_name": "<exact document title as printed>",
  "staff_name_on_cert": "<name as printed on document>",
  "expiry_date": "<expiry or review date as printed, e.g. 01/06/2027 or June 2027>",
  "issue_date": "<issue or completion date as printed>",
  "issuing_body": "<organization that issued the document>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nDOCUMENT TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result       = _cjson.loads(raw_out)
        cert_name    = _v(result.get('certificate_name') or '')
        cert_staff   = _v(result.get('staff_name_on_cert') or '')
        expiry_date  = _v(result.get('expiry_date') or '')
        issue_date   = _v(result.get('issue_date') or '')
        issuing_body = _v(result.get('issuing_body') or '')

        _mark_done({
            "vde_certificate_name": cert_name,
            "vde_staff_name":       cert_staff,
            "vde_expiry_date":      expiry_date,
            "vde_issue_date":       issue_date,
            "vde_issuing_body":     issuing_body,
            "vde_doc_url":          doc_url,
            "vde_doc_type":         vde_doc.get('document_type_name', ''),
            "vde_note":             "extracted successfully",
        })

        return jsonify({
            "success":            True,
            "email":              email,
            "staff_name":         full_name,
            "doc_found":          True,
            "certificate_name":   cert_name,
            "staff_name_on_cert": cert_staff,
            "expiry_date":        expiry_date,
            "issue_date":         issue_date,
            "issuing_body":       issuing_body,
            "remaining_count":    max(0, remaining_total - 1),
            "message": (
                f"VDE document extracted for {full_name} "
                f"(expires: {expiry_date or 'unknown'}) — "
                f"{max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"vde_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"vde_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: VDE documents to Excel ───────────────────────────────────

@admin_bp.route('/live-staffs/export/vde-xlsx')
@admin_required
def live_staff_export_vde_xlsx():
    """Export Visual Display Equipment document details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "vde_certificate_name": 1, "vde_staff_name": 1,
             "vde_expiry_date": 1, "vde_issue_date": 1,
             "vde_issuing_body": 1, "vde_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'VDE Documents'

        headers    = ['Sno', 'Staff Name', 'Email', 'Certificate Name',
                      'Name on Cert', 'Expiry Date', 'Issue Date', 'Issuing Body', 'Status']
        col_widths = [5, 28, 36, 36, 28, 16, 16, 28, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:I{len(docs)+1}'

        from datetime import date as _date
        today = _date.today()

        def _is_expired(expiry_str):
            if not expiry_str:
                return None
            for fmt in ('%d/%m/%Y','%m/%Y','%Y-%m-%d','%d-%m-%Y','%B %Y','%b %Y'):
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(expiry_str.strip(), fmt).date()
                    return d < today
                except Exception:
                    continue
            return None

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            cert_n   = _v(doc.get('vde_certificate_name') or '')
            cert_s   = _v(doc.get('vde_staff_name') or '')
            expiry   = _v(doc.get('vde_expiry_date') or '')
            issue    = _v(doc.get('vde_issue_date') or '')
            issuer   = _v(doc.get('vde_issuing_body') or '')
            fetched  = doc.get('vde_fetched', False)
            expired  = _is_expired(expiry)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not cert_n:
                status   = 'No Doc Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is True:
                status   = 'EXPIRED'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is False:
                status   = 'Valid'
                row_fill = None
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, cert_n, cert_s, expiry, issue, issuer, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        c_align, c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="vde_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Cron: Extract INEWS Certificate ──────────────────────────────────

@admin_bp.route('/live-staffs/cron/sync-inews', methods=['GET', 'POST'])
def live_staff_cron_sync_inews():
    """
    Cron job — processes ONE staff member per call.
    Finds "Irish National Early Warning System (Inews) V2: Nursing/Hscp" document.
    Saves: inews_certificate_name, inews_staff_name, inews_expiry_date,
           inews_issue_date, inews_issuing_body, inews_fetched = True
    """
    import requests as _req
    from google import genai as google_genai

    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', '')
    gemini_key  = os.environ.get('GEMINI_API_KEY', '')

    if not base_url:
        return jsonify({"success": False, "error": "LIVE_STAFF_URL not set"}), 500
    if not gemini_key:
        return jsonify({"success": False, "error": "GEMINI_API_KEY not set"}), 500

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"inews_fetched": {"$exists": False}},
            {"inews_fetched": False},
            {"inews_fetched": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff INEWS certificates already extracted.",
            "remaining_count": 0,
        })

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["inews_fetched"]    = True
        fields["inews_fetched_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    if not email:
        _mark_done({"inews_note": "skipped — no email"})
        return jsonify({
            "success":         True,
            "message":         "Skipped — no email",
            "remaining_count": max(0, remaining_total - 1),
        })

    endpoint    = f"{base_url}/ai/recruitments/user-document-list"
    api_headers = {
        "Api-Key":       api_key,
        "X-App-Country": app_country,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=api_headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=api_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        _mark_done({"inews_note": f"API error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": f"API error: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not data.get('success'):
        _mark_done({"inews_note": f"API error: {data.get('message')}"})
        return jsonify({
            "success": False, "email": email,
            "error": data.get('message', 'API error'),
            "remaining_count": max(0, remaining_total - 1),
        })

    api_data  = data.get('data')
    documents = api_data if isinstance(api_data, list) else                 (api_data.get('documents') or [] if isinstance(api_data, dict) else [])

    if not documents:
        _mark_done({"inews_note": "no documents returned"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No documents returned for {email}",
            "remaining_count": max(0, remaining_total - 1),
        })

    inews_doc = None
    for d in documents:
        doc_name = (d.get('document_type_name') or '').strip().lower()
        if any(t in doc_name for t in (
            'irish national early warning system',
            'inews', 'inews v2', 'national early warning',
            'early warning system', 'news',
        )) and d.get('url'):
            inews_doc = d
            break

    if not inews_doc:
        _mark_done({"inews_note": "no INEWS document found"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": False,
            "message": f"No INEWS certificate found for {full_name}",
            "remaining_count": max(0, remaining_total - 1),
        })

    doc_url = (inews_doc.get('url') or '').strip()

    if not doc_url:
        _mark_done({"inews_note": "document found but URL is empty — skipped"})
        return jsonify({
            "success": True, "email": email, "staff_name": full_name,
            "doc_found": True, "skipped": True,
            "reason": "Document URL is empty",
            "remaining_count": max(0, remaining_total - 1),
            "message": f"Skipped {full_name} ({email}) — INEWS doc has no URL",
        })

    try:
        dl_headers = {k: v for k, v in api_headers.items() if k != 'Content-Type'}
        dl_resp    = _req.get(doc_url, headers=dl_headers, timeout=60)

        if dl_resp.status_code == 404:
            _mark_done({"inews_note": "document URL 404 — skipped", "inews_doc_404": True})
            return jsonify({
                "success": True, "email": email, "staff_name": full_name,
                "doc_found": True, "skipped": True,
                "reason": "Document URL returned 404",
                "remaining_count": max(0, remaining_total - 1),
                "message": f"Skipped {full_name} ({email}) — INEWS doc URL 404",
            })

        dl_resp.raise_for_status()
        raw_bytes    = dl_resp.content
        content_type = dl_resp.headers.get('Content-Type', '').lower()

        client = google_genai.Client(api_key=gemini_key)

        prompt_text = """You are a certificate data extractor.

Extract the following details from this INEWS (Irish National Early Warning System) V2 training certificate for Nursing/HSCP:
1. Certificate name (e.g. "Irish National Early Warning System (INEWS) V2: Nursing/HSCP", "INEWS Training Certificate")
2. Staff name as printed on the certificate
3. Expiry date or renewal date (if shown)
4. Issue / completion date
5. Issuing body or training provider (e.g. "HSeLanD", "HSE", "Health Service Executive")

Return ONLY a JSON object — no markdown, no explanation:
{
  "certificate_name": "<exact certificate title as printed>",
  "staff_name_on_cert": "<name as printed on certificate>",
  "expiry_date": "<expiry or renewal date as printed, e.g. 01/06/2027 or June 2027>",
  "issue_date": "<issue or completion date as printed>",
  "issuing_body": "<organization that issued the certificate>"
}

If a field is not visible, set it to null.
"""

        is_image = any(t in content_type for t in ('image/', 'jpeg', 'jpg', 'png', 'webp'))
        is_pdf   = 'pdf' in content_type or doc_url.lower().split('?')[0].endswith('.pdf')

        if is_image:
            ext   = 'jpeg' if any(t in content_type for t in ('jpeg', 'jpg')) else                     'png'  if 'png'  in content_type else                     'webp' if 'webp' in content_type else 'jpeg'
            parts = [
                {"inline_data": {"mime_type": f"image/{ext}",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        elif is_pdf:
            parts = [
                {"inline_data": {"mime_type": "application/pdf",
                                 "data": base64.b64encode(raw_bytes).decode()}},
                {"text": prompt_text}
            ]
            response = client.models.generate_content(
                model='gemini-2.5-flash', contents=[{"parts": parts}]
            )
        else:
            try:
                import io as _io, pdfplumber
                with pdfplumber.open(_io.BytesIO(raw_bytes)) as pdf:
                    raw_text = chr(10).join(p.extract_text() or '' for p in pdf.pages).strip()
            except Exception:
                raw_text = raw_bytes.decode('utf-8', errors='replace').strip()
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt_text + "\n\nCERTIFICATE TEXT:\n" + raw_text[:5000]
            )

        raw_out = (response.text or '').strip()
        raw_out = _re.sub(r'^```(?:json)?\s*', '', raw_out, flags=_re.MULTILINE)
        raw_out = _re.sub(r'```\s*$', '', raw_out, flags=_re.MULTILINE).strip()

        result       = _cjson.loads(raw_out)
        cert_name    = _v(result.get('certificate_name') or '')
        cert_staff   = _v(result.get('staff_name_on_cert') or '')
        expiry_date  = _v(result.get('expiry_date') or '')
        issue_date   = _v(result.get('issue_date') or '')
        issuing_body = _v(result.get('issuing_body') or '')

        _mark_done({
            "inews_certificate_name": cert_name,
            "inews_staff_name":       cert_staff,
            "inews_expiry_date":      expiry_date,
            "inews_issue_date":       issue_date,
            "inews_issuing_body":     issuing_body,
            "inews_doc_url":          doc_url,
            "inews_doc_type":         inews_doc.get('document_type_name', ''),
            "inews_note":             "extracted successfully",
        })

        return jsonify({
            "success":            True,
            "email":              email,
            "staff_name":         full_name,
            "doc_found":          True,
            "certificate_name":   cert_name,
            "staff_name_on_cert": cert_staff,
            "expiry_date":        expiry_date,
            "issue_date":         issue_date,
            "issuing_body":       issuing_body,
            "remaining_count":    max(0, remaining_total - 1),
            "message": (
                f"INEWS cert extracted for {full_name} "
                f"(expires: {expiry_date or 'unknown'}) — "
                f"{max(0, remaining_total - 1)} remaining."
            ),
        })

    except _cjson.JSONDecodeError:
        _mark_done({"inews_note": "Gemini JSON parse error"})
        return jsonify({
            "success": False, "email": email,
            "error": "Gemini returned non-JSON",
            "remaining_count": max(0, remaining_total - 1),
        })
    except Exception as e:
        _mark_done({"inews_note": f"error: {e}"})
        return jsonify({
            "success": False, "email": email,
            "error": str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ── Export: INEWS certificates to Excel ──────────────────────────────

@admin_bp.route('/live-staffs/export/inews-xlsx')
@admin_required
def live_staff_export_inews_xlsx():
    """Export INEWS certificate details to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
        import io as _io

        docs = list(_staffs_col().find(
            {},
            {"section_1_personal_details": 1, "email": 1,
             "inews_certificate_name": 1, "inews_staff_name": 1,
             "inews_expiry_date": 1, "inews_issue_date": 1,
             "inews_issuing_body": 1, "inews_fetched": 1}
        ))
        docs.sort(key=lambda d: _v(
            (d.get('section_1_personal_details') or {}).get('full_name') or ''
        ).lower())

        NAVY = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT  = 'EFF6FF'; WARN  = 'FFF3CD'; RED   = 'FFDDDD'

        h_font  = Font(name='Arial', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Arial', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        green_b = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'INEWS Certificates'

        headers    = ['Sno', 'Staff Name', 'Email', 'Certificate Name',
                      'Name on Cert', 'Expiry Date', 'Issue Date', 'Issuing Body', 'Status']
        col_widths = [5, 28, 36, 44, 28, 16, 16, 24, 14]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = green_b
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:I{len(docs)+1}'

        from datetime import date as _date
        today = _date.today()

        def _is_expired(expiry_str):
            if not expiry_str:
                return None
            for fmt in ('%d/%m/%Y','%m/%Y','%Y-%m-%d','%d-%m-%Y','%B %Y','%b %Y'):
                try:
                    from datetime import datetime as _dt
                    d = _dt.strptime(expiry_str.strip(), fmt).date()
                    return d < today
                except Exception:
                    continue
            return None

        for ri, doc in enumerate(docs, start=2):
            s1       = doc.get('section_1_personal_details') or {}
            name     = _v(s1.get('full_name') or '')
            email    = _v(doc.get('email') or '')
            cert_n   = _v(doc.get('inews_certificate_name') or '')
            cert_s   = _v(doc.get('inews_staff_name') or '')
            expiry   = _v(doc.get('inews_expiry_date') or '')
            issue    = _v(doc.get('inews_issue_date') or '')
            issuer   = _v(doc.get('inews_issuing_body') or '')
            fetched  = doc.get('inews_fetched', False)
            expired  = _is_expired(expiry)

            if not fetched:
                status   = 'Not Checked'
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            elif not cert_n:
                status   = 'No Cert Found'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is True:
                status   = 'EXPIRED'
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif expired is False:
                status   = 'Valid'
                row_fill = None
            else:
                status   = 'Found'
                row_fill = None

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT)                        if ri % 2 == 0 and not row_fill else None

            row_vals = [ri-1, name, email, cert_n, cert_s, expiry, issue, issuer, status]
            aligns   = [c_align, l_align, l_align, l_align, l_align,
                        c_align, c_align, l_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Arial', bold=True, size=9)

        buf = _io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="inews_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── External API: Get staff details by email ─────────────────────────

@admin_bp.route('/live-staffs/api/staff-details', methods=['POST'])
def live_staff_api_get_staff_details():
    """
    External API — returns full staff details from live_staffs collection.

    Auth: X-API-Key header (LIVE_STAFF_API_KEY env var)

    Request:
        POST /admin/live-staffs/api/staff-details
        Headers: X-API-Key: <key>
        Body:    {"email": "staff@example.com"}

    Response:
        {
          "success": true,
          "data": { ...full staff document... }
        }
    """
    # ── Auth ──────────────────────────────────────────────────────────
    api_key = os.environ.get('LIVE_STAFF_API_KEY', '')
    if api_key:
        provided = (request.headers.get('X-API-Key') or
                    request.headers.get('X-Api-Key') or '')
        if provided != api_key:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    # ── Input ─────────────────────────────────────────────────────────
    body  = request.get_json(silent=True) or {}
    email = _v(body.get('email') or '').lower()

    if not email:
        return jsonify({"success": False, "error": "email is required"}), 400

    # ── Lookup ────────────────────────────────────────────────────────
    col   = _staffs_col()
    staff = col.find_one({
        "$or": [
            {"email":                                         email},
            {"section_1_personal_details.email_address":     email},
        ]
    })

    if not staff:
        return jsonify({"success": False, "error": "Staff not found"}), 404

    # ── Serialise (convert ObjectId / datetime to str) ────────────────
    def _serialise(obj):
        if isinstance(obj, dict):
            return {k: _serialise(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_serialise(i) for i in obj]
        if hasattr(obj, 'isoformat'):          # datetime
            return obj.isoformat()
        if hasattr(obj, '__str__') and type(obj).__name__ in ('ObjectId', 'Decimal128'):
            return str(obj)
        return obj

    data = _serialise(dict(staff))
    data['_id'] = str(staff['_id'])

    # ── Fetch document list from XN Portal ───────────────────────────
    documents     = []
    doc_url_map   = {}
    xn_base_url   = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    xn_api_key    = os.environ.get('XN_PORTAL_API_KEY', '')
    xn_country    = os.environ.get('XN_APP_COUNTRY', '')

    if xn_base_url and xn_api_key:
        try:
            import requests as _req2
            endpoint    = f"{xn_base_url}/ai/recruitments/user-document-list"
            api_headers = {
                "Api-Key":       xn_api_key,
                "X-App-Country": xn_country,
                "Content-Type":  "application/json",
                "Accept":        "application/json",
            }
            resp = _req2.post(endpoint, json={"email": email},
                              headers=api_headers, timeout=30)
            if resp.status_code == 405:
                resp = _req2.get(endpoint, params={"email": email},
                                 headers=api_headers, timeout=30)
            if resp.status_code == 200:
                xn_data   = resp.json()
                api_data  = xn_data.get('data')
                documents = api_data if isinstance(api_data, list) else                             (api_data.get('documents') or []
                             if isinstance(api_data, dict) else [])
                # Build a map: document_type_name -> url
                for d in documents:
                    dtype = _v(d.get('document_type_name') or '')
                    url   = _v(d.get('url') or '')
                    if dtype and url:
                        doc_url_map[dtype] = url
        except Exception as _e:
            documents   = []
            doc_url_map = {}

    # ── Fetch work_start_date from XN Portal (recruitments/detail) ────
    # Only call if work_start_date is not already saved in live_staffs.
    work_start_date = _v(staff.get('work_start_date') or '')
    if xn_base_url and xn_api_key and not work_start_date:
        # xn_staff_id is the _id sent to the detail API
        xn_staff_id = _v(
            staff.get('xn_staff_id') or staff.get('staff_id') or ''
        )
        if xn_staff_id:
            try:
                import requests as _req3
                detail_headers = {
                    "Api-Key":       xn_api_key,
                    "X-App-Country": xn_country,
                    "Content-Type":  "application/json",
                    "Accept":        "application/json",
                }
                det_resp = _req3.post(
                    f"{xn_base_url}/ai/recruitments/detail",
                    json={"_id": xn_staff_id},
                    headers=detail_headers,
                    timeout=30,
                )
                # Retry with GET if POST not allowed
                if det_resp.status_code == 405:
                    det_resp = _req3.get(
                        f"{xn_base_url}/ai/recruitments/detail",
                        params={"_id": xn_staff_id},
                        headers=detail_headers,
                        timeout=30,
                    )
                if det_resp.status_code == 200:
                    det_json        = det_resp.json()
                    det_data        = det_json.get('data') or {}
                    work_start_date = _v(det_data.get('work_start_date') or '')
                    if work_start_date:
                        # Persist to live_staffs so we never call this again
                        col.update_one(
                            {"_id": staff['_id']},
                            {"$set": {"work_start_date": work_start_date}},
                        )
                        data['work_start_date'] = work_start_date
            except Exception:
                pass   # non-fatal — omit from response silently

        # ── Fetch signature_url + references from XN Portal (not saved to DB) ──
    signature_url = ''
    references    = []
    if xn_base_url and xn_api_key:
        xn_staff_id = _v(
            staff.get('xn_staff_id') or staff.get('staff_id') or ''
        )
        if xn_staff_id:
            try:
                import requests as _req4
                sig_headers = {
                    "Api-Key":       xn_api_key,
                    "X-App-Country": xn_country,
                    "Content-Type":  "application/json",
                    "Accept":        "application/json",
                }
                sig_resp = _req4.post(
                    f"{xn_base_url}/ai/recruitments/detail",
                    json={"_id": xn_staff_id},
                    headers=sig_headers,
                    timeout=30,
                )
                if sig_resp.status_code == 405:
                    sig_resp = _req4.get(
                        f"{xn_base_url}/ai/recruitments/detail",
                        params={"_id": xn_staff_id},
                        headers=sig_headers,
                        timeout=30,
                    )
                if sig_resp.status_code == 200:
                    sig_json      = sig_resp.json()
                    sig_data      = sig_json.get('data') or {}
                    signature_url = _v(sig_data.get('signature_url') or '')
                    raw_refs      = sig_data.get('references')
                    references    = raw_refs if isinstance(raw_refs, list) else []
            except Exception:
                pass   # non-fatal — omit from response silently

    return jsonify({
        "success":       True,
        "email":         email,
        "data":          data,
        "documents":     documents,
        "document_urls": doc_url_map,
        "signature_url": signature_url,
        "references":    references,
    })


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

# ═══════════════════════════════════════════════════════════════════════
# ADD THIS to the same crons blueprint file as live_staff_cron_generate_pcc
# and live_staff_cron_generate_screening.
#
# Renames each already-generated Screening Interview Record from its
# name-based filename (e.g. "Screening_Akhildev_Mundapurath.docx") to
# just the staff's Employee Code (e.g. "XH_33307.docx"),
# re-uploads under the new GCS path, and updates the pointer in
# live_staff_screening. The old blob is left in place (not deleted) —
# safer in case anything else still references it.
# ═══════════════════════════════════════════════════════════════════════


@admin_bp.route('/live-staffs/cron/rename-screening-by-employee-code', methods=['GET', 'POST'])
def live_staff_cron_rename_screening_by_employee_code():
    """
    Cron job — renames ONE already-generated Screening Interview Record
    per call, from its old name-based filename to one keyed on the
    staff's Employee Code.

    Finds live_staff_screening records where screening_renamed is not True.
    Looks up the matching live_staffs document (by staff_id) to get the
    Employee Code — handles both schema variants ('Employee Code' with
    a space/capitals, and lowercase employee_code).

    Skips (and marks done so it isn't retried forever) when:
      - the live_staffs document can't be found
      - no Employee Code is available for that staff member

    To re-run this rename pass over ALL screening records, run in
    MongoDB first:
        db.live_staff_screening.updateMany({}, {$set: {screening_renamed: false}})

    Protect with ?cron_key=<CRON_SECRET> env var.
    """
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    screening_col = db.live_staff_screening
    staffs_col    = _staffs_col()

    pending_query = {
        "$or": [
            {"screening_renamed": {"$exists": False}},
            {"screening_renamed": False},
            {"screening_renamed": None},
        ]
    }
    remaining_total = screening_col.count_documents(pending_query)
    rec             = screening_col.find_one(pending_query)

    if not rec:
        return jsonify({
            "success":         True,
            "message":         "All Screening Interview Records already renamed.",
            "remaining_count": 0,
        })

    rec_id      = rec['_id']
    staff_id    = rec.get('staff_id', '')
    staff_name  = rec.get('staff_name', '') or 'staff'
    old_blob    = rec.get('gcs_blob', '')

    def _mark_done(fields):
        fields["screening_renamed"]    = True
        fields["screening_renamed_at"] = datetime.utcnow()
        screening_col.update_one({"_id": rec_id}, {"$set": fields})

    if not old_blob:
        _mark_done({"screening_rename_note": "no gcs_blob on record — skipped"})
        return jsonify({
            "success":         True,
            "staff_name":      staff_name,
            "skipped":         True,
            "message":         f"Skipped {staff_name} — no gcs_blob on record",
            "remaining_count": max(0, remaining_total - 1),
        })

    # ── Look up the staff doc for the Employee Code ────────────────────
    staff = None
    try:
        staff = staffs_col.find_one({"_id": ObjectId(staff_id)})
    except Exception:
        pass

    if not staff:
        _mark_done({"screening_rename_note": "live_staffs document not found — skipped"})
        return jsonify({
            "success":         True,
            "staff_name":      staff_name,
            "skipped":         True,
            "message":         f"Skipped {staff_name} — live_staffs document not found (staff_id={staff_id})",
            "remaining_count": max(0, remaining_total - 1),
        })

    def _first(*keys, default=''):
        """Same multi-schema lookup used elsewhere — live_staffs documents
        exist in more than one shape ('Employee Code' with a space/
        capitals in the new flat schema vs lowercase employee_code in
        the old nested schema)."""
        for k in keys:
            val = staff.get(k)
            if val not in (None, ''):
                return _v(val)
        return default

    employee_code = _first('Employee Code', 'employee_code')

    if not employee_code:
        _mark_done({"screening_rename_note": "no Employee Code available — skipped"})
        return jsonify({
            "success":         True,
            "staff_name":      staff_name,
            "skipped":         True,
            "message":         f"Skipped {staff_name} — no Employee Code available",
            "remaining_count": max(0, remaining_total - 1),
        })

    try:
        # Download the existing file and re-upload under the new name
        docx_bytes   = _gcs_download(old_blob)
        safe_code    = _v(employee_code).replace(' ', '_').replace('/', '_')
        new_filename = f"{safe_code}.docx"
        new_blob     = f"screening/{new_filename}"

        _gcs_upload(
            new_blob, docx_bytes,
            content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        )

        screening_col.update_one(
            {"_id": rec_id},
            {"$set": {
                "gcs_blob":          new_blob,
                "filename":          new_filename,
                "employee_code":     employee_code,
                "previous_gcs_blob": old_blob,
                "renamed_at":        datetime.utcnow(),
            }}
        )

        _mark_done({})

        return jsonify({
            "success":         True,
            "staff_name":      staff_name,
            "employee_code":   employee_code,
            "old_gcs_blob":    old_blob,
            "new_gcs_blob":    new_blob,
            "new_filename":    new_filename,
            "remaining_count": max(0, remaining_total - 1),
            "message": (
                f"Renamed Screening Interview Record for {staff_name} "
                f"({employee_code}) — {max(0, remaining_total - 1)} remaining."
            ),
        })

    except Exception as e:
        _mark_done({"screening_rename_note": f"error: {e}"})
        return jsonify({
            "success":         False,
            "staff_name":      staff_name,
            "employee_code":   employee_code,
            "error":           str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ═══════════════════════════════════════════════════════════════════════
# ADD THIS to the same crons blueprint file as live_staff_cron_generate_pcc /
# live_staff_cron_generate_screening / live_staff_cron_rename_screening_by_employee_code.
#
# PDF-ONLY regeneration — does NOT touch the saved .docx in GCS at all.
#
# Unlike re-converting the already-saved docx (which keeps whatever
# checkboxes/content that file was built with, however old), this
# REBUILDS the docx fresh in memory each time using the CURRENT
# template and the staff's current data — the exact same code path the
# frontend "Generate" button uses — then converts ONLY that in-memory
# copy to PDF and uploads just the PDF. The existing saved docx file in
# GCS is left completely untouched.
#
# This is what you want when the template was fixed (e.g. a font or
# layout bug) after documents were already generated, and you want
# corrected PDFs without re-uploading/overwriting the docx files.
# ═══════════════════════════════════════════════════════════════════════


# ── Add these wrappers alongside the other lazy-import wrappers ───────
def _build_screening_docx(first_shift_date=None, candidate_name='',
                           location='', candidate_id='', extracted_cv=''):
    from admin.live_staffs import _build_screening_docx as _f
    return _f(
        first_shift_date=first_shift_date,
        candidate_name=candidate_name,
        location=location,
        candidate_id=candidate_id,
        extracted_cv=extracted_cv,
    )


def _docx_to_pdf_bytes(docx_bytes):
    from admin.live_staffs import _docx_to_pdf_bytes as _f
    return _f(docx_bytes)


@admin_bp.route('/live-staffs/cron/generate-screening-pdf', methods=['GET', 'POST'])
def live_staff_cron_generate_screening_pdf():
    """
    Cron job — regenerates ONE Screening Interview Record as PDF per
    call, WITHOUT touching the saved .docx in GCS.

    For each live_staff_screening record with no pdf_gcs_blob yet:
      1. Looks up the matching live_staffs document
      2. Rebuilds the docx fresh IN MEMORY from the current template
         (same data/fields as the full generator — work_start_date,
         Employee Code, County, extracted_cv) — this is what actually
         picks up a template fix (e.g. corrected checkboxes)
      3. Converts that in-memory docx to PDF
      4. Uploads ONLY the PDF to GCS — the existing saved .docx blob
         is never re-uploaded or modified

    To re-run this pass over ALL screening records, run in MongoDB first:
        db.live_staff_screening.updateMany(
            {}, {$unset: {pdf_gcs_blob: "", pdf_filename: ""}, $set: {pdf_failed: false}}
        )

    Protect with ?cron_key=<CRON_SECRET> env var.
    """
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    screening_col = db.live_staff_screening
    staffs_col    = _staffs_col()

    pending_query = {
        "$and": [
            {"gcs_blob": {"$exists": True, "$ne": None, "$ne": ""}},
            {"$or": [
                {"pdf_gcs_blob": {"$exists": False}},
                {"pdf_gcs_blob": None},
                {"pdf_gcs_blob": ""},
            ]},
            {"$or": [
                {"pdf_failed": {"$exists": False}},
                {"pdf_failed": False},
            ]},
        ]
    }
    remaining_total = screening_col.count_documents(pending_query)
    rec             = screening_col.find_one(pending_query)

    if not rec:
        return jsonify({
            "success":         True,
            "message":         "All Screening Interview Records already have a PDF.",
            "remaining_count": 0,
        })

    rec_id       = rec['_id']
    staff_id     = rec.get('staff_id', '')
    staff_name   = rec.get('staff_name', '') or 'staff'
    docx_blob    = rec.get('gcs_blob', '')
    filename     = rec.get('filename', '') or ''

    def _mark_done(fields):
        screening_col.update_one({"_id": rec_id}, {"$set": fields})

    def _mark_failed(err):
        _mark_done({
            "pdf_failed":    True,
            "pdf_note":      f"error: {err}",
            "pdf_failed_at": datetime.utcnow(),
        })

    # ── Look up the staff doc to rebuild the docx with current data ───
    staff = None
    try:
        staff = staffs_col.find_one({"_id": ObjectId(staff_id)})
    except Exception:
        pass

    if not staff:
        _mark_failed("live_staffs document not found — cannot rebuild docx")
        return jsonify({
            "success":         False,
            "staff_name":      staff_name,
            "error":           f"live_staffs document not found (staff_id={staff_id})",
            "remaining_count": max(0, remaining_total - 1),
        })

    def _first(*keys, default=''):
        """Same multi-schema lookup used by the full generator — handles
        both 'Employee Code' (new flat schema) and employee_code (old
        nested schema)."""
        for k in keys:
            val = staff.get(k)
            if val not in (None, ''):
                return _v(val)
        return default

    s1           = staff.get('section_1_personal_details') or {}
    full_name    = _first('Name') or _v(s1.get('full_name') or '') or staff_name
    emp_code     = _first('Employee Code', 'employee_code')
    county       = _first('County', 'county', 'Location', 'location')
    extracted_cv = _v(staff.get('extracted_cv') or '')

    def _parse_any_date(value):
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

    try:
        first_shift_date = _parse_any_date(staff.get('work_start_date'))

        # Rebuild fresh in memory — this is what actually picks up any
        # template fix. Nothing from this is uploaded as a .docx.
        fresh_docx_bytes = _build_screening_docx(
            first_shift_date=first_shift_date,
            candidate_name=full_name,
            location=county,
            candidate_id=emp_code,
            extracted_cv=extracted_cv,
        )

        pdf_bytes = _docx_to_pdf_bytes(fresh_docx_bytes)

        # Match the SAVED docx's base name (e.g. "XH_0031.docx" -> "XH_0031.pdf")
        # so the PDF sits alongside the existing docx under the same name —
        # the existing docx blob itself is left untouched.
        base_name    = filename.rsplit('.', 1)[0] if filename else docx_blob.rsplit('/', 1)[-1].rsplit('.', 1)[0]
        pdf_filename = f"{base_name}.pdf"
        pdf_blob     = f"screening/{pdf_filename}"

        _gcs_upload(pdf_blob, pdf_bytes, content_type='application/pdf')

        _mark_done({
            "pdf_gcs_blob":     pdf_blob,
            "pdf_filename":     pdf_filename,
            "pdf_generated_at": datetime.utcnow(),
            "pdf_rebuilt_from_template": True,
        })

        return jsonify({
            "success":         True,
            "staff_name":      full_name,
            "docx_gcs_blob":   docx_blob,   # unchanged, informational only
            "pdf_gcs_blob":    pdf_blob,
            "pdf_filename":    pdf_filename,
            "remaining_count": max(0, remaining_total - 1),
            "message": (
                f"PDF rebuilt from current template for {full_name} "
                f"({pdf_filename}) — {max(0, remaining_total - 1)} remaining."
            ),
        })

    except Exception as e:
        _mark_failed(str(e))
        return jsonify({
            "success":         False,
            "staff_name":      full_name,
            "error":           str(e),
            "remaining_count": max(0, remaining_total - 1),
        })

# ═══════════════════════════════════════════════════════════════════════
# ADD THESE TWO ROUTES to the same crons blueprint file as
# live_staff_cron_generate_pcc / live_staff_cron_generate_screening /
# live_staff_cron_rename_screening_by_employee_code.
#
# Unlike the "generate" and "rename" crons, downloading to a LOCAL
# folder is inherently client-side state — the server has no way to
# know what's already on your machine — so there's no DB flag here.
# Instead: one route lists every generated record, one route streams a
# single file's bytes, and run_download_screening.py (below) skips any
# file that already exists locally, making it safely resumable.
# ═══════════════════════════════════════════════════════════════════════


@admin_bp.route('/live-staffs/cron/screening-list', methods=['GET'])
def live_staff_cron_screening_list():
    """
    Returns every generated Screening Interview Record's metadata in one
    call: staff_id, staff_name, employee_code, filename, gcs_blob, plus
    the PDF equivalents (pdf_filename, pdf_gcs_blob — empty strings if
    no PDF has been generated for that record yet).

    Protect with ?cron_key=<CRON_SECRET> env var.
    """
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    screening_col = db.live_staff_screening
    recs = list(screening_col.find(
        {},
        {"staff_id": 1, "staff_name": 1, "employee_code": 1,
         "filename": 1, "gcs_blob": 1,
         "pdf_filename": 1, "pdf_gcs_blob": 1}
    ))

    items = []
    for r in recs:
        items.append({
            "staff_id":      str(r.get("staff_id", "")),
            "staff_name":    r.get("staff_name", "") or "",
            "employee_code": r.get("employee_code", "") or "",
            "filename":      r.get("filename", "") or "",
            "gcs_blob":      r.get("gcs_blob", "") or "",
            "pdf_filename":  r.get("pdf_filename", "") or "",
            "pdf_gcs_blob":  r.get("pdf_gcs_blob", "") or "",
        })

    return jsonify({
        "success": True,
        "count":   len(items),
        "items":   items,
    })


_DOWNLOAD_MIMETYPES = {
    '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    '.pdf':  'application/pdf',
}




@admin_bp.route('/live-staffs/cron/screening-download-one', methods=['GET'])
def live_staff_cron_screening_download_one():
    """
    Streams the bytes of ONE generated Screening Interview Record file
    (docx OR pdf), identified by its gcs_blob (as returned by
    /screening-list in either gcs_blob or pdf_gcs_blob). The response
    mimetype is set from the blob's own file extension, so this one
    route correctly serves both formats.

    GET /live-staffs/cron/screening-download-one?cron_key=...&gcs_blob=screening/XH_33307.docx
    GET /live-staffs/cron/screening-download-one?cron_key=...&gcs_blob=screening/XH_33307.pdf

    Protect with ?cron_key=<CRON_SECRET> env var.
    """
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    gcs_blob = (request.args.get('gcs_blob') or '').strip()
    if not gcs_blob:
        return jsonify({"success": False, "error": "Missing gcs_blob"}), 400

    try:
        file_bytes = _gcs_download(gcs_blob)
        filename   = gcs_blob.rsplit('/', 1)[-1]
        ext        = '.' + filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
        mimetype   = _DOWNLOAD_MIMETYPES.get(ext, 'application/octet-stream')
        return Response(
            file_bytes,
            mimetype=mimetype,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ═══════════════════════════════════════════════════════════════════════
# ⚠️  ADD THIS ENTIRE FILE'S CONTENTS TO YOUR CRONS BLUEPRINT FILE ONLY ⚠️
# (the file that contains live_staff_cron_generate_pcc,
# live_staff_cron_generate_screening, etc.) — NOT to admin/live_staffs.py.
#
# This file's wrapper functions (_is_nurse, _fetch_point_scale_document_url,
# etc.) import the REAL implementations from admin/live_staffs.py — they
# only work correctly when they live in a DIFFERENT file than those real
# implementations. If you paste this into live_staffs.py itself, you will
# get "maximum recursion depth exceeded", because each wrapper would then
# resolve back to itself instead of the real function.
#
# Requires incremental_credit_for_live_staffs_py.py to already be added
# to admin/live_staffs.py first.
# ═══════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────
# PART 3 — bulk cron (add to the crons blueprint file)
# ─────────────────────────────────────────────────────────────────────

# ── Lazy-import wrappers (same style as the other crons in this file) ──
def _is_nurse(user_type_value):
    from admin.live_staffs import _is_nurse as _f
    return _f(user_type_value)

def _fetch_point_scale_document_url(email):
    from admin.live_staffs import _fetch_point_scale_document_url as _f
    return _f(email)

def _download_point_scale_pdf(url):
    from admin.live_staffs import _download_point_scale_pdf as _f
    return _f(url)

def _extract_point_scale_data(pdf_bytes, gemini_key):
    from admin.live_staffs import _extract_point_scale_data as _f
    return _f(pdf_bytes, gemini_key)

def _build_incremental_credit_docx(full_name, post, employee_code, point_scale_data):
    from admin.live_staffs import _build_incremental_credit_docx as _f
    return _f(full_name, post, employee_code, point_scale_data)

def _docx_to_pdf_bytes(docx_bytes):
    from admin.live_staffs import _docx_to_pdf_bytes as _f
    return _f(docx_bytes)

def _incremental_credit_col():
    return db.live_staff_incremental_credit


@admin_bp.route('/live-staffs/cron/generate-incremental-credit', methods=['GET', 'POST'])
def live_staff_cron_generate_incremental_credit():
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    col    = _staffs_col()
    ic_col = _incremental_credit_col()

    nurse_regex = "nurse|rgn|rnm|midwife|nchd|cns|phn"

    pending_query = {
        "$and": [
            {"$or": [
                {"User Type": {"$regex": nurse_regex, "$options": "i"}},
                {"user_type": {"$regex": nurse_regex, "$options": "i"}},
            ]},
            # ⚠️ TEMPORARY — only Point Scale == 15
            # Revert to {"$gte": 12} after this run completes
            {"$or": [
                {"Point Scale": 15},
                {"point_scale": 15},
            ]},
            {"$or": [
                {"incremental_credit_generated": {"$exists": False}},
                {"incremental_credit_generated": False},
                {"incremental_credit_generated": None},
            ]},
        ]
    }

    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All Point Scale 15 Incremental Credit documents generated.",
            "remaining_count": 0,
        })

    staff_id = str(staff['_id'])

    def _first(*keys, default=''):
        for k in keys:
            val = staff.get(k)
            if val not in (None, ''):
                return _v(val)
        return default

    s1        = staff.get('section_1_personal_details') or {}
    full_name = _first('Name') or _v(s1.get('full_name') or '') or 'staff'
    emp_code  = _first('Employee Code', 'employee_code')
    user_type = _first('User Type', 'user_type')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    def _mark_done(fields):
        fields["incremental_credit_generated"]    = True
        fields["incremental_credit_generated_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    gemini_key = os.environ.get('GEMINI_API_KEY', '')

    try:
        point_scale_url  = _fetch_point_scale_document_url(email)
        point_scale_data = {
            "previous_experience": [], "point": "", "effective_date": "",
            "incremental_date": "",   "assessment_date": "",
            "signed_date": "",        "verified_date": "",
        }
        warning = None

        if point_scale_url:
            try:
                pdf_bytes        = _download_point_scale_pdf(point_scale_url)
                point_scale_data = _extract_point_scale_data(pdf_bytes, gemini_key)
            except Exception as e:
                warning = f"Could not read Point Scale Document: {e}"
        else:
            warning = "No Point Scale Document found — generated with blank service history."

        # ── Always prefer the authoritative Point Scale from live_staffs ──
        direct_point_scale = _first('Point Scale', 'point_scale')
        if direct_point_scale:
            point_scale_data['point'] = direct_point_scale

        docx_bytes = _build_incremental_credit_docx(
            full_name=full_name, post=user_type,
            employee_code=emp_code, point_scale_data=point_scale_data,
        )

        base_name    = emp_code.replace(' ', '_').replace('/', '_') if emp_code \
                       else full_name.replace(' ', '_').replace('/', '_')
        filename     = f"{base_name}.docx"
        gcs_blob     = f"incremental_credit/{filename}"
        _gcs_upload(gcs_blob, docx_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        pdf_filename = f"{base_name}.pdf"
        pdf_gcs_blob = f"incremental_credit/{pdf_filename}"
        try:
            pdf_bytes_out = _docx_to_pdf_bytes(docx_bytes)
            _gcs_upload(pdf_gcs_blob, pdf_bytes_out, content_type='application/pdf')
        except Exception as e:
            pdf_gcs_blob = None
            warning = (warning + "; " if warning else "") + f"PDF conversion failed: {e}"

        existing = ic_col.find_one({"staff_id": staff_id})
        rec = {
            "staff_id":              staff_id,
            "staff_name":            full_name,
            "employee_code":         emp_code,
            "filename":              filename,
            "gcs_blob":              gcs_blob,
            "pdf_filename":          pdf_filename if pdf_gcs_blob else None,
            "pdf_gcs_blob":          pdf_gcs_blob,
            # ✅ FIX — point is now saved so download filter works
            "point":                 point_scale_data.get("point", ""),
            "point_scale_url_found": bool(point_scale_url),
            "generated_at":          datetime.utcnow(),
        }
        if existing:
            ic_col.update_one({"_id": existing["_id"]}, {"$set": rec})
            rec_id = str(existing["_id"])
        else:
            rec_id = str(ic_col.insert_one(rec).inserted_id)

        _mark_done({"incremental_credit_gcs_blob": gcs_blob})

        result = {
            "success":               True,
            "staff_name":            full_name,
            "email":                 email,
            "incremental_credit_id": rec_id,
            "filename":              filename,
            "gcs_blob":              gcs_blob,
            "pdf_filename":          pdf_filename if pdf_gcs_blob else None,
            "pdf_gcs_blob":          pdf_gcs_blob,
            "remaining_count":       max(0, remaining_total - 1),
            "message": (
                f"Incremental Credit generated for {full_name} "
                f"({'docx + pdf' if pdf_gcs_blob else 'docx only'}) — "
                f"{max(0, remaining_total - 1)} remaining."
            ),
        }
        if warning:
            result["warning"] = warning
        return jsonify(result)

    except Exception as e:
        _mark_done({"incremental_credit_note": f"error: {e}"})
        return jsonify({
            "success":         False,
            "staff_name":      full_name,
            "email":           email,
            "error":           str(e),
            "remaining_count": max(0, remaining_total - 1),
        })


# ═══════════════════════════════════════════════════════════════════════
# ADD THIS to the same crons blueprint file as live_staff_cron_screening_list
# / live_staff_cron_screening_download_one (the file with
# download_screening_cron_route.py's content already in it).
#
# Reuses the EXISTING /live-staffs/cron/screening-download-one route for
# the actual file download — that route is already generic (it just
# streams whatever gcs_blob path you give it, with the mimetype picked
# from the file extension), so no new download route is needed. This
# file only adds the LIST route for the Incremental Credit collection.
# ═══════════════════════════════════════════════════════════════════════


@admin_bp.route('/live-staffs/cron/incremental-credit-list', methods=['GET', 'POST'])
def live_staff_cron_incremental_credit_list():
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    ic_col   = _incremental_credit_col()
    col      = _staffs_col()          # live_staffs collection
    records  = list(ic_col.find({}))

    items = []
    for rec in records:
        # ── Join back to live_staffs for the authoritative Point Scale ──
        staff_point = None
        try:
            staff_rec = col.find_one(
                {"_id": ObjectId(rec["staff_id"])},
                {"Point Scale": 1, "point_scale": 1}   # projection — only fetch what we need
            )
            if staff_rec:
                raw = staff_rec.get("Point Scale") or staff_rec.get("point_scale")
                if raw not in (None, ""):
                    staff_point = float(str(raw).strip())
        except Exception:
            pass

        items.append({
            "staff_id":       rec.get("staff_id"),
            "staff_name":     rec.get("staff_name"),
            "employee_code":  rec.get("employee_code"),
            "pdf_gcs_blob":   rec.get("pdf_gcs_blob"),
            "pdf_filename":   rec.get("pdf_filename"),
            "gcs_blob":       rec.get("gcs_blob"),
            "filename":       rec.get("filename"),
            "point":          staff_point,          # ← authoritative value from live_staffs
            "generated_at":   rec.get("generated_at", "").isoformat()
                              if hasattr(rec.get("generated_at", ""), "isoformat") else "",
        })

    return jsonify({"success": True, "items": items})


# ═══════════════════════════════════════════════════════════════════════
# ADD THESE TWO ROUTES to the same crons blueprint file as
# live_staff_cron_rename_screening_pdf_by_xnid.
#
# Fully independent of /screening-list and /screening-download-one —
# dedicated list + download routes just for the Name_xn_staff_id PDFs.
#
# Lighter than the original /screening-list: since the rename cron
# already stores xn_staff_id directly on the live_staff_screening
# record (no live join against live_staffs needed here), this just
# reads one collection with a simple filter — avoids the 502/timeout
# risk the original route had entirely, rather than just working
# around it.
# ═══════════════════════════════════════════════════════════════════════


@admin_bp.route('/live-staffs/cron/screening-pdf-xnid-list', methods=['GET'])
def live_staff_cron_screening_pdf_xnid_list():
    """
    Returns metadata for every Screening PDF that has already been
    renamed to the Name_xn_staff_id format (pdf_renamed_xnid == True).
    No live join against live_staffs — xn_staff_id is read straight off
    the live_staff_screening record, where the rename cron already
    stored it.

    Protect with ?cron_key=<CRON_SECRET> env var.
    """
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    screening_col = db.live_staff_screening

    recs = list(screening_col.find(
        {"pdf_renamed_xnid": True},
        {"staff_id": 1, "staff_name": 1, "xn_staff_id": 1,
         "pdf_filename": 1, "pdf_gcs_blob": 1}
    ))

    items = []
    for r in recs:
        items.append({
            "staff_id":     str(r.get("staff_id", "")),
            "staff_name":   r.get("staff_name", "") or "",
            "xn_staff_id":  r.get("xn_staff_id", "") or "",
            "pdf_filename": r.get("pdf_filename", "") or "",
            "pdf_gcs_blob": r.get("pdf_gcs_blob", "") or "",
        })

    return jsonify({
        "success": True,
        "count":   len(items),
        "items":   items,
    })


@admin_bp.route('/live-staffs/cron/screening-pdf-xnid-download', methods=['GET'])
def live_staff_cron_screening_pdf_xnid_download():
    """
    Streams the bytes of ONE renamed Screening PDF, identified by its
    gcs_blob (as returned by /screening-pdf-xnid-list). Independent of
    the generic /screening-download-one route.

    GET /live-staffs/cron/screening-pdf-xnid-download?cron_key=...&gcs_blob=screening/Abdul_Koyan_488291.pdf

    Protect with ?cron_key=<CRON_SECRET> env var.
    """
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    gcs_blob = (request.args.get('gcs_blob') or '').strip()
    if not gcs_blob:
        return jsonify({"success": False, "error": "Missing gcs_blob"}), 400

    try:
        pdf_bytes = _gcs_download(gcs_blob)
        filename  = gcs_blob.rsplit('/', 1)[-1]
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500