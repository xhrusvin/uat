"""
live_staffs_cron_interview.py
────────────────────────────
Cron route — uploads interview documents to the HSE Document Upload API
as hse_document_type=interview_notes.

Processes ONE staff member per call.
Only staff with interview_fetched=True but interview_uploaded≠True are picked up.

Imported by admin/__init__.py — routes are registered on admin_bp automatically.

To re-upload ALL interview docs, run in MongoDB first:
    db.live_staffs.updateMany(
        {interview_fetched: true},
        {$set: {interview_uploaded: false}}
    )
"""

from flask import request, jsonify
from datetime import datetime
import os
import requests as _req

from . import admin_bp


def _v(val):
    if val is None:
        return ''
    return str(val).strip()


def _staffs_col():
    from flask import current_app
    return current_app.db.live_staffs


def _gcs_download(blob_name):
    from admin.live_staffs import _gcs_download as _f
    return _f(blob_name)


def _docx_to_pdf_bytes(docx_bytes):
    from admin.live_staffs import _docx_to_pdf_bytes as _f
    return _f(docx_bytes)


def _resolve_xn_staff_id(mongo_id, email):
    from admin.live_staffs import _resolve_xn_staff_id as _f
    return _f(mongo_id, email)


@admin_bp.route('/live-staffs/cron/upload-interview', methods=['GET', 'POST'])
def live_staff_cron_upload_interview():
    """
    Cron job — uploads interview doc to HSE API for ONE staff member per call.

    Finds staff where interview_fetched=True but interview_uploaded is not True.
    Downloads DOCX from GCS via interview_gcs_blob, converts to PDF inline,
    and POSTs to the HSE Document Upload API as hse_document_type=interview_notes.

    Uses DOC_API_KEY + DOC_BASE_URL env vars (same as _push_hse_document_background).
    Protect with ?cron_key=<CRON_SECRET> env var.
    """
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    col = _staffs_col()

    pending_query = {
        "interview_fetched": True,
        "$or": [
            {"interview_uploaded": {"$exists": False}},
            {"interview_uploaded": False},
            {"interview_uploaded": None},
        ],
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All interview documents already uploaded.",
            "remaining_count": 0,
        })

    staff_id     = str(staff['_id'])
    s1           = staff.get('section_1_personal_details') or {}
    full_name    = _v(s1.get('full_name') or '')
    email        = _v(staff.get('email') or s1.get('email_address') or '')
    gcs_blob     = _v(staff.get('interview_gcs_blob') or '')
    interview_name = _v(staff.get('interview_document_name') or
                      f"{email}_interview_form.docx")

    def _mark_done(fields):
        fields["interview_uploaded"]    = True
        fields["interview_uploaded_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    def _mark_failed(note):
        col.update_one(
            {"_id": staff['_id']},
            {"$set": {
                "interview_uploaded":    False,
                "interview_upload_note": note,
                "interview_uploaded_at": datetime.utcnow(),
            }},
        )

    if not gcs_blob:
        _mark_failed("skipped — no interview_gcs_blob")
        return jsonify({
            "success":         False,
            "email":           email,
            "staff_name":      full_name,
            "error":           "No interview_gcs_blob on record",
            "remaining_count": max(0, remaining_total - 1),
        })

    # ── Download DOCX from GCS ────────────────────────────────────────
    try:
        docx_bytes = _gcs_download(gcs_blob)
        if not docx_bytes:
            raise ValueError("Empty response from GCS")
    except Exception as e:
        _mark_failed(f"download error: {e}")
        return jsonify({
            "success":         False,
            "email":           email,
            "staff_name":      full_name,
            "error":           f"Failed to download from GCS: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    # ── Convert DOCX → PDF ────────────────────────────────────────────
    try:
        pdf_bytes = _docx_to_pdf_bytes(docx_bytes)
        if not pdf_bytes:
            raise ValueError("PDF conversion returned empty bytes")
    except Exception as e:
        _mark_failed(f"PDF conversion error: {e}")
        return jsonify({
            "success":         False,
            "email":           email,
            "staff_name":      full_name,
            "error":           f"PDF conversion failed: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    # ── Resolve XN Portal staff ID ────────────────────────────────────
    resolved_staff_id = staff_id
    try:
        xn_id = _resolve_xn_staff_id(staff_id, email)
        if xn_id:
            resolved_staff_id = xn_id
    except Exception:
        pass  # fall back to mongo _id

    # ── POST to HSE Document Upload API ──────────────────────────────
    base_url    = os.environ.get('DOC_BASE_URL', '').rstrip('/')
    api_key     = os.environ.get('DOC_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', 'ie')
    endpoint    = f"{base_url}/api/admin/staff/hse-document-upload"

    if not base_url:
        _mark_failed("DOC_BASE_URL not set")
        return jsonify({
            "success":         False,
            "email":           email,
            "staff_name":      full_name,
            "error":           "DOC_BASE_URL not set in environment",
            "remaining_count": max(0, remaining_total - 1),
        })

    try:
        resp = _req.post(
            endpoint,
            data={
                "staff_id":          resolved_staff_id,
                "hse_document_type": "interview_notes",
            },
            files={
                "file": ("interview_notes.pdf", pdf_bytes, "application/pdf"),
            },
            headers={
                "Api-Key":       api_key,
                "X-App-Country": app_country,
                "Accept":        "application/json",
            },
            timeout=60,
        )

        try:
            resp_json = resp.json()
        except Exception:
            resp_json = {"raw": resp.text[:500]}

        if not resp.ok:
            detail = str(resp_json)
            _mark_failed(f"HSE API {resp.status_code}: {detail}")
            return jsonify({
                "success":         False,
                "email":           email,
                "staff_name":      full_name,
                "error":           f"HSE upload HTTP {resp.status_code}",
                "detail":          detail,
                "remaining_count": max(0, remaining_total - 1),
            })

    except Exception as e:
        _mark_failed(f"upload error: {e}")
        return jsonify({
            "success":         False,
            "email":           email,
            "staff_name":      full_name,
            "error":           f"HSE upload failed: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not resp_json.get("success"):
        msg = resp_json.get("message") or str(resp_json)
        _mark_failed(f"HSE API error: {msg}")
        return jsonify({
            "success":         False,
            "email":           email,
            "staff_name":      full_name,
            "error":           msg,
            "remaining_count": max(0, remaining_total - 1),
        })

    _mark_done({"interview_upload_note": "uploaded successfully"})

    return jsonify({
        "success":         True,
        "staff_name":      full_name,
        "email":           email,
        "filename":        interview_name,
        "gcs_blob":        gcs_blob,
        "xn_staff_id":     resolved_staff_id,
        "remaining_count": max(0, remaining_total - 1),
        "message": (
            f"Interview doc uploaded for {full_name} — "
            f"{max(0, remaining_total - 1)} remaining."
        ),
    })
