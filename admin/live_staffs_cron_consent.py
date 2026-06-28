"""
live_staffs_cron_consent.py
────────────────────────────
Cron route — uploads consent documents to the HSE Document Upload API
as hse_document_type=others_1.

Processes ONE staff member per call.
Only staff with consent_fetched=True but consent_uploaded≠True are picked up.

Imported by admin/__init__.py — routes are registered on admin_bp automatically.

To re-upload ALL consent docs, run in MongoDB first:
    db.live_staffs.updateMany(
        {consent_fetched: true},
        {$set: {consent_uploaded: false}}
    )
"""

from flask import request, jsonify
from datetime import datetime
import os
import requests as _req

from . import admin_bp


# ── Helpers — lazy wrappers to avoid circular imports ─────────────────

def _v(val):
    if val is None:
        return ''
    return str(val).strip()


def _staffs_col():
    from flask import current_app
    return current_app.db.live_staffs


# ── Cron: Upload Consent Documents (others_1) ─────────────────────────

@admin_bp.route('/live-staffs/cron/upload-consent', methods=['GET', 'POST'])
def live_staff_cron_upload_consent():
    """
    Cron job — uploads consent doc to HSE API for ONE staff member per call.

    Finds staff where consent_fetched=True but consent_uploaded is not True.
    Downloads the DOCX from GCS (via consent_document_url signed URL),
    then POSTs it to the HSE Document Upload API as hse_document_type=others_1.

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
        "consent_fetched": True,
        "$or": [
            {"consent_uploaded": {"$exists": False}},
            {"consent_uploaded": False},
            {"consent_uploaded": None},
        ],
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All consent documents already uploaded.",
            "remaining_count": 0,
        })

    staff_id  = str(staff['_id'])
    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    consent_url  = _v(staff.get('consent_document_url') or
                      staff.get('consent_form_url') or '')
    consent_name = _v(staff.get('consent_document_name') or
                      staff.get('consent_form_name') or
                      f"{email}_consent_form.docx")

    def _mark_done(fields):
        fields["consent_uploaded"]    = True
        fields["consent_uploaded_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    def _mark_failed(note):
        col.update_one(
            {"_id": staff['_id']},
            {"$set": {
                "consent_uploaded":    False,
                "consent_upload_note": note,
                "consent_uploaded_at": datetime.utcnow(),
            }},
        )

    if not consent_url:
        _mark_failed("skipped — no consent_document_url")
        return jsonify({
            "success":         False,
            "email":           email,
            "staff_name":      full_name,
            "error":           "No consent_document_url on record",
            "remaining_count": max(0, remaining_total - 1),
        })

    # ── Download the DOCX from GCS ────────────────────────────────────
    try:
        dl_resp = _req.get(consent_url, timeout=60)
        dl_resp.raise_for_status()
        docx_bytes = dl_resp.content
    except Exception as e:
        _mark_failed(f"download error: {e}")
        return jsonify({
            "success":         False,
            "email":           email,
            "staff_name":      full_name,
            "error":           f"Failed to download consent doc: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    # ── Push to HSE Document Upload API as others_1 ───────────────────
    hse_api_url = os.environ.get(
        'HSE_DOCUMENT_UPLOAD_URL',
        'https://admin.xpresshealthapp.com/api/admin/staff/hse-document-upload',
    )
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', 'ie')

    try:
        upload_resp = _req.post(
            hse_api_url,
            headers={
                "Api-Key":       api_key,
                "X-App-Country": app_country,
            },
            files={
                "file": (consent_name, docx_bytes,
                         "application/vnd.openxmlformats-officedocument"
                         ".wordprocessingml.document"),
            },
            data={
                "staff_id":          staff_id,
                "hse_document_type": "others_1",
            },
            timeout=60,
        )
        upload_resp.raise_for_status()
        upload_data = upload_resp.json()
    except Exception as e:
        _mark_failed(f"upload error: {e}")
        return jsonify({
            "success":         False,
            "email":           email,
            "staff_name":      full_name,
            "error":           f"HSE upload failed: {e}",
            "remaining_count": max(0, remaining_total - 1),
        })

    if not upload_data.get("success"):
        msg = upload_data.get("message") or str(upload_data)
        _mark_failed(f"upload API error: {msg}")
        return jsonify({
            "success":         False,
            "email":           email,
            "staff_name":      full_name,
            "error":           msg,
            "remaining_count": max(0, remaining_total - 1),
        })

    _mark_done({"consent_upload_note": "uploaded successfully"})

    return jsonify({
        "success":         True,
        "staff_name":      full_name,
        "email":           email,
        "filename":        consent_name,
        "remaining_count": max(0, remaining_total - 1),
        "message": (
            f"Consent doc uploaded for {full_name} — "
            f"{max(0, remaining_total - 1)} remaining."
        ),
    })
