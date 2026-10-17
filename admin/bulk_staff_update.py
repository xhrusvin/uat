"""
bulk_staff_update.py
─────────────────────
Admin UI + backend proxy for the Bulk Staff Update API
(POST {{user_url}}/ai/recruitments/bulk-staff-update) and the
Master Document List API (GET {{user_url}}/audit/master/document-list).

Mirrors the structure of live_staffs.py: same admin_bp blueprint,
same admin_required decorator, same render_template pattern.

The browser never sees the external Api-Key — every request to the
outreach API is made server-side here. The upload form POSTs
multipart/form-data directly in the same field-naming convention the
external API expects (staff[0][staff_id], staff[0][documents][0]
[document_key], etc.); this module simply relays those fields and any
attached files through to the real endpoint with the secret headers
added, then returns the JSON response as-is.

Constants kept as pre-filled UI defaults (confirmed from a real
request): user_id and requested_by. Both remain editable in the UI.
"""

from flask import render_template, request, jsonify, Response
import os
import requests as _req
from datetime import datetime

from . import admin_bp
from admin.views import admin_required


# ── Defaults (kept as editable pre-filled values in the UI) ───────────
DEFAULT_USER_ID      = "6916f06c4d6070d4f90d9b61"
DEFAULT_REQUESTED_BY = "rusvin@xpresshealth.ie"


# ── Fixed reference data from the API documentation ────────────────────

# document_key values — the 8 kinds of document this API can touch.
DOCUMENT_KEYS = [
    {"key": "documents",              "label": "General (user type / sub type) document", "needs_id": True,  "id_source": "general"},
    {"key": "hse",                    "label": "Mandatory (HSE) document",                 "needs_id": True,  "id_source": "hse"},
    {"key": "occupational_health",    "label": "Occupational health certificate",          "needs_id": True,  "id_source": "occ_health"},
    {"key": "visa",                   "label": "Visa / IRP",                               "needs_id": False, "id_source": None},
    {"key": "passport_id_card",       "label": "Passport / ID card",                       "needs_id": False, "id_source": None},
    {"key": "qualification_document", "label": "Qualification document",                   "needs_id": False, "id_source": None},
    {"key": "address_proof",          "label": "Address proof",                            "needs_id": False, "id_source": None},
    {"key": "ni_document",            "label": "NI document",                              "needs_id": False, "id_source": None},
]

# hse document_id values — fixed IDs, from section 2 of the API docs.
# document_id can be sent as either the numeric ID or the name; the UI
# sends the numeric ID.
HSE_DOCUMENTS = [
    {"id": 2,  "name": "police_clearance_self_declaration", "label": "Self Declaration Of Criminal Records"},
    {"id": 3,  "name": "hse_cv",                             "label": "HSE CV"},
    {"id": 5,  "name": "point_scale_document",                "label": "Point Scale Document"},
    {"id": 6,  "name": "interview_notes",                     "label": "Interview Notes"},
    {"id": 8,  "name": "reference_1_2_3",                     "label": "Reference 1"},
    {"id": 17, "name": "mapa_cpi",                             "label": "MAPA/CPI"},
    {"id": 18, "name": "cpr",                                  "label": "CPR"},
    {"id": 19, "name": "manual_handling_practical",            "label": "Manual Handling Practical"},
    {"id": 20, "name": "manual_handling_theory",                "label": "Manual Handling Theory"},
    {"id": 21, "name": "fire_safety",                           "label": "Fire Safety"},
    {"id": 22, "name": "infection_prevention_control",          "label": "Infection Prevention Control"},
    {"id": 23, "name": "hand_hygiene",                           "label": "Hand Hygiene"},
    {"id": 24, "name": "dignity_at_work",                        "label": "Dignity At Work"},
    {"id": 25, "name": "cyber_security",                         "label": "Cyber Security"},
    {"id": 26, "name": "open_disclosure",                        "label": "Open Disclosure"},
    {"id": 27, "name": "children_first",                         "label": "Children First"},
    {"id": 28, "name": "occupational_health_clearance",          "label": "Occupational Health Clearance"},
    {"id": 31, "name": "validation_of_services",                 "label": "Validation Of Services"},
    {"id": 32, "name": "garda_vetting",                          "label": "Garda Vetting"},
    {"id": 33, "name": "passport_id_card",                       "label": "Passport ID card"},
    {"id": 34, "name": "visa",                                   "label": "Visa"},
    {"id": 35, "name": "nmbi_qqi_l5",                            "label": "NMBI / QQI L5 or equivalent"},
    {"id": 36, "name": "police_clearance_certificate",           "label": "Police Clearance Certificate (From Country Of Birth)"},
    {"id": 37, "name": "safeguarding_adults_at_risk",            "label": "Safeguarding Adults At Risk"},
    {"id": 38, "name": "gdpr",                                   "label": "GDPR"},
    {"id": 39, "name": "trust_in_care",                          "label": "Trust in Care"},
    {"id": 40, "name": "visual_display_equipment",               "label": "Visual Display Equipment"},
    {"id": 42, "name": "medication_management",                  "label": "Medication Management"},
    {"id": 43, "name": "inews_v2",                                "label": "Irish National Early Warning System (Inews) V2: Nursing/Hscp"},
    {"id": 44, "name": "sepsis_management",                       "label": "Sepsis Management"},
    {"id": 55, "name": "human_rights_based_approach",             "label": "Applying A Human Rights-Based Approach in Health and Social Care"},
    {"id": 56, "name": "assisted_decision_making_act",            "label": "Assisted Decision Making (Capacity) Act 2015: Advanced Health Care Directives"},
    {"id": 57, "name": "supporting_decision_making",              "label": "Supporting Decision Making In Health & Social Care"},
    {"id": 58, "name": "hse_national_consent_policy",             "label": "Hse National Consent Policy V1.2"},
    {"id": 59, "name": "dementia_non_cognitive_support",          "label": "Support Pathways For People With Non Cognitive Symptoms Of Dementia"},
    {"id": 60, "name": "hiqa_fundamentals_of_advocacy",           "label": "Hiqa The Fundamentals Of Advocacy In Health & Social Care"},
    {"id": 61, "name": "patient_safety_act_2023",                 "label": "The Patient Safety Act 2023: Overview & Guidance"},
    {"id": 62, "name": "reference_2",                             "label": "Reference 2"},
    {"id": 63, "name": "reference_3",                             "label": "Reference 3"},
    {"id": 64, "name": "haccp",                                   "label": "Hazard Analysis Critical Control Point (HACCP)"},
    {"id": 65, "name": "amric_standard_precautions",              "label": "AMRIC Standard and Transmission Based Precautions"},
]

# occupational_health document_id slots — fixed, from section 2.
OCCUPATIONAL_HEALTH_SLOTS = [
    {"id": 5, "label": "Occupational Health",        "vaccinations": False},
    {"id": 1, "label": "Vaccination Certificate 1",  "vaccinations": True},
    {"id": 2, "label": "Vaccination Certificate 2",  "vaccinations": True},
    {"id": 3, "label": "Vaccination Certificate 3",  "vaccinations": True},
    {"id": 4, "label": "Vaccination Certificate 4",  "vaccinations": True},
]

# vaccinations[] values — fixed, from section 2.
VACCINATIONS = [
    {"id": 2, "label": "MMR"},
    {"id": 3, "label": "TB (BCG)"},
    {"id": 4, "label": "Varicella (Chickenpox)"},
    {"id": 5, "label": "Hepatitis B"},
]

# status values — the same list the admin panel uses, from section 3.
# 6 (Deleted) is intentionally excluded — this API does not accept it.
STATUSES = [
    {"id": 0, "label": "Pending"},
    {"id": 1, "label": "Approved"},
    {"id": 2, "label": "Not Approved (submitted for review)"},
    {"id": 3, "label": "Rejected"},
    {"id": 4, "label": "Expired"},
    {"id": 5, "label": "Under Review"},
    {"id": 7, "label": "AI Accepted"},
    {"id": 8, "label": "AI Rejected"},
]

# X-App-Country options, from both API docs.
APP_COUNTRIES = ["ie", "uk", "ni", "au"]


# ── Config helpers ──────────────────────────────────────────────────────

def _base_url():
    return (os.environ.get('XN_PORTAL_BASE_URL', '').rstrip('/') or
            os.environ.get('LIVE_STAFF_URL', '').rstrip('/'))


def _api_key():
    return os.environ.get('XN_PORTAL_API_KEY', '')


def _staffs_col():
    from database import db
    return db.live_staffs


def _v(val):
    if val is None:
        return ''
    return str(val).strip()


# ── Page ─────────────────────────────────────────────────────────────

@admin_bp.route('/bulk-staff-update')
@admin_required
def bulk_staff_update_page():
    return render_template(
        'admin/bulk_staff_update.html',
        document_keys=DOCUMENT_KEYS,
        hse_documents=HSE_DOCUMENTS,
        occ_health_slots=OCCUPATIONAL_HEALTH_SLOTS,
        vaccinations=VACCINATIONS,
        statuses=STATUSES,
        app_countries=APP_COUNTRIES,
        default_user_id=DEFAULT_USER_ID,
        default_requested_by=DEFAULT_REQUESTED_BY,
        default_request_id=f"{datetime.utcnow().strftime('%Y-%m-%d')}-import-01",
    )


# ── Staff search (for picking staff_id values in the UI) ───────────────

@admin_bp.route('/bulk-staff-update/staff-search')
@admin_required
def bulk_staff_update_staff_search():
    """
    Lightweight staff search for the picker — matches by name, email,
    or Employee Code, across both schema variants. Returns at most 20
    results.

    GET /admin/bulk-staff-update/staff-search?q=<text>
    """
    q = (request.args.get('q') or '').strip()
    if len(q) < 2:
        return jsonify({"success": True, "items": []})

    import re as _re
    pattern = _re.compile(_re.escape(q), _re.IGNORECASE)

    docs = list(_staffs_col().find(
        {"$or": [
            {"section_1_personal_details.full_name": pattern},
            {"Name": pattern},
            {"email": pattern},
            {"employee_code": pattern},
            {"Employee Code": pattern},
        ]},
        {"section_1_personal_details": 1, "email": 1, "Name": 1,
         "employee_code": 1, "Employee Code": 1}
    ).limit(20))

    items = []
    for d in docs:
        s1 = d.get('section_1_personal_details') or {}
        name = d.get('Name') or s1.get('full_name') or ''
        code = d.get('Employee Code') or d.get('employee_code') or ''
        items.append({
            "staff_id": str(d['_id']),
            "name":     _v(name),
            "email":    _v(d.get('email') or ''),
            "employee_code": _v(code),
        })

    return jsonify({"success": True, "items": items})


# ── Master Document List proxy (general document type reference) ──────

@admin_bp.route('/bulk-staff-update/master-documents')
@admin_required
def bulk_staff_update_master_documents():
    """
    Proxies GET {{user_url}}/audit/master/document-list — used to show
    the admin which general (user type / sub type) document types
    exist, with their document_id, for the "documents" document_key.

    GET /admin/bulk-staff-update/master-documents?status=1&category_type=user_type&search=...
    """
    base_url = _base_url()
    api_key  = _api_key()
    app_country = request.args.get('app_country', 'ie')

    if not base_url:
        return jsonify({"success": False, "error": "XN_PORTAL_BASE_URL / LIVE_STAFF_URL not set"}), 500

    params = {}
    if request.args.get('status') in ('0', '1'):
        params['status'] = request.args.get('status')
    if request.args.get('category_type') in ('user_type', 'user_sub_type'):
        params['category_type'] = request.args.get('category_type')
    if request.args.get('search'):
        params['search'] = request.args.get('search')[:255]

    try:
        resp = _req.get(
            f"{base_url}/audit/master/document-list",
            params=params,
            headers={"Api-Key": api_key, "X-App-Country": app_country, "Accept": "application/json"},
            timeout=30,
        )
        return (resp.text, resp.status_code, {"Content-Type": "application/json"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 502


# ── Bulk update submit proxy ────────────────────────────────────────────

@admin_bp.route('/bulk-staff-update/submit', methods=['POST'])
@admin_required
def bulk_staff_update_submit():
    """
    Relays the submitted multipart/form-data straight through to
    POST {{user_url}}/ai/recruitments/bulk-staff-update, adding the
    secret Api-Key and X-App-Country headers server-side. The browser
    builds the form fields using the exact nested-array naming the
    external API expects (staff[0][staff_id],
    staff[0][documents][0][document_key], etc.) — this route does not
    need to understand that structure, it just passes every field and
    file through unchanged.
    """
    base_url = _base_url()
    api_key  = _api_key()
    if not base_url:
        return jsonify({"success": False, "error": "XN_PORTAL_BASE_URL / LIVE_STAFF_URL not set"}), 500

    app_country = request.form.get('x_app_country', 'ie')

    # Rebuild the multipart payload field-for-field and file-for-file,
    # preserving repeated keys (e.g. vaccinations[] with several values).
    data_payload = []
    for key in request.form:
        if key == 'x_app_country':
            continue  # consumed above, not part of the real API's fields
        for val in request.form.getlist(key):
            data_payload.append((key, val))

    files_payload = []
    for key in request.files:
        for f in request.files.getlist(key):
            if f and f.filename:
                files_payload.append((key, (f.filename, f.read(), f.mimetype)))

    try:
        resp = _req.post(
            f"{base_url}/ai/recruitments/bulk-staff-update",
            headers={"Api-Key": api_key, "X-App-Country": app_country, "Accept": "application/json"},
            data=data_payload,
            files=files_payload if files_payload else None,
            timeout=180,
        )
        return (resp.text, resp.status_code, {"Content-Type": "application/json"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 502


# ── Batch status proxy ──────────────────────────────────────────────────

@admin_bp.route('/bulk-staff-update/status')
@admin_required
def bulk_staff_update_status():
    """
    Proxies GET {{user_url}}/ai/recruitments/bulk-staff-update/status.

    GET /admin/bulk-staff-update/status?batch_id=...&status=failed&page=1&per_page=50&app_country=ie
    """
    base_url = _base_url()
    api_key  = _api_key()
    app_country = request.args.get('app_country', 'ie')

    if not base_url:
        return jsonify({"success": False, "error": "XN_PORTAL_BASE_URL / LIVE_STAFF_URL not set"}), 500

    batch_id = (request.args.get('batch_id') or '').strip()
    if not batch_id:
        return jsonify({"success": False, "error": "Missing batch_id"}), 400

    params = {"batch_id": batch_id}
    if request.args.get('status'):
        params['status'] = request.args.get('status')
    if request.args.get('page'):
        params['page'] = request.args.get('page')
    if request.args.get('per_page'):
        params['per_page'] = request.args.get('per_page')

    try:
        resp = _req.get(
            f"{base_url}/ai/recruitments/bulk-staff-update/status",
            params=params,
            headers={"Api-Key": api_key, "X-App-Country": app_country, "Accept": "application/json"},
            timeout=30,
        )
        return (resp.text, resp.status_code, {"Content-Type": "application/json"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 502
