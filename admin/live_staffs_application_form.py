"""
live_staffs_application_form.py
═══════════════════════════════
Generates the "Xpress Health Application Form" PDF for a staff member and
uploads it to the XN Portal HSE Document Upload API as
hse_document_type=application_form.

Register on admin_bp — add to admin/__init__.py:
    from . import live_staffs_application_form

Routes
──────
POST /live-staffs/webhook/user-to-staff          — XN Portal webhook (primary entry)
POST /live-staffs/application-form/generate/<staff_id>  — manual regenerate (admin)
GET  /live-staffs/application-form/download/<staff_id>   — download generated PDF
GET  /live-staffs/cron/upload-application-form   — backfill cron, ONE staff per call
GET  /live-staffs/api/application-form-status/<staff_id> — status lookup

Environment
───────────
DOC_BASE_URL         https://admin.xpresshealthapp.com   (HSE upload host)
DOC_API_KEY          Api-Key for the HSE upload endpoint
XN_APP_COUNTRY       ie
LIVE_STAFF_URL       XN Portal AI base URL (document list lookup)
XN_PORTAL_API_KEY    Api-Key for the document list lookup
WEBHOOK_SECRET       shared secret the XN Portal sends as X-Webhook-Key
CRON_SECRET          protects the backfill cron route

Mongo bookkeeping
─────────────────
Collection: live_staff_application_forms  (one doc per staff_id)
    status: processing | generated | uploaded | error | skipped
Flags mirrored onto the staff record (STAFF_COLLECTION, default `users`):
    application_form_generated, application_form_uploaded,
    application_form_gcs_blob, application_form_upload_note

To force a full re-run:
    db.live_staff_application_forms.deleteMany({})
    db.users.updateMany({}, {$unset: {application_form_uploaded: ""}})
"""

from flask import request, jsonify, Response
from bson import ObjectId
from datetime import datetime
import os
import io
import threading

from database import db
from . import admin_bp
from admin.views import admin_required


# ── Helpers ───────────────────────────────────────────────────────────

def _v(val):
    if val is None:
        return ''
    return str(val).strip()


# Collection holding the synced staff/user records. Override with
# STAFF_COLLECTION if your deployment uses a different name.
STAFF_COLLECTION = os.environ.get('STAFF_COLLECTION', 'users')


def _staffs_col():
    from flask import current_app
    return current_app.db[STAFF_COLLECTION]


def _af_col():
    """Application form bookkeeping collection."""
    return db.live_staff_application_forms


def _gcs_upload(blob_name, data_bytes, content_type='application/octet-stream'):
    from admin.live_staffs import _gcs_upload as _f
    return _f(blob_name, data_bytes, content_type)


def _gcs_download(blob_name):
    from admin.live_staffs import _gcs_download as _f
    return _f(blob_name)


def _resolve_xn_staff_id(mongo_id, email):
    from admin.live_staffs import _resolve_xn_staff_id as _f
    return _f(mongo_id, email)


def _yes_no(val):
    """1/True/'yes' → 'Yes'; 0/False/'no' → 'No'; anything empty → ''."""
    if val is None or val == '':
        return ''
    if isinstance(val, bool):
        return 'Yes' if val else 'No'
    s = _v(val).lower()
    if s in ('1', 'yes', 'true', 'y'):
        return 'Yes'
    if s in ('0', 'no', 'false', 'n'):
        return 'No'
    return _v(val)


_MASTER_CACHE = {}


def _master_name(oid, *collections):
    """
    Resolve a master-data ObjectId (visa type, county, gender…) to its name.
    Tries each candidate collection; returns '' when nothing matches.
    Cached per-process because these never change mid-run.
    """
    key = _v(oid)
    if not key:
        return ''
    if key in _MASTER_CACHE:
        return _MASTER_CACHE[key]

    name = ''
    candidates = collections or ('visa_types', 'counties', 'genders',
                                 'masters', 'xn_masters', 'user_sub_types')
    for cname in candidates:
        try:
            col = getattr(db, cname)
            q = {"_id": ObjectId(key)} if len(key) == 24 else {"_id": key}
            rec = col.find_one(q) or col.find_one({"xn_id": key})
            if rec:
                name = _v(rec.get('name') or rec.get('title') or
                          rec.get('label') or rec.get('value'))
                if name:
                    break
        except Exception:
            continue

    _MASTER_CACHE[key] = name
    return name


# ── Field mapping ─────────────────────────────────────────────────────

def _staff_fields(staff):
    """
    Flatten a staff/user document into the fields the application form needs.

    Handles BOTH shapes:
      • the XN-synced flat shape (first_name, designation, phone, references[]…)
      • the older nested shape (section_1_personal_details…)
    Flat values win; nested sections are the fallback.
    """
    s1    = staff.get('section_1_personal_details')          or {}
    s2    = staff.get('section_2_identity_verification')     or {}
    s3    = staff.get('section_3_professional_registration') or {}
    s5    = staff.get('section_5_employment_history')        or {}
    s7    = staff.get('section_7_references')                or {}
    s9    = staff.get('section_9_occupational_health')       or {}
    visa  = s1.get('work_permit_visa_status') or {}

    # ── Name ──────────────────────────────────────────────────────────
    full_name = _v(staff.get('name'))
    if not full_name:
        full_name = ' '.join(
            p for p in [_v(staff.get('first_name')), _v(staff.get('last_name'))] if p
        ).strip()
    if not full_name:
        full_name = _v(s1.get('full_name'))

    email = _v(staff.get('email') or s1.get('email_address'))
    role  = _v(staff.get('designation') or staff.get('user_type') or
               staff.get('job_title')  or s1.get('role'))

    # ── Address — flatten embedded newlines into a single readable line ─
    address = _v(staff.get('address') or s1.get('address'))
    address = ', '.join(
        seg.strip(' .,') for seg in address.replace('\r', '\n').split('\n')
        if seg.strip(' .,')
    )
    eir_code = _v(staff.get('eir_code') or staff.get('eircode') or
                  s1.get('eircode') or s1.get('eir_code'))

    # ── Phone ─────────────────────────────────────────────────────────
    mobile = _v(staff.get('phone') or s1.get('mobile_number'))
    if not mobile:
        mobile = ' '.join(p for p in [_v(staff.get('dial_code')),
                                      _v(staff.get('mobile'))] if p).strip()

    # ── Work permit / visa ────────────────────────────────────────────
    perm = staff.get('permission_to_work')
    if perm is None:
        perm = visa.get('permission_to_work')
    perm_txt = _yes_no(perm)

    visa_type = _v(staff.get('visa_type') or visa.get('visa_type'))
    if not visa_type:
        visa_type = _master_name(staff.get('visa_type_id'),
                                 'visa_types', 'master_visa_types', 'masters')
    visa_parts = [p for p in [perm_txt, visa_type] if p]
    if _v(staff.get('work_permit_exemption')) == '1':
        visa_parts.append('Work permit exempt')
    visa_status = '; '.join(visa_parts)

    # ── Experience ────────────────────────────────────────────────────
    exp_y = staff.get('experience_year')
    exp_m = staff.get('experience_month')
    if exp_y is None and exp_m is None:
        total_exp = _v(s5.get('total_experience'))
    else:
        try:
            exp_y = int(exp_y or 0)
            exp_m = int(exp_m or 0)
        except (TypeError, ValueError):
            exp_y, exp_m = 0, 0
        total_exp = (f"{exp_y} year{'s' if exp_y != 1 else ''}, "
                     f"{exp_m} month{'s' if exp_m != 1 else ''}")

    # ── Role type checkboxes (Nurse NMBI / HCA QQI L5) ────────────────
    role_l    = role.lower()
    subtypes  = ' '.join(_v(t) for t in (staff.get('user_sub_type_ids') or []))
    combined  = f"{role_l} {subtypes.lower()}"
    is_nurse  = ('nurse' in combined or 'nmbi' in combined or
                 'midwif' in combined)
    is_hca    = ('healthcare assistant' in combined or 'hca' in combined or
                 'qqi'  in combined or 'care assistant' in combined)
    if not (is_nurse or is_hca) and role_l:
        is_hca = True   # default non-nurse clinical staff to the HCA line

    # ── Vaccination ───────────────────────────────────────────────────
    vac_map = [
        ('TB',          staff.get('tuberculosis_vaccine')),
        ('Hep B',       staff.get('hepatitis_antibody')),
        ('MMR',         staff.get('mmr_vaccine')),
        ('COVID-19',    staff.get('covid_19_vaccine')),
    ]
    vac_present = [(lbl, val) for lbl, val in vac_map if val is not None]
    if vac_present:
        got = [lbl for lbl, val in vac_present if _yes_no(val) == 'Yes']
        vaccinated     = 'Yes' if len(got) == len(vac_present) else ('Partial' if got else 'No')
        vaccine_detail = ', '.join(got)
    else:
        vaccinated     = _yes_no(s9.get('vaccinated'))
        vaccine_detail = ''

    # ── References — always exactly 3 blocks (blank when missing) ─────
    raw_refs = staff.get('references')
    if not raw_refs:
        raw_refs = s7.get('references') or s7.get('entries') or []
    refs = []
    for r in (raw_refs or [])[:3]:
        r = r or {}
        phone = _v(r.get('phone') or r.get('telephone'))
        dial  = _v(r.get('dial_code'))
        if dial and phone and not phone.startswith('+'):
            phone = f"{dial} {phone}"
        refs.append({
            'name':         _v(r.get('name')),
            'position':     _v(r.get('job_role') or r.get('position') or
                               r.get('designation')),
            'organisation': _v(r.get('organization') or r.get('organisation') or
                               r.get('company_name')),
            'telephone':    phone,
            'email':        _v(r.get('email')),
            'status':       _v(r.get('status')),
        })
    while len(refs) < 3:
        refs.append({'name': '', 'position': '', 'organisation': '',
                     'telephone': '', 'email': '', 'status': ''})

    return {
        'full_name':      full_name,
        'email':          email,
        'role':           role,
        'address':        address,
        'eir_code':       eir_code,
        'mobile':         mobile,
        'visa_status':    visa_status,
        'pps_number':     _v(staff.get('pps_number') or s1.get('pps_number')),
        'dob':            _v(staff.get('dob') or s1.get('date_of_birth')),
        'total_exp':      total_exp,
        'is_nurse':       is_nurse,
        'is_hca':         is_hca,
        'vaccinated':     vaccinated,
        'vaccine_detail': vaccine_detail,
        'references':     refs,
        'nmbi_pin':       _v(s3.get('registration_number_pin')),
        'id_docs_nested': s2.get('documents_submitted') or {},
    }


# ── XN Portal document list → identity verification ticks ─────────────

_ID_PROOF_MATCHERS = {
    'passport':         ('passport',),
    'birth_cert':       ('birth cert', 'birth certificate'),
    'driving_licence':  ('driving licence', 'driving license', 'drivers licence',
                         'driver licence', 'driver license'),
    'proof_of_address': ('proof of address', 'address proof', 'utility bill',
                         'bank statement', 'proof of residence'),
}


def _fetch_xn_documents(email):
    """
    Fetch the staff member's uploaded document list from the XN Portal.
    Returns (docs, headers) — headers are reusable for downloading a doc URL.
    Never raises: returns ([], headers) on any failure.
    """
    import requests as _req

    base_url    = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', 'ie')

    hdrs = {"Api-Key": api_key, "X-App-Country": app_country,
            "Content-Type": "application/json", "Accept": "application/json"}
    dl_hdrs = {k: v for k, v in hdrs.items() if k != 'Content-Type'}

    if not base_url or not email:
        return [], dl_hdrs

    try:
        r = _req.post(f"{base_url}/ai/recruitments/user-document-list",
                      json={"email": email}, headers=hdrs, timeout=30)
        if r.status_code == 405:
            r = _req.get(f"{base_url}/ai/recruitments/user-document-list",
                         params={"email": email}, headers=hdrs, timeout=30)
        r.raise_for_status()
        api_data = r.json().get('data')
        docs = api_data if isinstance(api_data, list) else \
               (api_data.get('documents') or [] if isinstance(api_data, dict) else [])
        return docs or [], dl_hdrs
    except Exception:
        return [], dl_hdrs


def _id_proof_flags(docs, nested_fallback=None):
    """Tick the Section 2 boxes from the XN Portal document type names."""
    flags = {k: False for k in _ID_PROOF_MATCHERS}

    for doc in docs or []:
        name = _v(doc.get('document_type_name') or doc.get('name')).lower()
        if not name:
            continue
        for key, needles in _ID_PROOF_MATCHERS.items():
            if any(n in name for n in needles):
                flags[key] = True

    # Fall back to the nested schema's documents_submitted map
    nested = nested_fallback or {}
    alias = {
        'passport':         ('passport',),
        'birth_cert':       ('birth_certificate', 'birth_cert'),
        'driving_licence':  ('driving_licence', 'driving_license'),
        'proof_of_address': ('proof_of_address',),
    }
    for key, nkeys in alias.items():
        if not flags[key]:
            for nk in nkeys:
                if _yes_no(nested.get(nk)) == 'Yes':
                    flags[key] = True
                    break

    return flags


def _fetch_signature_bytes(staff, docs, dl_hdrs):
    """
    Best-effort signature image for the declaration block.
    Order: GCS blob on the staff doc → signature URL → an XN doc whose
    type name mentions 'signature'. Returns raw bytes or None.
    """
    import requests as _req

    blob = _v(staff.get('signature_gcs_blob'))
    if blob:
        try:
            data = _gcs_download(blob)
            if data:
                return data
        except Exception:
            pass

    url = _v(staff.get('signature_url') or staff.get('signature'))
    if not url:
        for doc in docs or []:
            name = _v(doc.get('document_type_name') or doc.get('name')).lower()
            if 'signature' in name and _v(doc.get('url')):
                url = _v(doc.get('url'))
                break

    if url.lower().startswith('http'):
        try:
            r = _req.get(url, headers=dl_hdrs, timeout=30)
            r.raise_for_status()
            ct = (r.headers.get('Content-Type') or '').lower()
            if 'image' in ct or url.lower().split('?')[0].endswith(
                    ('.png', '.jpg', '.jpeg')):
                return r.content
        except Exception:
            pass

    return None


# ── PDF builder ───────────────────────────────────────────────────────

def _build_application_form_pdf(staff, id_flags=None, signature_bytes=None):
    """
    Build the Xpress Health Application Form as PDF bytes.
    Mirrors the approved DOCX layout:
        Section 1 — Personal Details
        Section 2 — Identity Verification (tick boxes)
        Section 3 — Qualification and Experience
        References 1-3
        Vaccination + declaration + signature
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
        Image as RLImage, KeepTogether,
    )

    f = _staff_fields(staff)
    flags = id_flags or {k: False for k in _ID_PROOF_MATCHERS}

    # ── Brand palette (matches _build_cv_pdf) ─────────────────────────
    NAVY      = colors.HexColor('#1B3A6B')
    XH_GREEN  = colors.HexColor('#2E9E44')
    LIGHT_BG  = colors.HexColor('#EFF6FF')
    MID_GRAY  = colors.HexColor('#CBD5E1')
    TEXT_GRAY = colors.HexColor('#475569')
    WHITE     = colors.white

    W, H   = A4
    PAGE_W = W - 30 * mm

    def ps(name, **kw):
        d = dict(fontName='Helvetica', fontSize=10, textColor=TEXT_GRAY,
                 spaceAfter=2, leading=14)
        d.update(kw)
        return ParagraphStyle(name, **d)

    S = {
        'title':   ps('title',   fontName='Helvetica-Bold', fontSize=18,
                      textColor=WHITE, alignment=TA_CENTER, leading=22, spaceAfter=0),
        'sub':     ps('sub',     fontName='Helvetica-Bold', fontSize=11,
                      textColor=WHITE, alignment=TA_CENTER, leading=15, spaceAfter=0),
        'sec':     ps('sec',     fontName='Helvetica-Bold', fontSize=10,
                      textColor=WHITE, leading=13, spaceAfter=0),
        'refhead': ps('refhead', fontName='Helvetica-Bold', fontSize=10,
                      textColor=NAVY, leading=13, spaceAfter=0),
        'lbl':     ps('lbl',     fontName='Helvetica-Bold', fontSize=9.5,
                      textColor=NAVY, leading=13, spaceAfter=0),
        'val':     ps('val',     fontSize=9.5, leading=13, spaceAfter=0),
        'body':    ps('body',    fontSize=9.5, leading=14, alignment=TA_JUSTIFY,
                      spaceAfter=0),
        'cb':      ps('cb',      fontName='Helvetica-Bold', fontSize=8,
                      textColor=NAVY, alignment=TA_CENTER, leading=8, spaceAfter=0),
        'foot':    ps('foot',    fontSize=7, textColor=MID_GRAY,
                      alignment=TA_CENTER, spaceAfter=0),
    }

    sp = lambda n=3: Spacer(1, n * mm)

    def sec_bar(text):
        t = Table([[Paragraph(text, S['sec'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0, 0), (-1, -1), NAVY),
            ('TOPPADDING',    (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('LEFTPADDING',   (0, 0), (-1, -1), 8),
            ('LINEBELOW',     (0, 0), (-1, -1), 2, XH_GREEN),
        ]))
        return t

    def field_rows(pairs, lw=48 * mm):
        """Label/value table; renders an em-dash for blanks so gaps are visible."""
        rows = [[Paragraph(lbl, S['lbl']),
                 Paragraph(val if _v(val) else '\u2014', S['val'])]
                for lbl, val in pairs]
        t = Table(rows, colWidths=[lw, PAGE_W - lw])
        t.setStyle(TableStyle([
            ('TOPPADDING',    (0, 0), (-1, -1), 3.5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3.5),
            ('LEFTPADDING',   (0, 0), (-1, -1), 6),
            ('VALIGN',        (0, 0), (-1, -1), 'TOP'),
            ('LINEBELOW',     (0, 0), (-1, -2), 0.3, MID_GRAY),
        ]))
        return t

    def _box(checked):
        """A 4mm bordered square, ticked with an X."""
        inner = Table([[Paragraph('X' if checked else '', S['cb'])]],
                      colWidths=[4 * mm], rowHeights=[4 * mm])
        inner.setStyle(TableStyle([
            ('BOX',           (0, 0), (-1, -1), 0.7, NAVY),
            ('BACKGROUND',    (0, 0), (-1, -1), LIGHT_BG if checked else WHITE),
            ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING',    (0, 0), (-1, -1), 0),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
            ('LEFTPADDING',   (0, 0), (-1, -1), 0),
            ('RIGHTPADDING',  (0, 0), (-1, -1), 0),
        ]))
        return inner

    def checkbox_row(items):
        """items: [(label, checked), …] laid out evenly across the page."""
        n     = max(1, len(items))
        cell_w = PAGE_W / n
        cells, widths = [], []
        for label, checked in items:
            cells.extend([_box(checked), Paragraph(label, S['val'])])
            widths.extend([6 * mm, cell_w - 6 * mm])
        t = Table([cells], colWidths=widths)
        t.setStyle(TableStyle([
            ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING',    (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
            ('LEFTPADDING',   (0, 0), (-1, -1), 0),
            ('RIGHTPADDING',  (0, 0), (-1, -1), 2),
        ]))
        return t

    # ── Logo ──────────────────────────────────────────────────────────
    logo_path = None
    for c in [
        os.path.join(os.path.dirname(__file__), '..', 'static', 'images', 'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'image',  'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'img',    'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'logo.png'),
        'static/images/logo.png', 'static/image/logo.png',
        'static/img/logo.png', 'static/logo.png',
    ]:
        if os.path.exists(c):
            logo_path = c
            break

    buf   = io.BytesIO()
    story = []

    # ══ HEADER ════════════════════════════════════════════════════════
    head_rows = [
        [Paragraph('XPRESS HEALTH APPLICATION FORM', S['title'])],
        [Paragraph(f.get('full_name') or 'Applicant', S['sub'])],
    ]
    head_w   = PAGE_W - (50 * mm if logo_path else 0)
    head_tbl = Table(head_rows, colWidths=[head_w])
    head_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0, 0), (-1, -1), NAVY),
        ('TOPPADDING',    (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
        ('BACKGROUND',    (0, 1), (-1, 1), colors.HexColor('#162F58')),
        ('TOPPADDING',    (0, 1), (-1, 1), 5),
        ('BOTTOMPADDING', (0, 1), (-1, 1), 7),
    ]))

    if logo_path:
        try:
            logo_cell = Table(
                [[RLImage(logo_path, width=40 * mm, height=40 * mm * 94 / 316)]],
                colWidths=[50 * mm])
            logo_cell.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), WHITE),
                ('VALIGN',     (0, 0), (-1, -1), 'MIDDLE'),
                ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ]))
            banner = Table([[logo_cell, head_tbl]], colWidths=[50 * mm, head_w])
            banner.setStyle(TableStyle([
                ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
                ('LEFTPADDING',   (0, 0), (-1, -1), 0),
                ('RIGHTPADDING',  (0, 0), (-1, -1), 0),
                ('TOPPADDING',    (0, 0), (-1, -1), 0),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
                ('BOX',           (0, 0), (-1, -1), 0.5, NAVY),
            ]))
            story.append(banner)
        except Exception:
            story.append(head_tbl)
    else:
        story.append(head_tbl)

    story.append(sp(5))

    # ══ SECTION 1 — Personal Details ══════════════════════════════════
    story.append(sec_bar('SECTION 1 \u2014 PERSONAL DETAILS'))
    story.append(sp(1.5))
    story.append(field_rows([
        ('Full Name',            f['full_name']),
        ('Email',                f['email']),
        ('Role',                 f['role']),
        ('Address',              f['address']),
        ('Eircode / Postcode',   f['eir_code']),
        ('Mobile Number',        f['mobile']),
        ('Work Permit / Visa',   f['visa_status']),
        ('PPS Number',           f['pps_number']),
    ]))
    story.append(sp(4))

    # ══ SECTION 2 — Identity Verification ═════════════════════════════
    story.append(sec_bar('SECTION 2 \u2014 IDENTITY VERIFICATION'))
    story.append(sp(1.5))
    story.append(Paragraph('ID Proof submitted:', S['lbl']))
    story.append(sp(1))
    story.append(checkbox_row([
        ('Passport',         flags.get('passport')),
        ('Birth Certificate', flags.get('birth_cert')),
        ('Driving Licence',  flags.get('driving_licence')),
        ('Proof of Address', flags.get('proof_of_address')),
    ]))
    story.append(sp(4))

    # ══ SECTION 3 — Qualification and Experience ══════════════════════
    story.append(sec_bar('SECTION 3 \u2014 QUALIFICATION AND EXPERIENCE'))
    story.append(sp(1.5))
    story.append(checkbox_row([
        ('Nurse (NMBI)', f['is_nurse']),
        ('HCA (QQI L5)', f['is_hca']),
    ]))
    story.append(sp(1.5))
    exp_pairs = [('Total Experience', f['total_exp'])]
    if f['nmbi_pin']:
        exp_pairs.append(('NMBI PIN', f['nmbi_pin']))
    story.append(field_rows(exp_pairs))
    story.append(sp(4))

    # ══ REFERENCES ════════════════════════════════════════════════════
    story.append(sec_bar('REFERENCES'))
    story.append(sp(1.5))
    for i, ref in enumerate(f['references'], start=1):
        block = [
            Paragraph(f'Reference {i}', S['refhead']),
            sp(1),
            field_rows([
                ('Name',         ref['name']),
                ('Position',     ref['position']),
                ('Organisation', ref['organisation']),
                ('Telephone',    ref['telephone']),
                ('Email',        ref['email']),
            ]),
            sp(2.5),
        ]
        story.append(KeepTogether(block))

    # ══ VACCINATION ═══════════════════════════════════════════════════
    story.append(sec_bar('OCCUPATIONAL HEALTH'))
    story.append(sp(1.5))
    vac_pairs = [('Vaccinated (Yes / No)', f['vaccinated'])]
    if f['vaccine_detail']:
        vac_pairs.append(('Vaccines Recorded', f['vaccine_detail']))
    story.append(field_rows(vac_pairs))
    story.append(sp(4))

    # ══ DECLARATION ═══════════════════════════════════════════════════
    story.append(sec_bar('DECLARATION'))
    story.append(sp(1.5))
    decl = Table([[Paragraph(
        'I declare that the information provided in this application form is true and '
        'accurate to the best of my knowledge. I understand that any false or misleading '
        'information may result in the withdrawal of an offer of employment or '
        'termination of employment.', S['body'])]], colWidths=[PAGE_W])
    decl.setStyle(TableStyle([
        ('BACKGROUND',    (0, 0), (-1, -1), LIGHT_BG),
        ('BOX',           (0, 0), (-1, -1), 0.5, MID_GRAY),
        ('LINEBEFORE',    (0, 0), (0, -1), 3, XH_GREEN),
        ('TOPPADDING',    (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('LEFTPADDING',   (0, 0), (-1, -1), 10),
        ('RIGHTPADDING',  (0, 0), (-1, -1), 10),
    ]))
    story.append(decl)
    story.append(sp(4))

    # Signature — image when available, otherwise a rule to sign on
    sig_flowable = None
    if signature_bytes:
        try:
            sig_flowable = RLImage(io.BytesIO(signature_bytes),
                                   width=40 * mm, height=15 * mm)
        except Exception:
            sig_flowable = None
    if sig_flowable is None:
        sig_flowable = Paragraph('', S['val'])

    sig_tbl = Table(
        [[Paragraph('Applicant Signature', S['lbl']), sig_flowable],
         [Paragraph('Name', S['lbl']),  Paragraph(f['full_name'] or '\u2014', S['val'])],
         [Paragraph('Date', S['lbl']),  Paragraph(datetime.utcnow().strftime('%d %B %Y'),
                                                  S['val'])]],
        colWidths=[48 * mm, PAGE_W - 48 * mm])
    sig_tbl.setStyle(TableStyle([
        ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING',    (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('LEFTPADDING',   (0, 0), (-1, -1), 6),
        ('LINEBELOW',     (0, 0), (-1, -1), 0.3, MID_GRAY),
    ]))
    story.append(KeepTogether(sig_tbl))

    # ── Footer on every page ──────────────────────────────────────────
    def _footer(canvas, doc_):
        canvas.saveState()
        p = Paragraph(
            f"Xpress Health \u2014 Application Form \u00b7 "
            f"{f.get('full_name') or ''} \u00b7 "
            f"Generated {datetime.utcnow().strftime('%d %b %Y')} \u00b7 "
            f"Page {doc_.page}", S['foot'])
        p.wrapOn(canvas, PAGE_W, 10 * mm)
        p.drawOn(canvas, 15 * mm, 8 * mm)
        canvas.restoreState()

    pdf = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=15 * mm, rightMargin=15 * mm,
        topMargin=12 * mm,  bottomMargin=16 * mm,
        title=f"Application Form - {f.get('full_name') or 'Staff'}",
        author='Xpress Health',
    )
    pdf.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return buf.getvalue()


# ── HSE upload ────────────────────────────────────────────────────────

MAX_UPLOAD_BYTES = 5120 * 1024   # HSE API hard limit: 5120 KB


def _upload_to_hse(xn_staff_id, pdf_bytes, filename='application_form.pdf'):
    """
    POST the PDF to the HSE Document Upload API as application_form.
    Returns (ok: bool, note: str, response_json: dict).
    """
    import requests as _req

    base_url    = os.environ.get('XN_PORTAL_BASE_URL', '').rstrip('/')
    api_key     = os.environ.get('XN_PORTAL_API_KEY', '')
    app_country = os.environ.get('XN_APP_COUNTRY', 'ie')

    if not base_url:
        return False, 'DOC_BASE_URL not set in environment', {}
    if not xn_staff_id:
        return False, 'No XN Portal staff ID', {}
    if len(pdf_bytes) > MAX_UPLOAD_BYTES:
        return False, f'PDF too large ({len(pdf_bytes)} bytes > 5 MB limit)', {}

    endpoint = f"{base_url}/api/admin/staff/hse-document-upload"

    try:
        resp = _req.post(
            endpoint,
            data={
                "staff_id":          xn_staff_id,
                "hse_document_type": "application_form",
            },
            files={"file": (filename, pdf_bytes, "application/pdf")},
            headers={
                "Api-Key":       api_key,
                "X-App-Country": app_country,
                "Accept":        "application/json",
            },
            timeout=60,
        )
    except Exception as e:
        return False, f'upload error: {e}', {}

    try:
        resp_json = resp.json()
    except Exception:
        resp_json = {"raw": resp.text[:500]}

    if not resp.ok:
        return False, f'HSE API {resp.status_code}: {resp_json}', resp_json
    if not resp_json.get('success'):
        return False, f"HSE API error: {resp_json.get('message') or resp_json}", resp_json

    return True, 'uploaded successfully', resp_json


def _pick_xn_staff_id(staff, override=None):
    """
    Resolve the staff_id the HSE API expects, most-trusted source first.
    NOTE: confirm with the XN Portal team whether the HSE endpoint keys off
    the staff record ID or the user ID — swap the order here if needed.
    """
    for candidate in (override,
                      staff.get('xn_staff_id'),
                      staff.get('staff_id')):
        if _v(candidate):
            return _v(candidate)

    try:
        resolved = _resolve_xn_staff_id(str(staff.get('_id')),
                                        _v(staff.get('email')))
        if resolved:
            return _v(resolved)
    except Exception:
        pass

    return _v(staff.get('xn_user_id'))


# ── Core pipeline ─────────────────────────────────────────────────────

def _generate_and_upload(staff, xn_staff_id_override=None, force=False):
    """
    Full pipeline for ONE staff member:
        map fields → fetch XN docs → build PDF → store on GCS → upload to HSE
    Records progress in live_staff_application_forms and mirrors flags onto
    the staff document. Returns a result dict (never raises).
    """
    col        = _staffs_col()
    af_col     = _af_col()
    staff_id   = str(staff['_id'])
    f          = _staff_fields(staff)
    full_name  = f['full_name']
    email      = f['email']
    safe_name  = (full_name or 'staff').replace(' ', '_').replace('/', '_')
    filename   = f"ApplicationForm_{safe_name}.pdf"
    gcs_blob   = f"application_form/{staff_id}_{filename}"

    def _record(fields):
        fields['staff_id']   = staff_id
        fields['staff_name'] = full_name
        fields['email']      = email
        fields['updated_at'] = datetime.utcnow()
        af_col.update_one({"staff_id": staff_id}, {"$set": fields}, upsert=True)

    def _fail(note, **extra):
        _record({"status": "error", "note": note})
        col.update_one({"_id": staff['_id']}, {"$set": {
            "application_form_uploaded":    False,
            "application_form_upload_note": note,
            "application_form_checked_at":  datetime.utcnow(),
        }})
        out = {"success": False, "staff_id": staff_id, "staff_name": full_name,
               "email": email, "error": note}
        out.update(extra)
        return out

    existing = af_col.find_one({"staff_id": staff_id}) or {}
    if existing.get('status') == 'uploaded' and not force:
        return {"success": True, "skipped": True, "staff_id": staff_id,
                "staff_name": full_name, "email": email,
                "message": "Application form already uploaded — pass force=1 to redo."}

    _record({"status": "processing", "created_at": existing.get('created_at')
             or datetime.utcnow()})

    # ── XN Portal documents (identity ticks + signature) ──────────────
    try:
        docs, dl_hdrs = _fetch_xn_documents(email)
    except Exception:
        docs, dl_hdrs = [], {}
    flags = _id_proof_flags(docs, f.get('id_docs_nested'))
    sig   = _fetch_signature_bytes(staff, docs, dl_hdrs)

    # ── Build the PDF ─────────────────────────────────────────────────
    try:
        pdf_bytes = _build_application_form_pdf(staff, flags, sig)
        if not pdf_bytes:
            raise ValueError('PDF builder returned empty bytes')
    except Exception as e:
        return _fail(f'PDF build failed: {e}')

    # ── Store on GCS (best effort — upload still proceeds) ────────────
    stored = False
    try:
        _gcs_upload(gcs_blob, pdf_bytes, content_type='application/pdf')
        stored = True
    except Exception as e:
        _record({"gcs_error": str(e)})

    _record({
        "status":      "generated",
        "filename":    filename,
        "gcs_blob":    gcs_blob if stored else '',
        "id_flags":    flags,
        "has_signature": bool(sig),
        "pdf_bytes":   len(pdf_bytes),
        "generated_at": datetime.utcnow(),
    })
    col.update_one({"_id": staff['_id']}, {"$set": {
        "application_form_generated":    True,
        "application_form_gcs_blob":     gcs_blob if stored else '',
        "application_form_generated_at": datetime.utcnow(),
    }})

    # ── Upload to the XN Portal HSE endpoint ──────────────────────────
    xn_staff_id = _pick_xn_staff_id(staff, xn_staff_id_override)
    if not xn_staff_id:
        return _fail('No XN Portal staff ID resolved — nothing uploaded',
                     gcs_blob=gcs_blob if stored else '')

    ok, note, resp_json = _upload_to_hse(xn_staff_id, pdf_bytes, filename)
    if not ok:
        return _fail(note, gcs_blob=gcs_blob if stored else '',
                     xn_staff_id=xn_staff_id)

    _record({
        "status":        "uploaded",
        "xn_staff_id":   xn_staff_id,
        "note":          note,
        "hse_response":  resp_json.get('data') or {},
        "uploaded_at":   datetime.utcnow(),
    })
    col.update_one({"_id": staff['_id']}, {"$set": {
        "application_form_uploaded":    True,
        "application_form_upload_note": note,
        "application_form_uploaded_at": datetime.utcnow(),
    }})

    return {
        "success":     True,
        "staff_id":    staff_id,
        "staff_name":  full_name,
        "email":       email,
        "filename":    filename,
        "gcs_blob":    gcs_blob if stored else '',
        "xn_staff_id": xn_staff_id,
        "hse_response": resp_json.get('data') or {},
        "message":     f"Application form generated and uploaded for {full_name}.",
    }


def _find_staff(staff_id='', email='', xn_user_id=''):
    """Locate a staff/user document by Mongo _id, email, or XN user id."""
    col = _staffs_col()

    if staff_id and len(_v(staff_id)) == 24:
        try:
            doc = col.find_one({"_id": ObjectId(_v(staff_id))})
            if doc:
                return doc
        except Exception:
            pass

    for key in ('xn_user_id', 'xn_staff_id'):
        for val in (xn_user_id, staff_id):
            if _v(val):
                doc = col.find_one({key: _v(val)})
                if doc:
                    return doc

    if _v(email):
        # re.escape — '+' and '.' are common in addresses and are regex chars
        import re as _re
        doc = col.find_one({"email": {"$regex": f"^{_re.escape(_v(email))}$",
                                      "$options": "i"}})
        if doc:
            return doc

    return None


# ── Webhook — XN Portal "user converted to staff" ─────────────────────

@admin_bp.route('/live-staffs/webhook/user-to-staff', methods=['POST'])
@admin_bp.route('/live-staffs/application-form/webhook', methods=['POST'])
def live_staff_webhook_user_to_staff():
    """
    Webhook the XN Portal calls when a user becomes a staff member.
    Generates the Application Form PDF and uploads it back to that staff record.

    Auth (any one of):
        X-Webhook-Key: <WEBHOOK_SECRET>   header
        ?webhook_key=<WEBHOOK_SECRET>     query param

    Accepted JSON body keys (all optional, at least one identifier required):
        staff_id | xn_staff_id | user_id | xn_user_id | email
        force    — 1/true to regenerate even if already uploaded
        event    — echoed back for the caller's logs

    Example:
        curl -X POST https://<host>/admin/live-staffs/webhook/user-to-staff \\
             -H 'X-Webhook-Key: <secret>' -H 'Content-Type: application/json' \\
             -d '{"event":"user.converted_to_staff",
                  "staff_id":"67f1944f46b6a392510d7dc6",
                  "email":"allenalex51@gmail.com"}'

    Responds 202 immediately and does the work in a background thread, so the
    portal never waits on PDF generation or the upload round-trip.
    """
    secret = os.environ.get('WEBHOOK_SECRET', '')
    if secret:
        provided = (request.headers.get('X-Webhook-Key', '') or
                    request.args.get('webhook_key', ''))
        if provided != secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    payload = request.get_json(silent=True) or {}
    if not payload and request.form:
        payload = request.form.to_dict()

    # Some portals nest the record under data/staff/user
    inner = payload.get('data') or payload.get('staff') or payload.get('user') or {}
    if isinstance(inner, dict):
        merged = {**inner, **{k: v for k, v in payload.items() if k != 'data'}}
    else:
        merged = payload

    event       = _v(merged.get('event') or merged.get('type'))
    xn_staff_id = _v(merged.get('staff_id') or merged.get('xn_staff_id'))
    xn_user_id  = _v(merged.get('user_id')  or merged.get('xn_user_id'))
    email       = _v(merged.get('email'))
    force       = _v(merged.get('force')).lower() in ('1', 'true', 'yes')

    if not (xn_staff_id or xn_user_id or email):
        return jsonify({
            "success": False,
            "error":   "Provide at least one of staff_id, user_id or email",
        }), 400

    staff = _find_staff(staff_id=xn_staff_id, email=email, xn_user_id=xn_user_id)
    if not staff:
        # Not an error the portal can fix by retrying immediately — log it.
        _af_col().insert_one({
            "staff_id":   xn_staff_id or xn_user_id,
            "email":      email,
            "status":     "skipped",
            "note":       "No matching staff record — sync may not have run yet",
            "event":      event,
            "payload":    merged,
            "created_at": datetime.utcnow(),
        })
        return jsonify({
            "success": False,
            "error":   "No matching staff record found in the staff collection",
            "hint":    "Run the staff sync first, then replay this webhook.",
            "collection": STAFF_COLLECTION,
        }), 404

    app_obj = None
    try:                                  # keep app context inside the thread
        from flask import current_app
        app_obj = current_app._get_current_object()
    except Exception:
        pass

    def _worker():
        if app_obj is not None:
            with app_obj.app_context():
                _generate_and_upload(staff, xn_staff_id_override=xn_staff_id,
                                     force=force)
        else:
            _generate_and_upload(staff, xn_staff_id_override=xn_staff_id,
                                 force=force)

    threading.Thread(target=_worker, daemon=True).start()

    f = _staff_fields(staff)
    return jsonify({
        "success":    True,
        "accepted":   True,
        "event":      event,
        "staff_id":   str(staff['_id']),
        "staff_name": f['full_name'],
        "email":      f['email'],
        "message":    "Application form generation and upload started.",
    }), 202


# ── Manual regenerate (admin UI button) ───────────────────────────────

@admin_bp.route('/live-staffs/application-form/generate/<staff_id>',
                methods=['POST'])
@admin_required
def live_staff_application_form_generate(staff_id):
    """Regenerate and re-upload the Application Form for one staff member.
    Runs synchronously so the admin sees the real result. ?force=1 to redo."""
    staff = _find_staff(staff_id=staff_id)
    if not staff:
        return jsonify({"success": False, "error": "Staff not found"}), 404

    force  = _v(request.args.get('force')).lower() in ('1', 'true', 'yes')
    result = _generate_and_upload(staff, force=force or True)
    return jsonify(result), (200 if result.get('success') else 500)


# ── Download the generated PDF ─────────────────────────────────────────

@admin_bp.route('/live-staffs/application-form/download/<staff_id>')
@admin_required
def live_staff_application_form_download(staff_id):
    """Stream the stored Application Form PDF from GCS."""
    rec = _af_col().find_one({"staff_id": _v(staff_id)})
    blob = _v((rec or {}).get('gcs_blob'))
    if not blob:
        return jsonify({"success": False,
                        "error": "No generated application form for this staff"}), 404
    try:
        data = _gcs_download(blob)
    except Exception as e:
        return jsonify({"success": False, "error": f"GCS download failed: {e}"}), 500

    filename = _v(rec.get('filename')) or 'ApplicationForm.pdf'
    return Response(data, mimetype='application/pdf', headers={
        'Content-Disposition': f'inline; filename="{filename}"',
    })


# ── Status API ────────────────────────────────────────────────────────

@admin_bp.route('/live-staffs/api/application-form-status/<staff_id>')
@admin_required
def live_staff_application_form_status(staff_id):
    rec = _af_col().find_one({"staff_id": _v(staff_id)})
    if not rec:
        return jsonify({"success": True, "exists": False, "status": "not_generated"})
    return jsonify({
        "success":     True,
        "exists":      True,
        "status":      _v(rec.get('status')),
        "note":        _v(rec.get('note')),
        "filename":    _v(rec.get('filename')),
        "gcs_blob":    _v(rec.get('gcs_blob')),
        "xn_staff_id": _v(rec.get('xn_staff_id')),
        "uploaded_at": rec.get('uploaded_at'),
        "download_url": (f"/admin/live-staffs/application-form/download/{staff_id}"
                         if rec.get('gcs_blob') else ''),
    })


# ── Backfill cron — ONE staff per call ────────────────────────────────

@admin_bp.route('/live-staffs/cron/upload-application-form',
                methods=['GET', 'POST'])
def live_staff_cron_upload_application_form():
    """
    Backfill for staff who became staff before the webhook existed.
    Generates + uploads the Application Form for ONE staff member per call.

    Picks up staff where application_form_uploaded is not True and no error
    note has been recorded yet. Protect with ?cron_key=<CRON_SECRET>.
    """
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    col = _staffs_col()

    pending_query = {
        "$or": [
            {"application_form_uploaded": {"$exists": False}},
            {"application_form_uploaded": None},
            {"application_form_uploaded": False},
        ],
        "application_form_upload_note": {"$exists": False},
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All application forms generated and uploaded.",
            "remaining_count": 0,
        })

    result = _generate_and_upload(staff)
    result['remaining_count'] = max(0, remaining_total - 1)
    return jsonify(result)
