from flask import render_template, request, jsonify, Response
from bson import ObjectId
from datetime import datetime
import json
import csv
import io
import re

from database import db
from . import admin_bp
from admin.views import admin_required


# ── Helpers ──────────────────────────────────────────────────────────

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
    query = {}
    if search:
        pattern = re.compile(re.escape(search), re.IGNORECASE)
        query = {"$or": [
            {"section_1_personal_details.full_name": pattern},
            {"email": pattern},
            {"employee_code": pattern},
            {"section_1_personal_details.nationality": pattern},
            {"user_type": pattern},
        ]}
    col = _staffs_col()
    total = col.count_documents(query)
    items = list(
        col.find(query)
           .sort([("section_1_personal_details.full_name", 1)])
           .skip((page - 1) * per_page)
           .limit(per_page)
    )
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



def _build_cv_pdf(doc):
    """Build a branded Xpress Health CV PDF from a live_staffs MongoDB document."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table,
        TableStyle, HRFlowable, Image as RLImage
    )
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    import io as _io
    import os

    # ── Brand palette ──────────────────────────────────────────────────
    NAVY      = colors.HexColor('#1B3A6B')
    XH_GREEN  = colors.HexColor('#2E9E44')
    LIGHT_BG  = colors.HexColor('#EFF6FF')
    STRIPE    = colors.HexColor('#F0FDF4')
    MID_GRAY  = colors.HexColor('#CBD5E1')
    TEXT_DARK = colors.HexColor('#1E293B')
    TEXT_GRAY = colors.HexColor('#475569')
    WHITE     = colors.white

    W, H   = A4
    PAGE_W = W - 30 * mm

    # ── Styles ─────────────────────────────────────────────────────────
    def ps(name, **kw):
        d = dict(fontName='Helvetica', fontSize=9,
                 textColor=TEXT_GRAY, spaceAfter=2, leading=13)
        d.update(kw)
        return ParagraphStyle(name, **d)

    S = {
        'cv_title': ps('cv_title', fontName='Helvetica-Bold', fontSize=18,
                       textColor=WHITE, alignment=TA_CENTER, spaceAfter=0),
        'cv_sub':   ps('cv_sub',   fontSize=9,
                       textColor=colors.HexColor('#BFD9FF'),
                       alignment=TA_CENTER, spaceAfter=0),
        'sec_head': ps('sec_head', fontName='Helvetica-Bold', fontSize=9,
                       textColor=WHITE, spaceAfter=0),
        'lbl':      ps('lbl',      fontName='Helvetica-Bold', fontSize=9,
                       textColor=NAVY, spaceAfter=0),
        'val':      ps('val',      fontSize=9, textColor=TEXT_GRAY, spaceAfter=0),
        'th':       ps('th',       fontName='Helvetica-Bold', fontSize=8,
                       textColor=WHITE, alignment=TA_CENTER, spaceAfter=0),
        'td':       ps('td',       fontSize=8, textColor=TEXT_DARK, spaceAfter=0),
        'td_green': ps('td_green', fontName='Helvetica-Bold', fontSize=8,
                       textColor=XH_GREEN, spaceAfter=0),
        'td_red':   ps('td_red',   fontName='Helvetica-Bold', fontSize=8,
                       textColor=colors.HexColor('#DC2626'), spaceAfter=0),
        'footer':   ps('footer',   fontSize=7, textColor=MID_GRAY,
                       alignment=TA_CENTER, spaceAfter=0),
    }

    sp = lambda n=3: Spacer(1, n * mm)

    def sec(title):
        t = Table([[Paragraph(title, S['sec_head'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), NAVY),
            ('TOPPADDING',    (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
            ('LEFTPADDING',   (0,0), (-1,-1), 10),
            ('RIGHTPADDING',  (0,0), (-1,-1), 6),
        ]))
        return t

    def lv(label, value, lw=58*mm, highlight=False):
        bg = STRIPE if (highlight and value) else WHITE
        val_text = value if value else '—'
        t = Table(
            [[Paragraph(label, S['lbl']), Paragraph(val_text, S['val'])]],
            colWidths=[lw, PAGE_W - lw]
        )
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), bg),
            ('TOPPADDING',    (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ('LEFTPADDING',   (0,0), (0,0),   8),
            ('LEFTPADDING',   (1,0), (1,0),   4),
            ('LINEBELOW',     (0,0), (-1,-1), 0.3, MID_GRAY),
            ('LINEBEFORE',    (0,0), (0,-1),  3,   XH_GREEN),
        ]))
        return t

    def grid_tbl(headers, rows, col_widths, status_col=None):
        data = [[Paragraph(h, S['th']) for h in headers]]
        for row in rows:
            cells = []
            for ci, c in enumerate(row):
                v = str(c) if c else '—'
                if status_col is not None and ci == status_col:
                    sty = (S['td_green'] if v == 'Approved' else
                           S['td_red']   if v == 'Expired'  else S['td'])
                else:
                    sty = S['td']
                cells.append(Paragraph(v, sty))
            data.append(cells)
        if not rows:
            data.append([Paragraph('—', S['td'])] * len(headers))
        t = Table(data, colWidths=col_widths)
        t.setStyle(TableStyle([
            ('BACKGROUND',     (0,0), (-1,0),  NAVY),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [WHITE, LIGHT_BG]),
            ('GRID',           (0,0), (-1,-1), 0.35, MID_GRAY),
            ('TOPPADDING',     (0,0), (-1,-1), 5),
            ('BOTTOMPADDING',  (0,0), (-1,-1), 5),
            ('LEFTPADDING',    (0,0), (-1,-1), 6),
            ('RIGHTPADDING',   (0,0), (-1,-1), 6),
            ('VALIGN',         (0,0), (-1,-1), 'MIDDLE'),
            ('LINEBELOW',      (0,0), (-1,0),  1.5, XH_GREEN),
        ]))
        return t

    # ── Data extraction ────────────────────────────────────────────────
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

    full_name = _v(s1.get('full_name'))
    emp_code  = _v(doc.get('employee_code'))
    user_type = _v(doc.get('user_type'))

    doc_list = [
        k for k, label in [
            ('passport',          'Passport'),
            ('birth_certificate', 'Birth Certificate'),
            ('driving_licence',   'Driving Licence'),
            ('proof_of_address',  'Proof of Address'),
        ] if docs_sub.get(k)
    ]

    # ── Logo path ──────────────────────────────────────────────────────
    # Try several common locations
    logo_path = None
    for candidate in [
        os.path.join(os.path.dirname(__file__), '..', 'static', 'images', 'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'img', 'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'logo.png'),
        'static/images/logo.png',
        'static/img/logo.png',
        'static/logo.png',
    ]:
        if os.path.exists(candidate):
            logo_path = candidate
            break

    # ── Build story ────────────────────────────────────────────────────
    buf   = _io.BytesIO()
    story = []

    # Header banner
    title_rows = [
        [Paragraph('CURRICULUM VITAE', S['cv_title'])],
        [Paragraph(
            f'{user_type}  •  {emp_code}' if emp_code else (user_type or 'Xpress Health'),
            S['cv_sub']
        )],
    ]
    title_tbl = Table(title_rows, colWidths=[PAGE_W - (55*mm if logo_path else 0)])
    title_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), NAVY),
        ('TOPPADDING',    (0,0), (-1,-1), 10),
        ('BOTTOMPADDING', (0,0), (-1,-1), 10),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
    ]))

    if logo_path:
        logo_img   = RLImage(logo_path, width=45*mm, height=45*mm * 94/316)
        logo_cell  = Table([[logo_img]], colWidths=[55*mm])
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
        ('LINEBELOW',     (0,0), (-1,-1), 2.5, XH_GREEN),
    ]))
    story += [banner, sp(5)]

    # 1. Personal Details
    story += [sec('PERSONAL DETAILS'), sp(3)]
    for lbl, val, hi in [
        ('Full Name:',          full_name,                                True),
        ('Date of Birth:',      _v(s1.get('date_of_birth')),              False),
        ('Address:',            _v(s1.get('address')),                    False),
        ('Mobile Number:',      _v(s1.get('mobile_number')),              False),
        ('Email Address:',      _v(doc.get('email')),                     False),
        ('Nationality:',        _v(s1.get('nationality')),                False),
        ('PPS Number:',         _v(s1.get('pps_number')),                 False),
        ('Permission to Work:', _v(visa.get('permission_to_work')),       False),
        ('Visa / Stamp Type:',  _v(visa.get('visa_type')),                False),
        ('Documents Submitted:',', '.join(doc_list) if doc_list else '—', False),
    ]:
        story += [lv(lbl, val, highlight=hi), sp(1)]
    story.append(sp(4))

    # 2. Professional Registration
    story += [sec('PROFESSIONAL REGISTRATION'), sp(3)]
    divisions = ', '.join(s3.get('divisions_registered_in') or [])
    for lbl, val in [
        ('Registration PIN:',          _v(s3.get('registration_number_pin'))),
        ('Divisions:',                 divisions or '—'),
        ('Registration Expiry:',       _v(s3.get('registration_expiry_date'))),
        ('NMBI Active Declaration:',
         'Yes' if s3.get('nmbi_active_declaration') else 'No'),
    ]:
        story += [lv(lbl, val, lw=65*mm), sp(1)]
    story.append(sp(4))

    # 3. Education & Qualifications
    story += [sec('EDUCATION & QUALIFICATIONS'), sp(3)]
    qual_rows = []
    for qk in ['nursing_degree', 'postgraduate_qualification', 'other_qualification']:
        q = s4.get(qk) or {}
        if q.get('qualification') or q.get('institution'):
            qual_rows.append([
                _v(q.get('qualification')),
                _v(q.get('institution')),
                _v(q.get('year_completed')),
            ])
    story += [
        grid_tbl(['Qualification', 'Institution / College', 'Year'],
                 qual_rows,
                 [PAGE_W*0.42, PAGE_W*0.40, PAGE_W*0.18]),
        sp(4),
    ]

    # 4. Professional Experience
    story += [sec('PROFESSIONAL EXPERIENCE'), sp(3)]
    entries = s5.get('entries') or []
    for i, e in enumerate(entries):
        emp = _v(e.get('employer')); pos = _v(e.get('position'))
        if not (emp or pos):
            continue
        d_from  = _v(e.get('from'));  d_to = _v(e.get('to'))
        dates   = f"{d_from} – {d_to}" if (d_from or d_to) else '—'
        leaving = _v(e.get('reason_for_leaving'))
        rows    = [
            [Paragraph('Job Title', S['lbl']), Paragraph(pos or '—', S['val'])],
            [Paragraph('Employer',  S['lbl']), Paragraph(emp or '—', S['val'])],
            [Paragraph('Dates',     S['lbl']), Paragraph(dates,       S['val'])],
        ]
        if leaving:
            rows.append([Paragraph('Reason for Leaving', S['lbl']),
                         Paragraph(leaving, S['val'])])
        et = Table(rows, colWidths=[44*mm, PAGE_W - 44*mm])
        et.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (0,-1), LIGHT_BG),
            ('TOPPADDING',    (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ('LEFTPADDING',   (0,0), (0,-1),  8),
            ('LEFTPADDING',   (1,0), (1,-1),  4),
            ('LINEBELOW',     (0,0), (-1,-1), 0.3, MID_GRAY),
            ('LINEBEFORE',    (0,0), (0,-1),  3,   XH_GREEN),
        ]))
        story += [et, sp(3)]
        if i < len(entries) - 1:
            story += [HRFlowable(width=PAGE_W, color=MID_GRAY, thickness=0.4), sp(2)]

    total_exp = _v(s5.get('total_experience'))
    if total_exp:
        story += [lv('Total Experience:', total_exp, lw=55*mm), sp(1)]
    story.append(sp(3))

    # 5. Training & Certifications
    story += [sec('TRAINING & CERTIFICATIONS'), sp(3)]
    TLABELS = {
        'manual_handling':              'Manual Handling',
        'cpr_bls':                      'CPR / BLS',
        'fire_safety':                  'Fire Safety',
        'infection_prevention_control': 'Infection Prevention & Control',
        'hand_hygiene':                 'Hand Hygiene',
        'safeguarding':                 'Safeguarding',
        'children_first':               'Children First',
        'cyber_security':               'Cyber Security',
        'dignity_at_work':              'Dignity at Work',
        'open_disclosure':              'Open Disclosure',
        'mapa_pmav':                    'MAPA / PMAV',
    }
    trows = []
    for key, label in TLABELS.items():
        raw    = _v(s10.get(key))
        parts  = [p.strip() for p in raw.split(';')] if raw else []
        status = next((p for p in parts if p in ('Approved', 'Expired')),
                      parts[0] if parts else '—')
        expiry = next((p.replace('Expiry:', '').strip()
                       for p in parts if 'Expiry' in p), '—')
        trows.append([label, status, expiry])
    story += [
        grid_tbl(['Training / Certification', 'Status', 'Expiry Date'],
                 trows,
                 [PAGE_W*0.52, PAGE_W*0.22, PAGE_W*0.26],
                 status_col=1),
        sp(4),
    ]

    # 6. Occupational Health
    story += [sec('OCCUPATIONAL HEALTH'), sp(3)]
    for lbl, val in [
        ('Health Screening Completed:',    'Yes' if s9.get('occupational_health_screening')  else 'No'),
        ('Immunisation Records Provided:', 'Yes' if s9.get('immunisation_records_provided')  else 'No'),
        ('Fit for Nursing Duties:',        'Yes' if s9.get('fit_for_nursing_duties')          else 'No'),
    ]:
        story += [lv(lbl, val, lw=72*mm), sp(1)]
    story.append(sp(2))
    story += [
        grid_tbl(['Vaccination', 'Status'],
                 [['COVID-19 Vaccine',   _v(s9.get('covid_19_vaccine'))],
                  ['Tuberculosis (BCG)', _v(s9.get('tuberculosis_vaccine'))],
                  ['Hepatitis Antibody', _v(s9.get('hepatitis_antibody'))],
                  ['MMR Vaccine',        _v(s9.get('mmr_vaccine'))]],
                 [PAGE_W*0.60, PAGE_W*0.40]),
        sp(4),
    ]

    # 7. Identity Verification & Vetting
    story += [sec('IDENTITY VERIFICATION & VETTING'), sp(3)]
    for lbl, val in [
        ('Passport Expiry Date:',       _v(s2.get('expiry_date'))),
        ('Driving Licence:',            _v(s2.get('driving_licence_number'))),
        ('Verification Date:',          _v(s2.get('verification_date'))),
        ('Garda Vetting Submitted:',    'Yes' if s8.get('garda_vetting_submitted')    else 'No'),
        ('Police Clearance Submitted:', 'Yes' if s8.get('police_clearance_submitted') else 'No'),
    ]:
        story += [lv(lbl, val, lw=70*mm), sp(1)]
    story.append(sp(4))

    # 8. References
    story += [sec('REFERENCES'), sp(3)]
    rrows = []
    for rk in ['reference_1', 'reference_2', 'reference_3']:
        r = s7.get(rk) or {}
        if r.get('name'):
            rrows.append([_v(r.get('name')), _v(r.get('position')),
                          _v(r.get('organisation')), _v(r.get('email'))])
    story += [
        grid_tbl(['Name', 'Position', 'Organisation', 'Email'],
                 rrows,
                 [PAGE_W*0.22, PAGE_W*0.18, PAGE_W*0.28, PAGE_W*0.32]),
        sp(4),
    ]

    # 9. Declaration
    story += [sec('DECLARATION'), sp(3)]
    for lbl, val in [
        ('Declaration Agreed:', _v(s12.get('declaration_agreed'))),
        ('Date:',               _v(s12.get('date'))),
    ]:
        story += [lv(lbl, val, lw=55*mm), sp(1)]
    story.append(sp(8))
    sig_t = Table(
        [[Paragraph('Signature: _______________________________', S['val']),
          Paragraph('Date: _____________________',               S['val'])]],
        colWidths=[PAGE_W * 0.55, PAGE_W * 0.45]
    )
    story += [sig_t, sp(6)]

    # Footer
    story.append(HRFlowable(width=PAGE_W, color=XH_GREEN, thickness=1.2))
    story.append(sp(2))
    if logo_path:
        logo_sm = RLImage(logo_path, width=28*mm, height=28*mm * 94/316)
        ft = Table([[logo_sm,
                     Paragraph(
                         f'Xpress Health  •  {full_name}  •  {emp_code}<br/>'
                         '<font size="6">This document is confidential and for authorised use only.</font>',
                         S['footer']
                     )]],
                   colWidths=[32*mm, PAGE_W - 32*mm])
        ft.setStyle(TableStyle([
            ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
            ('LEFTPADDING',   (0,0), (-1,-1), 0),
            ('TOPPADDING',    (0,0), (-1,-1), 2),
            ('BOTTOMPADDING', (0,0), (-1,-1), 2),
        ]))
        story.append(ft)
    else:
        story.append(Paragraph(
            f'Xpress Health  •  {full_name}  •  {emp_code}  •  '
            'This document is confidential and for authorised use only.',
            S['footer']
        ))

    # Render
    pdf_doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=10*mm,  bottomMargin=12*mm,
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
