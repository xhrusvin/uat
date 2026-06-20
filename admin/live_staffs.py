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
    """Build a filled HSE CV PDF from a live_staffs MongoDB document."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table,
        TableStyle, HRFlowable
    )
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    import io as _io

    # ── Data ─────────────────────────────────────────────────────────
    s1  = doc.get('section_1_personal_details') or {}
    s2  = doc.get('section_2_identity_verification') or {}
    s3  = doc.get('section_3_professional_registration') or {}
    s4  = doc.get('section_4_qualifications') or {}
    s5  = doc.get('section_5_employment_history') or {}
    s7  = doc.get('section_7_references') or {}
    s8  = doc.get('section_8_garda_vetting_police_clearance') or {}
    s9  = doc.get('section_9_occupational_health') or {}
    s10 = doc.get('section_10_mandatory_training') or {}
    s11 = doc.get('section_11_criminal_convictions_declaration') or {}
    s12 = doc.get('section_12_declaration') or {}
    visa = s1.get('work_permit_visa_status') or {}
    docs_sub = s2.get('documents_submitted') or {}

    full_name    = _v(s1.get('full_name'))
    address      = _v(s1.get('address'))
    mobile       = _v(s1.get('mobile_number'))
    email        = _v(doc.get('email'))
    dob          = _v(s1.get('date_of_birth'))
    nationality  = _v(s1.get('nationality'))
    pps          = _v(s1.get('pps_number'))
    emp_code     = _v(doc.get('employee_code'))
    user_type    = _v(doc.get('user_type'))
    visa_type    = _v(visa.get('visa_type'))
    perm_work    = _v(visa.get('permission_to_work'))
    reg_exp      = _v(s3.get('registration_expiry_date'))
    reg_pin      = _v(s3.get('registration_number_pin'))
    divisions    = ', '.join(s3.get('divisions_registered_in') or [])
    passport_exp = _v(s2.get('expiry_date'))
    verify_date  = _v(s2.get('verification_date'))
    driving      = _v(s2.get('driving_licence_number'))
    garda        = 'Yes' if s8.get('garda_vetting_submitted') else 'No'
    police       = 'Yes' if s8.get('police_clearance_submitted') else 'No'
    fit          = 'Yes' if s9.get('fit_for_nursing_duties') else 'No'
    decl_date    = _v(s12.get('date'))
    total_exp    = _v(s5.get('total_experience'))

    # ── Palette ───────────────────────────────────────────────────────
    HSE_GREEN  = colors.HexColor('#007A33')
    DARK       = colors.HexColor('#1A1A1A')
    LIGHT_GRAY = colors.HexColor('#F5F5F5')
    MID_GRAY   = colors.HexColor('#CCCCCC')
    TEXT_GRAY  = colors.HexColor('#444444')

    W, H = A4
    PAGE_W = W - 30 * mm

    # ── Paragraph styles ──────────────────────────────────────────────
    def ps(name, **kw):
        defaults = dict(fontName='Helvetica', fontSize=9,
                        textColor=TEXT_GRAY, spaceAfter=2, leading=13)
        defaults.update(kw)
        return ParagraphStyle(name, **defaults)

    S_title   = ps('title',   fontName='Helvetica-Bold', fontSize=20,
                   textColor=colors.white, alignment=TA_CENTER, spaceAfter=0)
    S_sub     = ps('sub',     fontSize=8, textColor=colors.HexColor('#CCFFCC'),
                   alignment=TA_CENTER)
    S_shd     = ps('shd',     fontName='Helvetica-Bold', fontSize=9,
                   textColor=colors.white, alignment=TA_LEFT)
    S_lbl     = ps('lbl',     fontName='Helvetica-Bold', textColor=DARK)
    S_val     = ps('val',     textColor=TEXT_GRAY)
    S_th      = ps('th',      fontName='Helvetica-Bold', fontSize=8,
                   textColor=colors.white, alignment=TA_CENTER)
    S_td      = ps('td',      fontSize=8, textColor=DARK, alignment=TA_LEFT)
    S_body    = ps('body',    leading=14)
    S_footer  = ps('footer',  fontSize=7, textColor=MID_GRAY, alignment=TA_CENTER)
    S_badge   = ps('badge',   fontName='Helvetica-Bold', fontSize=8,
                   textColor=HSE_GREEN)

    def sec(title):
        t = Table([[Paragraph(title, S_shd)]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',   (0,0),(-1,-1), HSE_GREEN),
            ('TOPPADDING',   (0,0),(-1,-1), 5),
            ('BOTTOMPADDING',(0,0),(-1,-1), 5),
            ('LEFTPADDING',  (0,0),(-1,-1), 8),
        ]))
        return t

    def lv(label, value, lw=55*mm, highlight=False):
        bg = colors.HexColor('#F0FFF4') if highlight and value else colors.white
        val_text = value if value else '—'
        t = Table(
            [[Paragraph(label, S_lbl), Paragraph(val_text, S_val)]],
            colWidths=[lw, PAGE_W - lw]
        )
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0),(-1,-1), bg),
            ('TOPPADDING',    (0,0),(-1,-1), 3),
            ('BOTTOMPADDING', (0,0),(-1,-1), 3),
            ('LEFTPADDING',   (1,0),(1,0),   4),
            ('LINEBELOW',     (0,0),(-1,-1), 0.3, MID_GRAY),
        ]))
        return t

    def grid_table(headers, rows_data, col_widths):
        data = [[Paragraph(h, S_th) for h in headers]]
        for row in rows_data:
            data.append([Paragraph(_v(c), S_td) for c in row])
        if not rows_data:
            data.append([Paragraph('—', S_td)] * len(headers))
        t = Table(data, colWidths=col_widths)
        t.setStyle(TableStyle([
            ('BACKGROUND',     (0,0), (-1,0),  HSE_GREEN),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, LIGHT_GRAY]),
            ('GRID',           (0,0), (-1,-1), 0.4, MID_GRAY),
            ('TOPPADDING',     (0,0), (-1,-1), 5),
            ('BOTTOMPADDING',  (0,0), (-1,-1), 5),
            ('LEFTPADDING',    (0,0), (-1,-1), 6),
            ('RIGHTPADDING',   (0,0), (-1,-1), 6),
            ('VALIGN',         (0,0), (-1,-1), 'MIDDLE'),
        ]))
        return t

    sp = lambda n=3: Spacer(1, n * mm)

    # ── Build story ───────────────────────────────────────────────────
    buf   = _io.BytesIO()
    story = []

    # Header banner
    hdr = Table([
        [Paragraph('CURRICULUM VITAE', S_title)],
        [Paragraph(f'{user_type}  •  {emp_code}' if emp_code else user_type or 'Xpress Health', S_sub)],
    ], colWidths=[PAGE_W])
    hdr.setStyle(TableStyle([
        ('BACKGROUND',   (0,0),(-1,-1), HSE_GREEN),
        ('TOPPADDING',   (0,0),(-1,-1), 12),
        ('BOTTOMPADDING',(0,0),(-1,-1), 12),
    ]))
    story += [hdr, sp(5)]

    # ── 1. Personal Details ───────────────────────────────────────────
    story += [sec('PERSONAL DETAILS'), sp(3)]
    personal = [
        ('Full Name:',        full_name,   True),
        ('Address:',          address,     False),
        ('Mobile Number:',    mobile,      False),
        ('Email Address:',    email,       False),
        ('Date of Birth:',    dob,         False),
        ('Nationality:',      nationality, False),
        ('PPS Number:',       pps,         False),
        ('Permission to Work:', perm_work, False),
        ('Visa / Stamp Type:',  visa_type, False),
    ]
    for lbl, val, hi in personal:
        story += [lv(lbl, val, highlight=hi), sp(1)]
    story.append(sp(3))

    # ── 2. Professional Registration ──────────────────────────────────
    story += [sec('PROFESSIONAL REGISTRATION'), sp(3)]
    story += [
        lv('Registration PIN:',    reg_pin,    lw=60*mm),  sp(1),
        lv('Divisions:',           divisions,  lw=60*mm),  sp(1),
        lv('Registration Expiry:', reg_exp,    lw=60*mm),  sp(1),
        lv('NMBI Active:',
           'Yes' if s3.get('nmbi_active_declaration') else 'No',
           lw=60*mm),
        sp(4),
    ]

    # ── 3. Education & Qualifications ─────────────────────────────────
    story += [sec('EDUCATION & QUALIFICATIONS'), sp(3)]
    nd  = s4.get('nursing_degree') or {}
    pg  = s4.get('postgraduate_qualification') or {}
    oth = s4.get('other_qualification') or {}
    qual_rows = []
    for q in [nd, pg, oth]:
        if q.get('qualification') or q.get('institution'):
            qual_rows.append([
                _v(q.get('qualification')),
                _v(q.get('institution')),
                _v(q.get('year_completed')),
            ])
    story += [
        grid_table(
            ['Qualification', 'Institution / College', 'Year'],
            qual_rows,
            [PAGE_W * 0.40, PAGE_W * 0.40, PAGE_W * 0.20]
        ),
        sp(4),
    ]

    # ── 4. Professional Experience ────────────────────────────────────
    story += [sec('PROFESSIONAL EXPERIENCE'), sp(3)]
    entries = s5.get('entries') or []
    if not entries:
        entries = [{}]
    for i, e in enumerate(entries):
        employer = _v(e.get('employer'))
        position = _v(e.get('position'))
        date_from = _v(e.get('from'))
        date_to   = _v(e.get('to'))
        dates     = f"{date_from} – {date_to}" if (date_from or date_to) else ''
        leaving   = _v(e.get('reason_for_leaving'))
        if employer or position:
            story += [
                lv('Job Title:',  position, lw=40*mm), sp(1),
                lv('Employer:',   employer, lw=40*mm), sp(1),
                lv('Dates:',      dates,    lw=40*mm), sp(1),
            ]
            if leaving:
                story += [lv('Reason for Leaving:', leaving, lw=50*mm), sp(1)]
            story.append(sp(3))
            if i < len(entries) - 1:
                story.append(HRFlowable(width=PAGE_W, color=MID_GRAY, thickness=0.4))
                story.append(sp(2))

    story += [
        lv('Total Experience:', total_exp, lw=55*mm),
        sp(4),
    ]

    # ── 5. Training & Certifications ──────────────────────────────────
    story += [sec('TRAINING & CERTIFICATIONS'), sp(3)]
    TRAINING_LABELS = {
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
    training_rows = []
    for key, label in TRAINING_LABELS.items():
        raw = _v(s10.get(key))
        if not raw:
            status, expiry = '—', '—'
        else:
            parts  = [p.strip() for p in raw.split(';')]
            status = parts[0] if parts else '—'
            expiry = next((p.replace('Expiry:', '').strip()
                           for p in parts if 'Expiry' in p), '—')
            appr   = next((p.strip() for p in parts
                           if p.strip() in ('Approved', 'Expired')), '')
            if appr:
                status = appr
        training_rows.append([label, status, expiry])

    story += [
        grid_table(
            ['Training / Certification', 'Status', 'Expiry Date'],
            training_rows,
            [PAGE_W * 0.50, PAGE_W * 0.22, PAGE_W * 0.28]
        ),
        sp(4),
    ]

    # ── 6. Identity & Vetting ─────────────────────────────────────────
    story += [sec('IDENTITY VERIFICATION & VETTING'), sp(3)]
    doc_list = []
    if docs_sub.get('passport'):          doc_list.append('Passport')
    if docs_sub.get('birth_certificate'): doc_list.append('Birth Certificate')
    if docs_sub.get('driving_licence'):   doc_list.append('Driving Licence')
    if docs_sub.get('proof_of_address'):  doc_list.append('Proof of Address')

    story += [
        lv('Documents Submitted:', ', '.join(doc_list) or '—', lw=60*mm), sp(1),
        lv('Passport Expiry:',     passport_exp, lw=60*mm),               sp(1),
        lv('Driving Licence:',     driving,      lw=60*mm),               sp(1),
        lv('Verification Date:',   verify_date,  lw=60*mm),               sp(1),
        lv('Garda Vetting:',       garda,        lw=60*mm),               sp(1),
        lv('Police Clearance:',    police,       lw=60*mm),
        sp(4),
    ]

    # ── 7. Occupational Health ────────────────────────────────────────
    story += [sec('OCCUPATIONAL HEALTH'), sp(3)]
    vacc_rows = [
        ['COVID-19 Vaccine',  _v(s9.get('covid_19_vaccine'))],
        ['Tuberculosis',      _v(s9.get('tuberculosis_vaccine'))],
        ['Hepatitis Antibody',_v(s9.get('hepatitis_antibody'))],
        ['MMR Vaccine',       _v(s9.get('mmr_vaccine'))],
    ]
    story += [
        lv('Health Screening:',       'Yes' if s9.get('occupational_health_screening') else 'No', lw=60*mm), sp(1),
        lv('Immunisation Records:',   'Yes' if s9.get('immunisation_records_provided') else 'No', lw=60*mm), sp(1),
        lv('Fit for Nursing Duties:', fit, lw=60*mm), sp(2),
        grid_table(
            ['Vaccination', 'Status'],
            vacc_rows,
            [PAGE_W * 0.60, PAGE_W * 0.40]
        ),
        sp(4),
    ]

    # ── 8. References ─────────────────────────────────────────────────
    story += [sec('REFERENCES'), sp(3)]
    ref_rows = []
    for rk in ['reference_1', 'reference_2', 'reference_3']:
        r = s7.get(rk) or {}
        if r.get('name'):
            ref_rows.append([
                _v(r.get('name')),
                _v(r.get('position')),
                _v(r.get('organisation')),
                _v(r.get('telephone')),
                _v(r.get('email')),
            ])
    story += [
        grid_table(
            ['Name', 'Position', 'Organisation', 'Telephone', 'Email'],
            ref_rows,
            [PAGE_W*0.20, PAGE_W*0.18, PAGE_W*0.22, PAGE_W*0.18, PAGE_W*0.22]
        ),
        sp(4),
    ]

    # ── 9. Declaration ────────────────────────────────────────────────
    story += [sec('DECLARATION'), sp(3)]
    story += [
        lv('Declaration Agreed:', _v(s12.get('declaration_agreed')), lw=55*mm), sp(1),
        lv('Date:',               decl_date, lw=55*mm),
        sp(6),
    ]
    sig_t = Table(
        [[Paragraph('Signature: _______________________________', S_val),
          Paragraph('Date: _____________________', S_val)]],
        colWidths=[PAGE_W * 0.55, PAGE_W * 0.45]
    )
    story += [sig_t, sp(6)]

    # Footer
    story.append(HRFlowable(width=PAGE_W, color=MID_GRAY, thickness=0.5))
    story.append(sp(2))
    story.append(Paragraph(
        f'Generated by Xpress Health Admin  •  {full_name}  •  {emp_code}',
        S_footer
    ))

    # Build
    pdf_doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=10*mm, bottomMargin=12*mm,
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
