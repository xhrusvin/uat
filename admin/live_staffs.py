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
    """Build a branded Xpress Health CV PDF — section order matches HSE_CV.docx."""
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

    # ── Brand palette (Xpress Health logo colours) ─────────────────────
    NAVY      = colors.HexColor('#1B3A6B')   # XPRESS dark blue
    XH_GREEN  = colors.HexColor('#2E9E44')   # HEALTH green
    LIGHT_BG  = colors.HexColor('#EFF6FF')   # light blue row stripe
    STRIPE    = colors.HexColor('#F0FDF4')   # light green highlight
    MID_GRAY  = colors.HexColor('#CBD5E1')
    TEXT_DARK = colors.HexColor('#1E293B')
    TEXT_GRAY = colors.HexColor('#475569')
    WHITE     = colors.white

    W, H   = A4
    PAGE_W = W - 30 * mm

    # ── Paragraph styles ───────────────────────────────────────────────
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
        'italic':   ps('italic',   fontName='Helvetica-Oblique', fontSize=8,
                       textColor=TEXT_GRAY, spaceAfter=0),
        'th':       ps('th',       fontName='Helvetica-Bold', fontSize=8,
                       textColor=WHITE, alignment=TA_CENTER, spaceAfter=0),
        'td':       ps('td',       fontSize=8, textColor=TEXT_DARK, spaceAfter=0),
        'td_green': ps('td_green', fontName='Helvetica-Bold', fontSize=8,
                       textColor=XH_GREEN, spaceAfter=0),
        'td_red':   ps('td_red',   fontName='Helvetica-Bold', fontSize=8,
                       textColor=colors.HexColor('#DC2626'), spaceAfter=0),
        'footer':   ps('footer',   fontSize=7, textColor=MID_GRAY,
                       alignment=TA_CENTER, spaceAfter=0),
        'duties_lbl': ps('duties_lbl', fontName='Helvetica-Bold', fontSize=8,
                         textColor=NAVY, spaceAfter=0),
    }

    sp = lambda n=3: Spacer(1, n * mm)

    # ── Helpers ────────────────────────────────────────────────────────
    def sec(title):
        """Navy section header bar with green left accent."""
        t = Table([[Paragraph(title, S['sec_head'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), NAVY),
            ('TOPPADDING',    (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
            ('LEFTPADDING',   (0,0), (-1,-1), 10),
            ('RIGHTPADDING',  (0,0), (-1,-1), 6),
            ('LINEBELOW',     (0,0), (-1,-1), 1.5, XH_GREEN),
        ]))
        return t

    def lv(label, value, lw=58*mm, highlight=False):
        """Label : Value row."""
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
        """Table with navy header and alternating rows."""
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

    def duties_box(text=''):
        """Grey box for duties/profile text."""
        val = text if text else ''
        t = Table([[Paragraph(val, S['val'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), LIGHT_BG),
            ('BOX',           (0,0), (-1,-1), 0.4, MID_GRAY),
            ('LINEBEFORE',    (0,0), (0,-1),  3,   XH_GREEN),
            ('TOPPADDING',    (0,0), (-1,-1), 8),
            ('BOTTOMPADDING', (0,0), (-1,-1), 8),
            ('LEFTPADDING',   (0,0), (-1,-1), 8),
            ('RIGHTPADDING',  (0,0), (-1,-1), 8),
        ]))
        return t

    def para_box(text):
        """Plain flowing paragraph in a light box — no fixed height."""
        t = Table([[Paragraph(text or '', S['val'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), LIGHT_BG),
            ('BOX',           (0,0), (-1,-1), 0.4, MID_GRAY),
            ('LINEBEFORE',    (0,0), (0,-1),  3,   XH_GREEN),
            ('TOPPADDING',    (0,0), (-1,-1), 9),
            ('BOTTOMPADDING', (0,0), (-1,-1), 9),
            ('LEFTPADDING',   (0,0), (-1,-1), 10),
            ('RIGHTPADDING',  (0,0), (-1,-1), 10),
        ]))
        return t

    # Add experience-card styles to S
    S['exp_title'] = ps('exp_title', fontName='Helvetica-Bold', fontSize=10,
                        textColor=NAVY, spaceAfter=0, leading=14)
    S['exp_sub']   = ps('exp_sub',   fontName='Helvetica-Oblique', fontSize=9,
                        textColor=XH_GREEN, spaceAfter=0, leading=13)
    S['exp_date']  = ps('exp_date',  fontName='Helvetica', fontSize=8,
                        textColor=TEXT_GRAY, alignment=1, spaceAfter=0)  # TA_RIGHT=1

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

    # ── Logo path (searches common static locations) ───────────────────
    logo_path = None
    for candidate in [
        os.path.join(os.path.dirname(__file__), '..', 'static', 'images', 'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'img', 'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'logo.png'),
        'static/images/logo.png', 'static/img/logo.png', 'static/logo.png',
    ]:
        if os.path.exists(candidate):
            logo_path = candidate
            break

    # ── Build story ────────────────────────────────────────────────────
    buf   = _io.BytesIO()
    story = []

    # ── HEADER BANNER ─────────────────────────────────────────────────
    title_rows = [
        [Paragraph('CURRICULUM VITAE', S['cv_title'])],
        [Paragraph(
            f'{user_type}  •  {emp_code}' if emp_code else (user_type or 'Xpress Health'),
            S['cv_sub']
        )],
    ]
    title_w = PAGE_W - (55*mm if logo_path else 0)
    title_tbl = Table(title_rows, colWidths=[title_w])
    title_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), NAVY),
        ('TOPPADDING',    (0,0), (-1,-1), 12),
        ('BOTTOMPADDING', (0,0), (-1,-1), 12),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
    ]))

    if logo_path:
        logo_img  = RLImage(logo_path, width=45*mm, height=45*mm * 94/316)
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
        ('LINEBELOW',     (0,0), (-1,-1), 2.5, XH_GREEN),
    ]))
    story += [banner, sp(5)]

    # ═══════════════════════════════════════════════════════════════════
    # 1. PERSONAL DETAILS
    #    Full Name / Address / Mobile / Email / Date of Birth / Nationality
    # ═══════════════════════════════════════════════════════════════════
    story += [sec('PERSONAL DETAILS'), sp(3)]
    for lbl, val, hi in [
        ('Full Name:',      full_name,                          True),
        ('Address:',        _v(s1.get('address')),              False),
        ('Mobile Number:',  _v(s1.get('mobile_number')),        False),
        ('Email Address:',  _v(doc.get('email')),               False),
        ('Date of Birth:',  _v(s1.get('date_of_birth')),        False),
        ('Nationality:',    _v(s1.get('nationality')),          False),
    ]:
        story += [lv(lbl, val, highlight=hi), sp(1)]
    story.append(sp(4))

    # ═══════════════════════════════════════════════════════════════════
    # 2. PROFESSIONAL PROFILE — rich self-description paragraph
    # ═══════════════════════════════════════════════════════════════════
    story += [sec('PROFESSIONAL PROFILE'), sp(3)]

    reg_pin     = _v(s3.get('registration_number_pin'))
    reg_exp     = _v(s3.get('registration_expiry_date'))
    divisions   = ', '.join(s3.get('divisions_registered_in') or [])
    nmbi_active = s3.get('nmbi_active_declaration')
    perm_work   = _v(visa.get('permission_to_work'))
    visa_type_v = _v(visa.get('visa_type'))
    total_exp_p = _v(s5.get('total_experience'))
    entries_p   = [e for e in (s5.get('entries') or []) if e.get('employer') or e.get('position')]
    latest_emp  = entries_p[0] if entries_p else {}

    # Build a flowing self-description CV paragraph
    sentences = []

    # Opening sentence — who they are
    if full_name and user_type:
        opener = f"{full_name} is a dedicated and experienced {user_type}"
        if divisions:
            opener += f" specialising in {divisions}"
        if total_exp_p:
            opener += f", with {total_exp_p} of professional experience"
        sentences.append(opener)

    # Current/recent role
    if latest_emp:
        pos_p = _v(latest_emp.get('position'))
        emp_p = _v(latest_emp.get('employer'))
        if pos_p and emp_p:
            sentences.append(
                f"Most recently working as {pos_p} at {emp_p}"
            )

    # Registration
    if reg_pin or reg_exp:
        reg_str = "Professionally registered"
        if reg_pin:
            reg_str += f" (PIN: {reg_pin})"
        if reg_exp:
            reg_str += f" with registration valid until {reg_exp}"
        if nmbi_active:
            reg_str += " and holds an active NMBI declaration"
        sentences.append(reg_str)

    # Work authorisation
    if perm_work == 'Yes' and visa_type_v:
        sentences.append(
            f"Fully authorised to work in Ireland ({visa_type_v})"
        )
    elif perm_work == 'Yes':
        sentences.append("Fully authorised to work in Ireland")

    profile_text = '. '.join(sentences) + '.' if sentences else         'Experienced healthcare professional committed to delivering high-quality patient care. ' \
        'Holds current professional registration and mandatory training certifications.'

    story.append(para_box(profile_text))
    story.append(sp(4))

    # ═══════════════════════════════════════════════════════════════════
    # 3. EDUCATION & QUALIFICATIONS — description mode
    # ═══════════════════════════════════════════════════════════════════
    story += [sec('EDUCATION & QUALIFICATIONS'), sp(3)]

    qual_entries = []
    for qk in ['nursing_degree', 'postgraduate_qualification', 'other_qualification']:
        q = s4.get(qk) or {}
        if q.get('qualification') or q.get('institution'):
            qual_entries.append(q)

    if qual_entries:
        for q in qual_entries:
            qname = _v(q.get('qualification'))
            qinst = _v(q.get('institution'))
            qyear = _v(q.get('year_completed'))

            # Heading line: bold qualification name + year right-aligned
            heading_left  = Paragraph(f"<b>{qname}</b>" if qname else "<b>Qualification</b>", S['exp_title'])
            heading_right = Paragraph(qyear, S['exp_date'])
            heading_row   = Table([[heading_left, heading_right]],
                                   colWidths=[PAGE_W * 0.75, PAGE_W * 0.25])
            heading_row.setStyle(TableStyle([
                ('TOPPADDING',    (0,0), (-1,-1), 0),
                ('BOTTOMPADDING', (0,0), (-1,-1), 0),
                ('LEFTPADDING',   (0,0), (-1,-1), 0),
                ('RIGHTPADDING',  (0,0), (-1,-1), 0),
                ('VALIGN',        (0,0), (-1,-1), 'BOTTOM'),
            ]))
            story.append(heading_row)
            if qinst:
                story.append(Paragraph(qinst, S['exp_sub']))
            story.append(sp(2))
            story.append(HRFlowable(width=PAGE_W, color=MID_GRAY, thickness=0.3))
            story.append(sp(3))
    else:
        story.append(para_box('No qualifications recorded.'))
        story.append(sp(3))

    story.append(sp(1))

    # ═══════════════════════════════════════════════════════════════════
    # 4. PROFESSIONAL EXPERIENCE — one card per role, description mode
    # ═══════════════════════════════════════════════════════════════════
    story += [sec('PROFESSIONAL EXPERIENCE'), sp(3)]
    entries   = [e for e in (s5.get('entries') or []) if e.get('employer') or e.get('position')]
    total_exp = _v(s5.get('total_experience'))

    if entries:
        for i, e in enumerate(entries):
            emp     = _v(e.get('employer'))
            pos     = _v(e.get('position'))
            d_from  = _v(e.get('from'))
            d_to    = _v(e.get('to'))
            leaving = _v(e.get('reason_for_leaving'))

            # Date range label
            if d_from and d_to:
                date_label = f"{d_from} – {d_to}"
            elif d_from:
                date_label = f"{d_from} – Present"
            elif d_to:
                date_label = f"Until {d_to}"
            else:
                date_label = ''

            # ── Role heading row: job title left, dates right ─────────
            title_p = Paragraph(f"<b>{pos}</b>" if pos else "<b>Role</b>", S['exp_title'])
            date_p  = Paragraph(date_label, S['exp_date'])
            head_t  = Table([[title_p, date_p]],
                            colWidths=[PAGE_W * 0.70, PAGE_W * 0.30])
            head_t.setStyle(TableStyle([
                ('TOPPADDING',    (0,0), (-1,-1), 0),
                ('BOTTOMPADDING', (0,0), (-1,-1), 2),
                ('LEFTPADDING',   (0,0), (-1,-1), 0),
                ('RIGHTPADDING',  (0,0), (-1,-1), 0),
                ('VALIGN',        (0,0), (-1,-1), 'BOTTOM'),
            ]))

            # ── Employer sub-line ─────────────────────────────────────
            emp_p = Paragraph(emp, S['exp_sub']) if emp else None

            # ── Description paragraph ─────────────────────────────────
            desc_parts = []
            if pos and emp:
                desc_parts.append(
                    f"Worked as <b>{pos}</b> at {emp}"
                    + (f" from {d_from} to {d_to}" if (d_from and d_to) else
                       f" from {d_from}" if d_from else "")
                )
            if leaving:
                desc_parts.append(f"Reason for leaving: {leaving}")

            desc_text = '. '.join(desc_parts) + '.' if desc_parts else ''

            # ── Assemble the experience card ──────────────────────────
            story.append(head_t)
            if emp_p:
                story.append(emp_p)
            story.append(sp(2))
            if desc_text:
                story.append(para_box(desc_text))
            story.append(sp(3))

            if i < len(entries) - 1:
                story.append(HRFlowable(width=PAGE_W, color=MID_GRAY, thickness=0.3))
                story.append(sp(3))
    else:
        story.append(para_box('No employment history recorded.'))
        story.append(sp(3))

    if total_exp:
        story += [lv('Total Experience:', total_exp, lw=55*mm), sp(1)]
    story.append(sp(3))

    # ═══════════════════════════════════════════════════════════════════
    # 5. TRAINING & CERTIFICATIONS — bullet list, max 6 items
    # ═══════════════════════════════════════════════════════════════════
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
    # Collect only certs that exist in the record, cap at 6
    cert_labels = []
    for key, label in TLABELS.items():
        if s10.get(key):
            cert_labels.append(label)
        if len(cert_labels) == 6:
            break

    if cert_labels:
        bullet_rows = [[Paragraph(f'•  {label}', S['val'])] for label in cert_labels]
        cert_tbl = Table(bullet_rows, colWidths=[PAGE_W])
        cert_tbl.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), LIGHT_BG),
            ('BOX',           (0,0), (-1,-1), 0.4, MID_GRAY),
            ('LINEBEFORE',    (0,0), (0,-1),  3,   XH_GREEN),
            ('TOPPADDING',    (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
            ('LEFTPADDING',   (0,0), (-1,-1), 14),
            ('RIGHTPADDING',  (0,0), (-1,-1), 10),
        ]))
        story += [cert_tbl, sp(4)]
    else:
        story += [para_box('No training certifications recorded.'), sp(4)]

    # ═══════════════════════════════════════════════════════════════════
    # 6. KEY SKILLS  (occupational health)
    # ═══════════════════════════════════════════════════════════════════
    story += [sec('KEY SKILLS'), sp(3)]
    for lbl, val in [
        ('Health Screening Completed:',    'Yes' if s9.get('occupational_health_screening')  else 'No'),
        ('Immunisation Records Provided:', 'Yes' if s9.get('immunisation_records_provided')  else 'No'),
        ('Fit for Nursing Duties:',        'Yes' if s9.get('fit_for_nursing_duties')          else 'No'),
    ]:
        story += [lv(lbl, val, lw=72*mm), sp(1)]
    story.append(sp(4))

    # ═══════════════════════════════════════════════════════════════════
    # 7. ADDITIONAL INFORMATION
    #    Driving Licence / Own Transport / References / Vetting / Date
    # ═══════════════════════════════════════════════════════════════════
    story += [sec('ADDITIONAL INFORMATION'), sp(3)]

    driving = _v(s2.get('driving_licence_number'))
    docs_list = []
    if docs_sub.get('passport'):          docs_list.append('Passport')
    if docs_sub.get('birth_certificate'): docs_list.append('Birth Certificate')
    if docs_sub.get('driving_licence'):   docs_list.append('Driving Licence')
    if docs_sub.get('proof_of_address'):  docs_list.append('Proof of Address')

    for lbl, val in [
        ('Driving Licence:', 'No'),
        ('Own Transport:',   'No'),
    ]:
        story += [lv(lbl, val, lw=70*mm), sp(1)]
    story.append(sp(4))

    # Professional References
    story += [sec('PROFESSIONAL REFERENCES'), sp(3)]
    ref_items = []
    for rk in ['reference_1', 'reference_2', 'reference_3']:
        r = s7.get(rk) or {}
        if r.get('name'):
            ref_items.append(r)

    if ref_items:
        for idx, r in enumerate(ref_items):
            name  = _v(r.get('name'))
            pos   = _v(r.get('position'))
            org   = _v(r.get('organisation'))
            tel   = _v(r.get('telephone'))
            email = _v(r.get('email'))
            dates = _v(r.get('dates_worked_together'))

            # Build a natural paragraph sentence
            ref_parts = []
            if name:   ref_parts.append(f"<b>{name}</b>")
            if pos:    ref_parts.append(pos)
            if org:    ref_parts.append(f"at {org}")
            ref_sentence = ', '.join(ref_parts[:1]) + (f", {ref_parts[1]}" if len(ref_parts) > 1 else '') + (f", {ref_parts[2]}" if len(ref_parts) > 2 else '') + '.'

            contact_parts = []
            if tel:   contact_parts.append(f"Tel: {tel}")
            if email: contact_parts.append(f"Email: {email}")
            if dates: contact_parts.append(f"Dates worked together: {dates}")
            contact_line = '   |   '.join(contact_parts)

            ref_box_rows = [[Paragraph(ref_sentence, S['val'])]]
            if contact_line:
                ref_box_rows.append([Paragraph(contact_line, S['italic'])])

            ref_tbl = Table(ref_box_rows, colWidths=[PAGE_W])
            ref_tbl.setStyle(TableStyle([
                ('BACKGROUND',    (0,0), (-1,-1), LIGHT_BG),
                ('BOX',           (0,0), (-1,-1), 0.4, MID_GRAY),
                ('LINEBEFORE',    (0,0), (0,-1),  3,   XH_GREEN),
                ('TOPPADDING',    (0,0), (-1,-1), 7),
                ('BOTTOMPADDING', (0,0), (-1,-1), 7),
                ('LEFTPADDING',   (0,0), (-1,-1), 10),
                ('RIGHTPADDING',  (0,0), (-1,-1), 8),
            ]))
            story += [ref_tbl, sp(2)]
    else:
        story.append(duties_box('No references provided.'))

    story.append(sp(4))

    # Signature / Date
    sig_t = Table(
        [[Paragraph('Signature: _______________________________', S['val']),
          Paragraph(f'Date: {_v(s12.get("date")) or "_____________________"}', S['val'])]],
        colWidths=[PAGE_W * 0.55, PAGE_W * 0.45]
    )
    story += [sig_t, sp(6)]

    # ── Footer ─────────────────────────────────────────────────────────
    story.append(HRFlowable(width=PAGE_W, color=XH_GREEN, thickness=1.2))
    story.append(sp(2))
    if logo_path:
        logo_sm = RLImage(logo_path, width=28*mm, height=28*mm * 94/316)
        ft = Table(
            [[logo_sm,
              Paragraph(
                  f'Xpress Health  •  {full_name}  •  {emp_code}',
                  S['footer']
              )]],
            colWidths=[32*mm, PAGE_W - 32*mm]
        )
        ft.setStyle(TableStyle([
            ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
            ('LEFTPADDING',   (0,0), (-1,-1), 0),
            ('TOPPADDING',    (0,0), (-1,-1), 2),
            ('BOTTOMPADDING', (0,0), (-1,-1), 2),
        ]))
        story.append(ft)
    else:
        story.append(Paragraph(
            f'Xpress Health  •  {full_name}  •  {emp_code}',
            S['footer']
        ))

    # ── Render ─────────────────────────────────────────────────────────
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
