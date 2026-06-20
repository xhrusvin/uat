from flask import render_template, request, jsonify, Response
from bson import ObjectId
from datetime import datetime
import json
import csv
import io
import re
import os

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



# ── AI CV Collection helper ───────────────────────────────────────────

def _ai_cvs_col():
    return db.live_staff_ai_cvs


# ── Generate AI CV ────────────────────────────────────────────────────

@admin_bp.route('/live-staffs/ai-cv/generate', methods=['POST'])
@admin_required
def live_staff_ai_cv_generate():
    """
    Call Gemini to write a fully personalised CV for a staff member,
    then render it to PDF, store the PDF in MongoDB, and return the record id.
    """
    data     = request.get_json()
    staff_id = (data.get('staff_id') or '').strip()
    if not staff_id:
        return jsonify({"success": False, "error": "Missing staff_id"}), 400

    try:
        doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
        if not doc:
            return jsonify({"success": False, "error": "Staff record not found"}), 404

        # ── Build the data summary for Gemini ─────────────────────────
        s1   = doc.get('section_1_personal_details') or {}
        s2   = doc.get('section_2_identity_verification') or {}
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

        # Qualifications
        qual_lines = []
        for qk in ['nursing_degree', 'postgraduate_qualification', 'other_qualification']:
            q = s4.get(qk) or {}
            if q.get('qualification') or q.get('institution'):
                qual_lines.append(
                    f"  - {_vv(q.get('qualification'))} | "
                    f"{_vv(q.get('institution'))} | "
                    f"{_vv(q.get('year_completed'))}"
                )

        # Employment history
        entries = [e for e in (s5.get('entries') or [])
                   if e.get('employer') or e.get('position')]
        exp_lines = []
        for e in entries:
            exp_lines.append(
                f"  - {_vv(e.get('position'))} at {_vv(e.get('employer'))} "
                f"({_vv(e.get('from'))} – {_vv(e.get('to') or 'Present')})"
            )

        # Training
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
        certs = [label for k, label in TLABELS.items() if s10.get(k)][:6]

        data_summary = f"""
Candidate: {full_name}
Role / User Type: {user_type}
Employee Code: {emp_code}
Address: {address}
Mobile: {mobile}
Email: {email}
Date of Birth: {dob}
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

Employment History:
{chr(10).join(exp_lines) if exp_lines else '  None recorded'}

Training & Certifications (on file):
{chr(10).join('  - ' + c for c in certs) if certs else '  None recorded'}
""".strip()

        prompt = f"""You are an expert professional CV writer specialising in Irish healthcare staffing.

Using ONLY the candidate data below, write a complete, professional, and highly personalised Curriculum Vitae in plain text. Make it read as though it was written by a skilled CV writer who knows this person well — not a template. Expand descriptions naturally. Use full sentences and flowing prose for the profile. Use bullet points for duties and skills.

Structure the CV exactly as follows (use these exact section headings in UPPERCASE):

PERSONAL DETAILS
PROFESSIONAL PROFILE
EDUCATION & QUALIFICATIONS
PROFESSIONAL EXPERIENCE
TRAINING & CERTIFICATIONS
KEY SKILLS
ADDITIONAL INFORMATION

Rules:
- PROFESSIONAL PROFILE: 2–3 rich flowing paragraphs, first-person perspective is NOT used. Write about the candidate in third person. Include their role, experience, specialisation, registration status, and personal qualities. Do not invent facts — only use the data provided. Expand naturally on what is given.
- EDUCATION & QUALIFICATIONS: One entry per qualification. Format: Qualification Name | Institution | Year
- PROFESSIONAL EXPERIENCE: One section per role. Include: Job Title, Employer, Dates. Then write 6–8 bullet point duties that are realistic, professional, and appropriate for the role type. Do NOT copy generic text — tailor to the employer and role.
- TRAINING & CERTIFICATIONS: List only the certifications provided in the data, as bullet points.
- KEY SKILLS: 8–10 bullet points relevant to their role and experience.
- ADDITIONAL INFORMATION: Include: Driving Licence: No | Own Transport: No | References: Available on request

---
CANDIDATE DATA:
{data_summary}
---

Output the CV text only. No preamble, no explanation, no markdown formatting symbols like ** or ##. Use plain text with clear section headings and bullet points using the dash character (-).
"""

        # ── Call Gemini ───────────────────────────────────────────────
        gemini_key = os.environ.get('GEMINI_API_KEY', '')
        if not gemini_key:
            return jsonify({"success": False,
                            "error": "GEMINI_API_KEY not set in environment"}), 500

        from google import genai as google_genai
        client   = google_genai.Client(api_key=gemini_key)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        cv_text = response.text.strip()

        # ── Build PDF from the AI text ────────────────────────────────
        pdf_bytes = _build_ai_cv_pdf(doc, cv_text)

        # ── Store in MongoDB ──────────────────────────────────────────
        col = _ai_cvs_col()
        existing = col.find_one({"staff_id": str(doc['_id'])})
        ai_doc = {
            "staff_id":    str(doc['_id']),
            "staff_name":  full_name,
            "employee_code": emp_code,
            "cv_text":     cv_text,
            "pdf_bytes":   pdf_bytes,
            "generated_at": datetime.utcnow(),
        }
        if existing:
            col.update_one({"_id": existing["_id"]}, {"$set": ai_doc})
            ai_id = str(existing["_id"])
        else:
            result = col.insert_one(ai_doc)
            ai_id  = str(result.inserted_id)

        return jsonify({
            "success":    True,
            "ai_cv_id":   ai_id,
            "staff_name": full_name,
            "message":    f"AI CV generated for {full_name}"
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/ai-cv/download/<ai_cv_id>')
@admin_required
def live_staff_ai_cv_download(ai_cv_id):
    """Download a previously generated AI CV PDF."""
    try:
        rec = _ai_cvs_col().find_one({"_id": ObjectId(ai_cv_id)})
        if not rec:
            return "AI CV not found", 404
        name = (rec.get('staff_name') or 'staff').replace(' ', '_')
        return Response(
            rec['pdf_bytes'],
            mimetype='application/pdf',
            headers={"Content-Disposition": f'attachment; filename="AI_CV_{name}.pdf"'}
        )
    except Exception as e:
        return str(e), 500


@admin_bp.route('/live-staffs/ai-cv/saved/<staff_id>')
@admin_required
def live_staff_ai_cv_saved(staff_id):
    """Return metadata about a saved AI CV for this staff member."""
    try:
        rec = _ai_cvs_col().find_one(
            {"staff_id": staff_id},
            {"pdf_bytes": 0}   # exclude binary
        )
        if not rec:
            return jsonify({"success": True, "found": False})
        return jsonify({
            "success":      True,
            "found":        True,
            "ai_cv_id":     str(rec["_id"]),
            "generated_at": rec["generated_at"].strftime("%d %b %Y %H:%M"),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── Build AI CV PDF from Gemini text ─────────────────────────────────

def _build_ai_cv_pdf(doc, cv_text):
    """
    Parse Gemini plain-text CV and render it as a branded Xpress Health PDF.
    Sections are split on UPPERCASE headings. Each section renders appropriately.
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table,
        TableStyle, HRFlowable, Image as RLImage
    )
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY
    import io as _io

    NAVY      = colors.HexColor('#1B3A6B')
    XH_GREEN  = colors.HexColor('#2E9E44')
    LIGHT_BG  = colors.HexColor('#EFF6FF')
    MID_GRAY  = colors.HexColor('#CBD5E1')
    TEXT_GRAY = colors.HexColor('#475569')
    NAVY_DARK = colors.HexColor('#162F58')
    WHITE     = colors.white

    W, H   = A4
    PAGE_W = W - 30 * mm

    def ps(name, **kw):
        d = dict(fontName='Helvetica', fontSize=10, textColor=TEXT_GRAY,
                 spaceAfter=2, leading=15)
        d.update(kw)
        return ParagraphStyle(name, **d)

    S = {
        'cv_title'  : ps('cv_title',  fontName='Helvetica-Bold', fontSize=20,
                         textColor=WHITE, alignment=TA_CENTER, spaceAfter=0, leading=24),
        'cv_name'   : ps('cv_name',   fontName='Helvetica-Bold', fontSize=13,
                         textColor=NAVY, alignment=TA_CENTER, spaceAfter=0, leading=18),
        'sec_head'  : ps('sec_head',  fontName='Helvetica-Bold', fontSize=10,
                         textColor=WHITE, spaceAfter=0, leading=14),
        'lbl'       : ps('lbl',       fontName='Helvetica-Bold', fontSize=10,
                         textColor=NAVY, spaceAfter=0, leading=14),
        'val'       : ps('val',       fontSize=10, textColor=TEXT_GRAY,
                         spaceAfter=0, leading=14),
        'body'      : ps('body',      fontSize=10, textColor=TEXT_GRAY,
                         alignment=TA_JUSTIFY, spaceAfter=4, leading=16),
        'exp_title' : ps('exp_title', fontName='Helvetica-Bold', fontSize=11,
                         textColor=NAVY, spaceAfter=0, leading=15),
        'exp_sub'   : ps('exp_sub',   fontName='Helvetica-Oblique', fontSize=10,
                         textColor=XH_GREEN, spaceAfter=2, leading=14),
        'exp_date'  : ps('exp_date',  fontName='Helvetica-Bold', fontSize=9,
                         textColor=WHITE, alignment=TA_CENTER, spaceAfter=0, leading=12),
        'bullet'    : ps('bullet',    fontSize=10, textColor=TEXT_GRAY,
                         leftIndent=8, spaceAfter=3, leading=15),
        'lv_lbl'    : ps('lv_lbl',   fontName='Helvetica-Bold', fontSize=10,
                         textColor=NAVY, spaceAfter=0, leading=14),
        'lv_val'    : ps('lv_val',   fontSize=10, textColor=TEXT_GRAY,
                         spaceAfter=0, leading=14),
    }

    sp = lambda n=3: Spacer(1, n * mm)

    def sec_bar(title):
        t = Table([[Paragraph(title, S['sec_head'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), NAVY),
            ('TOPPADDING',    (0,0), (-1,-1), 6),
            ('BOTTOMPADDING', (0,0), (-1,-1), 6),
            ('LEFTPADDING',   (0,0), (-1,-1), 10),
            ('LINEBELOW',     (0,0), (-1,-1), 2, XH_GREEN),
        ]))
        return t

    def lv_row(label, value):
        t = Table(
            [[Paragraph(label, S['lv_lbl']), Paragraph(value or '—', S['lv_val'])]],
            colWidths=[55*mm, PAGE_W - 55*mm]
        )
        t.setStyle(TableStyle([
            ('TOPPADDING',    (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ('LEFTPADDING',   (0,0), (0,0),   6),
            ('LEFTPADDING',   (1,0), (1,0),   4),
            ('LINEBELOW',     (0,0), (-1,-1), 0.3, MID_GRAY),
        ]))
        return t

    def profile_box(text):
        t = Table([[Paragraph(text, S['body'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), LIGHT_BG),
            ('BOX',           (0,0), (-1,-1), 0.5, MID_GRAY),
            ('LINEBEFORE',    (0,0), (0,-1),  4,   XH_GREEN),
            ('TOPPADDING',    (0,0), (-1,-1), 10),
            ('BOTTOMPADDING', (0,0), (-1,-1), 10),
            ('LEFTPADDING',   (0,0), (-1,-1), 12),
            ('RIGHTPADDING',  (0,0), (-1,-1), 12),
        ]))
        return t

    def bullet_para(text):
        clean = text.lstrip('- •	').strip()
        return Paragraph(f'• {clean}', S['bullet'])

    # ── Parse the CV text into sections ──────────────────────────────
    SECTION_HEADINGS = [
        'PERSONAL DETAILS', 'PROFESSIONAL PROFILE',
        'EDUCATION & QUALIFICATIONS', 'PROFESSIONAL EXPERIENCE',
        'TRAINING & CERTIFICATIONS', 'KEY SKILLS', 'ADDITIONAL INFORMATION',
    ]

    def split_sections(text):
        """Return dict of {heading: [lines]}"""
        sections = {}
        current  = '__preamble__'
        sections[current] = []
        for line in text.splitlines():
            stripped = line.strip()
            matched  = next((h for h in SECTION_HEADINGS
                             if stripped.upper() == h.upper()), None)
            if matched:
                current = matched
                sections[current] = []
            else:
                sections.setdefault(current, []).append(line)
        return sections

    sections = split_sections(cv_text)

    # ── Candidate name from doc ───────────────────────────────────────
    s1_d     = doc.get('section_1_personal_details') or {}
    full_name= _v(s1_d.get('full_name')) or 'Candidate'
    emp_code = _v(doc.get('employee_code'))

    # ── Logo ──────────────────────────────────────────────────────────
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

    # ── Build story ───────────────────────────────────────────────────
    buf   = _io.BytesIO()
    story = []

    # Header banner
    title_rows = [
        [Paragraph('CURRICULUM VITAE', S['cv_title'])],
        [Paragraph(full_name, S['cv_name'])],
    ]
    title_w   = PAGE_W - (55*mm if logo_path else 0)
    title_tbl = Table(title_rows, colWidths=[title_w])
    title_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), NAVY),
        ('TOPPADDING',    (0,0), (-1,-1), 12),
        ('BOTTOMPADDING', (0,0), (-1,-1), 12),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        ('BACKGROUND',    (0,1), (-1,1),  NAVY_DARK),
        ('TOPPADDING',    (0,1), (-1,1),  6),
        ('BOTTOMPADDING', (0,1), (-1,1),  8),
    ]))

    if logo_path:
        logo_img  = RLImage(logo_path, width=45*mm, height=45*mm*94/316)
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
        ('LINEBELOW',     (0,0), (-1,-1), 3, XH_GREEN),
    ]))
    story += [banner, sp(5)]

    # ── Render each section ───────────────────────────────────────────
    for heading in SECTION_HEADINGS:
        lines = [l for l in sections.get(heading, []) if l.strip()]
        if not lines:
            continue

        story += [sec_bar(heading), sp(3)]

        if heading == 'PERSONAL DETAILS':
            for line in lines:
                if ':' in line:
                    parts = line.split(':', 1)
                    label = parts[0].strip().rstrip(':') + ':'
                    value = parts[1].strip()
                    if value:
                        story += [lv_row(label, value), sp(1)]
            story.append(sp(4))

        elif heading == 'PROFESSIONAL PROFILE':
            paras = []
            current_para = []
            for line in lines:
                if line.strip() == '':
                    if current_para:
                        paras.append(' '.join(current_para))
                        current_para = []
                else:
                    current_para.append(line.strip())
            if current_para:
                paras.append(' '.join(current_para))
            for para in paras:
                if para:
                    story.append(profile_box(para))
                    story.append(sp(3))
            story.append(sp(2))

        elif heading == 'EDUCATION & QUALIFICATIONS':
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue
                if stripped.startswith('-'):
                    stripped = stripped[1:].strip()
                parts = [p.strip() for p in stripped.split('|')]
                qual   = parts[0] if len(parts) > 0 else stripped
                inst   = parts[1] if len(parts) > 1 else ''
                year   = parts[2] if len(parts) > 2 else ''

                if year:
                    yr_cell = Table([[Paragraph(year, S['exp_date'])]],
                                    colWidths=[None])
                    yr_cell.setStyle(TableStyle([
                        ('BACKGROUND',    (0,0), (-1,-1), XH_GREEN),
                        ('TOPPADDING',    (0,0), (-1,-1), 3),
                        ('BOTTOMPADDING', (0,0), (-1,-1), 3),
                        ('LEFTPADDING',   (0,0), (-1,-1), 6),
                        ('RIGHTPADDING',  (0,0), (-1,-1), 6),
                    ]))
                    head_row = Table(
                        [[Paragraph(f'<b>{qual}</b>', S['lbl']), yr_cell]],
                        colWidths=[PAGE_W - 38*mm, 38*mm]
                    )
                else:
                    head_row = Table(
                        [[Paragraph(f'<b>{qual}</b>', S['lbl'])]],
                        colWidths=[PAGE_W]
                    )
                head_row.setStyle(TableStyle([
                    ('TOPPADDING',    (0,0), (-1,-1), 0),
                    ('BOTTOMPADDING', (0,0), (-1,-1), 2),
                    ('LEFTPADDING',   (0,0), (-1,-1), 0),
                    ('RIGHTPADDING',  (0,0), (-1,-1), 0),
                    ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
                ]))
                story.append(head_row)
                if inst:
                    story.append(Paragraph(inst, S['exp_sub']))
                story += [sp(2), HRFlowable(width=PAGE_W, color=MID_GRAY, thickness=0.4), sp(3)]
            story.append(sp(1))

        elif heading == 'PROFESSIONAL EXPERIENCE':
            # Group lines by role — each role starts with "Job Title:" or blank separator
            roles = []
            current_role = []
            for line in lines:
                stripped = line.strip()
                is_role_start = (
                    stripped.lower().startswith('job title:') or
                    stripped.lower().startswith('role:')
                )
                if is_role_start and current_role:
                    roles.append(current_role)
                    current_role = [line]
                else:
                    current_role.append(line)
            if current_role:
                roles.append(current_role)

            for ri, role_lines in enumerate(roles):
                job_title = emp_name = dates_str = ''
                duty_lines = []
                in_duties = False

                for rl in role_lines:
                    sl = rl.strip()
                    if not sl:
                        continue
                    sl_lower = sl.lower()
                    if sl_lower.startswith('job title:') or sl_lower.startswith('role:'):
                        job_title = sl.split(':', 1)[1].strip()
                    elif sl_lower.startswith('employer:') or sl_lower.startswith('company:'):
                        emp_name = sl.split(':', 1)[1].strip()
                    elif sl_lower.startswith('dates:') or sl_lower.startswith('period:'):
                        dates_str = sl.split(':', 1)[1].strip()
                    elif sl_lower.startswith('duties') or sl_lower.startswith('responsibilities'):
                        in_duties = True
                    elif in_duties and (sl.startswith('-') or sl.startswith('•')):
                        duty_lines.append(sl.lstrip('- •').strip())
                    elif sl.startswith('-') or sl.startswith('•'):
                        # bullet before explicit duties heading
                        duty_lines.append(sl.lstrip('- •').strip())

                if not job_title and not emp_name:
                    continue

                # Role heading + date badge
                t_para = Paragraph(f'<b>{job_title}</b>' if job_title else '<b>Role</b>',
                                   S['exp_title'])
                if dates_str:
                    d_badge = Table([[Paragraph(dates_str, S['exp_date'])]],
                                    colWidths=[None])
                    d_badge.setStyle(TableStyle([
                        ('BACKGROUND',    (0,0), (-1,-1), XH_GREEN),
                        ('TOPPADDING',    (0,0), (-1,-1), 4),
                        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
                        ('LEFTPADDING',   (0,0), (-1,-1), 10),
                        ('RIGHTPADDING',  (0,0), (-1,-1), 10),
                    ]))
                    head_t = Table([[t_para, d_badge]],
                                   colWidths=[PAGE_W * 0.60, PAGE_W * 0.40])
                else:
                    head_t = Table([[t_para]], colWidths=[PAGE_W])

                head_t.setStyle(TableStyle([
                    ('TOPPADDING',    (0,0), (-1,-1), 0),
                    ('BOTTOMPADDING', (0,0), (-1,-1), 2),
                    ('LEFTPADDING',   (0,0), (-1,-1), 0),
                    ('RIGHTPADDING',  (0,0), (-1,-1), 0),
                    ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
                ]))
                story.append(head_t)
                if emp_name:
                    story.append(Paragraph(emp_name, S['exp_sub']))
                story.append(sp(2))

                if duty_lines:
                    story.append(Paragraph('<b>Duties &amp; Responsibilities</b>', S['lbl']))
                    story.append(sp(1))
                    for d in duty_lines:
                        if d:
                            story.append(bullet_para(d))
                    story.append(sp(2))

                if ri < len(roles) - 1:
                    story += [HRFlowable(width=PAGE_W, color=MID_GRAY, thickness=0.5), sp(3)]
            story.append(sp(3))

        elif heading in ('TRAINING & CERTIFICATIONS', 'KEY SKILLS'):
            for line in lines:
                stripped = line.strip()
                if stripped and not stripped.endswith(':'):
                    story.append(bullet_para(stripped))
            story.append(sp(4))

        elif heading == 'ADDITIONAL INFORMATION':
            for line in lines:
                stripped = line.strip()
                if not stripped:
                    continue
                if ':' in stripped:
                    parts = stripped.split(':', 1)
                    story += [lv_row(parts[0].strip() + ':', parts[1].strip()), sp(1)]
                else:
                    story.append(Paragraph(stripped, S['body']))
            story.append(sp(4))

    # Render
    pdf_doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=10*mm,  bottomMargin=15*mm,
    )
    pdf_doc.build(story)
    return buf.getvalue()


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
    """
    Build a rich individual Xpress Health CV PDF.
    Mirrors the Abidemi Aluko CV structure:
      1. Personal Details
      2. Professional Profile  (auto-generated flowing paragraph)
      3. Education & Qualifications  (entry per qual)
      4. Professional Experience  (one card per role with full duties)
      5. Training & Certifications  (bullet list, max 6)
      6. Key Skills  (bullet list)
      7. Additional Information  (Driving / Transport / References / Date)
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table,
        TableStyle, HRFlowable, Image as RLImage, ListFlowable, ListItem
    )
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
    import io as _io, os

    # ── Brand palette ─────────────────────────────────────────────────
    NAVY      = colors.HexColor('#1B3A6B')
    XH_GREEN  = colors.HexColor('#2E9E44')
    LIGHT_BG  = colors.HexColor('#EFF6FF')
    STRIPE    = colors.HexColor('#F0FDF4')
    MID_GRAY  = colors.HexColor('#CBD5E1')
    TEXT_DARK = colors.HexColor('#1E293B')
    TEXT_GRAY = colors.HexColor('#475569')
    WHITE     = colors.white

    W, H   = A4
    PAGE_W = W - 30 * mm   # 15 mm margins each side

    # ── Styles ────────────────────────────────────────────────────────
    def ps(name, **kw):
        d = dict(fontName='Helvetica', fontSize=10, textColor=TEXT_GRAY,
                 spaceAfter=2, leading=15)
        d.update(kw)
        return ParagraphStyle(name, **d)

    S = {
        'cv_title'  : ps('cv_title',   fontName='Helvetica-Bold', fontSize=20,
                         textColor=WHITE, alignment=TA_CENTER, spaceAfter=0, leading=24),
        'cv_name'   : ps('cv_name',    fontName='Helvetica-Bold', fontSize=13,
                         textColor=NAVY, alignment=TA_CENTER, spaceAfter=0, leading=18),
        'sec_head'  : ps('sec_head',   fontName='Helvetica-Bold', fontSize=10,
                         textColor=WHITE, spaceAfter=0, leading=14),
        'lbl'       : ps('lbl',        fontName='Helvetica-Bold', fontSize=10,
                         textColor=NAVY, spaceAfter=0, leading=14),
        'val'       : ps('val',        fontSize=10, textColor=TEXT_GRAY,
                         spaceAfter=0, leading=14),
        'body'      : ps('body',       fontSize=10, textColor=TEXT_GRAY,
                         alignment=TA_JUSTIFY, spaceAfter=4, leading=15),
        'exp_title' : ps('exp_title',  fontName='Helvetica-Bold', fontSize=11,
                         textColor=NAVY, spaceAfter=0, leading=15),
        'exp_sub'   : ps('exp_sub',    fontName='Helvetica-Oblique', fontSize=10,
                         textColor=XH_GREEN, spaceAfter=0, leading=14),
        'exp_date'  : ps('exp_date',   fontName='Helvetica-Bold', fontSize=9,
                         textColor=WHITE, alignment=TA_CENTER, spaceAfter=0, leading=12),
        'duty'      : ps('duty',       fontSize=10, textColor=TEXT_GRAY,
                         leftIndent=8, spaceAfter=3, leading=15),
        'bullet'    : ps('bullet',     fontSize=10, textColor=TEXT_GRAY,
                         leftIndent=8, spaceAfter=3, leading=15),
        'footer'    : ps('footer',     fontSize=7,  textColor=MID_GRAY,
                         alignment=TA_CENTER, spaceAfter=0),
        'qual_title': ps('qual_title', fontName='Helvetica-Bold', fontSize=10,
                         textColor=NAVY, spaceAfter=1, leading=14),
        'qual_sub'  : ps('qual_sub',   fontName='Helvetica-Oblique', fontSize=9,
                         textColor=TEXT_GRAY, spaceAfter=0, leading=13),
    }

    sp = lambda n=3: Spacer(1, n * mm)

    # ── Helpers ───────────────────────────────────────────────────────
    def sec(title):
        t = Table([[Paragraph(title, S['sec_head'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), NAVY),
            ('TOPPADDING',    (0,0), (-1,-1), 6),
            ('BOTTOMPADDING', (0,0), (-1,-1), 6),
            ('LEFTPADDING',   (0,0), (-1,-1), 10),
            ('LINEBELOW',     (0,0), (-1,-1), 2, XH_GREEN),
        ]))
        return t

    def lv(label, value, lw=55*mm):
        val_text = value if value else '—'
        t = Table(
            [[Paragraph(label, S['lbl']), Paragraph(val_text, S['val'])]],
            colWidths=[lw, PAGE_W - lw]
        )
        t.setStyle(TableStyle([
            ('TOPPADDING',    (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ('LEFTPADDING',   (0,0), (0,0),   6),
            ('LEFTPADDING',   (1,0), (1,0),   4),
            ('LINEBELOW',     (0,0), (-1,-1), 0.3, MID_GRAY),
        ]))
        return t

    def date_badge(text):
        """Green pill badge for date range."""
        t = Table([[Paragraph(text, S['exp_date'])]], colWidths=[None])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), XH_GREEN),
            ('TOPPADDING',    (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ('LEFTPADDING',   (0,0), (-1,-1), 8),
            ('RIGHTPADDING',  (0,0), (-1,-1), 8),
            ('ROUNDEDCORNERS',(0,0), (-1,-1), 4),
        ]))
        return t

    def bullet_item(text):
        return Paragraph(f'\u2022\u2003{text}', S['bullet'])

    def duty_item(text):
        return Paragraph(f'\u2022\u2003{text}', S['duty'])

    def profile_box(text):
        t = Table([[Paragraph(text, S['body'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), LIGHT_BG),
            ('BOX',           (0,0), (-1,-1), 0.5, MID_GRAY),
            ('LINEBEFORE',    (0,0), (0,-1),  4,   XH_GREEN),
            ('TOPPADDING',    (0,0), (-1,-1), 10),
            ('BOTTOMPADDING', (0,0), (-1,-1), 10),
            ('LEFTPADDING',   (0,0), (-1,-1), 12),
            ('RIGHTPADDING',  (0,0), (-1,-1), 12),
        ]))
        return t

    # ── Data ─────────────────────────────────────────────────────────
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

    full_name  = _v(s1.get('full_name'))
    emp_code   = _v(doc.get('employee_code'))
    user_type  = _v(doc.get('user_type'))
    address    = _v(s1.get('address'))
    mobile     = _v(s1.get('mobile_number'))
    email      = _v(doc.get('email'))
    dob        = _v(s1.get('date_of_birth'))
    nationality= _v(s1.get('nationality'))
    reg_pin    = _v(s3.get('registration_number_pin'))
    reg_exp    = _v(s3.get('registration_expiry_date'))
    divisions  = ', '.join(s3.get('divisions_registered_in') or [])
    nmbi       = s3.get('nmbi_active_declaration')
    perm_work  = _v(visa.get('permission_to_work'))
    visa_type  = _v(visa.get('visa_type'))
    total_exp  = _v(s5.get('total_experience'))
    entries    = [e for e in (s5.get('entries') or [])
                  if e.get('employer') or e.get('position')]

    # ── Logo ─────────────────────────────────────────────────────────
    logo_path = None
    for c in [
        os.path.join(os.path.dirname(__file__), '..', 'static', 'images', 'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'img', 'logo.png'),
        os.path.join(os.path.dirname(__file__), '..', 'static', 'logo.png'),
        'static/images/logo.png', 'static/img/logo.png', 'static/logo.png',
    ]:
        if os.path.exists(c):
            logo_path = c
            break

    # ── Build story ───────────────────────────────────────────────────
    buf   = _io.BytesIO()
    story = []

    # ════════════════════════════════════════════════════════════════
    # HEADER — Logo + "CURRICULUM VITAE" + candidate name
    # ════════════════════════════════════════════════════════════════
    title_rows = [
        [Paragraph('CURRICULUM VITAE', S['cv_title'])],
        [Paragraph(full_name or 'Candidate', S['cv_name'])],   # name under title
    ]
    title_w   = PAGE_W - (55*mm if logo_path else 0)
    title_tbl = Table(title_rows, colWidths=[title_w])
    title_tbl.setStyle(TableStyle([
        ('BACKGROUND',    (0,0), (-1,-1), NAVY),
        ('TOPPADDING',    (0,0), (-1,-1), 12),
        ('BOTTOMPADDING', (0,0), (-1,-1), 12),
        ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        # name row slightly lighter bg
        ('BACKGROUND',    (0,1), (-1,1),  colors.HexColor('#162F58')),
        ('TOPPADDING',    (0,1), (-1,1),  6),
        ('BOTTOMPADDING', (0,1), (-1,1),  8),
    ]))

    if logo_path:
        logo_img  = RLImage(logo_path, width=45*mm, height=45*mm*94/316)
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
        ('LINEBELOW',     (0,0), (-1,-1), 3, XH_GREEN),
    ]))
    story += [banner, sp(5)]

    # ════════════════════════════════════════════════════════════════
    # 1. PERSONAL DETAILS
    # ════════════════════════════════════════════════════════════════
    story += [sec('PERSONAL DETAILS'), sp(3)]
    for label, value in [
        ('Full Name:',     full_name),
        ('Address:',       address),
        ('Mobile Number:', mobile),
        ('Email Address:', email),
        ('Date of Birth:', dob),
        ('Nationality:',   nationality),
    ]:
        if value:
            story += [lv(label, value), sp(1)]
    story.append(sp(4))

    # ════════════════════════════════════════════════════════════════
    # 2. PROFESSIONAL PROFILE — rich individual paragraph
    # ════════════════════════════════════════════════════════════════
    story += [sec('PROFESSIONAL PROFILE'), sp(3)]

    # Build a natural multi-sentence profile from the data
    para_sentences = []

    # Opening — who they are + experience + speciality
    if full_name and user_type:
        opener = f"{full_name} is a compassionate and dedicated {user_type}"
        if divisions:
            opener += f" specialising in {divisions}"
        if total_exp:
            opener += f", with {total_exp} of professional healthcare experience"
        para_sentences.append(opener)

    # Most recent role
    if entries:
        latest = entries[0]
        pos_l  = _v(latest.get('position'))
        emp_l  = _v(latest.get('employer'))
        d_from = _v(latest.get('from'))
        d_to   = _v(latest.get('to'))
        if pos_l and emp_l:
            role_s = f"Most recently working as {pos_l} at {emp_l}"
            if d_from:
                role_s += f" from {d_from}"
                role_s += f" to {d_to}" if d_to else " to present"
            para_sentences.append(role_s)

    # Registration / professional status
    if reg_pin or reg_exp or nmbi:
        reg_s = "Professionally registered"
        if reg_pin:
            reg_s += f" (PIN: {reg_pin})"
        if reg_exp:
            reg_s += f" with registration valid until {reg_exp}"
        if nmbi:
            reg_s += ", holding an active NMBI declaration"
        para_sentences.append(reg_s)

    # Work authorisation
    if perm_work == 'Yes' and visa_type:
        para_sentences.append(
            f"Fully authorised to work in Ireland ({visa_type})"
        )
    elif perm_work == 'Yes':
        para_sentences.append("Fully authorised to work in Ireland")

    # Occupational health
    if s9.get('fit_for_nursing_duties'):
        para_sentences.append(
            "Confirmed fit for nursing duties with up-to-date occupational health clearance"
        )

    # Garda vetting
    if s8.get('garda_vetting_submitted'):
        para_sentences.append("Garda vetted and cleared to work with vulnerable adults")

    # Qualities closing line
    qualities = (
        "Known for excellent communication, a caring and professional manner, "
        "and a genuine commitment to promoting client dignity, independence, and wellbeing"
    )
    para_sentences.append(qualities)

    profile_text = '. '.join(para_sentences) + '.'

    story.append(profile_box(profile_text))
    story.append(sp(4))

    # ════════════════════════════════════════════════════════════════
    # 3. EDUCATION & QUALIFICATIONS
    # ════════════════════════════════════════════════════════════════
    story += [sec('EDUCATION & QUALIFICATIONS'), sp(3)]

    qual_keys = ['nursing_degree', 'postgraduate_qualification', 'other_qualification']
    qual_found = False
    for qk in qual_keys:
        q     = s4.get(qk) or {}
        qname = _v(q.get('qualification'))
        qinst = _v(q.get('institution'))
        qyear = _v(q.get('year_completed'))
        if not (qname or qinst):
            continue
        qual_found = True

        # Heading row: qual name left, year right
        h_left  = Paragraph(f'<b>{qname}</b>' if qname else '<b>Qualification</b>',
                             S['qual_title'])
        h_right = Paragraph(qyear, S['exp_date'])
        yr_cell = Table([[h_right]], colWidths=[30*mm])
        yr_cell.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), XH_GREEN),
            ('TOPPADDING',    (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ('LEFTPADDING',   (0,0), (-1,-1), 6),
            ('RIGHTPADDING',  (0,0), (-1,-1), 6),
        ]))
        head_row = Table([[h_left, yr_cell]],
                         colWidths=[PAGE_W - 34*mm, 34*mm])
        head_row.setStyle(TableStyle([
            ('TOPPADDING',    (0,0), (-1,-1), 0),
            ('BOTTOMPADDING', (0,0), (-1,-1), 2),
            ('LEFTPADDING',   (0,0), (-1,-1), 0),
            ('RIGHTPADDING',  (0,0), (-1,-1), 0),
            ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        ]))
        story.append(head_row)
        if qinst:
            story.append(Paragraph(qinst, S['qual_sub']))
        story += [sp(2), HRFlowable(width=PAGE_W, color=MID_GRAY, thickness=0.4), sp(3)]

    if not qual_found:
        story.append(Paragraph('No qualifications recorded.', S['body']))
        story.append(sp(3))

    story.append(sp(1))

    # ════════════════════════════════════════════════════════════════
    # 4. PROFESSIONAL EXPERIENCE — one card per role with full duties
    # ════════════════════════════════════════════════════════════════
    story += [sec('PROFESSIONAL EXPERIENCE'), sp(3)]

    if entries:
        for i, e in enumerate(entries):
            pos     = _v(e.get('position'))
            emp     = _v(e.get('employer'))
            loc     = _v(e.get('location', ''))   # location field if present
            d_from  = _v(e.get('from'))
            d_to    = _v(e.get('to'))
            leaving = _v(e.get('reason_for_leaving'))

            # Date range string
            if d_from and d_to:
                date_str = f"{d_from} \u2013 {d_to}"
            elif d_from:
                date_str = f"{d_from} \u2013 Present"
            elif d_to:
                date_str = f"Until {d_to}"
            else:
                date_str = ''

            # ── Role heading: title left, date badge right ────────
            t_para  = Paragraph(f'<b>{pos}</b>' if pos else '<b>Role</b>', S['exp_title'])
            if date_str:
                d_badge = Table([[Paragraph(date_str, S['exp_date'])]],
                                colWidths=[None])
                d_badge.setStyle(TableStyle([
                    ('BACKGROUND',    (0,0), (-1,-1), XH_GREEN),
                    ('TOPPADDING',    (0,0), (-1,-1), 4),
                    ('BOTTOMPADDING', (0,0), (-1,-1), 4),
                    ('LEFTPADDING',   (0,0), (-1,-1), 10),
                    ('RIGHTPADDING',  (0,0), (-1,-1), 10),
                ]))
                head_t = Table([[t_para, d_badge]],
                               colWidths=[PAGE_W * 0.60, PAGE_W * 0.40])
            else:
                head_t = Table([[t_para]], colWidths=[PAGE_W])

            head_t.setStyle(TableStyle([
                ('TOPPADDING',    (0,0), (-1,-1), 0),
                ('BOTTOMPADDING', (0,0), (-1,-1), 2),
                ('LEFTPADDING',   (0,0), (-1,-1), 0),
                ('RIGHTPADDING',  (0,0), (-1,-1), 0),
                ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
            ]))
            story.append(head_t)

            # Employer + location sub-line
            sub_parts = []
            if emp: sub_parts.append(emp)
            if loc: sub_parts.append(loc)
            if sub_parts:
                story.append(Paragraph(' \u2022 '.join(sub_parts), S['exp_sub']))

            story.append(sp(2))

            # ── Description paragraph ─────────────────────────────
            # Build a rich descriptive paragraph for this role
            desc_parts = []
            if pos and emp:
                desc = f"Worked as <b>{pos}</b> at {emp}"
                if loc:
                    desc += f", based in {loc}"
                if d_from and d_to:
                    desc += f", from {d_from} to {d_to}"
                elif d_from:
                    desc += f" from {d_from} to present"
                desc_parts.append(desc)

            # Add responsibilities based on user_type keywords
            ut_lower = user_type.lower() if user_type else ''
            if 'nurse' in ut_lower or 'nursing' in ut_lower:
                role_duties = [
                    "Assisted residents and clients with all aspects of personal care including personal hygiene, dressing, and grooming",
                    "Supported safe mobility and transfers, assisting with walking, wheelchair use, and repositioning",
                    "Observed and reported changes in residents' condition — including skin integrity, pain, and behaviour — to the nursing team",
                    "Assisted with medication administration under the direct supervision of qualified nursing staff",
                    "Maintained accurate records and contributed to care planning in line with individual care plans",
                    "Built positive and respectful therapeutic relationships with residents and their families",
                    "Worked effectively within multidisciplinary teams, supporting a safe and caring environment",
                ]
            elif 'healthcare' in ut_lower or 'hca' in ut_lower or 'assistant' in ut_lower:
                role_duties = [
                    "Provided high-quality, person-centred care and support tailored to each individual client's needs",
                    "Assisted clients with all activities of daily living including personal care, meal preparation, and mobility support",
                    "Observed and reported changes in clients' physical or emotional wellbeing to the supervising care team",
                    "Promoted client independence, dignity, and choice throughout all aspects of care delivery",
                    "Maintained comprehensive and accurate care records in line with organisational policies",
                    "Collaborated effectively with colleagues, families, and multidisciplinary teams to ensure continuity of care",
                    "Followed safe working practices, infection control procedures, and moving and handling guidelines at all times",
                ]
            else:
                role_duties = [
                    "Delivered high standards of professional care and support in line with organisational policies and procedures",
                    "Maintained clear and accurate records and communicated effectively with the wider team",
                    "Promoted the dignity, independence, and wellbeing of all clients and residents at all times",
                ]

            if desc_parts:
                story.append(Paragraph('. '.join(desc_parts) + '.', S['body']))
                story.append(sp(2))

            # Duties heading
            story.append(Paragraph('<b>Duties &amp; Responsibilities</b>', S['lbl']))
            story.append(sp(1))
            for duty in role_duties:
                story.append(duty_item(duty))
            story.append(sp(2))

            if leaving:
                story.append(Paragraph(
                    f'<i>Reason for leaving: {leaving}</i>', S['qual_sub']
                ))
                story.append(sp(2))

            if i < len(entries) - 1:
                story.append(HRFlowable(width=PAGE_W, color=MID_GRAY, thickness=0.5))
                story.append(sp(3))

    else:
        story.append(Paragraph('No employment history recorded.', S['body']))
        story.append(sp(3))

    if total_exp:
        story += [sp(2), lv('Total Experience:', total_exp, lw=55*mm), sp(1)]
    story.append(sp(4))

    # ════════════════════════════════════════════════════════════════
    # 5. TRAINING & CERTIFICATIONS — bullet list, max 6
    # ════════════════════════════════════════════════════════════════
    story += [sec('TRAINING & CERTIFICATIONS'), sp(3)]
    TLABELS = {
        'manual_handling':              'Manual Handling',
        'cpr_bls':                      'CPR / Basic Life Support',
        'fire_safety':                  'Fire Safety',
        'infection_prevention_control': 'Infection Prevention & Control',
        'hand_hygiene':                 'Hand Hygiene',
        'safeguarding':                 'Safeguarding Vulnerable Adults',
        'children_first':               'Children First',
        'cyber_security':               'Cyber Security Awareness',
        'dignity_at_work':              'Dignity at Work',
        'open_disclosure':              'Open Disclosure',
        'mapa_pmav':                    'MAPA / PMAV (De-escalation)',
    }
    cert_labels = []
    for key, label in TLABELS.items():
        if s10.get(key):
            cert_labels.append(label)
        if len(cert_labels) == 6:
            break

    if cert_labels:
        for label in cert_labels:
            story.append(bullet_item(label))
        story.append(sp(4))
    else:
        story.append(Paragraph('No training certifications recorded.', S['body']))
        story.append(sp(4))

    # ════════════════════════════════════════════════════════════════
    # 6. KEY SKILLS — bullets from health/registration data
    # ════════════════════════════════════════════════════════════════
    story += [sec('KEY SKILLS'), sp(3)]
    ut_lower = user_type.lower() if user_type else ''
    if 'nurse' in ut_lower or 'nursing' in ut_lower:
        skills = [
            'Medication administration (under nursing supervision)',
            'Patient assessment and observation',
            'Personal and person-centred care',
            'Patient moving and handling / safe mobility support',
            'Communication and interpersonal skills',
            'Observation, monitoring, and reporting of patient condition',
            'Record keeping and report writing',
            'Teamwork and collaboration with multidisciplinary teams',
            'Compassion, empathy, and patience',
            'Promoting patient dignity and independence',
        ]
    else:
        skills = [
            'Person-centred care and support',
            'Assistance with all activities of daily living',
            'Patient moving and handling / safe mobility support',
            'Communication and interpersonal skills',
            'Observation, monitoring, and reporting of client condition',
            'Record keeping and report writing',
            'Teamwork and collaboration',
            'Compassion, empathy, and patience',
            'Promoting client dignity and independence',
        ]

    for skill in skills:
        story.append(bullet_item(skill))
    story.append(sp(4))

    # ════════════════════════════════════════════════════════════════
    # 7. ADDITIONAL INFORMATION
    # ════════════════════════════════════════════════════════════════
    story += [sec('ADDITIONAL INFORMATION'), sp(3)]
    for label, value in [
        ('Driving Licence:', 'No'),
        ('Own Transport:',   'No'),
        ('References:',      'Available on request'),
        ('Date:',            _v(s12.get('date')) or '_____________________'),
    ]:
        story += [lv(label, value), sp(1)]
    story.append(sp(4))

    # ── Render ────────────────────────────────────────────────────────
    pdf_doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm,
        topMargin=10*mm,  bottomMargin=15*mm,
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
