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

def _ai_interviews_col():
    return db.live_staff_ai_interviews


# ── Generate AI Interview Notes ───────────────────────────────────────

@admin_bp.route('/live-staffs/ai-interview/generate', methods=['POST'])
@admin_required
def live_staff_ai_interview_generate():
    """
    Call Gemini to write realistic interview notes for a staff member
    using the exact structure of the Nurse Interview Template.
    Saves PDF to static/interviews/ and metadata to MongoDB.
    """
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
        s5   = doc.get('section_5_employment_history') or {}
        s8   = doc.get('section_8_garda_vetting_police_clearance') or {}
        s9   = doc.get('section_9_occupational_health') or {}
        s10  = doc.get('section_10_mandatory_training') or {}
        visa = s1.get('work_permit_visa_status') or {}

        full_name   = _v(s1.get('full_name'))
        user_type   = _v(doc.get('user_type'))
        address     = _v(s1.get('address'))
        nationality = _v(s1.get('nationality'))
        reg_pin     = _v(s3.get('registration_number_pin'))
        visa_type   = _v(visa.get('visa_type'))
        divisions   = ', '.join(s3.get('divisions_registered_in') or [])
        total_exp   = _v(s5.get('total_experience'))
        entries     = [e for e in (s5.get('entries') or [])
                       if e.get('employer') or e.get('position')]
        nmbi        = 'Yes' if s3.get('nmbi_active_declaration') else 'No'
        garda       = 'Yes' if s8.get('garda_vetting_submitted') else 'No'
        bls         = 'Yes' if s10.get('cpr_bls') else 'No'
        manual      = 'Yes' if s10.get('manual_handling') else 'No'
        fit         = 'Yes' if s9.get('fit_for_nursing_duties') else 'No'

        # Preferred county from address
        county = ''
        if address:
            parts = [p.strip() for p in address.replace(',', ' ').split()]
            for p in parts:
                if p.lower().startswith('co.') or p.lower() == 'county':
                    idx = parts.index(p)
                    if idx + 1 < len(parts):
                        county = parts[idx + 1]
                    break
            if not county:
                county = parts[-1] if parts else ''

        # Build experience summary for prompt
        exp_lines = []
        for e in entries[:5]:
            pos = _v(e.get('position')); emp = _v(e.get('employer'))
            d_from = _v(e.get('from')); d_to = _v(e.get('to'))
            if pos or emp:
                exp_lines.append(
                    f"  - {pos} at {emp} ({d_from} – {d_to or 'Present'})"
                )

        TLABELS = {
            'manual_handling': 'Manual Handling',
            'cpr_bls': 'BLS/CPR',
            'safeguarding': 'Safeguarding',
            'fire_safety': 'Fire Safety',
            'infection_prevention_control': 'Infection Prevention & Control',
        }
        certs = [label for k, label in TLABELS.items() if s10.get(k)]

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
{chr(10).join(exp_lines) if exp_lines else '  None recorded'}

Certifications on file: {', '.join(certs) if certs else 'None recorded'}
""".strip()

        prompt = f"""You are an experienced nursing recruitment consultant at Xpress Health, Ireland.

Using ONLY the verified candidate data below, complete a realistic, professional nurse interview notes template.
Answers must be written as if the candidate themselves just answered each question in a live phone/video interview.
Write naturally — conversational but professional. First person where appropriate ("I have", "I work", "I currently").

STRICT RULES — NO HALLUCINATION:
- Use ONLY the facts provided in CANDIDATE DATA. Do not invent employers, dates, locations, or qualifications.
- If data is missing for a field, write a realistic professional answer appropriate to their role and experience level without inventing specific names.
- Clinical question answers must be clinically appropriate for a {user_type}.
- Assessment scores should reflect the quality of answers: score each out of 5.
- Do NOT add any text outside the template structure below.

Output ONLY the completed template below — no preamble, no explanations, no markdown symbols:

---
Completed {user_type} Interview Template

Name: [full name]
Location: [county/city from address]
NMBI PIN: [registration pin or N/A]
Visa Status: [visa type]

Experience

1. Tell me about your nursing experience.
[Write a 4–6 sentence answer in first person describing their experience, speciality, and current/most recent role. Use only the data provided.]

2. How many years in Ireland?
[Write a realistic answer based on employment history dates. If Ireland-based work is evident, state it clearly.]

3. Acute, Nursing Home, Community, or Mental Health?
[Based on employment history, state the most relevant care setting.]

Clinical Questions

1. How would you manage a deteriorating patient?
[Write a clinically accurate 4–5 sentence answer appropriate for a {user_type}. Use recognised frameworks (ABCDE, NEWS2, ISBAR) where appropriate.]

2. What would you do if you witnessed a medication error?
[Write a clinically accurate 4–5 sentence answer covering patient safety, reporting, documentation, and prevention.]

Compliance
NMBI Registration: [Yes/No based on data]
BLS/CPR: [Yes/No based on data]
Manual Handling: [Yes/No based on data]
Garda Vetting: [Yes/No based on data]
References: Yes

Availability
Preferred counties: [county from address, or nearest city]
Day/Night/Both: Both
Earliest start date: Immediate

Assessment
Communication: [X/5]
Clinical Knowledge: [X/5]
Experience: [X/5]
Suitable: Yes
---

CANDIDATE DATA (use ONLY this):
{data_summary}
"""

        gemini_key = os.environ.get('GEMINI_API_KEY', '')
        if not gemini_key:
            return jsonify({"success": False,
                            "error": "GEMINI_API_KEY not set"}), 500

        from google import genai as google_genai
        client   = google_genai.Client(api_key=gemini_key)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        interview_text = response.text.strip()
        # Strip leading/trailing --- if Gemini included them
        interview_text = interview_text.strip('-').strip()

        # Build PDF
        pdf_bytes = _build_interview_pdf(doc, interview_text)

        # Save to static/interviews/
        safe_name    = (full_name or 'staff').replace(' ', '_').replace('/', '_')
        filename     = f"Interview_{safe_name}_{staff_id}.pdf"
        folder       = os.path.join('static', 'interviews')
        os.makedirs(folder, exist_ok=True)
        filepath     = os.path.join(folder, filename)
        with open(filepath, 'wb') as f:
            f.write(pdf_bytes)

        # Save metadata to MongoDB
        col      = _ai_interviews_col()
        existing = col.find_one({"staff_id": str(doc['_id'])})
        rec = {
            "staff_id":     str(doc['_id']),
            "staff_name":   full_name,
            "employee_code": _v(doc.get('employee_code')),
            "interview_text": interview_text,
            "filename":     filename,
            "filepath":     filepath,
            "generated_at": datetime.utcnow(),
        }
        if existing:
            col.update_one({"_id": existing["_id"]}, {"$set": rec})
            rec_id = str(existing["_id"])
        else:
            result = col.insert_one(rec)
            rec_id = str(result.inserted_id)

        return jsonify({
            "success":      True,
            "interview_id": rec_id,
            "staff_name":   full_name,
            "message":      f"Interview notes generated for {full_name}"
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/ai-interview/download/<interview_id>')
@admin_required
def live_staff_ai_interview_download(interview_id):
    """Serve saved interview PDF from static/interviews/."""
    try:
        rec = _ai_interviews_col().find_one({"_id": ObjectId(interview_id)})
        if not rec:
            return "Interview notes not found", 404
        fp = rec.get('filepath', '')
        if not fp or not os.path.exists(fp):
            return "File not found — please regenerate", 404
        name = (rec.get('staff_name') or 'staff').replace(' ', '_')
        with open(fp, 'rb') as f:
            pdf_bytes = f.read()
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={"Content-Disposition":
                     f'attachment; filename="Interview_{name}.pdf"'}
        )
    except Exception as e:
        return str(e), 500


@admin_bp.route('/live-staffs/ai-interview/saved/<staff_id>')
@admin_required
def live_staff_ai_interview_saved(staff_id):
    """Check if saved interview notes exist for this staff member."""
    try:
        rec = _ai_interviews_col().find_one(
            {"staff_id": staff_id},
            {"interview_text": 0}
        )
        if not rec:
            return jsonify({"success": True, "found": False})
        return jsonify({
            "success":      True,
            "found":        True,
            "interview_id": str(rec["_id"]),
            "generated_at": rec["generated_at"].strftime("%d %b %Y %H:%M"),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── Build Interview Notes PDF ─────────────────────────────────────────

def _build_interview_pdf(doc, interview_text):
    """
    Render AI-generated interview notes as a clean professional PDF
    matching the Completed Nurse Interview Template structure.
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

    # ── Palette — professional, clinical feel ─────────────────────────
    NAVY      = colors.HexColor('#1B3A6B')
    XH_GREEN  = colors.HexColor('#2E9E44')
    DARK      = colors.HexColor('#111111')
    GRAY      = colors.HexColor('#555555')
    LT_GRAY   = colors.HexColor('#CCCCCC')
    BG_LIGHT  = colors.HexColor('#F4F7FB')
    BG_GREEN  = colors.HexColor('#EEF8F1')
    WHITE     = colors.white

    W, H   = A4
    LM=RM  = 18 * mm
    PAGE_W = W - LM - RM

    def ps(name, **kw):
        d = dict(fontName='Helvetica', fontSize=10, textColor=DARK,
                 spaceAfter=2, leading=15)
        d.update(kw)
        return ParagraphStyle(name, **d)

    S = {
        'doc_title' : ps('doc_title', fontName='Helvetica-Bold', fontSize=14,
                         textColor=WHITE, alignment=TA_CENTER, leading=18),
        'sec_head'  : ps('sec_head',  fontName='Helvetica-Bold', fontSize=10,
                         textColor=WHITE, leading=14),
        'q_label'   : ps('q_label',   fontName='Helvetica-Bold', fontSize=9,
                         textColor=NAVY, leading=13, spaceAfter=1),
        'answer'    : ps('answer',    fontSize=10, textColor=DARK,
                         alignment=TA_JUSTIFY, leading=15, spaceAfter=3),
        'field_lbl' : ps('field_lbl', fontName='Helvetica-Bold', fontSize=10,
                         textColor=NAVY, leading=14),
        'field_val' : ps('field_val', fontSize=10, textColor=DARK, leading=14),
        'score'     : ps('score',     fontName='Helvetica-Bold', fontSize=11,
                         textColor=XH_GREEN, leading=14),
        'footer'    : ps('footer',    fontSize=7, textColor=LT_GRAY,
                         alignment=TA_CENTER),
    }

    sp   = lambda n=3: Spacer(1, n * mm)

    def sec_bar(title):
        t = Table([[Paragraph(title, S['sec_head'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), NAVY),
            ('TOPPADDING',    (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
            ('LEFTPADDING',   (0,0), (-1,-1), 10),
            ('LINEBELOW',     (0,0), (-1,-1), 2, XH_GREEN),
        ]))
        return t

    def answer_box(text, bg=BG_LIGHT):
        t = Table([[Paragraph(text or '—', S['answer'])]], colWidths=[PAGE_W])
        t.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), bg),
            ('BOX',           (0,0), (-1,-1), 0.4, LT_GRAY),
            ('LINEBEFORE',    (0,0), (0,-1),  3,   XH_GREEN),
            ('TOPPADDING',    (0,0), (-1,-1), 8),
            ('BOTTOMPADDING', (0,0), (-1,-1), 8),
            ('LEFTPADDING',   (0,0), (-1,-1), 10),
            ('RIGHTPADDING',  (0,0), (-1,-1), 10),
        ]))
        return t

    def lv_row(label, value, col_w=55*mm):
        t = Table(
            [[Paragraph(label, S['field_lbl']),
              Paragraph(value or '—', S['field_val'])]],
            colWidths=[col_w, PAGE_W - col_w]
        )
        t.setStyle(TableStyle([
            ('TOPPADDING',    (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ('LEFTPADDING',   (0,0), (0,0),   6),
            ('LEFTPADDING',   (1,0), (1,0),   4),
            ('LINEBELOW',     (0,0), (-1,-1), 0.3, LT_GRAY),
        ]))
        return t

    def score_row(label, score):
        t = Table(
            [[Paragraph(label, S['field_lbl']),
              Paragraph(score, S['score'])]],
            colWidths=[PAGE_W * 0.60, PAGE_W * 0.40]
        )
        t.setStyle(TableStyle([
            ('TOPPADDING',    (0,0), (-1,-1), 4),
            ('BOTTOMPADDING', (0,0), (-1,-1), 4),
            ('LEFTPADDING',   (0,0), (0,0),   6),
            ('LINEBELOW',     (0,0), (-1,-1), 0.3, LT_GRAY),
            ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        ]))
        return t

    # ── Parse interview text into structured sections ──────────────────
    SECTIONS = [
        'Experience', 'Clinical Questions', 'Compliance',
        'Availability', 'Assessment'
    ]

    def parse_interview(text):
        result = {
            'header': {},
            'experience': {},
            'clinical': {},
            'compliance': {},
            'availability': {},
            'assessment': {},
        }
        current = 'header'
        q_num   = 0
        cur_q   = None
        cur_ans = []

        def flush_qa():
            nonlocal cur_q, cur_ans
            if cur_q is not None and cur_ans:
                result[current][cur_q] = ' '.join(cur_ans).strip()
                cur_ans = []
                cur_q   = None

        lines = [l.rstrip() for l in text.splitlines()]
        i = 0
        while i < len(lines):
            line = lines[i]
            sl   = line.strip()

            # Section detection
            if sl == 'Experience':
                flush_qa(); current = 'experience'; i += 1; continue
            elif sl == 'Clinical Questions':
                flush_qa(); current = 'clinical'; i += 1; continue
            elif sl == 'Compliance':
                flush_qa(); current = 'compliance'; i += 1; continue
            elif sl == 'Availability':
                flush_qa(); current = 'availability'; i += 1; continue
            elif sl == 'Assessment':
                flush_qa(); current = 'assessment'; i += 1; continue

            if current == 'header':
                if ':' in sl:
                    k, v = sl.split(':', 1)
                    result['header'][k.strip()] = v.strip()
            elif current in ('experience', 'clinical'):
                # Q&A blocks
                import re as _re
                m = _re.match(r'^\d+\.\s+.+$', sl)
                if m:
                    flush_qa()
                    cur_q   = sl
                    cur_ans = []
                elif cur_q is not None:
                    if sl:
                        cur_ans.append(sl)
                else:
                    pass
            elif current in ('compliance', 'availability', 'assessment'):
                if ':' in sl:
                    k, v = sl.split(':', 1)
                    result[current][k.strip()] = v.strip()

            i += 1
        flush_qa()
        return result

    parsed = parse_interview(interview_text)

    # ── Logo ──────────────────────────────────────────────────────────
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

    s1_d      = doc.get('section_1_personal_details') or {}
    full_name = _v(s1_d.get('full_name')) or 'Candidate'
    user_type = _v(doc.get('user_type')) or 'Nurse'
    emp_code  = _v(doc.get('employee_code'))

    # ── Build story ───────────────────────────────────────────────────
    buf   = _io.BytesIO()
    story = []

    # ── Document header banner ────────────────────────────────────────
    title_text = f'Completed {user_type} Interview Template'
    if logo_path:
        logo_img  = RLImage(logo_path, width=40*mm, height=40*mm*94/316)
        logo_cell = Table([[logo_img]], colWidths=[48*mm])
        logo_cell.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), WHITE),
            ('TOPPADDING',    (0,0), (-1,-1), 6),
            ('BOTTOMPADDING', (0,0), (-1,-1), 6),
            ('LEFTPADDING',   (0,0), (-1,-1), 4),
            ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
        ]))
        title_cell = Table(
            [[Paragraph(title_text, S['doc_title'])]],
            colWidths=[PAGE_W - 48*mm]
        )
        title_cell.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), NAVY),
            ('VALIGN',        (0,0), (-1,-1), 'MIDDLE'),
            ('TOPPADDING',    (0,0), (-1,-1), 14),
            ('BOTTOMPADDING', (0,0), (-1,-1), 14),
        ]))
        banner = Table([[logo_cell, title_cell]],
                       colWidths=[48*mm, PAGE_W - 48*mm])
    else:
        title_cell = Table(
            [[Paragraph(title_text, S['doc_title'])]],
            colWidths=[PAGE_W]
        )
        title_cell.setStyle(TableStyle([
            ('BACKGROUND',    (0,0), (-1,-1), NAVY),
            ('TOPPADDING',    (0,0), (-1,-1), 14),
            ('BOTTOMPADDING', (0,0), (-1,-1), 14),
        ]))
        banner = title_cell

    banner.setStyle(TableStyle([
        ('LINEBELOW',     (0,0), (-1,-1), 3, XH_GREEN),
        ('TOPPADDING',    (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
        ('LEFTPADDING',   (0,0), (-1,-1), 0),
        ('RIGHTPADDING',  (0,0), (-1,-1), 0),
    ]))
    story += [banner, sp(4)]

    # ── Header fields ─────────────────────────────────────────────────
    hdr = parsed['header']
    for key in ['Name', 'Location', 'NMBI PIN', 'Visa Status']:
        val = hdr.get(key, '')
        story += [lv_row(f'{key}:', val), sp(1)]
    story.append(sp(4))

    # ── Experience ────────────────────────────────────────────────────
    story += [sec_bar('Experience'), sp(3)]
    exp = parsed['experience']
    for q_text, answer in exp.items():
        story.append(Paragraph(q_text, S['q_label']))
        story.append(sp(1))
        story.append(answer_box(answer))
        story.append(sp(3))
    story.append(sp(2))

    # ── Clinical Questions ────────────────────────────────────────────
    story += [sec_bar('Clinical Questions'), sp(3)]
    clin = parsed['clinical']
    for q_text, answer in clin.items():
        story.append(Paragraph(q_text, S['q_label']))
        story.append(sp(1))
        story.append(answer_box(answer, BG_GREEN))
        story.append(sp(3))
    story.append(sp(2))

    # ── Compliance ────────────────────────────────────────────────────
    story += [sec_bar('Compliance'), sp(3)]
    comp = parsed['compliance']
    COMP_FIELDS = [
        'NMBI Registration', 'BLS/CPR', 'Manual Handling',
        'Garda Vetting', 'References'
    ]
    for field in COMP_FIELDS:
        val = comp.get(field, '—')
        badge_col = XH_GREEN if val.lower() == 'yes' else colors.HexColor('#CC0000')
        badge_p   = Paragraph(f'<b>{val}</b>',
                              ps('badge', fontName='Helvetica-Bold', fontSize=10,
                                 textColor=badge_col, leading=14))
        t = Table(
            [[Paragraph(f'{field}:', S['field_lbl']), badge_p]],
            colWidths=[PAGE_W * 0.55, PAGE_W * 0.45]
        )
        t.setStyle(TableStyle([
            ('TOPPADDING',    (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ('LEFTPADDING',   (0,0), (0,0),   6),
            ('LINEBELOW',     (0,0), (-1,-1), 0.3, LT_GRAY),
        ]))
        story += [t, sp(1)]
    story.append(sp(4))

    # ── Availability ──────────────────────────────────────────────────
    story += [sec_bar('Availability'), sp(3)]
    avail = parsed['availability']
    AVAIL_FIELDS = ['Preferred counties', 'Day/Night/Both', 'Earliest start date']
    for field in AVAIL_FIELDS:
        val = avail.get(field, '—')
        story += [lv_row(f'{field}:', val, col_w=60*mm), sp(1)]
    story.append(sp(4))

    # ── Assessment ────────────────────────────────────────────────────
    story += [sec_bar('Assessment'), sp(3)]
    assess = parsed['assessment']
    SCORE_FIELDS = ['Communication', 'Clinical Knowledge', 'Experience']
    for field in SCORE_FIELDS:
        val = assess.get(field, '—')
        story += [score_row(f'{field}:', val), sp(1)]

    # Suitable badge
    suitable = assess.get('Suitable', 'Yes')
    suit_col  = XH_GREEN if suitable.lower() == 'yes' else colors.HexColor('#CC0000')
    suit_t    = Table(
        [[Paragraph('Suitable:', S['field_lbl']),
          Paragraph(f'<b>{suitable}</b>',
                    ps('suit', fontName='Helvetica-Bold', fontSize=12,
                       textColor=suit_col, leading=15))]],
        colWidths=[PAGE_W * 0.45, PAGE_W * 0.55]
    )
    suit_t.setStyle(TableStyle([
        ('TOPPADDING',    (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('LEFTPADDING',   (0,0), (0,0),   6),
    ]))
    story += [suit_t, sp(6)]

    # No footer

    # Render
    pdf_doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=LM, rightMargin=RM,
        topMargin=12*mm, bottomMargin=12*mm,
    )
    pdf_doc.build(story)
    return buf.getvalue()




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

STRICT RULE — NO HALLUCINATION:
You MUST use ONLY the exact facts provided in the CANDIDATE DATA section below.
Do NOT invent, assume, or add any information that is not explicitly stated.
If a field is empty or says "None recorded", do not fabricate content for it — skip it or write only what is known.
Do not add employers, qualifications, dates, locations, certifications, or duties that are not in the data.
Do not pad sections with generic filler text disguised as personal experience.

Using ONLY the verified candidate data below, write a complete, professional, and ATS-optimised Curriculum Vitae in plain text.

Structure the CV exactly as follows (use these EXACT section headings in UPPERCASE on their own line):

PERSONAL DETAILS
PROFESSIONAL PROFILE
EDUCATION & QUALIFICATIONS
PROFESSIONAL EXPERIENCE
TRAINING & CERTIFICATIONS
KEY SKILLS
ADDITIONAL INFORMATION

Section rules:
- PERSONAL DETAILS: List each field as "Label: Value" on its own line. Only include fields that have actual values — skip blank ones.
- PROFESSIONAL PROFILE: 2 paragraphs in FIRST PERSON ("I am", "I have", "I bring"). Based strictly on the data provided. No invented qualities, no assumed skills. Only expand naturally on what is explicitly given. This must read like a genuine personal statement the candidate wrote themselves.
- EDUCATION & QUALIFICATIONS: One entry per qualification using format: Qualification Name | Institution | Year. Only list qualifications that are in the data.
- PROFESSIONAL EXPERIENCE: One block per role. Format exactly:
    Job Title: [title]
    Employer: [employer]
    Dates: [from] - [to]
    Duties:
    - [duty based only on the role type and employer — no invented specifics]
  Write 5–6 realistic duties appropriate to the job title. Do not invent employer-specific details not in the data.
- TRAINING & CERTIFICATIONS: Bullet list using only the certifications listed in the data. Do not add any others.
- KEY SKILLS: 8–10 bullet points drawn only from their role, qualifications, and certifications in the data.
- ADDITIONAL INFORMATION: Driving Licence: No | Own Transport: No | Date: [pick any date between January 2024 and December 2026, formatted as DD Month YYYY]

---
CANDIDATE DATA (use ONLY this — do not add anything else):
{data_summary}
---

Output the CV text only. No preamble, no explanation, no markdown symbols like ** or ##. Use plain text with section headings and dash bullet points.
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

        # ── Save PDF to static/cv/ folder ─────────────────────────────
        safe_name = (full_name or 'staff').replace(' ', '_').replace('/', '_')
        cv_filename  = f"AI_CV_{safe_name}_{staff_id}.pdf"
        cv_folder    = os.path.join('static', 'cv')
        os.makedirs(cv_folder, exist_ok=True)
        cv_filepath  = os.path.join(cv_folder, cv_filename)
        with open(cv_filepath, 'wb') as pdf_file:
            pdf_file.write(pdf_bytes)

        # ── Store metadata in MongoDB (no binary) ─────────────────────
        col = _ai_cvs_col()
        existing = col.find_one({"staff_id": str(doc['_id'])})
        ai_doc = {
            "staff_id":     str(doc['_id']),
            "staff_name":   full_name,
            "employee_code": emp_code,
            "cv_text":      cv_text,
            "cv_filename":  cv_filename,
            "cv_filepath":  cv_filepath,
            "generated_at": datetime.utcnow(),
        }
        if existing:
            col.update_one({"_id": existing["_id"]}, {"$set": ai_doc})
            ai_id = str(existing["_id"])
        else:
            result = col.insert_one(ai_doc)
            ai_id  = str(result.inserted_id)

        return jsonify({
            "success":      True,
            "ai_cv_id":     ai_id,
            "cv_filename":  cv_filename,
            "staff_name":   full_name,
            "message":      f"AI CV generated for {full_name}"
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/live-staffs/ai-cv/download/<ai_cv_id>')
@admin_required
def live_staff_ai_cv_download(ai_cv_id):
    """Serve the saved AI CV PDF from static/cv/."""
    try:
        rec = _ai_cvs_col().find_one({"_id": ObjectId(ai_cv_id)})
        if not rec:
            return "AI CV not found", 404

        cv_filepath = rec.get('cv_filepath', '')
        if not cv_filepath or not os.path.exists(cv_filepath):
            return "CV file not found on disk — please regenerate", 404

        name     = (rec.get('staff_name') or 'staff').replace(' ', '_')
        filename = f"AI_CV_{name}.pdf"

        with open(cv_filepath, 'rb') as f:
            pdf_bytes = f.read()

        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
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
            "cv_filename":  rec.get("cv_filename", ""),
            "generated_at": rec["generated_at"].strftime("%d %b %Y %H:%M"),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ── Build AI CV PDF from Gemini text — ATS-friendly, varied design ────


def _build_ai_cv_pdf(doc, cv_text):
    """
    4 visually distinct ATS-friendly CV designs, all black text.
    Theme chosen by md5(staff_id) % 4 — same staff always gets same design.

    Design differences (all black text, no colour):
      0 — Minimal ruled lines: thin top rule, section names LEFT-ALIGNED CAPS with underline
      1 — Centred header, section names with double-rule above and below
      2 — Left sidebar divider: thick left border for each section, Times Serif body
      3 — Boxed section headers: section name inside a simple drawn rectangle outline
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, HRFlowable,
        Image as RLImage, Table, TableStyle
    )
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY, TA_RIGHT
    import io as _io, hashlib

    BLACK     = colors.HexColor('#000000')
    NEAR_BLK  = colors.HexColor('#111111')
    DARK_GRAY = colors.HexColor('#333333')
    MID_GRAY  = colors.HexColor('#888888')
    LT_GRAY   = colors.HexColor('#CCCCCC')
    WHITE     = colors.white

    W, H = A4

    # ── Theme selection ───────────────────────────────────────────────
    id_str  = str(doc.get('_id', ''))
    theme_n = int(hashlib.md5(id_str.encode()).hexdigest(), 16) % 4

    # Per-theme layout parameters (all black text, structural differences only)
    LAYOUTS = [
        # 0 — Minimal: narrow margins, Helvetica, thin rules, left-aligned header
        {'lm': 22*mm, 'rm': 22*mm, 'tm': 18*mm, 'bm': 15*mm,
         'bf': 'Helvetica', 'bfb': 'Helvetica-Bold', 'bfi': 'Helvetica-Oblique',
         'name_size': 22, 'name_align': TA_LEFT, 'sec_size': 10,
         'body_size': 10, 'contact_align': TA_LEFT},
        # 1 — Centered classic: wide margins, Times Serif, centred header
        {'lm': 25*mm, 'rm': 25*mm, 'tm': 20*mm, 'bm': 15*mm,
         'bf': 'Times-Roman', 'bfb': 'Times-Bold', 'bfi': 'Times-Italic',
         'name_size': 24, 'name_align': TA_CENTER, 'sec_size': 11,
         'body_size': 10, 'contact_align': TA_CENTER},
        # 2 — Modern compact: narrow margins, Helvetica, smaller body text
        {'lm': 18*mm, 'rm': 18*mm, 'tm': 15*mm, 'bm': 12*mm,
         'bf': 'Helvetica', 'bfb': 'Helvetica-Bold', 'bfi': 'Helvetica-Oblique',
         'name_size': 20, 'name_align': TA_LEFT, 'sec_size': 9,
         'body_size': 9.5, 'contact_align': TA_LEFT},
        # 3 — Executive serif: generous margins, Times, larger name
        {'lm': 28*mm, 'rm': 28*mm, 'tm': 22*mm, 'bm': 18*mm,
         'bf': 'Times-Roman', 'bfb': 'Times-Bold', 'bfi': 'Times-Italic',
         'name_size': 26, 'name_align': TA_CENTER, 'sec_size': 11,
         'body_size': 10.5, 'contact_align': TA_CENTER},
    ]
    L      = LAYOUTS[theme_n]
    PAGE_W = W - L['lm'] - L['rm']
    BF     = L['bf']
    BFB    = L['bfb']
    BFI    = L['bfi']

    def ps(name, **kw):
        d = dict(fontName=BF, fontSize=L['body_size'], textColor=NEAR_BLK,
                 spaceAfter=2, leading=L['body_size'] * 1.5)
        d.update(kw)
        return ParagraphStyle(name, **d)

    S = {
        'name'    : ps('name', fontName=BFB, fontSize=L['name_size'],
                       textColor=BLACK, alignment=L['name_align'],
                       spaceAfter=3, leading=L['name_size'] * 1.3),
        'contact' : ps('contact', fontSize=L['body_size'] - 1,
                       textColor=DARK_GRAY, alignment=L['contact_align'],
                       spaceAfter=0, leading=14),
        'sec'     : ps('sec', fontName=BFB, fontSize=L['sec_size'],
                       textColor=BLACK, spaceAfter=0,
                       leading=L['sec_size'] * 1.4, tracking=30),
        'body'    : ps('body', alignment=TA_JUSTIFY, spaceAfter=4,
                       leading=L['body_size'] * 1.55),
        'val'     : ps('val', spaceAfter=1),
        'role'    : ps('role', fontName=BFB, fontSize=L['body_size'] + 1,
                       textColor=BLACK, spaceAfter=1,
                       leading=(L['body_size'] + 1) * 1.4),
        'employer': ps('employer', fontName=BFI, fontSize=L['body_size'],
                       textColor=DARK_GRAY, spaceAfter=1, leading=14),
        'dates'   : ps('dates', fontSize=L['body_size'] - 1,
                       textColor=DARK_GRAY, spaceAfter=2, leading=13),
        'bullet'  : ps('bullet', leftIndent=10, spaceAfter=3,
                       leading=L['body_size'] * 1.5),
        'qual_q'  : ps('qual_q', fontName=BFB, fontSize=L['body_size'],
                       textColor=BLACK, spaceAfter=1),
        'qual_i'  : ps('qual_i', fontName=BFI, fontSize=L['body_size'] - 1,
                       textColor=DARK_GRAY, spaceAfter=0, leading=13),
    }

    sp   = lambda n=3: Spacer(1, n * mm)
    thin = lambda: HRFlowable(width=PAGE_W, color=LT_GRAY, thickness=0.5, spaceAfter=2)

    # ── Theme-specific section heading renderer ───────────────────────
    def sec_heading(title):
        """Returns a list of flowables for the section heading."""
        if theme_n == 0:
            # Minimal: bold caps + thin rule below
            return [
                Paragraph(title, S['sec']),
                HRFlowable(width=PAGE_W, color=BLACK, thickness=0.8, spaceAfter=3),
            ]
        elif theme_n == 1:
            # Classic: thin rule above + bold caps + thin rule below (double-ruled)
            return [
                HRFlowable(width=PAGE_W, color=BLACK, thickness=0.4, spaceAfter=2),
                Paragraph(title, S['sec']),
                HRFlowable(width=PAGE_W, color=BLACK, thickness=1.2, spaceAfter=4),
            ]
        elif theme_n == 2:
            # Modern: section name with a thick short left accent rule as table
            # Achieved via a single-row table: thick left border cell + text
            cell_p = Paragraph(f'  {title}', S['sec'])
            t = Table([[cell_p]], colWidths=[PAGE_W])
            t.setStyle(TableStyle([
                ('LINEBEFORE',    (0,0), (0,-1), 3.5, BLACK),
                ('LINEBELOW',     (0,0), (-1,-1), 0.4, LT_GRAY),
                ('TOPPADDING',    (0,0), (-1,-1), 3),
                ('BOTTOMPADDING', (0,0), (-1,-1), 4),
                ('LEFTPADDING',   (0,0), (-1,-1), 6),
            ]))
            return [t]
        else:
            # Executive: section name inside a rectangle outline box
            cell_p = Paragraph(f'  {title}  ', S['sec'])
            t = Table([[cell_p]], colWidths=[PAGE_W])
            t.setStyle(TableStyle([
                ('BOX',           (0,0), (-1,-1), 0.8, BLACK),
                ('TOPPADDING',    (0,0), (-1,-1), 4),
                ('BOTTOMPADDING', (0,0), (-1,-1), 4),
                ('LEFTPADDING',   (0,0), (-1,-1), 8),
            ]))
            return [t]

    def bullet_p(text):
        clean = text.lstrip('- •	').strip()
        if not clean:
            return None
        return Paragraph(f'• {clean}', S['bullet'])

    # ── Parse sections from Gemini text ──────────────────────────────
    HEADINGS = [
        'PERSONAL DETAILS', 'PROFESSIONAL PROFILE',
        'EDUCATION & QUALIFICATIONS', 'PROFESSIONAL EXPERIENCE',
        'TRAINING & CERTIFICATIONS', 'KEY SKILLS', 'ADDITIONAL INFORMATION',
    ]
    sections = {}
    current  = '__pre__'
    sections[current] = []
    for line in cv_text.splitlines():
        matched = next((h for h in HEADINGS if line.strip().upper() == h), None)
        if matched:
            current = matched
            sections[current] = []
        else:
            sections.setdefault(current, []).append(line)

    # ── Candidate info from DB (header always uses real data) ─────────
    s1_d      = doc.get('section_1_personal_details') or {}
    full_name = _v(s1_d.get('full_name')) or 'Candidate'
    mobile    = _v(s1_d.get('mobile_number'))
    email     = _v(doc.get('email'))
    address   = _v(s1_d.get('address'))

    # ── Logo ──────────────────────────────────────────────────────────
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

    # Header
    if logo_path:
        logo_w   = 30*mm
        logo_img = RLImage(logo_path, width=logo_w, height=logo_w * 94/316)
        if L['name_align'] == TA_CENTER:
            # Centred themes: logo centred above name
            story.append(logo_img)
            story.append(sp(2))
        else:
            # Left-aligned themes: logo right-aligned via table
            spacer_cell = Paragraph('', S['body'])
            logo_cell   = logo_img
            hdr_t = Table([[spacer_cell, logo_cell]],
                          colWidths=[PAGE_W - logo_w - 2*mm, logo_w + 2*mm])
            hdr_t.setStyle(TableStyle([
                ('VALIGN',        (0,0), (-1,-1), 'TOP'),
                ('TOPPADDING',    (0,0), (-1,-1), 0),
                ('BOTTOMPADDING', (0,0), (-1,-1), 0),
                ('LEFTPADDING',   (0,0), (-1,-1), 0),
                ('RIGHTPADDING',  (0,0), (-1,-1), 0),
            ]))
            story.append(hdr_t)

    story.append(Paragraph(full_name, S['name']))
    contact_parts = [p for p in [mobile, email, address] if p]
    if contact_parts:
        sep = '   |   ' if L['contact_align'] == TA_CENTER else '  •  '
        story.append(Paragraph(sep.join(contact_parts), S['contact']))

    # Theme-specific header rule
    story.append(sp(3))
    if theme_n == 0:
        story.append(HRFlowable(width=PAGE_W, color=BLACK, thickness=1.5, spaceAfter=0))
        story.append(HRFlowable(width=PAGE_W, color=BLACK, thickness=0.4, spaceAfter=4))
    elif theme_n == 1:
        story.append(HRFlowable(width=PAGE_W, color=BLACK, thickness=1.2, spaceAfter=4))
    elif theme_n == 2:
        story.append(HRFlowable(width=PAGE_W, color=LT_GRAY, thickness=0.5, spaceAfter=4))
    else:
        story.append(HRFlowable(width=PAGE_W, color=BLACK, thickness=0.6, spaceAfter=2))
        story.append(HRFlowable(width=PAGE_W, color=BLACK, thickness=0.6, spaceAfter=4))
    story.append(sp(2))

    # ── Render sections ───────────────────────────────────────────────
    for heading in HEADINGS:
        lines = [l for l in sections.get(heading, []) if l.strip()]
        if not lines:
            continue

        story += sec_heading(heading)
        story.append(sp(2))

        if heading == 'PERSONAL DETAILS':
            for line in lines:
                if ':' in line:
                    parts = line.split(':', 1)
                    lbl_t = parts[0].strip() + ':'
                    val_t = parts[1].strip()
                    if val_t:
                        story.append(Paragraph(f'<b>{lbl_t}</b> {val_t}', S['val']))
                        story.append(sp(1))
            story.append(sp(3))

        elif heading == 'PROFESSIONAL PROFILE':
            pb = []
            for line in lines:
                if line.strip() == '':
                    if pb:
                        story.append(Paragraph(' '.join(pb), S['body']))
                        story.append(sp(2))
                        pb = []
                else:
                    pb.append(line.strip())
            if pb:
                story.append(Paragraph(' '.join(pb), S['body']))
            story.append(sp(4))

        elif heading == 'EDUCATION & QUALIFICATIONS':
            for line in lines:
                s = line.strip().lstrip('- ').strip()
                if not s:
                    continue
                parts = [p.strip() for p in s.split('|')]
                qual  = parts[0] if parts else s
                inst  = parts[1] if len(parts) > 1 else ''
                year  = parts[2] if len(parts) > 2 else ''
                yr_txt = f' ({year})' if year else ''
                story.append(Paragraph(f'<b>{qual}</b>{yr_txt}', S['qual_q']))
                if inst:
                    story.append(Paragraph(inst, S['qual_i']))
                story += [sp(2), thin(), sp(2)]
            story.append(sp(3))

        elif heading == 'PROFESSIONAL EXPERIENCE':
            roles = []
            cur   = []
            for line in lines:
                if line.strip().lower().startswith('job title:') and cur:
                    roles.append(cur)
                    cur = [line]
                else:
                    cur.append(line)
            if cur:
                roles.append(cur)

            for ri, role_lines in enumerate(roles):
                job_title = emp_name = dates_str = ''
                duties    = []

                for rl in role_lines:
                    sl    = rl.strip()
                    sl_lo = sl.lower()
                    if not sl:
                        continue
                    if sl_lo.startswith('job title:'):
                        job_title = sl.split(':', 1)[1].strip()
                    elif sl_lo.startswith('employer:'):
                        emp_name = sl.split(':', 1)[1].strip()
                    elif sl_lo.startswith('dates:') or sl_lo.startswith('period:'):
                        dates_str = sl.split(':', 1)[1].strip()
                    elif sl_lo.startswith('duties') or sl_lo.startswith('responsibilities'):
                        pass  # label line, skip
                    elif sl.startswith('-') or sl.startswith('•'):
                        duties.append(sl.lstrip('- •').strip())

                if not job_title and not emp_name:
                    continue

                # Theme 1 & 3: role title + dates on same line via table
                if theme_n in (1, 3) and dates_str:
                    role_p  = Paragraph(f'<b>{job_title}</b>', S['role'])
                    dates_p = Paragraph(dates_str, S['dates'])
                    rt = Table([[role_p, dates_p]],
                               colWidths=[PAGE_W * 0.65, PAGE_W * 0.35])
                    rt.setStyle(TableStyle([
                        ('VALIGN',        (0,0), (-1,-1), 'BOTTOM'),
                        ('TOPPADDING',    (0,0), (-1,-1), 0),
                        ('BOTTOMPADDING', (0,0), (-1,-1), 2),
                        ('LEFTPADDING',   (0,0), (-1,-1), 0),
                        ('RIGHTPADDING',  (0,0), (-1,-1), 0),
                        ('ALIGN',         (1,0), (1,-1), 'RIGHT'),
                    ]))
                    story.append(rt)
                else:
                    story.append(Paragraph(job_title or 'Role', S['role']))
                    if dates_str:
                        story.append(Paragraph(dates_str, S['dates']))

                if emp_name:
                    story.append(Paragraph(emp_name, S['employer']))
                story.append(sp(2))
                for d in duties:
                    if d:
                        bp = bullet_p(d)
                        if bp:
                            story.append(bp)
                story.append(sp(3))
                if ri < len(roles) - 1:
                    story.append(thin())
                    story.append(sp(2))
            story.append(sp(2))

        elif heading in ('TRAINING & CERTIFICATIONS', 'KEY SKILLS'):
            # Theme 2 & 0: 2-column bullets for skills
            if theme_n in (0, 2):
                bullet_items = []
                for line in lines:
                    bp = bullet_p(line.strip())
                    if bp:
                        bullet_items.append(bp)
                # Pair into 2 columns
                pairs = []
                for i in range(0, len(bullet_items), 2):
                    left  = bullet_items[i]
                    right = bullet_items[i+1] if i+1 < len(bullet_items) else Paragraph('', S['body'])
                    pairs.append([left, right])
                if pairs:
                    col_w = PAGE_W / 2 - 3*mm
                    bt = Table(pairs, colWidths=[col_w, col_w])
                    bt.setStyle(TableStyle([
                        ('VALIGN',        (0,0), (-1,-1), 'TOP'),
                        ('TOPPADDING',    (0,0), (-1,-1), 1),
                        ('BOTTOMPADDING', (0,0), (-1,-1), 1),
                        ('LEFTPADDING',   (0,0), (-1,-1), 0),
                        ('RIGHTPADDING',  (0,0), (-1,-1), 4),
                    ]))
                    story.append(bt)
            else:
                for line in lines:
                    bp = bullet_p(line.strip())
                    if bp:
                        story.append(bp)
            story.append(sp(4))

        elif heading == 'ADDITIONAL INFORMATION':
            for line in lines:
                s = line.strip()
                if not s:
                    continue
                if s.lower().startswith('reference'):
                    continue
                if ':' in s:
                    parts = s.split(':', 1)
                    story.append(
                        Paragraph(f'<b>{parts[0].strip()}:</b> {parts[1].strip()}', S['val'])
                    )
                    story.append(sp(1))
                else:
                    story.append(Paragraph(s, S['body']))
            story.append(sp(4))

    pdf_doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=L['lm'], rightMargin=L['rm'],
        topMargin=L['tm'],  bottomMargin=L['bm'],
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
    # Generate a random date between 01 Jan 2024 and 31 Dec 2026
    import random as _rand
    from datetime import date as _date, timedelta as _td
    _d_start = _date(2024, 1, 1); _d_end = _date(2026, 12, 31)
    _rand_date = _d_start + _td(days=_rand.randint(0, (_d_end - _d_start).days))
    _cv_date   = _rand_date.strftime('%d %B %Y')
    for label, value in [
        ('Driving Licence:', 'No'),
        ('Own Transport:',   'No'),
        ('Date:',            _cv_date),
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
