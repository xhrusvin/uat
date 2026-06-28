"""
live_staffs_point_scale.py
══════════════════════════
Independent blueprint for generating "Verification of Service – Agency Staff"
(Point Scale Document) for Healthcare Assistant staff.

Registers on admin_bp — import in admin/__init__.py:
    from . import live_staffs_point_scale

Routes:
    GET  /live-staffs/cron/generate-point-scale   — cron (one staff per call)
    GET  /live-staffs/ai-cv/<staff_id>/point-scale/download — download generated doc
    GET  /live-staffs/export/point-scale-xlsx      — Excel export of all records
"""

from flask import request, jsonify, Response
from bson import ObjectId
from datetime import datetime, date
import os
import re
import io
import json
import threading

from database import db
from . import admin_bp
from admin.views import admin_required


# ── Helpers ───────────────────────────────────────────────────────────

def _v(val):
    if val is None: return ''
    return str(val).strip()

def _staffs_col():
    from flask import current_app
    return current_app.db.live_staffs

def _ps_col():
    """point_scale documents collection."""
    return db.live_staff_point_scale

def _gcs_upload(blob_name, data_bytes, content_type='application/octet-stream'):
    from admin.live_staffs import _gcs_upload as _f
    return _f(blob_name, data_bytes, content_type)

def _gcs_download(blob_name):
    from admin.live_staffs import _gcs_download as _f
    return _f(blob_name)


# ── Employment entry parser ───────────────────────────────────────────

def _parse_employment_from_cv(extracted_cv, user_type=''):
    """
    Parse employment history from extracted_cv text.
    Returns list of dicts: {post, employer, from_date, to_date, verified}
    """
    if not extracted_cv:
        return []

    entries  = []
    lines    = extracted_cv.split('\n')
    in_exp   = False
    exp_stop = {
        'education', 'qualifications', 'training', 'certifications',
        'key skills', 'skills', 'references', 'declaration',
        'additional information', 'employment eligibility',
        'professional profile', 'profile', 'summary',
    }
    exp_headers = {
        'professional experience', 'work experience', 'employment history',
        'employment', 'experience', 'career history', 'work history',
        'positions held',
    }

    exp_lines = []
    for line in lines:
        l = line.strip()
        ll = l.lower()
        if any(ll.startswith(h) for h in exp_headers):
            in_exp = True
            continue
        if in_exp and l and any(ll.startswith(h) for h in exp_stop):
            break
        if in_exp and l:
            exp_lines.append(l)

    if not exp_lines:
        # Fallback: scan whole CV for date patterns
        exp_lines = [l.strip() for l in lines if l.strip() and
                     re.search(r'\d{2}/\d{2}/\d{4}|\b(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d{4}|\d{4}\s*[-–]\s*(\d{4}|present)', l, re.I)]

    # Parse each line into an entry
    date_pattern = re.compile(
        r'(\d{2}/\d{2}/\d{4}|\d{1,2}\s+\w+\s+\d{4}|'
        r'\w+\s+\d{4}|\d{4})'
        r'\s*[-–—to]+\s*'
        r'(\d{2}/\d{2}/\d{4}|\d{1,2}\s+\w+\s+\d{4}|'
        r'\w+\s+\d{4}|\d{4}|present|current|ongoing)',
        re.I
    )

    for line in exp_lines:
        dm = date_pattern.search(line)
        if dm:
            from_str = dm.group(1).strip()
            to_str   = dm.group(2).strip()
            # Extract employer/post from line (before or after dates)
            pre  = line[:dm.start()].strip().strip(',-')
            post_text = line[dm.end():].strip().strip(',-')
            employer  = pre or post_text or ''

            # Remove date from employer string
            employer = re.sub(r'\d{2}/\d{2}/\d{4}', '', employer).strip().strip(',-')

            entries.append({
                'post':      _v(user_type) or 'Healthcare Assistant',
                'employer':  employer,
                'from_str':  from_str,
                'to_str':    to_str,
            })

    return entries


def _parse_date_flex(s):
    """Parse a date string flexibly. Returns date or None."""
    s = _v(s).strip()
    if not s or s.lower() in ('present', 'current', 'ongoing', 'now'):
        return None  # None = Present

    fmts = [
        '%d/%m/%Y', '%m/%Y', '%B %Y', '%b %Y',
        '%d %B %Y', '%d %b %Y', '%Y-%m-%d', '%Y',
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    return None


def _calc_duration(from_date, to_date):
    """Calculate years, months, days between two dates."""
    if from_date is None:
        return 0, 0, 0
    end = to_date if to_date else date.today()
    if end < from_date:
        return 0, 0, 0

    years  = end.year  - from_date.year
    months = end.month - from_date.month
    days   = end.day   - from_date.day

    if days < 0:
        months -= 1
        # days in previous month
        from calendar import monthrange
        prev_month = end.month - 1 if end.month > 1 else 12
        prev_year  = end.year if end.month > 1 else end.year - 1
        days += monthrange(prev_year, prev_month)[1]

    if months < 0:
        years  -= 1
        months += 12

    return max(0, years), max(0, months), max(0, days)


def _total_service(rows):
    """Sum years, months, days across all rows, normalising overflow."""
    total_days = 0
    for r in rows:
        total_days += r['years'] * 365 + r['months'] * 30 + r['days']

    years  = total_days // 365
    remain = total_days  % 365
    months = remain // 30
    days   = remain  % 30
    return years, months, days


# ── DOCX builder ─────────────────────────────────────────────────────

def _build_point_scale_docx(staff_doc, rows):
    """
    Build "Verification of Service – Agency Staff" DOCX.
    rows: list of {post, employer, from_str, to_str, from_date, to_date, years, months, days}
    """
    from docx import Document
    from docx.shared import Pt, Cm, RGBColor, Inches
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.enum.table import WD_ALIGN_VERTICAL
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    NAVY  = RGBColor(0x1B, 0x3A, 0x6B)
    GREEN = RGBColor(0x2E, 0x9E, 0x44)
    BLACK = RGBColor(0x00, 0x00, 0x00)
    GRAY  = RGBColor(0x55, 0x55, 0x55)

    s1        = staff_doc.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff_doc.get('email') or s1.get('email_address') or '')
    user_type = _v(staff_doc.get('user_type') or 'Healthcare Assistant')

    d = Document()
    for sec in d.sections:
        sec.top_margin    = Cm(2.0)
        sec.bottom_margin = Cm(2.0)
        sec.left_margin   = Cm(2.5)
        sec.right_margin  = Cm(2.5)

    d.styles['Normal'].font.name = 'Calibri'
    d.styles['Normal'].font.size = Pt(11)

    def sp(pts):
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(0)
        p.paragraph_format.space_after  = Pt(0)
        p.paragraph_format.line_spacing = Pt(pts)

    def _set_cell_bg(cell, hex_color):
        tc   = cell._tc
        tcPr = tc.get_or_add_tcPr()
        shd  = OxmlElement('w:shd')
        shd.set(qn('w:val'),   'clear')
        shd.set(qn('w:color'), 'auto')
        shd.set(qn('w:fill'),  hex_color)
        tcPr.append(shd)

    def _set_cell_border(cell, color='CCCCCC'):
        tc   = cell._tc
        tcPr = tc.get_or_add_tcPr()
        tcBorders = OxmlElement('w:tcBorders')
        for side in ('top','left','bottom','right'):
            border = OxmlElement(f'w:{side}')
            border.set(qn('w:val'),   'single')
            border.set(qn('w:sz'),    '4')
            border.set(qn('w:color'), color)
            tcBorders.append(border)
        tcPr.append(tcBorders)

    def _add_run(para, text, bold=False, size=11, color=None, italic=False):
        run = para.add_run(text)
        run.bold        = bold
        run.italic      = italic
        run.font.name   = 'Calibri'
        run.font.size   = Pt(size)
        run.font.color.rgb = color if color else BLACK
        return run

    # ── Logo ─────────────────────────────────────────────────────────
    try:
        logo_path = os.path.join(os.path.dirname(__file__), '..', 'static', 'img', 'xpress_logo.png')
        if not os.path.exists(logo_path):
            # Try alternate paths
            for p in ['static/img/logo.png', 'static/logo.png', 'static/img/xpress_logo.png']:
                if os.path.exists(p):
                    logo_path = p
                    break
            else:
                logo_path = None
        if logo_path and os.path.exists(logo_path):
            p_logo = d.add_paragraph()
            p_logo.paragraph_format.space_before = Pt(0)
            p_logo.paragraph_format.space_after  = Pt(4)
            run_logo = p_logo.add_run()
            run_logo.add_picture(logo_path, width=Inches(1.5))
    except Exception:
        # Logo not available — write text logo
        p_logo = d.add_paragraph()
        r_logo = p_logo.add_run('XPRESS HEALTH')
        r_logo.bold = True
        r_logo.font.size = Pt(14)
        r_logo.font.color.rgb = NAVY
        r_logo.font.name = 'Calibri'

    sp(8)

    # ── Main Title ────────────────────────────────────────────────────
    p_title = d.add_paragraph()
    p_title.paragraph_format.space_before = Pt(4)
    p_title.paragraph_format.space_after  = Pt(4)
    r_title = _add_run(p_title, 'Verification of Service \u2013 Agency Staff',
                       bold=False, size=26, color=NAVY)
    # Underline via border
    from docx.oxml.ns import qn as _qn
    pPr  = p_title._p.get_or_add_pPr()
    pBdr = OxmlElement('w:pBdr')
    bot  = OxmlElement('w:bottom')
    bot.set(_qn('w:val'),   'single')
    bot.set(_qn('w:sz'),    '6')
    bot.set(_qn('w:color'), '1B3A6B')
    pBdr.append(bot)
    pPr.append(pBdr)

    sp(14)

    # ── Staff Details section ─────────────────────────────────────────
    p_sd = d.add_paragraph()
    _add_run(p_sd, 'Staff Details', bold=True, size=12, color=GREEN)

    def field_line(label, value):
        p = d.add_paragraph()
        p.paragraph_format.space_before = Pt(2)
        p.paragraph_format.space_after  = Pt(2)
        _add_run(p, f'{label}: ', bold=False, size=11, color=BLACK)
        _add_run(p, value, bold=False, size=11, color=BLACK)

    field_line('Full Name',       full_name)
    field_line('Role / Designation', user_type)
    field_line('Contact Email',   email)

    sp(12)

    # ── Service Details section ───────────────────────────────────────
    p_srv = d.add_paragraph()
    _add_run(p_srv, 'Service Details', bold=True, size=12, color=GREEN)

    sp(4)

    # ── Table ─────────────────────────────────────────────────────────
    headers = ['Post', 'HSE Location\n/ Employer', 'From Date', 'To Date',
               'Years', 'Months', 'Days', 'Verified']
    col_widths_cm = [3.0, 4.0, 3.0, 3.0, 1.4, 1.6, 1.4, 1.8]

    tbl = d.add_table(rows=1 + len(rows), cols=len(headers))
    tbl.style = 'Table Grid'

    # Header row
    hdr_row = tbl.rows[0]
    for ci, (hdr, w) in enumerate(zip(headers, col_widths_cm)):
        cell = hdr_row.cells[ci]
        cell.width = Cm(w)
        _set_cell_bg(cell, '1B3A6B')
        _set_cell_border(cell, '1B3A6B')
        p = cell.paragraphs[0]
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        r = _add_run(p, hdr, bold=True, size=10, color=RGBColor(0xFF,0xFF,0xFF))

    # Data rows
    for ri, row in enumerate(rows):
        tr = tbl.rows[ri + 1]
        row_data = [
            row.get('post', user_type),
            row.get('employer', ''),
            row.get('from_str', ''),
            row.get('to_str', '').capitalize() if row.get('to_str','').lower() in ('present','current','ongoing') else row.get('to_str',''),
            str(row.get('years', 0)),
            str(row.get('months', 0)),
            str(row.get('days', 0)),
            'Yes',
        ]
        bg = 'EFF6FF' if ri % 2 == 0 else 'FFFFFF'
        for ci, (val, w) in enumerate(zip(row_data, col_widths_cm)):
            cell = tr.cells[ci]
            cell.width = Cm(w)
            _set_cell_bg(cell, bg)
            _set_cell_border(cell, 'CCCCCC')
            p = cell.paragraphs[0]
            p.alignment = WD_ALIGN_PARAGRAPH.CENTER if ci >= 2 else WD_ALIGN_PARAGRAPH.LEFT
            _add_run(p, val, size=10)

    sp(12)

    # ── Total Eligible Service ────────────────────────────────────────
    tot_y, tot_m, tot_d = _total_service(rows)
    p_tot = d.add_paragraph()
    p_tot.paragraph_format.space_before = Pt(4)
    p_tot.paragraph_format.space_after  = Pt(12)
    _add_run(p_tot,
             f'Total Eligible Service for Incremental Credit: '
             f'{tot_y} Year{"s" if tot_y!=1 else ""}, '
             f'{tot_m} Month{"s" if tot_m!=1 else ""}, '
             f'{tot_d} Day{"s" if tot_d!=1 else ""}',
             bold=False, size=11)

    # ── Declaration ───────────────────────────────────────────────────
    p_decl = d.add_paragraph()
    p_decl.paragraph_format.space_before = Pt(4)
    p_decl.paragraph_format.space_after  = Pt(16)
    _add_run(p_decl,
             'I hereby confirm that the above details are accurate and verified as per agency records.',
             size=11)

    # ── Signatory ─────────────────────────────────────────────────────
    field_line('Authorised Signatory', 'Betsy Daniel')
    field_line('Designation',          'Recruitment Head')

    buf = io.BytesIO()
    d.save(buf)
    return buf.getvalue()


# ── Extract employment rows from staff doc ────────────────────────────

def _get_employment_rows(staff_doc):
    """
    Get employment rows from live_staffs document.
    Priority: section_5 entries → extracted_cv parsing.
    """
    user_type = _v(staff_doc.get('user_type') or 'Healthcare Assistant')
    rows = []

    # 1. Try section_5 employment history entries
    s5      = staff_doc.get('section_5_employment_history') or {}
    entries = [e for e in (s5.get('entries') or []) if e.get('employer') or e.get('position')]

    if entries:
        for e in entries:
            from_str = _v(e.get('from') or '')
            to_str   = _v(e.get('to') or 'Present')
            from_d   = _parse_date_flex(from_str)
            to_d     = _parse_date_flex(to_str)
            y, m, dys = _calc_duration(from_d, to_d)
            rows.append({
                'post':     _v(e.get('position') or user_type),
                'employer': _v(e.get('employer') or ''),
                'from_str': from_d.strftime('%d/%m/%Y') if from_d else from_str,
                'to_str':   to_d.strftime('%d/%m/%Y') if to_d else 'Present',
                'years':    y,
                'months':   m,
                'days':     dys,
            })
        return rows

    # 2. Fall back to extracted_cv parsing
    extracted_cv = _v(staff_doc.get('extracted_cv') or '')
    if extracted_cv:
        parsed = _parse_employment_from_cv(extracted_cv, user_type)
        for e in parsed:
            from_d  = _parse_date_flex(e['from_str'])
            to_d    = _parse_date_flex(e['to_str'])
            y, m, dys = _calc_duration(from_d, to_d)
            rows.append({
                'post':     e['post'],
                'employer': e['employer'],
                'from_str': from_d.strftime('%d/%m/%Y') if from_d else e['from_str'],
                'to_str':   to_d.strftime('%d/%m/%Y') if to_d else 'Present',
                'years':    y,
                'months':   m,
                'days':     dys,
            })

    return rows


# ── Cron: Generate Point Scale document ──────────────────────────────

@admin_bp.route('/live-staffs/cron/generate-point-scale', methods=['GET', 'POST'])
def live_staff_cron_generate_point_scale():
    """
    Cron — generates Verification of Service document for ONE HCA staff per call.
    Only processes staff where user_type contains 'Healthcare Assistant'.

    Auth: cron_key query param or X-Cron-Key header.
    """
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    ps_col     = _ps_col()
    staffs_col = _staffs_col()

    # Find staff already processed
    existing_ids = set(
        str(r['staff_id'])
        for r in ps_col.find({}, {"staff_id": 1})
        if r.get('staff_id')
    )

    # Find next HCA staff without a point scale document
    pending_query = {
        "$and": [
            {"user_type": {"$regex": "Healthcare Assistant", "$options": "i"}},
            {"_id": {"$nin": [ObjectId(i) for i in existing_ids if len(i) == 24]}},
        ]
    }
    remaining_total = staffs_col.count_documents(pending_query)
    staff = staffs_col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All Healthcare Assistant staff point scale documents generated.",
            "remaining_count": 0,
        })

    staff_id  = str(staff['_id'])
    s1        = staff.get('section_1_personal_details') or {}
    full_name = _v(s1.get('full_name') or '')
    email     = _v(staff.get('email') or s1.get('email_address') or '')

    # Mark as processing immediately to avoid duplicate runs
    ps_col.insert_one({
        "staff_id":    staff_id,
        "staff_name":  full_name,
        "status":      "processing",
        "created_at":  datetime.utcnow(),
    })

    def _do_generate():
        try:
            rows = _get_employment_rows(staff)

            if not rows:
                ps_col.update_one(
                    {"staff_id": staff_id},
                    {"$set": {
                        "status":  "no_employment_data",
                        "note":    "No employment history found in section_5 or extracted_cv",
                        "updated_at": datetime.utcnow(),
                    }}
                )
                return

            docx_bytes = _build_point_scale_docx(staff, rows)
            safe_name  = (full_name or 'staff').replace(' ', '_').replace('/', '_')
            filename   = f"PointScale_{safe_name}.docx"
            gcs_blob   = f"point_scale/{filename}"

            _gcs_upload(
                gcs_blob, docx_bytes,
                content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            )

            tot_y, tot_m, tot_d = _total_service(rows)

            ps_col.update_one(
                {"staff_id": staff_id},
                {"$set": {
                    "staff_name":    full_name,
                    "email":         email,
                    "user_type":     _v(staff.get('user_type')),
                    "filename":      filename,
                    "gcs_blob":      gcs_blob,
                    "rows":          rows,
                    "total_years":   tot_y,
                    "total_months":  tot_m,
                    "total_days":    tot_d,
                    "status":        "generated",
                    "generated_at":  datetime.utcnow(),
                }}
            )

        except Exception as e:
            ps_col.update_one(
                {"staff_id": staff_id},
                {"$set": {
                    "status": "error",
                    "error":  str(e),
                    "updated_at": datetime.utcnow(),
                }}
            )

    threading.Thread(target=_do_generate, daemon=True).start()

    return jsonify({
        "success":         True,
        "staff_id":        staff_id,
        "staff_name":      full_name,
        "email":           email,
        "remaining_count": max(0, remaining_total - 1),
        "message":         f"Point scale generation started for {full_name} — {max(0, remaining_total - 1)} remaining.",
    })


# ── Download generated document ───────────────────────────────────────

@admin_bp.route('/live-staffs/point-scale/download/<staff_id>')
@admin_required
def live_staff_point_scale_download(staff_id):
    """Download the generated Verification of Service document."""
    try:
        rec = _ps_col().find_one({"staff_id": staff_id})
        if not rec:
            # Try generating on-demand
            staff_doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
            if not staff_doc:
                return jsonify({"success": False, "error": "Staff not found"}), 404
            rows = _get_employment_rows(staff_doc)
            if not rows:
                return jsonify({"success": False, "error": "No employment data"}), 404
            docx_bytes = _build_point_scale_docx(staff_doc, rows)
            s1         = staff_doc.get('section_1_personal_details') or {}
            full_name  = _v(s1.get('full_name') or 'staff')
            filename   = f"PointScale_{full_name.replace(' ','_')}.docx"
        else:
            if rec.get('status') != 'generated' or not rec.get('gcs_blob'):
                return jsonify({"success": False,
                                "error": f"Document not ready (status: {rec.get('status')})"}), 404
            docx_bytes = _gcs_download(rec['gcs_blob'])
            filename   = rec.get('filename', 'PointScale.docx')

        return Response(
            docx_bytes,
            mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── Regenerate on-demand for a single staff ───────────────────────────

@admin_bp.route('/live-staffs/point-scale/generate/<staff_id>', methods=['POST'])
@admin_required
def live_staff_point_scale_generate(staff_id):
    """Regenerate point scale document for a specific staff member."""
    try:
        staff_doc = _staffs_col().find_one({"_id": ObjectId(staff_id)})
        if not staff_doc:
            return jsonify({"success": False, "error": "Staff not found"}), 404

        rows = _get_employment_rows(staff_doc)
        if not rows:
            return jsonify({"success": False, "error": "No employment data found"}), 400

        s1        = staff_doc.get('section_1_personal_details') or {}
        full_name = _v(s1.get('full_name') or 'staff')
        email     = _v(staff_doc.get('email') or s1.get('email_address') or '')

        docx_bytes = _build_point_scale_docx(staff_doc, rows)
        safe_name  = full_name.replace(' ', '_').replace('/', '_')
        filename   = f"PointScale_{safe_name}.docx"
        gcs_blob   = f"point_scale/{filename}"

        _gcs_upload(gcs_blob, docx_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        tot_y, tot_m, tot_d = _total_service(rows)

        _ps_col().update_one(
            {"staff_id": staff_id},
            {"$set": {
                "staff_name":   full_name,
                "email":        email,
                "user_type":    _v(staff_doc.get('user_type')),
                "filename":     filename,
                "gcs_blob":     gcs_blob,
                "rows":         rows,
                "total_years":  tot_y,
                "total_months": tot_m,
                "total_days":   tot_d,
                "status":       "generated",
                "generated_at": datetime.utcnow(),
            }},
            upsert=True
        )

        return jsonify({
            "success":     True,
            "staff_name":  full_name,
            "filename":    filename,
            "total":       f"{tot_y} Years, {tot_m} Months, {tot_d} Days",
            "row_count":   len(rows),
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── Excel export ──────────────────────────────────────────────────────

@admin_bp.route('/live-staffs/export/point-scale-xlsx')
@admin_required
def live_staff_export_point_scale_xlsx():
    """Export all generated point scale documents to Excel."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

        docs = list(_ps_col().find({}))
        docs.sort(key=lambda d: _v(d.get('staff_name') or '').lower())

        NAVY  = '1B3A6B'; GREEN = '2E9E44'; WHITE = 'FFFFFF'
        ALT   = 'EFF6FF'; RED   = 'FFDDDD'; WARN  = 'FFF3CD'

        h_font  = Font(name='Calibri', bold=True, color=WHITE, size=10)
        h_fill  = PatternFill('solid', start_color=NAVY, end_color=NAVY)
        h_align = Alignment(horizontal='center', vertical='center')
        b_font  = Font(name='Calibri', size=10)
        l_align = Alignment(horizontal='left',   vertical='center')
        c_align = Alignment(horizontal='center', vertical='center')
        thin    = Side(style='thin', color='CCCCCC')
        border  = Border(left=thin, right=thin, top=thin, bottom=thin)
        g_bot   = Border(left=thin, right=thin, top=thin,
                         bottom=Side(style='medium', color=GREEN))

        wb = Workbook()
        ws = wb.active
        ws.title = 'Point Scale Documents'

        headers    = ['Sno', 'Staff Name', 'Email', 'User Type',
                      'Total Years', 'Total Months', 'Total Days',
                      'Total Service', 'Status', 'Generated At']
        col_widths = [5, 30, 36, 24, 12, 13, 10, 28, 14, 20]

        for ci, (hdr, width) in enumerate(zip(headers, col_widths), start=1):
            cell = ws.cell(row=1, column=ci, value=hdr)
            cell.font = h_font; cell.fill = h_fill
            cell.alignment = h_align; cell.border = g_bot
            ws.column_dimensions[cell.column_letter].width = width
        ws.row_dimensions[1].height = 24
        ws.freeze_panes = 'A2'
        ws.auto_filter.ref = f'A1:J{len(docs)+1}'

        for ri, doc in enumerate(docs, start=2):
            tot_y = doc.get('total_years',  '')
            tot_m = doc.get('total_months', '')
            tot_d = doc.get('total_days',   '')
            status = _v(doc.get('status') or '')
            gen_at = doc.get('generated_at')
            gen_str = gen_at.strftime('%d %b %Y %H:%M') if gen_at else ''

            total_str = (f"{tot_y} Yr{'s' if tot_y!=1 else ''}, "
                         f"{tot_m} Mo, {tot_d} D") if tot_y != '' else ''

            if status == 'generated':
                row_fill = None
            elif status == 'error':
                row_fill = PatternFill('solid', start_color=RED, end_color=RED)
            elif status == 'no_employment_data':
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)
            else:
                row_fill = PatternFill('solid', start_color=WARN, end_color=WARN)

            alt_fill = PatternFill('solid', start_color=ALT, end_color=ALT) \
                       if ri % 2 == 0 and not row_fill else None

            row_vals = [
                ri-1,
                _v(doc.get('staff_name') or ''),
                _v(doc.get('email') or ''),
                _v(doc.get('user_type') or ''),
                tot_y, tot_m, tot_d, total_str, status, gen_str,
            ]
            aligns = [c_align, l_align, l_align, l_align,
                      c_align, c_align, c_align, l_align, c_align, c_align]

            for ci, (val, align) in enumerate(zip(row_vals, aligns), start=1):
                cell = ws.cell(row=ri, column=ci, value=val)
                cell.font = b_font; cell.alignment = align
                cell.border = border
                cell.fill = row_fill or alt_fill or PatternFill()

            ws.row_dimensions[ri].height = 17

        ws.cell(row=len(docs)+2, column=1,
                value=f'Total: {len(docs)}').font = Font(name='Calibri', bold=True, size=9)

        buf = io.BytesIO()
        wb.save(buf)
        return Response(
            buf.getvalue(),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition":
                     f'attachment; filename="point_scale_{datetime.utcnow().strftime("%Y%m%d")}.xlsx"'}
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
