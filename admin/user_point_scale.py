"""
admin/user_point_scale.py
=========================
Salary point-scale assessment + PDF preview for the `users` collection.

Flow
----
1.  Read the user from Mongo (`users`).
2.  Call the XN Portal API  ->  /ai/recruitments/user-document-list  (by email)
3.  Find the document whose `document_type_name == "Cv"`, download it and
    extract the text with Gemini (re-uses `_extract_text_from_url` from
    live_staffs_crons.py).
4.  Ask Gemini to return the experience rows as STRICT JSON — extraction only,
    it is never asked to do arithmetic.
5.  Compute durations, the total, the scale point, the next point and the
    increment date deterministically in Python.
6.  Render the "Verification of Service" form to PDF with ReportLab and
    stream it inline for preview.

Endpoints
---------
GET/POST  /admin/users/<user_id>/point-scale/generate   -> build + cache the assessment
GET       /admin/users/<user_id>/point-scale/preview    -> inline PDF (?refresh=1 to rebuild)
GET       /admin/users/<user_id>/point-scale/download    -> PDF as attachment
POST      /admin/users/<user_id>/point-scale/upload      -> push the PDF to the HSE document API
GET       /admin/users/<user_id>/point-scale/json        -> cached JSON (debug)
GET       /admin/users/<user_id>/point-scale/documents   -> raw XN Portal document list (debug)

Query params
------------
?refresh=1                  re-run the XN Portal fetch + Gemini extraction
?assessment_date=YYYY-MM-DD override the assessment date (default: today)

Env (identical to live_staffs_crons.py)
---------------------------------------
LIVE_STAFF_URL       base url of the XN Portal API
XN_PORTAL_API_KEY    Api-Key header
XN_APP_COUNTRY       X-App-Country header
GEMINI_API_KEY       Gemini key

Upload env (see admin/hse_document_upload.py)
---------------------------------------------
HSE_UPLOAD_URL       base url of the admin API (falls back to LIVE_STAFF_URL)
HSE_UPLOAD_API_KEY   Api-Key header (falls back to XN_PORTAL_API_KEY)
"""

from flask import request, jsonify, Response
from bson import ObjectId
from datetime import datetime, date, timedelta
import io
import os
import json
import re

from database import db
from . import admin_bp
from admin.views import admin_required
from admin.hse_document_upload import upload_hse_document, HSE_POINT_SCALE


# ══════════════════════════════════════════════════════════════════════
# Counting conventions
#
# Derived from the signed "Nurse Verification of Service" sample:
#
#   Oct 2020 -> Feb 2021  recorded as 0y 5m   (4 calendar months + 1)
#   Feb 2021 -> Mar 2023  recorded as 2y 2m   (25 calendar months + 1)
#   Apr 2024 -> Apr 2026  recorded as 2y 1m   (24 calendar months + 1)
#     => closed rows count BOTH the start and end month.
#
#   May 2026 -> 07/08/2026 recorded as 0y 3m 7d
#     => open-ended rows use exact day arithmetic to the assessment date.
#
# Set MONTH_COUNTING_INCLUSIVE = False if HR ever moves to plain
# calendar-difference counting.
# ══════════════════════════════════════════════════════════════════════

MONTH_COUNTING_INCLUSIVE = True
DAYS_PER_MONTH = 30          # only used to normalise leftover days


# ── Scales ────────────────────────────────────────────────────────────
# Each band is (lower_bound_in_years_inclusive, point). A band runs from its
# lower bound up to the next band's lower bound.
# Nurse skips points 14 and 15: the 12-15 band is point 13, then 16+ is 16.

NURSE_SCALE = [
    (0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7),
    (7, 8), (8, 9), (9, 10), (10, 11), (11, 12), (12, 13), (16, 16),
]

HCA_SCALE = [
    (0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7), (7, 8), (8, 9),
]

HCA_HINTS = ('hca', 'health care assistant', 'healthcare assistant',
             'care assistant', 'carer', 'support worker')


# ══════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════

def _v(val):
    if val is None:
        return ''
    return str(val).strip()


def _i(val, default=0):
    try:
        return int(float(str(val).strip()))
    except Exception:
        return default


def _users_col():
    return db.users


def _point_scales_col():
    """Cache of generated assessments — one document per user."""
    return db.user_point_scales


def _extract_text_from_url(url, headers=None):
    """Re-use the Gemini extractor already written for live_staffs."""
    from admin.live_staffs_crons import _extract_text_from_url as _f
    return _f(url, headers)


def _oid(val):
    try:
        return ObjectId(str(val))
    except Exception:
        return None


def _api_headers():
    return {
        "Api-Key":       os.environ.get('XN_PORTAL_API_KEY', ''),
        "X-App-Country": os.environ.get('XN_APP_COUNTRY', ''),
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }


def _lookup_name(collection_name, oid_str):
    """Soft lookup for the *_id reference fields on `users`."""
    oid = _oid(oid_str)
    if oid is None:
        return ''
    try:
        doc = db[collection_name].find_one(
            {"_id": oid}, {"name": 1, "title": 1, "label": 1}
        )
        if not doc:
            return ''
        return _v(doc.get('name') or doc.get('title') or doc.get('label') or '')
    except Exception:
        return ''


def _scale_for(designation):
    """Pick the Nurse or HCA scale from the user's designation."""
    d = (designation or '').strip().lower()
    if any(h in d for h in HCA_HINTS):
        return HCA_SCALE, 'HCA'
    return NURSE_SCALE, 'Nurse'


# ══════════════════════════════════════════════════════════════════════
# Step 1 — XN Portal: fetch document list + extract the CV text
# ══════════════════════════════════════════════════════════════════════

def _fetch_user_documents(email):
    """
    POST {LIVE_STAFF_URL}/ai/recruitments/user-document-list  {"email": ...}
    Falls back to GET if the endpoint rejects POST (405).

    Returns (documents_list, error_string_or_None)
    """
    import requests as _req

    base_url = os.environ.get('LIVE_STAFF_URL', '').rstrip('/')
    if not base_url:
        return [], "LIVE_STAFF_URL not set in environment"
    if not email:
        return [], "User has no email address"

    endpoint = f"{base_url}/ai/recruitments/user-document-list"
    headers  = _api_headers()

    try:
        resp = _req.post(endpoint, json={"email": email},
                         headers=headers, timeout=30)
        if resp.status_code == 405:
            resp = _req.get(endpoint, params={"email": email},
                            headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except _req.exceptions.ConnectionError as err:
        return [], f"Connection error — cannot reach XN Portal: {str(err)[:200]}"
    except _req.exceptions.Timeout:
        return [], "Timeout — XN Portal did not respond within 30s"
    except Exception as err:
        return [], f"{type(err).__name__}: {str(err)[:200]}"

    if not data.get('success'):
        return [], data.get('message', 'API returned success=false')

    api_data = data.get('data')
    if isinstance(api_data, list):
        documents = api_data
    elif isinstance(api_data, dict):
        documents = api_data.get('documents') or []
    else:
        documents = []

    return documents, None


def _extract_cv_for_user(email):
    """
    Returns dict:
      {cv_text, cv_url, documents_found, error}
    """
    documents, err = _fetch_user_documents(email)
    if err:
        return {"cv_text": '', "cv_url": None,
                "documents_found": 0, "error": err}

    if not documents:
        return {"cv_text": '', "cv_url": None,
                "documents_found": 0, "error": "No documents returned by API"}

    cv_url = next(
        (d.get('url') for d in documents
         if (d.get('document_type_name') or '').strip().lower() == 'cv'
         and d.get('url')),
        None
    )

    if not cv_url:
        return {"cv_text": '', "cv_url": None,
                "documents_found": len(documents),
                "error": "No document with document_type_name == 'Cv'"}

    dl_headers = {k: v for k, v in _api_headers().items() if k != 'Content-Type'}
    try:
        text = _extract_text_from_url(cv_url, dl_headers)
    except Exception as err:
        return {"cv_text": '', "cv_url": cv_url,
                "documents_found": len(documents),
                "error": f"CV extraction failed: {str(err)[:200]}"}

    if text and text.startswith('['):          # extractor's own error marker
        return {"cv_text": '', "cv_url": cv_url,
                "documents_found": len(documents), "error": text[:200]}

    return {"cv_text": _v(text), "cv_url": cv_url,
            "documents_found": len(documents), "error": None}


# ══════════════════════════════════════════════════════════════════════
# Step 2 — Gemini: extract the experience rows as strict JSON
#
# Gemini extracts ONLY. All arithmetic is done in Python below so the
# result is reproducible and auditable.
# ══════════════════════════════════════════════════════════════════════

_ROWS_SCHEMA_HINT = """{
  "experience": [
    {
      "post": "",
      "location": "",
      "from_date": "",
      "to_date": "",
      "from_year": 0,
      "from_month": 0,
      "to_year": 0,
      "to_month": 0,
      "is_current": false,
      "is_healthcare": true
    }
  ]
}"""


def _extract_experience_rows(cv_text):
    """
    Ask Gemini for the structured experience rows.
    Returns (rows_list, error_string_or_None).
    """
    if not cv_text:
        return [], "No CV text to analyse"

    gemini_key = os.environ.get('GEMINI_API_KEY', '')
    if not gemini_key:
        return [], "GEMINI_API_KEY not set"

    prompt = f"""You are a CV analyser for Irish healthcare staffing (HSE service verification).

Read the CV text below and extract EVERY work experience entry.
Include all roles regardless of type — nursing, care, agency, private, retail, admin, anything.

STRICT RULES
* NEVER invent, assume or embellish. Extract only what the CV states.
* Do NOT calculate durations, totals, years or months. Extraction only.
* Normalise each start/end date into a numeric year and month (month 1-12).
* If a date gives only a year, use month 1 for the start and month 12 for the end.
* If a role is ongoing ("Present", "To date", "Current"), set is_current = true
  and leave to_year / to_month as 0.
* Keep from_date / to_date as the raw text exactly as written in the CV.
* post     = job title as written.
* location = employer name and location, combined, as written.
* is_healthcare = true for nursing / care / healthcare roles, false otherwise.
* List roles most recent first.
* If a role has no usable start date at all, omit it.

OUTPUT
Return ONLY valid JSON matching exactly this shape — no markdown, no code fences, no commentary:
{_ROWS_SCHEMA_HINT}

If no roles are found, return: {{"experience": []}}

---
CV TEXT:
{cv_text}
---
"""

    try:
        from google import genai as _genai
        client = _genai.Client(api_key=gemini_key)
        resp = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config={"response_mime_type": "application/json"},
        )
        raw = (resp.text or '').strip()
        raw = re.sub(r'^```(?:json)?\s*', '', raw, flags=re.MULTILINE)
        raw = re.sub(r'```\s*$', '', raw, flags=re.MULTILINE).strip()
        parsed = json.loads(raw)
        rows = parsed.get('experience') if isinstance(parsed, dict) else parsed
        if not isinstance(rows, list):
            return [], "Gemini did not return an experience array"
        return rows, None
    except Exception as err:
        return [], f"Gemini extraction failed: {str(err)[:200]}"


# ══════════════════════════════════════════════════════════════════════
# Step 3 — Durations (deterministic)
# ══════════════════════════════════════════════════════════════════════

def _month_end(year, month):
    """Last day of the given month."""
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def _row_duration(row, assessment_date):
    """
    Return (years, months, days, note) for one extracted row.

    Closed rows  -> whole months, inclusive of both the start and end month.
    Current rows -> exact day arithmetic from the 1st of the start month
                    up to and including the assessment date.
    """
    fy, fm = _i(row.get('from_year')), _i(row.get('from_month'))
    if not fy:
        return 0, 0, 0, "no usable start date"
    fm = min(max(fm or 1, 1), 12)

    if row.get('is_current'):
        start = date(fy, fm, 1)
        if start > assessment_date:
            return 0, 0, 0, "start date is after the assessment date"
        months = (assessment_date.year - start.year) * 12 + \
                 (assessment_date.month - start.month)
        anchor_month = (start.month - 1 + months) % 12 + 1
        anchor_year  = start.year + (start.month - 1 + months) // 12
        anchor_day   = min(start.day, _month_end(anchor_year, anchor_month).day)
        anchor = date(anchor_year, anchor_month, anchor_day)
        days = (assessment_date - anchor).days + 1   # inclusive of both ends
        if days >= DAYS_PER_MONTH:
            months += days // DAYS_PER_MONTH
            days = days % DAYS_PER_MONTH
        return months // 12, months % 12, days, ''

    ty, tm = _i(row.get('to_year')), _i(row.get('to_month'))
    if not ty:
        return 0, 0, 0, "no usable end date"
    tm = min(max(tm or 12, 1), 12)

    months = (ty - fy) * 12 + (tm - fm)
    if MONTH_COUNTING_INCLUSIVE:
        months += 1
    if months < 0:
        return 0, 0, 0, "end date precedes start date"
    return months // 12, months % 12, 0, ''


def _build_rows(raw_rows, assessment_date):
    """Attach computed durations to each extracted row."""
    rows = []
    for r in raw_rows or []:
        y, m, d, note = _row_duration(r, assessment_date)
        rows.append({
            "post":          _v(r.get('post')),
            "location":      _v(r.get('location')),
            "from_date":     _v(r.get('from_date')),
            "to_date":       _v(r.get('to_date')) or ('To date' if r.get('is_current') else ''),
            "years":         y,
            "months":        m,
            "days":          d,
            "is_current":    bool(r.get('is_current')),
            "is_healthcare": r.get('is_healthcare') is not False,
            "counted":       bool(y or m or d),
            "note":          note,
            "source":        "cv",
        })
    return rows


def _rows_from_profile(user):
    """Fallback when no CV is available — use the Mongo profile fields."""
    y = _i(user.get('experience_year'))
    m = _i(user.get('experience_month'))
    if not (y or m):
        return []
    return [{
        "post":          _v(user.get('job_title')) or _v(user.get('designation')),
        "location":      _v(user.get('company_name')),
        "from_date":     '',
        "to_date":       '',
        "years":         y,
        "months":        m,
        "days":          0,
        "is_current":    False,
        "is_healthcare": True,
        "counted":       True,
        "note":          "from profile record — no CV available",
        "source":        "profile",
    }]


def _total_days(rows):
    return sum(
        ((r['years'] * 12 + r['months']) * DAYS_PER_MONTH) + r['days']
        for r in rows if r.get('counted')
    )


def _days_to_ymd(total_days):
    months, days = divmod(int(total_days), DAYS_PER_MONTH)
    years, months = divmod(months, 12)
    return years, months, days


def _fmt_ymd(y, m, d):
    parts = []
    if y:
        parts.append(f"{y} year{'s' if y != 1 else ''}")
    if m:
        parts.append(f"{m} month{'s' if m != 1 else ''}")
    if d:
        parts.append(f"{d} day{'s' if d != 1 else ''}")
    return ' '.join(parts) if parts else '0 months'


def _add_days(start, days):
    """Add a gap to a date, keeping the day-of-month stable where possible."""
    whole_months, leftover = divmod(int(days), DAYS_PER_MONTH)
    year  = start.year + (start.month - 1 + whole_months) // 12
    month = (start.month - 1 + whole_months) % 12 + 1
    day   = min(start.day, _month_end(year, month).day)
    result = date(year, month, day)
    if leftover:
        result += timedelta(days=leftover)
    return result


# ══════════════════════════════════════════════════════════════════════
# Step 4 — Point placement + increment date
# ══════════════════════════════════════════════════════════════════════

def _calculate_point_scale(rows, designation, assessment_date):
    scale, scale_name = _scale_for(designation)
    total_days = _total_days(rows)
    ty, tm, td = _days_to_ymd(total_days)

    idx = 0
    for i, (lower, _p) in enumerate(scale):
        if total_days >= lower * 12 * DAYS_PER_MONTH:
            idx = i
        else:
            break

    point = scale[idx][1]
    result = {
        "scale":            scale_name,
        "designation":      _v(designation),
        "experience_table": rows,
        "total_experience": {
            "years": ty, "months": tm, "days": td,
            "display": _fmt_ymd(ty, tm, td),
            "total_days": total_days,
        },
        "point":                    point,
        "band":                     _band_label(scale, idx),
        "next_point":               None,
        "next_band_starts_at_years": None,
        "experience_to_next_point": None,
        "assessment_date":          assessment_date.isoformat(),
        "increment_date":           None,
        "note": ("The increment date assumes the person remains in HSE "
                 "employment continuously from the assessment date onward."),
    }

    if idx == len(scale) - 1:
        result["note"] = (
            f"Point {point} is the top of the {scale_name} scale — "
            "no increment date applies."
        )
        return result

    next_lower, next_point = scale[idx + 1]
    gap_days = max(0, next_lower * 12 * DAYS_PER_MONTH - total_days)
    gy, gm, gd = _days_to_ymd(gap_days)

    result["next_point"] = next_point
    result["next_band_starts_at_years"] = next_lower
    result["experience_to_next_point"] = {
        "years": gy, "months": gm, "days": gd, "display": _fmt_ymd(gy, gm, gd),
    }
    result["increment_date"] = _add_days(assessment_date, gap_days).isoformat()
    return result


def _band_label(scale, idx):
    lower = scale[idx][0]
    if idx == len(scale) - 1:
        return f"{lower} years and above"
    return f"{lower}-{scale[idx + 1][0]} years"


# ══════════════════════════════════════════════════════════════════════
# Step 5 — Render to PDF (ReportLab) — "Verification of Service" form
# ══════════════════════════════════════════════════════════════════════

_FONT_CANDIDATES = [
    ('/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf',
     '/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf',
     '/usr/share/fonts/truetype/liberation/LiberationSans-Italic.ttf'),
    ('/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
     '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
     '/usr/share/fonts/truetype/dejavu/DejaVuSans-Oblique.ttf'),
]

_FONTS_READY = None


def _register_fonts():
    global _FONTS_READY
    if _FONTS_READY is not None:
        return _FONTS_READY

    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib.fonts import addMapping

    for reg, bold, ital in _FONT_CANDIDATES:
        if not (os.path.exists(reg) and os.path.exists(bold)):
            continue
        try:
            pdfmetrics.registerFont(TTFont('PSSans', reg))
            pdfmetrics.registerFont(TTFont('PSSans-Bold', bold))
            if os.path.exists(ital):
                pdfmetrics.registerFont(TTFont('PSSans-Italic', ital))
                addMapping('PSSans', 0, 1, 'PSSans-Italic')
            addMapping('PSSans', 0, 0, 'PSSans')
            addMapping('PSSans', 1, 0, 'PSSans-Bold')
            _FONTS_READY = ('PSSans', 'PSSans-Bold',
                            'PSSans-Italic' if os.path.exists(ital) else 'PSSans')
            return _FONTS_READY
        except Exception:
            continue

    _FONTS_READY = ('Helvetica', 'Helvetica-Bold', 'Helvetica-Oblique')
    return _FONTS_READY


def _fmt_date(iso_str):
    """ISO -> dd/mm/yyyy for the form."""
    if not iso_str:
        return ''
    try:
        return datetime.strptime(iso_str[:10], '%Y-%m-%d').strftime('%d/%m/%Y')
    except Exception:
        return _v(iso_str)


def _build_point_scale_pdf(assessment, user_block):
    """Render the assessment to PDF bytes using the house form layout."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                    Table, TableStyle)

    def esc(txt):
        return (_v(txt).replace('&', '&amp;')
                       .replace('<', '&lt;')
                       .replace('>', '&gt;'))

    FONT, FONT_B, FONT_I = _register_fonts()

    BLUE = colors.HexColor('#1F5C99')

    st_title = ParagraphStyle('t', fontName=FONT_B, fontSize=15, leading=19,
                              alignment=TA_CENTER, spaceAfter=16, textColor=BLUE)
    st_head  = ParagraphStyle('h', fontName=FONT_B, fontSize=12, leading=15,
                              alignment=TA_LEFT, spaceBefore=12, spaceAfter=6,
                              textColor=BLUE)
    st_body  = ParagraphStyle('b', fontName=FONT, fontSize=11, leading=17,
                              alignment=TA_LEFT, spaceAfter=4)
    st_cell  = ParagraphStyle('c', fontName=FONT, fontSize=8.5, leading=11)
    st_cellb = ParagraphStyle('cb', fontName=FONT_B, fontSize=8.5, leading=11)
    st_lbl   = ParagraphStyle('l', fontName=FONT_B, fontSize=10, leading=13)
    st_val   = ParagraphStyle('vv', fontName=FONT, fontSize=10, leading=13)
    st_ital  = ParagraphStyle('i', fontName=FONT_I, fontSize=9, leading=12,
                              textColor=colors.HexColor('#444444'))

    scale_name = assessment.get('scale') or 'Nurse'

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=19 * mm, rightMargin=19 * mm,
        topMargin=18 * mm, bottomMargin=18 * mm,
        title=f"{_v(user_block.get('name'))} — Verification of Service",
        author='Xpress Health',
    )
    W = doc.width
    story = []

    story.append(Paragraph(
        f'{esc(scale_name)} Verification of Service', st_title))

    # ── Staff details ─────────────────────────────────────────────────
    story.append(Paragraph('Staff Details', st_head))
    staff_rows = [
        ('Full Name',         user_block.get('name')),
        ('Role / Designation', user_block.get('designation')),
        ('Contact Email',     user_block.get('email')),
    ]
    tbl = Table(
        [[Paragraph(esc(l), st_lbl), Paragraph(esc(_v(v)), st_val)]
         for l, v in staff_rows],
        colWidths=[W * 0.42, W * 0.58], hAlign='LEFT')
    tbl.setStyle(TableStyle([
        ('GRID',          (0, 0), (-1, -1), 0.6, colors.black),
        ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING',   (0, 0), (-1, -1), 6),
        ('RIGHTPADDING',  (0, 0), (-1, -1), 6),
        ('TOPPADDING',    (0, 0), (-1, -1), 7),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 7),
    ]))
    story.append(tbl)

    # ── Previous service details ──────────────────────────────────────
    story.append(Paragraph('Previous Service Details', st_head))

    header = ['Post', 'Location', 'From Date', 'To Date',
              'Years', 'Months', 'Days', 'Verified']
    data = [[Paragraph(esc(h), st_cellb) for h in header]]

    for r in assessment.get('experience_table') or []:
        data.append([
            Paragraph(esc(r.get('post')), st_cell),
            Paragraph(esc(r.get('location')), st_cell),
            Paragraph(esc(r.get('from_date')), st_cell),
            Paragraph(esc(r.get('to_date')), st_cell),
            Paragraph(str(r.get('years', 0)), st_cell),
            Paragraph(str(r.get('months', 0)), st_cell),
            Paragraph(str(r.get('days', 0)), st_cell),
            Paragraph('', st_cell),
        ])

    if len(data) == 1:
        data.append([Paragraph('No experience records found', st_cell)] +
                    [Paragraph('', st_cell) for _ in range(7)])

    widths = [W * 0.17, W * 0.23, W * 0.11, W * 0.11,
              W * 0.07, W * 0.08, W * 0.07, W * 0.16]
    tbl = Table(data, colWidths=widths, hAlign='LEFT', repeatRows=1)
    tbl.setStyle(TableStyle([
        ('GRID',          (0, 0), (-1, -1), 0.6, colors.black),
        ('VALIGN',        (0, 0), (-1, -1), 'TOP'),
        ('BACKGROUND',    (0, 0), (-1, 0), colors.HexColor('#EFEFEF')),
        ('ALIGN',         (4, 1), (6, -1), 'CENTER'),
        ('LEFTPADDING',   (0, 0), (-1, -1), 4),
        ('RIGHTPADDING',  (0, 0), (-1, -1), 4),
        ('TOPPADDING',    (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    story.append(tbl)

    # ── Assessment summary ────────────────────────────────────────────
    total = assessment.get('total_experience') or {}
    to_next = assessment.get('experience_to_next_point') or {}

    story.append(Paragraph('Assessment', st_head))
    summary = [
        ('Total Experience',      total.get('display', '')),
        (f'{scale_name} Scale Band', assessment.get('band', '')),
        ('Point Awarded',         str(assessment.get('point', ''))),
        ('Next Point',            str(assessment.get('next_point'))
                                  if assessment.get('next_point') else 'N/A — top of scale'),
        ('Experience to Next Point', to_next.get('display', '')
                                     if to_next else 'N/A'),
        ('Assessment Date',       _fmt_date(assessment.get('assessment_date'))),
        ('Increment Date',        _fmt_date(assessment.get('increment_date'))
                                  or 'N/A — top of scale'),
    ]
    tbl = Table(
        [[Paragraph(esc(l), st_lbl), Paragraph(esc(_v(v)), st_val)]
         for l, v in summary],
        colWidths=[W * 0.42, W * 0.58], hAlign='LEFT')
    tbl.setStyle(TableStyle([
        ('GRID',          (0, 0), (-1, -1), 0.6, colors.black),
        ('VALIGN',        (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING',   (0, 0), (-1, -1), 6),
        ('RIGHTPADDING',  (0, 0), (-1, -1), 6),
        ('TOPPADDING',    (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    story.append(tbl)
    story.append(Spacer(1, 18))

    # ── Statement ─────────────────────────────────────────────────────
    name = esc(user_block.get('name'))
    story.append(Paragraph(
        f"Therefore Ms/Mr <b>{name}</b> should be appointed on the "
        f"<b>{assessment.get('point', '')}</b> point of the salary scale "
        f"with effect from <b>{_fmt_date(assessment.get('assessment_date'))}</b>.",
        st_body))
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        f"Incremental Date: <b>{_fmt_date(assessment.get('increment_date')) or 'N/A'}</b>",
        st_body))
    story.append(Spacer(1, 6))
    story.append(Paragraph(esc(assessment.get('note', '')), st_ital))
    story.append(Spacer(1, 22))

    story.append(Paragraph('Signed: _____________________'
                           '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'
                           'Date: _______________', st_body))
    story.append(Spacer(1, 10))
    story.append(Paragraph('Verified by: _________________'
                           '&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;'
                           'Date: _______________', st_body))
    story.append(Spacer(1, 18))
    story.append(Paragraph(
        '<b>Please ensure that back-up documentation is attached (Payslips)</b>',
        st_body))

    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════
# Orchestration + cache
# ══════════════════════════════════════════════════════════════════════

def _parse_assessment_date(raw):
    if not raw:
        return date.today()
    try:
        return datetime.strptime(_v(raw)[:10], '%Y-%m-%d').date()
    except Exception:
        return date.today()


def _user_block(user):
    first = _v(user.get('first_name'))
    last  = _v(user.get('last_name'))
    return {
        "name":        _v(user.get('name')) or f"{first} {last}".strip(),
        "email":       _v(user.get('email')),
        "designation": _v(user.get('designation')),
        "phone":       _v(user.get('phone')),
        "county":      _lookup_name('counties', user.get('county_id')),
        "xn_user_id":  _v(user.get('xn_user_id')),
    }


def _generate_for_user(user_id, force=False, assessment_date=None):
    """
    Returns (payload_dict, http_status).
    payload contains: user, assessment, meta
    """
    oid = _oid(user_id)
    if oid is None:
        return {"success": False, "error": "Invalid user id"}, 400

    user = _users_col().find_one({"_id": oid})
    if not user:
        return {"success": False, "error": "User not found"}, 404

    assessment_date = assessment_date or date.today()
    ub = _user_block(user)

    cache = _point_scales_col().find_one({"user_id": str(oid)})
    if (cache and cache.get('assessment') and not force
            and cache['assessment'].get('assessment_date') == assessment_date.isoformat()):
        return {
            "success":      True,
            "cached":       True,
            "user_id":      str(oid),
            "user":         ub,
            "assessment":   cache['assessment'],
            "meta":         cache.get('meta') or {},
            "generated_at": (cache.get('generated_at') or datetime.utcnow()).isoformat(),
        }, 200

    extraction = _extract_cv_for_user(ub['email'])
    raw_rows, extract_err = _extract_experience_rows(extraction.get('cv_text') or '')

    rows = _build_rows(raw_rows, assessment_date)
    source = "xn_portal_cv"
    if not [r for r in rows if r.get('counted')]:
        rows = _rows_from_profile(user)
        source = "mongo_profile_fallback" if rows else "none"

    assessment = _calculate_point_scale(rows, ub['designation'], assessment_date)

    meta = {
        "email":            ub['email'],
        "cv_url":           extraction.get('cv_url'),
        "documents_found":  extraction.get('documents_found'),
        "extraction_error": extraction.get('error'),
        "analysis_error":   extract_err,
        "source":           source,
        "month_counting":   "inclusive" if MONTH_COUNTING_INCLUSIVE else "calendar-difference",
    }

    _point_scales_col().update_one(
        {"user_id": str(oid)},
        {"$set": {
            "user_id":      str(oid),
            "xn_user_id":   ub['xn_user_id'],
            "name":         ub['name'],
            "email":        ub['email'],
            "designation":  ub['designation'],
            "extracted_cv": extraction.get('cv_text') or '',
            "assessment":   assessment,
            "meta":         meta,
            "generated_at": datetime.utcnow(),
        }},
        upsert=True,
    )

    return {
        "success":      True,
        "cached":       False,
        "user_id":      str(oid),
        "user":         ub,
        "assessment":   assessment,
        "meta":         meta,
        "generated_at": datetime.utcnow().isoformat(),
    }, 200


def _staff_id_for(user):
    """
    The HSE upload API validates staff_id against the user service, so it
    wants the staff/XN id — not the local Mongo _id. Prefer the explicit
    fields, fall back to the Mongo id as a last resort.
    """
    for key in ('staff_id', 'xn_user_id', 'xn_staff_id', 'user_service_id'):
        if _v(user.get(key)):
            return _v(user.get(key))
    return _v(user.get('_id'))


def _pdf_filename(user_block, fallback='point_scale'):
    name = _v(user_block.get('name')) or fallback
    safe = re.sub(r'[^A-Za-z0-9]+', '_', name).strip('_') or fallback
    return f"{safe}_Point_Scale.pdf"


# ══════════════════════════════════════════════════════════════════════
# Routes
# ══════════════════════════════════════════════════════════════════════

@admin_bp.route('/users/<user_id>/point-scale/generate', methods=['GET', 'POST'])
@admin_required
def user_point_scale_generate(user_id):
    """
    Build (or rebuild with ?refresh=1) the point-scale assessment for a user.
    ?assessment_date=YYYY-MM-DD overrides the assessment date.
    """
    try:
        force = request.args.get('refresh') in ('1', 'true', 'yes')
        adate = _parse_assessment_date(request.args.get('assessment_date'))
        payload, status = _generate_for_user(user_id, force=force,
                                             assessment_date=adate)
        return jsonify(payload), status
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/users/<user_id>/point-scale/preview', methods=['GET'])
@admin_required
def user_point_scale_preview(user_id):
    """Inline PDF preview — renders in the browser / an <iframe>."""
    try:
        force = request.args.get('refresh') in ('1', 'true', 'yes')
        adate = _parse_assessment_date(request.args.get('assessment_date'))
        payload, status = _generate_for_user(user_id, force=force,
                                             assessment_date=adate)
        if status != 200 or not payload.get('success'):
            return jsonify(payload), status

        pdf_bytes = _build_point_scale_pdf(payload['assessment'], payload['user'])
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={
                "Content-Disposition":
                    f'inline; filename="{_pdf_filename(payload["user"], user_id)}"',
                "Cache-Control": "no-store",
            },
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/users/<user_id>/point-scale/download', methods=['GET'])
@admin_required
def user_point_scale_download(user_id):
    """Same PDF, but as a file download."""
    try:
        force = request.args.get('refresh') in ('1', 'true', 'yes')
        adate = _parse_assessment_date(request.args.get('assessment_date'))
        payload, status = _generate_for_user(user_id, force=force,
                                             assessment_date=adate)
        if status != 200 or not payload.get('success'):
            return jsonify(payload), status

        pdf_bytes = _build_point_scale_pdf(payload['assessment'], payload['user'])
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={"Content-Disposition":
                     f'attachment; filename="{_pdf_filename(payload["user"], user_id)}"'},
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/users/<user_id>/point-scale/json', methods=['GET'])
@admin_required
def user_point_scale_json(user_id):
    """Cached assessment JSON — handy for debugging / editing before preview."""
    oid = _oid(user_id)
    if oid is None:
        return jsonify({"success": False, "error": "Invalid user id"}), 400
    cache = _point_scales_col().find_one({"user_id": str(oid)}, {"_id": 0})
    if not cache:
        return jsonify({"success": False,
                        "error": "Nothing generated yet — call /point-scale/generate"}), 404
    if cache.get('generated_at'):
        cache['generated_at'] = cache['generated_at'].isoformat()
    return jsonify({"success": True, **cache})


@admin_bp.route('/users/<user_id>/point-scale/documents', methods=['GET'])
@admin_required
def user_point_scale_documents(user_id):
    """Raw XN Portal document list for this user — debugging aid."""
    oid = _oid(user_id)
    if oid is None:
        return jsonify({"success": False, "error": "Invalid user id"}), 400
    user = _users_col().find_one({"_id": oid}, {"email": 1, "name": 1})
    if not user:
        return jsonify({"success": False, "error": "User not found"}), 404

    documents, err = _fetch_user_documents(_v(user.get('email')))
    return jsonify({
        "success":    err is None,
        "email":      _v(user.get('email')),
        "error":      err,
        "count":      len(documents),
        "documents":  documents,
        "cv_document": next(
            (d for d in documents
             if (d.get('document_type_name') or '').strip().lower() == 'cv'),
            None),
    })


@admin_bp.route('/users/<user_id>/point-scale/upload', methods=['POST'])
@admin_required
def user_point_scale_upload(user_id):
    """
    Generate the Verification of Service PDF and push it to the HSE document
    upload API as hse_document_type = point_scale_document.

    ?refresh=1                   rebuild before uploading
    ?assessment_date=YYYY-MM-DD  override the assessment date
    ?staff_id=...                override the staff id sent to the API
    """
    try:
        oid = _oid(user_id)
        if oid is None:
            return jsonify({"success": False, "error": "Invalid user id"}), 400

        user = _users_col().find_one({"_id": oid})
        if not user:
            return jsonify({"success": False, "error": "User not found"}), 404

        force = request.args.get('refresh') in ('1', 'true', 'yes')
        adate = _parse_assessment_date(request.args.get('assessment_date'))
        payload, status = _generate_for_user(user_id, force=force,
                                             assessment_date=adate)
        if status != 200 or not payload.get('success'):
            return jsonify(payload), status

        body = request.get_json(silent=True) or {}
        staff_id = (_v(request.args.get('staff_id'))
                    or _v(body.get('staff_id'))
                    or _staff_id_for(user))
        if not staff_id:
            return jsonify({"success": False,
                            "error": "No staff id available for this user"}), 400

        filename  = _pdf_filename(payload['user'], user_id)
        pdf_bytes = _build_point_scale_pdf(payload['assessment'], payload['user'])

        ok, result = upload_hse_document(pdf_bytes, filename, staff_id,
                                         HSE_POINT_SCALE)
        if not ok:
            return jsonify({
                "success":  False,
                "user_id":  str(oid),
                "staff_id": staff_id,
                "filename": filename,
                "upload":   result,
            }), result.get('status_code') or 502

        _point_scales_col().update_one(
            {"user_id": str(oid)},
            {"$set": {
                "uploaded_at":       datetime.utcnow(),
                "uploaded_staff_id": staff_id,
                "upload_response":   result.get('data') or {},
                "hse_document_type": HSE_POINT_SCALE,
            }},
            upsert=True,
        )

        return jsonify({
            "success":           True,
            "user_id":           str(oid),
            "staff_id":          staff_id,
            "filename":          filename,
            "hse_document_type": HSE_POINT_SCALE,
            "upload":            result,
        }), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500