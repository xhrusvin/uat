"""
admin/user_cv.py
================
CV generation + PDF preview for the `users` collection.

Flow
----
1.  Read the user from Mongo (`users`).
2.  Call the XN Portal API  ->  /ai/recruitments/user-document-list  (by email)
3.  Find the document whose `document_type_name == "Cv"`, download it and
    extract the text with Gemini (re-uses `_extract_text_from_url` from
    live_staffs_crons.py).
4.  Merge that text with the Mongo user record and ask Gemini to return a
    STRICT JSON structure matching the house CV template.
5.  Render that JSON to a PDF with ReportLab (same layout as the sample
    "Brian Long" CV) and stream it inline for preview.

Endpoints
---------
GET/POST  /admin/users/<user_id>/cv/generate     -> build + cache the CV JSON
GET       /admin/users/<user_id>/cv/preview      -> inline PDF (?refresh=1 to rebuild)
GET       /admin/users/<user_id>/cv/download     -> PDF as attachment
GET       /admin/users/<user_id>/cv/json         -> cached JSON (debug)
GET       /admin/users/<user_id>/cv/documents    -> raw XN Portal document list (debug)

Env (identical to live_staffs_crons.py)
---------------------------------------
LIVE_STAFF_URL       base url of the XN Portal API
XN_PORTAL_API_KEY    Api-Key header
XN_APP_COUNTRY       X-App-Country header
GEMINI_API_KEY       Gemini key
CRON_SECRET          optional, protects the unauthenticated variants
"""

from flask import request, jsonify, Response
from bson import ObjectId
from datetime import datetime
import io
import os
import json
import re

from database import db
from . import admin_bp
from admin.views import admin_required


# ══════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════

def _v(val):
    if val is None:
        return ''
    return str(val).strip()


def _users_col():
    return db.users


def _user_cvs_col():
    """Cache of generated CVs — one document per user."""
    return db.user_ai_cvs


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
    """
    Soft lookup for the *_id reference fields on `users`
    (county_id, country_id, gender_id, visa_type_id ...).
    Returns '' on any failure so it never breaks CV generation.
    """
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
# Step 2 — Flatten the Mongo `users` record for the prompt
# ══════════════════════════════════════════════════════════════════════

def _user_summary(user):
    """Human-readable summary of the Mongo record, fed to Gemini."""
    first = _v(user.get('first_name'))
    last  = _v(user.get('last_name'))
    name  = _v(user.get('name')) or f"{first} {last}".strip()

    address_parts = [_v(user.get('address')).replace('\n', ', ')]
    county  = _lookup_name('counties', user.get('county_id'))
    country = _lookup_name('countries', user.get('country_id'))
    if county:
        address_parts.append(county)
    if country:
        address_parts.append(country)
    if _v(user.get('eir_code')):
        address_parts.append(_v(user.get('eir_code')))
    address = ', '.join(p for p in address_parts if p)

    exp_y = user.get('experience_year') or 0
    exp_m = user.get('experience_month') or 0
    total_exp = ''
    if exp_y or exp_m:
        total_exp = f"{exp_y} year{'s' if exp_y != 1 else ''}"
        if exp_m:
            total_exp += f" {exp_m} month{'s' if exp_m != 1 else ''}"

    last_y = user.get('last_company_experience_year') or 0
    last_m = user.get('last_company_experience_month') or 0
    last_exp = ''
    if last_y or last_m:
        last_exp = f"{last_y} year{'s' if last_y != 1 else ''}"
        if last_m:
            last_exp += f" {last_m} month{'s' if last_m != 1 else ''}"

    specialities = ', '.join(
        _v(s) for s in (user.get('user_sub_type_ids') or []) if _v(s)
    )

    trainings = []
    for key, label in (
        ('tuberculosis_vaccine', 'Tuberculosis (TB) vaccination'),
        ('hepatitis_antibody',   'Hepatitis B antibody status'),
        ('mmr_vaccine',          'MMR vaccination'),
        ('covid_19_vaccine',     'COVID-19 vaccination'),
    ):
        if user.get(key):
            trainings.append(label)

    ref_lines = []
    for r in (user.get('references') or []):
        ref_lines.append(
            f"  - {_v(r.get('name'))} | {_v(r.get('job_role'))} | "
            f"{_v(r.get('organization'))} | status: {_v(r.get('status'))}"
        )

    return f"""
Full Name: {name}
Email: {_v(user.get('email'))}
Mobile: {_v(user.get('phone'))}
Address: {address}
Eircode: {_v(user.get('eir_code'))}
Date of Birth: {_v(user.get('dob'))}
Gender: {_lookup_name('genders', user.get('gender_id'))}
Nationality / Country: {country}
Designation / Role: {_v(user.get('designation'))}
Specialities / Divisions: {specialities}
Total Experience: {total_exp}
Current / Most Recent Employer: {_v(user.get('company_name'))}
Current / Most Recent Job Title: {_v(user.get('job_title'))}
Time in Most Recent Role: {last_exp}
Employer County: {_lookup_name('counties', user.get('company_county_id'))}
Permission to Work: {'Yes' if user.get('permission_to_work') else 'No'}
Visa / Stamp Type: {_lookup_name('visa_types', user.get('visa_type_id'))}
PPS Number: {_v(user.get('pps_number'))}
Masters Qualification: {'Yes' if user.get('masters') else 'No'}
Own Transport / Travel Mode: {'Yes' if user.get('travel_mode') else 'No'}
Occupational Health on file: {', '.join(trainings) if trainings else 'None recorded'}

References:
{chr(10).join(ref_lines) if ref_lines else '  None recorded'}
""".strip()


# ══════════════════════════════════════════════════════════════════════
# Step 3 — Gemini: produce the CV as strict JSON
# ══════════════════════════════════════════════════════════════════════

_CV_SCHEMA_HINT = """{
  "personal_details": {
    "full_name": "",
    "address": "",
    "mobile_number": "",
    "email_address": ""
  },
  "professional_profile": "",
  "education": [
    {"qualification": "", "institution": ""}
  ],
  "professional_experience": [
    {
      "job_title": "",
      "employer": "",
      "location": "",
      "dates": "",
      "duties": [""]
    }
  ],
  "previous_experience": [
    {"job_title": "", "employer": "", "location": "", "dates": ""}
  ],
  "key_skills": [""],
  "interests": [""]
}"""


def _build_cv_json(user, extracted_cv):
    """
    Ask Gemini for the structured CV. Falls back to a deterministic
    Mongo-only structure if Gemini is unavailable or misbehaves.
    """
    data_summary = _user_summary(user)
    fallback     = _fallback_cv_json(user)

    gemini_key = os.environ.get('GEMINI_API_KEY', '')
    if not gemini_key:
        return fallback, "GEMINI_API_KEY not set — built from Mongo data only"

    has_cv = bool(extracted_cv) and not extracted_cv.startswith('[')

    prompt = f"""You are a professional CV writer specialising in Irish healthcare recruitment.
Produce a clean, ATS-friendly CV for the candidate below.

STRICT RULES
* NEVER invent, assume or embellish information that is not present in the sources.
* Use ONLY: (1) CANDIDATE DATA from the database, (2) the CANDIDATE'S ORIGINAL CV.
* Preserve employer names, job titles, dates, institutions, duties and skills exactly as given.
* Improve grammar and phrasing only — never change meaning.
* Where the two sources conflict on contact details, prefer CANDIDATE DATA.
* Where the original CV has richer employment/education detail, prefer the CV.
* Omit any field or section you cannot support with the sources (use "" or []).
* professional_experience = healthcare / relevant roles, reverse chronological, each with duties.
* previous_experience = older non-relevant roles (retail, admin, farm work etc), no duties needed.
* professional_profile = 3-5 sentence summary built strictly from the sources.
* duties = bullet text WITHOUT any leading bullet character.

OUTPUT
Return ONLY valid JSON matching exactly this shape — no markdown, no code fences, no commentary:
{_CV_SCHEMA_HINT}

---
CANDIDATE DATA (database):
{data_summary}

---
CANDIDATE'S ORIGINAL CV (extracted text):
{extracted_cv if has_cv else "No CV document available — build from CANDIDATE DATA only."}
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
        raw = re.sub(r'^```(?:json)?|```$', '', raw, flags=re.MULTILINE).strip()
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("Gemini did not return a JSON object")
        return _merge_with_fallback(parsed, fallback), None
    except Exception as err:
        return fallback, f"Gemini generation failed: {str(err)[:200]}"


def _fallback_cv_json(user):
    """Deterministic CV built purely from the Mongo record."""
    first = _v(user.get('first_name'))
    last  = _v(user.get('last_name'))
    name  = _v(user.get('name')) or f"{first} {last}".strip()

    address_parts = [_v(user.get('address')).replace('\n', ', ')]
    for extra in (_lookup_name('counties', user.get('county_id')),
                  _v(user.get('eir_code'))):
        if extra:
            address_parts.append(extra)
    address = ', '.join(p for p in address_parts if p)

    exp_y = user.get('experience_year') or 0
    designation = _v(user.get('designation')) or 'Healthcare Professional'
    specialities = [_v(s) for s in (user.get('user_sub_type_ids') or []) if _v(s)]

    profile_bits = [f"Experienced {designation}"]
    if exp_y:
        profile_bits.append(f"with {exp_y} years of experience")
    if specialities:
        profile_bits.append(f"specialising in {', '.join(specialities)}")
    profile = ' '.join(profile_bits).strip() + '.'

    experience = []
    if _v(user.get('company_name')) or _v(user.get('job_title')):
        last_y = user.get('last_company_experience_year') or 0
        experience.append({
            "job_title": _v(user.get('job_title')) or designation,
            "employer":  _v(user.get('company_name')),
            "location":  _lookup_name('counties', user.get('company_county_id')),
            "dates":     f"{last_y} year{'s' if last_y != 1 else ''}" if last_y else '',
            "duties":    [],
        })

    return {
        "personal_details": {
            "full_name":     name,
            "address":       address,
            "mobile_number": _v(user.get('phone')),
            "email_address": _v(user.get('email')),
        },
        "professional_profile":    profile,
        "education":               [],
        "professional_experience": experience,
        "previous_experience":     [],
        "key_skills":              specialities,
        "interests":               [],
    }


def _merge_with_fallback(parsed, fallback):
    """Ensure required keys exist and contact details are never blank."""
    out = dict(fallback)
    out.update({k: v for k, v in parsed.items() if v not in (None, '', [])})

    pd_fb = fallback.get('personal_details') or {}
    pd_ai = parsed.get('personal_details') or {}
    out['personal_details'] = {
        k: (_v(pd_ai.get(k)) or _v(pd_fb.get(k)))
        for k in ('full_name', 'address', 'mobile_number', 'email_address')
    }

    for key, default in (('education', []),
                         ('professional_experience', []),
                         ('previous_experience', []),
                         ('key_skills', []),
                         ('interests', [])):
        if not isinstance(out.get(key), list):
            out[key] = default
    if not isinstance(out.get('professional_profile'), str):
        out['professional_profile'] = fallback.get('professional_profile', '')
    return out


# ══════════════════════════════════════════════════════════════════════
# Step 4 — Render the CV JSON to PDF (ReportLab)
# ══════════════════════════════════════════════════════════════════════

_FONT_CANDIDATES = [
    # (regular, bold, italic) — first triple that exists on disk wins
    ('/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf',
     '/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf',
     '/usr/share/fonts/truetype/liberation/LiberationSans-Italic.ttf'),
    ('/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
     '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
     '/usr/share/fonts/truetype/dejavu/DejaVuSans-Oblique.ttf'),
]

_FONTS_READY = None


def _register_fonts():
    """
    Try to register Liberation Sans (the font used by the reference CV) so the
    output matches exactly and bullets stay text-extractable for ATS parsers.
    Falls back to the built-in Helvetica set if no TTFs are installed.
    Returns (regular, bold, italic) font names.
    """
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
            pdfmetrics.registerFont(TTFont('CVSans', reg))
            pdfmetrics.registerFont(TTFont('CVSans-Bold', bold))
            if os.path.exists(ital):
                pdfmetrics.registerFont(TTFont('CVSans-Italic', ital))
                addMapping('CVSans', 0, 1, 'CVSans-Italic')
            addMapping('CVSans', 0, 0, 'CVSans')
            addMapping('CVSans', 1, 0, 'CVSans-Bold')
            _FONTS_READY = ('CVSans', 'CVSans-Bold',
                            'CVSans-Italic' if os.path.exists(ital) else 'CVSans')
            return _FONTS_READY
        except Exception:
            continue

    _FONTS_READY = ('Helvetica', 'Helvetica-Bold', 'Helvetica-Oblique')
    return _FONTS_READY


def _build_cv_pdf(cv):
    """Render the structured CV to PDF bytes using the house template."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                    Table, TableStyle, KeepTogether)

    def esc(txt):
        return (_v(txt).replace('&', '&amp;')
                       .replace('<', '&lt;')
                       .replace('>', '&gt;'))

    FONT, FONT_B, FONT_I = _register_fonts()

    st_title = ParagraphStyle('t', fontName=FONT_B, fontSize=16, leading=20,
                              alignment=TA_CENTER, spaceAfter=14)
    st_head  = ParagraphStyle('h', fontName=FONT_B, fontSize=12, leading=15,
                              alignment=TA_LEFT, spaceBefore=12, spaceAfter=5,
                              textColor=colors.black)
    st_body  = ParagraphStyle('b', fontName=FONT, fontSize=11, leading=14,
                              alignment=TA_LEFT, spaceAfter=3)
    st_sub   = ParagraphStyle('s', fontName=FONT_B, fontSize=11, leading=14,
                              spaceBefore=8, spaceAfter=2)
    st_bul   = ParagraphStyle('bu', fontName=FONT, fontSize=11, leading=14,
                              leftIndent=18, bulletIndent=6, spaceAfter=2,
                              bulletFontName=FONT, bulletFontSize=11)
    st_ital  = ParagraphStyle('i', fontName=FONT_I, fontSize=11, leading=14)
    st_cell  = ParagraphStyle('c', fontName=FONT, fontSize=11, leading=13)
    st_cellb = ParagraphStyle('cb', fontName=FONT_B, fontSize=11, leading=13)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=19 * mm, rightMargin=19 * mm,
        topMargin=18 * mm, bottomMargin=18 * mm,
        title=_v((cv.get('personal_details') or {}).get('full_name')) or 'Curriculum Vitae',
        author='Xpress Health',
    )
    W = doc.width
    story = []

    def heading(text):
        story.append(Paragraph(esc(text), st_head))

    # ── Title ─────────────────────────────────────────────────────────
    story.append(Paragraph('CURRICULUM VITAE', st_title))

    # ── Personal details ──────────────────────────────────────────────
    pd = cv.get('personal_details') or {}
    rows = [('Full Name', pd.get('full_name')),
            ('Address', pd.get('address')),
            ('Mobile Number', pd.get('mobile_number')),
            ('Email Address', pd.get('email_address'))]
    rows = [(lbl, _v(val)) for lbl, val in rows if _v(val)]
    if rows:
        heading('PERSONAL DETAILS')
        for lbl, val in rows:
            story.append(Paragraph(
                f"<b>{esc(lbl)}:</b> {esc(val)}", st_body))

    # ── Professional profile ──────────────────────────────────────────
    if _v(cv.get('professional_profile')):
        heading('PROFESSIONAL PROFILE')
        story.append(Paragraph(esc(cv['professional_profile']), st_body))

    # ── Education & qualifications (2-col grid table) ─────────────────
    education = [e for e in (cv.get('education') or [])
                 if _v(e.get('qualification')) or _v(e.get('institution'))]
    if education:
        heading('EDUCATION & QUALIFICATIONS')
        data = [[Paragraph('Qualification', st_cellb),
                 Paragraph('Institution', st_cellb)]]
        for e in education:
            data.append([Paragraph(esc(e.get('qualification')), st_cell),
                         Paragraph(esc(e.get('institution')), st_cell)])
        tbl = Table(data, colWidths=[W * 0.56, W * 0.44], hAlign='LEFT')
        tbl.setStyle(TableStyle([
            ('GRID',       (0, 0), (-1, -1), 0.5, colors.black),
            ('VALIGN',     (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING',(0, 0), (-1, -1), 5),
            ('RIGHTPADDING',(0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))
        story.append(tbl)

    # ── Professional experience ───────────────────────────────────────
    experience = [x for x in (cv.get('professional_experience') or [])
                  if _v(x.get('job_title')) or _v(x.get('employer'))]
    if experience:
        heading('PROFESSIONAL EXPERIENCE')
        for job in experience:
            block = [Paragraph(esc(job.get('job_title')), st_sub)]
            for lbl, key in (('Employer', 'employer'),
                             ('Location', 'location'),
                             ('Dates', 'dates')):
                if _v(job.get(key)):
                    block.append(Paragraph(
                        f"<b>{lbl}:</b> {esc(job.get(key))}", st_body))
            duties = [d for d in (job.get('duties') or []) if _v(d)]
            if duties:
                block.append(Paragraph('<b>Duties &amp; Responsibilities</b>', st_body))
            story.append(KeepTogether(block))
            for d in duties:
                story.append(Paragraph(esc(d).lstrip('•- '), st_bul,
                                       bulletText='\u2022'))

    # ── Previous experience ───────────────────────────────────────────
    previous = [x for x in (cv.get('previous_experience') or [])
                if _v(x.get('job_title')) or _v(x.get('employer'))]
    if previous:
        heading('PREVIOUS EXPERIENCE')
        for job in previous:
            block = [Paragraph(esc(job.get('job_title')), st_sub)]
            for lbl, key in (('Employer', 'employer'),
                             ('Location', 'location'),
                             ('Dates', 'dates')):
                if _v(job.get(key)):
                    block.append(Paragraph(
                        f"<b>{lbl}:</b> {esc(job.get(key))}", st_body))
            story.append(KeepTogether(block))

    # ── Key skills (borderless 3-col table) ───────────────────────────
    skills = [_v(s) for s in (cv.get('key_skills') or []) if _v(s)]
    if skills:
        heading('KEY SKILLS')
        cols = 3
        while len(skills) % cols:
            skills.append('')
        data = [[Paragraph(esc(s), st_cell) for s in skills[i:i + cols]]
                for i in range(0, len(skills), cols)]
        tbl = Table(data, colWidths=[W / cols] * cols, hAlign='LEFT')
        tbl.setStyle(TableStyle([
            ('VALIGN',        (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING',   (0, 0), (-1, -1), 0),
            ('RIGHTPADDING',  (0, 0), (-1, -1), 6),
            ('TOPPADDING',    (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        story.append(tbl)

    # ── Additional information ────────────────────────────────────────
    interests = [_v(i) for i in (cv.get('interests') or []) if _v(i)]
    if interests:
        heading('ADDITIONAL INFORMATION')
        story.append(Paragraph('<b>Interests &amp; Hobbies</b>', st_body))
        for i in interests:
            story.append(Paragraph(esc(i).lstrip('•- '), st_bul,
                                   bulletText='\u2022'))

    # ── References ────────────────────────────────────────────────────
    heading('REFERENCES')
    story.append(Paragraph('Available upon request.', st_ital))
    story.append(Spacer(1, 12))
    story.append(Paragraph(
        'Date: _____________________',
        ParagraphStyle('d', fontName=FONT, fontSize=10, leading=13)))

    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════
# Orchestration + cache
# ══════════════════════════════════════════════════════════════════════

def _generate_for_user(user_id, force=False):
    """
    Returns (payload_dict, http_status).
    payload contains: user, cv (json), meta (extraction info)
    """
    oid = _oid(user_id)
    if oid is None:
        return {"success": False, "error": "Invalid user id"}, 400

    user = _users_col().find_one({"_id": oid})
    if not user:
        return {"success": False, "error": "User not found"}, 404

    cache = _user_cvs_col().find_one({"user_id": str(oid)})
    if cache and cache.get('cv_json') and not force:
        return {
            "success":  True,
            "cached":   True,
            "user_id":  str(oid),
            "name":     _v(user.get('name')),
            "cv":       cache['cv_json'],
            "meta":     cache.get('meta') or {},
            "generated_at": (cache.get('generated_at') or datetime.utcnow()).isoformat(),
        }, 200

    email = _v(user.get('email'))
    extraction = _extract_cv_for_user(email)
    cv_json, gen_err = _build_cv_json(user, extraction.get('cv_text') or '')

    meta = {
        "email":           email,
        "cv_url":          extraction.get('cv_url'),
        "documents_found": extraction.get('documents_found'),
        "extraction_error": extraction.get('error'),
        "generation_error": gen_err,
        "source": ("xn_portal_cv + mongo"
                   if extraction.get('cv_text') else "mongo_only"),
    }

    _user_cvs_col().update_one(
        {"user_id": str(oid)},
        {"$set": {
            "user_id":      str(oid),
            "xn_user_id":   _v(user.get('xn_user_id')),
            "name":         _v(user.get('name')),
            "email":        email,
            "extracted_cv": extraction.get('cv_text') or '',
            "cv_json":      cv_json,
            "meta":         meta,
            "generated_at": datetime.utcnow(),
        }},
        upsert=True,
    )

    return {
        "success":      True,
        "cached":       False,
        "user_id":      str(oid),
        "name":         _v(user.get('name')),
        "cv":           cv_json,
        "meta":         meta,
        "generated_at": datetime.utcnow().isoformat(),
    }, 200


def _cv_filename(cv, fallback='cv'):
    name = _v((cv.get('personal_details') or {}).get('full_name')) or fallback
    safe = re.sub(r'[^A-Za-z0-9]+', '_', name).strip('_') or fallback
    return f"{safe}_CV.pdf"


# ══════════════════════════════════════════════════════════════════════
# Routes
# ══════════════════════════════════════════════════════════════════════

@admin_bp.route('/users/<user_id>/cv/generate', methods=['GET', 'POST'])
@admin_required
def user_cv_generate(user_id):
    """
    Build (or rebuild with ?refresh=1) the structured CV for a user.
    Returns the CV JSON plus extraction metadata.
    """
    try:
        force = request.args.get('refresh') in ('1', 'true', 'yes')
        payload, status = _generate_for_user(user_id, force=force)
        return jsonify(payload), status
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/users/<user_id>/cv/preview', methods=['GET'])
@admin_required
def user_cv_preview(user_id):
    """
    Inline PDF preview — renders in the browser / an <iframe>.
    ?refresh=1  re-runs the XN Portal fetch + Gemini generation.
    """
    try:
        force = request.args.get('refresh') in ('1', 'true', 'yes')
        payload, status = _generate_for_user(user_id, force=force)
        if status != 200 or not payload.get('success'):
            return jsonify(payload), status

        pdf_bytes = _build_cv_pdf(payload['cv'])
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={
                "Content-Disposition":
                    f'inline; filename="{_cv_filename(payload["cv"], user_id)}"',
                "Cache-Control": "no-store",
            },
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/users/<user_id>/cv/download', methods=['GET'])
@admin_required
def user_cv_download(user_id):
    """Same PDF, but as a file download."""
    try:
        force = request.args.get('refresh') in ('1', 'true', 'yes')
        payload, status = _generate_for_user(user_id, force=force)
        if status != 200 or not payload.get('success'):
            return jsonify(payload), status

        pdf_bytes = _build_cv_pdf(payload['cv'])
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={"Content-Disposition":
                     f'attachment; filename="{_cv_filename(payload["cv"], user_id)}"'},
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/users/<user_id>/cv/json', methods=['GET'])
@admin_required
def user_cv_json(user_id):
    """Cached CV JSON — handy for debugging / editing before preview."""
    oid = _oid(user_id)
    if oid is None:
        return jsonify({"success": False, "error": "Invalid user id"}), 400
    cache = _user_cvs_col().find_one({"user_id": str(oid)}, {"_id": 0})
    if not cache:
        return jsonify({"success": False,
                        "error": "No CV generated yet — call /cv/generate"}), 404
    if cache.get('generated_at'):
        cache['generated_at'] = cache['generated_at'].isoformat()
    return jsonify({"success": True, **cache})


@admin_bp.route('/users/<user_id>/cv/documents', methods=['GET'])
@admin_required
def user_cv_documents(user_id):
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