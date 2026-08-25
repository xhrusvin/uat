"""
admin/user_screening_record.py
==============================
Interview & Agency Screening Record generation for the `users` collection.

Flow
----
1.  Read the user from Mongo (`users`).
2.  Call the XN Portal API -> /ai/recruitments/user-document-list (by email).
3.  If a screening-record document ALREADY exists in that list
    ("Interview Screening Record" / "Screening Record" / similar), stream that
    file back untouched. Nothing is regenerated, nothing is sent to the AI.
4.  Otherwise build the record from scratch:
      a. Extract the CV text (Gemini) -> structured employment history.
      b. Read every other document in the list to populate S2 Documentation
         Review + S3 Registration (NMBI PIN, passport, Garda, training certs)
         from the cached `documents_new` AI results.
      c. Ask Gemini for the seven Interview Notes answers and the Overall
         Assessment, grounded ONLY in the extracted facts.
      d. Compute the compliance verdict + outstanding actions in Python
         (deterministic — the AI never decides whether someone may work).
5.  Render to PDF with ReportLab and stream inline / as a download.

Endpoints
---------
GET/POST  /admin/users/<user_id>/screening-record/generate   build + cache
GET       /admin/users/<user_id>/screening-record/preview    inline PDF
GET       /admin/users/<user_id>/screening-record/download   PDF attachment
GET       /admin/users/<user_id>/screening-record/json       cached JSON (debug)
GET       /admin/users/<user_id>/screening-record/documents  raw doc list (debug)
POST      /admin/users/<user_id>/screening-record/upload     push the PDF to the HSE API

Query params
------------
?refresh=1                 re-run the fetch + Gemini generation
?screening_date=YYYY-MM-DD override the screening date (default: today)
?regenerate=1              ignore an existing uploaded record and build fresh
?interviewer=Name          recruiter name for S1 / S7

Env (identical to user_point_scale.py)
--------------------------------------
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
from datetime import datetime, date
import io
import os
import json
import re

from database import db
from . import admin_bp
from admin.views import admin_required
from admin.hse_document_upload import upload_hse_document, HSE_INTERVIEW_NOTES


# ══════════════════════════════════════════════════════════════════════
# Document taxonomy
#
# Names as they come back from /ai/recruitments/user-document-list. Kept
# lowercase for matching; the display name in the PDF comes from LABEL.
# ══════════════════════════════════════════════════════════════════════

# Any document whose name matches one of these IS the screening record —
# if present we serve it instead of generating.
EXISTING_RECORD_HINTS = (
    'interview screening record',
    'interview & agency screening record',
    'interview and agency screening record',
    'agency screening record',
    'screening record',
    'interview record',
)

# S2 Documentation Review rows, in print order.
# key -> (label, [name fragments that identify it], required?)
DOC_ROWS = [
    ('cv',              'Curriculum Vitae',                        ['cv', 'curriculum'],                     True),
    ('photo_id',        'Photo ID / Passport',                     ['passport', 'photo id', 'national id'],  True),
    ('labour_market',   'Permission to Access Labour Market',      ['permission', 'labour', 'stamp',
                                                                    'immigration', 'work permit'],           False),
    ('nmbi',            'NMBI Registration Certificate',           ['nmbi'],                                 True),
    ('qualification',   'QQI / Nursing Qualification Certificates',['qqi', 'qualification', 'degree',
                                                                    'parchment', 'transcript'],              True),
    ('garda',           'Garda Vetting',                           ['garda', 'vetting'],                     True),
]

# S5 Q6 — mandatory training. key -> (label, name fragments)
TRAINING_ROWS = [
    ('cpr_bls',            'CPR / BLS',                       ['cpr', 'bls', 'basic life']),
    ('manual_handling',    'Manual & People Handling',        ['manual handling', 'people handling']),
    ('ipc',                'Infection Prevention & Control',  ['ipc', 'infection']),
    ('safeguarding',       'Safeguarding Adults at Risk',     ['safeguard']),
    ('medication',         'Medication Management',           ['medication']),
    ('fire_safety',        'Fire Safety',                     ['fire']),
    ('children_first',     'Children First',                  ['children first']),
    ('hand_hygiene',       'Hand Hygiene',                    ['hand hygiene']),
    ('cpi_mapa',           'CPI / MAPA / PMAV',               ['cpi', 'mapa', 'pmav']),
    ('open_disclosure',    'Open Disclosure',                 ['open disclosure']),
]

# Training certs that block placement if absent.
CORE_TRAINING = ('cpr_bls', 'manual_handling', 'ipc', 'safeguarding')

MIN_REFERENCES = 2


# ══════════════════════════════════════════════════════════════════════
# Helpers  (mirrors user_point_scale.py)
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


def _records_col():
    """Cache of generated screening records — one document per user."""
    return db.user_screening_records


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


def _extract_text_from_url(url, headers=None):
    """Re-use the Gemini extractor already written for live_staffs."""
    from admin.live_staffs_crons import _extract_text_from_url as _f
    return _f(url, headers)


def _fmt_date(iso_str):
    """ISO -> dd/mm/yyyy for the form."""
    if not iso_str:
        return ''
    try:
        return datetime.strptime(_v(iso_str)[:10], '%Y-%m-%d').strftime('%d/%m/%Y')
    except Exception:
        return _v(iso_str)


def _fmt_long_date(d):
    """date -> '07 August 2026'."""
    if isinstance(d, str):
        try:
            d = datetime.strptime(d[:10], '%Y-%m-%d').date()
        except Exception:
            return _v(d)
    try:
        return d.strftime('%d %B %Y')
    except Exception:
        return _v(d)


def _matches(name, fragments):
    n = (name or '').strip().lower()
    return any(f in n for f in fragments)


def _parse_json_block(raw):
    """Strip fences and parse. Returns (obj_or_None, error_or_None)."""
    if not raw:
        return None, "Empty AI response"
    txt = re.sub(r'^```(?:json)?\s*', '', _v(raw), flags=re.MULTILINE)
    txt = re.sub(r'```\s*$', '', txt, flags=re.MULTILINE).strip()
    try:
        return json.loads(txt), None
    except json.JSONDecodeError as err:
        return None, f"Malformed JSON from AI: {str(err)[:120]}"


def _gemini_json(prompt, temperature=0.2, max_tokens=4096):
    """Single Gemini call returning parsed JSON. Returns (obj, error)."""
    key = os.environ.get('GEMINI_API_KEY', '')
    if not key:
        return None, "GEMINI_API_KEY not set"
    try:
        from google import genai as _genai
        client = _genai.Client(api_key=key)
        resp = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config={
                "response_mime_type": "application/json",
                "temperature": temperature,
                "max_output_tokens": max_tokens,
            },
        )
        return _parse_json_block(resp.text)
    except Exception as err:
        return None, f"Gemini call failed: {str(err)[:200]}"


# ══════════════════════════════════════════════════════════════════════
# Step 1 — XN Portal: fetch the document list
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


def _find_existing_record(documents):
    """
    Is there already an Interview & Agency Screening Record on file?
    Returns the document dict or None.
    """
    for d in documents or []:
        name = _v(d.get('document_type_name'))
        if name and _matches(name, EXISTING_RECORD_HINTS) and _v(d.get('url')):
            return d
    return None


def _find_doc(documents, fragments):
    """First document matching any fragment, or None."""
    for d in documents or []:
        if _matches(_v(d.get('document_type_name')), fragments):
            return d
    return None


# ══════════════════════════════════════════════════════════════════════
# Step 2 — Documentation review, from the cached AI validation results
#
# We do NOT re-validate here. `documents_new` already holds the per-document
# Gemini verdict written by /api/verify-document. We only read it.
# ══════════════════════════════════════════════════════════════════════

def _ai_result_for(user_oid, doc_name):
    """Latest cached AI validation for one document name."""
    try:
        return db.documents_new.find_one(
            {"user_id": user_oid, "document_type_name": doc_name},
            sort=[("synced_at", -1)],
        )
    except Exception:
        return None


def _status_from(doc, ai_doc):
    """
    Map (portal doc, cached AI verdict) -> (status_word, note).

    Statuses match the paper form: Verified / PENDING / REJECTED / NOT SUPPLIED.
    """
    if not doc or not _v(doc.get('url')):
        return 'NOT SUPPLIED', 'No document uploaded.'

    ai_status = (ai_doc or {}).get('ai_status')
    reason = _v((ai_doc or {}).get('ai_reason'))

    if ai_status in (True, 'true', 'valid'):
        return 'Verified', reason or 'Document sighted and validated.'
    if ai_status in (False, 'false', 'invalid'):
        return 'REJECTED', reason or 'Failed automated validation — manual review required.'
    return 'PENDING', 'Uploaded; awaiting verification.'


def _expiry_from(ai_doc):
    """Pull an expiry_date out of the cached raw AI response, if present."""
    if not ai_doc:
        return ''
    raw = ai_doc.get('ai_raw_response')
    if isinstance(raw, str):
        parsed, _err = _parse_json_block(raw)
    else:
        parsed = raw if isinstance(raw, dict) else None
    if isinstance(parsed, dict):
        return _v(parsed.get('expiry_date'))
    return ''


def _build_documentation_review(user_oid, documents, user):
    """S2 — one row per required document."""
    rows = []
    for key, label, fragments, required in DOC_ROWS:
        doc = _find_doc(documents, fragments)
        ai_doc = _ai_result_for(user_oid, _v(doc.get('document_type_name'))) if doc else None
        status, note = _status_from(doc, ai_doc)

        # Irish / EEA citizens do not need labour-market permission.
        if key == 'labour_market' and status == 'NOT SUPPLIED':
            if _i(user.get('permission_to_work')) == 1 or _i(user.get('work_permit_exemption')) == 1:
                status = 'N/A'
                note = ('Not applicable — candidate holds an unrestricted right to '
                        'work in the State. No employment permit required.')

        expiry = _expiry_from(ai_doc)
        if expiry:
            note = f"{note} Expires {_fmt_date(expiry)}.".strip()

        rows.append({
            "key":      key,
            "label":    label,
            "status":   status,
            "note":     note,
            "url":      _v(doc.get('url')) if doc else '',
            "expiry":   expiry,
            "required": required,
        })
    return rows


def _build_training_review(user_oid, documents):
    """S5 Q6 — mandatory training certificates."""
    rows = []
    for key, label, fragments in TRAINING_ROWS:
        doc = _find_doc(documents, fragments)
        ai_doc = _ai_result_for(user_oid, _v(doc.get('document_type_name'))) if doc else None
        status, note = _status_from(doc, ai_doc)
        expiry = _expiry_from(ai_doc)

        in_date = None
        if expiry:
            try:
                in_date = datetime.strptime(expiry[:10], '%Y-%m-%d').date() >= date.today()
            except Exception:
                in_date = None
            if in_date is False:
                status = 'EXPIRED'
                note = f"Certificate expired {_fmt_date(expiry)}."

        rows.append({
            "key":     key,
            "label":   label,
            "status":  status,
            "expiry":  _fmt_date(expiry),
            "in_date": in_date,
            "note":    note,
            "core":    key in CORE_TRAINING,
        })
    return rows


# ══════════════════════════════════════════════════════════════════════
# Step 3 — Registration + employment history (Gemini extraction only)
# ══════════════════════════════════════════════════════════════════════

_REGISTRATION_SCHEMA = """{
  "nmbi_pin": "",
  "nmbi_division": "",
  "initial_registration_date": "",
  "retention_expiry_date": "",
  "conditions_or_restrictions": "",
  "passport_number": "",
  "passport_issue_date": "",
  "passport_expiry_date": "",
  "passport_issuing_authority": "",
  "nationality": "",
  "place_of_birth": ""
}"""


def _extract_registration(nmbi_text, passport_text):
    """
    Pull the registration and ID identifiers out of the two source documents.
    Extraction only — no inference, no verification.
    """
    if not (nmbi_text or passport_text):
        return {}, "No NMBI or passport text available"

    prompt = f"""You are a compliance document reader for an Irish healthcare staffing agency.

Extract the identifiers below from the supplied document text.

STRICT RULES
* NEVER invent, guess or infer a value. If a field is not stated, return "".
* Do not normalise or correct registration numbers — copy them exactly.
* Dates must be YYYY-MM-DD. If a date is partial or unreadable, return "".
* conditions_or_restrictions: copy any stated conditions. If the document
  states there are none, return "None declared". If silent, return "".

OUTPUT
Return ONLY valid JSON matching exactly this shape — no markdown, no commentary:
{_REGISTRATION_SCHEMA}

---
NMBI DOCUMENT TEXT:
{(nmbi_text or '(not supplied)')[:8000]}
---
PASSPORT / PHOTO ID TEXT:
{(passport_text or '(not supplied)')[:6000]}
---
"""
    obj, err = _gemini_json(prompt, temperature=0.0, max_tokens=1024)
    if err:
        return {}, err
    return (obj if isinstance(obj, dict) else {}), None


_HISTORY_SCHEMA = """{
  "employment": [
    {
      "employer": "",
      "location": "",
      "from_date": "",
      "to_date": "",
      "from_year": 0,
      "from_month": 0,
      "to_year": 0,
      "to_month": 0,
      "is_current": false,
      "role": "",
      "is_registered_role": true
    }
  ]
}"""


def _extract_employment_history(cv_text):
    """S4 — employment rows from the CV. Extraction only."""
    if not cv_text:
        return [], "No CV text to analyse"

    prompt = f"""You are a CV analyser for Irish healthcare staffing compliance screening.

Read the CV text below and extract EVERY work experience entry, in reverse
chronological order (most recent first).

STRICT RULES
* NEVER invent, assume or embellish. Extract only what the CV states.
* Do NOT calculate durations. Extraction only.
* Normalise each start/end date into a numeric year and month (month 1-12).
* If a date gives only a year, use month 1 for the start and month 12 for the end.
* If a role is ongoing ("Present", "To date", "Current"), set is_current = true
  and leave to_year / to_month as 0.
* Keep from_date / to_date as the raw text exactly as written in the CV.
* employer = employer name as written. location = town/county/country as written.
* role = job title as written.
* is_registered_role = true only for roles held as a REGISTERED nurse.
  Health care assistant, support worker, student/intern placement and all
  non-clinical roles are false.
* Include non-healthcare roles too — gaps matter for screening.

OUTPUT
Return ONLY valid JSON matching exactly this shape — no markdown, no commentary:
{_HISTORY_SCHEMA}

If no roles are found, return: {{"employment": []}}

---
CV TEXT:
{cv_text[:20000]}
---
"""
    obj, err = _gemini_json(prompt, temperature=0.0, max_tokens=4096)
    if err:
        return [], err
    rows = obj.get('employment') if isinstance(obj, dict) else obj
    if not isinstance(rows, list):
        return [], "Gemini did not return an employment array"
    return rows, None


# ══════════════════════════════════════════════════════════════════════
# Step 4 — Gaps and overlaps (deterministic)
# ══════════════════════════════════════════════════════════════════════

def _abs_month(year, month):
    return _i(year) * 12 + _i(month)


def _detect_gaps(rows, screening_date, gap_threshold_months=3):
    """
    Find employment gaps and overlaps. Pure arithmetic — the AI is never
    asked to do this, so the output is reproducible and auditable.
    """
    spans = []
    for r in rows or []:
        fy, fm = _i(r.get('from_year')), _i(r.get('from_month')) or 1
        if not fy:
            continue
        if r.get('is_current'):
            ty, tm = screening_date.year, screening_date.month
        else:
            ty, tm = _i(r.get('to_year')), _i(r.get('to_month')) or 12
        if not ty:
            continue
        spans.append({
            "label": f"{_v(r.get('employer'))} ({_v(r.get('from_date'))} – "
                     f"{_v(r.get('to_date')) or 'to date'})",
            "start": _abs_month(fy, fm),
            "end":   _abs_month(ty, tm),
            "employer": _v(r.get('employer')),
        })

    spans.sort(key=lambda s: s['start'])
    findings = []

    for i in range(len(spans) - 1):
        cur, nxt = spans[i], spans[i + 1]
        if nxt['start'] > cur['end'] + 1:
            months = nxt['start'] - cur['end'] - 1
            if months >= gap_threshold_months:
                findings.append({
                    "type": "gap",
                    "months": months,
                    "detail": f"Approx. {months} month gap between {cur['employer']} "
                              f"and {nxt['employer']} — confirm reason and exact dates.",
                })

    for i in range(len(spans)):
        for j in range(i + 1, len(spans)):
            a, b = spans[i], spans[j]
            overlap = min(a['end'], b['end']) - max(a['start'], b['start']) + 1
            if overlap >= 2:
                findings.append({
                    "type": "overlap",
                    "months": overlap,
                    "detail": f"Approx. {overlap} month overlap between {a['employer']} "
                              f"and {b['employer']} — confirm whether concurrent.",
                })

    return findings


def _flag_pre_registration(rows, registration):
    """
    Any registered-nurse role that starts before the NMBI initial registration
    date is a compliance flag.
    """
    reg_raw = _v(registration.get('initial_registration_date'))
    if not reg_raw:
        return []
    try:
        reg = datetime.strptime(reg_raw[:10], '%Y-%m-%d').date()
    except Exception:
        return []

    reg_abs = _abs_month(reg.year, reg.month)
    flags = []
    for r in rows or []:
        if not r.get('is_registered_role'):
            continue
        fy, fm = _i(r.get('from_year')), _i(r.get('from_month')) or 1
        if not fy:
            continue
        start_abs = _abs_month(fy, fm)
        if start_abs < reg_abs:
            flags.append({
                "type": "pre_registration",
                "detail": f"{_v(r.get('employer'))} — registered-nurse role appears to "
                          f"commence {_v(r.get('from_date'))}, before NMBI registration "
                          f"on {_fmt_date(reg_raw)}. Confirm exact start date in writing.",
            })
        elif start_abs == reg_abs:
            flags.append({
                "type": "pre_registration",
                "detail": f"{_v(r.get('employer'))} — start month coincides with NMBI "
                          f"registration ({_fmt_date(reg_raw)}). Confirm the exact start "
                          f"date falls on or after registration.",
            })
    return flags


# ══════════════════════════════════════════════════════════════════════
# Step 5 — References
# ══════════════════════════════════════════════════════════════════════

def _build_references(user):
    """S5 Q7 — from users.references. Duplicates are flagged, not dropped."""
    refs = []
    seen_emails = set()
    for r in user.get('references') or []:
        email = _v(r.get('email')).lower()
        name  = _v(r.get('name'))
        answers = [a for a in (r.get('question_answers') or [])
                   if _v(a.get('answer'))]
        duplicate = bool(email and email.split('@')[0] in
                         {e.split('@')[0] for e in seen_emails}) or \
                    any(name.lower() == _v(x['name']).lower() for x in refs if name)
        if email:
            seen_emails.add(email)

        refs.append({
            "name":         name,
            "job_role":     _v(r.get('job_role')),
            "organization": _v(r.get('organization')),
            "email":        _v(r.get('email')),
            "phone":        f"{_v(r.get('dial_code'))} {_v(r.get('phone'))}".strip(),
            "status":       _v(r.get('status')).upper() or 'PENDING',
            "mail_sent":    bool(r.get('mail_sent')),
            "answered":     len(answers) > 0,
            "duplicate":    duplicate,
        })
    return refs


def _reference_summary(refs):
    approved = [r for r in refs if r['status'] == 'APPROVED' and not r['duplicate']]
    return {
        "total":            len(refs),
        "approved":         len(approved),
        "distinct":         len([r for r in refs if not r['duplicate']]),
        "duplicates_found": any(r['duplicate'] for r in refs),
        "sufficient":       len(approved) >= MIN_REFERENCES,
    }


# ══════════════════════════════════════════════════════════════════════
# Step 6 — Interview Notes (AI) — grounded in extracted facts only
# ══════════════════════════════════════════════════════════════════════

_INTERVIEW_SCHEMA = """{
  "q1_registration": "",
  "q2_clinical_skills": "",
  "q3_right_to_work": "",
  "q4_availability": "",
  "q5_gaps": "",
  "q6_mandatory_training": "",
  "q7_references": "",
  "overall_impression": ""
}"""


def _generate_interview_notes(facts):
    """
    Ask Gemini for the seven Interview Notes answers plus the narrative
    Overall Impression. The model writes prose from a fact sheet; it does
    NOT decide compliance status — that is computed in Python below.
    """
    prompt = f"""You are a senior compliance officer at an Irish healthcare staffing
agency, writing the Interview Notes section of an Interview & Agency Screening
Record. Write in the clipped, factual register of a compliance file — not
marketing prose.

You are given a FACT SHEET assembled from the candidate's verified documents,
CV and profile record. Write the answers to the seven standard screening
questions using ONLY those facts.

ABSOLUTE RULES
* NEVER invent a fact. No invented PINs, dates, employers, certificates or
  competencies.
* Where the fact sheet shows a field as missing, empty or NOT SUPPLIED, say so
  explicitly in capitals, e.g. "NOT EVIDENCED" or "NO CERTIFICATE SUPPLIED",
  and state the action required. Do not soften it and do not speculate.
* Never state that something is compliant, cleared, or safe to place. That
  judgement is made elsewhere.
* Where a claim rests on the candidate's own statement rather than a sighted
  document, say so ("candidate reports…", "to be confirmed against the live
  register").
* Keep each answer to 2-5 sentences. Plain text only — no markdown, no bullets.

QUESTION GUIDANCE
q1_registration      NMBI PIN, division, status, initial registration and
                     retention dates. Note if anything is unsighted.
q2_clinical_skills   Medication administration, IV therapy, and escalation of
                     clinical deterioration. Treat IV competency as NOT
                     EVIDENCED unless an IV certificate appears in the fact
                     sheet. Mention NEWS/IMEWS familiarity as to be confirmed
                     if not evidenced.
q3_right_to_work     Citizenship / permission, whether a permit is required,
                     and any restriction on hours.
q4_availability      Only what the fact sheet states. If absent, write
                     "To be confirmed" and list what the recruiter must ask.
q5_gaps              Restate the supplied gap and overlap findings verbatim in
                     substance. Add nothing that is not listed.
q6_mandatory_training Summarise which certificates are in date, expired, or not
                     supplied. Name the missing ones.
q7_references        Number received, approved, and outstanding. Flag duplicate
                     referees and any reference where the questionnaire is
                     unanswered.
overall_impression   4-6 sentences. Experience profile, career stability, role
                     match, and the main compliance risks. No recommendation
                     verdict.

OUTPUT
Return ONLY valid JSON matching exactly this shape — no markdown, no commentary:
{_INTERVIEW_SCHEMA}

---
FACT SHEET:
{json.dumps(facts, indent=2, default=str)[:24000]}
---
"""
    obj, err = _gemini_json(prompt, temperature=0.2, max_tokens=4096)
    if err:
        return {}, err
    return (obj if isinstance(obj, dict) else {}), None


# ══════════════════════════════════════════════════════════════════════
# Step 7 — Compliance verdict (deterministic)
#
# The AI never decides whether a candidate may be placed. This function does,
# from the document statuses alone, so the verdict is reproducible.
# ══════════════════════════════════════════════════════════════════════

def _assess_compliance(doc_rows, training_rows, ref_summary, user, findings):
    """Returns the S6 block: status, blockers, outstanding actions."""
    blockers = []
    actions = []

    for row in doc_rows:
        if not row['required']:
            continue
        if row['status'] in ('NOT SUPPLIED', 'PENDING', 'REJECTED'):
            blockers.append(f"{row['label']}: {row['status']}")

    garda = next((r for r in doc_rows if r['key'] == 'garda'), None)
    if garda and garda['status'] != 'Verified':
        actions.append("Obtain Garda vetting — initiate immediately (longest lead time).")

    qual = next((r for r in doc_rows if r['key'] == 'qualification'), None)
    if qual and qual['status'] != 'Verified':
        actions.append("Obtain degree parchment and academic transcript.")

    nmbi = next((r for r in doc_rows if r['key'] == 'nmbi'), None)
    if nmbi and nmbi['status'] != 'Verified':
        actions.append("Obtain current NMBI certificate of annual retention and "
                       "verify the PIN against the live register.")

    missing_training = [t['label'] for t in training_rows
                        if t['status'] in ('NOT SUPPLIED', 'PENDING', 'EXPIRED', 'REJECTED')]
    core_missing = [t['label'] for t in training_rows
                    if t['core'] and t['status'] != 'Verified']
    if core_missing:
        blockers.append("Mandatory training outstanding: " + ", ".join(core_missing))
    if missing_training:
        actions.append("Obtain mandatory training certificates with expiry dates: "
                       + ", ".join(missing_training) + ".")

    if not ref_summary['sufficient']:
        blockers.append(
            f"References: {ref_summary['approved']} approved, "
            f"minimum {MIN_REFERENCES} required.")
        actions.append(f"Complete a minimum of {MIN_REFERENCES} approved references "
                       f"from distinct referees at the most recent employers.")
    if ref_summary['duplicates_found']:
        actions.append("Duplicate referee detected — replace with an independent "
                       "referee from a different organisation.")

    if not _v(user.get('eir_code')):
        actions.append("Confirm current Irish address including Eircode.")

    if any(f['type'] == 'pre_registration' for f in findings):
        actions.append("Confirm in writing the exact start dates of any registered-nurse "
                       "role commencing at or before NMBI registration.")
    if any(f['type'] in ('gap', 'overlap') for f in findings):
        actions.append("Clarify the employment gaps and overlaps listed in section 5.")

    on_hold = any('compliance hold' in _v(t.get('name')).lower()
                  for t in (user.get('tags') or []))
    if on_hold:
        blockers.append("Profile is tagged 'On Compliance Hold'.")

    iv_cert = any('iv' in t['key'] for t in training_rows if t['status'] == 'Verified')
    if not iv_cert:
        actions.append("Clarify IV therapy competency and obtain certification if claimed.")

    if blockers:
        status = "NOT YET COMPLIANT — cleared to interview, not cleared to work."
        recommendation = ("PROGRESS — subject to compliance. Recommend proceeding to full "
                          "registration and reference completion. Candidate must not be "
                          "placed on assignment until the outstanding documents are cleared.")
    else:
        status = "COMPLIANT — all required documentation verified."
        recommendation = ("PROGRESS — cleared for placement subject to the usual "
                          "pre-assignment checks.")

    # De-duplicate while preserving order.
    seen = set()
    ordered = [a for a in actions if not (a in seen or seen.add(a))]

    return {
        "compliance_status":   status,
        "blockers":            blockers,
        "outstanding_actions": ordered,
        "recommendation":      recommendation,
        "on_compliance_hold":  on_hold,
    }


# ══════════════════════════════════════════════════════════════════════
# Step 8 — PDF (ReportLab, house form layout)
# ══════════════════════════════════════════════════════════════════════

_FONT_CANDIDATES = [
    ('/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf',
     '/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf',
     '/usr/share/fonts/truetype/dejavu/DejaVuSans-Oblique.ttf'),
    ('/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf',
     '/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf',
     '/usr/share/fonts/truetype/liberation/LiberationSans-Italic.ttf'),
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


# Status -> colour, so a reviewer can scan the form.
_STATUS_COLOURS = {
    'Verified':     '#1B7F3B',
    'N/A':          '#555555',
    'PENDING':      '#B26B00',
    'EXPIRED':      '#B3261E',
    'REJECTED':     '#B3261E',
    'NOT SUPPLIED': '#B3261E',
    'APPROVED':     '#1B7F3B',
}


def _build_screening_pdf(record, user_block):
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
    BLUE = colors.HexColor('#1F5C99')
    GREY = colors.HexColor('#EFEFEF')

    st_brand = ParagraphStyle('br', fontName=FONT_B, fontSize=10, leading=13,
                              alignment=TA_CENTER, textColor=BLUE, spaceAfter=2)
    st_title = ParagraphStyle('t', fontName=FONT_B, fontSize=15, leading=19,
                              alignment=TA_CENTER, spaceAfter=14, textColor=BLUE)
    st_head  = ParagraphStyle('h', fontName=FONT_B, fontSize=11.5, leading=15,
                              alignment=TA_LEFT, spaceBefore=13, spaceAfter=6,
                              textColor=BLUE)
    st_q     = ParagraphStyle('q', fontName=FONT_B, fontSize=9.5, leading=13,
                              spaceBefore=7, spaceAfter=2)
    st_a     = ParagraphStyle('a', fontName=FONT, fontSize=9.5, leading=14,
                              spaceAfter=3, leftIndent=8)
    st_cell  = ParagraphStyle('c', fontName=FONT, fontSize=8.5, leading=11)
    st_cellb = ParagraphStyle('cb', fontName=FONT_B, fontSize=8.5, leading=11)
    st_lbl   = ParagraphStyle('l', fontName=FONT_B, fontSize=9.5, leading=13)
    st_val   = ParagraphStyle('vv', fontName=FONT, fontSize=9.5, leading=13)
    st_ital  = ParagraphStyle('i', fontName=FONT_I, fontSize=8, leading=11,
                              textColor=colors.HexColor('#444444'))

    def status_para(status):
        colour = _STATUS_COLOURS.get(_v(status), '#000000')
        return Paragraph(
            f'<font color="{colour}"><b>{esc(status)}</b></font>', st_cell)

    def grid(data, widths, header=True):
        t = Table(data, colWidths=widths, hAlign='LEFT', repeatRows=1 if header else 0)
        style = [
            ('GRID',          (0, 0), (-1, -1), 0.6, colors.black),
            ('VALIGN',        (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING',   (0, 0), (-1, -1), 5),
            ('RIGHTPADDING',  (0, 0), (-1, -1), 5),
            ('TOPPADDING',    (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]
        if header:
            style.append(('BACKGROUND', (0, 0), (-1, 0), GREY))
        t.setStyle(TableStyle(style))
        return t

    def kv_table(rows, W):
        return grid(
            [[Paragraph(esc(l), st_lbl), Paragraph(esc(_v(v)) or '—', st_val)]
             for l, v in rows],
            [W * 0.38, W * 0.62], header=False)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=17 * mm, rightMargin=17 * mm,
        topMargin=16 * mm, bottomMargin=16 * mm,
        title=f"{_v(user_block.get('name'))} — Interview & Agency Screening Record",
        author='Xpress Health',
    )
    W = doc.width
    story = []

    story.append(Paragraph('XPRESS HEALTH', st_brand))
    story.append(Paragraph('Interview &amp; Agency Screening Record', st_title))

    # ── 1. Candidate details ──────────────────────────────────────────
    cd = record.get('candidate') or {}
    story.append(Paragraph('1. Candidate Details', st_head))
    story.append(kv_table([
        ('Candidate Name (per CV)',            cd.get('name')),
        ('Address',                            cd.get('address')),
        ('Mobile Number',                      cd.get('phone')),
        ('Email Address',                      cd.get('email')),
        ('Role Applied For / Current Assignment', cd.get('role')),
        ('Date of Screening',                  cd.get('screening_date')),
        ('Interviewer / Screener',             cd.get('interviewer')),
        ('Date of Birth',                      cd.get('dob')),
        ('Nationality',                        cd.get('nationality')),
    ], W))

    # ── 2. Documentation review ───────────────────────────────────────
    story.append(Paragraph('2. Documentation Review', st_head))
    data = [[Paragraph('Document', st_cellb),
             Paragraph('Status', st_cellb),
             Paragraph('Notes', st_cellb)]]
    for row in record.get('documentation_review') or []:
        data.append([
            Paragraph(esc(row['label']), st_cell),
            status_para(row['status']),
            Paragraph(esc(row['note']) or '—', st_cell),
        ])
    story.append(grid(data, [W * 0.28, W * 0.16, W * 0.56]))

    # ── 3. Professional registration ──────────────────────────────────
    reg = record.get('registration') or {}
    story.append(Paragraph('3. Professional Registration Verification', st_head))
    story.append(kv_table([
        ('NMBI Registration Number',      reg.get('nmbi_pin')),
        ('Division',                      reg.get('nmbi_division')),
        ('NMBI Status (Active/Lapsed)',   reg.get('status_text')),
        ('Initial Registration Date',     _fmt_date(reg.get('initial_registration_date'))),
        ('Retention Valid Until',         _fmt_date(reg.get('retention_expiry_date'))),
        ('Conditions / Restrictions',     reg.get('conditions_or_restrictions')),
        ('Verified Via NMBI Register (Date)', reg.get('register_check_date')),
        ('Verified By',                   reg.get('verified_by')),
    ], W))
    story.append(Paragraph(
        'Registration details are extracted from the uploaded certificate and must be '
        'confirmed against the live NMBI register before placement.', st_ital))

    # ── 4. Employment history ─────────────────────────────────────────
    story.append(Paragraph('4. Employment History Verification', st_head))
    header = ['Employer', 'Location', 'Dates', 'Verified', 'Notes']
    data = [[Paragraph(h, st_cellb) for h in header]]
    for r in record.get('employment_history') or []:
        dates = f"{_v(r.get('from_date'))} – {_v(r.get('to_date')) or 'present'}"
        data.append([
            Paragraph(esc(r.get('employer')), st_cell),
            Paragraph(esc(r.get('location')), st_cell),
            Paragraph(esc(dates), st_cell),
            status_para(r.get('verified') or 'PENDING'),
            Paragraph(esc(r.get('note')) or '—', st_cell),
        ])
    if len(data) == 1:
        data.append([Paragraph('No employment history extracted', st_cell)] +
                    [Paragraph('', st_cell) for _ in range(4)])
    story.append(grid(data, [W * 0.24, W * 0.16, W * 0.18, W * 0.13, W * 0.29]))

    # ── 5. Interview notes ────────────────────────────────────────────
    notes = record.get('interview_notes') or {}
    story.append(Paragraph('5. Interview Notes', st_head))

    questions = [
        ('1. What is your current NMBI PIN and registration status? Any conditions or restrictions?',
         notes.get('q1_registration')),
        ('2. Do you have experience with medication administration, IV therapy, and escalation of clinical deterioration?',
         notes.get('q2_clinical_skills')),
        ('3. What is your current right-to-work / immigration status, and does it permit the hours/role offered?',
         notes.get('q3_right_to_work')),
        ('4. Availability (days/nights, hours per week, notice period required from current role).',
         notes.get('q4_availability')),
        ('5. Any gaps in employment — please confirm reasons and dates.',
         notes.get('q5_gaps')),
        ('6. Are your BLS, Manual Handling, and Infection Prevention & Control certificates in date?',
         notes.get('q6_mandatory_training')),
        ('7. References — names, relationship, and contact details for three most recent employers.',
         notes.get('q7_references')),
    ]
    for q, a in questions:
        story.append(Paragraph(esc(q), st_q))
        story.append(Paragraph(esc(a) or 'To be confirmed.', st_a))

    # Mandatory training grid supports Q6.
    story.append(Spacer(1, 5))
    data = [[Paragraph('Mandatory Training', st_cellb),
             Paragraph('Status', st_cellb),
             Paragraph('Expiry', st_cellb)]]
    for t in record.get('training_review') or []:
        data.append([
            Paragraph(esc(t['label']), st_cell),
            status_para(t['status']),
            Paragraph(esc(t['expiry']) or '—', st_cell),
        ])
    story.append(grid(data, [W * 0.50, W * 0.28, W * 0.22]))

    # References grid supports Q7.
    story.append(Spacer(1, 6))
    data = [[Paragraph(h, st_cellb) for h in
             ['Referee', 'Role', 'Organisation', 'Contact', 'Status']]]
    for r in record.get('references') or []:
        contact = f"{r['email']}<br/>{r['phone']}".strip()
        name = esc(r['name']) + (' <i>(duplicate)</i>' if r['duplicate'] else '')
        data.append([
            Paragraph(name, st_cell),
            Paragraph(esc(r['job_role']), st_cell),
            Paragraph(esc(r['organization']), st_cell),
            Paragraph(contact, st_cell),
            status_para(r['status']),
        ])
    if len(data) == 1:
        data.append([Paragraph('No references on file', st_cell)] +
                    [Paragraph('', st_cell) for _ in range(4)])
    story.append(grid(data, [W * 0.20, W * 0.17, W * 0.23, W * 0.25, W * 0.15]))

    # ── 6. Overall assessment ─────────────────────────────────────────
    assessment = record.get('assessment') or {}
    story.append(Paragraph('6. Overall Assessment &amp; Recommendation', st_head))

    story.append(Paragraph('Overall Impression', st_q))
    story.append(Paragraph(esc(notes.get('overall_impression')) or '—', st_a))

    story.append(Paragraph('Compliance Status', st_q))
    colour = ('#B3261E' if 'NOT YET' in _v(assessment.get('compliance_status'))
              else '#1B7F3B')
    story.append(Paragraph(
        f'<font color="{colour}"><b>{esc(assessment.get("compliance_status"))}</b></font>',
        st_a))
    for b in assessment.get('blockers') or []:
        story.append(Paragraph(f'&bull; {esc(b)}', st_a))

    story.append(Paragraph('Outstanding Actions', st_q))
    acts = assessment.get('outstanding_actions') or []
    if acts:
        for n, a in enumerate(acts, 1):
            story.append(Paragraph(f'{n}. {esc(a)}', st_a))
    else:
        story.append(Paragraph('None — all required documentation verified.', st_a))

    story.append(Paragraph('Recommendation', st_q))
    story.append(Paragraph(esc(assessment.get('recommendation')) or '—', st_a))

    # ── 7. Sign-off ───────────────────────────────────────────────────
    story.append(Paragraph('7. Sign-Off', st_head))
    sign = record.get('sign_off') or {}
    data = [
        [Paragraph('Interviewer Name / Signature', st_lbl),
         Paragraph(esc(sign.get('interviewer')) or '', st_val),
         Paragraph('Date', st_lbl),
         Paragraph(esc(sign.get('date')) or '', st_val)],
        [Paragraph('Compliance Reviewer Name / Signature', st_lbl),
         Paragraph(esc(sign.get('compliance_reviewer')) or '', st_val),
         Paragraph('Date', st_lbl),
         Paragraph(esc(sign.get('date')) or '', st_val)],
    ]
    story.append(grid(data, [W * 0.34, W * 0.30, W * 0.10, W * 0.26], header=False))

    meta = record.get('meta') or {}
    story.append(Spacer(1, 8))
    story.append(Paragraph(
        'Interview Notes and Overall Impression in this record were drafted by an AI '
        'assistant from the candidate\'s uploaded documents and profile, and require '
        'review and sign-off by a compliance officer before use. Compliance status and '
        'outstanding actions are computed from document verification states. '
        f'Generated {esc(meta.get("generated_at_display"))}.', st_ital))

    doc.build(story)
    return buf.getvalue()


# ══════════════════════════════════════════════════════════════════════
# Orchestration + cache
# ══════════════════════════════════════════════════════════════════════

def _parse_screening_date(raw):
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
        "country":     _lookup_name('countries', user.get('country_id')),
        "xn_user_id":  _v(user.get('xn_user_id')),
    }


def _candidate_block(user, ub, registration, screening_date, interviewer):
    sub_types = user.get('user_sub_type_ids') or []
    role = _v(user.get('job_title')) or ub['designation']
    if sub_types:
        role = f"{role} — {', '.join(_v(s) for s in sub_types)}"

    address = _v(user.get('address')).replace('\n', ', ')
    if _v(user.get('eir_code')):
        address = f"{address}, {_v(user.get('eir_code'))}".strip(', ')

    return {
        "name":            ub['name'],
        "address":         address or 'NOT SUPPLIED',
        "phone":           ub['phone'],
        "email":           ub['email'],
        "role":            role,
        "screening_date":  _fmt_long_date(screening_date),
        "interviewer":     interviewer or 'To be assigned',
        "dob":             _fmt_date(user.get('dob')),
        "nationality":     _v(registration.get('nationality')) or ub['country'] or 'To be confirmed',
        "pps_number":      _v(user.get('pps_number')),
    }


def _registration_block(reg_raw, doc_rows, user):
    nmbi_row = next((r for r in doc_rows if r['key'] == 'nmbi'), None)
    verified = nmbi_row and nmbi_row['status'] == 'Verified'

    expiry = _v(reg_raw.get('retention_expiry_date'))
    status_text = 'NOT EVIDENCED — no NMBI certificate on file'
    if verified and expiry:
        try:
            active = datetime.strptime(expiry[:10], '%Y-%m-%d').date() >= date.today()
            status_text = ('ACTIVE — annual retention certificate valid to '
                           f'{_fmt_date(expiry)}' if active else
                           f'LAPSED — retention expired {_fmt_date(expiry)}')
        except Exception:
            status_text = 'Certificate sighted — retention date unreadable'
    elif verified:
        status_text = 'Certificate sighted — retention date not stated'

    return {
        "nmbi_pin":                   _v(reg_raw.get('nmbi_pin')) or 'NOT SUPPLIED',
        "nmbi_division":              (_v(reg_raw.get('nmbi_division'))
                                       or ', '.join(_v(s) for s in (user.get('user_sub_type_ids') or []))
                                       or 'To be confirmed'),
        "status_text":                status_text,
        "initial_registration_date":  _v(reg_raw.get('initial_registration_date')),
        "retention_expiry_date":      expiry,
        "conditions_or_restrictions": (_v(reg_raw.get('conditions_or_restrictions'))
                                       or 'None declared by candidate; to be confirmed '
                                          'against the live register.'),
        "register_check_date":        'NOT YET CHECKED',
        "verified_by":                'Recruiter',
        "passport_number":            _v(reg_raw.get('passport_number')),
        "passport_expiry_date":       _v(reg_raw.get('passport_expiry_date')),
    }


def _employment_block(rows, findings):
    """Attach a verification state and note to each extracted employment row."""
    flagged = {}
    for f in findings:
        detail = f.get('detail', '')
        for token in re.findall(r'^([^—]+) —', detail):
            flagged[token.strip()] = detail

    out = []
    for r in rows or []:
        employer = _v(r.get('employer'))
        note = flagged.get(employer, '')
        if not r.get('is_registered_role'):
            note = (note + ' Non-registered role.').strip()
        out.append({
            "employer":   employer,
            "location":   _v(r.get('location')),
            "from_date":  _v(r.get('from_date')),
            "to_date":    _v(r.get('to_date')) or ('present' if r.get('is_current') else ''),
            "role":       _v(r.get('role')),
            "verified":   'PENDING',
            "note":       note or 'From CV — written confirmation outstanding.',
            "is_registered_role": bool(r.get('is_registered_role')),
        })
    return out


def _generate_for_user(user_id, force=False, screening_date=None,
                       interviewer=None, regenerate=False):
    """
    Returns (payload_dict, http_status).

    If an Interview & Agency Screening Record already exists in the XN Portal
    document list, the payload carries source == "existing_document" and the
    URL of that file — the caller streams it instead of rendering.
    """
    oid = _oid(user_id)
    if oid is None:
        return {"success": False, "error": "Invalid user id"}, 400

    user = _users_col().find_one({"_id": oid})
    if not user:
        return {"success": False, "error": "User not found"}, 404

    screening_date = screening_date or date.today()
    ub = _user_block(user)

    # ── Cache hit ─────────────────────────────────────────────────────
    cache = _records_col().find_one({"user_id": str(oid)})
    if (cache and cache.get('record') and not force
            and (cache.get('record', {}).get('meta') or {}).get('screening_date')
            == screening_date.isoformat()):
        return {
            "success":      True,
            "cached":       True,
            "user_id":      str(oid),
            "user":         ub,
            "source":       cache.get('source') or 'generated',
            "existing_url": cache.get('existing_url'),
            "record":       cache['record'],
            "generated_at": (cache.get('generated_at') or datetime.utcnow()).isoformat(),
        }, 200

    # ── Document list ─────────────────────────────────────────────────
    documents, doc_err = _fetch_user_documents(ub['email'])

    # ── Existing record wins ──────────────────────────────────────────
    if not regenerate:
        existing = _find_existing_record(documents)
        if existing:
            payload = {
                "success":       True,
                "cached":        False,
                "user_id":       str(oid),
                "user":          ub,
                "source":        "existing_document",
                "existing_url":  _v(existing.get('url')),
                "existing_name": _v(existing.get('document_type_name')),
                "record":        None,
                "message":       ("An Interview & Agency Screening Record already exists "
                                  "for this candidate. Serving the uploaded file. "
                                  "Use ?regenerate=1 to build a fresh record."),
                "generated_at":  datetime.utcnow().isoformat(),
            }
            _records_col().update_one(
                {"user_id": str(oid)},
                {"$set": {
                    "user_id":      str(oid),
                    "xn_user_id":   ub['xn_user_id'],
                    "name":         ub['name'],
                    "email":        ub['email'],
                    "source":       "existing_document",
                    "existing_url": payload['existing_url'],
                    "record":       None,
                    "generated_at": datetime.utcnow(),
                }},
                upsert=True,
            )
            return payload, 200

    # ── Build from scratch ────────────────────────────────────────────
    dl_headers = {k: v for k, v in _api_headers().items() if k != 'Content-Type'}

    def text_of(fragments):
        d = _find_doc(documents, fragments)
        if not d or not _v(d.get('url')):
            return ''
        try:
            txt = _extract_text_from_url(_v(d.get('url')), dl_headers)
        except Exception:
            return ''
        return '' if (txt or '').startswith('[') else _v(txt)

    cv_text       = text_of(['cv', 'curriculum'])
    nmbi_text     = text_of(['nmbi'])
    passport_text = text_of(['passport', 'photo id', 'national id'])

    doc_rows      = _build_documentation_review(oid, documents, user)
    training_rows = _build_training_review(oid, documents)

    reg_raw, reg_err = _extract_registration(nmbi_text, passport_text)
    registration = _registration_block(reg_raw or {}, doc_rows, user)

    hist_raw, hist_err = _extract_employment_history(cv_text)
    findings = _detect_gaps(hist_raw, screening_date)
    findings += _flag_pre_registration(hist_raw, reg_raw or {})
    employment = _employment_block(hist_raw, findings)

    refs = _build_references(user)
    ref_summary = _reference_summary(refs)

    assessment = _assess_compliance(doc_rows, training_rows, ref_summary,
                                    user, findings)

    # Fact sheet for the AI — only verified/extracted values, nothing derived
    # from the AI's own earlier output.
    facts = {
        "candidate": {
            "name":        ub['name'],
            "role":        _v(user.get('job_title')) or ub['designation'],
            "divisions":   [_v(s) for s in (user.get('user_sub_type_ids') or [])],
            "nationality": registration.get('nationality') or ub['country'],
            "experience":  f"{_i(user.get('experience_year'))}y "
                           f"{_i(user.get('experience_month'))}m (self-reported)",
            "permission_to_work":    _i(user.get('permission_to_work')),
            "work_permit_exemption": _i(user.get('work_permit_exemption')),
            "eircode_on_file":       bool(_v(user.get('eir_code'))),
            "availability":          "NOT RECORDED — no availability fields on the profile",
            "notice_period":         "NOT RECORDED",
        },
        "registration":         registration,
        "documentation_review": doc_rows,
        "mandatory_training":   training_rows,
        "employment_history":   employment,
        "gap_findings":         findings,
        "references":           refs,
        "reference_summary":    ref_summary,
        "computed_compliance":  assessment,
    }

    notes, notes_err = _generate_interview_notes(facts)
    notes = notes or {}

    record = {
        "candidate":            _candidate_block(user, ub, reg_raw or {},
                                                 screening_date, interviewer),
        "documentation_review": doc_rows,
        "registration":         registration,
        "employment_history":   employment,
        "training_review":      training_rows,
        "references":           refs,
        "reference_summary":    ref_summary,
        "gap_findings":         findings,
        "interview_notes":      notes,
        "assessment":           assessment,
        "sign_off": {
            "interviewer":         interviewer or '',
            "compliance_reviewer": '',
            "date":                screening_date.strftime('%d / %m / %Y'),
        },
        "meta": {
            "email":                ub['email'],
            "screening_date":       screening_date.isoformat(),
            "documents_found":      len(documents),
            "document_list_error":  doc_err,
            "cv_found":             bool(cv_text),
            "nmbi_text_found":      bool(nmbi_text),
            "passport_text_found":  bool(passport_text),
            "registration_error":   reg_err,
            "history_error":        hist_err,
            "interview_notes_error": notes_err,
            "ai_generated_sections": ["interview_notes", "overall_impression"],
            "generated_at_display": datetime.utcnow().strftime('%d %b %Y %H:%M UTC'),
        },
    }

    _records_col().update_one(
        {"user_id": str(oid)},
        {"$set": {
            "user_id":      str(oid),
            "xn_user_id":   ub['xn_user_id'],
            "name":         ub['name'],
            "email":        ub['email'],
            "designation":  ub['designation'],
            "source":       "generated",
            "existing_url": None,
            "extracted_cv": cv_text,
            "record":       record,
            "generated_at": datetime.utcnow(),
        }},
        upsert=True,
    )

    return {
        "success":      True,
        "cached":       False,
        "user_id":      str(oid),
        "user":         ub,
        "source":       "generated",
        "existing_url": None,
        "record":       record,
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


def _pdf_filename(user_block, fallback='screening_record'):
    name = _v(user_block.get('name')) or fallback
    safe = re.sub(r'[^A-Za-z0-9]+', '_', name).strip('_') or fallback
    return f"{safe}_Interview_Screening_Record.pdf"


def _stream_existing(url, filename, disposition='inline'):
    """Proxy an already-uploaded screening record back to the browser."""
    import requests as _req
    headers = {k: v for k, v in _api_headers().items() if k != 'Content-Type'}
    try:
        resp = _req.get(url, headers=headers, timeout=30, allow_redirects=True)
        resp.raise_for_status()
    except Exception as err:
        return jsonify({"success": False,
                        "error": f"Could not fetch existing record: {str(err)[:200]}",
                        "url": url}), 502

    mimetype = resp.headers.get('Content-Type', 'application/pdf').split(';')[0]
    return Response(
        resp.content,
        mimetype=mimetype or 'application/pdf',
        headers={
            "Content-Disposition": f'{disposition}; filename="{filename}"',
            "Cache-Control": "no-store",
            "X-Record-Source": "existing-document",
        },
    )


# ══════════════════════════════════════════════════════════════════════
# Routes
# ══════════════════════════════════════════════════════════════════════

def _common_args():
    return {
        "force":          request.args.get('refresh') in ('1', 'true', 'yes'),
        "screening_date": _parse_screening_date(request.args.get('screening_date')),
        "interviewer":    _v(request.args.get('interviewer')),
        "regenerate":     request.args.get('regenerate') in ('1', 'true', 'yes'),
    }


@admin_bp.route('/users/<user_id>/screening-record/generate', methods=['GET', 'POST'])
@admin_required
def user_screening_record_generate(user_id):
    """
    Build (or rebuild with ?refresh=1) the screening record for a user.
    If a record already exists on the portal, returns its URL instead.
    """
    try:
        payload, status = _generate_for_user(user_id, **_common_args())
        return jsonify(payload), status
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/users/<user_id>/screening-record/preview', methods=['GET'])
@admin_required
def user_screening_record_preview(user_id):
    """Inline PDF preview — renders in the browser / an <iframe>."""
    try:
        payload, status = _generate_for_user(user_id, **_common_args())
        if status != 200 or not payload.get('success'):
            return jsonify(payload), status

        filename = _pdf_filename(payload['user'], user_id)

        if payload.get('source') == 'existing_document':
            return _stream_existing(payload['existing_url'], filename, 'inline')

        pdf_bytes = _build_screening_pdf(payload['record'], payload['user'])
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={
                "Content-Disposition": f'inline; filename="{filename}"',
                "Cache-Control": "no-store",
                "X-Record-Source": "generated",
            },
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/users/<user_id>/screening-record/download', methods=['GET'])
@admin_required
def user_screening_record_download(user_id):
    """Same PDF, but as a file download."""
    try:
        payload, status = _generate_for_user(user_id, **_common_args())
        if status != 200 or not payload.get('success'):
            return jsonify(payload), status

        filename = _pdf_filename(payload['user'], user_id)

        if payload.get('source') == 'existing_document':
            return _stream_existing(payload['existing_url'], filename, 'attachment')

        pdf_bytes = _build_screening_pdf(payload['record'], payload['user'])
        return Response(
            pdf_bytes,
            mimetype='application/pdf',
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/users/<user_id>/screening-record/json', methods=['GET'])
@admin_required
def user_screening_record_json(user_id):
    """Cached record JSON — handy for debugging / editing before preview."""
    oid = _oid(user_id)
    if oid is None:
        return jsonify({"success": False, "error": "Invalid user id"}), 400
    cache = _records_col().find_one({"user_id": str(oid)}, {"_id": 0})
    if not cache:
        return jsonify({"success": False,
                        "error": "Nothing generated yet — call /screening-record/generate"}), 404
    if cache.get('generated_at'):
        cache['generated_at'] = cache['generated_at'].isoformat()
    return jsonify({"success": True, **cache})


@admin_bp.route('/users/<user_id>/screening-record/documents', methods=['GET'])
@admin_required
def user_screening_record_documents(user_id):
    """Raw XN Portal document list for this user — debugging aid."""
    oid = _oid(user_id)
    if oid is None:
        return jsonify({"success": False, "error": "Invalid user id"}), 400
    user = _users_col().find_one({"_id": oid}, {"email": 1, "name": 1})
    if not user:
        return jsonify({"success": False, "error": "User not found"}), 404

    documents, err = _fetch_user_documents(_v(user.get('email')))
    existing = _find_existing_record(documents)
    return jsonify({
        "success":         err is None,
        "email":           _v(user.get('email')),
        "error":           err,
        "count":           len(documents),
        "documents":       documents,
        "existing_record": existing,
        "has_existing_record": existing is not None,
    })


@admin_bp.route('/users/<user_id>/screening-record/upload', methods=['POST'])
@admin_required
def user_screening_record_upload(user_id):
    """
    Generate the Interview & Agency Screening Record and push it to the HSE
    document upload API as hse_document_type = interview_notes.

    Takes the same query params as the other screening-record routes
    (?refresh=1, ?screening_date=, ?interviewer=, ?regenerate=1), plus
    ?staff_id=... (or a JSON body staff_id) to override the staff id.

    If a screening record already exists on the portal the upload is skipped —
    pass ?regenerate=1 to build and upload a fresh one anyway.
    """
    try:
        oid = _oid(user_id)
        if oid is None:
            return jsonify({"success": False, "error": "Invalid user id"}), 400

        user = _users_col().find_one({"_id": oid})
        if not user:
            return jsonify({"success": False, "error": "User not found"}), 404

        payload, status = _generate_for_user(user_id, **_common_args())
        if status != 200 or not payload.get('success'):
            return jsonify(payload), status

        filename = _pdf_filename(payload['user'], user_id)

        if payload.get('source') == 'existing_document':
            return jsonify({
                "success":      True,
                "uploaded":     False,
                "user_id":      str(oid),
                "filename":     filename,
                "source":       "existing_document",
                "existing_url": payload.get('existing_url'),
                "message":      ("A screening record already exists on the portal — "
                                  "nothing uploaded. Use ?regenerate=1 to build and "
                                  "upload a fresh one."),
            }), 200

        body = request.get_json(silent=True) or {}
        staff_id = (_v(request.args.get('staff_id'))
                    or _v(body.get('staff_id'))
                    or _staff_id_for(user))
        if not staff_id:
            return jsonify({"success": False,
                            "error": "No staff id available for this user"}), 400

        pdf_bytes = _build_screening_pdf(payload['record'], payload['user'])

        ok, result = upload_hse_document(pdf_bytes, filename, staff_id,
                                         HSE_INTERVIEW_NOTES)
        if not ok:
            return jsonify({
                "success":  False,
                "user_id":  str(oid),
                "staff_id": staff_id,
                "filename": filename,
                "upload":   result,
            }), result.get('status_code') or 502

        _records_col().update_one(
            {"user_id": str(oid)},
            {"$set": {
                "uploaded_at":       datetime.utcnow(),
                "uploaded_staff_id": staff_id,
                "upload_response":   result.get('data') or {},
                "hse_document_type": HSE_INTERVIEW_NOTES,
            }},
            upsert=True,
        )

        return jsonify({
            "success":           True,
            "uploaded":          True,
            "user_id":           str(oid),
            "staff_id":          staff_id,
            "filename":          filename,
            "hse_document_type": HSE_INTERVIEW_NOTES,
            "upload":            result,
        }), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500