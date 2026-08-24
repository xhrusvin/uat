"""
admin/hse_document_upload.py
============================
Common helper to push a generated file (CV, point-scale form, ...) to the
Xpress Health admin HSE document upload API.

    POST {HSE_UPLOAD_URL}/api/admin/staff/hse-document-upload
    multipart/form-data: file, staff_id, hse_document_type

Only the Api-Key header is needed — no bearer token / login.

Env
---
HSE_UPLOAD_URL       base url of the admin API
                     (falls back to LIVE_STAFF_URL, same style as the
                      other modules)
HSE_UPLOAD_API_KEY   Api-Key header (falls back to XN_PORTAL_API_KEY)
XN_APP_COUNTRY       X-App-Country header

Usage
-----
    from admin.hse_document_upload import upload_hse_document, HSE_CV

    ok, resp = upload_hse_document(
        pdf_bytes, "Brian_Long_CV.pdf", staff_id, HSE_CV
    )

To test connectivity without uploading anything:

    from admin.hse_document_upload import check_hse_upload_connection

    report = check_hse_upload_connection(staff_id)   # staff_id optional
"""

import os


# ══════════════════════════════════════════════════════════════════════
# Document types — send the NAME, not the number.
# Only "uploadable" types are accepted by the API; the generated ones
# (health_declaration, police_clearance_self_declaration,
#  hse_clearance_pass, reference_1_2_3) are rejected.
# ══════════════════════════════════════════════════════════════════════

HSE_CV                = 'hse_cv'                 # 3
HSE_APPLICATION_FORM  = 'application_form'       # 4
HSE_POINT_SCALE       = 'point_scale_document'   # 5
HSE_INTERVIEW_NOTES   = 'interview_notes'        # 6
HSE_OTHERS_1          = 'others_1'               # 9
HSE_OTHERS_2          = 'others_2'               # 10
HSE_OTHERS_3          = 'others_3'               # 11
HSE_OTHERS_4          = 'others_4'               # 12
HSE_OTHERS_5          = 'others_5'               # 13
HSE_OTHERS_6          = 'others_6'               # 14
HSE_OTHERS_7          = 'others_7'               # 15

UPLOADABLE_HSE_TYPES = {
    HSE_CV: 3,
    HSE_APPLICATION_FORM: 4,
    HSE_POINT_SCALE: 5,
    HSE_INTERVIEW_NOTES: 6,
    HSE_OTHERS_1: 9,
    HSE_OTHERS_2: 10,
    HSE_OTHERS_3: 11,
    HSE_OTHERS_4: 12,
    HSE_OTHERS_5: 13,
    HSE_OTHERS_6: 14,
    HSE_OTHERS_7: 15,
}

HSE_UPLOAD_PATH = '/api/admin/staff/hse-document-upload'

MAX_UPLOAD_BYTES = 5120 * 1024          # 5120 KB (5 MB)

_MIME_BY_EXT = {
    '.pdf':  'application/pdf',
    '.jpg':  'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.png':  'image/png',
}


def _v(val):
    if val is None:
        return ''
    return str(val).strip()


def _upload_base_url():
    """Same base-url style as the other admin modules."""
    return (os.environ.get('HSE_UPLOAD_URL')
            or os.environ.get('LIVE_STAFF_URL')
            or '').rstrip('/')


def _upload_headers():
    """Api-Key only — Content-Type is left to requests so it sets the boundary."""
    return {
        "Api-Key":       os.environ.get('HSE_UPLOAD_API_KEY')
                         or os.environ.get('XN_PORTAL_API_KEY', ''),
        "X-App-Country": os.environ.get('XN_APP_COUNTRY', ''),
        "Accept":        "application/json",
    }


def _mime_for(filename):
    ext = os.path.splitext(_v(filename).lower())[1]
    return _MIME_BY_EXT.get(ext)


# ══════════════════════════════════════════════════════════════════════
# The common upload function
# ══════════════════════════════════════════════════════════════════════

def upload_hse_document(file_bytes, filename, staff_id,
                        hse_document_type, timeout=60):
    """
    Upload a generated file to the HSE document upload API.

    Parameters
    ----------
    file_bytes        : bytes    the generated file (e.g. PDF from ReportLab)
    filename          : str      original file name, e.g. "Brian_Long_CV.pdf"
    staff_id          : str      existing staff id (validated by the user service)
    hse_document_type : str      enum NAME — use the HSE_* constants above
    timeout           : int      request timeout in seconds

    Returns
    -------
    (ok, result) where:
        ok     : bool
        result : dict — API JSON on success, or
                 {"error": ..., "status_code": ..., "errors": {...}} on failure
    """
    import requests as _req

    base_url = _upload_base_url()
    if not base_url:
        return False, {"error": "HSE_UPLOAD_URL / LIVE_STAFF_URL not set in environment"}

    api_key = _upload_headers().get('Api-Key')
    if not api_key:
        return False, {"error": "HSE_UPLOAD_API_KEY not set in environment"}

    staff_id = _v(staff_id)
    if not staff_id:
        return False, {"error": "staff_id is required"}

    doc_type = _v(hse_document_type)
    if doc_type not in UPLOADABLE_HSE_TYPES:
        return False, {
            "error": ("Invalid HSE document type, or this type is generated "
                      "automatically and cannot be uploaded: "
                      f"'{doc_type}'. Allowed: "
                      f"{', '.join(sorted(UPLOADABLE_HSE_TYPES))}")
        }

    if not file_bytes:
        return False, {"error": "The file field is required."}

    if len(file_bytes) > MAX_UPLOAD_BYTES:
        return False, {
            "error": (f"File is {len(file_bytes) // 1024} KB — the maximum "
                      f"allowed is {MAX_UPLOAD_BYTES // 1024} KB (5 MB).")
        }

    filename = _v(filename) or 'document.pdf'
    mime = _mime_for(filename)
    if not mime:
        return False, {"error": "Allowed file types are pdf, jpeg and png only."}

    endpoint = f"{base_url}{HSE_UPLOAD_PATH}"

    # NOTE: do not set Content-Type manually — requests builds the
    # multipart/form-data boundary itself.
    files = {"file": (filename, file_bytes, mime)}
    data  = {
        "staff_id":          staff_id,
        "hse_document_type": doc_type,      # name, not the number
    }

    try:
        resp = _req.post(endpoint, headers=_upload_headers(),
                         files=files, data=data, timeout=timeout)
    except _req.exceptions.ConnectionError as err:
        return False, {"error": f"Connection error — cannot reach admin API: {str(err)[:200]}"}
    except _req.exceptions.Timeout:
        return False, {"error": f"Timeout — admin API did not respond within {timeout}s"}
    except Exception as err:
        return False, {"error": f"{type(err).__name__}: {str(err)[:200]}"}

    try:
        body = resp.json()
    except Exception:
        body = {"raw": (resp.text or '')[:500]}

    if resp.status_code == 200 and body.get('success'):
        return True, body

    return False, {
        "error":       body.get('message') or f"Upload failed ({resp.status_code})",
        "status_code": resp.status_code,
        "errors":      body.get('errors') or {},
        "response":    body,
    }


# ══════════════════════════════════════════════════════════════════════
# Connection check
#
# Deliberately sends a request the API MUST reject: every field except the
# file. Validation fails before anything is stored, so this proves the URL,
# TLS, API key and (optionally) the staff id are all good without leaving a
# document behind.
# ══════════════════════════════════════════════════════════════════════

def check_hse_upload_connection(staff_id=None, timeout=20):
    """
    Probe the HSE document upload API and report what works.

    Parameters
    ----------
    staff_id : str or None   if given, the probe also validates the id
    timeout  : int           request timeout in seconds

    Returns
    -------
    dict with:
        ok            : bool   True when the endpoint answered as expected
        summary       : str    one-line verdict for the UI
        endpoint      : str    the URL that was probed
        api_key       : str    masked, so a wrong key is visible in a log
        staff_id      : str    the id probed, or ''
        staff_id_ok   : bool or None   None when no staff id was checked
        status_code   : int or None
        elapsed_ms    : int or None
        checks        : list of {name, ok, detail}
        response      : dict   the API's own body, for the JSON view
    """
    import time
    import requests as _req

    checks = []

    def add(name, ok, detail=''):
        checks.append({"name": name, "ok": ok, "detail": detail})

    base_url = _upload_base_url()
    api_key  = _upload_headers().get('Api-Key') or ''
    masked   = (api_key[:4] + '…' + api_key[-4:]) if len(api_key) > 8 else ('set' if api_key else '')
    staff_id = _v(staff_id)

    report = {
        "ok":          False,
        "summary":     '',
        "endpoint":    f"{base_url}{HSE_UPLOAD_PATH}" if base_url else '',
        "api_key":     masked,
        "staff_id":    staff_id,
        "staff_id_ok": None,
        "status_code": None,
        "elapsed_ms":  None,
        "checks":      checks,
        "response":    {},
    }

    # ── Configuration ────────────────────────────────────────────────
    add("Base URL configured", bool(base_url),
        report["endpoint"] or "Set HSE_UPLOAD_URL (or LIVE_STAFF_URL) in the environment")
    add("API key configured", bool(api_key),
        f"Api-Key: {masked}" if api_key
        else "Set HSE_UPLOAD_API_KEY (or XN_PORTAL_API_KEY) in the environment")
    add("Country header", True,
        f"X-App-Country: {os.environ.get('XN_APP_COUNTRY', '') or '(not set)'}")

    if not base_url or not api_key:
        report["summary"] = "Not configured — see the failing checks above."
        return report

    # ── Probe ────────────────────────────────────────────────────────
    # Everything except the file. The API answers 422 "The file field is
    # required." — which is exactly the proof we want.
    data = {"hse_document_type": HSE_CV}
    if staff_id:
        data["staff_id"] = staff_id

    started = time.time()
    try:
        resp = _req.post(report["endpoint"], headers=_upload_headers(),
                         data=data, files={}, timeout=timeout)
    except _req.exceptions.SSLError as err:
        add("Reachable", False, f"TLS error: {str(err)[:200]}")
        report["summary"] = "TLS handshake failed."
        return report
    except _req.exceptions.ConnectionError as err:
        add("Reachable", False, f"Cannot connect: {str(err)[:200]}")
        report["summary"] = "Could not reach the API — check the URL, DNS and any firewall."
        return report
    except _req.exceptions.Timeout:
        add("Reachable", False, f"No response within {timeout}s")
        report["summary"] = "The API did not respond in time."
        return report
    except Exception as err:
        add("Reachable", False, f"{type(err).__name__}: {str(err)[:200]}")
        report["summary"] = "The probe failed before it reached the API."
        return report

    report["elapsed_ms"]  = int((time.time() - started) * 1000)
    report["status_code"] = resp.status_code
    add("Reachable", True, f"HTTP {resp.status_code} in {report['elapsed_ms']} ms")

    try:
        body = resp.json()
    except Exception:
        body = {"raw": (resp.text or '')[:500]}
    report["response"] = body

    errors = body.get('errors') or {}

    # ── Interpret ────────────────────────────────────────────────────
    if resp.status_code in (401, 403):
        add("API key accepted", False,
            body.get('message') or "Rejected — check HSE_UPLOAD_API_KEY")
        report["summary"] = "Reached the API, but the key was rejected."
        return report

    if resp.status_code == 404:
        add("Endpoint exists", False, "404 — the upload path was not found at this base URL")
        report["summary"] = "Wrong base URL — the upload route is not there."
        return report

    if resp.status_code == 422:
        # The expected answer. The file error confirms validation ran.
        add("API key accepted", True, "Reached validation, so the key was accepted")
        add("Endpoint exists", True, "Validation responded as documented")

        if 'file' in errors:
            add("Validation reached", True, "Rejected the empty probe, as expected")
        else:
            add("Validation reached", True,
                body.get('message') or "422 returned without a file error")

        if staff_id:
            bad = 'staff_id' in errors
            report["staff_id_ok"] = not bad
            add("Staff ID valid", not bad,
                (errors['staff_id'][0] if bad else f"{staff_id} accepted by the user service"))
            report["ok"] = not bad
            report["summary"] = ("Connection is good and the staff ID is valid."
                                 if not bad else
                                 "Connection is good, but that staff ID was rejected.")
        else:
            report["ok"] = True
            report["summary"] = ("Connection is good. Pick a candidate to also "
                                 "check their staff ID.")
        return report

    if resp.status_code >= 500:
        add("API healthy", False, body.get('message') or f"HTTP {resp.status_code}")
        report["summary"] = "Reached the API, but it returned a server error."
        return report

    # Anything else — reachable, but not the documented behaviour.
    add("Expected response", False,
        body.get('message') or f"Unexpected HTTP {resp.status_code}")
    report["summary"] = f"Reached the API, but it answered HTTP {resp.status_code}."
    return report