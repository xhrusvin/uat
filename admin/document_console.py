"""
admin/document_console.py
=========================
Search for a candidate, then generate their paperwork.

A single screen that finds a person in `users` (or `live_staffs`) and exposes
every document generator we have for them — preview, download, raw JSON, and
upload to the XN Portal.

Endpoints
---------
GET  /admin/documents                    the console
GET  /admin/documents/<user_id>          the console, opened on one person
GET  /admin/documents/search?q=          JSON search (name / email / phone / PPS / id)
POST /admin/documents/<user_id>/<key>/upload   push a generated PDF to the HSE
                                               document API
GET  /admin/documents/hse-check                probe the HSE upload API
GET  /admin/documents/<user_id>/hse-check      ... and validate their staff ID
POST /admin/documents/<user_id>/hse-test-upload  upload a marked sample file

Adding a generator
------------------
Append an entry to DOCUMENT_TYPES below. Nothing else needs to change — the
UI, the buttons and the upload hook are all driven off that list. Set
"status" to "live" once the routes exist; "planned" renders the card greyed
out with its buttons disabled. Give it an "upload" action and a "view" (the
dotted import path of its upload route) to enable the Upload button.

Upload env (see admin/hse_document_upload.py)
---------------------------------------------
HSE_UPLOAD_URL       base url of the admin API (falls back to LIVE_STAFF_URL)
HSE_UPLOAD_API_KEY   Api-Key header (falls back to XN_PORTAL_API_KEY)
"""

from flask import request, jsonify, render_template, Response
from bson import ObjectId
import os
import re

from database import db
from . import admin_bp
from admin.views import admin_required


# ══════════════════════════════════════════════════════════════════════
# The generator registry — the single place to add a new document type
# ══════════════════════════════════════════════════════════════════════

DOCUMENT_TYPES = [
    {
        "key":    "appform",
        "label":  "Application Form",
        "blurb":  "Personal details, ID checks, references, signed declaration.",
        "base":   "/admin/users/{user_id}/appform",
        "status": "live",
        "actions": {"preview": "/preview", "download": "/download",
                    "json": "/json", "upload": "/upload"},
        "view":   "admin.user_appform:appform_upload",
    },
    {
        "key":    "cv",
        "label":  "HSE CV",
        "blurb":  "Candidate HSE CV rebuilt to the house template.",
        "base":   "/admin/users/{user_id}/cv",
        "status": "live",
        "actions": {"preview": "/preview", "download": "/download",
                    "json": "/json", "upload": "/upload"},
        "view":   "admin.user_cv:user_cv_upload",
    },
    {
        "key":    "point_scale",
        "label":  "Point Scale",
        "blurb":  "Verification of service and salary point assessment.",
        "base":   "/admin/users/{user_id}/point-scale",
        "status": "live",
        "actions": {"preview": "/preview", "download": "/download",
                    "json": "/json", "upload": "/upload"},
        "view":   "admin.user_point_scale:user_point_scale_upload",
    },
    # {
    #     "key":    "hse_cv",
    #     "label":  "HSE CV",
    #     "blurb":  "HSE-format CV for public-sector placements.",
    #     "base":   "/admin/users/{user_id}/hse-cv",
    #     "status": "planned",
    #     "actions": {"preview": "/preview", "download": "/download", "json": "/json"},
    # },
    {
        "key":    "interview",
        "label":  "Interview Screening",
        "blurb":  "Screening call notes and scoring sheet.",
        "base":   "/admin/users/{user_id}/screening-record",
        "status": "live",
        "actions": {"preview": "/preview", "download": "/download",
                    "json": "/json", "upload": "/upload"},
        "view":   "admin.user_screening_record:user_screening_record_upload",
    },
]

# Upload is live — documents_upload() at the bottom of this file dispatches
# to each generator's own /upload route, which POSTs the generated PDF to the
# HSE document upload API (admin/hse_document_upload.py).
UPLOAD_STATUS = "live"


def _doc_types_for(user_id):
    """Resolve the registry into concrete URLs for one person."""
    out = []
    for d in DOCUMENT_TYPES:
        base = d["base"].format(user_id=user_id) if user_id else ''
        out.append({
            "key":    d["key"],
            "label":  d["label"],
            "blurb":  d["blurb"],
            "status": d["status"],
            "urls": {
                name: (base + suffix) if base else ''
                for name, suffix in d["actions"].items()
            },
            "upload_url": (f"/admin/documents/{user_id}/{d['key']}/upload"
                           if user_id else ''),
            "can_upload": bool(d.get('view')) and d["status"] == "live",
        })
    return out


# ══════════════════════════════════════════════════════════════════════
# Search
# ══════════════════════════════════════════════════════════════════════

def _v(val):
    return '' if val is None else str(val).strip()


def _oid(val):
    try:
        return ObjectId(str(val))
    except Exception:
        return None


def _name_of(doc):
    s1 = doc.get('section_1_personal_details') or {}
    return _v(doc.get('name')
              or s1.get('full_name')
              or f"{_v(doc.get('first_name'))} {_v(doc.get('last_name'))}")


def _email_of(doc):
    s1 = doc.get('section_1_personal_details') or {}
    return _v(doc.get('email') or s1.get('email_address'))


def _row(doc, source):
    return {
        "id":         str(doc.get('_id')),
        "source":     source,
        "name":       _name_of(doc) or '(no name)',
        "email":      _email_of(doc),
        "phone":      _v(doc.get('phone')),
        "role":       _v(doc.get('designation') or doc.get('user_type')),
        "xn_id":      _v(doc.get('xn_user_id')
                         or doc.get('staff_id')
                         or doc.get('xn_staff_id')),
        "status":     _v(doc.get('status')),
    }


_PROJECTION = {
    "name": 1, "first_name": 1, "last_name": 1, "email": 1, "phone": 1,
    "designation": 1, "user_type": 1, "xn_user_id": 1, "staff_id": 1,
    "xn_staff_id": 1, "status": 1, "section_1_personal_details": 1,
}


def _search(q, limit=25):
    """Search `users` first, then top up from `live_staffs`."""
    q = _v(q)
    if len(q) < 2:
        return []

    rx = {"$regex": re.escape(q), "$options": "i"}
    oid = _oid(q)
    seen, rows = set(), []

    def collect(collection, source, clauses):
        if len(rows) >= limit:
            return
        try:
            cursor = db[collection].find({"$or": clauses}, _PROJECTION) \
                                   .limit(limit - len(rows))
            for doc in cursor:
                key = (source, str(doc.get('_id')))
                if key in seen:
                    continue
                seen.add(key)
                rows.append(_row(doc, source))
        except Exception:
            pass

    user_clauses = [{"name": rx}, {"first_name": rx}, {"last_name": rx},
                    {"email": rx}, {"phone": rx}, {"pps_number": rx},
                    {"xn_user_id": q}]
    if oid is not None:
        user_clauses.append({"_id": oid})
    collect('users', 'users', user_clauses)

    staff_clauses = [{"name": rx}, {"email": rx},
                     {"section_1_personal_details.full_name": rx},
                     {"section_1_personal_details.email_address": rx},
                     {"staff_id": q}, {"xn_staff_id": q}]
    if oid is not None:
        staff_clauses.append({"_id": oid})
    collect('live_staffs', 'live_staffs', staff_clauses)

    return rows


def _find_one(user_id):
    """Load a single record for the header, trying both collections."""
    oid = _oid(user_id)
    key = _v(user_id)

    probes = [
        ('users', {"_id": oid} if oid else None),
        ('users', {"$or": [{"xn_user_id": key}, {"email": key}]}),
        ('live_staffs', {"_id": oid} if oid else None),
        ('live_staffs', {"$or": [{"staff_id": key}, {"xn_staff_id": key},
                                 {"email": key}]}),
    ]
    for collection, query in probes:
        if query is None:
            continue
        try:
            doc = db[collection].find_one(query, _PROJECTION)
        except Exception:
            doc = None
        if doc:
            return _row(doc, collection)
    return None


# ══════════════════════════════════════════════════════════════════════
# Routes
# ══════════════════════════════════════════════════════════════════════

@admin_bp.route('/documents', methods=['GET'])
@admin_bp.route('/documents/<user_id>', methods=['GET'])
@admin_required
def document_console(user_id=None):
    """The console. Optionally deep-linked to one person."""
    selected = _find_one(user_id) if user_id else None
    return render_template(
        'admin/document_console.html',
        doc_types=_doc_types_for(user_id or '{user_id}'),
        doc_registry=DOCUMENT_TYPES,
        upload_status=UPLOAD_STATUS,
        selected=selected,
        selected_id=user_id or '',
    )


@admin_bp.route('/documents/search', methods=['GET'])
@admin_required
def document_console_search():
    """Type-ahead search behind the console's search box."""
    q = request.args.get('q', '')
    try:
        results = _search(q)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    return jsonify({"success": True, "query": q,
                    "count": len(results), "results": results})


@admin_bp.route('/documents/<user_id>/types', methods=['GET'])
@admin_required
def document_console_types(user_id):
    """Resolved URLs for one person — lets other screens reuse the registry."""
    return jsonify({"success": True, "user_id": user_id,
                    "types": _doc_types_for(user_id),
                    "upload_status": UPLOAD_STATUS})


@admin_bp.route('/documents/hse-check', methods=['GET', 'POST'])
@admin_bp.route('/documents/<user_id>/hse-check', methods=['GET', 'POST'])
@admin_required
def documents_hse_check(user_id=None):
    """
    Check the connection to the HSE document upload API.

    Sends a deliberately incomplete request (everything except the file) so
    the API rejects it at validation — nothing is stored. That round trip
    still proves the URL, TLS and API key are right.

    With a user_id (or ?staff_id=) the probe also asks the user service
    whether that staff ID exists.
    """
    from admin.hse_document_upload import check_hse_upload_connection

    staff_id = _v(request.args.get('staff_id'))
    person   = None

    if not staff_id and user_id:
        person = _find_one(user_id)
        if person is None:
            return jsonify({"success": False,
                            "error": f"No record found for '{user_id}'"}), 404
        staff_id = person.get('xn_id') or ''

    try:
        report = check_hse_upload_connection(staff_id or None)
    except Exception as e:
        return jsonify({"success": False,
                        "error": f"{type(e).__name__}: {e}"}), 500

    if person is not None and not staff_id:
        report['checks'].append({
            "name":   "Staff ID on record",
            "ok":     False,
            "detail": (f"{person.get('name')} has no xn_user_id / staff_id stored, "
                       "so uploads for them will fail"),
        })
        report['ok'] = False
        report['summary'] = ("Connection checked, but this candidate has no "
                             "staff ID on record.")

    return jsonify({"success": True, "user_id": user_id or '',
                    "candidate": person, **report})


@admin_bp.route('/documents/<user_id>/hse-test-upload', methods=['POST'])
@admin_required
def documents_hse_test_upload(user_id):
    """
    Upload a sample PDF against the selected candidate — the full round trip.

    Unlike /hse-check this DOES store a document on the staff record, so the
    file is stamped "TEST UPLOAD — NOT A REAL DOCUMENT" and defaults to the
    others_1 slot. Pass ?type=others_2 (etc) to choose a different slot, or
    ?staff_id= to override the id taken from the record.
    """
    from admin.hse_document_upload import (send_test_document,
                                           SAMPLE_DEFAULT_TYPE,
                                           UPLOADABLE_HSE_TYPES)

    body = request.get_json(silent=True) or {}
    doc_type = (_v(request.args.get('type')) or _v(body.get('type'))
                or SAMPLE_DEFAULT_TYPE)
    if doc_type not in UPLOADABLE_HSE_TYPES:
        return jsonify({"success": False,
                        "error": f"'{doc_type}' is not an uploadable type",
                        "allowed": sorted(UPLOADABLE_HSE_TYPES)}), 400

    person = _find_one(user_id)
    if person is None:
        return jsonify({"success": False,
                        "error": f"No record found for '{user_id}'"}), 404

    staff_id = (_v(request.args.get('staff_id')) or _v(body.get('staff_id'))
                or person.get('xn_id') or '')
    if not staff_id:
        return jsonify({
            "success": False,
            "error": (f"{person.get('name')} has no xn_user_id / staff_id on "
                      "record — pass ?staff_id= to test with a known id"),
            "candidate": person,
        }), 400

    try:
        ok, result = send_test_document(
            staff_id, doc_type,
            note=f"console test for {person.get('name') or user_id}")
    except Exception as e:
        return jsonify({"success": False,
                        "error": f"{type(e).__name__}: {e}"}), 500

    payload = {
        "success":           ok,
        "user_id":           user_id,
        "candidate":         person,
        "staff_id":          staff_id,
        "hse_document_type": doc_type,
        "filename":          result.get('filename'),
        "size_kb":           result.get('size_kb'),
        "upload":            result,
    }
    if not ok:
        payload["error"] = result.get('error')
        return jsonify(payload), result.get('status_code') or 502

    payload["message"] = (f"Sample file uploaded to {person.get('name')} "
                          f"as {doc_type}. It can be deleted from the portal.")
    return jsonify(payload), 200


def _upload_view_for(doc_key):
    """
    Resolve a registry entry's "view" ("module:function") to the callable.
    Imported lazily so the console still loads if one generator module is
    missing or broken.
    """
    entry = next((d for d in DOCUMENT_TYPES if d["key"] == doc_key), None)
    if entry is None:
        return None, f"Unknown document type '{doc_key}'", 404
    if entry.get('status') != 'live':
        return None, f"'{entry['label']}' is not wired up yet", 501

    path = entry.get('view')
    if not path:
        return None, f"'{entry['label']}' has no upload route configured", 501

    module_name, _, func_name = path.partition(':')
    try:
        from importlib import import_module
        view = getattr(import_module(module_name), func_name)
    except Exception as err:
        return None, f"Could not load {path}: {type(err).__name__}: {err}", 500
    return view, None, 200


@admin_bp.route('/documents/<user_id>/<doc_key>/upload', methods=['POST'])
@admin_required
def documents_upload(user_id, doc_key):
    """
    Push a generated document to the HSE document upload API.

    Thin dispatcher — each generator owns its own /upload route, which knows
    its hse_document_type and how to build its PDF. This calls that view
    directly inside the current request context, so query params such as
    ?refresh=1, ?staff_id=, ?assessment_date= and ?regenerate=1 pass straight
    through from the console.
    """
    view, error, status = _upload_view_for(doc_key)
    if view is None:
        return jsonify({"success": False, "error": error,
                        "user_id": user_id, "document": doc_key}), status

    try:
        return view(user_id)
    except Exception as e:
        return jsonify({"success": False,
                        "error": f"{type(e).__name__}: {e}",
                        "user_id": user_id,
                        "document": doc_key}), 500