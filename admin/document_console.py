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
POST /admin/documents/<user_id>/<key>/upload   push a generated PDF to XN Portal

Adding a generator
------------------
Append an entry to DOCUMENT_TYPES below. Nothing else needs to change — the
UI, the buttons and the upload hook are all driven off that list. Set
"status" to "live" once the routes exist; "planned" renders the card greyed
out with its buttons disabled.
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
        "actions": {"preview": "/preview", "download": "/download", "json": "/json"},
    },
    {
        "key":    "cv",
        "label":  "CV",
        "blurb":  "Candidate CV rebuilt to the house template.",
        "base":   "/admin/users/{user_id}/cv",
        "status": "live",
        "actions": {"preview": "/preview", "download": "/download", "json": "/json"},
    },
    {
        "key":    "point_scale",
        "label":  "Point Scale",
        "blurb":  "Verification of service and salary point assessment.",
        "base":   "/admin/users/{user_id}/point-scale",
        "status": "live",
        "actions": {"preview": "/preview", "download": "/download", "json": "/json"},
    },
    {
        "key":    "hse_cv",
        "label":  "HSE CV",
        "blurb":  "HSE-format CV for public-sector placements.",
        "base":   "/admin/users/{user_id}/hse-cv",
        "status": "planned",
        "actions": {"preview": "/preview", "download": "/download", "json": "/json"},
    },
    {
        "key":    "interview",
        "label":  "Interview Screening",
        "blurb":  "Screening call notes and scoring sheet.",
        "base":   "/admin/users/{user_id}/interview-screening",
        "status": "planned",
        "actions": {"preview": "/preview", "download": "/download", "json": "/json"},
    },
]

# Upload to XN Portal is wired for every type but not yet implemented
# server-side — see documents_upload() at the bottom of this file.
UPLOAD_STATUS = "planned"


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


@admin_bp.route('/documents/<user_id>/<doc_key>/upload', methods=['POST'])
@admin_required
def documents_upload(user_id, doc_key):
    """
    Push a generated document to the XN Portal.

    Not implemented yet. When the portal endpoint is confirmed, generate the
    PDF with the matching builder, POST it, and record the result. Flip
    UPLOAD_STATUS to "live" to enable the buttons in the console.
    """
    known = {d["key"] for d in DOCUMENT_TYPES}
    if doc_key not in known:
        return jsonify({"success": False,
                        "error": f"Unknown document type '{doc_key}'"}), 404

    return jsonify({
        "success": False,
        "error": "Upload to XN Portal is not wired up yet.",
        "user_id": user_id,
        "document": doc_key,
    }), 501