"""
booking/care_learning_users.py
──────────────────────────────
Read-only listing of users from the users collection.

Fields shown
------------
  email · first_name · last_name · xn_user_id

Supports
--------
  • Text search (name, email, xn_user_id)
  • Pagination (20 per page)
  • JSON endpoint  GET /booking/care-learning-users/data
  • CSV export     GET /booking/care-learning-users/export-csv
"""

import csv
import io
from datetime import datetime

from bson import ObjectId
from flask import Response, jsonify, render_template, request

from database import db
from . import bp

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

PER_PAGE = 20


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _build_query(search: str) -> dict:
    """Return a MongoDB filter dict for the given search term."""
    if not search:
        return {}
    rx = {"$regex": search, "$options": "i"}
    return {
        "$or": [
            {"first_name":  rx},
            {"last_name":   rx},
            {"email":       rx},
            {"xn_user_id":  rx},
        ]
    }


def _format_user(u: dict) -> dict:
    """Flatten / format a raw users document for the template."""
    first = (u.get("first_name") or "").strip()
    last  = (u.get("last_name")  or "").strip()
    full  = f"{first} {last}".strip() or "—"

    raw_ca = u.get("created_at")
    if isinstance(raw_ca, datetime):
        created_fmt = raw_ca.strftime("%d %b %Y")
    else:
        created_fmt = "—"

    return {
        "_id_str":      str(u["_id"]),
        "first_name":   first or "—",
        "last_name":    last  or "—",
        "full_name":    full,
        "email":        u.get("email")      or "—",
        "xn_user_id":   u.get("xn_user_id") or "—",
        "is_active":    u.get("is_active", True),
        "created_fmt":  created_fmt,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@bp.route("/care-learning-users")
def care_learning_users():
    """Render the paginated user listing page."""
    page   = max(int(request.args.get("page", 1)), 1)
    search = request.args.get("search", "").strip()

    query = _build_query(search)
    total = db.care_learning_users.count_documents(query)
    pages = max((total + PER_PAGE - 1) // PER_PAGE, 1)

    raw = (
        db.care_learning_users
        .find(query, {
            "first_name":  1,
            "last_name":   1,
            "email":       1,
            "xn_user_id":  1,
            "is_active":   1,
            "created_at":  1,
        })
        .sort([("first_name", 1), ("last_name", 1)])
        .skip((page - 1) * PER_PAGE)
        .limit(PER_PAGE)
    )

    users = [_format_user(u) for u in raw]

    return render_template(
        "booking/care_learning_users.html",
        users   = users,
        page    = page,
        pages   = pages,
        total   = total,
        per_page= PER_PAGE,
        search  = search,
    )


@bp.route("/care-learning-users/data")
def care_learning_users_data():
    """JSON endpoint — used by the auto-refresh badge polling."""
    page   = max(int(request.args.get("page", 1)), 1)
    search = request.args.get("search", "").strip()

    query = _build_query(search)
    total = db.care_learning_users.count_documents(query)

    raw = (
        db.care_learning_users
        .find(query, {
            "first_name": 1,
            "last_name":  1,
            "email":      1,
            "xn_user_id": 1,
            "is_active":  1,
            "created_at": 1,
        })
        .sort([("first_name", 1), ("last_name", 1)])
        .skip((page - 1) * PER_PAGE)
        .limit(PER_PAGE)
    )

    users = [_format_user(u) for u in raw]

    return jsonify({
        "success": True,
        "users":   users,
        "total":   total,
        "page":    page,
        "pages":   max((total + PER_PAGE - 1) // PER_PAGE, 1),
    })


@bp.route("/care-learning-users/export-csv")
def care_learning_users_export_csv():
    """
    Export CSV — one row per document per user.

    Columns:
      Name | Email | Document Type | Care Learning Found
    """
    search = request.args.get("search", "").strip()
    query  = _build_query(search)

    # ── Fetch all matching users ──────────────────────────────────────
    raw_users = list(
        db.care_learning_users
        .find(query, {
            "first_name": 1,
            "last_name":  1,
            "name":       1,   # some docs may use a single name field
            "email":      1,
            "xn_user_id": 1,
            "phone":      1,
        })
        .sort([("first_name", 1), ("last_name", 1)])
    )

    # ── Ordered list of all 27 allowed document types ────────────────
    # Every user gets one row per type; value filled from DB or "—".
    ALLOWED_ORDERED = [
        "Infection Prevention Control Certificate",
        "Ppe",
        "Hand Hygiene",
        "Children First",
        "Safeguarding Adults At Risk",
        "Cpr/Bls",
        "Manual And People Handling Documents",
        "The Open Disclosure",
        "Cpi/ Mapa/Pmav",
        "Cyber Security",
        "Gdpr",
        "Dignity At Work",
        "Fire Safety",
        "QQI Level 5 or equivalent in Health Service Skills or Healthcare Support",
        "Managing Feeding, Eating, Drinking And Swallowing In People With An Intellectual Disability",
        "Enhanced Declaration Of Risk Assessment",
        "Applying A Human Rights-Based Approach In Health And Social Care",
        "Supporting Decision Making In Health & Social Care",
        "Hse National Consent Policy V1.2",
        "Sepsis Management",
        "Hse National Consent Policy V1.1",
        "Occupational Health",
        "Hse Effective Complaints Handling",
        "Medication Administration",
        "Neurogenic Bowel Dysfunction Training (Practical)",
        "Management Of Blood & Body Substance Spills",
        "Haccp/Food Safety",
    ]

    # ── Collect user_ids and build a lookup map ───────────────────────
    # lookup: user_id → { normalised_doc_type_name → care_learning_found }
    from collections import defaultdict
    user_id_strs = [str(u["_id"]) for u in raw_users]

    doc_cursor = db.care_learning_document.find(
        {"user_id": {"$in": user_id_strs}},
        {"user_id": 1, "document_type_name": 1, "care_learning_found": 1},
    )

    docs_by_user = defaultdict(dict)
    for d in doc_cursor:
        key = (d.get("document_type_name") or "").strip().lower()
        docs_by_user[d["user_id"]][key] = d.get("care_learning_found") or "—"

    # ── Write CSV ─────────────────────────────────────────────────────
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Name", "Email", "Phone", "Document Type", "Care Learning Found"])

    for u in raw_users:
        uid   = str(u["_id"])
        first = (u.get("first_name") or "").strip()
        last  = (u.get("last_name")  or "").strip()
        name  = f"{first} {last}".strip() or u.get("name") or "—"
        email = u.get("email") or "—"

        saved = docs_by_user.get(uid, {})   # normalised_name → care_learning_found

        # One row per allowed type — always all 27 rows per user
        phone = u.get("phone") or "—"   # ✅ added above the loop

        for i, doc_type in enumerate(ALLOWED_ORDERED):
          found = saved.get(doc_type.strip().lower(), "—")
          writer.writerow([
            name  if i == 0 else "",
            email if i == 0 else "",
            phone if i == 0 else "",   # ✅ added
            doc_type,
            found,
    ])

    ts    = datetime.utcnow().strftime("%Y%m%d_%H%M")
    fname = f"care_learning_users_{ts}.csv"
    output.seek(0)

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={fname}",
            "Content-Type": "text/csv; charset=utf-8",
        },
    )
