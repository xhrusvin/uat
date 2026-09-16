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
    """Stream all matching users (no pagination) as a CSV download."""
    search = request.args.get("search", "").strip()
    query  = _build_query(search)

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
    )

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "First Name", "Last Name", "Email", "XN User ID", "Active", "Created At"
    ])

    for u in raw:
        f = _format_user(u)
        writer.writerow([
            f["first_name"],
            f["last_name"],
            f["email"],
            f["xn_user_id"],
            "Yes" if f["is_active"] else "No",
            f["created_fmt"],
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
