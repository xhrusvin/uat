"""
booking/auto_outreach_cli.py
────────────────────────────
CRUD for the auto_outreach_cli collection.
Each document stores a reference to a client (via xn_client_id).

Routes
------
GET  /booking/auto-outreach-clients           → list page
POST /booking/auto-outreach-clients/add       → create entry
POST /booking/auto-outreach-clients/<id>/delete → soft/hard delete
GET  /booking/auto-outreach-clients/search    → JSON autocomplete for client picker
"""

from datetime import datetime

from bson import ObjectId
from bson.errors import InvalidId
from flask import (
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)

from database import db
from . import bp

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

PER_PAGE = 20


def _client_by_xn_id(xn_client_id: str):
    """Return the clients doc whose xn_client_id matches, or None."""
    return db.clients.find_one({"xn_client_id": xn_client_id})


def _enrich(entries: list) -> list:
    """
    For each auto_outreach_cli entry, join the matching client document
    so the template can display name, email, county, etc.
    """
    xn_ids = [e["xn_client_id"] for e in entries if e.get("xn_client_id")]
    clients = {
        c["xn_client_id"]: c
        for c in db.clients.find(
            {"xn_client_id": {"$in": xn_ids}},
            {
                "xn_client_id": 1,
                "name": 1,
                "email": 1,
                "phone": 1,
                "county": 1,
                "client_type": 1,
                "is_active": 1,
                "status_name": 1,
            },
        )
    }
    for e in entries:
        e["_client"] = clients.get(e.get("xn_client_id")) or {}
    return entries


# ──────────────────────────────────────────────────────────────────────────────
# List (+ search/filter)
# ──────────────────────────────────────────────────────────────────────────────


@bp.route("/auto-outreach-clients")
def auto_outreach_clients():
    page   = max(int(request.args.get("page", 1)), 1)
    search = request.args.get("search", "").strip()

    # ── Build pipeline ────────────────────────────────────────────────
    # Join clients so we can search by client name / email / county too
    pipeline: list = [
        {
            "$lookup": {
                "from": "clients",
                "localField": "xn_client_id",
                "foreignField": "xn_client_id",
                "as": "_c",
            }
        },
        {"$addFields": {"_client": {"$arrayElemAt": ["$_c", 0]}}},
        {"$unset": "_c"},
    ]

    if search:
        rx = {"$regex": search, "$options": "i"}
        pipeline.append(
            {
                "$match": {
                    "$or": [
                        {"xn_client_id": rx},
                        {"_client.name": rx},
                        {"_client.email": rx},
                        {"_client.county": rx},
                        {"_client.client_type": rx},
                    ]
                }
            }
        )

    count_res = list(
        db.auto_outreach_cli.aggregate(pipeline + [{"$count": "total"}])
    )
    total = count_res[0]["total"] if count_res else 0
    pages = max((total + PER_PAGE - 1) // PER_PAGE, 1)

    paginated = pipeline + [
        {"$sort": {"created_at": -1}},
        {"$skip": (page - 1) * PER_PAGE},
        {"$limit": PER_PAGE},
    ]

    raw_entries = list(db.auto_outreach_cli.aggregate(paginated))

    # Convert ObjectId / datetime for template
    entries = []
    for e in raw_entries:
        e["_id_str"] = str(e["_id"])
        c = e.get("_client") or {}
        # Flatten what the template needs
        e["client_name"]  = c.get("name") or "—"
        e["client_email"] = c.get("email") or "—"
        e["client_phone"] = c.get("phone") or "—"
        e["client_county"]= c.get("county") or "—"
        e["client_type"]  = c.get("client_type") or "—"
        e["client_active"]= c.get("is_active", True)
        e["client_status"]= c.get("status_name") or "—"

        raw_ca = e.get("created_at")
        if isinstance(raw_ca, datetime):
            e["created_at_fmt"] = raw_ca.strftime("%d %b %Y %H:%M")
        else:
            e["created_at_fmt"] = "—"

        entries.append(e)

    return render_template(
        "booking/auto_outreach_clients.html",
        entries=entries,
        page=page,
        pages=pages,
        total=total,
        per_page=PER_PAGE,
        search=search,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Add
# ──────────────────────────────────────────────────────────────────────────────


@bp.route("/auto-outreach-clients/add", methods=["POST"])
def auto_outreach_clients_add():
    xn_client_id = (request.form.get("xn_client_id") or "").strip()

    if not xn_client_id:
        flash("Please select a client.", "danger")
        return redirect(url_for("booking.auto_outreach_clients"))

    # Check the client actually exists
    client = _client_by_xn_id(xn_client_id)
    if not client:
        flash(f"Client with ID '{xn_client_id}' not found.", "danger")
        return redirect(url_for("booking.auto_outreach_clients"))

    # Prevent duplicates
    existing = db.auto_outreach_cli.find_one({"xn_client_id": xn_client_id})
    if existing:
        flash(
            f"'{client.get('name', xn_client_id)}' is already in the auto-outreach list.",
            "warning",
        )
        return redirect(url_for("booking.auto_outreach_clients"))

    db.auto_outreach_cli.insert_one(
        {
            "xn_client_id": xn_client_id,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
    )
    flash(
        f"'{client.get('name', xn_client_id)}' added to auto-outreach list.",
        "success",
    )
    return redirect(url_for("booking.auto_outreach_clients"))


# ──────────────────────────────────────────────────────────────────────────────
# Delete
# ──────────────────────────────────────────────────────────────────────────────


@bp.route("/auto-outreach-clients/<entry_id>/delete", methods=["POST"])
def auto_outreach_clients_delete(entry_id: str):
    try:
        oid = ObjectId(entry_id)
    except InvalidId:
        flash("Invalid entry ID.", "danger")
        return redirect(url_for("booking.auto_outreach_clients"))

    entry = db.auto_outreach_cli.find_one({"_id": oid})
    if not entry:
        flash("Entry not found.", "warning")
        return redirect(url_for("booking.auto_outreach_clients"))

    db.auto_outreach_cli.delete_one({"_id": oid})

    # Resolve name for the flash message
    client = _client_by_xn_id(entry.get("xn_client_id", ""))
    name   = client.get("name") if client else entry.get("xn_client_id", "entry")
    flash(f"'{name}' removed from auto-outreach list.", "success")

    # Honour the referring page so pagination is preserved
    return redirect(
        request.referrer or url_for("booking.auto_outreach_clients")
    )


# ──────────────────────────────────────────────────────────────────────────────
# Client autocomplete JSON endpoint
# ──────────────────────────────────────────────────────────────────────────────


@bp.route("/auto-outreach-clients/search")
def auto_outreach_clients_search():
    """
    Returns JSON list of clients whose name / email / county matches `q`.
    Only clients NOT already in auto_outreach_cli are returned.
    """
    q = (request.args.get("q") or "").strip()
    if not q or len(q) < 2:
        return jsonify([])

    # IDs already enrolled
    enrolled_ids = {
        d["xn_client_id"]
        for d in db.auto_outreach_cli.find({}, {"xn_client_id": 1, "_id": 0})
        if d.get("xn_client_id")
    }

    rx     = {"$regex": q, "$options": "i"}
    cursor = db.clients.find(
        {
            "$or": [{"name": rx}, {"email": rx}, {"county": rx}],
            "xn_client_id": {"$nin": list(enrolled_ids)},
            "is_active": True,
        },
        {
            "xn_client_id": 1,
            "name": 1,
            "email": 1,
            "county": 1,
            "client_type": 1,
        },
    ).limit(15)

    results = []
    for c in cursor:
        results.append(
            {
                "xn_client_id": c.get("xn_client_id") or str(c["_id"]),
                "name":         c.get("name") or "",
                "email":        c.get("email") or "",
                "county":       c.get("county") or "",
                "client_type":  c.get("client_type") or "",
            }
        )

    return jsonify(results)
