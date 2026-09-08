"""
booking/live_shifts.py
──────────────────────
Read-only live listing of shifts from the shifts collection.
Supports:
  • Text search  (name, location, shift ID)
  • Date filter
  • Status filter
  • Pagination (10 per page)
  • JSON endpoint for AJAX refresh (/booking/live-shifts/data)
  • Availability column — green tick if any shifts_users row for the shift
    has availability == 1
"""

from datetime import datetime
from flask import render_template, request, jsonify
from bson import ObjectId

from database import db
from . import bp


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _serialize(doc):
    """Recursively convert MongoDB doc to JSON-safe dict."""
    if isinstance(doc, dict):
        return {k: _serialize(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [_serialize(i) for i in doc]
    if isinstance(doc, ObjectId):
        return str(doc)
    if isinstance(doc, datetime):
        return doc.isoformat()
    return doc


def _build_pipeline(search: str, date_filter_str: str, status_filter: str):
    """
    Aggregation pipeline:
      1. Optional date pre-filter
      2. $lookup → shifts_users to detect any availability == 1 entry
      3. Optional text / status post-filter
    """
    base_match = {}
    if date_filter_str:
        try:
            base_match["date"] = datetime.strptime(date_filter_str, "%Y-%m-%d")
        except ValueError:
            pass

    pipeline = [
        {"$match": base_match},

        # ── Join shifts_users on shift_id == _id ──────────────────────
        # shifts_users.shift_id is stored as ObjectId so we match directly.
        {
            "$lookup": {
                "from": "shifts_users",
                "let": {"sid": "$_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {"$eq": ["$shift_id", "$$sid"]},
                            "availability": 1          # only interested in available rows
                        }
                    },
                    {"$limit": 1},                     # stop at first hit — we only need existence
                    {"$project": {"_id": 1}}
                ],
                "as": "available_users"
            }
        },

        # ── Derive boolean: true when at least one available user found ─
        {
            "$addFields": {
                "has_availability": {
                    "$gt": [{"$size": "$available_users"}, 0]
                }
            }
        },
        {"$unset": "available_users"},
    ]

    # ── Text search (no client lookup needed — client column removed) ──
    or_clauses = []
    if search:
        rx = {"$regex": search, "$options": "i"}
        or_clauses = [
            {"name": rx},
            {"location": rx},
            {"shift_xn_id": rx},
            {"description": rx},
        ]

    combined = {}
    if or_clauses:
        combined["$or"] = or_clauses
    if status_filter:
        combined["status"] = status_filter
    if combined:
        pipeline.append({"$match": combined})

    # ── Project only what the template needs ──────────────────────────
    pipeline.append({
        "$project": {
            "_id": 1,
            "name": 1,
            "date": 1,
            "start_time": 1,
            "end_time": 1,
            "shift_xn_id": 1,
            "description": 1,
            "location": 1,
            "postal_code": 1,
            "is_premium": 1,
            "status": 1,
            "rate": 1,
            "slots": 1,
            "created_at": 1,
            "updated_at": 1,
            "has_availability": 1,     # ← new
        }
    })

    return pipeline


def _format_shifts(shifts_list: list) -> list:
    """
    Post-process each shift document:
      • Format start/end times as HH:MM strings
      • Normalise slots for the template
    """
    for s in shifts_list:
        # ── Time formatting ────────────────────────────────────────────
        if isinstance(s.get("start_time"), datetime):
            s["start_time_formatted"] = s["start_time"].strftime("%H:%M")
        else:
            s["start_time_formatted"] = s.get("start_time") or "—"

        if isinstance(s.get("end_time"), datetime):
            s["end_time_formatted"] = s["end_time"].strftime("%H:%M")
        else:
            s["end_time_formatted"] = s.get("end_time") or "—"

        # ── Date formatting ────────────────────────────────────────────
        raw_date = s.get("date")
        if isinstance(raw_date, datetime):
            s["date_formatted"] = raw_date.strftime("%d %b %Y")
        elif isinstance(raw_date, str) and raw_date:
            s["date_formatted"] = raw_date.split("T")[0]
        else:
            s["date_formatted"] = "—"

        # ── Slots normalisation ────────────────────────────────────────
        slots_out = []
        for slot in (s.get("slots") or []):
            slot_date = slot.get("date")
            if isinstance(slot_date, datetime):
                date_str     = slot_date.strftime("%Y-%m-%d")
                date_display = slot_date.strftime("%d %b %Y")
            elif isinstance(slot_date, dict) and "$date" in slot_date:
                try:
                    dt = datetime.fromisoformat(
                        slot_date["$date"].replace("Z", "+00:00")
                    )
                    date_str     = dt.strftime("%Y-%m-%d")
                    date_display = dt.strftime("%d %b %Y")
                except Exception:
                    date_str = date_display = ""
            elif isinstance(slot_date, str):
                date_str     = slot_date.split("T")[0] if "T" in slot_date else slot_date
                date_display = date_str
            else:
                date_str = date_display = ""

            slots_out.append({
                "date":         date_str,
                "date_display": date_display,
                "start_time":   slot.get("start_time") or "—",
                "end_time":     slot.get("end_time")   or "—",
                "shift_xn_id":  slot.get("shift_xn_id") or "—",
                "shift_type":   slot.get("shift_type")  or "",
            })

        s["slots_normalised"] = slots_out

    return shifts_list


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

VALID_STATUSES = ["To be assigned", "Assigned", "Completed", "Cancelled"]
PER_PAGE = 10


@bp.route("/live-shifts")
def live_shifts():
    """Renders the live shift listing page."""
    page            = max(int(request.args.get("page", 1)), 1)
    search          = request.args.get("search", "").strip()
    date_filter_str = request.args.get("date_filter", "").strip()
    status_filter   = request.args.get("status_filter", "").strip()

    if status_filter not in VALID_STATUSES:
        status_filter = ""

    pipeline = _build_pipeline(search, date_filter_str, status_filter)

    count_result = list(db.shifts.aggregate(pipeline + [{"$count": "total"}]))
    total        = count_result[0]["total"] if count_result else 0

    paginated = pipeline + [
        {"$sort": {"date": -1, "created_at": -1}},
        {"$skip":  (page - 1) * PER_PAGE},
        {"$limit": PER_PAGE},
    ]

    shifts_list = _format_shifts(list(db.shifts.aggregate(paginated)))
    pages       = max((total + PER_PAGE - 1) // PER_PAGE, 1)

    return render_template(
        "booking/live_shifts.html",
        shifts        = shifts_list,
        page          = page,
        total         = total,
        per_page      = PER_PAGE,
        pages         = pages,
        search        = search,
        date_filter   = date_filter_str,
        status_filter = status_filter,
        valid_statuses= VALID_STATUSES,
    )


@bp.route("/live-shifts/data")
def live_shifts_data():
    """JSON endpoint for AJAX polling / auto-refresh."""
    page            = max(int(request.args.get("page", 1)), 1)
    search          = request.args.get("search", "").strip()
    date_filter_str = request.args.get("date_filter", "").strip()
    status_filter   = request.args.get("status_filter", "").strip()

    if status_filter not in VALID_STATUSES:
        status_filter = ""

    pipeline     = _build_pipeline(search, date_filter_str, status_filter)
    count_result = list(db.shifts.aggregate(pipeline + [{"$count": "total"}]))
    total        = count_result[0]["total"] if count_result else 0

    paginated = pipeline + [
        {"$sort": {"date": -1, "created_at": -1}},
        {"$skip":  (page - 1) * PER_PAGE},
        {"$limit": PER_PAGE},
    ]

    shifts_list = _format_shifts(list(db.shifts.aggregate(paginated)))

    return jsonify({
        "success": True,
        "shifts":  _serialize(shifts_list),
        "total":   total,
        "page":    page,
        "pages":   max((total + PER_PAGE - 1) // PER_PAGE, 1),
    })