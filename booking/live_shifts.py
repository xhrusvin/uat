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
  • Availability column — green tick if:
      PRIMARY  : any shifts_users row for the shift has availability == 1, OR
      FALLBACK : any shifts_group_users.availability_details[] element has
                 shift_id == str(shift._id) AND availability == 1
  • O.Date column — latest outreach date:
      PRIMARY  : outreach.shift_id (ObjectId) == shifts._id → latest created_at
      FALLBACK : shifts_group.shift_ids contains shifts._id
                 → outreach_shift_group.group_id == shifts_group._id → latest created_at
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
      1.  Optional date pre-filter
      2.  $lookup → shifts_users          (availability primary)
      3.  $lookup → shifts_group_users    (availability fallback via availability_details[])
      4.  has_availability derived
      5.  $lookup → outreach              (outreach_date primary)
              outreach.shift_id (ObjectId) == shifts._id  → latest created_at
      6.  $lookup → shifts_group          (outreach_date fallback step A)
              shifts_group.shift_ids contains shifts._id
      7.  $lookup → outreach_shift_group  (outreach_date fallback step B)
              outreach_shift_group.group_id == shifts_group._id → latest created_at
      8.  outreach_date = primary hit  OR  fallback hit  (prefer primary)
      9.  Optional text / status post-filter
      10. $project
    """
    base_match = {}
    if date_filter_str:
        try:
            base_match["date"] = datetime.strptime(date_filter_str, "%Y-%m-%d")
        except ValueError:
            pass

    pipeline = [
        {"$match": base_match},

        # ════════════════════════════════════════════════════════════════
        # AVAILABILITY — PRIMARY
        # shifts_users.shift_id (ObjectId) == shifts._id, availability == 1
        # ════════════════════════════════════════════════════════════════
        {
            "$lookup": {
                "from": "shifts_users",
                "let":  {"sid": "$_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {"$eq": ["$shift_id", "$$sid"]},
                            "availability": 1
                        }
                    },
                    {"$limit": 1},
                    {"$project": {"_id": 1}}
                ],
                "as": "_avail_primary"
            }
        },

        # ════════════════════════════════════════════════════════════════
        # AVAILABILITY — FALLBACK
        # shifts_group_users.availability_details[].shift_id (string) == str(shifts._id)
        # AND availability_details[].availability == 1
        # ════════════════════════════════════════════════════════════════
        {
            "$lookup": {
                "from": "shifts_group_users",
                "let":  {"sid_str": {"$toString": "$_id"}},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$gt": [
                                    {
                                        "$size": {
                                            "$filter": {
                                                "input": {"$ifNull": ["$availability_details", []]},
                                                "as":    "d",
                                                "cond": {
                                                    "$and": [
                                                        {"$eq": ["$$d.shift_id",    "$$sid_str"]},
                                                        {"$eq": ["$$d.availability", 1]}
                                                    ]
                                                }
                                            }
                                        }
                                    },
                                    0
                                ]
                            }
                        }
                    },
                    {"$limit": 1},
                    {"$project": {"_id": 1}}
                ],
                "as": "_avail_fallback"
            }
        },

        # Derive has_availability
        {
            "$addFields": {
                "has_availability": {
                    "$or": [
                        {"$gt": [{"$size": "$_avail_primary"},  0]},
                        {"$gt": [{"$size": "$_avail_fallback"}, 0]}
                    ]
                }
            }
        },
        {"$unset": ["_avail_primary", "_avail_fallback"]},

        # ════════════════════════════════════════════════════════════════
        # OUTREACH DATE — PRIMARY
        # outreach.shift_id (ObjectId) == shifts._id
        # Pick the latest outreach.created_at
        # ════════════════════════════════════════════════════════════════
        {
            "$lookup": {
                "from": "outreach",
                "let":  {"sid": "$_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {"$eq": ["$shift_id", "$$sid"]}
                        }
                    },
                    {"$sort":    {"created_at": -1}},
                    {"$limit":   1},
                    {"$project": {"created_at": 1, "_id": 0}}
                ],
                "as": "_outreach_primary"
            }
        },

        # ════════════════════════════════════════════════════════════════
        # OUTREACH DATE — FALLBACK  (two-hop join)
        #   Step A: find shifts_group docs whose shift_ids[] contains shifts._id
        #   Step B: for each such group, look up outreach_shift_group on group_id
        #           and grab the latest created_at
        # ════════════════════════════════════════════════════════════════

        # Step A — shifts_group
        {
            "$lookup": {
                "from": "shifts_group",
                "let":  {"sid": "$_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {"$in": ["$$sid", {"$ifNull": ["$shift_ids", []]}]}
                        }
                    },
                    {"$project": {"_id": 1}}
                ],
                "as": "_shift_groups"
            }
        },

        # Step B — outreach_shift_group matched on group_id IN _shift_groups._id
        {
            "$lookup": {
                "from": "outreach_shift_group",
                "let":  {"group_ids": "$_shift_groups._id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {"$in": ["$group_id", {"$ifNull": ["$$group_ids", []]}]}
                        }
                    },
                    {"$sort":    {"created_at": -1}},
                    {"$limit":   1},
                    {"$project": {"created_at": 1, "_id": 0}}
                ],
                "as": "_outreach_fallback"
            }
        },

        # Derive outreach_date: prefer primary; fall back to group outreach
        {
            "$addFields": {
                "outreach_date": {
                    "$cond": {
                        "if":   {"$gt": [{"$size": "$_outreach_primary"}, 0]},
                        "then": {"$arrayElemAt": ["$_outreach_primary.created_at", 0]},
                        "else": {
                            "$cond": {
                                "if":   {"$gt": [{"$size": "$_outreach_fallback"}, 0]},
                                "then": {"$arrayElemAt": ["$_outreach_fallback.created_at", 0]},
                                "else": None
                            }
                        }
                    }
                }
            }
        },
        {"$unset": ["_outreach_primary", "_outreach_fallback", "_shift_groups"]},
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
            "has_availability": 1,
            "outreach_date": 1,
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

        # ── Outreach date formatting ───────────────────────────────────
        raw_od = s.get("outreach_date")
        if isinstance(raw_od, datetime):
            s["outreach_date_formatted"] = raw_od.strftime("%d %b %Y %H:%M")
        elif isinstance(raw_od, str) and raw_od:
            # ISO string from _serialize — reformat nicely
            try:
                dt = datetime.fromisoformat(raw_od.replace("Z", "+00:00"))
                s["outreach_date_formatted"] = dt.strftime("%d %b %Y %H:%M")
            except Exception:
                s["outreach_date_formatted"] = raw_od[:16]
        else:
            s["outreach_date_formatted"] = None

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