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
  • CSV export of all filtered results (/booking/live-shifts/export-csv)
  • Availability column — green tick if:
      PRIMARY  : any shifts_users row for the shift has availability == 1, OR
      FALLBACK : any shifts_group_users.availability_details[] element has
                 shift_id == str(shift._id) AND availability == 1
  • O.Date column — taken directly from shifts.last_outreach_date
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


def _build_pipeline(
    search: str,
    shift_date_from: str,
    shift_date_to: str,
    outreach_date_from: str,
    outreach_date_to: str,
    status_filter: str,
):
    """
    Aggregation pipeline:
      1.  Optional shift-date range pre-filter (shift_date_from / shift_date_to)
      2.  $lookup → shifts_users          (availability primary)
      3.  $lookup → shifts_group_users    (availability fallback via availability_details[])
      4.  has_availability derived
      5.  outreach_date sourced directly from shifts.last_outreach_date
      6.  Optional outreach_date range post-filter (outreach_date_from / outreach_date_to)
      7.  Optional text / status post-filter
      8.  $project
    """
    # ── Shift date range pre-filter ────────────────────────────────────
    base_match = {}
    date_range = {}
    if shift_date_from:
        try:
            date_range["$gte"] = datetime.strptime(shift_date_from, "%Y-%m-%d")
        except ValueError:
            pass
    if shift_date_to:
        try:
            dt_to = datetime.strptime(shift_date_to, "%Y-%m-%d")
            date_range["$lte"] = dt_to.replace(hour=23, minute=59, second=59)
        except ValueError:
            pass
    if date_range:
        base_match["date"] = date_range

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
                },
                # Alias last_outreach_date → outreach_date for uniform downstream handling
                "outreach_date": "$last_outreach_date",
            }
        },
        {"$unset": ["_avail_primary", "_avail_fallback"]},
    ]

    # ── Outreach date range filter (applied after outreach_date is derived) ──
    od_range = {}
    if outreach_date_from:
        try:
            od_range["$gte"] = datetime.strptime(outreach_date_from, "%Y-%m-%d")
        except ValueError:
            pass
    if outreach_date_to:
        try:
            dt_to = datetime.strptime(outreach_date_to, "%Y-%m-%d")
            od_range["$lte"] = dt_to.replace(hour=23, minute=59, second=59)
        except ValueError:
            pass
    if od_range:
        pipeline.append({"$match": {"outreach_date": od_range}})

    # ── Text search ────────────────────────────────────────────────────
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
            "client_name": 1,
            "assigned_staff": 1,
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
            try:
                dt = datetime.fromisoformat(raw_od.replace("Z", "+00:00"))
                s["outreach_date_formatted"] = dt.strftime("%d %b %Y %H:%M")
            except Exception:
                s["outreach_date_formatted"] = raw_od[:16]
        else:
            s["outreach_date_formatted"] = None

        # ── Location fallback → client_name ───────────────────────────
        if not s.get("location"):
            s["location"] = s.get("client_name") or None

        # ── Assigned staff normalisation ───────────────────────────────
        raw_staff = s.get("assigned_staff")
        if isinstance(raw_staff, list) and raw_staff:
            s["assigned_staff_display"] = ", ".join(
                str(x).strip() for x in raw_staff if x
            ) or None
        elif isinstance(raw_staff, str) and raw_staff.strip():
            s["assigned_staff_display"] = raw_staff.strip()
        else:
            s["assigned_staff_display"] = None

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

VALID_STATUSES = [
    "To Be Filled",
    "To be assigned",
    "Upcoming",
    "Un Filled",
    "Ongoing",
    "Under Review",
    "Cancelled By Client",
    "Failed",
    "Duplicated",
    "Rejected",
    "Send Back",
    "Cancelled By Staff",
    "Completed",
    "Cancelled",
]
PER_PAGE = 10

# Today's date string used as the default outreach_date_from
_TODAY = datetime.utcnow().strftime("%Y-%m-%d")


@bp.route("/live-shifts")
def live_shifts():
    """Renders the live shift listing page."""
    page               = max(int(request.args.get("page", 1)), 1)
    search             = request.args.get("search", "").strip()
    shift_date_from    = request.args.get("shift_date_from", "").strip()
    shift_date_to      = request.args.get("shift_date_to", "").strip()
    # Default outreach_date_from to today when not supplied
    outreach_date_from = request.args.get("outreach_date_from",
                                          datetime.utcnow().strftime("%Y-%m-%d")).strip()
    outreach_date_to   = request.args.get("outreach_date_to", "").strip()
    # Default status to "To Be Filled" when not supplied
    status_filter      = request.args.get("status_filter", "To Be Filled").strip()

    if status_filter not in VALID_STATUSES:
        status_filter = "To Be Filled"

    pipeline = _build_pipeline(
        search, shift_date_from, shift_date_to,
        outreach_date_from, outreach_date_to, status_filter,
    )

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
        shifts             = shifts_list,
        page               = page,
        total              = total,
        per_page           = PER_PAGE,
        pages              = pages,
        search             = search,
        shift_date_from    = shift_date_from,
        shift_date_to      = shift_date_to,
        outreach_date_from = outreach_date_from,
        outreach_date_to   = outreach_date_to,
        status_filter      = status_filter,
        valid_statuses     = VALID_STATUSES,
    )


@bp.route("/live-shifts/data")
def live_shifts_data():
    """JSON endpoint for AJAX polling / auto-refresh."""
    page               = max(int(request.args.get("page", 1)), 1)
    search             = request.args.get("search", "").strip()
    shift_date_from    = request.args.get("shift_date_from", "").strip()
    shift_date_to      = request.args.get("shift_date_to", "").strip()
    outreach_date_from = request.args.get("outreach_date_from",
                                          datetime.utcnow().strftime("%Y-%m-%d")).strip()
    outreach_date_to   = request.args.get("outreach_date_to", "").strip()
    status_filter      = request.args.get("status_filter", "To Be Filled").strip()

    if status_filter not in VALID_STATUSES:
        status_filter = "To Be Filled"

    pipeline = _build_pipeline(
        search, shift_date_from, shift_date_to,
        outreach_date_from, outreach_date_to, status_filter,
    )
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


@bp.route("/live-shifts/export-csv")
def live_shifts_export_csv():
    """
    Stream all matching shifts (no pagination) as a CSV download.
    Accepts the same filter params as /live-shifts.
    Columns: Shift ID, Shift Date, Start, End, Type, Location, Status,
             Premium, Availability, Outreach Date
    """
    import csv
    import io
    from flask import Response

    search             = request.args.get("search", "").strip()
    shift_date_from    = request.args.get("shift_date_from", "").strip()
    shift_date_to      = request.args.get("shift_date_to", "").strip()
    outreach_date_from = request.args.get("outreach_date_from",
                                          datetime.utcnow().strftime("%Y-%m-%d")).strip()
    outreach_date_to   = request.args.get("outreach_date_to", "").strip()
    status_filter      = request.args.get("status_filter", "To Be Filled").strip()

    if status_filter not in VALID_STATUSES:
        status_filter = "To Be Filled"

    pipeline = _build_pipeline(
        search, shift_date_from, shift_date_to,
        outreach_date_from, outreach_date_to, status_filter,
    )

    # No pagination — fetch all matching records sorted newest first
    full_pipeline = pipeline + [{"$sort": {"date": -1, "created_at": -1}}]
    shifts_list   = _format_shifts(list(db.shifts.aggregate(full_pipeline)))

    # ── Build CSV in memory ────────────────────────────────────────────
    output = io.StringIO()
    writer = csv.writer(output)

    # Header row
    writer.writerow([
        "Shift ID",
        "Unit Name",
        "Shift Date",
        "Start Time",
        "End Time",
        "Shift Type",
        "Location",
        "Postal Code",
        "Status",
        "Premium",
        "Availability",
        "Outreach Date",
        "Staff Assigned",
    ])

    for s in shifts_list:
        slots = s.get("slots_normalised") or []

        if not slots:
            slots = [{
                "shift_xn_id":  s.get("shift_xn_id") or "",
                "date_display": s.get("date_formatted") or "",
                "start_time":   s.get("start_time_formatted") or "",
                "end_time":     s.get("end_time_formatted") or "",
                "shift_type":   "",
            }]

        for slot in slots:
            writer.writerow([
                slot.get("shift_xn_id")  or "",
                s.get("name")            or "",
                slot.get("date_display") or s.get("date_formatted") or "",
                slot.get("start_time")   or "",
                slot.get("end_time")     or "",
                slot.get("shift_type")   or "",
                s.get("location")        or "",
                s.get("postal_code")     or "",
                s.get("status")          or "",
                "Yes" if s.get("is_premium") else "No",
                "Yes" if s.get("has_availability") else "No",
                s.get("outreach_date_formatted") or "",
                s.get("assigned_staff_display") or "",
            ])

    # ── Build filename with filter context ────────────────────────────
    ts    = datetime.utcnow().strftime("%Y%m%d_%H%M")
    fname = f"live_shifts_{ts}.csv"

    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={fname}",
            "Content-Type": "text/csv; charset=utf-8",
        },
    )