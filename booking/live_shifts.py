"""
booking/live_shifts.py
──────────────────────
Read-only live listing of shifts from the shifts collection.
Supports:
  • Text search  (name, location, shift ID, description)
  • Shift-date / outreach-date / status / availability filters
  • Pagination (10 per page)
  • JSON endpoint for AJAX loading (/booking/live-shifts/data)
  • CSV export of all filtered results (/booking/live-shifts/export-csv)
  • Availability column — green tick if:
      PRIMARY  : any shifts_users row for the shift has availability == 1, OR
      FALLBACK : any shifts_group_users.availability_details[] element has
                 shift_id == str(shift._id) AND availability == 1
  • O.Date column — taken directly from shifts.last_outreach_date

How a page load works (fast path)
─────────────────────────────────
  1. Every filter that lives on the shifts document itself (shift date,
     status, outreach date, text search) goes into ONE leading $match.
  2. PASS 1 — find the 10 _ids for the page + the total count:
       • No availability filter  → count_documents() + find().sort().skip()
         .limit(). Pure index work, zero $lookup.
       • Availability Yes / No   → aggregation with existence-only lookups
         ($limit: 1 inside each sub-pipeline) using indexed
         localField/foreignField joins, then $facet for count + ids.
  3. PASS 2 — only for those ≤10 _ids: full availability lookups
     (user ids) + staff-name lookup + projection.

  All $lookups use localField/foreignField (indexed) instead of
  $expr-only matches, which could not use indexes and scanned the whole
  foreign collection once per shift.

  Requires MongoDB 5.0+ (localField/foreignField combined with pipeline).

Required MongoDB indexes (run once):
  db.shifts.createIndex({ date: -1, created_at: -1 })
  db.shifts.createIndex({ status: 1, date: -1, created_at: -1 })
  db.shifts.createIndex({ last_outreach_date: -1 })
  db.shifts_users.createIndex({ shift_id: 1, availability: 1 })
  db.shifts_group_users.createIndex({ "availability_details.shift_id": 1 })
  (users._id is indexed by default)
"""

import csv
import io
from datetime import datetime

from bson import ObjectId
from flask import Response, render_template, request, jsonify

from database import db
from . import bp


# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

PER_PAGE     = 10
CSV_MAX_ROWS = 5_000

SORT_SPEC  = [("date", -1), ("created_at", -1)]
SORT_STAGE = {"$sort": {"date": -1, "created_at": -1}}

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

_VALID_STATUSES_SET = set(VALID_STATUSES)

FINAL_PROJECTION = {
    "_id":                   1,
    "name":                  1,
    "date":                  1,
    "start_time":            1,
    "end_time":              1,
    "shift_xn_id":           1,
    "description":           1,
    "location":              1,
    "postal_code":           1,
    "is_premium":            1,
    "status":                1,
    "rate":                  1,
    "slots":                 1,
    "created_at":            1,
    "updated_at":            1,
    "has_availability":      1,
    "available_staff_names": 1,
    "outreach_date":         1,
    "client_name":           1,
    "assigned_staff":        1,
}


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


def _parse_day(value: str):
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ──────────────────────────────────────────────────────────────────────────────
# Query building
# ──────────────────────────────────────────────────────────────────────────────

def _build_leading_match(
    search: str,
    shift_date_from: str,
    shift_date_to: str,
    outreach_date_from: str,
    outreach_date_to: str,
    status_filter: str,
) -> dict:
    """
    Every filter that only needs fields on the shifts document, combined
    into one query so MongoDB can use indexes before any join happens.
    """
    conditions: list = []

    # ── Shift date (defaults to today onwards) ────────────────────────
    date_range: dict = {}
    d_from = _parse_day(shift_date_from)
    d_to   = _parse_day(shift_date_to)
    if d_from:
        date_range["$gte"] = d_from
    if d_to:
        date_range["$lte"] = d_to.replace(hour=23, minute=59, second=59)
    if not date_range:
        date_range["$gte"] = datetime.utcnow().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    conditions.append({"date": date_range})

    # ── Status ────────────────────────────────────────────────────────
    if status_filter:
        conditions.append({"status": status_filter})

    # ── Outreach date (only when the user sets it) ────────────────────
    # last_outreach_date may be a datetime or an ISO string — match both.
    od_from = _parse_day(outreach_date_from)
    od_to   = _parse_day(outreach_date_to)
    if od_from or od_to:
        dt_range:  dict = {}
        str_range: dict = {"$type": "string"}
        if od_from:
            dt_range["$gte"]  = od_from
            str_range["$gte"] = outreach_date_from
        if od_to:
            dt_range["$lte"]  = od_to.replace(hour=23, minute=59, second=59)
            str_range["$lte"] = outreach_date_to + "T23:59:59~"
        conditions.append({"$or": [
            {"last_outreach_date": dt_range},
            {"last_outreach_date": str_range},
        ]})

    # ── Text search (native shift fields) ─────────────────────────────
    if search:
        rx = {"$regex": search, "$options": "i"}
        conditions.append({"$or": [
            {"name":        rx},
            {"location":    rx},
            {"shift_xn_id": rx},
            {"description": rx},
        ]})

    return conditions[0] if len(conditions) == 1 else {"$and": conditions}


def _availability_stages(existence_only: bool) -> list:
    """
    Availability lookups using indexed localField/foreignField joins.

    existence_only=True  → stop at the first matching row per source
                           ($limit: 1). Enough to compute has_availability
                           for filtering/counting in pass 1.
    existence_only=False → collect all user_ids (needed for staff names).

    Output fields: has_availability, _avail_user_ids, outreach_date.
    """
    stop = [{"$limit": 1}] if existence_only else []

    return [
        {"$addFields": {"_sid_str": {"$toString": "$_id"}}},

        # PRIMARY — shifts_users.shift_id (ObjectId) == shifts._id
        {
            "$lookup": {
                "from":         "shifts_users",
                "localField":   "_id",
                "foreignField": "shift_id",
                "pipeline": [
                    {"$match": {"availability": 1}},
                    *stop,
                    {"$project": {"_id": 0, "user_id": 1}},
                ],
                "as": "_avail_primary",
            }
        },

        # FALLBACK — shifts_group_users.availability_details[].shift_id
        # (string) == str(shifts._id). The index on
        # availability_details.shift_id narrows candidates; the $expr only
        # runs on those few docs to confirm the SAME element has
        # availability == 1.
        {
            "$lookup": {
                "from":         "shifts_group_users",
                "localField":   "_sid_str",
                "foreignField": "availability_details.shift_id",
                "let":          {"sid_str": "$_sid_str"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$anyElementTrue": [{
                                    "$map": {
                                        "input": {"$ifNull": ["$availability_details", []]},
                                        "as":    "d",
                                        "in": {
                                            "$and": [
                                                {"$eq": ["$$d.shift_id",     "$$sid_str"]},
                                                {"$eq": ["$$d.availability", 1]},
                                            ]
                                        },
                                    }
                                }]
                            }
                        }
                    },
                    *stop,
                    {"$project": {"_id": 0, "user_id": 1}},
                ],
                "as": "_avail_fallback",
            }
        },

        {
            "$addFields": {
                "_avail_user_ids": {
                    "$setUnion": [
                        "$_avail_primary.user_id",
                        "$_avail_fallback.user_id",
                    ]
                },
                "has_availability": {
                    "$gt": [
                        {"$add": [
                            {"$size": "$_avail_primary"},
                            {"$size": "$_avail_fallback"},
                        ]},
                        0,
                    ]
                },
                "outreach_date": "$last_outreach_date",
            }
        },
        {"$unset": ["_sid_str", "_avail_primary", "_avail_fallback"]},
    ]


def _availability_match(availability_filter: str) -> list:
    if availability_filter == "yes":
        return [{"$match": {"has_availability": True}}]
    if availability_filter == "no":
        return [{"$match": {"has_availability": False}}]
    return []


def _enrich_stages() -> list:
    """Staff names via indexed users._id join, then final projection."""
    return [
        {
            "$lookup": {
                "from":         "users",
                "localField":   "_avail_user_ids",
                "foreignField": "_id",
                "pipeline": [
                    {
                        "$project": {
                            "_id": 0,
                            "full_name": {
                                "$trim": {
                                    "input": {
                                        "$concat": [
                                            {"$ifNull": ["$first_name", ""]},
                                            " ",
                                            {"$ifNull": ["$last_name",  ""]},
                                        ]
                                    }
                                }
                            },
                        }
                    },
                ],
                "as": "_avail_users",
            }
        },
        {"$addFields": {"available_staff_names": "$_avail_users.full_name"}},
        {"$unset": ["_avail_user_ids", "_avail_users"]},
        {"$project": FINAL_PROJECTION},
    ]


def _fetch_full_docs(ids: list) -> list:
    """
    PASS 2 — full availability + staff names for a known list of _ids,
    returned in the same order as `ids`.
    """
    if not ids:
        return []

    pipeline = (
        [{"$match": {"_id": {"$in": ids}}}]
        + _availability_stages(existence_only=False)
        + _enrich_stages()
    )
    docs  = list(db.shifts.aggregate(pipeline, allowDiskUse=True))
    order = {_id: i for i, _id in enumerate(ids)}
    docs.sort(key=lambda d: order.get(d["_id"], 0))
    return docs


def _run_paginated(leading_match: dict, availability_filter: str, page: int):
    """
    PASS 1 — total + page _ids, as cheaply as possible.
    PASS 2 — enrich only those ≤ PER_PAGE docs.
    """
    skip = (page - 1) * PER_PAGE

    if not availability_filter:
        # Pure index path — no joins needed to count or page.
        total = db.shifts.count_documents(leading_match)
        ids = [
            d["_id"]
            for d in db.shifts.find(leading_match, {"_id": 1})
                              .sort(SORT_SPEC)
                              .skip(skip)
                              .limit(PER_PAGE)
        ]
    else:
        # Availability filter needs has_availability for every candidate,
        # but existence-only lookups keep each join to one indexed probe.
        pipeline = (
            [{"$match": leading_match}]
            + _availability_stages(existence_only=True)
            + _availability_match(availability_filter)
            + [{
                "$facet": {
                    "data": [
                        SORT_STAGE,
                        {"$skip":  skip},
                        {"$limit": PER_PAGE},
                        {"$project": {"_id": 1}},
                    ],
                    "count": [{"$count": "total"}],
                }
            }]
        )
        result = list(db.shifts.aggregate(pipeline, allowDiskUse=True))
        facet  = result[0] if result else {}
        count_arr = facet.get("count") or [{}]
        total  = count_arr[0].get("total", 0)
        ids    = [d["_id"] for d in facet.get("data", [])]

    return _format_shifts(_fetch_full_docs(ids)), total


def _build_export_pipeline(leading_match: dict, availability_filter: str) -> list:
    """
    CSV export. When there's no availability filter, sort + limit happen
    BEFORE the joins so at most CSV_MAX_ROWS shifts are joined.
    """
    if not availability_filter:
        return (
            [{"$match": leading_match}, SORT_STAGE, {"$limit": CSV_MAX_ROWS}]
            + _availability_stages(existence_only=False)
            + _enrich_stages()
            + [SORT_STAGE]
        )
    return (
        [{"$match": leading_match}]
        + _availability_stages(existence_only=False)
        + _availability_match(availability_filter)
        + [SORT_STAGE, {"$limit": CSV_MAX_ROWS}]
        + _enrich_stages()
        + [SORT_STAGE]
    )


def _format_shifts(shifts_list: list) -> list:
    for s in shifts_list:
        st = s.get("start_time")
        s["start_time_formatted"] = st.strftime("%H:%M") if isinstance(st, datetime) else (st or "—")

        et = s.get("end_time")
        s["end_time_formatted"] = et.strftime("%H:%M") if isinstance(et, datetime) else (et or "—")

        raw_date = s.get("date")
        if isinstance(raw_date, datetime):
            s["date_formatted"] = raw_date.strftime("%d %b %Y")
        elif isinstance(raw_date, str) and raw_date:
            s["date_formatted"] = raw_date.split("T")[0]
        else:
            s["date_formatted"] = "—"

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

        raw_names = s.get("available_staff_names") or []
        s["available_staff_list"] = [n for n in raw_names if n and n.strip()]

        if not s.get("location"):
            s["location"] = s.get("client_name") or None

        raw_staff = s.get("assigned_staff")
        if isinstance(raw_staff, list) and raw_staff:
            s["assigned_staff_display"] = (
                ", ".join(str(x).strip() for x in raw_staff if x) or None
            )
        elif isinstance(raw_staff, str) and raw_staff.strip():
            s["assigned_staff_display"] = raw_staff.strip()
        else:
            s["assigned_staff_display"] = None

        slots_out = []
        for slot in (s.get("slots") or []):
            slot_date = slot.get("date")
            if isinstance(slot_date, datetime):
                date_str     = slot_date.strftime("%Y-%m-%d")
                date_display = slot_date.strftime("%d %b %Y")
            elif isinstance(slot_date, dict) and "$date" in slot_date:
                try:
                    dt = datetime.fromisoformat(slot_date["$date"].replace("Z", "+00:00"))
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


def _parse_filters():
    """Parse and validate common query-string filter parameters."""
    page               = max(int(request.args.get("page", 1)), 1)
    search             = request.args.get("search", "").strip()
    shift_date_from    = request.args.get("shift_date_from", "").strip()
    shift_date_to      = request.args.get("shift_date_to", "").strip()
    outreach_date_from = request.args.get("outreach_date_from", "").strip()
    outreach_date_to    = request.args.get("outreach_date_to", "").strip()
    status_filter       = request.args.get("status_filter", "").strip()
    availability_filter = request.args.get("availability_filter", "").strip().lower()

    if status_filter not in _VALID_STATUSES_SET:
        status_filter = ""
    if availability_filter not in ("yes", "no", ""):
        availability_filter = ""

    return (
        page, search,
        shift_date_from, shift_date_to,
        outreach_date_from, outreach_date_to,
        status_filter, availability_filter,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────────────

@bp.route("/live-shifts")
def live_shifts():
    """Skeleton screen: render the page shell with zero DB work."""
    (
        page, search,
        shift_date_from, shift_date_to,
        outreach_date_from, outreach_date_to,
        status_filter, availability_filter,
    ) = _parse_filters()

    return render_template(
        "booking/live_shifts.html",
        page                 = page,
        search               = search,
        shift_date_from      = shift_date_from,
        shift_date_to        = shift_date_to,
        outreach_date_from   = outreach_date_from,
        outreach_date_to     = outreach_date_to,
        status_filter        = status_filter,
        availability_filter  = availability_filter,
        valid_statuses       = VALID_STATUSES,
    )


@bp.route("/live-shifts/data")
def live_shifts_data():
    """JSON endpoint used by the page JS for load, filter and pagination."""
    (
        page, search,
        shift_date_from, shift_date_to,
        outreach_date_from, outreach_date_to,
        status_filter, availability_filter,
    ) = _parse_filters()

    leading_match = _build_leading_match(
        search, shift_date_from, shift_date_to,
        outreach_date_from, outreach_date_to,
        status_filter,
    )

    shifts_list, total = _run_paginated(leading_match, availability_filter, page)

    resp = jsonify({
        "success":              True,
        "shifts":               _serialize(shifts_list),
        "total":                total,
        "page":                 page,
        "pages":                max((total + PER_PAGE - 1) // PER_PAGE, 1),
        "per_page":             PER_PAGE,
        "search":               search,
        "shift_date_from":      shift_date_from,
        "shift_date_to":        shift_date_to,
        "outreach_date_from":   outreach_date_from,
        "outreach_date_to":     outreach_date_to,
        "status_filter":        status_filter,
        "availability_filter":  availability_filter,
    })
    resp.headers["Content-Type"] = "application/json; charset=utf-8"
    resp.headers["Vary"] = "Accept-Encoding"
    return resp


@bp.route("/live-shifts/export-csv")
def live_shifts_export_csv():
    """All matching shifts (capped at CSV_MAX_ROWS) as a CSV download."""
    (
        _, search,
        shift_date_from, shift_date_to,
        outreach_date_from, outreach_date_to,
        status_filter, availability_filter,
    ) = _parse_filters()

    leading_match = _build_leading_match(
        search, shift_date_from, shift_date_to,
        outreach_date_from, outreach_date_to,
        status_filter,
    )

    shifts_list = _format_shifts(list(db.shifts.aggregate(
        _build_export_pipeline(leading_match, availability_filter),
        allowDiskUse=True,
    )))

    output = io.StringIO()
    writer = csv.writer(output)

    writer.writerow([
        "Shift ID", "Unit Name", "Shift Date", "Start Time", "End Time",
        "Shift Type", "Location", "Postal Code", "Status", "Premium",
        "Availability", "Available Staff", "Outreach Date", "Staff Assigned",
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
                ", ".join(s.get("available_staff_list") or []),
                s.get("outreach_date_formatted") or "",
                s.get("assigned_staff_display") or "",
            ])

    ts    = datetime.utcnow().strftime("%Y%m%d_%H%M")
    fname = f"live_shifts_{ts}.csv"

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={fname}",
            "Content-Type":        "text/csv; charset=utf-8",
        },
    )