"""
shifts_available.py
───────────────────
Read-only listing of shifts that have at least one available staff member.

Availability is determined using a two-source check (mirroring live_shifts.py):
  PRIMARY  : shifts_users collection — any row where
               shift_id == shift._id  AND  availability == 1
  FALLBACK : shifts_group_users collection — any element in
               availability_details[] where
               shift_id == str(shift._id)  AND  availability == 1

Endpoint
────────
  POST  /shifts-available/have-availability

  Body mirrors shifts_db.py's ShiftsDbListRequest:
    search, status, client_id, user_type, user_type_multiple,
    county_multiple, is_premium, start_date, end_date,
    sort_by, sort_order, page, per_page

  Extra filter:
    availability_filter  "yes" | "no" | null (default: "yes")
      "yes" → only shifts WITH available staff
      "no"  → only shifts WITHOUT available staff
      null  → all shifts (availability shown as metadata)

Response shape
──────────────
  {
    "success": true,
    "total": <int>,
    "page": <int>,
    "per_page": <int>,
    "data": [
      {
        "id":                    "<shift._id>",
        "shift_id":              "<xn shift id>",
        "shift_code":            "...",
        "name":                  "...",
        "date":                  "...",
        "start_time":            "...",
        "end_time":              "...",
        "shift_timing":          "...",
        "user_type":             "...",
        "client_id":             "...",
        "client_name":           "...",
        "client_county":         "...",
        "status":                "...",
        "upstream_status":       "...",
        "is_premium":            true | false,
        "has_availability":      1 | 0,
        "available_count":       <int>,
        "available_staff_names": ["Name 1", ...],
        "outreach_status":       <int>,
        "outreach_status_text":  "...",
        "staff_counts": { ... },
      },
      ...
    ]
  }
"""

import logging
import re
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/shifts-available", tags=["Shifts Available"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


# ── Helpers (shared with shifts_db.py pattern) ────────────────────────────────

def _serialize(doc: dict) -> dict:
    if doc is None:
        return {}
    result = {}
    for k, v in doc.items():
        key = "id" if k == "_id" else k
        if isinstance(v, ObjectId):
            result[key] = str(v)
        elif hasattr(v, "isoformat"):
            result[key] = v.isoformat()
        elif isinstance(v, dict):
            result[key] = _serialize(v)
        elif isinstance(v, list):
            result[key] = [
                _serialize(i) if isinstance(i, dict)
                else str(i) if isinstance(i, ObjectId)
                else i.isoformat() if hasattr(i, "isoformat")
                else i
                for i in v
            ]
        else:
            result[key] = v
    return result


def _normalize_shift_timing(s: dict) -> str:
    timing = s.get("shift_timing") or s.get("shift") or ""
    start  = s.get("start_time", "")
    end    = s.get("end_time", "")
    if (not start or not end) and s.get("slots"):
        first = (s["slots"][0] if isinstance(s["slots"], list) and s["slots"] else {})
        start = start or first.get("start_time", "")
        end   = end   or first.get("end_time", "")
        if not timing:
            timing = first.get("shift_type", "")
    if timing and start and end:
        if "(" in timing and ")" in timing:
            return timing
        return f"{timing} ({start} - {end})"
    return timing or ""


async def _resolve_user_type_names(db, ids: list) -> list:
    if not ids:
        return []
    oids = [ObjectId(i) for i in ids if ObjectId.is_valid(str(i))]
    names = []
    if oids:
        async for doc in db["user_types"].find({"_id": {"$in": oids}}, {"name": 1}):
            if doc.get("name"):
                names.append(doc["name"])
    for i in ids:
        if not ObjectId.is_valid(str(i)) and i not in names:
            names.append(i)
    return names


async def _resolve_county_names(db, ids: list) -> list:
    if not ids:
        return []
    oids = [ObjectId(i) for i in ids if ObjectId.is_valid(str(i))]
    names = []
    if oids:
        async for doc in db["county"].find({"_id": {"$in": oids}}, {"name": 1}):
            if doc.get("name"):
                names.append(doc["name"])
    for i in ids:
        if not ObjectId.is_valid(str(i)) and i not in names:
            names.append(i)
    return names


async def _build_client_map(db, client_ids: list) -> dict:
    client_map: dict = {}
    if not client_ids:
        return client_map
    projection = {"name": 1, "title": 1, "xn_client_id": 1}
    async for cl in db["clients"].find({"xn_client_id": {"$in": client_ids}}, projection):
        xn_id = cl.get("xn_client_id")
        if xn_id:
            client_map[str(xn_id)] = cl
    unresolved = [cid for cid in client_ids if cid not in client_map]
    if unresolved:
        valid_oids = [ObjectId(c) for c in unresolved if ObjectId.is_valid(c)]
        if valid_oids:
            async for cl in db["clients"].find({"_id": {"$in": valid_oids}}, projection):
                client_map[str(cl["_id"])] = cl
    return client_map


def _client_name(cl: dict) -> str:
    if not cl:
        return "—"
    return cl.get("name") or cl.get("title") or "—"


# ── Core availability aggregation (from live_shifts.py logic) ─────────────────

async def _get_available_shift_ids(db, candidate_oids: list) -> dict:
    """
    For each shift_oid in candidate_oids determine:
      - has_availability (bool)
      - available_count  (int)
      - available_user_ids (list of ObjectId)

    PRIMARY  : shifts_users where shift_id in candidate_oids AND availability == 1
    FALLBACK : shifts_group_users where availability_details[].shift_id (string)
               matches AND that same element has availability == 1

    Returns dict keyed by shift_id_str:
      {
        "has_availability": True/False,
        "available_count":  <int>,
        "user_ids":         [ObjectId, ...]
      }
    """
    result: dict = {str(oid): {"has_availability": False, "available_count": 0, "user_ids": []} for oid in candidate_oids}

    # ── PRIMARY: shifts_users ─────────────────────────────────────────────────
    async for su in db["shifts_users"].find(
        {"shift_id": {"$in": candidate_oids}, "availability": 1},
        {"shift_id": 1, "user_id": 1}
    ):
        sid = str(su["shift_id"])
        uid = su.get("user_id")
        if sid in result:
            result[sid]["has_availability"] = True
            result[sid]["available_count"]  += 1
            if uid:
                result[sid]["user_ids"].append(uid)

    # ── FALLBACK: shifts_group_users ──────────────────────────────────────────
    # Match on the string version of shift_id inside availability_details[].
    # Mirrors live_shifts.py's fallback $lookup logic.
    sid_strs = [str(oid) for oid in candidate_oids]

    async for gsu in db["shifts_group_users"].find(
        {"availability_details.shift_id": {"$in": sid_strs}},
        {"user_id": 1, "availability_details": 1}
    ):
        uid = gsu.get("user_id")
        for ad in (gsu.get("availability_details") or []):
            sid = str(ad.get("shift_id", ""))
            if sid in result and ad.get("availability") == 1:
                # Only count if not already counted via PRIMARY
                if uid and uid not in result[sid]["user_ids"]:
                    result[sid]["has_availability"] = True
                    result[sid]["available_count"]  += 1
                    result[sid]["user_ids"].append(uid)

    return result


async def _resolve_staff_names(db, user_ids: list) -> list:
    """Fetch full names for a list of user ObjectIds."""
    if not user_ids:
        return []
    valid_oids = [uid if isinstance(uid, ObjectId) else ObjectId(str(uid))
                  for uid in user_ids if ObjectId.is_valid(str(uid))]
    if not valid_oids:
        return []
    names = []
    async for u in db["users"].find(
        {"_id": {"$in": valid_oids}},
        {"first_name": 1, "last_name": 1}
    ):
        full = " ".join(filter(None, [u.get("first_name", ""), u.get("last_name", "")])).strip()
        if full:
            names.append(full)
    return names


# ── Staff counts (reuse pattern from shifts_db.py) ────────────────────────────

async def _get_staff_counts_bulk(db, shift_oids: list) -> dict:
    if not shift_oids:
        return {}
    pipeline = [
        {"$match": {"shift_id": {"$in": shift_oids}}},
        {"$group": {
            "_id":            "$shift_id",
            "available":      {"$sum": {"$cond": [{"$eq": ["$availability", 1]},  1, 0]}},
            "with_outreach":  {"$sum": {"$cond": [{"$and": [{"$ne": ["$outreach_id", None]}, {"$gt": ["$outreach_id", None]}]}, 1, 0]}},
            "no_reply":       {"$sum": {"$cond": [{"$eq": ["$availability", 6]},  1, 0]}},
            "phone_declined": {"$sum": {"$cond": [{"$and": [{"$in": ["$availability", [0, 3, 4]]}, {"$or": [{"$eq": ["$channel", "Phone"]}, {"$eq": ["$channel", None]}, {"$not": {"$ifNull": ["$channel", False]}}]}]}, 1, 0]}},
            "email_wa_declined": {"$sum": {"$cond": [{"$and": [{"$eq": ["$availability", 0]}, {"$in": ["$channel", ["Email", "WhatsApp", "SMS"]]}]}, 1, 0]}},
        }},
    ]
    counts: dict = {}
    async for row in db["shifts_users"].aggregate(pipeline):
        sid      = str(row["_id"])
        declined = row["phone_declined"] + row["email_wa_declined"]
        avail    = row["available"]
        no_reply = row["no_reply"]
        counts[sid] = {
            "available":     avail,
            "with_outreach": row["with_outreach"],
            "declined":      declined,
            "no_reply":      no_reply,
            "display":       f"{avail} Available · {declined} Declined · {no_reply} No reply",
            "has_available": 1 if avail > 0 else 0,
        }
    return counts


# ── Request schema ─────────────────────────────────────────────────────────────

class HaveAvailabilityRequest(BaseModel):
    # Filters (mirrors ShiftsDbListRequest)
    search:              str  = ""
    status:              Optional[str]  = None
    client_id:           Optional[str]  = None
    user_type:           Optional[str]  = None
    user_type_multiple:  Optional[list] = None
    county_multiple:     Optional[list] = None
    is_premium:          Optional[int]  = None
    start_date:          Optional[str]  = None
    end_date:            Optional[str]  = None
    # Availability filter: "yes" (default) | "no" | null (all)
    availability_filter: Optional[str]  = "yes"
    # Pagination / sorting
    sort_by:    str = "date"
    sort_order: str = "desc"
    page:       int = 1
    per_page:   int = 20


# ── Route ─────────────────────────────────────────────────────────────────────

@router.post(
    "/have-availability",
    summary="List shifts that have at least one available staff member",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def have_availability(request: Request, payload: HaveAvailabilityRequest):
    """
    Returns shifts where at least one staff member responded as available,
    checked across both shifts_users (primary) and shifts_group_users
    availability_details (fallback) — same logic as the booking live-shifts page.

    Set `availability_filter` to:
    - `"yes"` (default) — only shifts WITH available staff
    - `"no"`            — only shifts WITHOUT available staff
    - `null`            — all shifts (availability shown as metadata)
    """
    db   = _get_db()
    skip = (payload.page - 1) * payload.per_page

    avail_filter = (payload.availability_filter or "yes").strip().lower()
    if avail_filter not in ("yes", "no", "all"):
        avail_filter = "yes"

    # ── Build Mongo filter ────────────────────────────────────────────────────
    clauses: list = []

    search = (payload.search or "").strip()
    if search:
        rx = {"$regex": re.escape(search), "$options": "i"}
        clauses.append({"$or": [
            {"name":          rx},
            {"shift_xn_id":   rx},
            {"shift_code":    rx},
            {"location":      rx},
            {"client_name":   rx},
            {"client_county": rx},
            {"user_type":     rx},
        ]})

    if payload.status:
        clauses.append({"status": {"$regex": re.escape(payload.status.strip()), "$options": "i"}})

    if payload.client_id:
        clauses.append({"client_id": payload.client_id.strip()})

    if payload.user_type:
        clauses.append({"user_type": {"$regex": re.escape(payload.user_type.strip()), "$options": "i"}})

    if payload.user_type_multiple:
        type_names = await _resolve_user_type_names(db, payload.user_type_multiple)
        if type_names:
            clauses.append({"user_type": {"$in": type_names}})

    if payload.county_multiple:
        county_names = await _resolve_county_names(db, payload.county_multiple)
        if county_names:
            clauses.append({"client_county": {"$in": county_names}})

    if payload.is_premium is not None:
        clauses.append({"is_premium": payload.is_premium == 1})

    if payload.start_date or payload.end_date:
        date_cond: dict = {}
        if payload.start_date:
            try:
                dt = datetime.strptime(payload.start_date.strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
                date_cond["$gte"] = dt
            except ValueError:
                pass
        if payload.end_date:
            try:
                dt = datetime.strptime(payload.end_date.strip(), "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59, tzinfo=timezone.utc)
                date_cond["$lte"] = dt
            except ValueError:
                pass
        if date_cond:
            clauses.append({"date": date_cond})

    mongo_filter = {"$and": clauses} if clauses else {}

    sort_dir = -1 if (payload.sort_order or "desc").lower() == "desc" else 1
    sort_by  = payload.sort_by or "date"

    # ── PASS 1: collect candidate shift _ids matching base filters ────────────
    # We scan only _id so this is cheap even for large collections.
    all_oids = [
        doc["_id"]
        async for doc in db["shifts"].find(mongo_filter, {"_id": 1})
                                     .sort(sort_by, sort_dir)
                                     .limit(5000)   # safety cap
    ]

    if not all_oids:
        return {
            "success":           True,
            "total":             0,
            "page":              payload.page,
            "per_page":          payload.per_page,
            "availability_filter": avail_filter,
            "data":              [],
        }

    # ── PASS 2: batch-resolve availability for all candidates ─────────────────
    avail_map = await _get_available_shift_ids(db, all_oids)

    # ── Filter by availability_filter ─────────────────────────────────────────
    if avail_filter == "yes":
        filtered_oids = [oid for oid in all_oids if avail_map[str(oid)]["has_availability"]]
    elif avail_filter == "no":
        filtered_oids = [oid for oid in all_oids if not avail_map[str(oid)]["has_availability"]]
    else:  # "all"
        filtered_oids = all_oids

    total = len(filtered_oids)

    # ── Paginate ──────────────────────────────────────────────────────────────
    page_oids = filtered_oids[skip: skip + payload.per_page]

    if not page_oids:
        return {
            "success":             True,
            "total":               total,
            "page":                payload.page,
            "per_page":            payload.per_page,
            "availability_filter": avail_filter,
            "data":                [],
        }

    # ── PASS 3: fetch full shift docs for this page only ──────────────────────
    raw_docs = await db["shifts"].find({"_id": {"$in": page_oids}}).to_list(length=payload.per_page)
    # Restore sort order ($in doesn't preserve it)
    doc_index = {doc["_id"]: doc for doc in raw_docs}
    page_docs = [doc_index[oid] for oid in page_oids if oid in doc_index]

    # ── Enrich: client names ──────────────────────────────────────────────────
    client_ids = list({d.get("client_id") for d in page_docs if d.get("client_id")})
    client_map = await _build_client_map(db, client_ids)

    # ── Enrich: staff counts (reuses shifts_db.py pattern) ───────────────────
    staff_counts_map = await _get_staff_counts_bulk(db, page_oids)

    # ── Enrich: outreach status (latest per shift) ────────────────────────────
    STATUS_TEXT = {0: "Not Started", 1: "Live", 2: "Paused", 3: "Ended", 10: "Completed"}
    outreach_map: dict = {}
    async for o in db["outreach"].find(
        {"shift_id": {"$in": page_oids}},
        {"shift_id": 1, "outreach_status": 1, "created_at": 1}
    ):
        sid = str(o["shift_id"])
        existing = outreach_map.get(sid)
        if not existing or (o.get("created_at") or 0) > (existing.get("created_at") or 0):
            outreach_map[sid] = o

    # ── Enrich: available staff names ─────────────────────────────────────────
    # Batch all user_ids across the page into one users query.
    all_uid_map: dict = {}   # user_id_str → ObjectId
    for oid in page_oids:
        for uid in avail_map[str(oid)]["user_ids"]:
            uid_str = str(uid)
            if uid_str not in all_uid_map:
                all_uid_map[uid_str] = uid if isinstance(uid, ObjectId) else ObjectId(str(uid))

    name_by_uid: dict = {}
    if all_uid_map:
        async for u in db["users"].find(
            {"_id": {"$in": list(all_uid_map.values())}},
            {"first_name": 1, "last_name": 1}
        ):
            full = " ".join(filter(None, [u.get("first_name", ""), u.get("last_name", "")])).strip()
            if full:
                name_by_uid[str(u["_id"])] = full

    # ── Build response ────────────────────────────────────────────────────────
    results = []
    for doc in page_docs:
        sid_str  = str(doc["_id"])
        s        = _serialize(doc)
        cid      = s.get("client_id", "")
        cl       = client_map.get(cid)

        av_entry = avail_map.get(sid_str, {"has_availability": False, "available_count": 0, "user_ids": []})
        avail_names = [
            name_by_uid[str(uid)]
            for uid in av_entry["user_ids"]
            if str(uid) in name_by_uid
        ]

        o_doc    = outreach_map.get(sid_str, {})
        o_status = o_doc.get("outreach_status", 0)

        sc = staff_counts_map.get(sid_str, {
            "available": 0, "with_outreach": 0, "declined": 0,
            "no_reply": 0, "display": "0 Available · 0 Declined · 0 No reply",
            "has_available": 0,
        })

        results.append({
            "id":                    sid_str,
            "shift_id":              s.get("shift_id") or s.get("shift_xn_id") or "",
            "shift_code":            s.get("shift_code") or s.get("shift_xn_id") or "",
            "name":                  s.get("name") or "",
            "date":                  s.get("date"),
            "start_time":            s.get("start_time"),
            "end_time":              s.get("end_time"),
            "shift_timing":          _normalize_shift_timing(doc),
            "user_type":             s.get("user_type") or "",
            "client_id":             cid,
            "client_name":           _client_name(cl),
            "client_county":         s.get("client_county"),
            "status":                s.get("status"),
            "upstream_status":       s.get("upstream_status"),
            "is_premium":            s.get("is_premium"),
            "has_availability":      1 if av_entry["has_availability"] else 0,
            "available_count":       av_entry["available_count"],
            "available_staff_names": avail_names,
            "outreach_status":       o_status,
            "outreach_status_text":  STATUS_TEXT.get(o_status, "Not Started"),
            "staff_counts":          sc,
        })

    logger.info(
        f"[shifts-available/have-availability] filter={avail_filter} "
        f"candidates={len(all_oids)} matched={total} page={len(results)}"
    )

    return {
        "success":             True,
        "total":               total,
        "page":                payload.page,
        "per_page":            payload.per_page,
        "availability_filter": avail_filter,
        "data":                results,
    }