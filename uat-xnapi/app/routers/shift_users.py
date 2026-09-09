import asyncio
import logging
import math
import re
from datetime import datetime, timezone
from typing import List, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from pymongo import UpdateOne
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/shift-users", tags=["Shift Users"])


# ── Shared listing constants ──────────────────────────────────────────────────

# Fields fetched for every candidate user in /list and /list-multi
_LIST_PROJECTION = {
    "first_name": 1, "last_name": 1, "email": 1, "phone": 1,
    "xn_user_id": 1, "designation": 1, "rating": 1,
    "location": 1, "latitude": 1, "longitude": 1, "status": 1,
    "tags": 1, "county_id": 1, "user_type_id": 1, "country_id": 1,
    "visa_hours_used": 1, "visa_hours_total": 1, "banned_clients": 1,
    "gender_id": 1, "work_permit_exemption": 1, "consumed_hours": 1,
    "qqi_status_number": 1, "user_sub_type_oids": 1, "user_sub_type_ids": 1,
    "visa_type_id": 1, "exclusion_cache_by_shift": 1,
}

# Exclusion cache lifetime — a user's schedule can change without us knowing
_EXCLUSION_CACHE_TTL_SECONDS = 15 * 60

# How many exclusion computations run concurrently within a chunk
_EXCLUSION_CONCURRENCY = 10

# Hard ceiling on how many candidate users one request may walk.
# Keeps the CSV exports (per_page=5000) from becoming a full collection scan.
_MAX_SCAN = 5000


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize(doc: dict) -> dict:
    result = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            result[k] = str(v)
        elif hasattr(v, "isoformat"):
            result[k] = v.isoformat()
        else:
            result[k] = v
    return result


def _resolve_oid(val: str, field: str) -> ObjectId:
    if not ObjectId.is_valid(val):
        raise HTTPException(status_code=422, detail=f"Invalid ObjectId for '{field}': {val}")
    return ObjectId(val)


# ── Request schemas ───────────────────────────────────────────────────────────

class AddUserToShiftRequest(BaseModel):
    user_id:  str
    shift_id: str
    channel:  Optional[str] = "Phone"


class RemoveUserFromShiftRequest(BaseModel):
    id:       str   # user_id (users._id)
    shift_id: str   # shifts._id


class AddUsersToShiftRequest(BaseModel):
    shift_id:  str
    user_ids:  List[str]
    channel:   Optional[str] = "Phone"


# ── ADD single user to shift ──────────────────────────────────────────────────

@router.post(
    "/",
    summary="Add a user to the shift pool (shifts_pool collection)",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def add_user_to_shift(request: Request, payload: AddUserToShiftRequest):
    """
    Adds user to shifts_pool (not shifts_users).
    shifts_users is populated when outreach/create is called.
    Returns 409 if user is already in the pool for this shift.
    """
    db = _get_db()
    now = datetime.now(timezone.utc)

    user_oid  = _resolve_oid(payload.user_id,  "user_id")
    shift_oid = _resolve_oid(payload.shift_id, "shift_id")

    shift = await db["shifts"].find_one({"_id": shift_oid}, {"_id": 1, "shift_code": 1, "name": 1})
    if not shift:
        raise HTTPException(status_code=404, detail=f"Shift {payload.shift_id} not found")

    user = await db["users"].find_one({"_id": user_oid}, {"_id": 1, "first_name": 1, "last_name": 1, "email": 1})
    if not user:
        raise HTTPException(status_code=404, detail=f"User {payload.user_id} not found")

    existing = await db["shifts_pool"].find_one({"shift_id": shift_oid, "user_id": user_oid})
    if existing:
        full_name = " ".join(filter(None, [
            user.get("first_name", ""), user.get("last_name", "")
        ])).strip() or payload.user_id
        shift_code = shift.get("shift_code") or shift.get("name") or payload.shift_id
        raise HTTPException(status_code=409,
            detail=f"{full_name} is already in the pool for shift {shift_code}")

    channel = (payload.channel or "Phone").strip()
    if channel not in ("Phone", "WhatsApp", "Email", "SMS"):
         raise HTTPException(status_code=422, detail="channel must be Phone, WhatsApp, Email or SMS")

    doc = {
        "user_id":  user_oid,
        "shift_id": shift_oid,
        "channel":  channel,
        "added_at": now,
        "added_by": "manual",
        "updated_at": now,
    }
    result = await db["shifts_pool"].insert_one(doc)
    doc["_id"] = result.inserted_id

    logger.info(f"shifts_pool: added user={payload.user_id} shift={payload.shift_id}")
    return {"success": True, "message": "User added to shift pool", "data": _serialize(doc)}


# ── ADD multiple users to shift ───────────────────────────────────────────────

@router.post(
    "/bulk",
    summary="Add multiple users to a shift",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def add_users_to_shift_bulk(request: Request, payload: AddUsersToShiftRequest):
    """
    Adds multiple users to a shift in one call.
    Skips duplicates silently. Returns counts of inserted/skipped.
    """
    db = _get_db()
    now = datetime.now(timezone.utc)

    shift_oid = _resolve_oid(payload.shift_id, "shift_id")

    shift = await db["shifts"].find_one({"_id": shift_oid}, {"_id": 1, "shift_code": 1, "name": 1})
    if not shift:
        raise HTTPException(status_code=404, detail=f"Shift {payload.shift_id} not found")

    # Validate all user_ids
    user_oids = []
    invalid = []
    for uid in payload.user_ids:
        if ObjectId.is_valid(uid):
            user_oids.append(ObjectId(uid))
        else:
            invalid.append(uid)

    if invalid:
        raise HTTPException(status_code=422, detail=f"Invalid user_ids: {invalid}")

    # Check which users exist
    existing_users = {
        str(u["_id"])
        async for u in db["users"].find({"_id": {"$in": user_oids}}, {"_id": 1})
    }

    # Check which are already in shifts_pool
    already_added = {
        str(su["user_id"])
        async for su in db["shifts_pool"].find(
            {"shift_id": shift_oid, "user_id": {"$in": user_oids}},
            {"user_id": 1}
        )
    }

    channel = (payload.channel or "Phone").strip()
    if channel not in ("Phone", "WhatsApp", "Email", "SMS"):
         raise HTTPException(status_code=422, detail="channel must be Phone, WhatsApp, Email or SMS")

    # Mark all existing pool users as unselected first
    await db["shifts_pool"].update_many(
        {"shift_id": shift_oid},
        {"$set": {"selected": 0}}
    )

    # Remove users from pool NOT in payload — but keep users who have shifts_users record
    all_pool_users = {
        str(su["user_id"])
        async for su in db["shifts_pool"].find({"shift_id": shift_oid}, {"user_id": 1})
    }
    to_remove = all_pool_users - {str(oid) for oid in user_oids}
    removed = 0
    if to_remove:
        remove_oids = [ObjectId(uid) for uid in to_remove if ObjectId.is_valid(uid)]
        if remove_oids:
            # Protect users who have any shifts_users record for this shift
            protected = {
                str(su["user_id"])
                async for su in db["shifts_users"].find(
                    {"shift_id": shift_oid, "user_id": {"$in": remove_oids}},
                    {"user_id": 1}
                )
            }
            final_remove = [oid for oid in remove_oids if str(oid) not in protected]
            if final_remove:
                res = await db["shifts_pool"].delete_many({"shift_id": shift_oid, "user_id": {"$in": final_remove}})
                removed = res.deleted_count
                logger.info(f"shifts_pool bulk: removed {removed} (protected {len(protected)} with shifts_users)")

    inserted = skipped_dup = skipped_missing = 0
    inserted_ids = []

    for user_oid in user_oids:
        uid_str = str(user_oid)
        if uid_str not in existing_users:
            skipped_missing += 1
            continue
        if uid_str in already_added:
            # Update channel even for existing pool users
            await db["shifts_pool"].update_one(
                {"shift_id": shift_oid, "user_id": user_oid},
                {"$set": {"channel": channel, "updated_at": now}}
            )
            skipped_dup += 1
            continue

        doc = {
            "user_id":    user_oid,
            "shift_id":   shift_oid,
            "channel":    channel,
            "added_at":   now,
            "added_by":   "bulk",
            "updated_at": now,
        }
        result = await db["shifts_pool"].insert_one(doc)
        inserted_ids.append(str(result.inserted_id))
        inserted += 1

    logger.info(f"shifts_pool bulk: shift={payload.shift_id} inserted={inserted} dup={skipped_dup} missing={skipped_missing}")

    # Always update channel and mark as selected for ALL users in payload
    await db["shifts_pool"].update_many(
        {"shift_id": shift_oid, "user_id": {"$in": user_oids}},
        {"$set": {"channel": channel, "selected": 1, "updated_at": now}}
    )

    # Collect all pool _ids for users in payload
    pool_ids = []
    async for pd in db["shifts_pool"].find(
        {"shift_id": shift_oid, "user_id": {"$in": user_oids}},
        {"_id": 1}
    ):
        pool_ids.append(str(pd["_id"]))

    return {
        "success": True,
        "message": f"{inserted + skipped_dup} user(s) added to shift pool",
        "data": {
            "shift_id":             payload.shift_id,
            "inserted":             inserted,
            "removed":              removed,
            "skipped_duplicate":    skipped_dup,
            "skipped_missing_user": skipped_missing,
            "inserted_ids":         inserted_ids,
            "pool_ids":             pool_ids,
        },
    }


# ── LIST users for a shift ────────────────────────────────────────────────────

@router.get(
    "/by-shift/{shift_id}",
    summary="List all users assigned to a shift",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("120/minute")
async def list_shift_users(request: Request, shift_id: str):
    """Returns all shift_users records for a shift, enriched with user details."""
    db = _get_db()
    shift_oid = _resolve_oid(shift_id, "shift_id")

    su_docs = await db["shifts_users"].find({"shift_id": shift_oid}).to_list(500)
    if not su_docs:
        return {"success": True, "total": 0, "shift_id": shift_id, "data": []}

    user_oids = [su["user_id"] for su in su_docs if ObjectId.is_valid(str(su.get("user_id", "")))]
    user_map: dict = {}
    async for u in db["users"].find(
        {"_id": {"$in": user_oids}},
        {"first_name": 1, "last_name": 1, "email": 1, "phone": 1, "xn_user_id": 1, "designation": 1, "rating": 1}
    ):
        user_map[str(u["_id"])] = u

    results = []
    for su in su_docs:
        s = _serialize(su)
        uid_str = str(su.get("user_id", ""))
        u = user_map.get(uid_str, {})
        s["user"] = {
            "user_id":     uid_str,
            "xn_user_id":  u.get("xn_user_id"),
            "name":        " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() or "—",
            "email":       u.get("email"),
            "phone":       u.get("phone"),
            "designation": u.get("designation"),
            "rating":      u.get("rating"),
        }
        results.append(s)

    return {"success": True, "total": len(results), "shift_id": shift_id, "data": results}


# ── REMOVE user from shift ────────────────────────────────────────────────────

@router.post(
    "/remove",
    summary="Remove a user from a shift using shift_users._id",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def remove_user_from_shift(request: Request, payload: RemoveUserFromShiftRequest):
    """
    Body: { "id": "<user._id>", "shift_id": "<shift._id>" }
    Removes the user from shifts_pool, shifts_users, and any shifts_group_pool
    where the shift belongs to a group.
    """
    db        = _get_db()
    user_oid  = _resolve_oid(payload.id,       "id")
    shift_oid = _resolve_oid(payload.shift_id, "shift_id")

    # Remove from shifts_pool
    pool_result = await db["shifts_pool"].delete_one({
        "user_id":  user_oid,
        "shift_id": shift_oid,
    })

    # Remove from shifts_users
    su_result = await db["shifts_users"].delete_many({
        "user_id":  user_oid,
        "shift_id": shift_oid,
    })

    # Find any shift groups that contain this shift
    group_oids = []
    async for g in db["shifts_group"].find(
        {"shift_ids": shift_oid},
        {"_id": 1}
    ):
        group_oids.append(g["_id"])

    # Remove from shifts_group_pool and shifts_group_users for each group
    group_pool_removed       = 0
    group_users_removed      = 0

    if group_oids:
        gp_result = await db["shifts_group_pool"].delete_many({
            "group_id": {"$in": group_oids},
            "user_id":  user_oid,
        })
        group_pool_removed = gp_result.deleted_count

        gu_result = await db["shifts_group_users"].delete_many({
            "group_id": {"$in": group_oids},
            "user_id":  user_oid,
        })
        group_users_removed = gu_result.deleted_count

    return {
        "success":                    True,
        "message":                    "User removed from pool, shifts_users, and group pool",
        "user_id":                    payload.id,
        "shift_id":                   payload.shift_id,
        "pool_removed":               pool_result.deleted_count > 0,
        "shifts_users_removed":       su_result.deleted_count,
        "groups_affected":            [str(g) for g in group_oids],
        "group_pool_removed":         group_pool_removed,
        "group_users_removed":        group_users_removed,
    }


# ── Geo / time helpers ────────────────────────────────────────────────────────

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return round(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)), 2)


async def _get_shift_client_coords(db, shift_oid: ObjectId):
    """shifts._id → shifts.client_id → clients.xn_client_id → lat/lng + name"""
    shift = await db["shifts"].find_one({"_id": shift_oid}, {"client_id": 1, "client_name": 1})
    if not shift or not shift.get("client_id"):
        return None
    client = await db["clients"].find_one(
        {"xn_client_id": shift["client_id"]},
        {"latitude": 1, "longitude": 1, "location": 1, "name": 1, "address": 1}
    )
    if not client:
        return None
    # Check top-level lat/lng first, then nested location dict
    lat = client.get("latitude")
    lng = client.get("longitude")
    if (lat is None or lng is None):
        loc = client.get("location") or {}
        lat = loc.get("latitude") or loc.get("lat")
        lng = loc.get("longitude") or loc.get("lng") or loc.get("lon")
    if lat is None or lng is None:
        return None
    return {
        "latitude":  float(lat),
        "longitude": float(lng),
        "name":      client.get("name") or shift.get("client_name"),
        "address":   client.get("address"),
    }


def _user_location_coords(u: dict):
    loc = u.get("location")
    if isinstance(loc, dict):
        lat = loc.get("latitude") or loc.get("lat")
        lng = loc.get("longitude") or loc.get("lng") or loc.get("lon")
        if lat is not None and lng is not None:
            return (float(lat), float(lng))
    lat, lng = u.get("latitude"), u.get("longitude")
    if lat is not None and lng is not None:
        return (float(lat), float(lng))
    return None


def _format_time_ago(dt) -> str:
    """Format a datetime as 'just now', 'X minutes ago', 'X hours ago', 'X days ago'."""
    if not dt:
        return None
    if not hasattr(dt, 'tzinfo'):
        return str(dt)
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    diff = now - dt
    seconds = int(diff.total_seconds())
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        m = seconds // 60
        return f"{m} minute{'s' if m != 1 else ''} ago"
    if seconds < 86400:
        h = seconds // 3600
        return f"{h} hour{'s' if h != 1 else ''} ago"
    d = seconds // 86400
    return f"{d} day{'s' if d != 1 else ''} ago"


def _parse_time(t: str):
    """Parse 'HH:MM' to total minutes from midnight."""
    if not t:
        return None
    try:
        h, m = t.split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return None


def _times_overlap(s1: str, e1: str, s2: str, e2: str) -> bool:
    """Check if two time ranges overlap (handles overnight shifts)."""
    a = _parse_time(s1)
    b = _parse_time(e1)
    c = _parse_time(s2)
    d = _parse_time(e2)
    if None in (a, b, c, d):
        return False
    # Handle overnight: if end < start, add 24h
    if b <= a:
        b += 1440
    if d <= c:
        d += 1440
    return a < d and c < b


def _gap_minutes(e1: str, s2: str) -> int:
    """Gap in minutes between end of shift 1 and start of shift 2."""
    e = _parse_time(e1)
    s = _parse_time(s2)
    if None in (e, s):
        return 9999
    gap = s - e
    if gap < 0:
        gap += 1440
    return gap


def _shift_type(timing: str) -> str:
    """Extract shift type from shift_timing or shift_type field."""
    if not timing:
        return ""
    t = timing.lower()
    if "night" in t:
        return "night"
    if "day" in t or "morning" in t or "afternoon" in t:
        return "day"
    return ""


# ── Exclusion tags ────────────────────────────────────────────────────────────
# NOTE (unchanged from the original, flagged for a follow-up fix):
#   * The "Rule 6" minimum-gap check below sits OUTSIDE the `if same_day:` block,
#     so it evaluates against every shift in the ±10 day window, not just the
#     same or adjacent day as the comment claims.
#   * The threshold is 120 minutes, the tag is named `under_6h_gap`, and
#     /assign's message says "Less than 5 hours". Three different numbers.

async def _get_user_exclusion_tags(db, user_email: str, target_shift: dict, banned_clients: list = None, user_tags: list = None, user_oid=None) -> list:
    """
    Returns list of exclusion tag strings for a user against a target shift.
    Checks:
      0.  User has exclusion staff tags (Last-Resort Booking, Avoid Booking, …)
      0b. Client banned by staff (banned_clients)
      0c. Level 1 document expired/pending/not_approved
      1.  Same-day overlapping shift
      ...
    """
    if not user_email:
        return []

    tags: list = []

    # ── 0. Check user staff tags ──────────────────────────────────────────────
    EXCLUDED_TAG_NAMES = {
        "last-resort booking",
        "avoid booking",
        "temporarily unavailable",
        "no calls or emails",
        "no bulk emails",
        "no calls",
        "direct bookings only",
        "contact on request only",
    }
    if user_tags:
        for tag in user_tags:
            tag_name = (tag.get("name", "") if isinstance(tag, dict) else str(tag)).lower().strip()
            if tag_name in EXCLUDED_TAG_NAMES:
                tags.append(f"tag:{tag.get('name', tag_name) if isinstance(tag, dict) else tag_name}")
            # Premium Shifts Only — exclude when shift is NOT premium
            if tag_name == "premium shifts only" and not target_shift.get("is_premium"):
                tags.append("tag:Premium Shifts Only")

    # ── 0b. Check if client is banned by this staff (priority) ───────────────
    if banned_clients:
        shift_client_id = str(target_shift.get("client_id", ""))
        for bc in banned_clients:
            bc_id = str(bc.get("id", "")) if isinstance(bc, dict) else str(bc)
            if bc_id and bc_id == shift_client_id:
                # Insert at front for priority, keep any tag: entries after
                tags.insert(0, "Client Banned Staff")
                break

    # Return early if we have any tags so far
    if tags:
        return tags

    # ── 0c. Check Level 1 documents ──────────────────────────────────────────
    if user_oid:
        BAD_STATUSES = {"expired", "pending", "not_approved"}
        level1_doc = await db["documents_new"].find_one(
            {
                "user_id": user_oid,
                "level":   1,
                "status":  {"$in": list(BAD_STATUSES)},
            },
            {"status": 1}
        )
        if level1_doc:
            status = level1_doc.get("status", "invalid")
            return [f"level1_doc_{status}"]

        # ── Rule 6 only: same-day gap < 2 hours ──────────────────────────────────
    target_date  = target_shift.get("date")
    target_start = target_shift.get("start_time", "")
    target_end   = target_shift.get("end_time", "")

    if not target_date or not target_start or not target_end:
        return tags

    # ±1 day window — catches overnight shifts whose date field is the
    # previous calendar day, while excluding all unrelated days
    try:
        from datetime import timedelta
        if hasattr(target_date, "date"):
            td = target_date
        else:
            from datetime import datetime as _dt
            td = _dt.strptime(str(target_date)[:10], "%Y-%m-%d")

        date_filter = {
            "date": {
                "$gte": td - timedelta(days=1),
                "$lte": td + timedelta(days=1),
            }
        }
    except Exception:
        return tags

    existing_shifts_raw = await db["shifts"].find(
        {"staff_email": user_email, **date_filter},
        {"date": 1, "start_time": 1, "end_time": 1, "slots": 1, "upstream_status": 1}
    ).to_list(length=200)

    # Only Upcoming shifts count
    existing_shifts = [
        s for s in existing_shifts_raw
        if s.get("upstream_status") == "Upcoming"
    ]

    for es in existing_shifts:
        es_slots = es.get("slots") or []

        # Build time_ranges from slots or top-level fields
        time_ranges = []
        if es_slots:
            for sl in es_slots:
                time_ranges.append({
                    "date":  sl.get("date"),
                    "start": sl.get("start_time", ""),
                    "end":   sl.get("end_time", ""),
                })
        else:
            time_ranges.append({
                "date":  es.get("date"),
                "start": es.get("start_time", ""),
                "end":   es.get("end_time", ""),
            })

        for tr in time_ranges:
            tr_date  = tr["date"]
            tr_start = tr["start"]
            tr_end   = tr["end"]

            # ── MUST be the same calendar day ────────────────────────────────
            if not tr_date or not target_date:
                continue
            try:
                tr_day  = tr_date.date() if hasattr(tr_date, "date") else None
                tgt_day = target_date.date() if hasattr(target_date, "date") else None
                if not tr_day or not tgt_day or tr_day != tgt_day:
                    continue          # different day → skip entirely
            except Exception:
                continue

            if not tr_start or not tr_end:
                continue

            # gap1: existing ends → target starts
            #   e.g. existing 10:00-14:00, target 15:00-20:00 → gap = 60 min ✅
            gap1 = _gap_minutes(tr_end, target_start)

            # gap2: target ends → existing starts
            #   e.g. target 06:00-08:00, existing 09:30-14:00 → gap = 90 min ✅
            gap2 = _gap_minutes(target_end, tr_start)

            if (0 < gap1 < 120) or (0 < gap2 < 120):
                if "under_6h_gap" not in tags:
                    tags.append("under_6h_gap")
                return tags   # no need to check further

    return tags


# ── POST /shift-users/list ────────────────────────────────────────────────────

class ListShiftUsersRequest(BaseModel):
    shift_id:           str
    page:               int = 1
    per_page:           int = 20
    radius:             Optional[float] = None
    order_by:           Optional[str]   = None
    sort:               Optional[str]   = "asc"
    county_multiple:    Optional[list]  = None
    user_type_multiple: Optional[list]  = None
    excluded:           Optional[int]   = None
    in_pool:            Optional[int]   = None
    search:             Optional[str]   = None
    gender_id:          Optional[str]   = None
    gender_multiple:    Optional[list]  = None
    visa_type_id:       Optional[str]   = None
    group_id:           Optional[str]   = None

    qqi_status_number:      Optional[int]   = None
    user_sub_type_multiple: Optional[list]  = None


@router.post(
    "/list",
    summary="List shift_users records for a shift with pagination",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("120/minute")
async def list_shift_users_paginated(request: Request, payload: ListShiftUsersRequest):
    """
    Body: { "shift_id": "<shift_id>", "page": 1, "per_page": 20 }

    Returns Enabled users whose designation matches the shift's user_type
    (exact, case-insensitive), narrowed to the upstream available-staff-list.

    Exclusion tags are computed for the users on the requested page only.
    When excluded / in_pool / radius filters are active the collection is
    walked in chunks and the walk stops as soon as the page is full, so
    per_page=10 costs ~10 exclusion checks, never a full-table pass.
    """
    db        = _get_db()
    shift_oid = _resolve_oid(payload.shift_id, "shift_id")
    skip      = max(0, (payload.page - 1) * payload.per_page)
    limit     = payload.per_page

    order_by = payload.order_by or "name"
    reverse  = (payload.sort or "asc").lower() == "desc"

    # ── Shift ────────────────────────────────────────────────────────────────
    target_shift = await db["shifts"].find_one(
        {"_id": shift_oid},
        {"date": 1, "start_time": 1, "end_time": 1, "shift_timing": 1,
         "shift_type": 1, "slots": 1, "user_type": 1, "client_id": 1,
         "is_premium": 1, "requested_staff_list": 1, "shift_id": 1}
    )
    if not target_shift:
        raise HTTPException(status_code=404, detail=f"Shift {payload.shift_id} not found")

    xn_shift_id = target_shift.get("shift_id")

    # ── Upstream available-staff-list ────────────────────────────────────────
    upstream_xn_ids:       list = []
    upstream_distance_map: dict = {}

    if xn_shift_id:
        try:
            import httpx as _httpx
            upstream_url = f"{settings.SHIFT_URL.rstrip('/')}/ai/shifts/available-staff-list"
            upstream_headers = {
                "Api-Key":      settings.SHIFT_INTERNAL_API_KEY,
                "Content-Type": "application/json",
                "Accept":       "application/json",
            }
            async with _httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    upstream_url,
                    json={"shift_id": xn_shift_id},
                    headers=upstream_headers,
                )
            if resp.status_code == 200:
                for s in (resp.json().get("data") or []):
                    xn_id = str(s.get("id", ""))
                    if xn_id:
                        upstream_xn_ids.append(xn_id)
                        upstream_distance_map[xn_id] = s.get("staff_shift_distance")
        except Exception as e:
            logger.warning(f"[list] upstream available-staff-list failed: {e}")

    # ── Build the Mongo filter ───────────────────────────────────────────────
    # Collected as $and clauses so no later block can clobber an earlier $or.
    clauses: list = [{"status": "Enabled"}]

    # shifts.user_type  ==  users.designation   (exact, case-insensitive)
    if not payload.user_type_multiple:
        shift_user_type = target_shift.get("user_type")
        if shift_user_type:
            clauses.append({
                "designation": {
                    "$regex":   f"^{re.escape(str(shift_user_type).strip())}$",
                    "$options": "i",
                }
            })

    if upstream_xn_ids:
        clauses.append({"xn_user_id": {"$in": upstream_xn_ids}})

    # Gender
    if payload.gender_multiple:
        _gids = [str(g).strip() for g in payload.gender_multiple if g]
        if _gids:
            clauses.append({"gender_id": {"$in": _gids}})
    elif payload.gender_id:
        clauses.append({"gender_id": payload.gender_id.strip()})

    # Visa type
    if payload.visa_type_id:
        clauses.append({"visa_type_id": payload.visa_type_id.strip()})

    # QQI status
    if payload.qqi_status_number is not None:
        clauses.append({"qqi_status_number": payload.qqi_status_number})

    # User sub type
    if payload.user_sub_type_multiple:
        sub_oids = [ObjectId(str(i)) for i in payload.user_sub_type_multiple
                    if ObjectId.is_valid(str(i))]
        if sub_oids:
            clauses.append({"user_sub_type_oids": {"$in": sub_oids}})

    # County — county_id may be stored as string or ObjectId
    if payload.county_multiple:
        county_values = []
        for c in payload.county_multiple:
            c_str = str(c)
            county_values.append(c_str)
            if ObjectId.is_valid(c_str):
                county_values.append(ObjectId(c_str))
        if county_values:
            clauses.append({"county_id": {"$in": county_values}})

    # Explicit user types — match by user_type_id or by designation name
    if payload.user_type_multiple:
        valid_type_oids = [ObjectId(str(t)) for t in payload.user_type_multiple
                           if ObjectId.is_valid(str(t))]
        if valid_type_oids:
            type_names = []
            async for ut in db["user_types"].find({"_id": {"$in": valid_type_oids}}, {"name": 1}):
                if ut.get("name"):
                    type_names.append(ut["name"])
            clauses.append({"$or": [
                {"user_type_id": {"$in": valid_type_oids}},
                {"designation":  {"$in": type_names}},
            ]})

    # Search — name / email / phone
    if payload.search and payload.search.strip():
        s     = payload.search.strip()
        s_rx  = re.escape(s)
        parts = s.split()
        conds = [
            {"first_name": {"$regex": s_rx, "$options": "i"}},
            {"last_name":  {"$regex": s_rx, "$options": "i"}},
            {"email":      {"$regex": s_rx, "$options": "i"}},
            {"phone":      {"$regex": s_rx, "$options": "i"}},
        ]
        if len(parts) >= 2:
            conds.append({"$and": [
                {"first_name": {"$regex": re.escape(parts[0]),  "$options": "i"}},
                {"last_name":  {"$regex": re.escape(parts[-1]), "$options": "i"}},
            ]})
        clauses.append({"$or": conds})

    user_filter = clauses[0] if len(clauses) == 1 else {"$and": clauses}

    db_total = await db["users"].count_documents(user_filter)

    # ── Client coords (needed during selection for the radius filter) ────────
    client_data   = await _get_shift_client_coords(db, shift_oid)
    client_coords = (client_data["latitude"], client_data["longitude"]) if client_data else None

    shift_client_info = None
    client_location   = None
    if client_data:
        shift_client_info = {
            "name":             client_data.get("name"),
            "address":          client_data.get("address"),
            "client_latitude":  client_data["latitude"],
            "client_longitude": client_data["longitude"],
        }
        client_location = {
            "latitude":  client_data["latitude"],
            "longitude": client_data["longitude"],
        }

    # ── Exclusion: per-(user, shift) cache + bounded concurrency ─────────────
    _sem          = asyncio.Semaphore(_EXCLUSION_CONCURRENCY)
    _cache_writes: list = []
    _shift_key    = str(shift_oid)
    _now          = datetime.now(timezone.utc)

    def _cached_tags(u: dict):
        """Return cached tags for THIS shift if present and fresh, else None."""
        entry = (u.get("exclusion_cache_by_shift") or {}).get(_shift_key)
        if not isinstance(entry, dict):
            return None
        at = entry.get("at")
        if at is None:
            return None
        if getattr(at, "tzinfo", None) is None:
            at = at.replace(tzinfo=timezone.utc)
        if (_now - at).total_seconds() > _EXCLUSION_CACHE_TTL_SECONDS:
            return None
        tags = entry.get("tags")
        return tags if isinstance(tags, list) else None

    async def _exclusion_for(u: dict) -> list:
        cached = _cached_tags(u)
        if cached is not None:
            return cached
        if not (u.get("email") and target_shift):
            return []
        async with _sem:
            tags = await _get_user_exclusion_tags(
                db,
                u.get("email"),
                target_shift,
                u.get("banned_clients") or [],
                u.get("tags") or [],
                u["_id"],
            )
        _cache_writes.append(UpdateOne(
            {"_id": u["_id"]},
            {"$set": {f"exclusion_cache_by_shift.{_shift_key}": {"tags": tags, "at": _now}}},
        ))
        return tags

    async def _annotate(chunk: list) -> None:
        """
        Compute exclusion, pool membership, distance and last-contacted for one
        chunk of candidates. Results are stashed on the user dicts under _keys.
        """
        if not chunk:
            return
        oids = [u["_id"] for u in chunk]

        # pool membership
        pool_ids = {
            str(p["user_id"])
            async for p in db["shifts_pool"].find(
                {"shift_id": shift_oid, "user_id": {"$in": oids}}, {"user_id": 1}
            )
        }

        # last contacted — shifts_users, then shifts_group_users if newer
        lc_map: dict = {}
        async for su in db["shifts_users"].find(
            {"user_id": {"$in": oids}, "call_processed_at": {"$ne": None}},
            {"user_id": 1, "call_processed_at": 1, "channel": 1}
        ).sort("call_processed_at", -1):
            uid = str(su.get("user_id", ""))
            if uid not in lc_map:
                lc_map[uid] = (su.get("call_processed_at"), su.get("channel") or "")

        async for gu in db["shifts_group_users"].find(
            {"user_id": {"$in": oids}, "call_processed_at": {"$ne": None}},
            {"user_id": 1, "call_processed_at": 1, "channel": 1}
        ).sort("call_processed_at", -1):
            uid = str(gu.get("user_id", ""))
            dt  = gu.get("call_processed_at")
            cur = lc_map.get(uid)
            if cur is None or (dt and cur[0] and dt > cur[0]):
                lc_map[uid] = (dt, gu.get("channel") or "")

        # exclusion — concurrent, capped by _sem
        all_tags = await asyncio.gather(*[_exclusion_for(u) for u in chunk])

        for u, tags in zip(chunk, all_tags):
            uid_str = str(u["_id"])
            u["_excl_tags"] = tags
            u["_excluded"]  = 1 if tags else 0
            u["_in_pool"]   = 1 if uid_str in pool_ids else 0
            u["_lc"]        = lc_map.get(uid_str)

            ucoords = _user_location_coords(u)
            dist = None
            if client_coords and ucoords:
                dist = _haversine_km(client_coords[0], client_coords[1],
                                     ucoords[0], ucoords[1])
            xn = str(u.get("xn_user_id", ""))
            if xn and upstream_distance_map.get(xn) is not None:
                dist = upstream_distance_map[xn]
            u["_distance_km"] = dist
            u["_coords"]      = ucoords

    def _keep(u: dict) -> bool:
        if payload.excluded is not None and u.get("_excluded", 0) != payload.excluded:
            return False
        if payload.in_pool is not None and u.get("_in_pool", 0) != payload.in_pool:
            return False
        if payload.radius is not None and client_coords:
            d = u.get("_distance_km")
            if d is not None and d > payload.radius:
                return False
        return True

    # ── Selection ────────────────────────────────────────────────────────────
    _has_local_filter = (payload.excluded is not None
                         or payload.in_pool is not None
                         or payload.radius is not None)

    # These orderings depend on values we compute locally, so the page cannot
    # be taken straight from the DB — the candidate set must be walked.
    _local_sort = order_by in ("distance_km", "rating", "last_contacted")

    db_sort        = [("first_name", -1 if (order_by == "name" and reverse) else 1)]
    scan_truncated = False

    if not _has_local_filter and not _local_sort:
        # Fast path: DB pagination is exact, annotate exactly this page.
        users = await db["users"].find(user_filter, _LIST_PROJECTION) \
            .sort(db_sort).skip(skip).limit(limit).to_list(length=limit)
        await _annotate(users)
        filtered_total = db_total
        scanned        = len(users)
    else:
        need       = skip + limit
        chunk_size = max(min(limit, 200) * 5, 100)
        # A local sort needs every candidate before it can order them; a plain
        # filter can stop as soon as the requested page is full.
        scan_cap   = min(_MAX_SCAN, 3000 if _local_sort else max(need * 20, 500))

        matched: list = []
        scanned = offset = 0

        while scanned < scan_cap:
            chunk = await db["users"].find(user_filter, _LIST_PROJECTION) \
                .sort(db_sort).skip(offset).limit(chunk_size) \
                .to_list(length=chunk_size)
            if not chunk:
                break
            offset  += len(chunk)
            scanned += len(chunk)

            await _annotate(chunk)
            matched.extend([u for u in chunk if _keep(u)])

            if not _local_sort and len(matched) >= need:
                break

        scan_truncated = scanned >= scan_cap and scanned < db_total

        # Local ordering, applied to everything we walked
        if order_by == "distance_km":
            matched.sort(
                key=lambda u: u.get("_distance_km") if u.get("_distance_km") is not None
                else float("inf"),
                reverse=reverse,
            )
        elif order_by == "rating":
            matched.sort(key=lambda u: u.get("rating") or 0, reverse=reverse)
        elif order_by == "last_contacted":
            _epoch = datetime.min.replace(tzinfo=timezone.utc)

            def _lc_key(u):
                lc = u.get("_lc")
                if not lc or not lc[0]:
                    return _epoch
                dt = lc[0]
                if getattr(dt, "tzinfo", None) is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt

            matched.sort(key=_lc_key, reverse=reverse)

        filtered_total = len(matched)
        users          = matched[skip: skip + limit]

    logger.info(
        f"[list] shift={payload.shift_id} designation={target_shift.get('user_type')} "
        f"db_total={db_total} scanned={scanned} matched={filtered_total} page={len(users)}"
    )

    # ══════════════════════════════════════════════════════════════════════════
    #  From here on we only ever touch `users` — the current page.
    # ══════════════════════════════════════════════════════════════════════════
    user_oids_page = [u["_id"] for u in users]

    # ── Requested staff ──────────────────────────────────────────────────────
    requested_user_ids: set = {
        str(rs.get("staff_id", ""))
        for rs in (target_shift.get("requested_staff_list") or [])
        if rs.get("staff_id")
    }

    # ── checked flag via shifts_group_pool ───────────────────────────────────
    checked_user_set: set = set()
    if payload.group_id and ObjectId.is_valid(str(payload.group_id)) and user_oids_page:
        async for gp in db["shifts_group_pool"].find(
            {"group_id": ObjectId(str(payload.group_id)), "user_id": {"$in": user_oids_page}},
            {"user_id": 1}
        ):
            checked_user_set.add(str(gp["user_id"]))

    # ── Sub type names ───────────────────────────────────────────────────────
    all_sub_oids = [
        ObjectId(str(oid))
        for u in users
        for oid in (u.get("user_sub_type_oids") or [])
        if oid and ObjectId.is_valid(str(oid))
    ]
    sub_type_name_map: dict = {}
    if all_sub_oids:
        async for st in db["user_sub_types"].find({"_id": {"$in": all_sub_oids}}, {"name": 1}):
            sub_type_name_map[str(st["_id"])] = st.get("name", "")

    # ── Visa type names ──────────────────────────────────────────────────────
    visa_type_name_map: dict = {}
    _vt_oids = [ObjectId(str(u["visa_type_id"])) for u in users
                if u.get("visa_type_id") and ObjectId.is_valid(str(u["visa_type_id"]))]
    if _vt_oids:
        async for vt in db["visa_types"].find({"_id": {"$in": _vt_oids}}, {"name": 1}):
            visa_type_name_map[str(vt["_id"])] = vt.get("name", "")

    # ── Visa hours — at most 2 users on this page missing consumed_hours ─────
    visa_info_map: dict = {}
    if xn_shift_id:
        missing = [u for u in users
                   if u.get("xn_user_id") and u.get("consumed_hours") is None][:2]
        if missing:
            try:
                import httpx as _httpx_v
                _visa_url     = f"{settings.USER_API_URL.rstrip('/')}/ai/recruitments/visa-hours"
                _visa_headers = {"Api-Key": settings.USER_EXTERNAL_API_KEY,
                                 "Content-Type": "application/json"}

                async def _fetch_visa(client, u):
                    xn_uid  = u.get("xn_user_id")
                    uid_str = str(u["_id"])
                    try:
                        _vr = await client.get(
                            _visa_url,
                            params={"shift_id": xn_shift_id, "staff_id": xn_uid},
                            headers=_visa_headers,
                        )
                        if _vr.status_code == 200:
                            _vd  = _vr.json().get("data") or {}
                            _upd = {k: _vd[k] for k in ("work_permit_exemption", "consumed_hours")
                                    if k in _vd}
                            if _upd:
                                await db["users"].update_one({"_id": u["_id"]}, {"$set": _upd})
                                u.update(_upd)
                                visa_info_map[uid_str] = {"status": "fetched", "data": _vd}
                            else:
                                visa_info_map[uid_str] = {"status": "empty_response", "data": _vd}
                        else:
                            visa_info_map[uid_str] = {"status": f"http_{_vr.status_code}",
                                                      "error": _vr.text[:200]}
                    except Exception as _e:
                        logger.error(f"[visa] {xn_uid}: {_e}")
                        visa_info_map[uid_str] = {"status": "error", "error": str(_e)}

                async with _httpx_v.AsyncClient(timeout=10.0) as _vc:
                    await asyncio.gather(*[_fetch_visa(_vc, u) for u in missing])
            except Exception as _e2:
                logger.error(f"[visa] outer: {_e2}")

    # ── County / user_type name maps ─────────────────────────────────────────
    county_name_to_id:        dict = {}
    county_oid_to_name:       dict = {}
    designation_to_type_id:   dict = {}
    designation_to_type_name: dict = {}
    type_id_to_name:          dict = {}

    users_needing_county = [u for u in users if not u.get("county_id") and u.get("country_id")]
    users_needing_type   = [u for u in users if not u.get("user_type_id") and u.get("designation")]

    if users_needing_county:
        raw_cids   = list({str(u["country_id"]) for u in users_needing_county if u.get("country_id")})
        valid_oids = [ObjectId(c) for c in raw_cids if ObjectId.is_valid(c)]
        if valid_oids:
            async for co in db["county"].find({"_id": {"$in": valid_oids}}, {"_id": 1, "name": 1}):
                county_name_to_id[str(co["_id"])]  = str(co["_id"])
                county_oid_to_name[str(co["_id"])] = co.get("name", "")

    existing_county_oids = list({
        ObjectId(str(u["county_id"])) for u in users
        if u.get("county_id") and ObjectId.is_valid(str(u["county_id"]))
    })
    if existing_county_oids:
        async for co in db["county"].find({"_id": {"$in": existing_county_oids}}, {"_id": 1, "name": 1}):
            county_oid_to_name[str(co["_id"])] = co.get("name", "")

    if users_needing_type:
        designations = list({u["designation"] for u in users_needing_type if u.get("designation")})
        async for ut in db["user_types"].find({"name": {"$in": designations}}, {"_id": 1, "name": 1}):
            designation_to_type_id[ut["name"]]   = str(ut["_id"])
            designation_to_type_name[ut["name"]] = ut["name"]

    existing_type_oids = list({
        ObjectId(str(u["user_type_id"])) for u in users
        if u.get("user_type_id") and ObjectId.is_valid(str(u["user_type_id"]))
    })
    if existing_type_oids:
        async for ut in db["user_types"].find({"_id": {"$in": existing_type_oids}}, {"_id": 1, "name": 1}):
            type_id_to_name[str(ut["_id"])] = ut.get("name", "")

    # ── Prior shifts at this client ──────────────────────────────────────────
    shift_client_id          = target_shift.get("client_id")
    prior_shifts_map:         dict = {}
    last_shift_at_client_map: dict = {}

    if user_oids_page:
        if shift_client_id:
            client_shift_ids = await db["shifts"].distinct(
                "_id", {"client_id": shift_client_id,
                        "staff_email": {"$exists": True, "$ne": None}}
            )
            if client_shift_ids:
                async for psu in db["shifts_users"].find(
                    {"user_id": {"$in": user_oids_page},
                     "shift_id": {"$in": client_shift_ids},
                     "availability": 1},
                    {"user_id": 1, "assigned_at": 1}
                ):
                    uid = str(psu.get("user_id", ""))
                    prior_shifts_map[uid] = prior_shifts_map.get(uid, 0) + 1
                    assigned = psu.get("assigned_at")
                    if assigned and (uid not in last_shift_at_client_map
                                     or assigned > last_shift_at_client_map[uid]):
                        last_shift_at_client_map[uid] = assigned
        else:
            async for ps in db["shifts_users"].aggregate([
                {"$match": {"user_id": {"$in": user_oids_page}, "availability": 1}},
                {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
            ]):
                prior_shifts_map[str(ps["_id"])] = ps["count"]

    # ── Build the response rows ──────────────────────────────────────────────
    results = []
    _backfill: list = []

    for u in users:
        uid_str = str(u["_id"])
        ucoords = u.get("_coords")

        # staff tags
        staff_tags = [
            {"id": str(t.get("id", "")), "name": t.get("name", "")} if isinstance(t, dict)
            else {"id": "", "name": str(t)}
            for t in (u.get("tags") or [])
        ]

        # last contacted
        last_contacted = None
        lc_entry = u.get("_lc")
        if lc_entry:
            lc_dt, lc_channel = lc_entry
            last_contacted = _format_time_ago(lc_dt)
            if last_contacted and lc_channel:
                last_contacted = f"{last_contacted} · {lc_channel}"

        # county
        county_id = county_name = None
        if u.get("county_id"):
            county_id   = str(u["county_id"])
            county_name = county_oid_to_name.get(county_id)
        elif u.get("country_id"):
            cid_str = str(u["country_id"])
            if cid_str in county_name_to_id:
                county_id   = county_name_to_id[cid_str]
                county_name = county_oid_to_name.get(county_id)
                _backfill.append(UpdateOne(
                    {"_id": u["_id"]}, {"$set": {"county_id": ObjectId(county_id)}}
                ))

        # user type
        user_type_id = user_type_name = None
        if u.get("user_type_id"):
            user_type_id   = str(u["user_type_id"])
            user_type_name = type_id_to_name.get(user_type_id)
        elif u.get("designation") and u["designation"] in designation_to_type_id:
            user_type_id   = designation_to_type_id[u["designation"]]
            user_type_name = designation_to_type_name.get(u["designation"])
            _backfill.append(UpdateOne(
                {"_id": u["_id"]}, {"$set": {"user_type_id": ObjectId(user_type_id)}}
            ))

        # visa hours
        visa_used  = u.get("visa_hours_used")
        visa_total = u.get("visa_hours_total")
        consumed   = u.get("consumed_hours")
        visa_hours_remaining = consumed if consumed is not None else (
            f"{visa_used}/{visa_total}" if visa_used is not None and visa_total else None
        )

        # work history
        prior_shifts      = prior_shifts_map.get(uid_str, 0)
        last_at_client_dt = last_shift_at_client_map.get(uid_str) if shift_client_id else None
        last_at_client    = _format_time_ago(last_at_client_dt) if last_at_client_dt else None
        _plural           = "s" if prior_shifts != 1 else ""
        if prior_shifts > 0 and last_at_client:
            work_history = f"{prior_shifts} Shift{_plural} · {last_at_client}"
        elif prior_shifts > 0:
            work_history = f"{prior_shifts} Shift{_plural}"
        elif last_at_client:
            work_history = f"0 Shifts · {last_at_client}"
        else:
            work_history = "0 Shifts"

        sub_oids = u.get("user_sub_type_oids") or []
        results.append({
            "id":                    uid_str,
            "xn_user_id":            u.get("xn_user_id"),
            "name":                  " ".join(filter(None, [u.get("first_name", ""),
                                                            u.get("last_name", "")])).strip() or "—",
            "email":                 u.get("email"),
            "phone":                 u.get("phone"),
            "designation":           u.get("designation"),
            "rating":                u.get("rating"),
            "channel":               "Phone",
            "staff_tags":            staff_tags,
            "last_contacted":        last_contacted,
            "visa_hours_remaining":  visa_hours_remaining,
            "work_permit_exemption": u.get("work_permit_exemption"),
            "consumed_hours":        u.get("consumed_hours"),
            "visa_info":             visa_info_map.get(
                                        uid_str,
                                        {"status": "cached"} if u.get("consumed_hours") is not None
                                        else {"status": "not_called"}),
            "gender_id":             str(u["gender_id"]) if u.get("gender_id") else None,
            "qqi_status_number":     u.get("qqi_status_number"),
            "user_sub_type_ids":     u.get("user_sub_type_ids") or [],
            "user_sub_type_oids":    [str(oid) for oid in sub_oids],
            "user_sub_types":        ([{"id": str(oid), "name": sub_type_name_map.get(str(oid), "")}
                                       for oid in sub_oids if ObjectId.is_valid(str(oid))]
                                      or [{"id": None, "name": n}
                                          for n in (u.get("user_sub_type_ids") or []) if n]),
            "visa_type_id":          u.get("visa_type_id"),
            "visa_type_name":        visa_type_name_map.get(str(u.get("visa_type_id", "")))
                                     if u.get("visa_type_id") else None,
            "prior_shifts":          prior_shifts,
            "work_history":          work_history,
            "status":                u.get("status"),
            "county_id":             county_id,
            "county":                county_name,
            "user_type_id":          user_type_id,
            "user_type":             user_type_name,
            "user_latitude":         ucoords[0] if ucoords else None,
            "user_longitude":        ucoords[1] if ucoords else None,
            "distance_km":           u.get("_distance_km"),
            "excluded":              u.get("_excluded", 0),
            "exclusion_tags":        u.get("_excl_tags") or [],
            "requested":             1 if uid_str in requested_user_ids else 0,
            "in_pool":               u.get("_in_pool", 0),
            "checked":               1 if uid_str in checked_user_set else 0,
        })

    # Name sort within the page (DB already ordered by first_name; this makes
    # the full display name authoritative). Other orderings were applied above.
    if order_by == "name":
        results.sort(key=lambda r: r["name"].lower(), reverse=reverse)

    # ── Flush writes ─────────────────────────────────────────────────────────
    if _cache_writes:
        try:
            await db["users"].bulk_write(_cache_writes, ordered=False)
        except Exception as e:
            logger.warning(f"[list] exclusion cache write failed: {e}")

    if _backfill:
        try:
            await db["users"].bulk_write(_backfill, ordered=False)
        except Exception as e:
            logger.warning(f"[list] county/user_type backfill failed: {e}")

    return {
        "success":             True,
        "total":               filtered_total,
        "db_total":            db_total,
        "scanned":             scanned,
        "scan_truncated":      scan_truncated,
        "exclusions_computed": len(_cache_writes),
        "page":                payload.page,
        "per_page":            payload.per_page,
        "shift_id":            payload.shift_id,
        "shift_user_type":     target_shift.get("user_type"),
        "shift_client":        shift_client_info,
        "client_location":     client_location,
        "radius":              payload.radius,
        "order_by":            order_by,
        "sort":                payload.sort or "asc",
        "data":                results,
    }


# ── POST /shift-users/list/export ─────────────────────────────────────────────

@router.post(
    "/list/export",
    summary="Export shift users list as CSV",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("10/minute")
async def export_shift_users_list(request: Request, payload: ListShiftUsersRequest):
    """Same payload as /list — exports matching users as CSV."""
    import csv, io
    from fastapi.responses import StreamingResponse

    payload.page     = 1
    payload.per_page = _MAX_SCAN

    result = await list_shift_users_paginated(request, payload)
    users  = result.get("data", [])

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["#", "Name", "Email", "Phone", "Designation", "County",
                     "Distance (km)", "Rating", "Excluded", "Exclusion Tags",
                     "Prior Shifts", "In Pool", "Requested"])

    for i, u in enumerate(users, 1):
        writer.writerow([
            i,
            u.get("name", ""),
            u.get("email", ""),
            u.get("phone", ""),
            u.get("designation", ""),
            u.get("county", ""),
            u.get("distance_km", ""),
            u.get("rating", ""),
            u.get("excluded", ""),
            ", ".join(u.get("exclusion_tags") or []),
            u.get("prior_shifts", ""),
            u.get("in_pool", ""),
            u.get("requested", ""),
        ])

    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=shift_users_{payload.shift_id}.csv"}
    )


# ── POST /shift-users/assign ──────────────────────────────────────────────────

class AssignStaffRequest(BaseModel):
    shift_id: str
    user_id:  str


@router.post(
    "/assign",
    summary="Assign a staff member to a shift (sets staff_email and assigned_staff)",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def assign_staff_to_shift(request: Request, payload: AssignStaffRequest):
    """
    Body: { "shift_id": "<shift._id>", "user_id": "<user._id>" }
    Sets shifts.staff_email = users.email and shifts.assigned_staff = users full name.
    """
    db = _get_db()

    user_oid  = _resolve_oid(payload.user_id,  "user_id")
    shift_oid = _resolve_oid(payload.shift_id, "shift_id")

    # Fetch user
    user = await db["users"].find_one(
        {"_id": user_oid},
        {"first_name": 1, "last_name": 1, "email": 1, "xn_user_id": 1,
         "designation": 1, "rating": 1, "banned_clients": 1, "tags": 1}
    )
    if not user:
        raise HTTPException(status_code=404, detail=f"User {payload.user_id} not found")

    # Fetch shift
    shift = await db["shifts"].find_one({"_id": shift_oid}, {"_id": 1, "shift_code": 1, "name": 1, "shift_id": 1})
    if not shift:
        raise HTTPException(status_code=404, detail=f"Shift {payload.shift_id} not found")

    xn_shift_id = shift.get("shift_id")
    xn_user_id  = user.get("xn_user_id")

    # ── Call upstream assign-staff-with-checks ────────────────────────────────
    if not xn_shift_id or not xn_user_id:
        raise HTTPException(
            status_code=422,
            detail=f"Missing upstream IDs — shift_id={xn_shift_id} staff_id={xn_user_id}"
        )

    import httpx as _httpx
    upstream_url = f"{settings.SHIFT_URL.rstrip('/')}/ai/shifts/assign-staff-with-checks"
    upstream_headers = {
        "Api-Key":      settings.SHIFT_INTERNAL_API_KEY,
        "Content-Type": "application/json",
        "Accept":       "application/json",
    }
    logger.info(f"[assign] upstream={upstream_url} shift_id={xn_shift_id} staff_id={xn_user_id}")

    try:
        async with _httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                upstream_url,
                json={"shift_id": xn_shift_id, "staff_id": xn_user_id},
                headers=upstream_headers
            )
        try:
            upstream_body = resp.json()
        except Exception:
            upstream_body = {}
        logger.info(f"[assign] upstream status={resp.status_code} body={upstream_body}")

        if resp.status_code != 200 or not upstream_body.get("success"):
            msg = upstream_body.get("message") or f"Upstream failed (status {resp.status_code})"
            return {
                "success":         False,
                "message":         msg,
                "upstream_status": resp.status_code,
                "upstream_data":   upstream_body.get("data"),
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Upstream error: {str(e)}")

    email      = user.get("email") or ""
    full_name  = " ".join(filter(None, [user.get("first_name",""), user.get("last_name","")])).strip() or "—"
    now        = datetime.now(timezone.utc)

    # Check exclusion conditions before assigning
    target_shift = await db["shifts"].find_one(
        {"_id": shift_oid},
        {"date": 1, "start_time": 1, "end_time": 1, "shift_timing": 1,
         "shift_type": 1, "slots": 1, "client_id": 1, "is_premium": 1}
    ) or {}
    exclusion_tags = await _get_user_exclusion_tags(
        db, email, target_shift,
        user.get("banned_clients") or [], user.get("tags") or [], user_oid
    ) if email and target_shift else []

    if exclusion_tags:
        tag_messages = {
            "Client Banned Staff":      "Staff has banned this client",
            "banned_client":            "Staff has banned this client",
            "level1_doc_expired":       "Level 1 certificate is expired",
            "level1_doc_pending":       "Level 1 certificate is pending approval",
            "level1_doc_not_approved":  "Level 1 certificate is not approved",
            "overlap":                  "User has an overlapping shift on the same day",
            "duplicate_day":            "User already has a day shift on this date",
            "duplicate_night":          "User already has a night shift on this date",
            "consecutive_day_night":    "User has both day and night shifts on this date",
            "exceeds_16h":              "Assignment would exceed 16 consecutive hours",
            "under_6h_gap":             "Less than 2 hours gap between shifts",
        }
        reasons = [
            tag_messages.get(t, t.replace("tag:", "Staff tag: ") if t.startswith("tag:") else t)
            for t in exclusion_tags
        ]
        raise HTTPException(
            status_code=409,
            detail={
                "message":        "Cannot assign — exclusion conditions triggered",
                "exclusion_tags": exclusion_tags,
                "reasons":        reasons,
            }
        )

    # Update shift with assigned staff
    await db["shifts"].update_one(
        {"_id": shift_oid},
        {"$set": {
            "staff_email":     email,
            "assigned_staff":  full_name,
            "staff_id":        str(user_oid),
            "assigned_at":     now,
            "updated_at":      now,
            "status":          "Upcoming",
            "upstream_status": "Upcoming",
        }}
    )

    # Clear cached visa hours and the exclusion cache — the user's schedule just
    # changed, so every cached (user, shift) verdict is now stale.
    await db["users"].update_one(
        {"_id": user_oid},
        {"$unset": {
            "work_permit_exemption":    "",
            "consumed_hours":           "",
            "exclusion_cache_by_shift": "",
            "exclusion_cache":          "",   # legacy key
            "exclusion_cache_at":       "",   # legacy key
        }}
    )

    logger.info(f"Assigned user={payload.user_id} ({email}) to shift={payload.shift_id}")

    return {
        "success":        True,
        "message":        f"{full_name} assigned to shift",
        "shift_id":       payload.shift_id,
        "user_id":        payload.user_id,
        "assigned_staff": full_name,
        "staff_email":    email,
        "designation":    user.get("designation"),
        "rating":         user.get("rating"),
        "assigned_at":    now.isoformat(),
    }


# ── POST /shift-users/list-multi ─────────────────────────────────────────────

class ListMultiShiftUsersRequest(BaseModel):
    shift_ids:          list                    # shifts._id strings
    group_id:           Optional[str]   = None  # also check shifts_group_pool
    page:               int = 1
    per_page:           int = 20
    radius:             Optional[float] = None
    order_by:           Optional[str]   = None
    sort:               Optional[str]   = "asc"
    county_multiple:    Optional[list]  = None
    user_type_multiple: Optional[list]  = None
    excluded:           Optional[int]   = None
    in_pool:            Optional[int]   = None  # 1 = in pool for ANY shift or the group
    gender_id:          Optional[str]   = None
    gender_multiple:    Optional[list]  = None

    qqi_status_number:      Optional[int]   = None
    user_sub_type_multiple: Optional[list]  = None
    visa_type_id:           Optional[str]   = None
    search:                 Optional[str]   = None


@router.post(
    "/list-multi/export",
    summary="Export list-multi shift users as CSV",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("10/minute")
async def export_shift_users_list_multi(request: Request, payload: ListMultiShiftUsersRequest):
    """Same payload as /list-multi — exports matching users as CSV."""
    import csv, io
    from fastapi.responses import StreamingResponse

    payload.page     = 1
    payload.per_page = _MAX_SCAN

    result = await list_shift_users_multi(request, payload)
    users  = result.get("data", [])

    buf    = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["#", "Name", "Email", "Phone", "Designation", "County",
                     "Distance (km)", "Rating", "Excluded", "Exclusion Tags",
                     "Prior Shifts", "In Pool", "Requested"])

    for i, u in enumerate(users, 1):
        writer.writerow([
            i,
            u.get("name", ""),
            u.get("email", ""),
            u.get("phone", ""),
            u.get("designation", ""),
            u.get("county", ""),
            u.get("distance_km", ""),
            u.get("rating", ""),
            u.get("excluded", ""),
            ", ".join(u.get("exclusion_tags") or []),
            u.get("prior_shifts", ""),
            u.get("in_pool", ""),
            u.get("requested", ""),
        ])

    buf.seek(0)
    shift_label = "_".join(payload.shift_ids[:2]) if payload.shift_ids else "multi"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=shift_users_{shift_label}.csv"},
    )


@router.post(
    "/list-multi",
    summary="List users for multiple shifts with same enrichment as /list",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def list_shift_users_multi(request: Request, payload: ListMultiShiftUsersRequest):
    """
    Body: { "shift_ids": ["<id1>", "<id2>", ...], "page": 1, "per_page": 20, ... }

    Candidates are Enabled users whose designation matches any of the shifts'
    user_type values (exact, case-insensitive), narrowed to the union of the
    upstream available-staff-lists.

    Exclusion is evaluated against the FIRST shift_id (the primary) and is
    computed only for the users on the requested page. When
    excluded / in_pool / radius is active the candidate set is walked in
    chunks and the walk stops as soon as the page is full.
    """
    db = _get_db()

    if not payload.shift_ids:
        raise HTTPException(status_code=400, detail="shift_ids must not be empty")

    shift_oids = []
    for sid in payload.shift_ids:
        if not ObjectId.is_valid(str(sid)):
            raise HTTPException(status_code=422, detail=f"Invalid shift_id: {sid}")
        shift_oids.append(ObjectId(str(sid)))

    primary_oid = shift_oids[0]
    skip        = max(0, (payload.page - 1) * payload.per_page)
    limit       = payload.per_page

    order_by = payload.order_by or "name"
    reverse  = (payload.sort or "asc").lower() == "desc"

    # ── Shifts ───────────────────────────────────────────────────────────────
    shift_docs = await db["shifts"].find(
        {"_id": {"$in": shift_oids}},
        {"user_type": 1, "requested_staff_list": 1, "shift_id": 1}
    ).to_list(length=100)

    if not shift_docs:
        raise HTTPException(status_code=404, detail="No shifts found for the given shift_ids")

    shift_user_types = list({s["user_type"] for s in shift_docs if s.get("user_type")})

    # Primary shift — exclusion + distance are measured against this one
    target_shift = await db["shifts"].find_one(
        {"_id": primary_oid},
        {"date": 1, "start_time": 1, "end_time": 1, "shift_timing": 1,
         "shift_type": 1, "slots": 1, "client_id": 1, "is_premium": 1}
    ) or {}

    # ── Upstream available-staff-list, union across all shifts ───────────────
    upstream_xn_ids:       set  = set()
    upstream_distance_map: dict = {}
    try:
        import httpx as _httpx_m
        _upstream_url     = f"{settings.SHIFT_URL.rstrip('/')}/ai/shifts/available-staff-list"
        _upstream_headers = {
            "Api-Key":      settings.SHIFT_INTERNAL_API_KEY,
            "Content-Type": "application/json",
            "Accept":       "application/json",
        }
        _xn_ids = [s.get("shift_id") for s in shift_docs if s.get("shift_id")]

        async with _httpx_m.AsyncClient(timeout=30.0) as _uc:

            async def _one(_xn_id):
                try:
                    _resp = await _uc.post(
                        _upstream_url,
                        json={"shift_id": _xn_id},
                        headers=_upstream_headers,
                    )
                    if _resp.status_code == 200:
                        return _resp.json().get("data") or []
                except Exception as _ie:
                    logger.warning(f"[list-multi] upstream {_xn_id}: {_ie}")
                return []

            for _batch in await asyncio.gather(*[_one(x) for x in _xn_ids]):
                for _s in _batch:
                    _uid = str(_s.get("id", ""))
                    if _uid:
                        upstream_xn_ids.add(_uid)
                        if _s.get("staff_shift_distance") is not None:
                            upstream_distance_map[_uid] = _s.get("staff_shift_distance")
    except Exception as _e:
        logger.warning(f"[list-multi] upstream available-staff-list failed: {_e}")

    # ── Build the Mongo filter as $and clauses ───────────────────────────────
    clauses: list = [{"status": "Enabled"}]

    # shifts.user_type == users.designation (exact, case-insensitive)
    if not payload.user_type_multiple and shift_user_types:
        clauses.append({"$or": [
            {"designation": {"$regex": f"^{re.escape(str(t).strip())}$", "$options": "i"}}
            for t in shift_user_types
        ]})

    if upstream_xn_ids:
        clauses.append({"xn_user_id": {"$in": list(upstream_xn_ids)}})

    # Gender
    if payload.gender_multiple:
        _gids = [str(g).strip() for g in payload.gender_multiple if g]
        if _gids:
            clauses.append({"gender_id": {"$in": _gids}})
    elif payload.gender_id:
        clauses.append({"gender_id": payload.gender_id.strip()})

    # Visa type
    if payload.visa_type_id:
        clauses.append({"visa_type_id": payload.visa_type_id.strip()})

    # QQI status
    if payload.qqi_status_number is not None:
        clauses.append({"qqi_status_number": payload.qqi_status_number})

    # User sub type
    if payload.user_sub_type_multiple:
        sub_oids_m = [ObjectId(str(i)) for i in payload.user_sub_type_multiple
                      if ObjectId.is_valid(str(i))]
        if sub_oids_m:
            clauses.append({"user_sub_type_oids": {"$in": sub_oids_m}})

    # County — stored as string or ObjectId
    if payload.county_multiple:
        county_values = []
        for c in payload.county_multiple:
            c_str = str(c)
            county_values.append(c_str)
            if ObjectId.is_valid(c_str):
                county_values.append(ObjectId(c_str))
        if county_values:
            clauses.append({"county_id": {"$in": county_values}})

    # Explicit user types
    if payload.user_type_multiple:
        valid_type_oids = [ObjectId(str(t)) for t in payload.user_type_multiple
                           if ObjectId.is_valid(str(t))]
        if valid_type_oids:
            type_names = []
            async for ut in db["user_types"].find({"_id": {"$in": valid_type_oids}}, {"name": 1}):
                if ut.get("name"):
                    type_names.append(ut["name"])
            clauses.append({"$or": [
                {"user_type_id": {"$in": valid_type_oids}},
                {"designation":  {"$in": type_names}},
            ]})

    # Search
    if payload.search and payload.search.strip():
        _s    = payload.search.strip()
        _rx   = re.escape(_s)
        parts = _s.split()
        conds = [
            {"first_name": {"$regex": _rx, "$options": "i"}},
            {"last_name":  {"$regex": _rx, "$options": "i"}},
            {"email":      {"$regex": _rx, "$options": "i"}},
            {"phone":      {"$regex": _rx, "$options": "i"}},
        ]
        if len(parts) >= 2:
            conds.append({"$and": [
                {"first_name": {"$regex": re.escape(parts[0]),  "$options": "i"}},
                {"last_name":  {"$regex": re.escape(parts[-1]), "$options": "i"}},
            ]})
        clauses.append({"$or": conds})

    user_filter = clauses[0] if len(clauses) == 1 else {"$and": clauses}

    db_total = await db["users"].count_documents(user_filter)

    # ── Client coords — from the primary shift ───────────────────────────────
    client_data   = await _get_shift_client_coords(db, primary_oid)
    client_coords = (client_data["latitude"], client_data["longitude"]) if client_data else None
    client_location = {
        "latitude":  client_coords[0],
        "longitude": client_coords[1],
    } if client_coords else None

    # ── Group ids — explicit, else derived from the shift_ids ────────────────
    group_oids: list = []
    if payload.group_id and ObjectId.is_valid(str(payload.group_id)):
        group_oids.append(ObjectId(str(payload.group_id)))
    else:
        async for g in db["shifts_group"].find({"shift_ids": {"$in": shift_oids}}, {"_id": 1}):
            group_oids.append(g["_id"])

    # ── Exclusion: per-(user, primary shift) cache, bounded concurrency ──────
    _sem          = asyncio.Semaphore(_EXCLUSION_CONCURRENCY)
    _cache_writes: list = []
    _shift_key    = str(primary_oid)
    _now          = datetime.now(timezone.utc)

    def _cached_tags(u: dict):
        entry = (u.get("exclusion_cache_by_shift") or {}).get(_shift_key)
        if not isinstance(entry, dict):
            return None
        at = entry.get("at")
        if at is None:
            return None
        if getattr(at, "tzinfo", None) is None:
            at = at.replace(tzinfo=timezone.utc)
        if (_now - at).total_seconds() > _EXCLUSION_CACHE_TTL_SECONDS:
            return None
        tags = entry.get("tags")
        return tags if isinstance(tags, list) else None

    async def _exclusion_for(u: dict) -> list:
        cached = _cached_tags(u)
        if cached is not None:
            return cached
        if not (u.get("email") and target_shift):
            return []
        async with _sem:
            tags = await _get_user_exclusion_tags(
                db,
                u.get("email"),
                target_shift,
                u.get("banned_clients") or [],
                u.get("tags") or [],
                u["_id"],
            )
        _cache_writes.append(UpdateOne(
            {"_id": u["_id"]},
            {"$set": {f"exclusion_cache_by_shift.{_shift_key}": {"tags": tags, "at": _now}}},
        ))
        return tags

    async def _annotate(chunk: list) -> None:
        """Exclusion, pool membership, distance and last-contacted for one chunk."""
        if not chunk:
            return
        oids = [u["_id"] for u in chunk]

        # pool — shifts_pool for any of the shifts, plus shifts_group_pool
        pool_ids = {
            str(p["user_id"])
            async for p in db["shifts_pool"].find(
                {"shift_id": {"$in": shift_oids}, "user_id": {"$in": oids}},
                {"user_id": 1}
            )
        }
        if group_oids:
            async for gp in db["shifts_group_pool"].find(
                {"group_id": {"$in": group_oids}, "user_id": {"$in": oids}},
                {"user_id": 1}
            ):
                pool_ids.add(str(gp["user_id"]))

        # last contacted — scoped to these shifts, then group users if newer
        lc_map: dict = {}
        async for su in db["shifts_users"].find(
            {"user_id": {"$in": oids},
             "shift_id": {"$in": shift_oids},
             "call_processed_at": {"$ne": None}},
            {"user_id": 1, "call_processed_at": 1, "channel": 1}
        ).sort("call_processed_at", -1):
            uid = str(su.get("user_id", ""))
            if uid not in lc_map:
                lc_map[uid] = (su.get("call_processed_at"), su.get("channel") or "")

        _gu_filter = {"user_id": {"$in": oids}, "call_processed_at": {"$ne": None}}
        if group_oids:
            _gu_filter["group_id"] = {"$in": group_oids}
        async for gu in db["shifts_group_users"].find(
            _gu_filter, {"user_id": 1, "call_processed_at": 1, "channel": 1}
        ).sort("call_processed_at", -1):
            uid = str(gu.get("user_id", ""))
            dt  = gu.get("call_processed_at")
            cur = lc_map.get(uid)
            if cur is None or (dt and cur[0] and dt > cur[0]):
                lc_map[uid] = (dt, gu.get("channel") or "")

        all_tags = await asyncio.gather(*[_exclusion_for(u) for u in chunk])

        for u, tags in zip(chunk, all_tags):
            uid_str = str(u["_id"])
            u["_excl_tags"] = tags
            u["_excluded"]  = 1 if tags else 0
            u["_in_pool"]   = 1 if uid_str in pool_ids else 0
            u["_lc"]        = lc_map.get(uid_str)

            ucoords = _user_location_coords(u)
            dist = None
            if client_coords and ucoords:
                dist = _haversine_km(client_coords[0], client_coords[1],
                                     ucoords[0], ucoords[1])
            xn = str(u.get("xn_user_id", ""))
            if xn and upstream_distance_map.get(xn) is not None:
                dist = upstream_distance_map[xn]
            u["_coords"]      = ucoords
            u["_distance_km"] = dist

    def _keep(u: dict) -> bool:
        if payload.excluded is not None and u.get("_excluded", 0) != payload.excluded:
            return False
        if payload.in_pool is not None and u.get("_in_pool", 0) != payload.in_pool:
            return False
        if payload.radius is not None and client_coords:
            d = u.get("_distance_km")
            if d is not None and d > payload.radius:
                return False
        return True

    # ── Selection ────────────────────────────────────────────────────────────
    _has_local_filter = (payload.excluded is not None
                         or payload.in_pool is not None
                         or payload.radius is not None)
    _local_sort = order_by in ("distance_km", "rating", "last_contacted")

    db_sort        = [("first_name", -1 if (order_by == "name" and reverse) else 1)]
    scan_truncated = False

    if not _has_local_filter and not _local_sort:
        # DB pagination is exact — annotate exactly this page.
        users = await db["users"].find(user_filter, _LIST_PROJECTION) \
            .sort(db_sort).skip(skip).limit(limit).to_list(length=limit)
        await _annotate(users)
        filtered_total = db_total
        scanned        = len(users)
    else:
        need       = skip + limit
        chunk_size = max(min(limit, 200) * 5, 100)
        scan_cap   = min(_MAX_SCAN, 3000 if _local_sort else max(need * 20, 500))

        matched: list = []
        scanned = offset = 0

        while scanned < scan_cap:
            chunk = await db["users"].find(user_filter, _LIST_PROJECTION) \
                .sort(db_sort).skip(offset).limit(chunk_size) \
                .to_list(length=chunk_size)
            if not chunk:
                break
            offset  += len(chunk)
            scanned += len(chunk)

            await _annotate(chunk)
            matched.extend([u for u in chunk if _keep(u)])

            if not _local_sort and len(matched) >= need:
                break

        scan_truncated = scanned >= scan_cap and scanned < db_total

        if order_by == "distance_km":
            matched.sort(
                key=lambda u: u.get("_distance_km") if u.get("_distance_km") is not None
                else float("inf"),
                reverse=reverse,
            )
        elif order_by == "rating":
            matched.sort(key=lambda u: u.get("rating") or 0, reverse=reverse)
        elif order_by == "last_contacted":
            _epoch = datetime.min.replace(tzinfo=timezone.utc)

            def _lc_key(u):
                lc = u.get("_lc")
                if not lc or not lc[0]:
                    return _epoch
                dt = lc[0]
                if getattr(dt, "tzinfo", None) is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt

            matched.sort(key=_lc_key, reverse=reverse)

        filtered_total = len(matched)
        users          = matched[skip: skip + limit]

    logger.info(
        f"[list-multi] shifts={len(shift_oids)} types={shift_user_types} "
        f"db_total={db_total} scanned={scanned} matched={filtered_total} page={len(users)}"
    )

    # ══════════════════════════════════════════════════════════════════════════
    #  Everything below touches the current page only.
    # ══════════════════════════════════════════════════════════════════════════
    user_oids_page = [u["_id"] for u in users]

    # ── Requested staff, union across all shifts ─────────────────────────────
    requested_user_ids: set = {
        str(rs.get("staff_id", ""))
        for s in shift_docs
        for rs in (s.get("requested_staff_list") or [])
        if rs.get("staff_id")
    }

    # ── Sub type names ───────────────────────────────────────────────────────
    all_sub_oids = [
        ObjectId(str(oid))
        for u in users
        for oid in (u.get("user_sub_type_oids") or [])
        if oid and ObjectId.is_valid(str(oid))
    ]
    sub_type_name_map: dict = {}
    if all_sub_oids:
        async for st in db["user_sub_types"].find({"_id": {"$in": all_sub_oids}}, {"name": 1}):
            sub_type_name_map[str(st["_id"])] = st.get("name", "")

    # ── Visa type names ──────────────────────────────────────────────────────
    visa_type_name_map: dict = {}
    _vt_oids = [ObjectId(str(u["visa_type_id"])) for u in users
                if u.get("visa_type_id") and ObjectId.is_valid(str(u["visa_type_id"]))]
    if _vt_oids:
        async for vt in db["visa_types"].find({"_id": {"$in": _vt_oids}}, {"name": 1}):
            visa_type_name_map[str(vt["_id"])] = vt.get("name", "")

    # ── County / user_type name maps ─────────────────────────────────────────
    county_name_to_id:        dict = {}
    county_oid_to_name:       dict = {}
    designation_to_type_id:   dict = {}
    designation_to_type_name: dict = {}
    type_id_to_name:          dict = {}

    users_needing_county = [u for u in users if not u.get("county_id") and u.get("country_id")]
    users_needing_type   = [u for u in users if not u.get("user_type_id") and u.get("designation")]

    if users_needing_county:
        raw_cids   = list({str(u["country_id"]) for u in users_needing_county})
        valid_oids = [ObjectId(c) for c in raw_cids if ObjectId.is_valid(c)]
        if valid_oids:
            async for co in db["county"].find({"_id": {"$in": valid_oids}}, {"_id": 1, "name": 1}):
                county_name_to_id[str(co["_id"])]  = str(co["_id"])
                county_oid_to_name[str(co["_id"])] = co.get("name", "")

    existing_county_oids = list({
        ObjectId(str(u["county_id"])) for u in users
        if u.get("county_id") and ObjectId.is_valid(str(u["county_id"]))
    })
    if existing_county_oids:
        async for co in db["county"].find({"_id": {"$in": existing_county_oids}}, {"_id": 1, "name": 1}):
            county_oid_to_name[str(co["_id"])] = co.get("name", "")

    if users_needing_type:
        designations = list({u["designation"] for u in users_needing_type if u.get("designation")})
        async for ut in db["user_types"].find({"name": {"$in": designations}}, {"_id": 1, "name": 1}):
            designation_to_type_id[ut["name"]]   = str(ut["_id"])
            designation_to_type_name[ut["name"]] = ut["name"]

    existing_type_oids = list({
        ObjectId(str(u["user_type_id"])) for u in users
        if u.get("user_type_id") and ObjectId.is_valid(str(u["user_type_id"]))
    })
    if existing_type_oids:
        async for ut in db["user_types"].find({"_id": {"$in": existing_type_oids}}, {"_id": 1, "name": 1}):
            type_id_to_name[str(ut["_id"])] = ut.get("name", "")

    # ── Prior shifts — page users only ───────────────────────────────────────
    prior_shifts_map: dict = {}
    if user_oids_page:
        async for ps in db["shifts_users"].aggregate([
            {"$match": {"user_id": {"$in": user_oids_page}, "availability": 1}},
            {"$group": {"_id": "$user_id", "count": {"$sum": 1}}},
        ]):
            prior_shifts_map[str(ps["_id"])] = ps["count"]

    # ── Build response rows ──────────────────────────────────────────────────
    results = []
    _backfill: list = []

    for u in users:
        uid_str = str(u["_id"])
        ucoords = u.get("_coords")

        staff_tags = [
            {"id": str(t.get("id", "")), "name": t.get("name", "")} if isinstance(t, dict)
            else {"id": "", "name": str(t)}
            for t in (u.get("tags") or [])
        ]

        last_contacted = None
        lc_entry = u.get("_lc")
        if lc_entry:
            lc_dt, lc_channel = lc_entry
            last_contacted = _format_time_ago(lc_dt)
            if last_contacted and lc_channel:
                last_contacted = f"{last_contacted} · {lc_channel}"

        # county
        county_id = county_name = None
        if u.get("county_id"):
            county_id   = str(u["county_id"])
            county_name = county_oid_to_name.get(county_id)
        elif u.get("country_id"):
            cid_str = str(u["country_id"])
            if cid_str in county_name_to_id:
                county_id   = county_name_to_id[cid_str]
                county_name = county_oid_to_name.get(county_id)
                _backfill.append(UpdateOne(
                    {"_id": u["_id"]}, {"$set": {"county_id": ObjectId(county_id)}}
                ))

        # user type
        user_type_id = user_type_name = None
        if u.get("user_type_id"):
            user_type_id   = str(u["user_type_id"])
            user_type_name = type_id_to_name.get(user_type_id)
        elif u.get("designation") and u["designation"] in designation_to_type_id:
            user_type_id   = designation_to_type_id[u["designation"]]
            user_type_name = designation_to_type_name.get(u["designation"])
            _backfill.append(UpdateOne(
                {"_id": u["_id"]}, {"$set": {"user_type_id": ObjectId(user_type_id)}}
            ))

        # visa hours
        visa_used  = u.get("visa_hours_used")
        visa_total = u.get("visa_hours_total")
        consumed   = u.get("consumed_hours")
        visa_hours_remaining = consumed if consumed is not None else (
            f"{visa_used}/{visa_total}" if visa_used is not None and visa_total else None
        )

        # work history
        prior_shifts = prior_shifts_map.get(uid_str, 0)
        _plural      = "s" if prior_shifts != 1 else ""
        if prior_shifts > 0 and last_contacted:
            work_history = f"{prior_shifts} Shift{_plural} · {last_contacted}"
        elif prior_shifts > 0:
            work_history = f"{prior_shifts} Shift{_plural}"
        elif last_contacted:
            work_history = last_contacted
        else:
            work_history = None

        sub_oids = u.get("user_sub_type_oids") or []
        results.append({
            "id":                    uid_str,
            "xn_user_id":            u.get("xn_user_id"),
            "name":                  " ".join(filter(None, [u.get("first_name", ""),
                                                            u.get("last_name", "")])).strip() or "—",
            "email":                 u.get("email"),
            "phone":                 u.get("phone"),
            "designation":           u.get("designation"),
            "rating":                u.get("rating"),
            "channel":               "Phone",
            "staff_tags":            staff_tags,
            "last_contacted":        last_contacted,
            "visa_hours_remaining":  visa_hours_remaining,
            "work_permit_exemption": u.get("work_permit_exemption"),
            "consumed_hours":        u.get("consumed_hours"),
            "qqi_status_number":     u.get("qqi_status_number"),
            "gender_id":             str(u["gender_id"]) if u.get("gender_id") else None,
            "user_sub_type_ids":     u.get("user_sub_type_ids") or [],
            "user_sub_type_oids":    [str(oid) for oid in sub_oids],
            "user_sub_types":        ([{"id": str(oid), "name": sub_type_name_map.get(str(oid), "")}
                                       for oid in sub_oids if ObjectId.is_valid(str(oid))]
                                      or [{"id": None, "name": n}
                                          for n in (u.get("user_sub_type_ids") or []) if n]),
            "visa_type_id":          u.get("visa_type_id"),
            "visa_type_name":        visa_type_name_map.get(str(u.get("visa_type_id", "")))
                                     if u.get("visa_type_id") else None,
            "prior_shifts":          prior_shifts,
            "work_history":          work_history,
            "status":                u.get("status"),
            "county_id":             county_id,
            "county":                county_name,
            "user_type_id":          user_type_id,
            "user_type":             user_type_name,
            "user_latitude":         ucoords[0] if ucoords else None,
            "user_longitude":        ucoords[1] if ucoords else None,
            "distance_km":           u.get("_distance_km"),
            "excluded":              u.get("_excluded", 0),
            "exclusion_tags":        u.get("_excl_tags") or [],
            "requested":             1 if uid_str in requested_user_ids else 0,
            "in_pool":               u.get("_in_pool", 0),
        })

    if order_by == "name":
        results.sort(key=lambda r: r["name"].lower(), reverse=reverse)

    # ── by_designation — shift user_types + upstream only, ignores filters ───
    desig_clauses: list = [{"status": "Enabled"}]
    if shift_user_types:
        desig_clauses.append({"designation": {"$in": shift_user_types}})
    if upstream_xn_ids:
        desig_clauses.append({"xn_user_id": {"$in": list(upstream_xn_ids)}})
    desig_filter = desig_clauses[0] if len(desig_clauses) == 1 else {"$and": desig_clauses}

    designation_list  = []
    designation_total = 0
    async for row in db["users"].aggregate([
        {"$match": desig_filter},
        {"$group": {
            "_id":          {"$ifNull": ["$designation", "Unknown"]},
            "user_type_id": {"$first": "$user_type_id"},
            "count":        {"$sum": 1},
        }},
        {"$sort": {"count": -1}},
    ]):
        designation_list.append({
            "designation":  row["_id"],
            "user_type_id": str(row["user_type_id"]) if row.get("user_type_id") else None,
            "count":        row["count"],
        })
        designation_total += row["count"]

    # ── Flush writes ─────────────────────────────────────────────────────────
    if _cache_writes:
        try:
            await db["users"].bulk_write(_cache_writes, ordered=False)
        except Exception as e:
            logger.warning(f"[list-multi] exclusion cache write failed: {e}")

    if _backfill:
        try:
            await db["users"].bulk_write(_backfill, ordered=False)
        except Exception as e:
            logger.warning(f"[list-multi] county/user_type backfill failed: {e}")

    return {
        "success":             True,
        "total":               designation_total,
        "db_total":            db_total,
        "filtered_total":      filtered_total,
        "scanned":             scanned,
        "scan_truncated":      scan_truncated,
        "exclusions_computed": len(_cache_writes),
        "page":                payload.page,
        "per_page":            payload.per_page,
        "shift_ids":           [str(o) for o in shift_oids],
        "primary_shift_id":    str(primary_oid),
        "shift_user_types":    shift_user_types,
        "group_ids":           [str(g) for g in group_oids],
        "client_location":     client_location,
        "radius":              payload.radius,
        "order_by":            order_by,
        "sort":                payload.sort or "asc",
        "by_designation":      designation_list,
        "data":                results,
    }


# ── POST /shift-users/confirm ─────────────────────────────────────────────────

class ConfirmStaffRequest(BaseModel):
    shift_id: str   # shifts._id
    staff_id: str   # users._id


@router.post(
    "/confirm",
    summary="Save staff confirmation to requested_confirm collection",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def confirm_staff(request: Request, payload: ConfirmStaffRequest):
    """
    Body: { "shift_id": "<shift._id>", "staff_id": "<user._id>" }
    Saves to requested_confirm collection and updates shifts.assigned_staff.
    """
    db = _get_db()

    shift_oid = _resolve_oid(payload.shift_id, "shift_id")
    user_oid  = _resolve_oid(payload.staff_id,  "staff_id")

    shift = await db["shifts"].find_one({"_id": shift_oid},
        {"_id": 1, "shift_code": 1, "name": 1, "date": 1,
         "start_time": 1, "end_time": 1, "client_id": 1, "client_name": 1, "user_type": 1})
    if not shift:
        raise HTTPException(status_code=404, detail="Shift not found")

    user = await db["users"].find_one({"_id": user_oid},
        {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
         "xn_user_id": 1, "designation": 1, "rating": 1})
    if not user:
        raise HTTPException(status_code=404, detail="Staff not found")

    now       = datetime.now(timezone.utc)
    full_name = " ".join(filter(None, [user.get("first_name",""), user.get("last_name","")])).strip() or "—"
    email     = user.get("email") or ""

    doc = {
        "shift_id":      shift_oid,
        "staff_id":      user_oid,
        "xn_user_id":    user.get("xn_user_id"),
        "staff_name":    full_name,
        "staff_email":   email,
        "shift_code":    shift.get("shift_code") or shift.get("name") or "",
        "client_id":     shift.get("client_id"),
        "client_name":   shift.get("client_name"),
        "user_type":     shift.get("user_type"),
        "confirmed_by":  None,
        "confirmed_at":  now,
        "updated_at":    now,
    }

    # Upsert — update if same shift_id + staff_id already exists
    existing = await db["requested_confirm"].find_one(
        {"shift_id": shift_oid, "staff_id": user_oid}, {"_id": 1}
    )
    if existing:
        await db["requested_confirm"].update_one(
            {"_id": existing["_id"]},
            {"$set": {**doc}}
        )
        record_id = str(existing["_id"])
        action    = "updated"
    else:
        doc["created_at"] = now
        result    = await db["requested_confirm"].insert_one(doc)
        record_id = str(result.inserted_id)
        action    = "created"


    return {
        "success":      True,
        "action":       action,
        "message":      f"Confirmation call sent to {full_name}",
        "id":           record_id,
        "shift_id":     payload.shift_id,
        "staff_id":     payload.staff_id,
        "staff_name":   full_name,
        "staff_email":  email,
        "confirmed_by": None,
        "confirmed_at": now.isoformat(),
    }


# ── POST /shift-users/booking-confirmed-call ──────────────────────────────────

@router.post(
    "/booking-confirmed-call",
    summary="Save booking confirmed call record to booking_confirmed_call collection",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def booking_confirmed_call(request: Request, payload: ConfirmStaffRequest):
    """
    Body: { "shift_id": "<shift._id>", "staff_id": "<user._id>" }
    Upserts to booking_confirmed_call collection.
    """
    db = _get_db()

    shift_oid = _resolve_oid(payload.shift_id, "shift_id")
    user_oid  = _resolve_oid(payload.staff_id,  "staff_id")

    shift = await db["shifts"].find_one({"_id": shift_oid},
        {"_id": 1, "shift_code": 1, "name": 1, "date": 1,
         "start_time": 1, "end_time": 1, "client_id": 1, "client_name": 1,
         "user_type": 1, "shift_id": 1})
    if not shift:
        raise HTTPException(status_code=404, detail="Shift not found")

    user = await db["users"].find_one({"_id": user_oid},
        {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
         "xn_user_id": 1, "designation": 1, "rating": 1})
    if not user:
        raise HTTPException(status_code=404, detail="Staff not found")

    now       = datetime.now(timezone.utc)
    full_name = " ".join(filter(None, [user.get("first_name",""), user.get("last_name","")])).strip() or "—"
    email     = user.get("email") or ""

    doc = {
        "shift_id":      shift_oid,
        "staff_id":      user_oid,
        "xn_user_id":    user.get("xn_user_id"),
        "xn_shift_id":   shift.get("shift_id"),
        "staff_name":    full_name,
        "staff_email":   email,
        "staff_phone":   user.get("phone"),
        "shift_code":    shift.get("shift_code") or shift.get("name") or "",
        "client_id":     shift.get("client_id"),
        "client_name":   shift.get("client_name"),
        "user_type":     shift.get("user_type"),
        "call_status":   "pending",
        "confirmed_by":  "System",
        "confirmed_at":  now,
        "updated_at":    now,
    }

    # Upsert — update if same shift_id + staff_id already exists
    existing = await db["booking_confirmed_call"].find_one(
        {"shift_id": shift_oid, "staff_id": user_oid}, {"_id": 1}
    )
    if existing:
        await db["booking_confirmed_call"].update_one(
            {"_id": existing["_id"]},
            {"$set": {**doc}}
        )
        record_id = str(existing["_id"])
        action    = "updated"
    else:
        doc["created_at"] = now
        result    = await db["booking_confirmed_call"].insert_one(doc)
        record_id = str(result.inserted_id)
        action    = "created"

    return {
        "success":      True,
        "action":       action,
        "message":      f"Booking confirmed call saved for {full_name}",
        "id":           record_id,
        "shift_id":     payload.shift_id,
        "staff_id":     payload.staff_id,
        "staff_name":   full_name,
        "staff_email":  email,
        "confirmed_at": now.isoformat(),
    }


# ── Ignore ────────────────────────────────────────────────────────────────────

IGNORE_REASONS = [
    {"id": "actually_declined",        "title": "Actually Declined",             "description": "They said no, even if it sounded ambiguous"},
    {"id": "unclear_follow_up",        "title": "Unclear · needs follow-up",     "description": "Response was ambiguous; ops should call back"},
    {"id": "available_with_conditions","title": "Available but with conditions", "description": 'e.g. "yes if I can leave early"'},
    {"id": "not_suitable",             "title": "Not Suitable",                  "description": "Staff does not meet shift requirements"},
    {"id": "already_placed",           "title": "Already Placed",                "description": "Staff confirmed for another shift"},
]


@router.get(
    "/reasons/ignore",
    summary="Get list of ignore reasons",
    dependencies=[Depends(verify_api_key)],
)
async def get_ignore_reasons(request: Request):
    return {"success": True, "data": IGNORE_REASONS}


class IgnoreStaffRequest(BaseModel):
    shift_id: str   # shifts._id
    staff_id: str   # users._id  (matches requested_staff_list.staff_id)
    reason:   Optional[str] = None   # id from ignore-reasons
    notes:    Optional[str] = None


@router.post(
    "/ignore",
    summary="Ignore a requested staff member for a shift",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def ignore_staff(request: Request, payload: IgnoreStaffRequest):
    """
    Body: { "shift_id": "...", "staff_id": "...", "reason"?: "...", "notes"?: "..." }
    Sets ignored=1 on matching entry in shifts.requested_staff_list.
    Also saves to shift_ignored collection for audit.
    """
    db = _get_db()

    shift_oid = _resolve_oid(payload.shift_id, "shift_id")
    user_oid  = _resolve_oid(payload.staff_id,  "staff_id")

    shift = await db["shifts"].find_one({"_id": shift_oid}, {"_id": 1, "requested_staff_list": 1})
    if not shift:
        raise HTTPException(status_code=404, detail="Shift not found")

    # Resolve reason label
    reason_label = next(
        (r["title"] for r in IGNORE_REASONS if r["id"] == payload.reason), payload.reason
    ) if payload.reason else None

    now = datetime.now(timezone.utc)

    # Update the matching entry in requested_staff_list array
    result = await db["shifts"].update_one(
        {
            "_id": shift_oid,
            "requested_staff_list.staff_id": payload.staff_id,
        },
        {"$set": {
            "requested_staff_list.$.ignored":       1,
            "requested_staff_list.$.ignore_reason": payload.reason,
            "requested_staff_list.$.ignore_reason_text": reason_label,
            "requested_staff_list.$.ignore_notes":  payload.notes,
            "requested_staff_list.$.ignored_at":    now.isoformat(),
            "updated_at": now,
        }}
    )

    # Also try matching by xn_staff_id
    if result.modified_count == 0:
        result = await db["shifts"].update_one(
            {
                "_id": shift_oid,
                "requested_staff_list.xn_staff_id": payload.staff_id,
            },
            {"$set": {
                "requested_staff_list.$.ignored":            1,
                "requested_staff_list.$.ignore_reason":      payload.reason,
                "requested_staff_list.$.ignore_reason_text": reason_label,
                "requested_staff_list.$.ignore_notes":       payload.notes,
                "requested_staff_list.$.ignored_at":         now.isoformat(),
                "updated_at": now,
            }}
        )

    # Save to audit collection
    await db["shift_ignored"].insert_one({
        "shift_id":     shift_oid,
        "staff_id":     user_oid,
        "reason":       payload.reason,
        "reason_text":  reason_label,
        "notes":        payload.notes,
        "ignored_at":   now,
        "created_at":   now,
    })

    # Also update shifts_users so available_staff shows ignored=1 (all outreach rounds)
    await db["shifts_users"].update_many(
        {"shift_id": shift_oid, "user_id": user_oid},
        {"$set": {
            "ignored":       1,
            "ignore_reason": payload.reason,
            "ignore_notes":  payload.notes,
            "ignored_at":    now.isoformat(),
            "updated_at":    now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        }}
    )

    # Also update shifts_pool record if exists
    await db["shifts_pool"].update_many(
        {"shift_id": shift_oid, "user_id": user_oid},
        {"$set": {"ignored": 1, "updated_at": now}}
    )

    return {
        "success":      True,
        "message":      "Staff ignored",
        "shift_id":     payload.shift_id,
        "staff_id":     payload.staff_id,
        "ignored":      1,
        "reason":       payload.reason,
        "reason_text":  reason_label,
        "notes":        payload.notes,
        "ignored_at":   now.isoformat(),
        "modified":     result.modified_count > 0,
    }


# ── POST /shift-users/ghost-booking ──────────────────────────────────────────

@router.post(
    "/ghost-booking",
    summary="Mark a shift as ghost booking and assign staff",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def ghost_booking(request: Request, payload: AssignStaffRequest):
    """
    Body: { "shift_id": "<shift._id>", "user_id": "<user._id>" }
    Calls upstream /ai/shifts/ghost-booking first, then updates collection.
    """
    db        = _get_db()
    shift_oid = _resolve_oid(payload.shift_id, "shift_id")
    user_oid  = _resolve_oid(payload.user_id,  "user_id")

    shift = await db["shifts"].find_one({"_id": shift_oid}, {"_id": 1, "shift_id": 1})
    if not shift:
        raise HTTPException(status_code=404, detail="Shift not found")

    user = await db["users"].find_one(
        {"_id": user_oid},
        {"first_name": 1, "last_name": 1, "email": 1, "designation": 1, "rating": 1, "xn_user_id": 1}
    )
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    xn_shift_id = shift.get("shift_id")
    xn_user_id  = user.get("xn_user_id")

    # ── Call upstream ghost-booking API ──────────────────────────────────────
    if not xn_shift_id or not xn_user_id:
        raise HTTPException(
            status_code=422,
            detail=f"Missing upstream IDs — shift_id={xn_shift_id} staff_id={xn_user_id}"
        )

    import httpx as _httpx
    upstream_url = f"{settings.SHIFT_URL.rstrip('/')}/ai/shifts/ghost-booking"
    upstream_headers = {
        "Api-Key":      settings.SHIFT_INTERNAL_API_KEY,
        "Content-Type": "application/json",
        "Accept":       "application/json",
    }
    logger.info(f"[ghost-booking] upstream={upstream_url} shift_id={xn_shift_id} staff_id={xn_user_id}")

    try:
        async with _httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                upstream_url,
                json={"shift_id": xn_shift_id, "staff_id": xn_user_id},
                headers=upstream_headers
            )
        try:
            upstream_body = resp.json()
        except Exception:
            upstream_body = {}
        logger.info(f"[ghost-booking] upstream status={resp.status_code} body={upstream_body}")

        if resp.status_code != 200 or not upstream_body.get("success"):
            msg = upstream_body.get("message") or f"Upstream failed (status {resp.status_code})"
            return {
                "success": False,
                "message": msg,
                "upstream_status": resp.status_code,
                "upstream_data": upstream_body.get("data"),
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Upstream error: {str(e)}")

    full_name = " ".join(filter(None, [user.get("first_name",""), user.get("last_name","")])).strip() or "—"
    now       = datetime.now(timezone.utc)

    await db["shifts"].update_one(
        {"_id": shift_oid},
        {"$set": {
            "ghost_booking": True,
            "staff": {
                "id":   payload.user_id,
                "name": full_name,
            },
            "staff_email":    user.get("email", ""),
            "assigned_staff": full_name,
            "staff_id":       payload.user_id,
            "assigned_at":    now,
            "updated_at":     now,
        }}
    )

    # The user's schedule changed — drop every cached exclusion verdict
    await db["users"].update_one(
        {"_id": user_oid},
        {"$unset": {"exclusion_cache_by_shift": "",
                    "exclusion_cache": "", "exclusion_cache_at": ""}}
    )

    return {
        "success":       True,
        "message":       f"Ghost booking set for {full_name}",
        "shift_id":      payload.shift_id,
        "user_id":       payload.user_id,
        "ghost_booking": True,
        "staff": {
            "id":   payload.user_id,
            "name": full_name,
        },
        "assigned_at": now.isoformat(),
    }


# ── POST /shift-users/decline ─────────────────────────────────────────────────

DECLINE_REASONS = [
    {"id": "not_available",  "title": "Not Available",  "description": "Staff confirmed they cannot take the shift"},
    {"id": "no_response",    "title": "No Response",    "description": "Staff did not respond after multiple attempts"},
    {"id": "already_booked", "title": "Already Booked", "description": "Staff is already placed on another shift"},
    {"id": "not_suitable",   "title": "Not Suitable",   "description": "Staff does not meet the shift requirements"},
    {"id": "withdrew",       "title": "Withdrew",       "description": "Staff initially accepted but later withdrew"},
]


@router.get(
    "/reasons/decline",
    summary="Get list of decline reasons",
    dependencies=[Depends(verify_api_key)],
)
async def get_decline_reasons(request: Request):
    return {"success": True, "data": DECLINE_REASONS}


class DeclineStaffRequest(BaseModel):
    shift_id: str
    staff_id: str
    reason:   Optional[str] = None
    notes:    Optional[str] = None


@router.post(
    "/decline",
    summary="Decline a requested staff member for a shift",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def decline_staff(request: Request, payload: DeclineStaffRequest):
    db        = _get_db()
    shift_oid = _resolve_oid(payload.shift_id, "shift_id")
    user_oid  = _resolve_oid(payload.staff_id,  "staff_id")

    shift = await db["shifts"].find_one({"_id": shift_oid}, {"_id": 1})
    if not shift:
        raise HTTPException(status_code=404, detail="Shift not found")

    reason_label = next(
        (r["title"] for r in DECLINE_REASONS if r["id"] == payload.reason), payload.reason
    ) if payload.reason else None

    now = datetime.now(timezone.utc)
    update_set = {
        "requested_staff_list.$.declined":            1,
        "requested_staff_list.$.decline_reason":      payload.reason,
        "requested_staff_list.$.decline_reason_text": reason_label,
        "requested_staff_list.$.decline_notes":       payload.notes,
        "requested_staff_list.$.declined_at":         now.isoformat(),
        "updated_at": now,
    }

    result = await db["shifts"].update_one(
        {"_id": shift_oid, "requested_staff_list.staff_id": payload.staff_id},
        {"$set": update_set}
    )
    if result.modified_count == 0:
        result = await db["shifts"].update_one(
            {"_id": shift_oid, "requested_staff_list.xn_staff_id": payload.staff_id},
            {"$set": update_set}
        )

    await db["shift_declined"].insert_one({
        "shift_id":    shift_oid,
        "staff_id":    user_oid,
        "reason":      payload.reason,
        "reason_text": reason_label,
        "notes":       payload.notes,
        "declined_at": now,
        "created_at":  now,
    })

    return {
        "success":     True,
        "message":     "Staff declined",
        "shift_id":    payload.shift_id,
        "staff_id":    payload.staff_id,
        "declined":    1,
        "reason":      payload.reason,
        "reason_text": reason_label,
        "notes":       payload.notes,
        "declined_at": now.isoformat(),
        "modified":    result.modified_count > 0,
    }