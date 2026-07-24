import logging
import math
from datetime import datetime, timezone
from typing import List, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/shift-users", tags=["Shift Users"])


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
    user_id:  str   # MongoDB ObjectId of the user
    shift_id: str   # MongoDB ObjectId of the shift


class RemoveUserFromShiftRequest(BaseModel):
    id:       str   # user_id (users._id)
    shift_id: str   # shifts._id


class AddUsersToShiftRequest(BaseModel):
    shift_id:  str        # MongoDB ObjectId of the shift
    user_ids:  List[str]  # List of user MongoDB ObjectIds


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

    doc = {
        "user_id":  user_oid,
        "shift_id": shift_oid,
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

    inserted = skipped_dup = skipped_missing = 0
    inserted_ids = []

    for user_oid in user_oids:
        uid_str = str(user_oid)
        if uid_str not in existing_users:
            skipped_missing += 1
            continue
        if uid_str in already_added:
            skipped_dup += 1
            continue

        doc = {
            "user_id":    user_oid,
            "shift_id":   shift_oid,
            "added_at":   now,
            "added_by":   "bulk",
            "updated_at": now,
        }
        result = await db["shifts_pool"].insert_one(doc)
        inserted_ids.append(str(result.inserted_id))
        inserted += 1

    logger.info(f"shifts_pool bulk: shift={payload.shift_id} inserted={inserted} dup={skipped_dup} missing={skipped_missing}")

    return {
        "success": True,
        "message": f"{inserted} user(s) added to shift pool",
        "data": {
            "shift_id":             payload.shift_id,
            "inserted":             inserted,
            "skipped_duplicate":    skipped_dup,
            "skipped_missing_user": skipped_missing,
            "inserted_ids":         inserted_ids,
        },
    }


# ── LIST users for a shift ────────────────────────────────────────────────────

@router.get(
    "/{shift_id}",
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
    Body: { "id": "<shift_users._id>" }
    Removes the shift_users record by its own _id.
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

    return {
        "success":              True,
        "message":              "User removed from pool and shifts_users",
        "user_id":              payload.id,
        "shift_id":             payload.shift_id,
        "pool_removed":         pool_result.deleted_count > 0,
        "shifts_users_removed": su_result.deleted_count,
    }



def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return round(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)), 2)


async def _get_shift_client_coords(db, shift_oid: ObjectId):
    """shifts._id → shifts.client_id → clients.xn_client_id → lat/lng"""
    shift = await db["shifts"].find_one({"_id": shift_oid}, {"client_id": 1})
    if not shift or not shift.get("client_id"):
        return None
    client = await db["clients"].find_one(
        {"xn_client_id": shift["client_id"]},
        {"latitude": 1, "longitude": 1}
    )
    if not client:
        return None
    lat = client.get("latitude")
    lng = client.get("longitude")
    if lat is None or lng is None:
        return None
    return (float(lat), float(lng))


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


async def _get_user_exclusion_tags(db, user_email: str, target_shift: dict) -> list:
    """
    Returns list of exclusion tag strings for a user against a target shift.
    Checks:
      1. Same-day overlapping shift
      2. No overlapping shifts (time)
      3. Consecutive day/night shift conflict
      4. Duplicate shift type same day
      5. Exceeds 16 consecutive hours
      6. Minimum 6h gap violation (under_6)
    """
    if not user_email:
        return []

    target_date   = target_shift.get("date")
    target_start  = target_shift.get("start_time", "")
    target_end    = target_shift.get("end_time", "")
    target_type   = _shift_type(target_shift.get("shift_timing") or target_shift.get("shift_type") or "")

    # Find all shifts where staff_email matches
    existing_shifts = await db["shifts"].find(
        {"staff_email": user_email},
        {"date": 1, "start_time": 1, "end_time": 1, "shift_timing": 1,
         "shift_type": 1, "slots": 1}
    ).to_list(length=500)

    tags = []

    for es in existing_shifts:
        es_date   = es.get("date")
        es_slots  = es.get("slots") or []

        # Use slots if present, else top-level fields
        time_ranges = []
        if es_slots:
            for sl in es_slots:
                sl_date = sl.get("date")
                time_ranges.append({
                    "date":   sl_date,
                    "start":  sl.get("start_time", ""),
                    "end":    sl.get("end_time", ""),
                    "type":   _shift_type(sl.get("shift_type") or ""),
                })
        else:
            time_ranges.append({
                "date":  es_date,
                "start": es.get("start_time", ""),
                "end":   es.get("end_time", ""),
                "type":  _shift_type(es.get("shift_timing") or es.get("shift_type") or ""),
            })

        for tr in time_ranges:
            tr_date  = tr["date"]
            tr_start = tr["start"]
            tr_end   = tr["end"]
            tr_type  = tr["type"]

            # Normalize dates for comparison
            same_day = False
            if tr_date and target_date:
                try:
                    td = tr_date.date() if hasattr(tr_date, "date") else None
                    tgt = target_date.date() if hasattr(target_date, "date") else None
                    same_day = td and tgt and td == tgt
                except Exception:
                    pass

            if same_day:
                # Rule 1 & 2: Time overlap
                if _times_overlap(target_start, target_end, tr_start, tr_end):
                    if "overlap" not in tags:
                        tags.append("overlap")

                # Rule 4: Duplicate shift type same day
                if target_type and tr_type and target_type == tr_type:
                    tag = f"duplicate_{target_type}"
                    if tag not in tags:
                        tags.append(tag)

                # Rule 3: Consecutive day/night on same day
                if target_type and tr_type and target_type != tr_type:
                    if "consecutive_day_night" not in tags:
                        tags.append("consecutive_day_night")

                # Rule 5: Exceeds 16 consecutive hours
                if tr_start and tr_end and target_start and target_end:
                    t1s = _parse_time(target_start)
                    t1e = _parse_time(target_end)
                    t2s = _parse_time(tr_start)
                    t2e = _parse_time(tr_end)
                    if None not in (t1s, t1e, t2s, t2e):
                        combined = abs(max(t1e, t2e) - min(t1s, t2s))
                        if combined > 16 * 60:
                            if "exceeds_16h" not in tags:
                                tags.append("exceeds_16h")

            # Rule 6: Minimum 6h gap (same or adjacent day)
            if tr_end and target_start:
                gap = _gap_minutes(tr_end, target_start)
                if 0 < gap < 360:
                    if "under_6h_gap" not in tags:
                        tags.append("under_6h_gap")
            if target_end and tr_start:
                gap = _gap_minutes(target_end, tr_start)
                if 0 < gap < 360:
                    if "under_6h_gap" not in tags:
                        tags.append("under_6h_gap")

    return tags


# ── LIST shift_users with pagination (POST body) ──────────────────────────────

class ListShiftUsersRequest(BaseModel):
    shift_id:           str
    page:               int = 1
    per_page:           int = 20
    radius:             Optional[float] = None
    order_by:           Optional[str]   = None
    sort:               Optional[str]   = "asc"
    county_multiple:    Optional[list]  = None  # list of county _id strings
    user_type_multiple: Optional[list]  = None  # list of user_type _id strings
    excluded:           Optional[int]   = None  # 0 = not excluded only, 1 = excluded only, None = all


@router.post(
    "/list",
    summary="List shift_users records for a shift with pagination",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("120/minute")
async def list_shift_users_paginated(request: Request, payload: ListShiftUsersRequest):
    """
    Body: { "shift_id": "<shift_id>", "page": 1, "per_page": 20 }
    Returns Enabled users from users table — no join with shifts_users.
    Distance calculated from shift client coords vs user location.
    """
    db = _get_db()
    shift_oid = _resolve_oid(payload.shift_id, "shift_id")
    skip  = (payload.page - 1) * payload.per_page
    limit = payload.per_page

    # Fetch shift early — needed for user_type filter and exclusion check
    target_shift = await db["shifts"].find_one(
        {"_id": shift_oid},
        {"date": 1, "start_time": 1, "end_time": 1, "shift_timing": 1,
         "shift_type": 1, "slots": 1, "user_type": 1, "client_id": 1}
    ) or {}

    # Query only Enabled users — no shifts_users join
    user_filter: dict = {"status": "Enabled"}

    # Auto-filter by shift's user_type if no explicit user_type_multiple provided
    if not payload.user_type_multiple:
        shift_user_type = target_shift.get("user_type") if target_shift else None
        if shift_user_type:
            # Match by designation name or user_type_id name
            ut_doc = await db["user_types"].find_one({"name": shift_user_type}, {"_id": 1})
            if ut_doc:
                user_filter["$or"] = [
                    {"user_type_id": ut_doc["_id"]},
                    {"designation":  shift_user_type},
                ]
            else:
                user_filter["designation"] = shift_user_type

    # county_multiple filter — match both string and ObjectId stored county_id
    if payload.county_multiple:
        county_values = []
        for c in payload.county_multiple:
            c_str = str(c)
            county_values.append(c_str)           # string stored value
            if ObjectId.is_valid(c_str):
                county_values.append(ObjectId(c_str))  # ObjectId stored value
        if county_values:
            user_filter["county_id"] = {"$in": county_values}

    # user_type_multiple filter — resolve IDs to ObjectIds for users.user_type_id
    # Also match via designation name in case user_type_id not yet set
    if payload.user_type_multiple:
        valid_type_oids = [ObjectId(t) for t in payload.user_type_multiple if ObjectId.is_valid(str(t))]
        if valid_type_oids:
            # Look up names for fallback designation match
            type_names_filter = []
            async for ut in db["user_types"].find({"_id": {"$in": valid_type_oids}}, {"name": 1}):
                type_names_filter.append(ut["name"])
            user_filter["$or"] = [
                {"user_type_id": {"$in": valid_type_oids}},
                {"designation":  {"$in": type_names_filter}},
            ]

    total = await db["users"].count_documents(user_filter)
    users = await db["users"].find(
        user_filter,
        {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
         "xn_user_id": 1, "designation": 1, "rating": 1,
         "location": 1, "latitude": 1, "longitude": 1, "status": 1,
         "tags": 1, "county_id": 1, "user_type_id": 1, "country_id": 1,
         "visa_hours_used": 1, "visa_hours_total": 1}
    ).sort("first_name", 1).skip(skip).limit(limit).to_list(length=limit)

    # Fetch latest shifts_users.call_processed_at per user for last_contacted
    user_ids_page = [u["_id"] for u in users]
    last_contacted_map: dict = {}
    if user_ids_page:
        async for su in db["shifts_users"].find(
            {"user_id": {"$in": user_ids_page}, "call_processed_at": {"$ne": None}},
            {"user_id": 1, "call_processed_at": 1}
        ).sort("call_processed_at", -1):
            uid = str(su.get("user_id", ""))
            if uid not in last_contacted_map:
                last_contacted_map[uid] = su.get("call_processed_at")

    # Build batch lookup maps for county_id and user_type_id
    # Collect users missing county_id or user_type_id for batch resolution
    county_name_to_id: dict = {}
    county_oid_to_name: dict = {}
    designation_to_type_id: dict = {}
    designation_to_type_name: dict = {}
    users_needing_county   = [u for u in users if not u.get("county_id") and u.get("country_id")]
    users_needing_type     = [u for u in users if not u.get("user_type_id") and u.get("designation")]

    # Batch resolve county: users.country_id == county._id
    if users_needing_county:
        raw_cids = list({str(u["country_id"]) for u in users_needing_county if u.get("country_id")})
        valid_oids = [ObjectId(c) for c in raw_cids if ObjectId.is_valid(c)]
        if valid_oids:
            async for co in db["county"].find({"_id": {"$in": valid_oids}}, {"_id": 1, "name": 1}):
                county_name_to_id[str(co["_id"])]   = str(co["_id"])
                county_oid_to_name[str(co["_id"])]  = co.get("name", "")

    # Also batch resolve county names for users that already have county_id
    existing_county_oids = list({
        ObjectId(str(u["county_id"])) for u in users
        if u.get("county_id") and ObjectId.is_valid(str(u["county_id"]))
    })
    if existing_county_oids:
        async for co in db["county"].find({"_id": {"$in": existing_county_oids}}, {"_id": 1, "name": 1}):
            county_oid_to_name[str(co["_id"])] = co.get("name", "")

    # Batch resolve user_type: users.designation == user_types.name
    if users_needing_type:
        designations = list({u["designation"] for u in users_needing_type if u.get("designation")})
        async for ut in db["user_types"].find(
            {"name": {"$in": designations}},
            {"_id": 1, "name": 1}
        ):
            designation_to_type_id[ut["name"]]   = str(ut["_id"])
            designation_to_type_name[ut["name"]] = ut["name"]

    # Also batch resolve user_type names for users that already have user_type_id
    existing_type_oids = list({
        ObjectId(str(u["user_type_id"])) for u in users
        if u.get("user_type_id") and ObjectId.is_valid(str(u["user_type_id"]))
    })
    type_id_to_name: dict = {}
    if existing_type_oids:
        async for ut in db["user_types"].find({"_id": {"$in": existing_type_oids}}, {"_id": 1, "name": 1}):
            type_id_to_name[str(ut["_id"])] = ut["name"]

    # Get shift client coords for distance calculation
    client_coords = await _get_shift_client_coords(db, shift_oid)
    shift_client_info = None
    if client_coords:
        shift_client_info = {
            "client_latitude":  client_coords[0],
            "client_longitude": client_coords[1],
        }

    # Full client location object
    client_location = None
    if client_coords:
        client_location = {
            "latitude":  client_coords[0],
            "longitude": client_coords[1],
        }

    # ── Batch: pool membership ─────────────────────────────────────────────────
    user_oids_page = [u["_id"] for u in users]
    pool_records   = await db["shifts_pool"].find(
        {"shift_id": shift_oid, "user_id": {"$in": user_oids_page}},
        {"user_id": 1}
    ).to_list(5000)
    pool_user_set = {str(p["user_id"]) for p in pool_records}

    # ── Batch: prior shifts count ──────────────────────────────────────────────
    prior_su_docs = await db["shifts_users"].find(
        {"user_id": {"$in": user_oids_page}, "availability": 1},
        {"user_id": 1}
    ).to_list(50000)
    prior_shifts_map: dict = {}
    for psu in prior_su_docs:
        uid = str(psu.get("user_id", ""))
        prior_shifts_map[uid] = prior_shifts_map.get(uid, 0) + 1

    results = []
    for u in users:
        uid_str  = str(u["_id"])
        ucoords  = _user_location_coords(u)

        distance_km = None
        if client_coords and ucoords:
            distance_km = _haversine_km(
                client_coords[0], client_coords[1],
                ucoords[0],       ucoords[1],
            )

        # staff_tags from user.tags array
        raw_tags = u.get("tags") or []
        staff_tags = []
        for t in raw_tags:
            if isinstance(t, dict):
                staff_tags.append({"id": str(t.get("id", "")), "name": t.get("name", "")})
            else:
                staff_tags.append({"id": "", "name": str(t)})

        # last_contacted from shifts_users.call_processed_at (latest)
        lc_dt = last_contacted_map.get(uid_str)
        last_contacted = _format_time_ago(lc_dt)

        # Resolve county_id — use cached or join via country_id
        county_id   = None
        county_name = None
        if u.get("county_id"):
            county_id   = str(u["county_id"])
            county_name = county_oid_to_name.get(county_id)
        elif u.get("country_id"):
            cid_str = str(u["country_id"])
            if cid_str in county_name_to_id:
                county_id   = county_name_to_id[cid_str]
                county_name = county_oid_to_name.get(county_id)
                await db["users"].update_one(
                    {"_id": u["_id"]}, {"$set": {"county_id": ObjectId(county_id)}}
                )

        # Resolve user_type_id — use cached or join via designation
        user_type_id   = None
        user_type_name = None
        if u.get("user_type_id"):
            user_type_id   = str(u["user_type_id"])
            user_type_name = type_id_to_name.get(user_type_id)
        elif u.get("designation") and u["designation"] in designation_to_type_id:
            user_type_id   = designation_to_type_id[u["designation"]]
            user_type_name = designation_to_type_name.get(u["designation"])
            await db["users"].update_one(
                {"_id": u["_id"]}, {"$set": {"user_type_id": ObjectId(user_type_id)}}
            )

        # Exclusion tags — check user's existing shifts against target shift
        user_email = u.get("email")
        exclusion_tags = await _get_user_exclusion_tags(db, user_email, target_shift) if user_email and target_shift else []
        excluded = 1 if exclusion_tags else 0

        # in_pool — from batch
        in_pool   = 1 if uid_str in pool_user_set else 0
        requested = 0

        # Visa hours remaining
        visa_used  = u.get("visa_hours_used")
        visa_total = u.get("visa_hours_total")
        visa_hours_remaining = f"{visa_used}/{visa_total}" if visa_used is not None and visa_total else None

        # Prior shifts count — from batch
        prior_shifts = prior_shifts_map.get(uid_str, 0)

        # Work history display string
        work_history = None
        if prior_shifts > 0 and last_contacted:
            work_history = f"{prior_shifts} Shift{'s' if prior_shifts != 1 else ''} · {last_contacted}"
        elif prior_shifts > 0:
            work_history = f"{prior_shifts} Shift{'s' if prior_shifts != 1 else ''}"
        elif last_contacted:
            work_history = last_contacted

        results.append({
            "id":                  uid_str,
            "xn_user_id":          u.get("xn_user_id"),
            "name":                " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() or "—",
            "email":               u.get("email"),
            "phone":               u.get("phone"),
            "designation":         u.get("designation"),
            "rating":              u.get("rating"),
            "channel":             "Phone",
            "staff_tags":          staff_tags,
            "last_contacted":      last_contacted,
            "visa_hours_remaining": visa_hours_remaining,
            "prior_shifts":        prior_shifts,
            "work_history":        work_history,
            "status":              u.get("status"),
            "county_id":           county_id,
            "county":              county_name,
            "user_type_id":        user_type_id,
            "user_type":           user_type_name,
            "user_latitude":       ucoords[0] if ucoords else None,
            "user_longitude":      ucoords[1] if ucoords else None,
            "distance_km":         distance_km,
            "excluded":            excluded,
            "exclusion_tags":      exclusion_tags,
            "requested":           requested,
            "in_pool":             in_pool,
        })

    # Apply radius filter
    if payload.radius is not None and client_coords:
        results = [r for r in results if r["distance_km"] is not None and r["distance_km"] <= payload.radius]

    # Apply excluded filter
    if payload.excluded is not None:
        results = [r for r in results if (r.get("excluded") or 0) == payload.excluded]

    # Sort results
    order_by = payload.order_by or "name"
    reverse  = (payload.sort or "asc").lower() == "desc"
    if order_by == "distance_km":
        results.sort(key=lambda r: r["distance_km"] if r["distance_km"] is not None else float("inf"), reverse=reverse)
    elif order_by == "rating":
        results.sort(key=lambda r: r["rating"] if r["rating"] is not None else 0, reverse=reverse)
    elif order_by == "name":
        results.sort(key=lambda r: r["name"].lower(), reverse=reverse)
    elif order_by == "last_contacted":
        results.sort(key=lambda r: r["last_contacted"] or "", reverse=reverse)

    filtered_total = len(results)

    return {
        "success":         True,
        "total":           filtered_total,
        "page":            payload.page,
        "per_page":        payload.per_page,
        "shift_id":        payload.shift_id,
        "shift_client":    shift_client_info,
        "client_location": client_location,
        "radius":          payload.radius,
        "order_by":        order_by,
        "sort":            payload.sort or "asc",
        "data":            results,
    }


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
        {"first_name": 1, "last_name": 1, "email": 1, "xn_user_id": 1, "designation": 1, "rating": 1}
    )
    if not user:
        raise HTTPException(status_code=404, detail=f"User {payload.user_id} not found")

    # Fetch shift
    shift = await db["shifts"].find_one({"_id": shift_oid}, {"_id": 1, "shift_code": 1, "name": 1})
    if not shift:
        raise HTTPException(status_code=404, detail=f"Shift {payload.shift_id} not found")

    email      = user.get("email") or ""
    full_name  = " ".join(filter(None, [user.get("first_name",""), user.get("last_name","")])).strip() or "—"
    now        = datetime.now(timezone.utc)

    # Check exclusion conditions before assigning
    target_shift = await db["shifts"].find_one(
        {"_id": shift_oid},
        {"date": 1, "start_time": 1, "end_time": 1, "shift_timing": 1, "shift_type": 1, "slots": 1}
    ) or {}
    exclusion_tags = await _get_user_exclusion_tags(db, email, target_shift) if email and target_shift else []

    if exclusion_tags:
        tag_messages = {
            "overlap":              "User has an overlapping shift on the same day",
            "duplicate_day":        "User already has a day shift on this date",
            "duplicate_night":      "User already has a night shift on this date",
            "consecutive_day_night": "User has both day and night shifts on this date",
            "exceeds_16h":          "Assignment would exceed 16 consecutive hours",
            "under_6h_gap":         "Less than 6 hours gap between shifts",
        }
        reasons = [tag_messages.get(t, t) for t in exclusion_tags]
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
    shift_ids:          list            # multiple shifts._id strings
    group_id:           Optional[str]   = None  # also check shifts_group_pool for this group
    page:               int = 1
    per_page:           int = 20
    radius:             Optional[float] = None
    order_by:           Optional[str]   = None
    sort:               Optional[str]   = "asc"
    county_multiple:    Optional[list]  = None
    user_type_multiple: Optional[list]  = None
    excluded:           Optional[int]   = None
    in_pool:            Optional[int]   = None  # 1 = in pool for ANY shift or the group


@router.post(
    "/list-multi",
    summary="List users for multiple shifts with same enrichment as /list",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def list_shift_users_multi(request: Request, payload: ListMultiShiftUsersRequest):
    """
    Body: { "shift_ids": ["<id1>", "<id2>", ...], "page": 1, "per_page": 20, ... }
    Returns Enabled users enriched with exclusion tags, pool status,
    work history, visa hours — same structure as /shift-users/list.
    Uses the first shift_id as primary for exclusion/distance checks.
    """
    db   = _get_db()
    skip = (payload.page - 1) * payload.per_page

    if not payload.shift_ids:
        raise HTTPException(status_code=400, detail="shift_ids must not be empty")

    shift_oids = []
    for sid in payload.shift_ids:
        if not ObjectId.is_valid(str(sid)):
            raise HTTPException(status_code=422, detail=f"Invalid shift_id: {sid}")
        shift_oids.append(ObjectId(str(sid)))

    # ── User filters ────────────────────────────────────────────────────────
    user_filter: dict = {"status": "Enabled"}

    if payload.county_multiple:
        county_values = []
        for c in payload.county_multiple:
            c_str = str(c)
            county_values.append(c_str)
            if ObjectId.is_valid(c_str):
                county_values.append(ObjectId(c_str))
        if county_values:
            user_filter["county_id"] = {"$in": county_values}

    if payload.user_type_multiple:
        valid_type_oids = [ObjectId(str(t)) for t in payload.user_type_multiple if ObjectId.is_valid(str(t))]
        if valid_type_oids:
            type_names_filter = []
            async for ut in db["user_types"].find({"_id": {"$in": valid_type_oids}}, {"name": 1}):
                type_names_filter.append(ut["name"])
            user_filter["$or"] = [
                {"user_type_id": {"$in": valid_type_oids}},
                {"designation":  {"$in": type_names_filter}},
            ]

    total = await db["users"].count_documents(user_filter)
    users = await db["users"].find(
        user_filter,
        {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
         "xn_user_id": 1, "designation": 1, "rating": 1,
         "location": 1, "latitude": 1, "longitude": 1, "status": 1,
         "tags": 1, "county_id": 1, "user_type_id": 1, "country_id": 1,
         "visa_hours_used": 1, "visa_hours_total": 1}
    ).sort("first_name", 1).skip(skip).limit(payload.per_page).to_list(length=payload.per_page)

    # Last contacted across all provided shifts
    user_ids_page = [u["_id"] for u in users]
    last_contacted_map: dict = {}
    if user_ids_page:
        async for su in db["shifts_users"].find(
            {"user_id": {"$in": user_ids_page},
             "shift_id": {"$in": shift_oids},
             "call_processed_at": {"$ne": None}},
            {"user_id": 1, "call_processed_at": 1}
        ).sort("call_processed_at", -1):
            uid = str(su.get("user_id", ""))
            if uid not in last_contacted_map:
                last_contacted_map[uid] = su.get("call_processed_at")

    # Client coords — use first shift
    primary_oid   = shift_oids[0]
    client_coords = await _get_shift_client_coords(db, primary_oid)
    client_location = {
        "latitude":  client_coords[0],
        "longitude": client_coords[1],
    } if client_coords else None

    # Primary shift for exclusion checks
    target_shift = await db["shifts"].find_one(
        {"_id": primary_oid},
        {"date": 1, "start_time": 1, "end_time": 1, "shift_timing": 1, "shift_type": 1, "slots": 1}
    ) or {}

    # Batch county / user_type lookups
    county_name_to_id: dict   = {}
    county_oid_to_name: dict  = {}
    designation_to_type_id: dict   = {}
    designation_to_type_name: dict = {}
    type_id_to_name: dict     = {}

    users_needing_county = [u for u in users if not u.get("county_id") and u.get("country_id")]
    users_needing_type   = [u for u in users if not u.get("user_type_id") and u.get("designation")]

    if users_needing_county:
        raw_cids = list({str(u["country_id"]) for u in users_needing_county})
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
            type_id_to_name[str(ut["_id"])] = ut["name"]

    # Pool map — check shifts_pool (by shift_id) AND shifts_group_pool
    pool_records = await db["shifts_pool"].find(
        {"shift_id": {"$in": shift_oids}, "user_id": {"$in": user_ids_page}},
        {"user_id": 1}
    ).to_list(5000)
    pool_user_ids = {str(p["user_id"]) for p in pool_records}

    # Check shifts_group_pool — use explicit group_id if provided, else find by shift_ids
    group_oids = []
    if payload.group_id and ObjectId.is_valid(payload.group_id):
        group_oids.append(ObjectId(payload.group_id))
    else:
        async for g in db["shifts_group"].find({"shift_ids": {"$in": shift_oids}}, {"_id": 1}):
            group_oids.append(g["_id"])

    if group_oids:
        async for gp in db["shifts_group_pool"].find(
            {"group_id": {"$in": group_oids}, "user_id": {"$in": user_ids_page}},
            {"user_id": 1}
        ):
            pool_user_ids.add(str(gp["user_id"]))

    from app.routers.staff import _haversine_km as _hav_m, _user_coords as _uc_m

    results = []
    for u in users:
        uid_str = str(u["_id"])
        ucoords = _uc_m(u)

        distance_km = None
        if client_coords and ucoords:
            distance_km = _hav_m(client_coords[0], client_coords[1], ucoords[0], ucoords[1])

        raw_tags   = u.get("tags") or []
        staff_tags = [
            {"id": str(t.get("id","")), "name": t.get("name","")} if isinstance(t, dict)
            else {"id": "", "name": str(t)} for t in raw_tags
        ]

        lc_dt = last_contacted_map.get(uid_str)
        last_contacted = None
        if lc_dt:
            if hasattr(lc_dt, "tzinfo") and lc_dt.tzinfo is None:
                from datetime import timezone as _tz
                lc_dt = lc_dt.replace(tzinfo=_tz.utc)
            from datetime import timezone as _tz
            diff = int((datetime.now(_tz.utc) - lc_dt).total_seconds())
            if diff < 60:       last_contacted = "just now"
            elif diff < 3600:   last_contacted = f"{diff//60} minute{'s' if diff//60!=1 else ''} ago"
            elif diff < 86400:  last_contacted = f"{diff//3600} hour{'s' if diff//3600!=1 else ''} ago"
            else:               last_contacted = f"{diff//86400} day{'s' if diff//86400!=1 else ''} ago"

        visa_used  = u.get("visa_hours_used")
        visa_total = u.get("visa_hours_total")
        visa_hours_remaining = f"{visa_used}/{visa_total}" if visa_used is not None and visa_total else None

        prior_shifts = await db["shifts_users"].count_documents({"user_id": u["_id"], "availability": 1})
        work_history = None
        if prior_shifts > 0 and last_contacted:
            work_history = f"{prior_shifts} Shift{'s' if prior_shifts != 1 else ''} · {last_contacted}"
        elif prior_shifts > 0:
            work_history = f"{prior_shifts} Shift{'s' if prior_shifts != 1 else ''}"
        elif last_contacted:
            work_history = last_contacted

        # County / user_type
        county_id = county_name = None
        if u.get("county_id"):
            county_id   = str(u["county_id"])
            county_name = county_oid_to_name.get(county_id)
        elif u.get("country_id"):
            cid_str = str(u["country_id"])
            if cid_str in county_name_to_id:
                county_id   = county_name_to_id[cid_str]
                county_name = county_oid_to_name.get(county_id)
                await db["users"].update_one({"_id": u["_id"]}, {"$set": {"county_id": ObjectId(county_id)}})

        user_type_id = user_type_name = None
        if u.get("user_type_id"):
            user_type_id   = str(u["user_type_id"])
            user_type_name = type_id_to_name.get(user_type_id)
        elif u.get("designation") and u["designation"] in designation_to_type_id:
            user_type_id   = designation_to_type_id[u["designation"]]
            user_type_name = designation_to_type_name.get(u["designation"])
            await db["users"].update_one({"_id": u["_id"]}, {"$set": {"user_type_id": ObjectId(user_type_id)}})

        # Exclusion check against primary shift
        user_email     = u.get("email")
        exclusion_tags = await _get_user_exclusion_tags(db, user_email, target_shift) if user_email and target_shift else []
        excluded       = 1 if exclusion_tags else 0
        in_pool_val    = 1 if uid_str in pool_user_ids else 0

        results.append({
            "id":                  uid_str,
            "xn_user_id":          u.get("xn_user_id"),
            "name":                " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() or "—",
            "email":               u.get("email"),
            "phone":               u.get("phone"),
            "designation":         u.get("designation"),
            "rating":              u.get("rating"),
            "channel":             "Phone",
            "staff_tags":          staff_tags,
            "last_contacted":      last_contacted,
            "visa_hours_remaining": visa_hours_remaining,
            "prior_shifts":        prior_shifts,
            "work_history":        work_history,
            "status":              u.get("status"),
            "county_id":           county_id,
            "county":              county_name,
            "user_type_id":        user_type_id,
            "user_type":           user_type_name,
            "user_latitude":       ucoords[0] if ucoords else None,
            "user_longitude":      ucoords[1] if ucoords else None,
            "distance_km":         distance_km,
            "excluded":            excluded,
            "exclusion_tags":      exclusion_tags,
            "requested":           0,
            "in_pool":             in_pool_val,
        })

    # by_designation — count ALL enabled users by designation (respecting user_type_multiple if set)
    desig_query: dict = {"status": "Enabled"}
    if payload.user_type_multiple:
        valid_type_oids_d = [ObjectId(t) for t in payload.user_type_multiple if ObjectId.is_valid(str(t))]
        if valid_type_oids_d:
            type_names_d = []
            async for ut in db["user_types"].find({"_id": {"$in": valid_type_oids_d}}, {"name": 1}):
                type_names_d.append(ut["name"])
            desig_query["$or"] = [
                {"user_type_id": {"$in": valid_type_oids_d}},
                {"designation":  {"$in": type_names_d}},
            ]

    desig_map: dict = {}
    async for u in db["users"].find(desig_query, {"designation": 1, "user_type_id": 1}):
        d  = u.get("designation") or "Unknown"
        ut = str(u["user_type_id"]) if u.get("user_type_id") else None
        if d not in desig_map:
            desig_map[d] = {"designation": d, "user_type_id": ut, "count": 0}
        desig_map[d]["count"] += 1

    designation_list = sorted(desig_map.values(), key=lambda x: -x["count"])

    # Filters
    if payload.radius is not None and client_coords:
        results = [r for r in results if r["distance_km"] is not None and r["distance_km"] <= payload.radius]
    if payload.excluded is not None:
        results = [r for r in results if (r.get("excluded") or 0) == payload.excluded]
    if payload.in_pool is not None:
        results = [r for r in results if r["in_pool"] == payload.in_pool]

    # Sort
    order_by = payload.order_by or "name"
    reverse  = (payload.sort or "asc").lower() == "desc"
    if order_by == "distance_km":
        results.sort(key=lambda r: r["distance_km"] if r["distance_km"] is not None else float("inf"), reverse=reverse)
    elif order_by == "rating":
        results.sort(key=lambda r: r["rating"] if r["rating"] is not None else 0, reverse=reverse)
    elif order_by == "name":
        results.sort(key=lambda r: r["name"].lower(), reverse=reverse)

    return {
        "success":         True,
        "total":           await db["users"].count_documents(desig_query),
        "filtered_total":  len(results),
        "page":            payload.page,
        "per_page":        payload.per_page,
        "shift_ids":       [str(o) for o in shift_oids],
        "client_location": client_location,
        "radius":          payload.radius,
        "order_by":        order_by,
        "sort":            payload.sort or "asc",
        "by_designation":  designation_list,
        "data":            results,
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

    # Also update shift with assigned staff
    await db["shifts"].update_one(
        {"_id": shift_oid},
        {"$set": {
            "staff_email":    email,
            "assigned_staff": full_name,
            "staff_id":       str(user_oid),
            "assigned_at":    now,
            "updated_at":     now,
        }}
    )

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
    Sets shifts.ghost_booking = true and shifts.staff = {id, name}.
    """
    db        = _get_db()
    shift_oid = _resolve_oid(payload.shift_id, "shift_id")
    user_oid  = _resolve_oid(payload.user_id,  "user_id")

    shift = await db["shifts"].find_one({"_id": shift_oid}, {"_id": 1})
    if not shift:
        raise HTTPException(status_code=404, detail="Shift not found")

    user = await db["users"].find_one(
        {"_id": user_oid},
        {"first_name": 1, "last_name": 1, "email": 1, "designation": 1, "rating": 1}
    )
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

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
