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
    id: str   # shift_users._id


class AddUsersToShiftRequest(BaseModel):
    shift_id:  str        # MongoDB ObjectId of the shift
    user_ids:  List[str]  # List of user MongoDB ObjectIds


# ── ADD single user to shift ──────────────────────────────────────────────────

@router.post(
    "/",
    summary="Add a user to a shift (creates shift_users record)",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def add_user_to_shift(request: Request, payload: AddUserToShiftRequest):
    """
    Creates a shift_users document linking a user to a shift.
    All fields default to 0/null as per schema.
    Returns 409 if user is already added to this shift.
    """
    db = _get_db()
    now = datetime.now(timezone.utc)

    user_oid  = _resolve_oid(payload.user_id,  "user_id")
    shift_oid = _resolve_oid(payload.shift_id, "shift_id")

    # Validate shift exists
    shift = await db["shifts"].find_one({"_id": shift_oid}, {"_id": 1, "shift_code": 1, "name": 1})
    if not shift:
        raise HTTPException(status_code=404, detail=f"Shift {payload.shift_id} not found")

    # Validate user exists
    user = await db["users"].find_one({"_id": user_oid}, {"_id": 1, "first_name": 1, "last_name": 1, "email": 1})
    if not user:
        raise HTTPException(status_code=404, detail=f"User {payload.user_id} not found")

    # Check for duplicate
    existing = await db["shifts_users"].find_one({
        "shift_id": shift_oid,
        "user_id":  user_oid,
    })
    if existing:
        full_name = " ".join(filter(None, [
            user.get("first_name", ""), user.get("last_name", "")
        ])).strip() or payload.user_id
        shift_code = shift.get("shift_code") or shift.get("name") or payload.shift_id
        raise HTTPException(
            status_code=409,
            detail=f"{full_name} is already added to shift {shift_code}"
        )

    doc = {
        "user_id":           user_oid,
        "shift_id":          shift_oid,
        "assigned_at":       now,
        "availability":      6,
        "call_enabled":      0,
        "call_processed":    0,
        "call_processed_at": now,
        "conversation_id":   None,
        "agent_id":          None,
        "call_status":       0,
        "call_summary_title":None,
        "ended_at":          None,
        "started_at":        None,
        "updated_at":        now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
    }

    result = await db["shifts_users"].insert_one(doc)
    doc["_id"] = result.inserted_id

    logger.info(f"shift_users: added user={payload.user_id} shift={payload.shift_id}")

    return {
        "success": True,
        "message": "User added to shift",
        "data":    _serialize(doc),
    }


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

    # Check which are already in shift_users
    already_added = {
        str(su["user_id"])
        async for su in db["shifts_users"].find(
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
            "user_id":           user_oid,
            "shift_id":          shift_oid,
            "assigned_at":       now,
            "availability":      6,
            "call_enabled":      0,
            "call_processed":    0,
            "call_processed_at": now,
            "conversation_id":   None,
            "agent_id":          None,
            "call_status":       0,
            "call_summary_title":None,
            "ended_at":          None,
            "started_at":        None,
            "updated_at":        now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        }
        result = await db["shifts_users"].insert_one(doc)
        inserted_ids.append(str(result.inserted_id))
        inserted += 1

    logger.info(f"shift_users bulk: shift={payload.shift_id} inserted={inserted} dup={skipped_dup} missing={skipped_missing}")

    return {
        "success": True,
        "message": f"{inserted} user(s) added to shift",
        "data": {
            "shift_id":       payload.shift_id,
            "inserted":       inserted,
            "skipped_duplicate": skipped_dup,
            "skipped_missing_user": skipped_missing,
            "inserted_ids":   inserted_ids,
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
    db  = _get_db()
    oid = _resolve_oid(payload.id, "id")

    # Fetch before deleting to log outreach state
    existing = await db["shifts_users"].find_one({"_id": oid}, {"outreach_id": 1, "shift_id": 1})
    if not existing:
        raise HTTPException(status_code=404, detail=f"shift_users record {payload.id} not found")

    result = await db["shifts_users"].delete_one({"_id": oid})

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail=f"shift_users record {payload.id} not found")

    return {
        "success": True,
        "message": "User removed from shift",
        "id": payload.id,
        "had_outreach": existing.get("outreach_id") is not None,
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


# ── LIST shift_users with pagination (POST body) ──────────────────────────────

class ListShiftUsersRequest(BaseModel):
    shift_id:  str
    page:      int = 1
    per_page:  int = 20
    radius:    Optional[float] = None   # km — only return users within this radius
    order_by:  Optional[str]  = None   # e.g. "distance_km", "name", "rating"
    sort:      Optional[str]  = "asc"  # "asc" or "desc"


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

    # Query only Enabled users — no shifts_users join
    user_filter = {"status": "Enabled"}

    total = await db["users"].count_documents(user_filter)
    users = await db["users"].find(
        user_filter,
        {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
         "xn_user_id": 1, "designation": 1, "rating": 1,
         "location": 1, "latitude": 1, "longitude": 1, "status": 1,
         "tags": 1, "county_id": 1, "user_type_id": 1, "country_id": 1}
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

        results.append({
            "id":             uid_str,
            "xn_user_id":     u.get("xn_user_id"),
            "name":           " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() or "—",
            "email":          u.get("email"),
            "phone":          u.get("phone"),
            "designation":    u.get("designation"),
            "rating":         u.get("rating"),
            "channel":        "Phone",
            "staff_tags":     staff_tags,
            "last_contacted": last_contacted,
            "status":         u.get("status"),
            "county_id":      county_id,
            "county":         county_name,
            "user_type_id":   user_type_id,
            "user_type":      user_type_name,
            "user_latitude":  ucoords[0] if ucoords else None,
            "user_longitude": ucoords[1] if ucoords else None,
            "distance_km":    distance_km,
        })

    # Apply radius filter
    if payload.radius is not None and client_coords:
        results = [r for r in results if r["distance_km"] is not None and r["distance_km"] <= payload.radius]

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
