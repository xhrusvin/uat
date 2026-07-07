import logging
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
router = APIRouter(prefix="/shifts-group", tags=["Shifts Group"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize(doc: dict) -> dict:
    result = {}
    for k, v in doc.items():
        key = "id" if k == "_id" else k
        if isinstance(v, ObjectId):
            result[key] = str(v)
        elif hasattr(v, "isoformat"):
            result[key] = v.isoformat()
        elif isinstance(v, list):
            result[key] = [str(i) if isinstance(i, ObjectId) else i for i in v]
        else:
            result[key] = v
    return result


# ── Schemas ───────────────────────────────────────────────────────────────────

class ShiftGroupCreate(BaseModel):
    name:        Optional[str] = None
    shift_ids:   list          = []   # list of shifts._id strings


class AddShiftsRequest(BaseModel):
    group_id:  str
    shift_ids: list   # list of shifts._id strings to add


class RemoveShiftsRequest(BaseModel):
    group_id:  str
    shift_ids: list   # list of shifts._id strings to remove


# ── CREATE group ──────────────────────────────────────────────────────────────

@router.post(
    "/",
    summary="Create a shift group",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def create_shift_group(request: Request, payload: ShiftGroupCreate):
    """
    Body: { "name": "Optional name", "shift_ids": ["<shift_id>", ...] }
    Creates a shifts_group document and validates all shift_ids exist.
    """
    db = _get_db()
    now = datetime.now(timezone.utc)

    # Validate and resolve shift ObjectIds
    shift_oids = []
    invalid = []
    for sid in payload.shift_ids:
        if ObjectId.is_valid(str(sid)):
            shift_oids.append(ObjectId(str(sid)))
        else:
            invalid.append(sid)

    if invalid:
        raise HTTPException(status_code=422, detail=f"Invalid shift_ids: {invalid}")

    # Verify shifts exist
    if shift_oids:
        found = await db["shifts"].count_documents({"_id": {"$in": shift_oids}})
        if found != len(shift_oids):
            raise HTTPException(status_code=404, detail="One or more shifts not found")

    doc = {
        "name":       payload.name or None,
        "shift_ids":  shift_oids,
        "created_at": now,
        "updated_at": now,
    }
    result = await db["shifts_group"].insert_one(doc)
    doc["_id"] = result.inserted_id

    logger.info(f"shifts_group created: {result.inserted_id} shifts={len(shift_oids)}")
    return {"success": True, "data": _serialize(doc)}


# ── GET group ─────────────────────────────────────────────────────────────────

class ShiftGroupDetailRequest(BaseModel):
    group_id: str


@router.post(
    "/detail",
    summary="Get a shift group with shift details",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def get_shift_group(request: Request, payload: ShiftGroupDetailRequest):
    db = _get_db()
    if not ObjectId.is_valid(payload.group_id):
        raise HTTPException(status_code=422, detail="Invalid group_id")

    group = await db["shifts_group"].find_one({"_id": ObjectId(payload.group_id)})
    if not group:
        raise HTTPException(status_code=404, detail="Shift group not found")

    # Enrich with shift details
    shift_oids = group.get("shift_ids") or []
    shifts = []
    if shift_oids:
        async for sh in db["shifts"].find(
            {"_id": {"$in": shift_oids}},
            {"name": 1, "shift_code": 1, "date": 1, "start_time": 1,
             "end_time": 1, "location": 1, "user_type": 1, "shift_timing": 1}
        ):
            shifts.append({
                "id":          str(sh["_id"]),
                "name":        sh.get("name") or sh.get("shift_code"),
                "shift_code":  sh.get("shift_code"),
                "date":        sh["date"].isoformat() if sh.get("date") and hasattr(sh["date"], "isoformat") else str(sh.get("date", "")),
                "start_time":  sh.get("start_time"),
                "end_time":    sh.get("end_time"),
                "location":    sh.get("location"),
                "user_type":   sh.get("user_type"),
                "shift_timing": sh.get("shift_timing"),
            })

    s = _serialize(group)
    s["shifts"] = shifts
    s["shift_count"] = len(shifts)

    # Fetch pool users from shifts_group_pool
    pool_docs = await db["shifts_group_pool"].find({"group_id": ObjectId(payload.group_id)}).to_list(5000)
    pool_user_oids = [
        p["user_id"] for p in pool_docs
        if p.get("user_id") and ObjectId.is_valid(str(p.get("user_id", "")))
    ]
    pool_user_map: dict = {}
    if pool_user_oids:
        async for u in db["users"].find(
            {"_id": {"$in": pool_user_oids}},
            {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
             "xn_user_id": 1, "designation": 1, "rating": 1, "status": 1}
        ):
            pool_user_map[str(u["_id"])] = u

    pool_users = []
    for p in pool_docs:
        uid_str = str(p.get("user_id", ""))
        u = pool_user_map.get(uid_str, {})
        pool_users.append({
            "id":          str(p["_id"]),
            "user_id":     uid_str,
            "xn_user_id":  u.get("xn_user_id"),
            "name":        " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() or "—",
            "email":       u.get("email"),
            "phone":       u.get("phone"),
            "designation": u.get("designation"),
            "rating":      u.get("rating"),
            "status":      u.get("status"),
            "added_at":    p["added_at"].isoformat() if p.get("added_at") and hasattr(p["added_at"], "isoformat") else None,
            "added_by":    p.get("added_by"),
        })

    s["pool_users"]  = pool_users
    s["pool_count"]  = len(pool_users)

    # Available staff — full structure matching /shifts-db/detail
    avail_su = await db["shifts_group_users"].find(
        {"group_id": group_oid, "availability": 1},
        {"user_id": 1, "availability": 1, "call_processed_at": 1,
         "outreach_id": 1, "conversation_id": 1, "shift_id": 1}
    ).to_list(length=500)

    avail_user_oids = [
        ObjectId(str(su["user_id"])) for su in avail_su
        if su.get("user_id") and ObjectId.is_valid(str(su.get("user_id", "")))
    ]
    avail_user_map: dict = {}
    if avail_user_oids:
        async for u in db["users"].find(
            {"_id": {"$in": avail_user_oids}},
            {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
             "xn_user_id": 1, "designation": 1, "rating": 1,
             "county": 1, "county_id": 1, "tags": 1, "location": 1,
             "visa_hours_used": 1, "visa_hours_total": 1}
        ):
            avail_user_map[str(u["_id"])] = u

    AVAIL_TEXT = {1: "Available", 0: "Not Available", 3: "Voicemail", 4: "Call Not Attended", 6: "Call Not Triggered"}

    from app.routers.staff import _haversine_km as _hav_sg, _user_coords as _uc_sg

    available_staff = []
    for su in avail_su:
        uid_str   = str(su.get("user_id", ""))
        u         = avail_user_map.get(uid_str, {})
        avail_val = su.get("availability")
        raw_oid   = su.get("outreach_id")
        user_oid_val = su.get("user_id")

        # Staff tags
        raw_tags   = u.get("tags") or []
        staff_tags = [
            {"id": str(t.get("id","")), "name": t.get("name","")} if isinstance(t, dict)
            else {"id": "", "name": str(t)} for t in raw_tags
        ]

        # Last contacted
        last_contacted = None
        lc_su = await db["shifts_group_users"].find_one(
            {"user_id": user_oid_val, "call_processed_at": {"$ne": None}},
            sort=[("call_processed_at", -1)], projection={"call_processed_at": 1}
        )
        if lc_su and lc_su.get("call_processed_at"):
            from datetime import timezone as _tz_sg
            lc = lc_su["call_processed_at"]
            if hasattr(lc, "tzinfo") and lc.tzinfo is None:
                lc = lc.replace(tzinfo=_tz_sg.utc)
            diff = int((datetime.now(_tz_sg.utc) - lc).total_seconds())
            if diff < 60:       last_contacted = "just now"
            elif diff < 3600:   last_contacted = f"{diff//60} minute{'s' if diff//60!=1 else ''} ago"
            elif diff < 86400:  last_contacted = f"{diff//3600} hour{'s' if diff//3600!=1 else ''} ago"
            else:               last_contacted = f"{diff//86400} day{'s' if diff//86400!=1 else ''} ago"

        # Distance (no single client coord for group — skip)
        distance_km = None

        # Response + call_details
        response_text = response_time_sg = None
        call_details  = None
        conv_sg = await db["shift_booking_conv"].find_one(
            {"user_id": uid_str},
            {"turns": 1, "started_at": 1, "ended_at": 1,
             "elevenlabs_conversation_id": 1, "round_number": 1,
             "phone": 1, "duration_seconds": 1, "confidence": 1}
        )
        if conv_sg:
            for turn in reversed(conv_sg.get("turns") or []):
                if turn.get("role") in ("user", "human") and turn.get("message"):
                    response_text = turn["message"]
                    ts = turn.get("ts")
                    if ts and hasattr(ts, "strftime"):
                        response_time_sg = ts.strftime("%H:%M")
                    break
            started_sg = conv_sg.get("started_at")
            ended_sg   = conv_sg.get("ended_at")
            dur_sg     = conv_sg.get("duration_seconds")
            if not dur_sg and started_sg and ended_sg:
                dur_sg = int((ended_sg - started_sg).total_seconds())
            pt_sg      = started_sg.strftime("%H:%M:%S") if started_sg and hasattr(started_sg, "strftime") else None
            rn_sg      = conv_sg.get("round_number", 1)
            ph_sg      = conv_sg.get("phone") or u.get("phone")
            ai_sg      = None
            conf_sg    = conv_sg.get("confidence")
            for turn in reversed(conv_sg.get("turns") or []):
                if turn.get("role") in ("user", "human") and turn.get("message"):
                    t_ts = turn.get("ts")
                    t_t  = t_ts.strftime("%H:%M") if t_ts and hasattr(t_ts, "strftime") else None
                    cp   = f"{int(conf_sg * 100)}% confidence" if conf_sg else None
                    parts = [f'"{turn["message"]}"']
                    if t_t: parts.append(f"at {t_t}")
                    if cp:  parts.append(f"· {cp}")
                    ai_sg = " ".join(parts)
                    break
            call_details = {
                "called_via": f"{ph_sg} (phone)" if ph_sg else "Phone",
                "placed_at":  f"{pt_sg} · Round {rn_sg}" if pt_sg else None,
                "duration":   f"{dur_sg} seconds" if dur_sg else None,
                "ai_heard":   ai_sg,
            }

        available_staff.append({
            "id":                  uid_str,
            "xn_user_id":          u.get("xn_user_id"),
            "name":                " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() or "—",
            "email":               u.get("email"),
            "phone":               u.get("phone"),
            "designation":         u.get("designation"),
            "rating":              u.get("rating"),
            "county":              u.get("county"),
            "county_id":           str(u["county_id"]) if u.get("county_id") else None,
            "prior_shifts_here":   0,
            "last_contacted":      last_contacted,
            "staff_tags":          staff_tags,
            "visa_hours_remaining": "8/24",
            "channel":             "Phone",
            "response_text":       response_text,
            "response_time":       response_time_sg,
            "availability":        avail_val,
            "availability_text":   AVAIL_TEXT.get(avail_val, "Unknown"),
            "group_id":            str(group_oid),
            "outreach_id":         str(raw_oid) if raw_oid else None,
            "conversation_id":     su.get("conversation_id"),
            "distance_km":         distance_km,
            "call_details":        call_details,
            "confirm": {
                "staff_label":   f"{' '.join(filter(None, [u.get('first_name',''), u.get('last_name','')])).strip()} · ★ {u.get('rating') or '—'} · 0 prior shifts here",
                "prior_shifts_here": 0,
                "rating":        u.get("rating"),
                "shift":         None,
                "placed_at":     None,
                "confirmed_by":  "System",
            },
        })

    s["available_staff"] = available_staff
    s["available_count"] = len(available_staff)

    return {"success": True, "data": s}


# ── LIST groups ───────────────────────────────────────────────────────────────

@router.get(
    "/",
    summary="List all shift groups",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def list_shift_groups(request: Request):
    db = _get_db()
    docs = await db["shifts_group"].find({}).sort("created_at", -1).to_list(200)
    data = []
    for d in docs:
        s = _serialize(d)
        s["shift_count"] = len(d.get("shift_ids") or [])
        data.append(s)
    return {"success": True, "total": len(data), "data": data}


# ── ADD shifts to group ───────────────────────────────────────────────────────

@router.post(
    "/add-shifts",
    summary="Add shifts to an existing group",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def add_shifts_to_group(request: Request, payload: AddShiftsRequest):
    """
    Body: { "group_id": "...", "shift_ids": ["<shift_id>", ...] }
    Adds shifts to group (skips duplicates).
    """
    db = _get_db()
    if not ObjectId.is_valid(payload.group_id):
        raise HTTPException(status_code=422, detail="Invalid group_id")

    group = await db["shifts_group"].find_one({"_id": ObjectId(payload.group_id)})
    if not group:
        raise HTTPException(status_code=404, detail="Shift group not found")

    # Validate shift_ids
    shift_oids = []
    invalid = []
    for sid in payload.shift_ids:
        if ObjectId.is_valid(str(sid)):
            shift_oids.append(ObjectId(str(sid)))
        else:
            invalid.append(sid)

    if invalid:
        raise HTTPException(status_code=422, detail=f"Invalid shift_ids: {invalid}")

    if not shift_oids:
        raise HTTPException(status_code=400, detail="No valid shift_ids provided")

    now = datetime.now(timezone.utc)
    result = await db["shifts_group"].update_one(
        {"_id": ObjectId(payload.group_id)},
        {
            "$addToSet": {"shift_ids": {"$each": shift_oids}},
            "$set":      {"updated_at": now},
        }
    )

    updated = await db["shifts_group"].find_one({"_id": ObjectId(payload.group_id)})
    return {
        "success":     True,
        "message":     f"{len(shift_oids)} shift(s) added to group",
        "group_id":    payload.group_id,
        "shift_count": len(updated.get("shift_ids") or []),
    }


# ── REMOVE shifts from group ──────────────────────────────────────────────────

@router.post(
    "/remove-shifts",
    summary="Remove shifts from a group",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def remove_shifts_from_group(request: Request, payload: RemoveShiftsRequest):
    """
    Body: { "group_id": "...", "shift_ids": ["<shift_id>", ...] }
    """
    db = _get_db()
    if not ObjectId.is_valid(payload.group_id):
        raise HTTPException(status_code=422, detail="Invalid group_id")

    group = await db["shifts_group"].find_one({"_id": ObjectId(payload.group_id)})
    if not group:
        raise HTTPException(status_code=404, detail="Shift group not found")

    shift_oids = [ObjectId(str(sid)) for sid in payload.shift_ids if ObjectId.is_valid(str(sid))]
    if not shift_oids:
        raise HTTPException(status_code=400, detail="No valid shift_ids provided")

    now = datetime.now(timezone.utc)
    await db["shifts_group"].update_one(
        {"_id": ObjectId(payload.group_id)},
        {
            "$pull": {"shift_ids": {"$in": shift_oids}},
            "$set":  {"updated_at": now},
        }
    )

    updated = await db["shifts_group"].find_one({"_id": ObjectId(payload.group_id)})
    return {
        "success":     True,
        "message":     f"{len(shift_oids)} shift(s) removed from group",
        "group_id":    payload.group_id,
        "shift_count": len(updated.get("shift_ids") or []),
    }


# ── DELETE group ──────────────────────────────────────────────────────────────

class ShiftGroupDeleteRequest(BaseModel):
    group_id: str


@router.post(
    "/delete",
    summary="Delete a shift group",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def delete_shift_group(request: Request, payload: ShiftGroupDeleteRequest):
    db = _get_db()
    if not ObjectId.is_valid(payload.group_id):
        raise HTTPException(status_code=422, detail="Invalid group_id")

    result = await db["shifts_group"].delete_one({"_id": ObjectId(payload.group_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Shift group not found")

    return {"success": True, "message": "Shift group deleted", "id": payload.group_id}


# ── shifts_group_pool ─────────────────────────────────────────────────────────

class GroupPoolAddRequest(BaseModel):
    group_id:  str
    user_ids:  list   # list of users._id strings


class GroupPoolRemoveRequest(BaseModel):
    group_id: str
    user_id:  str   # single users._id


@router.post(
    "/pool/add",
    summary="Add staff to a shift group pool",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def add_staff_to_group_pool(request: Request, payload: GroupPoolAddRequest):
    """
    Body: { "group_id": "...", "user_ids": ["<user_id>", ...] }
    Adds users to shifts_group_pool for this group (skips duplicates).
    """
    db = _get_db()
    if not ObjectId.is_valid(payload.group_id):
        raise HTTPException(status_code=422, detail="Invalid group_id")

    group = await db["shifts_group"].find_one({"_id": ObjectId(payload.group_id)}, {"_id": 1})
    if not group:
        raise HTTPException(status_code=404, detail="Shift group not found")

    # Validate user ObjectIds
    user_oids = []
    invalid = []
    for uid in payload.user_ids:
        if ObjectId.is_valid(str(uid)):
            user_oids.append(ObjectId(str(uid)))
        else:
            invalid.append(uid)
    if invalid:
        raise HTTPException(status_code=422, detail=f"Invalid user_ids: {invalid}")
    if not user_oids:
        raise HTTPException(status_code=400, detail="No valid user_ids provided")

    now = datetime.now(timezone.utc)
    group_oid = ObjectId(payload.group_id)

    # Get already added user_ids
    existing = {
        str(p["user_id"])
        async for p in db["shifts_group_pool"].find(
            {"group_id": group_oid, "user_id": {"$in": user_oids}},
            {"user_id": 1}
        )
    }

    inserted = 0
    skipped  = 0
    for oid in user_oids:
        if str(oid) in existing:
            skipped += 1
            continue
        await db["shifts_group_pool"].insert_one({
            "group_id":  group_oid,
            "user_id":   oid,
            "added_at":  now,
            "added_by":  "manual",
        })
        inserted += 1

    return {
        "success":  True,
        "message":  f"{inserted} staff added to group pool",
        "group_id": payload.group_id,
        "inserted": inserted,
        "skipped":  skipped,
    }


@router.post(
    "/pool/remove",
    summary="Remove staff from a shift group pool",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def remove_staff_from_group_pool(request: Request, payload: GroupPoolRemoveRequest):
    """
    Body: { "group_id": "...", "user_id": "<user_id>" }
    Removes user from shifts_group_pool and shifts_group_users.
    """
    db = _get_db()
    if not ObjectId.is_valid(payload.group_id):
        raise HTTPException(status_code=422, detail="Invalid group_id")
    if not ObjectId.is_valid(str(payload.user_id)):
        raise HTTPException(status_code=422, detail="Invalid user_id")

    group_oid = ObjectId(payload.group_id)
    user_oid  = ObjectId(str(payload.user_id))

    pool_result = await db["shifts_group_pool"].delete_many({
        "group_id": group_oid,
        "user_id":  user_oid,
    })

    # Also remove from shifts_group_users
    su_result = await db["shifts_group_users"].delete_many({
        "group_id": group_oid,
        "user_id":  user_oid,
    })

    return {
        "success":                    True,
        "message":                    "Staff removed from group pool and users",
        "group_id":                   payload.group_id,
        "user_id":                    payload.user_id,
        "pool_removed":               pool_result.deleted_count,
        "shifts_group_users_removed": su_result.deleted_count,
    }


@router.post(
    "/pool/list",
    summary="List staff in a shift group pool",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def list_group_pool(request: Request, payload: ShiftGroupDetailRequest):
    """
    Body: { "group_id": "..." }
    Returns all users in the shifts_group_pool for this group with user details.
    """
    db = _get_db()
    if not ObjectId.is_valid(payload.group_id):
        raise HTTPException(status_code=422, detail="Invalid group_id")

    group_oid = ObjectId(payload.group_id)
    group = await db["shifts_group"].find_one({"_id": group_oid}, {"_id": 1, "name": 1})
    if not group:
        raise HTTPException(status_code=404, detail="Shift group not found")

    pool_docs = await db["shifts_group_pool"].find({"group_id": group_oid}).to_list(5000)

    # Batch user lookup
    user_oids = [p["user_id"] for p in pool_docs if p.get("user_id") and ObjectId.is_valid(str(p.get("user_id", "")))]
    user_map: dict = {}
    if user_oids:
        async for u in db["users"].find(
            {"_id": {"$in": user_oids}},
            {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
             "xn_user_id": 1, "designation": 1, "rating": 1, "status": 1}
        ):
            user_map[str(u["_id"])] = u

    staff = []
    for p in pool_docs:
        uid_str = str(p.get("user_id", ""))
        u = user_map.get(uid_str, {})
        staff.append({
            "id":          str(p["_id"]),
            "user_id":     uid_str,
            "xn_user_id":  u.get("xn_user_id"),
            "name":        " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() or "—",
            "email":       u.get("email"),
            "phone":       u.get("phone"),
            "designation": u.get("designation"),
            "rating":      u.get("rating"),
            "status":      u.get("status"),
            "added_at":    p["added_at"].isoformat() if p.get("added_at") and hasattr(p["added_at"], "isoformat") else None,
            "added_by":    p.get("added_by"),
        })

    return {
        "success":    True,
        "group_id":   payload.group_id,
        "group_name": group.get("name"),
        "total":      len(staff),
        "data":       staff,
    }
