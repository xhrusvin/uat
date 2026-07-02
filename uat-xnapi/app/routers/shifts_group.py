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
    group_id:  str
    user_ids:  list   # list of users._id strings


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
    Body: { "group_id": "...", "user_ids": ["<user_id>", ...] }
    Removes users from shifts_group_pool for this group.
    """
    db = _get_db()
    if not ObjectId.is_valid(payload.group_id):
        raise HTTPException(status_code=422, detail="Invalid group_id")

    group_oid = ObjectId(payload.group_id)
    user_oids = [ObjectId(str(uid)) for uid in payload.user_ids if ObjectId.is_valid(str(uid))]
    if not user_oids:
        raise HTTPException(status_code=400, detail="No valid user_ids provided")

    result = await db["shifts_group_pool"].delete_many({
        "group_id": group_oid,
        "user_id":  {"$in": user_oids},
    })

    return {
        "success":  True,
        "message":  f"{result.deleted_count} staff removed from group pool",
        "group_id": payload.group_id,
        "removed":  result.deleted_count,
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
