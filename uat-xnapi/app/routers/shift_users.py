import logging
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
router = APIRouter(prefix="/shifts-users", tags=["Shift Users"])


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
    shift = await db["shifts"].find_one({"_id": shift_oid}, {"_id": 1})
    if not shift:
        raise HTTPException(status_code=404, detail=f"Shift {payload.shift_id} not found")

    # Validate user exists
    user = await db["users"].find_one({"_id": user_oid}, {"_id": 1, "first_name": 1, "last_name": 1})
    if not user:
        raise HTTPException(status_code=404, detail=f"User {payload.user_id} not found")

    # Check for duplicate
    existing = await db["shifts_users"].find_one({
        "shift_id": shift_oid,
        "user_id":  user_oid,
    })
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"User {payload.user_id} is already added to shift {payload.shift_id}"
        )

    doc = {
        "user_id":           user_oid,
        "shift_id":          shift_oid,
        "assigned_at":       now,
        "availability":      0,
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

    shift = await db["shifts"].find_one({"_id": shift_oid}, {"_id": 1})
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
            "availability":      0,
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

@router.delete(
    "/{shift_id}/{user_id}",
    summary="Remove a user from a shift",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def remove_user_from_shift(request: Request, shift_id: str, user_id: str):
    db = _get_db()
    shift_oid = _resolve_oid(shift_id, "shift_id")
    user_oid  = _resolve_oid(user_id,  "user_id")

    result = await db["shifts_users"].delete_one({
        "shift_id": shift_oid,
        "user_id":  user_oid,
    })

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="User not found in this shift")

    return {"success": True, "message": "User removed from shift"}
