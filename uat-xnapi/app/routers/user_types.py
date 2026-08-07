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
router = APIRouter(prefix="/user-types", tags=["User Types"])


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
        else:
            result[key] = v
    return result


# ── Default user types ────────────────────────────────────────────────────────

DEFAULT_USER_TYPES = [
    {"name": "Nurse",                  "description": "Registered nurse"},
    {"name": "Healthcare Assistant",   "description": "HCA / care assistant"},
    {"name": "Pharmacist",             "description": "Registered pharmacist"},
    {"name": "Pharmacy Technician",    "description": "Pharmacy technician"},
    {"name": "Doctor",                 "description": "Medical doctor / physician"},
    {"name": "Midwife",                "description": "Registered midwife"},
    {"name": "Paramedic",              "description": "Paramedic / emergency care"},
    {"name": "Social Worker",          "description": "Qualified social worker"},
    {"name": "Occupational Therapist", "description": "OT / occupational therapist"},
    {"name": "Physiotherapist",        "description": "Physiotherapist"},
]


async def _seed_defaults(db):
    count = await db["user_types"].count_documents({})
    if count == 0:
        now = datetime.now(timezone.utc)
        docs = [{**t, "is_active": True, "is_default": True,
                 "sort_order": i + 1, "created_at": now, "updated_at": now}
                for i, t in enumerate(DEFAULT_USER_TYPES)]
        await db["user_types"].insert_many(docs)


# ── Schemas ───────────────────────────────────────────────────────────────────


class UserTypeListRequest(BaseModel):
    search:   str = ""
    page:     int = 1
    per_page: int = 20


class UserTypeCreate(BaseModel):
    name:        str
    description: Optional[str] = None
    is_active:   bool = True
    sort_order:  Optional[int] = None


class UserTypeUpdate(BaseModel):
    name:        Optional[str] = None
    description: Optional[str] = None
    is_active:   Optional[bool] = None
    sort_order:  Optional[int] = None


# ── LIST ──────────────────────────────────────────────────────────────────────

@router.post(
    "/",
    summary="List user types with search and pagination",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("120/minute")
async def list_user_types(request: Request, payload: UserTypeListRequest):
    """
    Body: { "search": "", "page": 1, "per_page": 20 }
    Seeds 10 default user types on first call.
    """
    db = _get_db()
    await _seed_defaults(db)

    skip = (payload.page - 1) * payload.per_page
    query = {}
    if payload.search:
        query["name"] = {"$regex": payload.search, "$options": "i"}

    total = await db["user_types"].count_documents(query)
    docs  = await db["user_types"].find(query).sort("sort_order", 1)                                   .skip(skip).limit(payload.per_page).to_list(payload.per_page)

    return {
        "success":  True,
        "total":    total,
        "page":     payload.page,
        "per_page": payload.per_page,
        "data":     [_serialize(d) for d in docs],
    }


# ── CREATE ────────────────────────────────────────────────────────────────────

@router.post(
    "/create",
    summary="Create a user type",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def create_user_type(request: Request, payload: UserTypeCreate):
    db = _get_db()

    exists = await db["user_types"].find_one(
        {"name": {"$regex": f"^{payload.name}$", "$options": "i"}}
    )
    if exists:
        raise HTTPException(status_code=409, detail=f"User type '{payload.name}' already exists")

    if payload.sort_order is None:
        last = await db["user_types"].find_one({}, sort=[("sort_order", -1)])
        payload.sort_order = (last.get("sort_order", 0) + 1) if last else 1

    now = datetime.now(timezone.utc)
    doc = {**payload.model_dump(), "is_default": False, "created_at": now, "updated_at": now}
    result = await db["user_types"].insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"success": True, "data": _serialize(doc)}


# ── GET single ────────────────────────────────────────────────────────────────

# ── GET /user-types/sync-from-upstream ───────────────────────────────────────

@router.get(
    "/sync-from-upstream",
    summary="Fetch user types from upstream and sync to user_types collection",
    dependencies=[Depends(verify_api_key)],
)
async def sync_user_types_from_upstream(request: Request):
    import httpx as _httpx
    from app.db.database import _client
    from datetime import datetime, timezone

    url = f"{settings.USER_API_URL.rstrip('/')}/ai/common/user-type-list"
    headers = {
        "Api-Key":      settings.USER_INTERNAL_API_KEY,
        "Content-Type": "application/json",
        "Accept":       "application/json",
    }

    async with _httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers=headers)

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Upstream failed: {resp.status_code}")

    body      = resp.json()
    data_list = body.get("data") or []
    db        = _client[settings.MONGODB_DB]
    now       = datetime.now(timezone.utc)

    updated = inserted = 0
    results = []

    async with _httpx.AsyncClient(timeout=30.0) as client:
        for item in data_list:
            xn_id = str(item.get("_id") or "").strip()
            name  = (item.get("name") or "").strip()
            if not xn_id or not name:
                continue

            # ── Upsert user type ──────────────────────────────────────────────
            local_ut_id = None
            existing = await db["user_types"].find_one({"xn_id": xn_id})
            if existing:
                await db["user_types"].update_one(
                    {"xn_id": xn_id},
                    {"$set": {"name": name, "xn_id": xn_id, "updated_at": now}}
                )
                local_ut_id = existing["_id"]
                results.append({"xn_id": xn_id, "name": name, "action": "updated"})
                updated += 1
            else:
                existing_name = await db["user_types"].find_one({"name": {"$regex": f"^{name}$", "$options": "i"}})
                if existing_name:
                    await db["user_types"].update_one(
                        {"_id": existing_name["_id"]},
                        {"$set": {"xn_id": xn_id, "updated_at": now}}
                    )
                    local_ut_id = existing_name["_id"]
                    results.append({"xn_id": xn_id, "name": name, "action": "matched_by_name"})
                    updated += 1
                else:
                    result = await db["user_types"].insert_one({
                        "name":       name,
                        "xn_id":      xn_id,
                        "is_active":  True,
                        "is_default": False,
                        "sort_order": 99,
                        "created_at": now,
                        "updated_at": now,
                    })
                    local_ut_id = result.inserted_id
                    results.append({"xn_id": xn_id, "name": name, "action": "inserted"})
                    inserted += 1

            # sub types synced separately via /user-types/sync-sub-types/{xn_id}

    return {
        "success":         True,
        "upstream_status": resp.status_code,
        "total":           len(data_list),
        "updated":         updated,
        "inserted":        inserted,
        "results":         results,
    }


@router.get(
    "/{type_id}",
    summary="Get a user type by ID",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("120/minute")
async def get_user_type(request: Request, type_id: str):
    db = _get_db()
    if not ObjectId.is_valid(type_id):
        raise HTTPException(status_code=422, detail="Invalid ID")
    doc = await db["user_types"].find_one({"_id": ObjectId(type_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="User type not found")
    return {"success": True, "data": _serialize(doc)}


# ── UPDATE ────────────────────────────────────────────────────────────────────

@router.patch(
    "/{type_id}",
    summary="Update a user type",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def update_user_type(request: Request, type_id: str, payload: UserTypeUpdate):
    db = _get_db()
    if not ObjectId.is_valid(type_id):
        raise HTTPException(status_code=422, detail="Invalid ID")

    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    updates["updated_at"] = datetime.now(timezone.utc)
    result = await db["user_types"].update_one(
        {"_id": ObjectId(type_id)}, {"$set": updates}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User type not found")

    doc = await db["user_types"].find_one({"_id": ObjectId(type_id)})
    return {"success": True, "data": _serialize(doc)}


# ── DELETE ────────────────────────────────────────────────────────────────────

@router.delete(
    "/{type_id}",
    summary="Delete a user type (custom only)",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def delete_user_type(request: Request, type_id: str):
    db = _get_db()
    if not ObjectId.is_valid(type_id):
        raise HTTPException(status_code=422, detail="Invalid ID")

    doc = await db["user_types"].find_one({"_id": ObjectId(type_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="User type not found")
    if doc.get("is_default"):
        raise HTTPException(status_code=403, detail="Default user types cannot be deleted")

    await db["user_types"].delete_one({"_id": ObjectId(type_id)})
    return {"success": True, "message": f"User type '{doc['name']}' deleted"}


# ── County list ───────────────────────────────────────────────────────────────

class CountyListRequest(BaseModel):
    search:   str = ""
    page:     int = 1
    per_page: int = 20


county_router = APIRouter(prefix="/county", tags=["County"])


@county_router.post(
    "/",
    summary="List counties with search and pagination",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("120/minute")
async def list_counties(request: Request, payload: CountyListRequest):
    """
    Body: { "search": "", "page": 1, "per_page": 20 }
    Returns counties from the county collection.
    """
    db   = _get_db()
    skip = (payload.page - 1) * payload.per_page

    query = {}
    if payload.search:
        query["name"] = {"$regex": payload.search, "$options": "i"}

    total = await db["county"].count_documents(query)
    docs  = await db["county"].find(query).sort("name", 1) \
                              .skip(skip).limit(payload.per_page).to_list(payload.per_page)

    return {
        "success":  True,
        "total":    total,
        "page":     payload.page,
        "per_page": payload.per_page,
        "data":     [_serialize(d) for d in docs],
    }


# ── GET /user-types/sync-sub-types/{xn_id} ───────────────────────────────────

@router.get(
    "/sync-sub-types/{xn_id}",
    summary="Fetch sub types for a user type from upstream",
    dependencies=[Depends(verify_api_key)],
)
async def sync_sub_types(request: Request, xn_id: str):
    import httpx as _httpx
    from app.db.database import _client
    from datetime import datetime, timezone

    db  = _client[settings.MONGODB_DB]
    now = datetime.now(timezone.utc)

    # Find local user_type by xn_id
    ut = await db["user_types"].find_one({"xn_id": xn_id}, {"_id": 1, "name": 1})
    if not ut:
        raise HTTPException(status_code=404, detail=f"User type with xn_id={xn_id} not found locally")

    local_ut_id = ut["_id"]

    url = f"{settings.USER_API_URL.rstrip('/')}/ai/common/user-sub-type-list"
    headers = {
        "Api-Key":      settings.USER_INTERNAL_API_KEY,
        "Content-Type": "application/json",
        "Accept":       "application/json",
    }

    async with _httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json={"user_type_id": xn_id}, headers=headers)

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Upstream failed: {resp.status_code}")

    sub_list    = resp.json().get("data") or []
    updated = inserted = 0
    results = []

    for sub in sub_list:
        sub_name  = (sub.get("name") or "").strip()
        sub_xn_id = str(sub.get("_id") or "").strip()
        if not sub_name:
            continue
        existing = await db["user_sub_types"].find_one({
            "user_type_id": local_ut_id,
            "name": {"$regex": f"^{sub_name}$", "$options": "i"}
        })
        if existing:
            await db["user_sub_types"].update_one(
                {"_id": existing["_id"]},
                {"$set": {"xn_id": sub_xn_id, "updated_at": now}}
            )
            results.append({"name": sub_name, "action": "updated"})
            updated += 1
        else:
            await db["user_sub_types"].insert_one({
                "name":         sub_name,
                "xn_id":        sub_xn_id,
                "user_type_id": local_ut_id,
                "is_active":    True,
                "created_at":   now,
                "updated_at":   now,
            })
            results.append({"name": sub_name, "action": "inserted"})
            inserted += 1

    return {
        "success":      True,
        "user_type":    ut.get("name"),
        "xn_id":        xn_id,
        "total":        len(sub_list),
        "updated":      updated,
        "inserted":     inserted,
        "results":      results,
    }
