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
