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
router  = APIRouter(prefix="/user-sub-types", tags=["User Sub Types"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize(doc: dict, user_type_map: dict = None) -> dict:
    ut_id = str(doc.get("user_type_id", "")) if doc.get("user_type_id") else None
    return {
        "id":           str(doc["_id"]),
        "name":         doc.get("name", ""),
        "user_type_id": ut_id,
        "user_type":    user_type_map.get(ut_id) if user_type_map and ut_id else None,
        "is_active":    doc.get("is_active", True),
        "created_at":   doc["created_at"].isoformat() if doc.get("created_at") and hasattr(doc["created_at"], "isoformat") else None,
        "updated_at":   doc["updated_at"].isoformat() if doc.get("updated_at") and hasattr(doc["updated_at"], "isoformat") else None,
    }


class UserSubTypeCreate(BaseModel):
    name:         str
    user_type_id: str
    is_active:    bool = True


class UserSubTypeUpdate(BaseModel):
    name:         Optional[str]  = None
    user_type_id: Optional[str]  = None
    is_active:    Optional[bool] = None


@router.get("/", summary="List all user sub types", dependencies=[Depends(verify_api_key)])
async def list_user_sub_types(request: Request):
    db   = _get_db()
    docs = await db["user_sub_types"].find({}).sort("name", 1).to_list(500)
    # Build user_type name map
    ut_ids = list({str(d["user_type_id"]) for d in docs if d.get("user_type_id")})
    ut_map: dict = {}
    if ut_ids:
        valid_oids = [ObjectId(i) for i in ut_ids if ObjectId.is_valid(i)]
        async for ut in db["user_types"].find({"_id": {"$in": valid_oids}}, {"name": 1}):
            ut_map[str(ut["_id"])] = ut.get("name")
    return {"success": True, "total": len(docs), "data": [_serialize(d, ut_map) for d in docs]}


@router.get("/user-types", summary="Get user types dropdown", dependencies=[Depends(verify_api_key)])
async def get_user_types_dropdown(request: Request):
    db   = _get_db()
    docs = await db["user_types"].find({"is_active": True}).sort("name", 1).to_list(200)
    return {"success": True, "data": [{"id": str(d["_id"]), "name": d.get("name", "")} for d in docs]}


@router.post("/create", summary="Create a user sub type", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def create_user_sub_type(request: Request, payload: UserSubTypeCreate):
    db = _get_db()
    if not ObjectId.is_valid(payload.user_type_id):
        raise HTTPException(status_code=422, detail="Invalid user_type_id")
    ut = await db["user_types"].find_one({"_id": ObjectId(payload.user_type_id)}, {"_id": 1})
    if not ut:
        raise HTTPException(status_code=404, detail="User type not found")
    # Check duplicate
    existing = await db["user_sub_types"].find_one({
        "name":         {"$regex": f"^{payload.name.strip()}$", "$options": "i"},
        "user_type_id": ObjectId(payload.user_type_id),
    })
    if existing:
        raise HTTPException(status_code=409, detail="Sub type already exists for this user type")
    now = datetime.now(timezone.utc)
    doc = {
        "name":         payload.name.strip(),
        "user_type_id": ObjectId(payload.user_type_id),
        "is_active":    payload.is_active,
        "created_at":   now,
        "updated_at":   now,
    }
    result = await db["user_sub_types"].insert_one(doc)
    doc["_id"] = result.inserted_id
    ut_map = {payload.user_type_id: (await db["user_types"].find_one({"_id": ObjectId(payload.user_type_id)}, {"name": 1})).get("name")}
    return {"success": True, "message": "User sub type created", "data": _serialize(doc, ut_map)}


@router.patch("/{sub_type_id}", summary="Update a user sub type", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def update_user_sub_type(request: Request, sub_type_id: str, payload: UserSubTypeUpdate):
    db = _get_db()
    if not ObjectId.is_valid(sub_type_id):
        raise HTTPException(status_code=422, detail="Invalid sub_type_id")
    now    = datetime.now(timezone.utc)
    update = {"updated_at": now}
    if payload.name is not None:
        update["name"] = payload.name.strip()
    if payload.user_type_id is not None:
        if not ObjectId.is_valid(payload.user_type_id):
            raise HTTPException(status_code=422, detail="Invalid user_type_id")
        update["user_type_id"] = ObjectId(payload.user_type_id)
    if payload.is_active is not None:
        update["is_active"] = payload.is_active
    result = await db["user_sub_types"].update_one({"_id": ObjectId(sub_type_id)}, {"$set": update})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User sub type not found")
    doc = await db["user_sub_types"].find_one({"_id": ObjectId(sub_type_id)})
    ut_id = str(doc.get("user_type_id", "")) if doc.get("user_type_id") else None
    ut_map: dict = {}
    if ut_id and ObjectId.is_valid(ut_id):
        ut = await db["user_types"].find_one({"_id": ObjectId(ut_id)}, {"name": 1})
        if ut:
            ut_map[ut_id] = ut.get("name")
    return {"success": True, "message": "User sub type updated", "data": _serialize(doc, ut_map)}


@router.delete("/{sub_type_id}", summary="Delete a user sub type", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def delete_user_sub_type(request: Request, sub_type_id: str):
    db = _get_db()
    if not ObjectId.is_valid(sub_type_id):
        raise HTTPException(status_code=422, detail="Invalid sub_type_id")
    result = await db["user_sub_types"].delete_one({"_id": ObjectId(sub_type_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="User sub type not found")
    return {"success": True, "message": "User sub type deleted", "id": sub_type_id}
