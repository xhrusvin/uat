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
router  = APIRouter(prefix="/genders", tags=["Genders"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize(doc: dict) -> dict:
    return {
        "id":         str(doc["_id"]),
        "name":       doc.get("name", ""),
        "created_at": doc["created_at"].isoformat() if doc.get("created_at") and hasattr(doc["created_at"], "isoformat") else None,
        "updated_at": doc["updated_at"].isoformat() if doc.get("updated_at") and hasattr(doc["updated_at"], "isoformat") else None,
    }


class GenderCreate(BaseModel):
    name: str


class GenderUpdate(BaseModel):
    name: str


@router.get("/", summary="List all genders", dependencies=[Depends(verify_api_key)])
async def list_genders(request: Request):
    db   = _get_db()
    docs = await db["genders"].find({}).sort("name", 1).to_list(100)
    return {"success": True, "total": len(docs), "data": [_serialize(d) for d in docs]}


@router.post("/create", summary="Create a gender", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def create_gender(request: Request, payload: GenderCreate):
    db  = _get_db()
    now = datetime.now(timezone.utc)
    # Check duplicate
    existing = await db["genders"].find_one({"name": {"$regex": f"^{payload.name.strip()}$", "$options": "i"}})
    if existing:
        raise HTTPException(status_code=409, detail="Gender already exists")
    doc = {"name": payload.name.strip(), "created_at": now, "updated_at": now}
    result = await db["genders"].insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"success": True, "message": "Gender created", "data": _serialize(doc)}


@router.patch("/{gender_id}", summary="Update a gender", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def update_gender(request: Request, gender_id: str, payload: GenderUpdate):
    db = _get_db()
    if not ObjectId.is_valid(gender_id):
        raise HTTPException(status_code=422, detail="Invalid gender_id")
    now = datetime.now(timezone.utc)
    result = await db["genders"].update_one(
        {"_id": ObjectId(gender_id)},
        {"$set": {"name": payload.name.strip(), "updated_at": now}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Gender not found")
    doc = await db["genders"].find_one({"_id": ObjectId(gender_id)})
    return {"success": True, "message": "Gender updated", "data": _serialize(doc)}


@router.delete("/{gender_id}", summary="Delete a gender", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def delete_gender(request: Request, gender_id: str):
    db = _get_db()
    if not ObjectId.is_valid(gender_id):
        raise HTTPException(status_code=422, detail="Invalid gender_id")
    result = await db["genders"].delete_one({"_id": ObjectId(gender_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Gender not found")
    return {"success": True, "message": "Gender deleted", "id": gender_id}
