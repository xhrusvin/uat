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
router  = APIRouter(prefix="/visa-types", tags=["Visa Types"])


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


class VisaTypeCreate(BaseModel):
    name: str


class VisaTypeUpdate(BaseModel):
    name: str


@router.get("/", summary="List all visa types", dependencies=[Depends(verify_api_key)])
async def list_visa_types(request: Request):
    db   = _get_db()
    docs = await db["visa_types"].find({}).sort("name", 1).to_list(200)
    return {"success": True, "total": len(docs), "data": [_serialize(d) for d in docs]}


@router.post("/create", summary="Create a visa type", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def create_visa_type(request: Request, payload: VisaTypeCreate):
    db  = _get_db()
    now = datetime.now(timezone.utc)
    existing = await db["visa_types"].find_one({"name": {"$regex": f"^{payload.name.strip()}$", "$options": "i"}})
    if existing:
        raise HTTPException(status_code=409, detail="Visa type already exists")
    doc = {"name": payload.name.strip(), "created_at": now, "updated_at": now}
    result = await db["visa_types"].insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"success": True, "message": "Visa type created", "data": _serialize(doc)}


@router.patch("/{visa_type_id}", summary="Update a visa type", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def update_visa_type(request: Request, visa_type_id: str, payload: VisaTypeUpdate):
    db = _get_db()
    if not ObjectId.is_valid(visa_type_id):
        raise HTTPException(status_code=422, detail="Invalid visa_type_id")
    now = datetime.now(timezone.utc)
    result = await db["visa_types"].update_one(
        {"_id": ObjectId(visa_type_id)},
        {"$set": {"name": payload.name.strip(), "updated_at": now}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Visa type not found")
    doc = await db["visa_types"].find_one({"_id": ObjectId(visa_type_id)})
    return {"success": True, "message": "Visa type updated", "data": _serialize(doc)}


@router.delete("/{visa_type_id}", summary="Delete a visa type", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def delete_visa_type(request: Request, visa_type_id: str):
    db = _get_db()
    if not ObjectId.is_valid(visa_type_id):
        raise HTTPException(status_code=422, detail="Invalid visa_type_id")
    result = await db["visa_types"].delete_one({"_id": ObjectId(visa_type_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Visa type not found")
    return {"success": True, "message": "Visa type deleted", "id": visa_type_id}
