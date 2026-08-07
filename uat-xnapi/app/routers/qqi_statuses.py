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
router  = APIRouter(prefix="/qqi-statuses", tags=["QQI Statuses"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize(doc: dict) -> dict:
    return {
        "id":         str(doc["_id"]),
        "name":       doc.get("name", ""),
        "code":       doc.get("code"),
        "is_active":  doc.get("is_active", True),
        "created_at": doc["created_at"].isoformat() if doc.get("created_at") and hasattr(doc["created_at"], "isoformat") else None,
        "updated_at": doc["updated_at"].isoformat() if doc.get("updated_at") and hasattr(doc["updated_at"], "isoformat") else None,
    }


class QQIStatusCreate(BaseModel):
    name:      str
    code:      Optional[str] = None
    is_active: bool = True


class QQIStatusUpdate(BaseModel):
    name:      Optional[str]  = None
    code:      Optional[str]  = None
    is_active: Optional[bool] = None


@router.get("/", summary="List all QQI statuses", dependencies=[Depends(verify_api_key)])
async def list_qqi_statuses(request: Request):
    db   = _get_db()
    docs = await db["qqi_statuses"].find({}).sort("name", 1).to_list(500)
    return {"success": True, "total": len(docs), "data": [_serialize(d) for d in docs]}


@router.post("/create", summary="Create a QQI status", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def create_qqi_status(request: Request, payload: QQIStatusCreate):
    db  = _get_db()
    now = datetime.now(timezone.utc)
    existing = await db["qqi_statuses"].find_one(
        {"name": {"$regex": f"^{payload.name.strip()}$", "$options": "i"}}
    )
    if existing:
        raise HTTPException(status_code=409, detail="QQI status already exists")
    doc = {
        "name":      payload.name.strip(),
        "code":      payload.code.strip() if payload.code else None,
        "is_active": payload.is_active,
        "created_at": now,
        "updated_at": now,
    }
    result = await db["qqi_statuses"].insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"success": True, "message": "QQI status created", "data": _serialize(doc)}


@router.patch("/{status_id}", summary="Update a QQI status", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def update_qqi_status(request: Request, status_id: str, payload: QQIStatusUpdate):
    db = _get_db()
    if not ObjectId.is_valid(status_id):
        raise HTTPException(status_code=422, detail="Invalid status_id")
    now    = datetime.now(timezone.utc)
    update = {"updated_at": now}
    if payload.name      is not None: update["name"]      = payload.name.strip()
    if payload.code      is not None: update["code"]      = payload.code.strip()
    if payload.is_active is not None: update["is_active"] = payload.is_active
    result = await db["qqi_statuses"].update_one({"_id": ObjectId(status_id)}, {"$set": update})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="QQI status not found")
    doc = await db["qqi_statuses"].find_one({"_id": ObjectId(status_id)})
    return {"success": True, "message": "QQI status updated", "data": _serialize(doc)}


@router.delete("/{status_id}", summary="Delete a QQI status", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def delete_qqi_status(request: Request, status_id: str):
    db = _get_db()
    if not ObjectId.is_valid(status_id):
        raise HTTPException(status_code=422, detail="Invalid status_id")
    result = await db["qqi_statuses"].delete_one({"_id": ObjectId(status_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="QQI status not found")
    return {"success": True, "message": "QQI status deleted", "id": status_id}
