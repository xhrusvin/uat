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
router = APIRouter(prefix="/outreach-end-reasons", tags=["Outreach End Reasons"])


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


# ── Default reasons from the Figma ───────────────────────────────────────────

DEFAULT_REASONS = [
    {"reason": "Trying a Different Sequence", "sort_order": 1},
    {"reason": "Shift Filled Elsewhere",      "sort_order": 2},
    {"reason": "Shift no longer needed",      "sort_order": 3},
    {"reason": "Pool wasn't right",           "sort_order": 4},
]


async def _seed_defaults(db):
    count = await db["outreach_end_reasons"].count_documents({})
    if count == 0:
        now = datetime.now(timezone.utc)
        docs = [{**r, "is_active": True, "is_default": True,
                 "created_at": now, "updated_at": now}
                for r in DEFAULT_REASONS]
        await db["outreach_end_reasons"].insert_many(docs)


# ── Schemas ───────────────────────────────────────────────────────────────────

class EndReasonCreate(BaseModel):
    reason:     str
    sort_order: Optional[int] = None
    is_active:  bool = True


class EndReasonUpdate(BaseModel):
    reason:     Optional[str] = None
    sort_order: Optional[int] = None
    is_active:  Optional[bool] = None


# ── LIST ──────────────────────────────────────────────────────────────────────

@router.get("/", summary="List all outreach end reasons",
            dependencies=[Depends(verify_api_key)])
@limiter.limit("120/minute")
async def list_end_reasons(request: Request, active_only: bool = False):
    db = _get_db()
    await _seed_defaults(db)
    query = {"is_active": True} if active_only else {}
    docs = await db["outreach_end_reasons"].find(query).sort("sort_order", 1).to_list(200)
    return {"success": True, "total": len(docs), "data": [_serialize(d) for d in docs]}


# ── CREATE ────────────────────────────────────────────────────────────────────

@router.post("/", summary="Create an end reason",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def create_end_reason(request: Request, payload: EndReasonCreate):
    db = _get_db()
    exists = await db["outreach_end_reasons"].find_one(
        {"reason": {"$regex": f"^{payload.reason}$", "$options": "i"}}
    )
    if exists:
        raise HTTPException(status_code=409, detail=f"Reason '{payload.reason}' already exists")

    if payload.sort_order is None:
        last = await db["outreach_end_reasons"].find_one({}, sort=[("sort_order", -1)])
        payload.sort_order = (last.get("sort_order", 0) + 1) if last else 1

    now = datetime.now(timezone.utc)
    doc = {**payload.model_dump(), "is_default": False, "created_at": now, "updated_at": now}
    result = await db["outreach_end_reasons"].insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"success": True, "data": _serialize(doc)}


# ── GET single ────────────────────────────────────────────────────────────────

@router.get("/{reason_id}", summary="Get an end reason by ID",
            dependencies=[Depends(verify_api_key)])
@limiter.limit("120/minute")
async def get_end_reason(request: Request, reason_id: str):
    db = _get_db()
    if not ObjectId.is_valid(reason_id):
        raise HTTPException(status_code=422, detail="Invalid ID")
    doc = await db["outreach_end_reasons"].find_one({"_id": ObjectId(reason_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="End reason not found")
    return {"success": True, "data": _serialize(doc)}


# ── UPDATE ────────────────────────────────────────────────────────────────────

@router.patch("/{reason_id}", summary="Update an end reason",
              dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def update_end_reason(request: Request, reason_id: str, payload: EndReasonUpdate):
    db = _get_db()
    if not ObjectId.is_valid(reason_id):
        raise HTTPException(status_code=422, detail="Invalid ID")
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    updates["updated_at"] = datetime.now(timezone.utc)
    result = await db["outreach_end_reasons"].update_one(
        {"_id": ObjectId(reason_id)}, {"$set": updates}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="End reason not found")
    doc = await db["outreach_end_reasons"].find_one({"_id": ObjectId(reason_id)})
    return {"success": True, "data": _serialize(doc)}


# ── DELETE ────────────────────────────────────────────────────────────────────

@router.delete("/{reason_id}", summary="Delete an end reason (custom only)",
               dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def delete_end_reason(request: Request, reason_id: str):
    db = _get_db()
    if not ObjectId.is_valid(reason_id):
        raise HTTPException(status_code=422, detail="Invalid ID")
    doc = await db["outreach_end_reasons"].find_one({"_id": ObjectId(reason_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="End reason not found")
    if doc.get("is_default"):
        raise HTTPException(status_code=403, detail="Default reasons cannot be deleted")
    await db["outreach_end_reasons"].delete_one({"_id": ObjectId(reason_id)})
    return {"success": True, "message": f"Reason '{doc['reason']}' deleted"}
