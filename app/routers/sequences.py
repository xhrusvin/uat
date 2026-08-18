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
router = APIRouter(prefix="/sequences", tags=["Sequences"])


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


# ── Default sequences from the Figma design ──────────────────────────────────

DEFAULT_SEQUENCES = [
    {
        "name":        "Previously worked here",
        "description": "Staff who've worked here before. Easiest fit, fastest onboarding.",
        "best_for":    "Routine shifts at familiar venues",
        "icon":        "clock",
        "icon_color":  "#6366f1",
        "is_suggested": True,
        "is_active":   True,
        "sort_order":  1,
    },
    {
        "name":        "By rating",
        "description": "Highest-rated staff first. Default for routine shifts.",
        "best_for":    "Routine shifts at familiar venues",
        "icon":        "star",
        "icon_color":  "#f59e0b",
        "is_suggested": False,
        "is_active":   True,
        "sort_order":  2,
    },
    {
        "name":        "By favourites",
        "description": "Client favourites first, then everyone else by rating.",
        "best_for":    "Routine shifts at familiar venues",
        "icon":        "heart",
        "icon_color":  "#22c55e",
        "is_suggested": False,
        "is_active":   True,
        "sort_order":  3,
    },
    {
        "name":        "By distance",
        "description": "Closest staff first. Useful for location-sensitive shifts.",
        "best_for":    "Routine shifts at familiar venues",
        "icon":        "map-pin",
        "icon_color":  "#ec4899",
        "is_suggested": False,
        "is_active":   True,
        "sort_order":  4,
    },
    {
        "name":        "Urgent · cast wide",
        "description": "Client favourites first, then everyone else by rating.",
        "best_for":    "Routine shifts at familiar venues",
        "icon":        "clock-fast",
        "icon_color":  "#10b981",
        "is_suggested": False,
        "is_active":   True,
        "sort_order":  5,
    },
    {
        "name":        "By favourites",
        "description": "Staff not contacted in 30+ days. When other sequences fail.",
        "best_for":    "Routine shifts at familiar venues",
        "icon":        "clipboard",
        "icon_color":  "#f97316",
        "is_suggested": False,
        "is_active":   True,
        "sort_order":  6,
    },
]


async def _seed_defaults(db):
    count = await db["sequences"].count_documents({})
    if count == 0:
        now = datetime.now(timezone.utc)
        docs = [{**s, "is_default": True, "created_at": now, "updated_at": now}
                for s in DEFAULT_SEQUENCES]
        await db["sequences"].insert_many(docs)


# ── Schemas ───────────────────────────────────────────────────────────────────

class SequenceCreate(BaseModel):
    name:        str
    description: Optional[str] = None
    best_for:    Optional[str] = None
    icon:        Optional[str] = None
    icon_color:  Optional[str] = None
    is_suggested: bool = False
    is_active:   bool = True
    sort_order:  Optional[int] = None


class SequenceUpdate(BaseModel):
    name:        Optional[str] = None
    description: Optional[str] = None
    best_for:    Optional[str] = None
    icon:        Optional[str] = None
    icon_color:  Optional[str] = None
    is_suggested: Optional[bool] = None
    is_active:   Optional[bool] = None
    sort_order:  Optional[int] = None


# ── LIST ──────────────────────────────────────────────────────────────────────

@router.get("/", summary="List all sequences",
            dependencies=[Depends(verify_api_key)])
@limiter.limit("120/minute")
async def list_sequences(request: Request, active_only: bool = False):
    db = _get_db()
    await _seed_defaults(db)
    query = {"is_active": True} if active_only else {}
    docs = await db["sequences"].find(query).sort("sort_order", 1).to_list(200)
    return {"success": True, "total": len(docs), "data": [_serialize(d) for d in docs]}


# ── CREATE ────────────────────────────────────────────────────────────────────

@router.post("/", summary="Create a sequence",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def create_sequence(request: Request, payload: SequenceCreate):
    db = _get_db()
    now = datetime.now(timezone.utc)

    # Auto sort_order
    if payload.sort_order is None:
        last = await db["sequences"].find_one({}, sort=[("sort_order", -1)])
        payload.sort_order = (last.get("sort_order", 0) + 1) if last else 1

    doc = {**payload.model_dump(), "is_default": False,
           "created_at": now, "updated_at": now}
    result = await db["sequences"].insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"success": True, "data": _serialize(doc)}


# ── GET single ────────────────────────────────────────────────────────────────

@router.get("/{seq_id}", summary="Get a sequence by ID",
            dependencies=[Depends(verify_api_key)])
@limiter.limit("120/minute")
async def get_sequence(request: Request, seq_id: str):
    db = _get_db()
    if not ObjectId.is_valid(seq_id):
        raise HTTPException(status_code=422, detail="Invalid ID")
    doc = await db["sequences"].find_one({"_id": ObjectId(seq_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Sequence not found")
    return {"success": True, "data": _serialize(doc)}


# ── UPDATE ────────────────────────────────────────────────────────────────────

@router.patch("/{seq_id}", summary="Update a sequence",
              dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def update_sequence(request: Request, seq_id: str, payload: SequenceUpdate):
    db = _get_db()
    if not ObjectId.is_valid(seq_id):
        raise HTTPException(status_code=422, detail="Invalid ID")
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    updates["updated_at"] = datetime.now(timezone.utc)
    result = await db["sequences"].update_one(
        {"_id": ObjectId(seq_id)}, {"$set": updates})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Sequence not found")
    doc = await db["sequences"].find_one({"_id": ObjectId(seq_id)})
    return {"success": True, "data": _serialize(doc)}


# ── DELETE ────────────────────────────────────────────────────────────────────

@router.delete("/{seq_id}", summary="Delete a sequence",
               dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def delete_sequence(request: Request, seq_id: str):
    db = _get_db()
    if not ObjectId.is_valid(seq_id):
        raise HTTPException(status_code=422, detail="Invalid ID")
    doc = await db["sequences"].find_one({"_id": ObjectId(seq_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Sequence not found")
    if doc.get("is_default"):
        raise HTTPException(status_code=403, detail="Default sequences cannot be deleted")
    await db["sequences"].delete_one({"_id": ObjectId(seq_id)})
    return {"success": True, "message": f"Sequence '{doc['name']}' deleted"}
