import logging
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/criteria", tags=["Criteria"])


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


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class CriteriaCreate(BaseModel):
    label:       str                    # Display name e.g. "User Type"
    field:       str                    # DB field e.g. "user_type"
    description: Optional[str] = None
    is_active:   bool = True


class CriteriaUpdate(BaseModel):
    label:       Optional[str] = None
    field:       Optional[str] = None
    description: Optional[str] = None
    is_active:   Optional[bool] = None


# ── Seed defaults ─────────────────────────────────────────────────────────────

DEFAULT_CRITERIA = [
    {"label": "User Type",         "field": "user_type",         "description": "Filter by user/role type"},
    {"label": "Automation Status", "field": "automation_status", "description": "Filter by automation/shift status"},
    {"label": "County",            "field": "client_county",     "description": "Filter by county"},
    {"label": "Client",            "field": "location",          "description": "Filter by client/location name"},
    {"label": "Client Tags",       "field": "client_tags",       "description": "Filter by client tags"},
    {"label": "Shift Time",        "field": "shift_timing",      "description": "Filter by shift time/timing"},
    {"label": "Has Available",     "field": "assigned_staff",    "description": "Filter by staff availability"},
    {"label": "Shift Type",        "field": "shift_type",        "description": "Filter by shift type (Day/Night)"},
    {"label": "Distance",          "field": "distance",          "description": "Filter by distance"},
]


async def _seed_defaults(db):
    """Insert default criteria if collection is empty."""
    count = await db["criteria"].count_documents({})
    if count == 0:
        now = datetime.now(timezone.utc)
        docs = [{**c, "is_active": True, "is_default": True,
                 "created_at": now, "updated_at": now}
                for c in DEFAULT_CRITERIA]
        await db["criteria"].insert_many(docs)


# ── LIST ──────────────────────────────────────────────────────────────────────

@router.get("/", summary="List all shift filter criteria",
            dependencies=[Depends(verify_api_key)])
@limiter.limit("120/minute")
async def list_criteria(
    request: Request,
    active_only: bool = Query(False),
):
    db = _get_db()
    await _seed_defaults(db)
    query = {"is_active": True} if active_only else {}
    docs = await db["criteria"].find(query).sort("label", 1).to_list(500)
    return {"success": True, "total": len(docs), "data": [_serialize(d) for d in docs]}


# ── CREATE ────────────────────────────────────────────────────────────────────

@router.post("/", summary="Create a new criteria",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def create_criteria(request: Request, payload: CriteriaCreate):
    db = _get_db()

    # Prevent duplicate labels
    exists = await db["criteria"].find_one({"label": {"$regex": f"^{payload.label}$", "$options": "i"}})
    if exists:
        raise HTTPException(status_code=409, detail=f"Criteria '{payload.label}' already exists")

    now = datetime.now(timezone.utc)
    doc = {
        **payload.model_dump(),
        "is_default":  False,
        "created_at":  now,
        "updated_at":  now,
    }
    result = await db["criteria"].insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"success": True, "data": _serialize(doc)}


# ── GET single ────────────────────────────────────────────────────────────────

@router.get("/{criteria_id}", summary="Get a criteria by ID",
            dependencies=[Depends(verify_api_key)])
@limiter.limit("120/minute")
async def get_criteria(request: Request, criteria_id: str):
    db = _get_db()
    if not ObjectId.is_valid(criteria_id):
        raise HTTPException(status_code=422, detail="Invalid ID")
    doc = await db["criteria"].find_one({"_id": ObjectId(criteria_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Criteria not found")
    return {"success": True, "data": _serialize(doc)}


# ── UPDATE ────────────────────────────────────────────────────────────────────

@router.patch("/{criteria_id}", summary="Update a criteria",
              dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def update_criteria(request: Request, criteria_id: str, payload: CriteriaUpdate):
    db = _get_db()
    if not ObjectId.is_valid(criteria_id):
        raise HTTPException(status_code=422, detail="Invalid ID")

    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    updates["updated_at"] = datetime.now(timezone.utc)
    result = await db["criteria"].update_one(
        {"_id": ObjectId(criteria_id)},
        {"$set": updates}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Criteria not found")

    doc = await db["criteria"].find_one({"_id": ObjectId(criteria_id)})
    return {"success": True, "data": _serialize(doc)}


# ── DELETE ────────────────────────────────────────────────────────────────────

@router.delete("/{criteria_id}", summary="Delete a criteria",
               dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def delete_criteria(request: Request, criteria_id: str):
    db = _get_db()
    if not ObjectId.is_valid(criteria_id):
        raise HTTPException(status_code=422, detail="Invalid ID")

    doc = await db["criteria"].find_one({"_id": ObjectId(criteria_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Criteria not found")
    if doc.get("is_default"):
        raise HTTPException(status_code=403, detail="Default criteria cannot be deleted")

    await db["criteria"].delete_one({"_id": ObjectId(criteria_id)})
    return {"success": True, "message": f"Criteria '{doc['label']}' deleted"}
