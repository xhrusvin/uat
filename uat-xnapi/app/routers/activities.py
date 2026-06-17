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
router = APIRouter(prefix="/activities", tags=["Activities"])


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


# ── Activity types ─────────────────────────────────────────────────────────────

ACTIVITY_TYPES = [
    {
        "key":         "round_paused",
        "label":       "Round Paused",
        "description": "A calling round was paused",
        "icon":        "pause",
        "color":       "#f59e0b",
    },
    {
        "key":         "available_response",
        "label":       "Available Response",
        "description": "A staff member responded as available",
        "icon":        "check-circle",
        "color":       "#22c55e",
    },
    {
        "key":         "ai_call_placed",
        "label":       "AI Call Placed",
        "description": "An AI phone call was placed to a staff member",
        "icon":        "phone",
        "color":       "#6366f1",
    },
    {
        "key":         "ai_whatsapp_sent",
        "label":       "AI WhatsApp Sent",
        "description": "An AI WhatsApp message was sent to a staff member",
        "icon":        "message",
        "color":       "#25d366",
    },
    {
        "key":         "round_started",
        "label":       "Round Started",
        "description": "A new calling round was started",
        "icon":        "refresh",
        "color":       "#3b82f6",
    },
    {
        "key":         "round_completed",
        "label":       "Round Completed",
        "description": "A calling round was completed",
        "icon":        "check",
        "color":       "#8b5cf6",
    },
    {
        "key":         "round_ended",
        "label":       "Round Ended",
        "description": "A calling round ended",
        "icon":        "stop",
        "color":       "#ef4444",
    },
]

# ── Default seed ───────────────────────────────────────────────────────────────

async def _seed_defaults(db):
    count = await db["activity_types"].count_documents({})
    if count == 0:
        now = datetime.now(timezone.utc)
        docs = [{**t, "is_active": True, "is_default": True,
                 "created_at": now, "updated_at": now}
                for t in ACTIVITY_TYPES]
        await db["activity_types"].insert_many(docs)


# ── Schemas ────────────────────────────────────────────────────────────────────

class ActivityTypeCreate(BaseModel):
    key:         str
    label:       str
    description: Optional[str] = None
    icon:        Optional[str] = None
    color:       Optional[str] = None
    is_active:   bool = True


class ActivityTypeUpdate(BaseModel):
    label:       Optional[str] = None
    description: Optional[str] = None
    icon:        Optional[str] = None
    color:       Optional[str] = None
    is_active:   Optional[bool] = None


class ActivityCreate(BaseModel):
    activity_type: str            # key e.g. "sequence_started"
    shift_id:      Optional[str] = None
    outreach_id:   Optional[str] = None
    user_id:       Optional[str] = None
    metadata:      Optional[dict] = None


class ActivityListRequest(BaseModel):
    shift_id:      Optional[str] = None
    outreach_id:   Optional[str] = None
    activity_type: Optional[str] = None
    page:          int = 1
    per_page:      int = 20


# ── Activity Types CRUD ────────────────────────────────────────────────────────

@router.get("/types", summary="List activity types",
            dependencies=[Depends(verify_api_key)])
@limiter.limit("120/minute")
async def list_activity_types(request: Request):
    db = _get_db()
    await _seed_defaults(db)
    docs = await db["activity_types"].find({}).sort("key", 1).to_list(100)
    return {"success": True, "total": len(docs), "data": [_serialize(d) for d in docs]}


@router.post("/types", summary="Create an activity type",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def create_activity_type(request: Request, payload: ActivityTypeCreate):
    db = _get_db()
    exists = await db["activity_types"].find_one({"key": payload.key})
    if exists:
        raise HTTPException(status_code=409, detail=f"Activity type '{payload.key}' already exists")
    now = datetime.now(timezone.utc)
    doc = {**payload.model_dump(), "is_default": False, "created_at": now, "updated_at": now}
    result = await db["activity_types"].insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"success": True, "data": _serialize(doc)}


@router.patch("/types/{type_id}", summary="Update an activity type",
              dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def update_activity_type(request: Request, type_id: str, payload: ActivityTypeUpdate):
    db = _get_db()
    if not ObjectId.is_valid(type_id):
        raise HTTPException(status_code=422, detail="Invalid ID")
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")
    updates["updated_at"] = datetime.now(timezone.utc)
    result = await db["activity_types"].update_one({"_id": ObjectId(type_id)}, {"$set": updates})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Activity type not found")
    doc = await db["activity_types"].find_one({"_id": ObjectId(type_id)})
    return {"success": True, "data": _serialize(doc)}


@router.delete("/types/{type_id}", summary="Delete an activity type (custom only)",
               dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def delete_activity_type(request: Request, type_id: str):
    db = _get_db()
    if not ObjectId.is_valid(type_id):
        raise HTTPException(status_code=422, detail="Invalid ID")
    doc = await db["activity_types"].find_one({"_id": ObjectId(type_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Activity type not found")
    if doc.get("is_default"):
        raise HTTPException(status_code=403, detail="Default activity types cannot be deleted")
    await db["activity_types"].delete_one({"_id": ObjectId(type_id)})
    return {"success": True, "message": f"Activity type '{doc['key']}' deleted"}


# ── Activities CRUD ────────────────────────────────────────────────────────────

@router.post("/list", summary="List activities with filters and pagination",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("60/minute")
async def list_activities(request: Request, payload: ActivityListRequest):
    """
    Body: { "shift_id": null, "outreach_id": null, "activity_type": null, "page": 1, "per_page": 20 }
    """
    db = _get_db()
    filters: list = []

    if payload.shift_id and ObjectId.is_valid(payload.shift_id):
        filters.append({"shift_id": ObjectId(payload.shift_id)})
    if payload.outreach_id and ObjectId.is_valid(payload.outreach_id):
        filters.append({"outreach_id": ObjectId(payload.outreach_id)})
    if payload.activity_type:
        filters.append({"activity_type": payload.activity_type})

    mongo_filter = {"$and": filters} if filters else {}
    skip  = (payload.page - 1) * payload.per_page
    total = await db["activities"].count_documents(mongo_filter)
    docs  = await db["activities"].find(mongo_filter) \
                                  .sort("created_at", -1) \
                                  .skip(skip).limit(payload.per_page) \
                                  .to_list(payload.per_page)

    return {
        "success":  True,
        "total":    total,
        "page":     payload.page,
        "per_page": payload.per_page,
        "data":     [_serialize(d) for d in docs],
    }


@router.post("/", summary="Create an activity log entry",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("120/minute")
async def create_activity(request: Request, payload: ActivityCreate):
    """
    Body: { "activity_type": "sequence_started", "shift_id": "...", "outreach_id": "...", "user_id": "...", "metadata": {} }
    """
    db  = _get_db()
    now = datetime.now(timezone.utc)

    doc: dict = {
        "activity_type": payload.activity_type,
        "metadata":      payload.metadata or {},
        "created_at":    now,
    }
    if payload.shift_id and ObjectId.is_valid(payload.shift_id):
        doc["shift_id"] = ObjectId(payload.shift_id)
    if payload.outreach_id and ObjectId.is_valid(payload.outreach_id):
        doc["outreach_id"] = ObjectId(payload.outreach_id)
    if payload.user_id and ObjectId.is_valid(payload.user_id):
        doc["user_id"] = ObjectId(payload.user_id)

    result = await db["activities"].insert_one(doc)
    doc["_id"] = result.inserted_id

    logger.info(f"Activity: {payload.activity_type} shift={payload.shift_id}")
    return {"success": True, "data": _serialize(doc)}


@router.get("/{activity_id}", summary="Get a single activity",
            dependencies=[Depends(verify_api_key)])
@limiter.limit("120/minute")
async def get_activity(request: Request, activity_id: str):
    db = _get_db()
    if not ObjectId.is_valid(activity_id):
        raise HTTPException(status_code=422, detail="Invalid ID")
    doc = await db["activities"].find_one({"_id": ObjectId(activity_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Activity not found")
    return {"success": True, "data": _serialize(doc)}


@router.delete("/{activity_id}", summary="Delete an activity log entry",
               dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def delete_activity(request: Request, activity_id: str):
    db = _get_db()
    if not ObjectId.is_valid(activity_id):
        raise HTTPException(status_code=422, detail="Invalid ID")
    result = await db["activities"].delete_one({"_id": ObjectId(activity_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Activity not found")
    return {"success": True, "message": "Activity deleted"}
