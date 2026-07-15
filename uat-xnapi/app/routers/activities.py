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


# Tab groupings
ACTIVITY_TAB_MAP = {
    "system": ["round_started", "round_paused", "round_completed", "round_ended"],
    "ai":     ["ai_call_placed", "ai_whatsapp_sent", "available_response"],
    "people": [],   # non-system, non-AI activity types (custom or manually logged)
}


class ActivityListRequest(BaseModel):
    shift_id:      Optional[str] = None
    outreach_id:   Optional[str] = None
    group_id:      Optional[str] = None   # for outreach_group activities
    activity_type: Optional[str] = None
    tab:           Optional[str] = None  # "all" | "system" | "ai" | "people"
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
    if payload.group_id and ObjectId.is_valid(payload.group_id):
        filters.append({"group_id": ObjectId(payload.group_id)})
    if payload.activity_type:
        filters.append({"activity_type": payload.activity_type})

    # Tab filter
    tab = (payload.tab or "all").lower()
    if tab == "system":
        filters.append({"activity_type": {"$in": ACTIVITY_TAB_MAP["system"]}})
    elif tab == "ai":
        filters.append({"activity_type": {"$in": ACTIVITY_TAB_MAP["ai"]}})
    elif tab == "people":
        all_known = ACTIVITY_TAB_MAP["system"] + ACTIVITY_TAB_MAP["ai"]
        filters.append({"activity_type": {"$nin": all_known}})

    base_filter  = {"$and": filters[:-1]} if len(filters) > 1 else (filters[0] if len(filters) == 1 and tab == "all" else {})
    mongo_filter = {"$and": filters} if filters else {}
    skip  = (payload.page - 1) * payload.per_page
    total = await db["activities"].count_documents(mongo_filter)

    # Tab counts (always based on shift_id/outreach_id filter, not tab)
    count_base = {}
    if payload.shift_id and ObjectId.is_valid(payload.shift_id):
        count_base["shift_id"] = ObjectId(payload.shift_id)
    if payload.outreach_id and ObjectId.is_valid(payload.outreach_id):
        count_base["outreach_id"] = ObjectId(payload.outreach_id)
    if payload.group_id and ObjectId.is_valid(payload.group_id):
        count_base["group_id"] = ObjectId(payload.group_id)

    count_all    = await db["activities"].count_documents(count_base)
    count_system = await db["activities"].count_documents({**count_base, "activity_type": {"$in": ACTIVITY_TAB_MAP["system"]}})
    count_ai     = await db["activities"].count_documents({**count_base, "activity_type": {"$in": ACTIVITY_TAB_MAP["ai"]}})
    count_people = await db["activities"].count_documents({**count_base, "activity_type": {"$nin": ACTIVITY_TAB_MAP["system"] + ACTIVITY_TAB_MAP["ai"]}})
    
    docs  = await db["activities"].find(mongo_filter) \
                                  .sort("created_at", -1) \
                                  .skip(skip).limit(payload.per_page) \
                                  .to_list(payload.per_page)

    results = []
    for d in docs:
        s = _serialize(d)
        meta = d.get("metadata") or {}
        # Build display summary like image: "Round 1 paused · 0 available, 2 declined, 4 no-reply"
        rn        = meta.get("round_number", 1)
        available = meta.get("available", 0)
        declined  = meta.get("declined", 0)
        no_reply  = meta.get("no_reply", 0)
        atype     = d.get("activity_type", "")
        label_map = {
            "round_paused":       f"Round {rn} paused",
            "round_completed":    f"Round {rn} completed",
            "round_started":      f"Round {rn} started",
            "round_ended":        f"Round {rn} ended",
            "available_response": "Available response",
            "ai_call_placed":     "AI call placed",
            "ai_whatsapp_sent":   "AI WhatsApp sent",
        }
        label = label_map.get(atype, atype.replace("_", " ").title())
        if available is not None and declined is not None and no_reply is not None:
            s["display_title"]   = label
            s["display_summary"] = f"{available} available, {declined} declined, {no_reply} no-reply"
        else:
            s["display_title"]   = label
            s["display_summary"] = meta.get("summary") or ""

        # Icon — from activity_types collection or inline map
        icon_map = {
            "round_paused":    "pause",
            "round_completed": "check",
            "round_started":   "refresh",
            "round_ended":     "stop",
            "available_response": "check-circle",
            "ai_call_placed":  "phone",
            "ai_whatsapp_sent":"message",
        }
        s["icon"] = icon_map.get(atype, "check")

        # Ensure key fields are always present
        s["id"]            = str(d["_id"])
        s["activity_type"] = atype
        s["shift_id"]      = str(d["shift_id"]) if d.get("shift_id") else None
        s["created_at"]    = d["created_at"].isoformat() if hasattr(d.get("created_at"), "isoformat") else str(d.get("created_at", ""))

        # time_ago from created_at
        time_ago = None
        raw_dt   = d.get("created_at")
        if raw_dt and hasattr(raw_dt, "tzinfo"):
            from datetime import timezone as _tz
            if raw_dt.tzinfo is None:
                raw_dt = raw_dt.replace(tzinfo=_tz.utc)
            diff = int((datetime.now(_tz.utc) - raw_dt).total_seconds())
            if diff < 60:       time_ago = "just now"
            elif diff < 3600:   time_ago = f"{diff//60} minute{'s' if diff//60!=1 else ''} ago"
            elif diff < 86400:  time_ago = f"{diff//3600} hour{'s' if diff//3600!=1 else ''} ago"
            else:               time_ago = f"{diff//86400} day{'s' if diff//86400!=1 else ''} ago"
        s["time_ago"] = time_ago

        results.append(s)

    return {
        "success":  True,
        "total":    total,
        "page":     payload.page,
        "per_page": payload.per_page,
        "tab":      tab,
        "tabs": {
            "all":    count_all,
            "system": count_system,
            "ai":     count_ai,
            "people": count_people,
        },
        "data":     results,
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
