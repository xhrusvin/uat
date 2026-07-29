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
router  = APIRouter(prefix="/prompts", tags=["Prompts"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize(doc: dict) -> dict:
    return {
        "id":                 str(doc["_id"]),
        "document_type_code": doc.get("document_type_code", ""),
        "prompt_text":        doc.get("prompt_text", ""),
        "version":            doc.get("version", 1),
        "is_active":          doc.get("is_active", True),
        "level":              doc.get("level", 1),
        "created_at":         doc["created_at"].isoformat() if doc.get("created_at") and hasattr(doc["created_at"], "isoformat") else None,
        "updated_at":         doc["updated_at"].isoformat() if doc.get("updated_at") and hasattr(doc["updated_at"], "isoformat") else None,
    }


class PromptListRequest(BaseModel):
    search:   Optional[str] = None
    page:     int = 1
    per_page: int = 20


class PromptCreate(BaseModel):
    document_type_code: str
    prompt_text:        str
    version:            int = 1
    is_active:          bool = True
    level:              Optional[int] = None  # 1-5, optional


class PromptUpdate(BaseModel):
    document_type_code: Optional[str] = None
    prompt_text:        Optional[str] = None
    version:            Optional[int] = None
    is_active:          Optional[bool] = None
    level:              Optional[int] = None  # 1-5


@router.post("/", summary="List prompts", dependencies=[Depends(verify_api_key)])
@limiter.limit("60/minute")
async def list_prompts(request: Request, payload: PromptListRequest):
    db     = _get_db()
    filt: dict = {}
    if payload.search:
        s = payload.search.strip()
        if s:
            filt["$or"] = [
                {"document_type_code": {"$regex": s, "$options": "i"}},
                {"prompt_text":        {"$regex": s, "$options": "i"}},
            ]
    skip  = (payload.page - 1) * payload.per_page
    total = await db["prompts"].count_documents(filt)
    docs  = await db["prompts"].find(filt).sort("created_at", -1).skip(skip).limit(payload.per_page).to_list(payload.per_page)
    return {"success": True, "total": total, "page": payload.page, "per_page": payload.per_page, "data": [_serialize(d) for d in docs]}


@router.post("/create", summary="Create a prompt", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def create_prompt(request: Request, payload: PromptCreate):
    db  = _get_db()
    now = datetime.now(timezone.utc)
    doc = {
        "document_type_code": payload.document_type_code.strip().upper(),
        "prompt_text":        payload.prompt_text.strip(),
        "version":            payload.version,
        "is_active":          payload.is_active,
        "level":              max(1, min(5, payload.level)) if payload.level is not None else None,
        "created_at":         now,
        "updated_at":         now,
    }
    result = await db["prompts"].insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"success": True, "message": "Prompt created", "data": _serialize(doc)}


@router.get("/{prompt_id}", summary="Get a prompt by ID", dependencies=[Depends(verify_api_key)])
async def get_prompt(request: Request, prompt_id: str):
    db = _get_db()
    if not ObjectId.is_valid(prompt_id):
        raise HTTPException(status_code=422, detail="Invalid prompt_id")
    doc = await db["prompts"].find_one({"_id": ObjectId(prompt_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Prompt not found")
    return {"success": True, "data": _serialize(doc)}


@router.patch("/{prompt_id}", summary="Update a prompt", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def update_prompt(request: Request, prompt_id: str, payload: PromptUpdate):
    db = _get_db()
    if not ObjectId.is_valid(prompt_id):
        raise HTTPException(status_code=422, detail="Invalid prompt_id")
    now    = datetime.now(timezone.utc)
    update = {"updated_at": now}
    if payload.document_type_code is not None:
        update["document_type_code"] = payload.document_type_code.strip().upper()
    if payload.prompt_text is not None:
        update["prompt_text"] = payload.prompt_text.strip()
    if payload.version is not None:
        update["version"] = payload.version
    if payload.is_active is not None:
        update["is_active"] = payload.is_active
    if payload.level is not None:
        update["level"] = max(1, min(5, payload.level))
    result = await db["prompts"].update_one({"_id": ObjectId(prompt_id)}, {"$set": update})
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Prompt not found")
    doc = await db["prompts"].find_one({"_id": ObjectId(prompt_id)})
    return {"success": True, "message": "Prompt updated", "data": _serialize(doc)}


@router.delete("/{prompt_id}", summary="Delete a prompt", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def delete_prompt(request: Request, prompt_id: str):
    db = _get_db()
    if not ObjectId.is_valid(prompt_id):
        raise HTTPException(status_code=422, detail="Invalid prompt_id")
    result = await db["prompts"].delete_one({"_id": ObjectId(prompt_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Prompt not found")
    return {"success": True, "message": "Prompt deleted", "id": prompt_id}
