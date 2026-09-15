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

logger  = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router  = APIRouter(prefix="/session-users", tags=["Session Users"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize(doc: dict) -> dict:
    """Serialize a session_users document into a clean dict."""
    return {
        "id":           str(doc["_id"]),
        "xn_user_id":   doc.get("xn_user_id") or doc.get("id") or None,
        "name":         doc.get("full_name") or doc.get("name") or "",
        "first_name":   doc.get("first_name") or "",
        "last_name":    doc.get("last_name") or "",
        "email":        doc.get("email") or "",
        "phone":        doc.get("phone") or doc.get("mobile") or "",
        "role":         doc.get("role") or doc.get("user_type") or doc.get("user_type_name") or "",
        "is_active":    doc.get("is_active", True),
        "synced_at":    doc["synced_at"].isoformat()    if doc.get("synced_at")    and hasattr(doc["synced_at"],    "isoformat") else None,
        "created_at":   doc["created_at"].isoformat()   if doc.get("created_at")   and hasattr(doc["created_at"],   "isoformat") else None,
        "updated_at":   doc["updated_at"].isoformat()   if doc.get("updated_at")   and hasattr(doc["updated_at"],   "isoformat") else None,
    }


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class SessionUserCreate(BaseModel):
    first_name: str
    last_name:  Optional[str] = ""
    email:      Optional[str] = ""
    phone:      Optional[str] = ""
    role:       Optional[str] = ""
    is_active:  Optional[bool] = True


class SessionUserUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name:  Optional[str] = None
    email:      Optional[str] = None
    phone:      Optional[str] = None
    role:       Optional[str] = None
    is_active:  Optional[bool] = None


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/", summary="List all session users", dependencies=[Depends(verify_api_key)])
@limiter.limit("120/minute")
async def list_session_users(
    request: Request,
    search:  str  = "",
    page:    int  = 1,
    per_page: int = 50,
):
    db = _get_db()
    query: dict = {}
    if search.strip():
        pattern = {"$regex": search.strip(), "$options": "i"}
        query["$or"] = [
            {"full_name":   pattern},
            {"first_name":  pattern},
            {"last_name":   pattern},
            {"email":       pattern},
            {"role":        pattern},
            {"user_type":   pattern},
        ]

    total = await db["session_users"].count_documents(query)
    skip  = (page - 1) * per_page
    docs  = await db["session_users"].find(query).sort("created_at", -1).skip(skip).limit(per_page).to_list(per_page)

    return {
        "success": True,
        "total":   total,
        "page":    page,
        "per_page": per_page,
        "data":    [_serialize(d) for d in docs],
    }


@router.post("/create", summary="Create a session user", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def create_session_user(request: Request, payload: SessionUserCreate):
    db  = _get_db()
    now = datetime.now(timezone.utc)

    full_name = f"{payload.first_name.strip()} {(payload.last_name or '').strip()}".strip()

    # Duplicate check on email
    if payload.email and payload.email.strip():
        existing = await db["session_users"].find_one({"email": payload.email.strip().lower()})
        if existing:
            raise HTTPException(status_code=409, detail="A session user with this email already exists")

    doc = {
        "first_name": payload.first_name.strip(),
        "last_name":  (payload.last_name or "").strip(),
        "full_name":  full_name,
        "email":      (payload.email or "").strip().lower(),
        "phone":      (payload.phone or "").strip(),
        "role":       (payload.role or "").strip(),
        "is_active":  payload.is_active if payload.is_active is not None else True,
        "created_at": now,
        "updated_at": now,
        "synced_at":  None,
    }
    result = await db["session_users"].insert_one(doc)
    doc["_id"] = result.inserted_id
    return {"success": True, "message": "Session user created", "data": _serialize(doc)}


@router.patch("/{user_id}", summary="Update a session user", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def update_session_user(request: Request, user_id: str, payload: SessionUserUpdate):
    db = _get_db()
    if not ObjectId.is_valid(user_id):
        raise HTTPException(status_code=422, detail="Invalid user_id")

    now    = datetime.now(timezone.utc)
    update: dict = {"updated_at": now}

    if payload.first_name is not None:
        update["first_name"] = payload.first_name.strip()
    if payload.last_name is not None:
        update["last_name"] = payload.last_name.strip()
    if payload.email is not None:
        update["email"] = payload.email.strip().lower()
    if payload.phone is not None:
        update["phone"] = payload.phone.strip()
    if payload.role is not None:
        update["role"] = payload.role.strip()
    if payload.is_active is not None:
        update["is_active"] = payload.is_active

    # Rebuild full_name if first/last changed
    if "first_name" in update or "last_name" in update:
        doc_now = await db["session_users"].find_one({"_id": ObjectId(user_id)})
        if doc_now:
            fn = update.get("first_name", doc_now.get("first_name", ""))
            ln = update.get("last_name",  doc_now.get("last_name", ""))
            update["full_name"] = f"{fn} {ln}".strip()

    result = await db["session_users"].update_one(
        {"_id": ObjectId(user_id)},
        {"$set": update},
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Session user not found")

    doc = await db["session_users"].find_one({"_id": ObjectId(user_id)})
    return {"success": True, "message": "Session user updated", "data": _serialize(doc)}


@router.delete("/{user_id}", summary="Delete a session user", dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def delete_session_user(request: Request, user_id: str):
    db = _get_db()
    if not ObjectId.is_valid(user_id):
        raise HTTPException(status_code=422, detail="Invalid user_id")

    result = await db["session_users"].delete_one({"_id": ObjectId(user_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Session user not found")

    return {"success": True, "message": "Session user deleted", "id": user_id}
