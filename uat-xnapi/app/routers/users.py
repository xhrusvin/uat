from datetime import datetime, timezone
import logging
from bson import ObjectId
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key
from app.models.user import User
from app.schemas.user import UserListResponse, UserResponse, UserUpdate

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/users", tags=["Users"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _user_to_response(user: User) -> UserResponse:
    return UserResponse(
        id=str(user.id),
        email=user.email,
        first_name=user.first_name,
        last_name=user.last_name,
        full_name=user.full_name,
        phone=user.phone,
        is_admin=user.is_admin,
        status=user.status,
        xn_user_id=user.xn_user_id,
        designation=user.designation,
        created_at=user.created_at,
        tags=user.tags or [],
    )


def _build_date_filter(date_from, date_to):
    if not date_from and not date_to:
        return {}
    try:
        if date_from:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d").replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        if date_to:
            dt_to = datetime.strptime(date_to, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc)
    except ValueError:
        raise HTTPException(status_code=422, detail="Invalid date format. Use YYYY-MM-DD")

    datetime_cond = {}
    string_cond   = {}
    if date_from:
        datetime_cond["$gte"] = dt_from
        string_cond["$gte"]   = date_from
    if date_to:
        datetime_cond["$lte"] = dt_to
        string_cond["$lte"]   = date_to + "~"

    return {"$or": [
        {"created_at": datetime_cond},
        {"created_at": string_cond},
    ]}


# ── LIST — 30 requests/minute per IP ─────────────────────────────────────────

@router.get("/", response_model=UserListResponse, summary="List all non-admin users",
            dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def list_users(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    search: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    not_admin = {"is_admin": {"$ne": True}}
    filters = [not_admin]

    if search:
        search_terms = search.strip().split()
        if len(search_terms) > 1:
            term_filters = []
            for term in search_terms:
                term_filters.append({"$or": [
                    {"email":       {"$regex": term, "$options": "i"}},
                    {"first_name":  {"$regex": term, "$options": "i"}},
                    {"last_name":   {"$regex": term, "$options": "i"}},
                    {"phone":       {"$regex": term, "$options": "i"}},
                    {"xn_user_id":  {"$regex": term, "$options": "i"}},
                    {"designation": {"$regex": term, "$options": "i"}},
                    {"tags.name":   {"$regex": term, "$options": "i"}},
                ]})
            filters.append({"$and": term_filters})
        else:
            filters.append({"$or": [
                {"email":       {"$regex": search, "$options": "i"}},
                {"first_name":  {"$regex": search, "$options": "i"}},
                {"last_name":   {"$regex": search, "$options": "i"}},
                {"phone":       {"$regex": search, "$options": "i"}},
                {"xn_user_id":  {"$regex": search, "$options": "i"}},
                {"designation": {"$regex": search, "$options": "i"}},
                {"tags.name":   {"$regex": search, "$options": "i"}},
            ]})

    date_filter = _build_date_filter(date_from, date_to)
    if date_filter:
        filters.append(date_filter)

    mongo_filter = {"$and": filters} if len(filters) > 1 else filters[0]
    query = User.find(mongo_filter).sort("+created_at")

    total = await query.count()
    users = await query.skip(skip).limit(limit).to_list()
    return UserListResponse(total=total, users=[_user_to_response(u) for u in users])


# ── GET single ────────────────────────────────────────────────────────────────

@router.get("/{user_id}", response_model=UserResponse, summary="Get user by ID",
            dependencies=[Depends(verify_api_key)])
@limiter.limit("60/minute")
async def get_user(request: Request, user_id: str):
    from beanie import PydanticObjectId
    try:
        oid = PydanticObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid user ID")
    user = await User.get(oid)
    if not user or user.is_admin is True:
        raise HTTPException(status_code=404, detail="User not found")
    return _user_to_response(user)


# ── PATCH ─────────────────────────────────────────────────────────────────────

@router.patch("/{user_id}", response_model=UserResponse, summary="Update user",
              dependencies=[Depends(verify_api_key)])
@limiter.limit("20/minute")
async def update_user(request: Request, user_id: str, payload: UserUpdate):
    from beanie import PydanticObjectId
    try:
        oid = PydanticObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid user ID")

    user = await User.get(oid)
    if not user or user.is_admin is True:
        raise HTTPException(status_code=404, detail="User not found")

    update_data = payload.model_dump(exclude_unset=True)
    if "xn_user_id" in update_data:
        user.xn_user_id = update_data["xn_user_id"]
    if "designation" in update_data:
        user.designation = update_data["designation"]

    # Save visa fields directly via Motor (not in Beanie model)
    extra = {}
    if "consumed_hours" in update_data:
        extra["consumed_hours"] = update_data["consumed_hours"]
    if "work_permit_exemption" in update_data:
        extra["work_permit_exemption"] = update_data["work_permit_exemption"]
    if extra:
        from app.db.database import _client
        from app.core.config import settings as _settings
        _db = _client[_settings.MONGODB_DB]
        await _db["users"].update_one({"_id": oid}, {"$set": extra})

    user.updated_at = datetime.now(timezone.utc)
    await user.save()
    return _user_to_response(user)


@router.delete(
    "/{user_id}",
    summary="Delete a user by ID",
    dependencies=[Depends(verify_api_key)],
)
async def delete_user(request: Request, user_id: str):
    try:
        from beanie import PydanticObjectId
        oid = PydanticObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid user_id")
    user = await User.get(oid)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    await user.delete()
    return {"success": True, "message": "User deleted", "id": user_id}


# ── POST /users/sync-xn-user-id ───────────────────────────────────────────────

@router.post(
    "/sync-xn-user-id",
    summary="Trigger background sync of xn_user_id for users missing it",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("5/minute")
async def sync_xn_user_id(request: Request, limit: int = 100):
    """
    Finds users missing xn_user_id and calls upstream
    GET /ai/recruitments/user-document-list to resolve it.
    Runs in background — returns immediately with job status.
    """
    import asyncio
    import httpx as _httpx

    db  = _get_db()

    USER_API_URL      = settings.USER_API_URL.rstrip("/")
    USER_EXTERNAL_KEY = settings.USER_EXTERNAL_API_KEY
    DOCUMENT_ID       = "68daa26ba580ebbd1001fc8b"

    query = {
        "$or": [
            {"xn_user_id": {"$exists": False}},
            {"xn_user_id": None},
            {"xn_user_id": ""},
        ],
        "email": {"$exists": True, "$ne": None, "$ne": ""},
    }

    total_missing = await db["users"].count_documents(query)

    async def _run_sync():
        users = await db["users"].find(
            query, {"_id": 1, "email": 1}
        ).limit(limit).to_list(limit)

        success = failed = 0
        headers = {
            "Api-Key":      USER_EXTERNAL_KEY,
            "Content-Type": "application/json",
            "Accept":       "application/json",
        }

        async with _httpx.AsyncClient(timeout=20.0) as client:
            for u in users:
                email = (u.get("email") or "").strip()
                if not email:
                    continue
                try:
                    resp = await client.get(
                        f"{USER_API_URL}/ai/recruitments/user-document-list",
                        params={"email": email, "document_id": DOCUMENT_ID},
                        headers=headers,
                    )
                    if resp.status_code == 200:
                        body  = resp.json()
                        xn_id = (body.get("data") or {}).get("id") if body.get("success") else None
                        if xn_id:
                            await db["users"].update_one(
                                {"_id": u["_id"]},
                                {"$set": {"xn_user_id": xn_id}}
                            )
                            success += 1
                        else:
                            failed += 1
                    else:
                        failed += 1
                except Exception:
                    failed += 1

        logger.info(f"[sync-xn-user-id] done success={success} failed={failed}")

    asyncio.create_task(_run_sync())

    return {
        "success":       True,
        "message":       f"Sync started in background for up to {limit} users",
        "total_missing": total_missing,
        "processing":    min(limit, total_missing),
    }


# ── POST /users/{id}/clear-exclusion-cache ────────────────────────────────────

@router.post(
    "/{user_id}/clear-exclusion-cache",
    summary="Clear exclusion cache for a user",
)
async def clear_exclusion_cache(request: Request, user_id: str):
    if not ObjectId.is_valid(user_id):
        raise HTTPException(status_code=422, detail="Invalid user_id")
    db = _get_db()
    result = await db["users"].update_one(
        {"_id": ObjectId(user_id)},
        {"$unset": {"exclusion_cache": "", "exclusion_cache_at": ""}}
    )
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    return {
        "success": True,
        "message": "Exclusion cache cleared — will recompute on next shift list call",
        "user_id": user_id,
        "modified": result.modified_count > 0,
    }
