from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.security import verify_api_key
from app.models.user import User
from app.schemas.user import UserListResponse, UserResponse, UserUpdate

limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/users", tags=["Users"])


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
        filters.append({"$or": [
            {"email":       {"$regex": search, "$options": "i"}},
            {"first_name":  {"$regex": search, "$options": "i"}},
            {"last_name":   {"$regex": search, "$options": "i"}},
            {"phone":       {"$regex": search, "$options": "i"}},
            {"xn_user_id":  {"$regex": search, "$options": "i"}},
            {"designation": {"$regex": search, "$options": "i"}},
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

    user.updated_at = datetime.now(timezone.utc)
    await user.save()
    return _user_to_response(user)
