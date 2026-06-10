from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.security import verify_api_key
from app.models.user import User
from app.schemas.user import UserListResponse, UserResponse, UserUpdate

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


# ── READ (list) ───────────────────────────────────────────────────────────────

@router.get(
    "/",
    response_model=UserListResponse,
    summary="List all non-admin users",
    dependencies=[Depends(verify_api_key)],
)
async def list_users(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    search: Optional[str] = Query(None),
):
    not_admin = {"is_admin": {"$ne": True}}
    if search:
        query = User.find({"$and": [not_admin, {"$or": [
            {"email":       {"$regex": search, "$options": "i"}},
            {"first_name":  {"$regex": search, "$options": "i"}},
            {"last_name":   {"$regex": search, "$options": "i"}},
            {"phone":       {"$regex": search, "$options": "i"}},
            {"xn_user_id":  {"$regex": search, "$options": "i"}},
            {"designation": {"$regex": search, "$options": "i"}},
        ]}]})
    else:
        query = User.find(not_admin)

    total = await query.count()
    users = await query.skip(skip).limit(limit).to_list()
    return UserListResponse(total=total, users=[_user_to_response(u) for u in users])


# ── READ (single) ─────────────────────────────────────────────────────────────

@router.get(
    "/{user_id}",
    response_model=UserResponse,
    summary="Get user by ID",
    dependencies=[Depends(verify_api_key)],
)
async def get_user(user_id: str):
    from beanie import PydanticObjectId
    try:
        oid = PydanticObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid user ID")
    user = await User.get(oid)
    if not user or user.is_admin is True:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return _user_to_response(user)


# ── UPDATE xn_user_id + designation (API key auth) ────────────────────────────

@router.patch(
    "/{user_id}",
    response_model=UserResponse,
    summary="Update user xn_user_id and designation",
    dependencies=[Depends(verify_api_key)],
)
async def update_user(user_id: str, payload: UserUpdate):
    from beanie import PydanticObjectId
    try:
        oid = PydanticObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid user ID")

    user = await User.get(oid)
    if not user or user.is_admin is True:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    update_data = payload.model_dump(exclude_unset=True)
    if "xn_user_id" in update_data:
        user.xn_user_id = update_data["xn_user_id"]
    if "designation" in update_data:
        user.designation = update_data["designation"]

    user.updated_at = datetime.now(timezone.utc)
    await user.save()
    return _user_to_response(user)
