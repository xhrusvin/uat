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


def _build_date_filter(date_from: Optional[str], date_to: Optional[str]) -> dict:
    """
    Build a MongoDB filter for created_at that works whether the field is
    stored as a datetime object OR as an ISO string (both formats exist in
    the existing collection).

    Strategy: use $or so either storage format matches.
    """
    if not date_from and not date_to:
        return {}

    try:
        if date_from:
            dt_from = datetime.strptime(date_from, "%Y-%m-%d").replace(
                hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
            )
            # String equivalents for $regex boundary
            str_from = date_from  # "2026-04-09"

        if date_to:
            dt_to = datetime.strptime(date_to, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc
            )
            str_to = date_to  # "2026-04-09"

    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Invalid date format. Use YYYY-MM-DD",
        )

    # Build conditions for datetime storage
    datetime_cond = {}
    if date_from:
        datetime_cond["$gte"] = dt_from
    if date_to:
        datetime_cond["$lte"] = dt_to

    # Build conditions for string storage — ISO strings sort lexicographically
    # so plain string comparison works correctly for YYYY-MM-DD prefix
    string_cond = {}
    if date_from:
        string_cond["$gte"] = str_from  # "2026-04-09T00:00:00" >= "2026-04-09"
    if date_to:
        # "2026-04-09T23:59:59" <= "2026-04-09~" (tilde comes after digits in ASCII)
        string_cond["$lte"] = str_to + "~"

    return {
        "$or": [
            {"created_at": datetime_cond},    # stored as datetime
            {"created_at": string_cond},      # stored as ISO string
        ]
    }


# ── READ (list) ───────────────────────────────────────────────────────────────

@router.get(
    "/",
    response_model=UserListResponse,
    summary="List all non-admin users — sorted by joined date ascending",
    dependencies=[Depends(verify_api_key)],
)
async def list_users(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    search: Optional[str] = Query(None, description="Search by name, email, phone, designation, xn_user_id"),
    date_from: Optional[str] = Query(None, description="Filter joined from date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Filter joined to date (YYYY-MM-DD)"),
):
    not_admin = {"is_admin": {"$ne": True}}
    filters = [not_admin]

    # ── Search filter ─────────────────────────────────────────────────────────
    if search:
        filters.append({"$or": [
            {"email":       {"$regex": search, "$options": "i"}},
            {"first_name":  {"$regex": search, "$options": "i"}},
            {"last_name":   {"$regex": search, "$options": "i"}},
            {"phone":       {"$regex": search, "$options": "i"}},
            {"xn_user_id":  {"$regex": search, "$options": "i"}},
            {"designation": {"$regex": search, "$options": "i"}},
        ]})

    # ── Date range filter — handles both datetime and ISO string storage ───────
    date_filter = _build_date_filter(date_from, date_to)
    if date_filter:
        filters.append(date_filter)

    mongo_filter = {"$and": filters} if len(filters) > 1 else filters[0]
    query = User.find(mongo_filter).sort("+created_at")

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
