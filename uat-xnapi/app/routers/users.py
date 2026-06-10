from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status

from app.core.security import verify_api_key
from app.models.user import User
from app.schemas.user import UserCreate, UserListResponse, UserResponse, UserUpdate

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
        created_at=user.created_at,
    )


# ── CREATE ────────────────────────────────────────────────────────────────────

@router.post(
    "/",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new user",
    dependencies=[Depends(verify_api_key)],
)
async def create_user(payload: UserCreate):
    import bcrypt
    if await User.find_one(User.email == payload.email):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already registered")

    user = User(
        email=payload.email,
        password=bcrypt.hashpw(payload.password.encode("utf-8"), bcrypt.gensalt()),
        first_name=payload.first_name,
        last_name=payload.last_name,
        phone=payload.phone,
        is_admin=False,
        status="Enabled",
    )
    await user.insert()
    return _user_to_response(user)


# ── READ (list) ───────────────────────────────────────────────────────────────

@router.get(
    "/",
    response_model=UserListResponse,
    summary="List all users",
    dependencies=[Depends(verify_api_key)],
)
async def list_users(
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    search: Optional[str] = Query(None, description="Filter by name, email, or phone"),
):
    from beanie.operators import Or, RegEx

    if search:
        pattern = f".*{search}.*"
        query = User.find(
            Or(
                RegEx(User.email, pattern, options="i"),
                RegEx(User.first_name, pattern, options="i"),
                RegEx(User.last_name, pattern, options="i"),
                RegEx(User.phone, pattern, options="i"),
            )
        )
    else:
        query = User.find()

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
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return _user_to_response(user)


# ── UPDATE ────────────────────────────────────────────────────────────────────

@router.patch(
    "/{user_id}",
    response_model=UserResponse,
    summary="Update a user",
    dependencies=[Depends(verify_api_key)],
)
async def update_user(user_id: str, payload: UserUpdate):
    import bcrypt
    from beanie import PydanticObjectId
    try:
        oid = PydanticObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid user ID")

    user = await User.get(oid)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    update_data = payload.model_dump(exclude_unset=True)

    if "email" in update_data:
        existing = await User.find_one(User.email == update_data["email"])
        if existing and str(existing.id) != user_id:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already registered")
        user.email = update_data["email"]

    if "password" in update_data:
        user.password = bcrypt.hashpw(update_data["password"].encode("utf-8"), bcrypt.gensalt())

    for field in ("first_name", "last_name", "phone", "status"):
        if field in update_data:
            setattr(user, field, update_data[field])

    user.updated_at = datetime.now(timezone.utc)
    await user.save()
    return _user_to_response(user)


# ── DELETE ────────────────────────────────────────────────────────────────────

@router.delete(
    "/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a user",
    dependencies=[Depends(verify_api_key)],
)
async def delete_user(user_id: str):
    from beanie import PydanticObjectId
    try:
        oid = PydanticObjectId(user_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid user ID")

    user = await User.get(oid)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    if user.is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Cannot delete admin user")

    await user.delete()
