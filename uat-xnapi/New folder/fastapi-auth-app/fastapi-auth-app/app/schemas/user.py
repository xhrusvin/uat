from datetime import datetime
from typing import Optional

from pydantic import BaseModel, EmailStr, Field


# ── Auth ──────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ── User ──────────────────────────────────────────────────────────────────────

class UserUpdate(BaseModel):
    xn_user_id: Optional[str] = Field(None, description="XpressHealth portal user ID")
    designation: Optional[str] = Field(None, description="User job designation/role")


class UserResponse(BaseModel):
    id: str
    email: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    phone: Optional[str] = None
    is_admin: bool = False
    status: Optional[str] = None
    xn_user_id: Optional[str] = None
    designation: Optional[str] = None
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class UserListResponse(BaseModel):
    total: int
    users: list[UserResponse]
