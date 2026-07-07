from datetime import datetime, timezone
from typing import Optional

from beanie import Document
from bson.binary import Binary
from pydantic import EmailStr, Field


class User(Document):
    """
    Mirrors the existing xpress_health_uat.users collection schema.
    """
    # ── Identity ──────────────────────────────────────────────────────────────
    email: Optional[str] = None
    password: Optional[object] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None

    # ── Roles / Status ────────────────────────────────────────────────────────
    is_admin: bool = False
    status: Optional[str] = "Enabled"

    # ── XpressHealth portal fields ────────────────────────────────────────────
    xn_user_id: Optional[str] = None
    designation: Optional[str] = None

    # ── Timestamps ────────────────────────────────────────────────────────────
    created_at: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))

    # ── Extra operational fields ──────────────────────────────────────────────
    call_sent: Optional[int] = None
    garda_email_sent: Optional[int] = None
    follow_up_sent: Optional[int] = None
    onboarded: Optional[int] = None

    class Settings:
        name = "users"
        use_state_management = True

    @property
    def full_name(self) -> str:
        return " ".join(filter(None, [self.first_name, self.last_name])).strip() or "—"
