from datetime import datetime, timezone
from typing import Optional

from beanie import Document
from bson.binary import Binary
from pydantic import EmailStr, Field


class User(Document):
    """
    Mirrors the existing xpress_health_uat.users collection schema.

    Key differences from a greenfield model:
    - password field (not hashed_password) — stored as bytes, Binary, or str
    - is_admin flag (not is_active/is_superuser)
    - first_name / last_name instead of username
    - phone, call_sent etc. are optional (not all rows have them)
    """

    # ── Identity ──────────────────────────────────────────────────────────────
    email: EmailStr
    password: Optional[object] = None   # bytes | Binary | str — handled by security layer
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    phone: Optional[str] = None

    # ── Roles / Status ────────────────────────────────────────────────────────
    is_admin: bool = False
    status: Optional[str] = "Enabled"

    # ── Timestamps ────────────────────────────────────────────────────────────
    created_at: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: Optional[datetime] = Field(default_factory=lambda: datetime.now(timezone.utc))

    # ── Extra operational fields (may or may not exist on older docs) ─────────
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
