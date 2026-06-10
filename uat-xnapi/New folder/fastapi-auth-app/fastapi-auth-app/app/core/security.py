import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
from bson.binary import Binary
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from app.core.config import settings

bearer_scheme = HTTPBearer()


# ── API Key auth (for external API calls) ─────────────────────────────────────

def verify_api_key(
    credentials: HTTPAuthorizationCredentials = Security(bearer_scheme),
):
    if not secrets.compare_digest(credentials.credentials, settings.API_KEY):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing API key",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return credentials.credentials


# ── JWT (for admin panel) ─────────────────────────────────────────────────────

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    )
    to_encode["exp"] = expire
    return jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)


def decode_access_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, settings.SECRET_KEY, algorithms=[settings.ALGORITHM])
    except JWTError:
        return None


# ── Password ──────────────────────────────────────────────────────────────────

def normalize_stored_hash(stored_hash) -> Optional[bytes]:
    if isinstance(stored_hash, str):
        try:
            return stored_hash.encode("utf-8")
        except (UnicodeEncodeError, AttributeError):
            return None
    elif isinstance(stored_hash, (bytes, bytearray, Binary)):
        return bytes(stored_hash)
    return None


def hash_password(password: str) -> bytes:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt())


def verify_password(plain_password: str, stored_hash) -> bool:
    try:
        normalized = normalize_stored_hash(stored_hash)
        if normalized is None:
            return False
        return bcrypt.checkpw(plain_password.encode("utf-8"), normalized)
    except Exception:
        return False
