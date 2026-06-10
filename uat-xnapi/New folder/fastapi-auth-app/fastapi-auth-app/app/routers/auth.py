import logging

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.security import create_access_token, decode_access_token, verify_password
from app.models.user import User
from app.schemas.user import LoginRequest, Token, UserResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["Authentication"])
bearer_scheme = HTTPBearer()


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


async def get_current_admin(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> User:
    """Dependency — validates JWT and ensures user is admin."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = decode_access_token(credentials.credentials)
        if payload is None:
            raise credentials_exception
        email: str = payload.get("sub")
        if not email:
            raise credentials_exception
        user = await User.find_one(User.email == email)
        if not user:
            raise credentials_exception
        if not user.is_admin:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Auth error: {e}", exc_info=True)
        raise credentials_exception


@router.post("/login", response_model=Token, summary="Admin login")
async def login(payload: LoginRequest):
    """Login with email + password. Returns a JWT token for admin panel use."""
    try:
        user = await User.find_one(User.email == payload.email)
        if not user or not verify_password(payload.password, user.password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if not user.is_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin access required",
            )
        token = create_access_token(data={"sub": user.email})
        return Token(access_token=token)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Unexpected error")


@router.get("/me", response_model=UserResponse, summary="Get current admin user")
async def get_me(current_user: User = Depends(get_current_admin)):
    return _user_to_response(current_user)
