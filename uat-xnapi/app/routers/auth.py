import logging

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from app.core.security import create_access_token, decode_access_token, verify_password
from app.models.user import User
from app.schemas.user import LoginRequest, Token, UserResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["Authentication"])

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


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


async def get_current_user(token: str = Depends(oauth2_scheme)) -> User:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = decode_access_token(token)
        if payload is None:
            raise credentials_exception

        email: str = payload.get("sub")
        if not email:
            raise credentials_exception

        user = await User.find_one(User.email == email)
        if user is None:
            raise credentials_exception

        if user.status and user.status.lower() == "disabled":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Account is disabled",
            )
        return user

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"get_current_user error: {e}", exc_info=True)
        raise credentials_exception


async def get_current_admin(current_user: User = Depends(get_current_user)) -> User:
    """Dependency that ensures the caller is an admin."""
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return current_user


@router.post("/login", response_model=Token, summary="Login with email + password")
async def login(payload: LoginRequest):
    """
    Authenticate with email + password.
    Supports all password storage formats used by the existing system:
    bytes, BSON Binary, and plain UTF-8 strings.
    """
    try:
        user = await User.find_one(User.email == payload.email)

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Use the same verification logic as the Flask admin — handles bytes/Binary/str
        if not verify_password(payload.password, user.password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        if user.status and user.status.lower() == "disabled":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Account is disabled",
            )

        token = create_access_token(data={"sub": user.email})
        return Token(access_token=token)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Login error for {payload.email}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred. Check server logs.",
        )


@router.get("/me", response_model=UserResponse, summary="Get current logged-in user")
async def get_me(current_user: User = Depends(get_current_user)):
    return _user_to_response(current_user)
