import logging

import httpx
from fastapi import APIRouter, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/common", tags=["Common Lookups"])

# Shared headers for all User API calls
def _user_api_headers() -> dict:
    return {
        "Api-Key":       settings.USER_INTERNAL_API_KEY,
        "X-App-Country": settings.APP_COUNTRY,
        "Accept":        "application/json",
    }


async def _get(path: str) -> dict:
    """GET from User API and return a clean response dict."""
    url = f"{settings.USER_API_URL.rstrip('/')}/{path.lstrip('/')}"
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(url, headers=_user_api_headers())

        try:
            data = response.json()
        except Exception:
            data = response.text

        return {
            "success":      response.status_code == 200,
            "status_code":  response.status_code,
            "upstream_url": url,
            "data":         data,
            "message":      data.get("message") if isinstance(data, dict) else None,
        }

    except httpx.TimeoutException:
        return {"success": False, "status_code": 504, "upstream_url": url,
                "message": "Request timed out", "data": None}
    except httpx.RequestError as e:
        return {"success": False, "status_code": 502, "upstream_url": url,
                "message": str(e), "data": None}
    except Exception as e:
        logger.error(f"User API error [{path}]: {e}", exc_info=True)
        return {"success": False, "status_code": 500, "upstream_url": url,
                "message": str(e), "data": None}


# ── No auth required on these — public lookup endpoints ───────────────────────

@router.get(
    "/client-type-list",
    summary="Get client type list from User API",
)
@limiter.limit("60/minute")
async def client_type_list(request: Request):
    """
    Fetches client types from:
    GET {USER_API_URL}ai/common/client-type-list
    Headers: Api-Key + X-App-Country (from env)
    No Bearer token required.
    """
    return await _get("ai/common/client-type-list")
