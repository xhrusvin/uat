import logging

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, status
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key
from app.schemas.shift import ShiftListRequest, ShiftListResponse

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/shifts", tags=["Shifts"])


@router.post(
    "/list",
    response_model=ShiftListResponse,
    summary="Fetch shift list from XpressHealth Shift API",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def list_shifts(request: Request, payload: ShiftListRequest):
    """
    Proxy to the XpressHealth Shift API.
    Forwards the request body to `{SHIFT_URL}ai/shifts/list`
    with the internal API key header.
    """
    url = f"{settings.SHIFT_URL.rstrip('/')}ai/shifts/list"

    headers = {
        "Api-Key": settings.SHIFT_INTERNAL_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    body = payload.model_dump(exclude_none=True)

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=body, headers=headers)

        if response.status_code == 200:
            try:
                data = response.json()
                return ShiftListResponse(
                    success=True,
                    data=data.get("data") or data,
                    message=data.get("message"),
                    total=data.get("total") or data.get("count"),
                    page=payload.page,
                    per_page=payload.per_page,
                )
            except Exception:
                return ShiftListResponse(success=True, data=response.text)

        logger.warning(f"Shift API returned {response.status_code}: {response.text[:200]}")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Shift API error {response.status_code}: {response.text[:200]}",
        )

    except httpx.TimeoutException:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Shift API request timed out",
        )
    except httpx.RequestError as e:
        logger.error(f"Shift API request error: {e}")
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Could not reach Shift API: {str(e)}",
        )
