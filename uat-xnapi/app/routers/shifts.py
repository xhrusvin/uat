import logging
from typing import Any

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
    summary="Fetch shift list from XpressHealth Shift API",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def list_shifts(request: Request, payload: ShiftListRequest):
    """
    Proxy to the XpressHealth Shift API.
    Always returns the raw response from the upstream — including any error
    messages — so the frontend can display them directly.
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

        # Try to parse as JSON whatever the status code
        try:
            upstream_data: Any = response.json()
        except Exception:
            upstream_data = response.text

        if response.status_code == 200:
            # Success — wrap and return
            if isinstance(upstream_data, dict):
                return {
                    "success": True,
                    "status_code": 200,
                    "data":     upstream_data.get("data")    or upstream_data,
                    "message":  upstream_data.get("message") or "OK",
                    "total":    upstream_data.get("total")   or upstream_data.get("count"),
                    "page":     payload.page,
                    "per_page": payload.per_page,
                }
            return {"success": True, "status_code": 200, "data": upstream_data}

        # Non-200 — return upstream error as-is so frontend shows it
        logger.warning(f"Shift API {response.status_code}: {str(upstream_data)[:300]}")

        # Extract a human-readable message from the upstream response
        upstream_msg = None
        if isinstance(upstream_data, dict):
            upstream_msg = (
                upstream_data.get("message")
                or upstream_data.get("error")
                or upstream_data.get("detail")
                or upstream_data.get("msg")
            )
        if not upstream_msg:
            upstream_msg = str(upstream_data)[:300] if upstream_data else f"HTTP {response.status_code}"

        return {
            "success":     False,
            "status_code": response.status_code,
            "message":     f"Shift API error ({response.status_code}): {upstream_msg}",
            "data":        upstream_data,   # send full upstream response to frontend
        }

    except httpx.TimeoutException:
        logger.error("Shift API timed out")
        return {
            "success":     False,
            "status_code": 504,
            "message":     "Shift API request timed out after 30 seconds. Please try again.",
            "data":        None,
        }

    except httpx.RequestError as e:
        logger.error(f"Shift API connection error: {e}")
        return {
            "success":     False,
            "status_code": 502,
            "message":     f"Could not connect to Shift API: {str(e)}",
            "data":        None,
        }
