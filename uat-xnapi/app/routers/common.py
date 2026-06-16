import logging
from datetime import datetime, timezone

import httpx
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/common", tags=["Common Lookups"])


def _user_api_headers() -> dict:
    return {
        "Api-Key":       settings.USER_INTERNAL_API_KEY,
        "X-App-Country": settings.APP_COUNTRY,
        "Accept":        "application/json",
    }


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize(doc: dict) -> dict:
    result = {}
    for k, v in doc.items():
        from bson import ObjectId
        if isinstance(v, ObjectId):
            result[k] = str(v)
        elif hasattr(v, "isoformat"):
            result[k] = v.isoformat()
        else:
            result[k] = v
    return result


async def _fetch_and_store_client_types() -> dict:
    """Fetch from upstream, upsert into client_types collection, return result."""
    url = f"{settings.USER_API_URL.rstrip('/')}/ai/common/client-type-list"
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(url, params={"per_page": 3000}, headers=_user_api_headers())

        try:
            upstream = response.json()
        except Exception:
            upstream = response.text

        if response.status_code != 200:
            msg = upstream.get("message") if isinstance(upstream, dict) else str(upstream)
            return {"success": False, "status_code": response.status_code,
                    "upstream_url": url, "message": msg, "data": upstream, "sync": None}

        # Extract list — handle various response shapes
        raw = upstream if isinstance(upstream, dict) else {}
        items = raw.get("data") or raw.get("list") or raw.get("types") or []
        if not isinstance(items, list):
            items = [items] if items else []

        # Upsert into client_types collection
        db = _get_db()
        now = datetime.now(timezone.utc)
        inserted = updated = 0

        for item in items:
            if not isinstance(item, dict):
                continue
            # Use 'id' or '_id' or 'value' as the dedup key
            uid = (str(item.get("id") or item.get("_id") or item.get("value") or "")).strip()
            if not uid:
                continue

            doc = {**item, "synced_at": now}
            existing = await db["client_types"].find_one({"id": uid})
            if existing:
                await db["client_types"].update_one({"id": uid}, {"$set": doc})
                updated += 1
            else:
                doc["id"]         = uid
                doc["created_at"] = now
                await db["client_types"].insert_one(doc)
                inserted += 1

        return {
            "success":      True,
            "status_code":  200,
            "upstream_url": url,
            "message":      raw.get("message") or "Client type list",
            "data":         items,
            "sync":         {"fetched": len(items), "inserted": inserted, "updated": updated},
        }

    except httpx.TimeoutException:
        return {"success": False, "status_code": 504, "upstream_url": url,
                "message": "Request timed out", "data": None, "sync": None}
    except httpx.RequestError as e:
        return {"success": False, "status_code": 502, "upstream_url": url,
                "message": str(e), "data": None, "sync": None}
    except Exception as e:
        logger.error(f"client-type-list error: {e}", exc_info=True)
        return {"success": False, "status_code": 500, "upstream_url": url,
                "message": str(e), "data": None, "sync": None}


# ── Fetch from upstream + sync to DB ─────────────────────────────────────────

@router.get("/client-type-list", summary="Fetch client types from User API and sync to DB")
@limiter.limit("60/minute")
async def client_type_list(request: Request):
    """No auth required. Fetches and stores into client_types collection."""
    return await _fetch_and_store_client_types()


# ── Read from DB ──────────────────────────────────────────────────────────────

@router.get(
    "/client-types",
    summary="List client types from database",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("120/minute")
async def list_client_types(request: Request):
    """Returns all client types stored in the client_types collection."""
    db = _get_db()
    docs = await db["client_types"].find().sort("name", 1).to_list(length=500)
    items = [_serialize(d) for d in docs]
    return {"success": True, "total": len(items), "data": items}


# ── Client Details ─────────────────────────────────────────────────────────────

class ClientDetailRequest(BaseModel):
    client_id: str


@router.post(
    "/client-detail",
    summary="Get client details from User API",
)
@limiter.limit("60/minute")
async def client_detail(request: Request, payload: ClientDetailRequest):
    """
    POST body: { "client_id": "<xn_client_id>" }
    No authentication required.
    Fetches from {USER_API_URL}ai/clients/details using USER_INTERNAL_API_KEY.
    """
    url  = f"{settings.USER_API_URL.rstrip('/')}/ai/clients/details"
    body = {"client_id": payload.client_id.strip()}

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(url, json=body, headers=_user_api_headers())
        try:
            upstream = response.json()
        except Exception:
            upstream = response.text

        if response.status_code != 200:
            msg = upstream.get("message") if isinstance(upstream, dict) else str(upstream)
            return {"success": False, "status_code": response.status_code,
                    "upstream_url": url, "message": msg, "data": upstream}

        raw  = upstream if isinstance(upstream, dict) else {}
        return {
            "success":      True,
            "status_code":  200,
            "upstream_url": url,
            "message":      raw.get("message") or "Client details",
            "data":         raw.get("data") or raw,
        }

    except httpx.TimeoutException:
        return {"success": False, "status_code": 504, "upstream_url": url,
                "message": "Request timed out", "data": None}
    except httpx.RequestError as e:
        return {"success": False, "status_code": 502, "upstream_url": url,
                "message": str(e), "data": None}
    except Exception as e:
        logger.error(f"client-detail error: {e}", exc_info=True)
        return {"success": False, "status_code": 500, "upstream_url": url,
                "message": str(e), "data": None}
