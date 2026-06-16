import logging
from datetime import datetime, timezone
from typing import List, Optional

import httpx
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/clients", tags=["Clients"])


def _user_api_headers() -> dict:
    return {
        "Api-Key":       settings.USER_INTERNAL_API_KEY,
        "X-App-Country": settings.APP_COUNTRY,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize(doc: dict) -> dict:
    result = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            result[k] = str(v)
        elif hasattr(v, "isoformat"):
            result[k] = v.isoformat()
        elif isinstance(v, dict):
            result[k] = _serialize(v)
        elif isinstance(v, list):
            result[k] = [
                _serialize(i) if isinstance(i, dict)
                else str(i) if isinstance(i, ObjectId)
                else i
                for i in v
            ]
        else:
            result[k] = v
    return result


def _parse_dt(val) -> Optional[datetime]:
    """Parse various date formats to datetime."""
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(val, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def _extract_coord(item: dict, key: str):
    """Extract lat/lng — handles 0 and negative values correctly."""
    loc = item.get("location")
    if isinstance(loc, dict) and key in loc and loc[key] is not None:
        return loc[key]
    return None


def _build_client_doc(item: dict, now: datetime) -> dict:
    """
    Map upstream client fields → existing clients collection schema.
    Schema:  name, email, phone, address, county, notes,
             client_type, is_active, created_at, updated_at
    All extra upstream fields are stored too.
    """
    # Resolve is_active from status or is_active field
    raw_status = item.get("status")
    raw_status_name = item.get("status_name") or ""
    if isinstance(raw_status, bool):
        is_active = raw_status
    elif isinstance(raw_status, int):
        is_active = raw_status == 1
    elif isinstance(raw_status_name, str) and raw_status_name.lower() in ("enabled", "active"):
        is_active = True
    else:
        is_active = bool(item.get("is_active", True))

    doc = {
        # ── Core schema fields ────────────────────────────────────────────────
        "name":        (item.get("name") or item.get("title") or "").strip(),
        "email":       item.get("email") or None,
        "phone":       item.get("phone_number") or item.get("phone") or item.get("mobile") or None,
        "dial_code":   item.get("dial_code") or None,
        "address":     item.get("address") or item.get("full_address") or "",
        "county":      item.get("county") or item.get("county_name") or item.get("city") or "",
        "notes":       item.get("notes") or item.get("description") or "",
        "client_type": (item.get("client_type") or item.get("client_type_name") or
                        item.get("type") or ""),
        "type_of_client": item.get("type_of_client") or None,
        "is_active":   is_active,
        "updated_at":  now,

        # ── Identity ──────────────────────────────────────────────────────────
        "xn_client_id":    item.get("_id") or item.get("id") or None,
        "client_type_id":  item.get("client_type_id") or None,
        "county_id":       item.get("county_id") or None,

        # ── Contact / location ────────────────────────────────────────────────
        "eir_code":        item.get("eir_code") or item.get("eircode") or item.get("postal_code") or None,
        "province":        item.get("province") or None,
        "city":            item.get("city") or None,
        "location_name":   item.get("location_name") or None,
        "location":        item.get("location") or None,   # full object
        "longitude":       _extract_coord(item, "longitude"),
        "latitude":        _extract_coord(item, "latitude"),
        "website":         item.get("website") or None,
        "contact_person":  item.get("contact_person") or item.get("contact_name") or None,
        "account_manager": item.get("account_manager") or None,

        # ── Region / country ──────────────────────────────────────────────────
        "region":          item.get("region") or item.get("region_name") or None,
        "country":         item.get("country") or item.get("country_name") or None,

        # ── Status ────────────────────────────────────────────────────────────
        "status":          raw_status,
        "status_name":     item.get("status_name") or None,

        # ── Operational ───────────────────────────────────────────────────────
        "type_of_staff":        item.get("type_of_staff") or [],
        "units":                item.get("units") or None,
        "image":                item.get("image") or None,
        "travel_expense":       item.get("travel_expense") or None,
        "break_time_invoice":   item.get("break_time_invoice") or None,
        "break_time_payment":   item.get("break_time_payment") or None,
        "cancellation_time":    item.get("cancellation_time") or None,

        "synced_at": now,
    }

    # Store any remaining upstream fields not yet captured
    skip = {
        "_id", "id", "name", "title", "email", "phone", "phone_number", "mobile",
        "dial_code", "address", "full_address", "county", "county_name", "city",
        "notes", "description", "client_type", "client_type_name", "type",
        "type_of_client", "is_active", "client_type_id", "county_id",
        "eir_code", "eircode", "postal_code", "province", "location_name", "location",
        "website", "contact_person", "contact_name", "account_manager",
        "region", "region_name", "country", "country_name",
        "status", "status_name", "type_of_staff", "units", "image",
        "travel_expense", "break_time_invoice", "break_time_payment",
        "cancellation_time", "created_at", "updated_at",
    }
    for k, v in item.items():
        if k not in skip and k not in doc:
            doc[k] = v

    return doc


async def _upsert_clients(items: list, now: datetime) -> dict:
    """Upsert by xn_client_id. Falls back to name+email dedup."""
    db = _get_db()
    inserted = updated = skipped = 0

    for item in items:
        if not isinstance(item, dict):
            skipped += 1
            continue

        xn_id = str(item.get("_id") or item.get("id") or "").strip()
        name  = (item.get("name") or item.get("title") or "").strip()

        if not xn_id and not name:
            skipped += 1
            continue

        try:
            doc = _build_client_doc(item, now)

            # Determine created_at for new inserts
            raw_created = item.get("created_at") or item.get("createdAt")
            created_at  = _parse_dt(raw_created) or now

            # Find existing by xn_client_id first, then name
            existing = None
            if xn_id:
                existing = await db["clients"].find_one({"xn_client_id": xn_id})
            if not existing and name:
                existing = await db["clients"].find_one({"name": name, "xn_client_id": {"$exists": False}})

            if existing:
                await db["clients"].update_one({"_id": existing["_id"]}, {"$set": doc})
                updated += 1
            else:
                doc["created_at"] = created_at
                await db["clients"].insert_one(doc)
                inserted += 1

        except Exception as e:
            logger.error(f"Upsert error for client {xn_id or name}: {e}")
            skipped += 1

    return {"inserted": inserted, "updated": updated, "skipped": skipped}


# ── Request schema ────────────────────────────────────────────────────────────

class ClientFilters(BaseModel):
    client_type: Optional[List[str]] = None
    county:      Optional[List[str]] = None


class ClientListRequest(BaseModel):
    search:     str = ""
    page:       int = 1
    per_page:   int = 3000
    sort_by:    str = "created_at"
    sort_order: str = "desc"
    filters:    Optional[ClientFilters] = None


# ── Fetch from upstream + sync ────────────────────────────────────────────────

@router.post(
    "/sync",
    summary="Fetch clients from User API and sync to DB",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def sync_clients(request: Request, payload: ClientListRequest):
    url = f"{settings.USER_API_URL.rstrip('/')}/ai/clients/list"
    body: dict = {
        "search":     payload.search,
        "page":       payload.page,
        "per_page":   payload.per_page,
        "sort_by":    payload.sort_by,
        "sort_order": payload.sort_order,
    }
    if payload.filters:
        f = {}
        if payload.filters.client_type:
            f["client_type"] = payload.filters.client_type
        if payload.filters.county:
            f["county"] = payload.filters.county
        if f:
            body["filters"] = f

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=body, headers=_user_api_headers())

        try:
            upstream = response.json()
        except Exception:
            upstream = response.text

        if response.status_code != 200:
            msg = upstream.get("message") if isinstance(upstream, dict) else str(upstream)
            return {"success": False, "status_code": response.status_code,
                    "upstream_url": url, "message": msg, "data": upstream, "sync": None}

        raw   = upstream if isinstance(upstream, dict) else {}
        inner = raw.get("data") or {}
        if isinstance(inner, dict):
            clients_list = inner.get("data") or inner.get("list") or []
            total_count  = inner.get("total_count") or inner.get("total") or 0
            current_page = inner.get("current_page") or payload.page
            per_page     = inner.get("per_page") or payload.per_page
        elif isinstance(inner, list):
            clients_list = inner
            total_count  = len(inner)
            current_page = payload.page
            per_page     = payload.per_page
        else:
            clients_list = []
            total_count  = 0
            current_page = payload.page
            per_page     = payload.per_page

        now         = datetime.now(timezone.utc)
        sync_result = await _upsert_clients(clients_list, now)

        logger.info(f"Client sync: fetched={len(clients_list)} {sync_result}")

        return {
            "success":      True,
            "status_code":  200,
            "upstream_url": url,
            "message":      raw.get("message") or "Client list",
            "data":         clients_list,
            "total":        total_count,
            "page":         current_page,
            "per_page":     per_page,
            "sync":         {"fetched": len(clients_list), **sync_result},
        }

    except httpx.TimeoutException:
        return {"success": False, "status_code": 504, "upstream_url": url,
                "message": "Request timed out", "data": None, "sync": None}
    except httpx.RequestError as e:
        return {"success": False, "status_code": 502, "upstream_url": url,
                "message": str(e), "data": None, "sync": None}
    except Exception as e:
        logger.error(f"clients/sync error: {e}", exc_info=True)
        return {"success": False, "status_code": 500, "upstream_url": url,
                "message": str(e), "data": None, "sync": None}


# ── Read from DB ──────────────────────────────────────────────────────────────

@router.get(
    "/",
    summary="List clients from database",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("120/minute")
async def list_clients(
    request: Request,
    skip:    int           = Query(0, ge=0),
    limit:   int           = Query(20, ge=1, le=100),
    search:  Optional[str] = Query(None),
    status:  Optional[str] = Query(None, description="active|inactive"),
):
    db = _get_db()
    filters = []

    if search:
        filters.append({"$or": [
            {"name":         {"$regex": search, "$options": "i"}},
            {"email":        {"$regex": search, "$options": "i"}},
            {"phone":        {"$regex": search, "$options": "i"}},
            {"county":       {"$regex": search, "$options": "i"}},
            {"client_type":  {"$regex": search, "$options": "i"}},
            {"xn_client_id": {"$regex": search, "$options": "i"}},
        ]})

    if status == "active":
        filters.append({"is_active": True})
    elif status == "inactive":
        filters.append({"is_active": False})

    mongo_filter = {"$and": filters} if filters else {}
    total = await db["clients"].count_documents(mongo_filter)
    docs  = await db["clients"].find(mongo_filter).sort("name", 1).skip(skip).limit(limit).to_list(limit)

    return {"success": True, "total": total, "skip": skip, "limit": limit,
            "data": [_serialize(d) for d in docs]}


@router.get(
    "/{client_id}",
    summary="Get a single client from database",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("120/minute")
async def get_client(request: Request, client_id: str):
    db  = _get_db()
    doc = None
    if ObjectId.is_valid(client_id):
        doc = await db["clients"].find_one({"_id": ObjectId(client_id)})
    if not doc:
        doc = await db["clients"].find_one({"xn_client_id": client_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Client not found")
    return {"success": True, "data": _serialize(doc)}


# ── Sync single client from detail API ───────────────────────────────────────

class ClientDetailSyncRequest(BaseModel):
    client_id: str


@router.post(
    "/sync-detail",
    summary="Fetch single client detail from User API and upsert to clients collection",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def sync_client_detail(request: Request, payload: ClientDetailSyncRequest):
    """
    Body: { "client_id": "<xn_client_id>" }
    Fetches from {USER_API_URL}ai/clients/details and upserts to clients collection.
    """
    url  = f"{settings.USER_API_URL.rstrip('/')}/ai/clients/details"
    body = {"client_id": payload.client_id.strip()}

    from app.core.config import settings as s
    headers = {
        "Api-Key":       s.USER_INTERNAL_API_KEY,
        "X-App-Country": s.APP_COUNTRY,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(url, params=body, headers=headers)

        try:
            upstream = response.json()
        except Exception:
            upstream = response.text

        if response.status_code != 200:
            msg = upstream.get("message") if isinstance(upstream, dict) else str(upstream)
            return {"success": False, "status_code": response.status_code,
                    "message": msg, "data": upstream, "sync": None}

        raw  = upstream if isinstance(upstream, dict) else {}
        item = raw.get("data") or {}
        if not item:
            return {"success": False, "status_code": 200,
                    "message": "No data returned", "data": raw, "sync": None}

        # Ensure xn_client_id is set — detail API returns "id" not "_id"
        xn_id = str(item.get("id") or item.get("_id") or payload.client_id).strip()
        if not item.get("_id"):
            item["_id"] = xn_id
        if not item.get("id"):
            item["id"]  = xn_id

        now = datetime.now(timezone.utc)
        doc = _build_client_doc(item, now)
        db  = None
        from app.db.database import _client as _db_client
        db  = _db_client[settings.MONGODB_DB]

        # Match existing client by xn_client_id OR by the id string directly
        existing = await db["clients"].find_one({"$or": [
            {"xn_client_id": xn_id},
            {"xn_client_id": payload.client_id.strip()},
        ]})
        if existing:
            # Update ALL fields including longitude/latitude
            doc["xn_client_id"] = xn_id  # ensure xn_client_id is set correctly
            await db["clients"].update_one(
                {"_id": existing["_id"]},
                {"$set": doc}
            )
            action = "updated"
            client_db_id = str(existing["_id"])
        else:
            doc["xn_client_id"] = xn_id
            doc["created_at"]   = now
            result = await db["clients"].insert_one(doc)
            action = "inserted"
            client_db_id = str(result.inserted_id)

        sync = {"action": action, "xn_client_id": xn_id, "client_db_id": client_db_id,
                "fields_updated": list(doc.keys())}

        return {
            "success":      True,
            "status_code":  200,
            "message":      raw.get("message") or "Client details synced",
            "data":         item,
            "sync":         sync,
        }

    except httpx.TimeoutException:
        return {"success": False, "status_code": 504, "message": "Request timed out",
                "data": None, "sync": None}
    except Exception as e:
        logger.error(f"sync-client-detail error: {e}", exc_info=True)
        return {"success": False, "status_code": 500, "message": str(e),
                "data": None, "sync": None}
