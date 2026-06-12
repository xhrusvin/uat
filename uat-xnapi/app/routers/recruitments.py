import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Request
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/recruitments", tags=["Recruitments"])


def _external_api_headers() -> dict:
    return {
        "Api-Key":       settings.USER_EXTERNAL_API_KEY,
        "X-App-Country": settings.APP_COUNTRY,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _map_user_fields(data: dict, now: datetime) -> dict:
    doc: dict = {"updated_at": now, "synced_at": now}

    for field, dest in [
        ("first_name",  "first_name"),
        ("last_name",   "last_name"),
        ("email",       "email"),
        ("user_type",   "designation"),
        ("status",      "status"),
    ]:
        if data.get(field) is not None:
            doc[dest] = data[field]

    phone = data.get("phone_number") or data.get("phone")
    if phone is not None:
        doc["phone"] = phone

    extra = [
        "dob", "gender_id", "country_id", "county_id", "eir_code", "address",
        "experience_year", "experience_month", "masters", "travel_mode",
        "company_name", "job_title", "company_dial_code", "company_phone",
        "last_company_experience_year", "last_company_experience_month",
        "company_county_id", "permission_to_work", "pps_number",
        "work_permit_exemption", "visa_type_id", "uniform_size",
        "tuberculosis_vaccine", "hepatitis_antibody", "mmr_vaccine",
        "covid_19_vaccine", "face_verification_status", "recruitment_status",
        "user_sub_type_ids", "location", "tags", "banned_clients", "references",
    ]
    for f in extra:
        if f in data:
            doc[f] = data[f]

    return doc


async def _upsert_user(xn_user_id: str, update_doc: dict, now: datetime) -> dict:
    db = _get_db()
    existing = await db["users"].find_one({"xn_user_id": xn_user_id})
    if existing:
        await db["users"].update_one({"xn_user_id": xn_user_id}, {"$set": update_doc})
        return {"action": "updated", "user_id": str(existing["_id"])}
    else:
        new_doc = {**update_doc, "xn_user_id": xn_user_id,
                   "is_admin": False, "is_active": True, "created_at": now}
        result = await db["users"].insert_one(new_doc)
        return {"action": "inserted", "user_id": str(result.inserted_id)}


# ── Pydantic model — use alias so JSON body still uses "_id" ──────────────────

class RecruitmentDetailRequest(BaseModel):
    xn_id: str = Field(..., alias="_id")

    model_config = {"populate_by_name": True}


def _error(msg: str, status: int = 500, url: str = "", upstream: Any = None) -> dict:
    return {
        "success":      False,
        "status_code":  status,
        "upstream_url": url,
        "message":      msg,
        "data":         upstream,
        "sync":         None,
    }


# ── Endpoint — no auth required ───────────────────────────────────────────────

@router.post("/detail", summary="Fetch recruitment detail and sync to users collection")
@limiter.limit("60/minute")
async def recruitment_detail(request: Request, payload: RecruitmentDetailRequest):
    """
    POST body: { "_id": "<xn_user_id>" }
    No authentication required.
    Fetches from {USER_API_URL}ai/recruitments/detail using USER_EXTERNAL_API_KEY,
    then upserts to users collection by xn_user_id.
    """
    xn_user_id = payload.xn_id.strip()
    if not xn_user_id:
        return _error("_id is required", 400)

    url = f"{settings.USER_API_URL.rstrip('/')}/ai/recruitments/detail"
    body = {"_id": xn_user_id}

    upstream: Any = None
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, json=body, headers=_external_api_headers())

        # Always try to parse upstream response
        try:
            upstream = response.json()
        except Exception:
            upstream = response.text

        if response.status_code != 200:
            msg = (upstream.get("message") if isinstance(upstream, dict) else None) \
                  or f"Upstream returned HTTP {response.status_code}"
            return _error(
                f"User API error ({response.status_code}): {msg}",
                response.status_code, url, upstream,
            )

        raw  = upstream if isinstance(upstream, dict) else {}
        data = raw.get("data") or {}

        if not data:
            return _error(
                raw.get("message") or "No data returned from upstream API",
                200, url, raw,
            )

        now         = datetime.now(timezone.utc)
        update_doc  = _map_user_fields(data, now)
        sync_result = await _upsert_user(xn_user_id, update_doc, now)

        logger.info(f"Recruitment sync: xn_user_id={xn_user_id} {sync_result['action']}")

        return {
            "success":      True,
            "status_code":  200,
            "upstream_url": url,
            "message":      raw.get("message") or "Recruitment detail",
            "data":         data,
            "sync": {
                "xn_user_id":     xn_user_id,
                "action":         sync_result["action"],
                "user_id":        sync_result["user_id"],
                "fields_updated": list(update_doc.keys()),
            },
        }

    except httpx.TimeoutException:
        return _error("Request to User API timed out after 30 seconds", 504, url, upstream)
    except httpx.RequestError as e:
        return _error(f"Could not connect to User API: {e}", 502, url, upstream)
    except Exception as e:
        logger.error(f"recruitment/detail unexpected error: {e}", exc_info=True)
        return _error(f"Internal error: {e}", 500, url, upstream)
