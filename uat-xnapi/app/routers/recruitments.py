import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from bson import ObjectId
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

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
    """
    Map upstream recruitment detail response to users collection fields.
    Existing users schema:
      email, password, first_name, last_name, phone, is_admin, status,
      xn_user_id, designation, created_at, updated_at, call_sent,
      garda_email_sent, follow_up_sent, onboarded
    """
    doc: dict = {
        "updated_at": now,
        "synced_at":  now,
    }

    # ── Core identity ──────────────────────────────────────────────────────────
    if data.get("first_name") is not None:
        doc["first_name"] = data["first_name"]
    if data.get("last_name") is not None:
        doc["last_name"]  = data["last_name"]
    if data.get("email") is not None:
        doc["email"]      = data["email"]

    phone = data.get("phone_number") or data.get("phone")
    if phone is not None:
        doc["phone"] = phone

    # ── Designation / role ────────────────────────────────────────────────────
    if data.get("user_type") is not None:
        doc["designation"] = data["user_type"]

    # ── Status ────────────────────────────────────────────────────────────────
    if data.get("status") is not None:
        doc["status"] = data["status"]

    # ── Extra recruitment fields — stored directly ────────────────────────────
    extra_fields = [
        "dob", "gender_id", "country_id", "county_id", "eir_code", "address",
        "experience_year", "experience_month", "masters", "travel_mode",
        "company_name", "job_title", "company_dial_code", "company_phone",
        "last_company_experience_year", "last_company_experience_month",
        "company_county_id", "permission_to_work", "pps_number",
        "work_permit_exemption", "visa_type_id", "uniform_size",
        "tuberculosis_vaccine", "hepatitis_antibody", "mmr_vaccine",
        "covid_19_vaccine", "face_verification_status", "recruitment_status",
        "user_sub_type_ids", "location", "tags", "banned_clients",
        "references",
    ]
    for field in extra_fields:
        if field in data:
            doc[field] = data[field]

    return doc


async def _upsert_user(xn_user_id: str, update_doc: dict, now: datetime) -> dict:
    """
    Find user by xn_user_id.
    - Exists  → update fields
    - Missing → insert new minimal user doc
    """
    db = _get_db()
    existing = await db["users"].find_one({"xn_user_id": xn_user_id})

    if existing:
        await db["users"].update_one(
            {"xn_user_id": xn_user_id},
            {"$set": update_doc}
        )
        return {"action": "updated", "user_id": str(existing["_id"])}
    else:
        # Insert new user — include xn_user_id and created_at
        new_doc = {
            **update_doc,
            "xn_user_id":  xn_user_id,
            "is_admin":    False,
            "is_active":   True,
            "created_at":  now,
        }
        result = await db["users"].insert_one(new_doc)
        return {"action": "inserted", "user_id": str(result.inserted_id)}


# ── Request schema ────────────────────────────────────────────────────────────

class RecruitmentDetailRequest(BaseModel):
    _id: str  # xn_user_id


# ── Endpoint ──────────────────────────────────────────────────────────────────

@router.post(
    "/detail",
    summary="Fetch recruitment detail from User API and sync to users collection",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def recruitment_detail(request: Request, payload: RecruitmentDetailRequest):
    """
    POST body: { "_id": "<xn_user_id>" }

    Fetches from {USER_API_URL}ai/recruitments/detail using USER_EXTERNAL_API_KEY.
    Updates the matching user in the users collection by xn_user_id.
    If no user found, creates a new one.
    """
    xn_user_id = payload._id.strip()
    if not xn_user_id:
        return {"success": False, "message": "_id is required", "data": None, "sync": None}

    url = f"{settings.USER_API_URL.rstrip('/')}/ai/recruitments/detail"
    body = {"_id": xn_user_id}

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, json=body, headers=_external_api_headers())

        try:
            upstream = response.json()
        except Exception:
            upstream = response.text

        if response.status_code != 200:
            msg = upstream.get("message") if isinstance(upstream, dict) else str(upstream)
            return {"success": False, "status_code": response.status_code,
                    "upstream_url": url, "message": msg, "data": upstream, "sync": None}

        raw  = upstream if isinstance(upstream, dict) else {}
        data = raw.get("data") or {}

        if not data:
            return {"success": False, "status_code": 200, "upstream_url": url,
                    "message": "No data returned from API", "data": raw, "sync": None}

        # Map fields and upsert
        now        = datetime.now(timezone.utc)
        update_doc = _map_user_fields(data, now)
        sync_result = await _upsert_user(xn_user_id, update_doc, now)

        logger.info(f"Recruitment detail sync: xn_user_id={xn_user_id} action={sync_result['action']}")

        return {
            "success":      True,
            "status_code":  200,
            "upstream_url": url,
            "message":      raw.get("message") or "Recruitment detail",
            "data":         data,
            "sync":         {
                "xn_user_id": xn_user_id,
                "action":     sync_result["action"],
                "user_id":    sync_result["user_id"],
                "fields_updated": list(update_doc.keys()),
            },
        }

    except httpx.TimeoutException:
        return {"success": False, "status_code": 504, "upstream_url": url,
                "message": "Request timed out", "data": None, "sync": None}
    except httpx.RequestError as e:
        return {"success": False, "status_code": 502, "upstream_url": url,
                "message": str(e), "data": None, "sync": None}
    except Exception as e:
        logger.error(f"recruitment/detail error: {e}", exc_info=True)
        return {"success": False, "status_code": 500, "upstream_url": url,
                "message": str(e), "data": None, "sync": None}
