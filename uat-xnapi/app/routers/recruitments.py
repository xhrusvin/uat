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

    # Save rating — default to 0 if null
    doc["rating"] = data.get("rating") or 0

    # QQI status fields
    if data.get("qqi_status_number") is not None:
        doc["qqi_status_number"] = data.get("qqi_status_number")
    if data.get("qqi_status_name") is not None:
        doc["qqi_status_name"] = data.get("qqi_status_name")

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
        "user_sub_type_ids", "location", "tags", "banned_clients", "references", "source",
    ]
    for f in extra:
        if f in data:
            doc[f] = data[f]

    return doc


async def _upsert_user(xn_user_id: str, update_doc: dict, now: datetime) -> dict:
    db = _get_db()
    existing = await db["users"].find_one({"xn_user_id": xn_user_id})
    if existing:
        SKIP_FIELDS = {
            "call_sent", "garda_email_sent", "follow_up_sent",
            "onboarded", "professional_reference_call_sent",
            "compliance_documents_status",
        }
        filtered_doc = {k: v for k, v in update_doc.items() if k not in SKIP_FIELDS}
        await db["users"].update_one({"xn_user_id": xn_user_id}, {"$set": filtered_doc})
        return {"action": "updated", "user_id": str(existing["_id"])}
    else:
        new_doc = {
            **update_doc,
            "xn_user_id":       xn_user_id,
            "is_admin":         False,
            "is_active":        True,
            # "call_sent":                       1,
            # "garda_email_sent":                1,
            # "follow_up_sent":                  1,
            # "onboarded":                       0,
            # "professional_reference_call_sent": 1,
            # "compliance_documents_status":      1,
            "created_at":       now,
        }
        result = await db["users"].insert_one(new_doc)
        return {"action": "inserted", "user_id": str(result.inserted_id)}


# ── Pydantic model — use alias so JSON body still uses "_id" ──────────────────

class RecruitmentDetailRequest(BaseModel):
    xn_id: str = Field(..., alias="_id")

    model_config = {"populate_by_name": True}


class UserDocumentListRequest(BaseModel):
    staff_id:    str = Field(..., description="XN user / staff id")
    document_id: Optional[str] = Field(None, description="Optional single document id")


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
            response = await client.get(url, params=body, headers=_external_api_headers())

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
        db          = _get_db()

        # Resolve user_sub_type_ids names → ObjectIds from user_sub_types collection
        raw_sub_ids = update_doc.get("user_sub_type_ids") or []
        if raw_sub_ids and isinstance(raw_sub_ids, list):
            resolved_ids = []
            import re as _re
            for item in raw_sub_ids:
                name = item.strip() if isinstance(item, str) else None
                if not name:
                    continue
                # Escape special regex chars (apostrophes, brackets etc)
                escaped = _re.escape(name)
                sub = await db["user_sub_types"].find_one(
                    {"name": {"$regex": f"^{escaped}$", "$options": "i"}},
                    {"_id": 1}
                )
                if not sub:
                    # Fallback: partial match
                    sub = await db["user_sub_types"].find_one(
                        {"name": {"$regex": escaped, "$options": "i"}},
                        {"_id": 1}
                    )
                if sub:
                    resolved_ids.append(sub["_id"])
            if resolved_ids:
                update_doc["user_sub_type_oids"] = resolved_ids

        sync_result = await _upsert_user(xn_user_id, update_doc, now)
        # Clear exclusion cache so it's recomputed next list call
        await db["users"].update_one(
            {"xn_user_id": xn_user_id},
            {"$unset": {"exclusion_cache": "", "exclusion_cache_at": ""}}
        )

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


@router.post("/user-document-list", summary="Fetch a staff member's documents from the User API")
@limiter.limit("60/minute")
async def user_document_list(request: Request, payload: UserDocumentListRequest):
    """
    POST body: { "staff_id": "<xn_user_id>", "document_id": "<document_id>" }

    Proxies GET {USER_API_URL}ai/recruitments/user-document-list using
    USER_EXTERNAL_API_KEY and returns the staff member's name, email and
    document list (type, status, signed url, expiry and last update).
    """
    staff_id = (payload.staff_id or "").strip()
    if not staff_id:
        return _error("staff_id is required", 400)

    url  = f"{settings.USER_API_URL.rstrip('/')}/ai/recruitments/user-document-list"
    body = {"staff_id": staff_id}
    if payload.document_id and payload.document_id.strip():
        body["document_id"] = payload.document_id.strip()

    upstream: Any = None
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=body, headers=_external_api_headers())

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

        documents = data.get("documents") or []
        if not isinstance(documents, list):
            documents = []

        results = []
        for d in documents:
            if not isinstance(d, dict):
                continue
            results.append({
                "document_id":            str(d.get("document_id") or ""),
                "document_type_name":     d.get("document_type_name"),
                "document_category_type": d.get("document_category_type"),
                "sub_type_id":            d.get("sub_type_id"),
                "sub_type_name":          d.get("sub_type_name"),
                "url":                    d.get("url"),
                "status":                 d.get("status"),
                "expiry_date":            d.get("expiry_date"),
                "updated_at":             d.get("updated_at"),
            })

        logger.info(
            f"user-document-list: staff_id={staff_id} "
            f"document_id={payload.document_id} docs={len(results)}"
        )

        return {
            "success":      True,
            "status_code":  200,
            "upstream_url": url,
            "message":      raw.get("message") or "User Document List",
            "data": {
                "id":        str(data.get("id") or staff_id),
                "name":      data.get("name"),
                "email":     data.get("email"),
                "documents": results,
            },
        }

    except httpx.TimeoutException:
        return _error("Request to User API timed out after 30 seconds", 504, url, upstream)
    except httpx.RequestError as e:
        return _error(f"Could not connect to User API: {e}", 502, url, upstream)
    except Exception as e:
        logger.error(f"recruitments/user-document-list unexpected error: {e}", exc_info=True)
        return _error(f"Internal error: {e}", 500, url, upstream)
