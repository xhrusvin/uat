import logging
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from fastapi import APIRouter, Depends, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key
from app.schemas.shift import ShiftListRequest

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/shifts", tags=["Shifts"])


def _get_collection():
    """Get shifts collection fresh each call — avoids stale _client reference."""
    from app.db.database import _client
    return _client[settings.MONGODB_DB]["shifts"]


def _parse_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%d-%m-%Y").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _map_status(status_name: Optional[str]) -> str:
    mapping = {
        "to be filled":        "To be assigned",
        "upcoming":            "Upcoming",
        "cancelled by client": "Cancelled",
        "cancelled by staff":  "Cancelled",
        "completed":           "Completed",
        "in progress":         "In Progress",
    }
    return mapping.get((status_name or "").lower(), status_name or "To be assigned")


def _build_doc(item: dict, now: datetime) -> dict:
    start_time, end_time = "", ""
    timing = item.get("shift_timing") or ""
    if "(" in timing and "-" in timing:
        try:
            times_part = timing.split("(")[1].rstrip(")")
            parts = [t.strip() for t in times_part.split("-")]
            if len(parts) == 2:
                start_time, end_time = parts[0], parts[1]
        except Exception:
            pass

    date_obj = _parse_date(item.get("date"))

    return {
        "name":               item.get("shift_code", ""),
        "slots":              [{
            "date":        date_obj,
            "start_time":  start_time,
            "end_time":    end_time,
            "shift_xn_id": item.get("shift_code", ""),
            "shift_type":  timing.split("(")[0].strip() if "(" in timing else timing,
        }],
        "date":               date_obj,
        "start_time":         start_time,
        "end_time":           end_time,
        "shift_xn_id":        item.get("shift_code", ""),
        "description":        "",
        "client_id":          item.get("client_id", ""),
        "client_type":        item.get("type_of_client") or "Private",
        "location":           item.get("location") or item.get("client_county") or "",
        "postal_code":        None,
        "is_active":          True,
        "is_premium":         (item.get("type") or "").lower() == "premium",
        "status":             _map_status(item.get("status_name")),
        "rate":               item.get("pay_rate"),
        "shift_id":           item.get("shift_id", ""),
        "shift_code":         item.get("shift_code"),
        "shift_timing":       item.get("shift_timing"),
        "user_type":          item.get("user_type"),
        "unit":               item.get("unit"),
        "client_county":      item.get("client_county"),
        "assigned_staff":     item.get("assigned_staff"),
        "staff_email":        item.get("staff_email"),
        "booking_type":       item.get("booking_type"),
        "created_by":         item.get("created_by"),
        "upstream_status":    item.get("status_name"),
        "upstream_status_id": item.get("status"),
        "updated_at":         now,
    }


async def _upsert_shifts(items: list) -> dict:
    collection = _get_collection()
    now = datetime.now(timezone.utc)
    inserted = updated = skipped = 0

    for item in items:
        if not isinstance(item, dict):
            skipped += 1
            continue
        shift_id = item.get("shift_code")
        if not shift_id:
            skipped += 1
            continue
        try:
            doc = _build_doc(item, now)
            existing = await collection.find_one({"shift_xn_id": shift_id})
            if existing:
                await collection.update_one({"shift_xn_id": shift_id}, {"$set": doc})
                updated += 1
            else:
                doc["created_at"] = now
                await collection.insert_one(doc)
                inserted += 1
        except Exception as e:
            logger.error(f"Upsert error for shift {shift_id}: {e}")
            skipped += 1

    return {"inserted": inserted, "updated": updated, "skipped": skipped}


@router.post(
    "/list",
    summary="Fetch shifts from XpressHealth Shift API and sync to DB",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def list_shifts(request: Request, payload: ShiftListRequest):
    url = f"{settings.SHIFT_URL.rstrip('/')}/ai/shifts/list"
    headers = {
        "Api-Key": settings.SHIFT_INTERNAL_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    # Map criteria to the upstream search field name
    CRITERIA_FIELD = {
        "User Type":         "user_type",
        "Automation Status": "automation_status",
        "County":            "client_county",
        "Client":            "location",
    }

    body: dict = {
        "search":     payload.search,
        "page":       payload.page,
        "per_page":   payload.per_page,
        "sort_by":    payload.sort_by,
        "sort_order": payload.sort_order,
    }
    if payload.start_date:
        body["start_date"] = payload.start_date
    if payload.end_date:
        body["end_date"] = payload.end_date
    # Pass criteria to upstream if provided and search is non-empty
    if payload.criteria and payload.search:
        field = CRITERIA_FIELD.get(payload.criteria)
        if field:
            body["criteria"] = payload.criteria
            body["search_field"] = field

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=body, headers=headers)

        try:
            upstream: Any = response.json()
        except Exception:
            upstream = response.text

        if response.status_code != 200:
            msg = None
            if isinstance(upstream, dict):
                msg = upstream.get("message") or upstream.get("error") or upstream.get("detail")
            return {
                "success":      False,
                "status_code":  response.status_code,
                "upstream_url": url,
                "message":      f"Shift API error ({response.status_code}): {msg or upstream}",
                "data":         upstream,
                "sync":         None,
            }

        # Extract nested data: { data: { data: [...], total_count, ... } }
        raw      = upstream if isinstance(upstream, dict) else {}
        inner    = raw.get("data") or {}
        if isinstance(inner, dict):
            shifts_list  = inner.get("data") or []
            total_count  = inner.get("total_count") or 0
            current_page = inner.get("current_page") or payload.page
            per_page     = inner.get("per_page") or payload.per_page
        else:
            shifts_list  = inner if isinstance(inner, list) else []
            total_count  = len(shifts_list)
            current_page = payload.page
            per_page     = payload.per_page

        # Upsert to MongoDB
        sync_result = await _upsert_shifts(shifts_list)
        logger.info(f"Shift sync: fetched={len(shifts_list)} {sync_result}")

        return {
            "success":      True,
            "status_code":  200,
            "upstream_url": url,
            "message":      raw.get("message") or "Shift list",
            "data":         shifts_list,
            "total":        total_count,
            "page":         current_page,
            "per_page":     per_page,
            "sync":         {"fetched": len(shifts_list), **sync_result},
        }

    except httpx.TimeoutException:
        return {"success": False, "status_code": 504, "upstream_url": url,
                "message": "Shift API timed out after 30 seconds.", "data": None, "sync": None}
    except httpx.RequestError as e:
        return {"success": False, "status_code": 502, "upstream_url": url,
                "message": f"Could not connect to Shift API: {e}", "data": None, "sync": None}
    except Exception as e:
        logger.error(f"shifts/list unexpected error: {e}", exc_info=True)
        return {"success": False, "status_code": 500, "upstream_url": url,
                "message": f"Internal error: {e}", "data": None, "sync": None}


# ── POST /shifts/sync-detail ──────────────────────────────────────────────────

class ShiftSyncDetailRequest(BaseModel):
    id: str   # xn shift id e.g. "69c2679dd3565ae372023eb6"


@router.post(
    "/sync-detail",
    summary="Fetch single shift from XpressHealth API and upsert to DB",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def sync_shift_detail(request: Request, payload: ShiftSyncDetailRequest):
    """
    Body: { "id": "<xn_shift_id>" }
    Calls SHIFT_URL/ai/shifts/detail, maps response and upserts to shifts collection
    matching on shifts.shift_id == response.data.id
    """
    url = f"{settings.SHIFT_URL.rstrip('/')}/ai/shifts/detail"
    headers = {
        "Api-Key": settings.SHIFT_INTERNAL_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(url, json={"id": payload.id}, headers=headers)

    if resp.status_code != 200:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Upstream API error: {resp.text[:300]}"
        )

    body = resp.json()
    if not body.get("success"):
        raise HTTPException(status_code=502, detail=body.get("message", "Upstream error"))

    data = body.get("data", {})
    if not data:
        raise HTTPException(status_code=404, detail="No shift data returned")

    now = datetime.now(timezone.utc)

    # Parse date
    date_obj = _parse_date(data.get("date"))

    # Parse times
    start_time = data.get("scheduled_start_time", "")[:5] if data.get("scheduled_start_time") else ""
    end_time   = data.get("scheduled_end_time", "")[:5]   if data.get("scheduled_end_time")   else ""

    client_details = data.get("client_details") or {}
    staff_details  = data.get("staff") or {}

    doc = {
        "shift_id":           data.get("id", ""),
        "shift_code":         data.get("shift_code", ""),
        "shift_xn_id":        data.get("shift_code", ""),
        "name":               data.get("shift_code", ""),
        "date":               date_obj,
        "start_time":         start_time,
        "end_time":           end_time,
        "shift_timing":       data.get("shift", ""),
        "user_type":          data.get("user_type"),
        "user_type_id":       data.get("user_type_id"),
        "is_premium":         (data.get("type") or "").lower() == "premium",
        "status":             _map_status(data.get("status_name")),
        "upstream_status":    data.get("status_name"),
        "upstream_status_id": data.get("status"),
        "round":              data.get("round"),
        "pay_rate":           data.get("pay_rate"),
        "client_id":          client_details.get("id", ""),
        "client_name":        client_details.get("name"),
        "client_county":      client_details.get("county"),
        "unit":               client_details.get("unit"),
        "assigned_staff":     staff_details.get("name"),
        "staff_id":           staff_details.get("id"),
        "slots": [{
            "date":        date_obj,
            "start_time":  start_time,
            "end_time":    end_time,
            "shift_xn_id": data.get("shift_code", ""),
            "shift_type":  data.get("shift", ""),
        }],
        "updated_at": now,
    }

    collection = _get_collection()
    # Match on shift_id (xn id string)
    existing = await collection.find_one({"shift_id": data["id"]})
    if existing:
        await collection.update_one({"shift_id": data["id"]}, {"$set": doc})
        action = "updated"
    else:
        # Also try matching by shift_code
        existing_code = await collection.find_one({"shift_xn_id": data.get("shift_code")})
        if existing_code:
            await collection.update_one({"shift_xn_id": data["shift_code"]}, {"$set": doc})
            action = "updated"
        else:
            doc["created_at"] = now
            await collection.insert_one(doc)
            action = "inserted"

    return {
        "success": True,
        "action":  action,
        "shift_id": data.get("id"),
        "shift_code": data.get("shift_code"),
        "data": {
            "shift_id":    data.get("id"),
            "shift_code":  data.get("shift_code"),
            "date":        data.get("date"),
            "start_time":  start_time,
            "end_time":    end_time,
            "user_type":   data.get("user_type"),
            "status":      data.get("status_name"),
            "client":      client_details.get("name"),
            "staff":       staff_details.get("name"),
            "is_premium":  (data.get("type") or "").lower() == "premium",
        },
    }
