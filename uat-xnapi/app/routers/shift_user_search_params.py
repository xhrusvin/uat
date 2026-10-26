import logging
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/shift-users/search-params", tags=["Shift User Search Params"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


# ── Schema ─────────────────────────────────────────────────────────────────────
# Mirrors ListShiftUsersRequest (shift_users.py) minus page/per_page,
# which are transient pagination controls that should not be persisted.

class ShiftUserSearchParams(BaseModel):
    """
    Persistable subset of ListShiftUsersRequest.
    All fields are optional so callers can save a partial filter.
    """
    shift_id:               Optional[str]   = None
    radius:                 Optional[float] = None
    order_by:               Optional[str]   = None
    sort:                   Optional[str]   = "asc"
    county_multiple:        Optional[list]  = None
    user_type_multiple:     Optional[list]  = None
    excluded:               Optional[int]   = None
    in_pool:                Optional[int]   = None
    search:                 Optional[str]   = None
    gender_id:              Optional[str]   = None
    gender_multiple:        Optional[list]  = None
    visa_type_id:           Optional[str]   = None
    group_id:               Optional[str]   = None
    qqi_status_number:      Optional[int]   = None
    user_sub_type_multiple: Optional[list]  = None


class SaveSearchParamsRequest(BaseModel):
    session_user_id: str                    # session_users.id  (upstream string ID)
    params:          ShiftUserSearchParams  # the filter state to persist
    label:           Optional[str] = None   # optional human-readable name, e.g. "Day nurses – Cork"


# ── Helpers ────────────────────────────────────────────────────────────────────

def _resolve_session_user(raw: str) -> str:
    """Strip and validate that session_user_id is a non-empty string."""
    val = (raw or "").strip()
    if not val:
        raise HTTPException(status_code=422, detail="session_user_id must not be empty")
    return val


# ── SAVE (upsert) ──────────────────────────────────────────────────────────────

@router.post(
    "/save",
    summary="Save (upsert) shift-users list search parameters for a session user",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def save_search_params(request: Request, payload: SaveSearchParamsRequest):
    """
    Upserts a `shift_user_search_params` document keyed on `session_user_id`.

    One document is kept per session user — calling this again overwrites the
    previous saved state for that user.  Pass `label` to give the preset a
    human-readable name (e.g. "Day nurses – Cork").

    Body example:
    ```json
    {
      "session_user_id": "67ee949503d9854bfb0e3b93",
      "label": "Night RNs within 30 km",
      "params": {
        "shift_id":           "664abc123def456789012345",
        "radius":             30,
        "order_by":           "distance_km",
        "sort":               "asc",
        "county_multiple":    ["664ccc000aaa111222333444"],
        "user_type_multiple": [],
        "excluded":           0,
        "in_pool":            null,
        "gender_id":          null,
        "visa_type_id":       null,
        "qqi_status_number":  null
      }
    }
    ```
    """
    db = _get_db()
    session_user_id = _resolve_session_user(payload.session_user_id)
    now = datetime.now(timezone.utc)

    # Verify the session user exists (non-blocking: warn only, don't reject)
    su_doc = await db["session_users"].find_one({"id": session_user_id}, {"id": 1, "name": 1})
    if not su_doc:
        logger.warning(f"[search-params/save] session_user_id={session_user_id} not found in session_users — saving anyway")

    params_dict = payload.params.model_dump(exclude_none=False)

    doc_update = {
        "session_user_id": session_user_id,
        "session_user_name": su_doc.get("name") if su_doc else None,
        "label":           payload.label,
        "params":          params_dict,
        "updated_at":      now,
    }

    result = await db["shift_user_search_params"].update_one(
        {"session_user_id": session_user_id},
        {
            "$set":         doc_update,
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )

    action = "created" if result.upserted_id else "updated"
    record_id = str(result.upserted_id) if result.upserted_id else None

    # Fetch the full saved doc to return it
    saved = await db["shift_user_search_params"].find_one(
        {"session_user_id": session_user_id},
        {"_id": 1, "session_user_id": 1, "session_user_name": 1,
         "label": 1, "params": 1, "created_at": 1, "updated_at": 1}
    )

    logger.info(f"[search-params/save] {action} session_user_id={session_user_id}")

    return {
        "success":    True,
        "action":     action,
        "message":    f"Search parameters {action} for session user {session_user_id}",
        "id":         record_id or (str(saved["_id"]) if saved else None),
        "data":       _serialize_doc(saved) if saved else doc_update,
    }


# ── GET ────────────────────────────────────────────────────────────────────────

@router.get(
    "/{session_user_id}",
    summary="Retrieve saved shift-users list search parameters for a session user",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("120/minute")
async def get_search_params(request: Request, session_user_id: str):
    """
    Returns the saved search parameters for `session_user_id`, or 404 if none
    have been saved yet.

    The `params` object is ready to be spread directly into a
    `POST /shift-users/list` request body (just add `page` and `per_page`).
    """
    db = _get_db()
    session_user_id = _resolve_session_user(session_user_id)

    doc = await db["shift_user_search_params"].find_one(
        {"session_user_id": session_user_id},
        {"_id": 1, "session_user_id": 1, "session_user_name": 1,
         "label": 1, "params": 1, "created_at": 1, "updated_at": 1}
    )

    if not doc:
        raise HTTPException(
            status_code=404,
            detail=f"No saved search parameters found for session_user_id={session_user_id}",
        )

    return {
        "success": True,
        "data":    _serialize_doc(doc),
    }


# ── DELETE ─────────────────────────────────────────────────────────────────────

@router.delete(
    "/{session_user_id}",
    summary="Delete saved shift-users list search parameters for a session user",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def delete_search_params(request: Request, session_user_id: str):
    """
    Removes the saved search parameters for `session_user_id`.
    Returns 404 if no record exists.
    """
    db = _get_db()
    session_user_id = _resolve_session_user(session_user_id)

    result = await db["shift_user_search_params"].delete_one(
        {"session_user_id": session_user_id}
    )

    if result.deleted_count == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No saved search parameters found for session_user_id={session_user_id}",
        )

    logger.info(f"[search-params/delete] deleted session_user_id={session_user_id}")

    return {
        "success":         True,
        "message":         f"Search parameters deleted for session user {session_user_id}",
        "session_user_id": session_user_id,
    }


# ── LIST (all saved presets — admin / debug) ───────────────────────────────────

@router.get(
    "/",
    summary="List all saved shift-users search parameter presets (admin)",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def list_search_params(request: Request, page: int = 1, per_page: int = 20):
    """
    Returns a paginated list of all saved presets across all session users.
    Useful for admin inspection or debugging.
    """
    db     = _get_db()
    skip   = max(0, (page - 1) * per_page)
    total  = await db["shift_user_search_params"].count_documents({})
    cursor = (
        db["shift_user_search_params"]
        .find(
            {},
            {"_id": 1, "session_user_id": 1, "session_user_name": 1,
             "label": 1, "params": 1, "created_at": 1, "updated_at": 1},
        )
        .sort("updated_at", -1)
        .skip(skip)
        .limit(per_page)
    )
    docs = [_serialize_doc(d) async for d in cursor]

    return {
        "success":  True,
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "data":     docs,
    }


# ── Serialiser ─────────────────────────────────────────────────────────────────

def _serialize_doc(doc: dict) -> dict:
    """Convert ObjectId / datetime fields to JSON-safe strings."""
    if not doc:
        return {}
    out = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        elif isinstance(v, dict):
            out[k] = _serialize_doc(v)
        elif isinstance(v, list):
            out[k] = [_serialize_doc(i) if isinstance(i, dict) else i for i in v]
        else:
            out[k] = v
    return out