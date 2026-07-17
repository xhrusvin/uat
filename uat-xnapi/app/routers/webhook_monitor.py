import logging
from datetime import datetime, timezone
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router  = APIRouter(prefix="/webhook", tags=["Webhook Monitor"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


class WebhookListRequest(BaseModel):
    page:     int = 1
    per_page: int = 20


@router.post(
    "/document-uploaded",
    summary="List uploaded_documents with user details from users.xn_user_id",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def list_document_uploaded(request: Request, payload: WebhookListRequest):
    db   = _get_db()
    skip = (payload.page - 1) * payload.per_page

    total = await db["uploaded_documents"].count_documents({})
    docs  = await db["uploaded_documents"].find({}) \
        .sort("uploaded_at", -1) \
        .skip(skip).limit(payload.per_page) \
        .to_list(length=payload.per_page)

    # Batch user lookup by xn_user_id
    user_ids = list({str(d.get("user_id", "")) for d in docs if d.get("user_id")})
    user_map: dict = {}
    if user_ids:
        async for u in db["users"].find(
            {"xn_user_id": {"$in": user_ids}},
            {"xn_user_id": 1, "first_name": 1, "last_name": 1, "email": 1, "phone": 1}
        ):
            user_map[str(u.get("xn_user_id", ""))] = u

    results = []
    for d in docs:
        uid = str(d.get("user_id", ""))
        u   = user_map.get(uid)  # may be None — leave blank if not found

        def _fmt(dt):
            return dt.isoformat() if dt and hasattr(dt, "isoformat") else str(dt) if dt else None

        results.append({
            "id":          str(d["_id"]),
            "user_id":     uid,
            "document_id": str(d.get("document_id", "")),
            "uploaded_at": _fmt(d.get("uploaded_at")),
            "status":      d.get("status", "uploaded"),
            # User details — None if not found in users collection
            "name":        " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() if u else None,
            "email":       u.get("email") if u else None,
            "phone":       u.get("phone") if u else None,
        })

    return {
        "success":  True,
        "total":    total,
        "page":     payload.page,
        "per_page": payload.per_page,
        "data":     results,
    }
