import logging
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, Query, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/staff", tags=["Staff"])


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
        else:
            result[k] = v
    return result


@router.get(
    "/",
    summary="List enabled staff with county name",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def list_staff(
    request: Request,
    skip:        int           = Query(0, ge=0),
    limit:       int           = Query(20, ge=1, le=200),
    page:        Optional[int] = Query(None, ge=1),
    per_page:    Optional[int] = Query(None, ge=1, le=200),
    search:      Optional[str] = Query(None),
    designation: Optional[str] = Query(None),
    county_id:   Optional[str] = Query(None),
):
    db = _get_db()

    # page/per_page → skip/limit
    if page is not None and per_page is not None:
        skip  = (page - 1) * per_page
        limit = per_page
    elif page is not None:
        skip = (page - 1) * limit
    elif per_page is not None:
        limit = per_page

    filters: list = [{"status": "Enabled"}]

    if search:
        filters.append({"$or": [
            {"first_name":  {"$regex": search, "$options": "i"}},
            {"last_name":   {"$regex": search, "$options": "i"}},
            {"email":       {"$regex": search, "$options": "i"}},
            {"phone":       {"$regex": search, "$options": "i"}},
            {"xn_user_id":  {"$regex": search, "$options": "i"}},
            {"designation": {"$regex": search, "$options": "i"}},
        ]})

    if designation:
        filters.append({"designation": {"$regex": designation, "$options": "i"}})

    if county_id and ObjectId.is_valid(county_id):
        filters.append({"county_id": {"$in": [ObjectId(county_id), county_id]}})

    mongo_filter = {"$and": filters}

    total = await db["users"].count_documents(mongo_filter)
    docs  = await db["users"].find(
        mongo_filter,
        {
            "_id": 1, "xn_user_id": 1,
            "first_name": 1, "last_name": 1,
            "email": 1, "phone": 1,
            "designation": 1, "status": 1,
            "county_id": 1, "rating": 1,
        }
    ).sort("first_name", 1).skip(skip).limit(limit).to_list(length=limit)

    # Batch county lookup
    county_ids = []
    for d in docs:
        cid = d.get("county_id")
        if cid:
            oid = ObjectId(str(cid)) if ObjectId.is_valid(str(cid)) else None
            if oid:
                county_ids.append(oid)

    county_map: dict = {}
    if county_ids:
        async for c in db["county"].find(
            {"_id": {"$in": county_ids}},
            {"name": 1}
        ):
            county_map[str(c["_id"])] = c.get("name") or "—"

    results = []
    for d in docs:
        cid = d.get("county_id")
        cid_str = str(cid) if cid else None
        results.append({
            "id":          str(d["_id"]),
            "xn_user_id":  d.get("xn_user_id"),
            "name":        " ".join(filter(None, [d.get("first_name",""), d.get("last_name","")])).strip() or "—",
            "first_name":  d.get("first_name"),
            "last_name":   d.get("last_name"),
            "email":       d.get("email"),
            "phone":       d.get("phone"),
            "designation": d.get("designation"),
            "status":      d.get("status"),
            "county_id":   cid_str,
            "county_name": county_map.get(cid_str, "—") if cid_str else "—",
            "rating":      d.get("rating"),
        })

    return {
        "success": True,
        "total":   total,
        "skip":    skip,
        "limit":   limit,
        "data":    results,
    }
