import logging
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
router = APIRouter(prefix="/staff", tags=["Staff"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize_oid(v):
    if isinstance(v, ObjectId):
        return str(v)
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return v


class StaffListRequest(BaseModel):
    search:      str = ""
    designation: Optional[str] = None
    county_id:   Optional[str] = None
    page:        int = 1
    per_page:    int = 20


@router.post(
    "/",
    summary="List enabled staff with county name",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def list_staff(request: Request, payload: StaffListRequest):
    """
    Body:
    {
        "search": "",
        "designation": null,
        "county_id": null,
        "page": 1,
        "per_page": 20
    }
    Returns users where status == 'Enabled', joined with county name.
    """
    db   = _get_db()
    skip = (payload.page - 1) * payload.per_page

    filters: list = [{"status": "Enabled"}]

    if payload.search:
        filters.append({"$or": [
            {"first_name":  {"$regex": payload.search, "$options": "i"}},
            {"last_name":   {"$regex": payload.search, "$options": "i"}},
            {"email":       {"$regex": payload.search, "$options": "i"}},
            {"phone":       {"$regex": payload.search, "$options": "i"}},
            {"xn_user_id":  {"$regex": payload.search, "$options": "i"}},
            {"designation": {"$regex": payload.search, "$options": "i"}},
            # Full name match: concatenate first + last with $expr
            {"$expr": {"$regexMatch": {
                "input": {"$concat": [
                    {"$ifNull": ["$first_name", ""]},
                    " ",
                    {"$ifNull": ["$last_name", ""]}
                ]},
                "regex": payload.search,
                "options": "i"
            }}},
        ]})

    if payload.designation:
        filters.append({"designation": {"$regex": payload.designation, "$options": "i"}})

    if payload.county_id and ObjectId.is_valid(payload.county_id):
        filters.append({"county_id": {
            "$in": [ObjectId(payload.county_id), payload.county_id]
        }})

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
    ).sort("first_name", 1).skip(skip).limit(payload.per_page).to_list(length=payload.per_page)

    # Batch county lookup
    county_oids = []
    for d in docs:
        cid = d.get("county_id")
        if cid and ObjectId.is_valid(str(cid)):
            county_oids.append(ObjectId(str(cid)))

    county_map: dict = {}
    if county_oids:
        async for c in db["county"].find({"_id": {"$in": county_oids}}, {"name": 1}):
            county_map[str(c["_id"])] = c.get("name") or "—"

    results = []
    for d in docs:
        cid     = d.get("county_id")
        cid_str = str(cid) if cid else None
        results.append({
            "id":          str(d["_id"]),
            "xn_user_id":  d.get("xn_user_id"),
            "name":        " ".join(filter(None, [
                              d.get("first_name", ""), d.get("last_name", "")
                           ])).strip() or "—",
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
        "success":  True,
        "total":    total,
        "page":     payload.page,
        "per_page": payload.per_page,
        "data":     results,
    }
