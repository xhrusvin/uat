import logging
import math
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


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in km between two lat/lng points using Haversine formula."""
    R = 6371.0  # Earth radius in km
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi  = math.radians(lat2 - lat1)
    dlam  = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return round(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)), 2)


async def _get_shift_client_coords(db, shift_id: str) -> Optional[tuple]:
    """
    Given a shift _id string, return (lat, lng) of its client.
    shifts._id → shifts.client_id → clients.xn_client_id → clients.latitude/longitude
    """
    if not ObjectId.is_valid(shift_id):
        return None

    shift = await db["shifts"].find_one(
        {"_id": ObjectId(shift_id)},
        {"client_id": 1}
    )
    if not shift or not shift.get("client_id"):
        return None

    client = await db["clients"].find_one(
        {"xn_client_id": shift["client_id"]},
        {"latitude": 1, "longitude": 1}
    )
    if not client:
        return None

    lat = client.get("latitude")
    lng = client.get("longitude")
    if lat is None or lng is None:
        return None

    return (float(lat), float(lng))


def _user_coords(user: dict) -> Optional[tuple]:
    """Extract (lat, lng) from user.location dict."""
    loc = user.get("location")
    if isinstance(loc, dict):
        lat = loc.get("latitude") or loc.get("lat")
        lng = loc.get("longitude") or loc.get("lng") or loc.get("lon")
        if lat is not None and lng is not None:
            return (float(lat), float(lng))
    # Also try flat fields
    lat = user.get("latitude")
    lng = user.get("longitude")
    if lat is not None and lng is not None:
        return (float(lat), float(lng))
    return None


class StaffListRequest(BaseModel):
    search:      str = ""
    designation: Optional[str] = None
    county_id:   Optional[str] = None
    shift_id:    Optional[str] = None   # if provided, calculate distance to shift's client
    page:        int = 1
    per_page:    int = 20


@router.post(
    "/",
    summary="List enabled staff with county name and optional distance calculation",
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
        "shift_id": null,   <- optional: shifts._id to calculate distance km
        "page": 1,
        "per_page": 20
    }

    If shift_id is provided:
      shifts._id → shifts.client_id → clients.xn_client_id → clients.lat/lng
      vs users.location.latitude / users.location.longitude
      → distance_km returned per staff member
    """
    db   = _get_db()
    skip = (payload.page - 1) * payload.per_page

    # Resolve shift client coordinates if shift_id provided
    client_coords: Optional[tuple] = None
    shift_client_info: Optional[dict] = None
    if payload.shift_id:
        client_coords = await _get_shift_client_coords(db, payload.shift_id)
        if client_coords:
            shift_client_info = {
                "shift_id":          payload.shift_id,
                "client_latitude":   client_coords[0],
                "client_longitude":  client_coords[1],
            }

    filters: list = [{"status": "Enabled"}]

    if payload.search:
        filters.append({"$or": [
            {"first_name":  {"$regex": payload.search, "$options": "i"}},
            {"last_name":   {"$regex": payload.search, "$options": "i"}},
            {"email":       {"$regex": payload.search, "$options": "i"}},
            {"phone":       {"$regex": payload.search, "$options": "i"}},
            {"xn_user_id":  {"$regex": payload.search, "$options": "i"}},
            {"designation": {"$regex": payload.search, "$options": "i"}},
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
            "location": 1,
            "latitude": 1, "longitude": 1,
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

        # Calculate distance if client coords available
        distance_km = None
        if client_coords:
            ucoords = _user_coords(d)
            if ucoords:
                distance_km = _haversine_km(
                    client_coords[0], client_coords[1],
                    ucoords[0],       ucoords[1],
                )

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
            "distance_km": distance_km,
        })

    # Sort by distance if available
    if client_coords:
        results.sort(key=lambda x: x["distance_km"] if x["distance_km"] is not None else float("inf"))

    return {
        "success":     True,
        "total":       total,
        "page":        payload.page,
        "per_page":    payload.per_page,
        "shift_client": shift_client_info,
        "data":        results,
    }
