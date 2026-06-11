import logging
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/shifts-db", tags=["Shifts DB"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize(doc: dict) -> dict:
    """Recursively convert ObjectId and datetime to JSON-safe types."""
    if doc is None:
        return {}
    result = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            result[k] = str(v)
        elif hasattr(v, "isoformat"):          # datetime
            result[k] = v.isoformat()
        elif isinstance(v, dict):
            result[k] = _serialize(v)
        elif isinstance(v, list):
            result[k] = [
                _serialize(i) if isinstance(i, dict)
                else str(i) if isinstance(i, ObjectId)
                else i.isoformat() if hasattr(i, "isoformat")
                else i
                for i in v
            ]
        else:
            result[k] = v
    return result


async def _enrich_with_client(shift: dict, db) -> dict:
    """Look up client name from clients collection by client_id."""
    client_id = shift.get("client_id")
    if client_id:
        try:
            oid = ObjectId(client_id) if ObjectId.is_valid(client_id) else None
            query = {"$or": [{"_id": oid}, {"_id": client_id}]} if oid else {"_id": client_id}
            client = await db["clients"].find_one(query, {"name": 1, "email": 1, "phone": 1})
            if client:
                shift["client_name"]  = client.get("name") or client.get("title") or "—"
                shift["client_email"] = client.get("email")
                shift["client_phone"] = client.get("phone")
            else:
                shift["client_name"] = "—"
        except Exception:
            shift["client_name"] = "—"
    else:
        shift["client_name"] = "—"
    return shift


# ── LIST ──────────────────────────────────────────────────────────────────────

@router.get(
    "/",
    summary="List shifts from DB with client name",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def list_shifts_db(
    request: Request,
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
    search: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_to: Optional[str] = Query(None, description="YYYY-MM-DD"),
):
    db = _get_db()
    filters: list = []

    if search:
        filters.append({"$or": [
            {"name":          {"$regex": search, "$options": "i"}},
            {"shift_xn_id":   {"$regex": search, "$options": "i"}},
            {"shift_code":    {"$regex": search, "$options": "i"}},
            {"location":      {"$regex": search, "$options": "i"}},
            {"user_type":     {"$regex": search, "$options": "i"}},
            {"assigned_staff":{"$regex": search, "$options": "i"}},
        ]})

    if status:
        filters.append({"status": {"$regex": status, "$options": "i"}})

    if date_from or date_to:
        from datetime import datetime, timezone
        date_cond: dict = {}
        if date_from:
            try:
                dt = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                date_cond["$gte"] = dt
            except ValueError:
                pass
        if date_to:
            try:
                dt = datetime.strptime(date_to, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59, tzinfo=timezone.utc)
                date_cond["$lte"] = dt
            except ValueError:
                pass
        if date_cond:
            filters.append({"$or": [
                {"date": date_cond},
                {"date": {"$regex": (date_from or ""), "$options": "i"}}
            ]})

    mongo_filter = {"$and": filters} if filters else {}

    total = await db["shifts"].count_documents(mongo_filter)
    cursor = db["shifts"].find(mongo_filter).sort("date", -1).skip(skip).limit(limit)
    docs = await cursor.to_list(length=limit)

    # Batch client lookup — collect unique client_ids
    client_ids = list({d.get("client_id") for d in docs if d.get("client_id")})
    client_map: dict = {}
    if client_ids:
        valid_oids = [ObjectId(c) for c in client_ids if ObjectId.is_valid(c)]
        async for cl in db["clients"].find(
            {"_id": {"$in": valid_oids}},
            {"name": 1, "title": 1, "email": 1, "phone": 1}
        ):
            client_map[str(cl["_id"])] = cl

    results = []
    for doc in docs:
        s = _serialize(doc)
        cid = s.get("client_id", "")
        cl = client_map.get(cid)
        s["client_name"]  = (cl.get("name") or cl.get("title") or "—") if cl else "—"
        s["client_email"] = cl.get("email") if cl else None
        s["client_phone"] = cl.get("phone") if cl else None
        results.append(s)

    return {"success": True, "total": total, "skip": skip, "limit": limit, "data": results}


# ── GET single ────────────────────────────────────────────────────────────────

@router.get(
    "/{shift_id}",
    summary="Get a single shift with full details and client name",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def get_shift_db(request: Request, shift_id: str):
    db = _get_db()

    # Try by ObjectId first, then by shift_xn_id / shift_code
    doc = None
    if ObjectId.is_valid(shift_id):
        doc = await db["shifts"].find_one({"_id": ObjectId(shift_id)})
    if not doc:
        doc = await db["shifts"].find_one({"$or": [
            {"shift_xn_id": shift_id},
            {"shift_code":  shift_id},
        ]})
    if not doc:
        raise HTTPException(status_code=404, detail="Shift not found")

    s = _serialize(doc)

    # Enrich with client
    cid = s.get("client_id", "")
    if cid:
        cl = None
        if ObjectId.is_valid(cid):
            cl = await db["clients"].find_one({"_id": ObjectId(cid)}, {"name": 1, "title": 1, "email": 1, "phone": 1, "address": 1})
        if cl:
            s["client_name"]    = cl.get("name") or cl.get("title") or "—"
            s["client_email"]   = cl.get("email")
            s["client_phone"]   = cl.get("phone")
            s["client_address"] = cl.get("address")
        else:
            s["client_name"] = "—"

    return {"success": True, "data": s}
