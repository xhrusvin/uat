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
    if doc is None:
        return {}
    result = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            result[k] = str(v)
        elif hasattr(v, "isoformat"):
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


async def _build_client_map(db, client_ids: list) -> dict:
    """
    Look up clients by both:
      - clients._id        (ObjectId)   — legacy local clients
      - clients.xn_client_id (string)   — clients synced from User API

    shifts.client_id stores the XN client ID string (e.g. "6921c52323d4e88656035a1d"),
    which maps to clients.xn_client_id, NOT clients._id.

    Returns a dict keyed by the xn_client_id / _id string.
    """
    client_map: dict = {}
    if not client_ids:
        return client_map

    projection = {"name": 1, "title": 1, "email": 1, "phone": 1, "xn_client_id": 1}

    # 1. Match by xn_client_id (primary join key)
    async for cl in db["clients"].find(
        {"xn_client_id": {"$in": client_ids}},
        projection,
    ):
        xn_id = cl.get("xn_client_id")
        if xn_id:
            client_map[str(xn_id)] = cl

    # 2. Also try matching by _id for any unresolved IDs (legacy local clients)
    unresolved = [cid for cid in client_ids if cid not in client_map]
    if unresolved:
        valid_oids = [ObjectId(c) for c in unresolved if ObjectId.is_valid(c)]
        if valid_oids:
            async for cl in db["clients"].find(
                {"_id": {"$in": valid_oids}},
                projection,
            ):
                client_map[str(cl["_id"])] = cl

    return client_map


def _client_name(cl: dict) -> str:
    if not cl:
        return "—"
    return cl.get("name") or cl.get("title") or "—"


# ── LIST ──────────────────────────────────────────────────────────────────────

@router.get(
    "/",
    summary="List shifts from DB with client name",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def list_shifts_db(
    request: Request,
    skip:       int           = Query(0, ge=0),
    limit:      int           = Query(20, ge=1, le=100),
    page:       Optional[int] = Query(None, ge=1, description="Page number (overrides skip)"),
    per_page:   Optional[int] = Query(None, ge=1, le=500, description="Items per page (overrides limit)"),
    search:     Optional[str] = Query(None),
    status:     Optional[str] = Query(None),
    date_from:  Optional[str] = Query(None, description="YYYY-MM-DD"),
    date_to:    Optional[str] = Query(None, description="YYYY-MM-DD"),
    start_date: Optional[str] = Query(None, description="Alias for date_from (YYYY-MM-DD)"),
    end_date:   Optional[str] = Query(None, description="Alias for date_to (YYYY-MM-DD)"),
    sort_by:    Optional[str] = Query("date", description="Sort field"),
    sort_order: Optional[str] = Query("desc", description="asc or desc"),
    client_id:        Optional[str] = Query(None),
    user_type:        Optional[str] = Query(None),
    automation_status: Optional[str] = Query(None),
    criteria:          Optional[str] = Query(None),
):
    db = _get_db()

    # ── Resolve param aliases ─────────────────────────────────────────────────
    # page/per_page → skip/limit
    if page is not None and per_page is not None:
        skip  = (page - 1) * per_page
        limit = per_page
    elif page is not None:
        skip = (page - 1) * limit
    elif per_page is not None:
        limit = per_page

    # start_date/end_date → date_from/date_to
    effective_date_from = date_from or start_date
    effective_date_to   = date_to   or end_date

    filters: list = []

    # ── Resolve criteria label → DB field ────────────────────────────────────
    # criteria param may be a label ("Client", "User Type") or a raw field name ("location")
    LABEL_TO_FIELD = {
        "User Type":         "user_type",
        "Automation Status": "automation_status",
        "County":            "client_county",
        "Client":            "location",
        "Client Tags":       "client_tags",
        "Shift Time":        "shift_timing",
        "Has Available":     "assigned_staff",
        "Shift Type":        "shift_type",
        "Distance":          "distance",
    }
    criteria_field: Optional[str] = None
    if criteria:
        if criteria in LABEL_TO_FIELD:
            # It's a label — map to field name
            criteria_field = LABEL_TO_FIELD[criteria]
        else:
            # Assume it's already a raw field name
            criteria_field = criteria
        # Also try to resolve from DB criteria collection
        try:
            cr_doc = await db["criteria"].find_one(
                {"$or": [{"label": criteria}, {"field": criteria}]},
                {"field": 1}
            )
            if cr_doc and cr_doc.get("field"):
                criteria_field = cr_doc["field"]
        except Exception:
            pass

    if search:
        if criteria_field:
            # Scope search to the specific DB field
            filters.append({criteria_field: {"$regex": search, "$options": "i"}})
        else:
            # Broad search across all relevant fields
            filters.append({"$or": [
                {"name":           {"$regex": search, "$options": "i"}},
                {"shift_xn_id":    {"$regex": search, "$options": "i"}},
                {"shift_code":     {"$regex": search, "$options": "i"}},
                {"location":       {"$regex": search, "$options": "i"}},
                {"client_county":  {"$regex": search, "$options": "i"}},
                {"client_id":      {"$regex": search, "$options": "i"}},
                {"user_type":      {"$regex": search, "$options": "i"}},
                {"assigned_staff": {"$regex": search, "$options": "i"}},
                {"unit":           {"$regex": search, "$options": "i"}},
            ]})

    if status:
        filters.append({"status": {"$regex": status, "$options": "i"}})

    if client_id:
        filters.append({"client_id": client_id})

    if user_type:
        filters.append({"user_type": {"$regex": user_type, "$options": "i"}})

    if automation_status:
        filters.append({"$or": [
            {"automation_status": {"$regex": automation_status, "$options": "i"}},
            {"upstream_status":   {"$regex": automation_status, "$options": "i"}},
        ]})

    if effective_date_from or effective_date_to:
        from datetime import datetime, timezone
        date_cond: dict = {}
        if effective_date_from:
            try:
                dt = datetime.strptime(effective_date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                date_cond["$gte"] = dt
            except ValueError:
                pass
        if effective_date_to:
            try:
                dt = datetime.strptime(effective_date_to, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59, tzinfo=timezone.utc)
                date_cond["$lte"] = dt
            except ValueError:
                pass
        if date_cond:
            # Match datetime-stored dates AND string-stored dates ("DD-MM-YYYY" or "YYYY-MM-DD")
            regex_val = effective_date_from or effective_date_to or ""
            filters.append({"$or": [
                {"date": date_cond},
                {"date": {"$regex": regex_val.replace("-", "[-/]"), "$options": "i"}}
            ]})

    mongo_filter = {"$and": filters} if filters else {}

    total = await db["shifts"].count_documents(mongo_filter)
    sort_dir = -1 if (sort_order or "desc").lower() == "desc" else 1
    sort_field = sort_by or "date"
    cursor = db["shifts"].find(mongo_filter).sort(sort_field, sort_dir).skip(skip).limit(limit)
    docs   = await cursor.to_list(length=limit)

    # Batch client lookup using xn_client_id join
    client_ids = list({d.get("client_id") for d in docs if d.get("client_id")})
    client_map = await _build_client_map(db, client_ids)

    results = []
    for doc in docs:
        s   = _serialize(doc)
        cid = s.get("client_id", "")
        cl  = client_map.get(cid)
        s["client_name"]  = _client_name(cl)
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

    s   = _serialize(doc)
    cid = s.get("client_id", "")

    if cid:
        client_map = await _build_client_map(db, [cid])
        cl = client_map.get(cid)
        s["client_name"]    = _client_name(cl)
        s["client_email"]   = cl.get("email")   if cl else None
        s["client_phone"]   = cl.get("phone")   if cl else None
        s["client_address"] = cl.get("address") if cl else None
    else:
        s["client_name"] = "—"

    return {"success": True, "data": s}


# ── DETAIL — shift + client + staff pool stub ─────────────────────────────────

@router.get(
    "/{shift_id}/detail",
    summary="Get full shift detail with client info and pool metadata",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def get_shift_detail(request: Request, shift_id: str):
    """
    Returns a shift document enriched with:
    - Full client info (from clients collection via xn_client_id)
    - Slot breakdown
    - All stored upstream fields
    Lookups by MongoDB _id, shift_xn_id, or shift_code.
    """
    db = _get_db()

    doc = None
    if ObjectId.is_valid(shift_id):
        doc = await db["shifts"].find_one({"_id": ObjectId(shift_id)})
    if not doc:
        doc = await db["shifts"].find_one({"$or": [
            {"shift_xn_id": shift_id},
            {"shift_code":  shift_id},
            {"shift_id":    shift_id},
        ]})
    if not doc:
        raise HTTPException(status_code=404, detail="Shift not found")

    s = _serialize(doc)

    # Enrich with full client data
    cid = s.get("client_id", "")
    client_detail: dict = {}
    if cid:
        client_map = await _build_client_map(db, [cid])
        cl = client_map.get(cid)
        if cl:
            client_detail = {
                "client_name":    cl.get("name") or cl.get("title") or "—",
                "client_email":   cl.get("email"),
                "client_phone":   cl.get("phone"),
                "client_address": cl.get("address"),
                "client_county":  cl.get("county"),
                "client_type":    cl.get("client_type"),
                "xn_client_id":   cl.get("xn_client_id"),
            }
        else:
            client_detail = {"client_name": "—"}

    # Build summary stats from slots
    slots = s.get("slots") or []
    slot_count = len(slots)

    return {
        "success": True,
        "data": {
            **s,
            **client_detail,
            "slot_count":   slot_count,
            # Pool metadata — placeholders (real data from Shift API pool endpoint)
            "pool": {
                "total_staff":       0,
                "from_bulk_pool":    0,
                "added_by_user":     0,
                "excluded_by_system":0,
                "channels": {
                    "phone":    0,
                    "whatsapp": 0,
                    "email":    0,
                },
            },
        },
    }
