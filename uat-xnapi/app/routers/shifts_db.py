import logging
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
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



async def _resolve_names_from_collection(db, collection: str, ids: list) -> list:
    """
    Resolve a list of ObjectId strings to their 'name' field values.
    Falls through non-ObjectId values as raw strings.
    Works for both user_types and county collections.
    """
    if not ids:
        return []
    oids = [ObjectId(i) for i in ids if ObjectId.is_valid(str(i))]
    names = []
    if oids:
        async for doc in db[collection].find({"_id": {"$in": oids}}, {"name": 1}):
            if doc.get("name"):
                names.append(doc["name"])
    # Also accept raw name strings passed directly (not IDs)
    for i in ids:
        if not ObjectId.is_valid(str(i)) and i not in names:
            names.append(i)
    return names


async def _resolve_user_type_names(db, ids: list) -> list:
    return await _resolve_names_from_collection(db, "user_types", ids)


async def _resolve_county_names(db, ids: list) -> list:
    return await _resolve_names_from_collection(db, "county", ids)


def _serialize(doc: dict) -> dict:
    if doc is None:
        return {}
    result = {}
    for k, v in doc.items():
        key = "id" if k == "_id" else k
        if isinstance(v, ObjectId):
            result[key] = str(v)
        elif hasattr(v, "isoformat"):
            result[key] = v.isoformat()
        elif isinstance(v, dict):
            result[key] = _serialize(v)
        elif isinstance(v, list):
            result[key] = [
                _serialize(i) if isinstance(i, dict)
                else str(i) if isinstance(i, ObjectId)
                else i.isoformat() if hasattr(i, "isoformat")
                else i
                for i in v
            ]
        else:
            result[key] = v
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



# ── Request schema ────────────────────────────────────────────────────────────

class ShiftsDbListRequest(BaseModel):
    search:              str = ""
    criteria:            Optional[str] = None
    status:              Optional[str] = None
    client_id:           Optional[str] = None
    user_type:           Optional[str] = None
    user_type_multiple:  Optional[list] = None  # list of user_type _id strings
    county_multiple:           Optional[list] = None  # list of county _id strings → shifts.client_county
    automation_status_multiple:  Optional[list] = None  # list of ints: 0,1,2,3,10
    is_premium:                  Optional[int]  = None  # 1 = true, 0 = false
    automation_status:   Optional[str] = None
    start_date:          Optional[str] = None   # YYYY-MM-DD
    end_date:            Optional[str] = None   # YYYY-MM-DD
    sort_by:             str = "date"
    sort_order:          str = "desc"
    page:                int = 1
    per_page:            int = 20


async def _get_shift_users(db, shift_oid: ObjectId) -> list:
    """
    Join shifts_users → users.
    shifts_users.shift_id == shifts._id
    shifts_users.user_id  == users._id
    Returns list of user summaries: id, name, email, phone, rating.
    """
    # Fetch all shifts_users rows for this shift
    su_docs = await db["shifts_users"].find(
        {"shift_id": shift_oid},
        {"user_id": 1, "rating": 1, "status": 1, "outreach_id": 1, "call_enabled": 1}
    ).to_list(length=500)

    if not su_docs:
        return []

    # Collect valid user ObjectIds
    user_oids = []
    su_map: dict = {}   # user_id str → shifts_users doc
    for su in su_docs:
        uid = su.get("user_id")
        if uid and ObjectId.is_valid(str(uid)):
            oid = ObjectId(str(uid))
            user_oids.append(oid)
            su_map[str(oid)] = su

    if not user_oids:
        return []

    # Fetch matching users
    users: list = []
    async for u in db["users"].find(
        {"_id": {"$in": user_oids}},
        {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
         "xn_user_id": 1, "designation": 1, "rating": 1}
    ):
        uid_str = str(u["_id"])
        su      = su_map.get(uid_str, {})
        full_name = " ".join(filter(None, [
            u.get("first_name", ""), u.get("last_name", "")
        ])).strip() or "—"
        raw_oid = su.get("outreach_id")
        users.append({
            "id":           str(su.get("_id", "")),
            "user_id":      uid_str,
            "xn_user_id":   u.get("xn_user_id"),
            "name":         full_name,
            "email":        u.get("email"),
            "phone":        u.get("phone"),
            "designation":  u.get("designation"),
            "rating":       su.get("rating") or u.get("rating"),
            "outreach_id":  str(raw_oid) if raw_oid else None,
            "call_enabled": su.get("call_enabled", 0),
        })

    return users




# ── LIST (POST — JSON body) ────────────────────────────────────────────────────

@router.post(
    "/",
    summary="List shifts from DB with client name (POST body)",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def list_shifts_db_post(request: Request, payload: ShiftsDbListRequest):
    """
    POST body:
    {
        "search": "",
        "criteria": null,
        "status": null,
        "client_id": null,
        "user_type": null,
        "automation_status": null,
        "start_date": "YYYY-MM-DD",
        "end_date": "YYYY-MM-DD",
        "sort_by": "date",
        "sort_order": "desc",
        "page": 1,
        "per_page": 20
    }
    """
    db   = _get_db()
    skip = (payload.page - 1) * payload.per_page
    limit = payload.per_page

    search            = payload.search or None
    status            = payload.status
    client_id         = payload.client_id
    user_type         = payload.user_type
    automation_status = payload.automation_status
    criteria          = payload.criteria
    effective_date_from = payload.start_date
    effective_date_to   = payload.end_date
    sort_by           = payload.sort_by or "date"
    sort_order        = payload.sort_order or "desc"

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
        criteria_field = LABEL_TO_FIELD.get(criteria, criteria)
        try:
            cr_doc = await db["criteria"].find_one(
                {"$or": [{"label": criteria}, {"field": criteria}]}, {"field": 1}
            )
            if cr_doc and cr_doc.get("field"):
                criteria_field = cr_doc["field"]
        except Exception:
            pass

    filters: list = []

    if search:
        if criteria_field:
            filters.append({criteria_field: {"$regex": search, "$options": "i"}})
        else:
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

    user_type_multiple = payload.user_type_multiple
    if user_type_multiple:
        type_names = await _resolve_user_type_names(db, user_type_multiple)
        if type_names:
            filters.append({"user_type": {"$in": type_names}})

    county_multiple = payload.county_multiple
    if county_multiple:
        county_names = await _resolve_county_names(db, county_multiple)
        if county_names:
            filters.append({"client_county": {"$in": county_names}})
    if automation_status:
        filters.append({"$or": [
            {"automation_status": {"$regex": automation_status, "$options": "i"}},
            {"upstream_status":   {"$regex": automation_status, "$options": "i"}},
        ]})

    if payload.is_premium is not None:
        filters.append({"is_premium": payload.is_premium == 1})

    # automation_status_multiple filter
    if payload.automation_status_multiple:
        asm = [int(s) for s in payload.automation_status_multiple if str(s).lstrip('-').isdigit()]
        if asm:
            include_not_started = 0 in asm
            active_sts = [s for s in asm if s != 0]
            if include_not_started and active_sts:
                o_sids, all_sids = [], []
                async for o in db["outreach"].find({"outreach_status": {"$in": active_sts}}, {"shift_id": 1}):
                    o_sids.append(o["shift_id"])
                async for o in db["outreach"].find({}, {"shift_id": 1}):
                    all_sids.append(o["shift_id"])
                filters.append({"$or": [{"_id": {"$nin": all_sids}}, {"_id": {"$in": o_sids}}]})
            elif include_not_started:
                all_sids = []
                async for o in db["outreach"].find({}, {"shift_id": 1}):
                    all_sids.append(o["shift_id"])
                filters.append({"_id": {"$nin": all_sids}})
            else:
                o_sids = []
                async for o in db["outreach"].find({"outreach_status": {"$in": active_sts}}, {"shift_id": 1}):
                    o_sids.append(o["shift_id"])
                filters.append({"_id": {"$in": o_sids}})

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
            regex_val = effective_date_from or effective_date_to or ""
            filters.append({"$or": [
                {"date": date_cond},
                {"date": {"$regex": regex_val.replace("-", "[-/]"), "$options": "i"}}
            ]})

    mongo_filter = {"$and": filters} if filters else {}
    total  = await db["shifts"].count_documents(mongo_filter)
    sort_dir = -1 if sort_order.lower() == "desc" else 1
    cursor = db["shifts"].find(mongo_filter).sort(sort_by, sort_dir).skip(skip).limit(limit)
    docs   = await cursor.to_list(length=limit)

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
        shift_oid_l = doc["_id"] if isinstance(doc["_id"], ObjectId) else ObjectId(str(doc["_id"]))
        s["staff_counts"] = await _get_staff_counts_light(db, shift_oid_l)
        outreach_info = await _get_outreach_status(db, shift_oid_l)
        s["outreach_status"]        = outreach_info["outreach_status"]
        s["outreach_status_text"]   = outreach_info["outreach_status_text"]
        s["outreach_sequence_name"] = outreach_info["outreach_sequence_name"]
        s["shift_preference"]       = outreach_info["shift_preference"]
        s["client_preference"]      = outreach_info["client_preference"]
        s["ghost_booking"]          = 0
        results.append(s)

    return {"success": True, "total": total, "page": payload.page,
            "per_page": payload.per_page, "data": results}



class ShiftsAutomationRequest(ShiftsDbListRequest):
    outreach_status: Optional[int] = None   # filter by specific outreach status



# ── SHIFTS AUTOMATION ─────────────────────────────────────────────────────────

@router.post(
    "/automation",
    summary="List shifts where outreach is active (outreach_status > 0 and != 10)",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def list_shifts_automation(request: Request, payload: ShiftsAutomationRequest):
    """
    Clone of POST /shifts-db/ but only returns shifts that have an outreach record
    with outreach_status > 0 AND outreach_status != 10.

    Optional additional filter:
    {
        ...,
        "outreach_status": 1   // filter by specific status (1=Live, 2=Paused, 3=Ended)
    }
    """
    db   = _get_db()
    skip = (payload.page - 1) * payload.per_page
    limit = payload.per_page

    search            = payload.search or None
    status            = payload.status
    client_id         = payload.client_id
    user_type         = payload.user_type
    automation_status = payload.automation_status
    criteria          = payload.criteria
    effective_date_from = payload.start_date
    effective_date_to   = payload.end_date
    sort_by           = payload.sort_by or "date"
    sort_order        = payload.sort_order or "desc"
    filter_outreach_status = payload.outreach_status  # optional specific status filter

    # ── Resolve outreach-active shift IDs from outreach collection ─────────────
    # input outreach_status mapping:
    #   1 → outreach_status > 0 AND != 10  (all active: Live/Paused/Ended)
    #   2 → outreach_status == 10           (Completed)
    #   None → default: outreach_status > 0 AND != 10
    if filter_outreach_status == 2:
        outreach_query: dict = {"outreach_status": 10}
    else:
        # 1 or None → all active
        outreach_query: dict = {"outreach_status": {"$gt": 0, "$ne": 10}}

    # Get shift_ids that have matching outreach records
    outreach_docs = await db["outreach"].find(
        outreach_query,
        {"shift_id": 1, "outreach_status": 1, "sequence_id": 1, "created_at": 1}
    ).to_list(length=10000)

    if not outreach_docs:
        return {"success": True, "total": 0, "page": payload.page,
                "per_page": payload.per_page, "data": []}

    # Latest outreach per shift (keep most recent)
    shift_outreach_map: dict = {}
    for o in outreach_docs:
        sid = str(o["shift_id"])
        if sid not in shift_outreach_map:
            shift_outreach_map[sid] = o

    active_shift_oids = [ObjectId(sid) for sid in shift_outreach_map]

    # ── Build shift filters ────────────────────────────────────────────────────
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
        criteria_field = LABEL_TO_FIELD.get(criteria, criteria)
        try:
            cr_doc = await db["criteria"].find_one(
                {"$or": [{"label": criteria}, {"field": criteria}]}, {"field": 1}
            )
            if cr_doc and cr_doc.get("field"):
                criteria_field = cr_doc["field"]
        except Exception:
            pass

    filters: list = [{"_id": {"$in": active_shift_oids}}]

    if search:
        if criteria_field:
            filters.append({criteria_field: {"$regex": search, "$options": "i"}})
        else:
            filters.append({"$or": [
                {"name":           {"$regex": search, "$options": "i"}},
                {"shift_xn_id":    {"$regex": search, "$options": "i"}},
                {"shift_code":     {"$regex": search, "$options": "i"}},
                {"location":       {"$regex": search, "$options": "i"}},
                {"client_county":  {"$regex": search, "$options": "i"}},
                {"user_type":      {"$regex": search, "$options": "i"}},
            ]})

    if status:
        filters.append({"status": {"$regex": status, "$options": "i"}})
    if client_id:
        filters.append({"client_id": client_id})
    if user_type:
        filters.append({"user_type": {"$regex": user_type, "$options": "i"}})

    user_type_multiple = payload.user_type_multiple
    if user_type_multiple:
        type_names = await _resolve_user_type_names(db, user_type_multiple)
        if type_names:
            filters.append({"user_type": {"$in": type_names}})

    county_multiple = payload.county_multiple
    if county_multiple:
        county_names = await _resolve_county_names(db, county_multiple)
        if county_names:
            filters.append({"client_county": {"$in": county_names}})
    if automation_status:
        filters.append({"$or": [
            {"automation_status": {"$regex": automation_status, "$options": "i"}},
            {"upstream_status":   {"$regex": automation_status, "$options": "i"}},
        ]})

    if payload.is_premium is not None:
        filters.append({"is_premium": payload.is_premium == 1})

    # automation_status_multiple: filter active_shift_oids by outreach status
    if payload.automation_status_multiple:
        asm = [int(s) for s in payload.automation_status_multiple if str(s).lstrip('-').isdigit()]
        active_sts = [s for s in asm if s != 0]
        if active_sts:
            active_shift_oids = [
                oid for oid in active_shift_oids
                if shift_outreach_map.get(str(oid), {}).get("outreach_status") in active_sts
            ]
            filters[0] = {"_id": {"$in": active_shift_oids}}

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
            regex_val = effective_date_from or effective_date_to or ""
            filters.append({"$or": [
                {"date": date_cond},
                {"date": {"$regex": regex_val.replace("-", "[-/]"), "$options": "i"}}
            ]})

    mongo_filter = {"$and": filters}
    total    = await db["shifts"].count_documents(mongo_filter)
    sort_dir = -1 if sort_order.lower() == "desc" else 1
    cursor   = db["shifts"].find(mongo_filter).sort(sort_by, sort_dir).skip(skip).limit(limit)
    docs     = await cursor.to_list(length=limit)

    client_ids = list({d.get("client_id") for d in docs if d.get("client_id")})
    client_map = await _build_client_map(db, client_ids)

    STATUS_TEXT = {0: "Not Started", 1: "Live", 2: "Paused", 3: "Ended", 10: "Completed"}

    results = []
    for doc in docs:
        s   = _serialize(doc)
        cid = s.get("client_id", "")
        cl  = client_map.get(cid)
        s["client_name"]  = _client_name(cl)
        s["client_email"] = cl.get("email") if cl else None
        s["client_phone"] = cl.get("phone") if cl else None

        # Staff counts
        shift_oid_l = doc["_id"] if isinstance(doc["_id"], ObjectId) else ObjectId(str(doc["_id"]))
        s["staff_counts"] = await _get_staff_counts_light(db, shift_oid_l)

        # Outreach info from map
        o_doc = shift_outreach_map.get(str(shift_oid_l), {})
        o_status = o_doc.get("outreach_status", 0)

        # Sequence name
        seq_name = None
        seq_oid = o_doc.get("sequence_id")
        if seq_oid:
            seq = await db["sequences"].find_one({"_id": seq_oid}, {"name": 1})
            if seq:
                seq_name = seq.get("name")

        # start_time from latest outreach.created_at
        created_at = o_doc.get("created_at")
        if created_at and hasattr(created_at, "isoformat"):
            start_time = created_at.isoformat()
        else:
            start_time = str(created_at) if created_at else None

        s["outreach_id"]            = str(o_doc["_id"]) if o_doc.get("_id") else None
        s["outreach_status"]        = o_status
        s["outreach_status_text"]   = STATUS_TEXT.get(o_status, "Not Started")
        s["outreach_sequence_name"] = seq_name
        s["start_time"]             = start_time
        s["shift_preference"]       = None
        s["client_preference"]      = None
        s["ghost_booking"]          = 0
        results.append(s)

    return {
        "success":  True,
        "total":    total,
        "page":     payload.page,
        "per_page": payload.per_page,
        "data":     results,
    }

class ShiftDetailRequest(BaseModel):
    id: str   # shift _id, shift_xn_id, or shift_code




async def _get_staff_counts_light(db, shift_oid: ObjectId) -> dict:
    """Lightweight counts for list endpoint."""
    available = await db["shifts_users"].count_documents({
        "shift_id":     shift_oid,
        "availability": {"$gt": 0},
    })
    with_outreach = await db["shifts_users"].count_documents({
        "shift_id":    shift_oid,
        "outreach_id": {"$exists": True, "$ne": None},
    })
    # pending: call_enabled=1 AND call_processed=0
    pending = await db["shifts_users"].count_documents({
        "shift_id":      shift_oid,
        "call_enabled":  1,
        "call_processed": 0,
    })
    # declined: availability != 1
    declined = await db["shifts_users"].count_documents({
        "shift_id":     shift_oid,
        "availability": {"$ne": 1},
    })
    return {
        "available":     available,
        "requested":     0,
        "with_outreach": with_outreach,
        "pending":       pending,
        "declined":      declined,
    }


async def _get_staff_counts(db, shift_oid: ObjectId) -> dict:
    """
    Full staff counts for detail endpoint:
    - number_of_staff : total shifts_users records
    - available       : count where availability > 0
    - requested       : 0 (static)
    - phone           : count where call_enabled > 0
    - whatsapp        : 0 (placeholder)
    - email           : 0 (placeholder)
    """
    total     = await db["shifts_users"].count_documents({"shift_id": shift_oid})
    available = await db["shifts_users"].count_documents({
        "shift_id":     shift_oid,
        "availability": {"$gt": 0},
    })
    phone = await db["shifts_users"].count_documents({
        "shift_id":     shift_oid,
        "call_enabled": {"$gt": 0},
    })
    with_outreach    = await db["shifts_users"].count_documents({
        "shift_id":   shift_oid,
        "outreach_id": {"$exists": True, "$ne": None},
    })
    without_outreach = total - with_outreach
    return {
        "number_of_staff":   total,
        "available":         available,
        "requested":         0,
        "phone":             phone,
        "whatsapp":          0,
        "email":             0,
        "with_outreach":     with_outreach,
        "without_outreach":  without_outreach,
    }



async def _get_outreach_status(db, shift_oid: ObjectId) -> dict:
    """
    Returns latest outreach status + sequence name for a shift.
    If no outreach found, returns outreach_status=0, text='Not Started'.
    """
    STATUS_TEXT = {
        0:  "Not Started",
        1:  "Live",
        2:  "Paused",
        3:  "Ended",
        10: "Completed",
    }
    latest = await db["outreach"].find_one(
        {"shift_id": shift_oid},
        sort=[("created_at", -1)]
    )
    if not latest:
        return {
            "outreach_status":          0,
            "outreach_status_text":     "Not Started",
            "outreach_sequence_name":   None,
            "shift_preference":         None,
            "client_preference":        None,
            "ghost_booking":            0,
        }
    status = latest.get("outreach_status", 0)

    # Resolve sequence name
    sequence_name = None
    seq_oid = latest.get("sequence_id")
    if seq_oid:
        seq = await db["sequences"].find_one({"_id": seq_oid}, {"name": 1})
        if seq:
            sequence_name = seq.get("name")

    return {
        "outreach_status":          status,
        "outreach_status_text":     STATUS_TEXT.get(status, "Not Started"),
        "outreach_id":              str(latest["_id"]),
        "outreach_sequence_name":   sequence_name,
        "shift_preference":         None,
        "client_preference":        None,
        "ghost_booking":            0,
    }


# ── GET single ────────────────────────────────────────────────────────────────

@router.post(
    "/detail",
    summary="Get a single shift with full details and client name",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def get_shift_db(request: Request, payload: ShiftDetailRequest):
    """
    Body: { "id": "<shift _id | shift_xn_id | shift_code>" }
    """
    db = _get_db()
    shift_id = payload.id.strip()

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

    shift_oid = doc["_id"] if isinstance(doc["_id"], ObjectId) else ObjectId(str(doc["_id"]))
    s["shift_users"]  = await _get_shift_users(db, shift_oid)
    s["staff_counts"] = await _get_staff_counts(db, shift_oid)
    outreach_info = await _get_outreach_status(db, shift_oid)
    s["outreach_status"]        = outreach_info["outreach_status"]
    s["outreach_status_text"]   = outreach_info["outreach_status_text"]
    s["outreach_sequence_name"] = outreach_info["outreach_sequence_name"]
    s["shift_preference"]       = outreach_info["shift_preference"]
    s["client_preference"]      = outreach_info["client_preference"]
    s["ghost_booking"]          = 0
    if "outreach_id" in outreach_info:
        s["outreach_id"] = outreach_info["outreach_id"]

    # Resolve user_type_id — use cached value or join and save
    user_type_id = None
    if doc.get("user_type_id"):
        user_type_id = str(doc["user_type_id"])
    elif s.get("user_type"):
        ut = await db["user_types"].find_one(
            {"name": {"$regex": f"^{s['user_type']}$", "$options": "i"}},
            {"_id": 1}
        )
        if ut:
            user_type_id = str(ut["_id"])
            await db["shifts"].update_one(
                {"_id": doc["_id"]}, {"$set": {"user_type_id": ut["_id"]}}
            )
    s["user_type_id"] = user_type_id

    # Resolve county_id — use cached value or join and save
    county_id = None
    if doc.get("county_id"):
        county_id = str(doc["county_id"])
    elif s.get("client_county"):
        co = await db["county"].find_one(
            {"name": {"$regex": f"^{s['client_county']}$", "$options": "i"}},
            {"_id": 1}
        )
        if co:
            county_id = str(co["_id"])
            await db["shifts"].update_one(
                {"_id": doc["_id"]}, {"$set": {"county_id": co["_id"]}}
            )
    s["county_id"] = county_id

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
            "shift_users":  await _get_shift_users(db, doc["_id"]),
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
