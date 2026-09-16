"""
shift_users_outreach.py
=======================
Orchestrator endpoints extracted from shift_users.py.

Endpoints:
  GET /shift-users/run-outreach
  GET /shift-users/run-group-outreach

Register in main.py:
    from app.routers import shift_users_outreach
    app.include_router(shift_users_outreach.router)
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/shift-users", tags=["Shift Users Outreach"])

# Hard ceiling on how many candidate users one request may walk.
_MAX_SCAN = 5000


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


# ─────────────────────────────────────────────────────────────────────────────
# Shared Pydantic request models (re-declared here so this module is
# self-contained; they mirror the originals in shift_users.py exactly).
# ─────────────────────────────────────────────────────────────────────────────

class AddUsersToShiftRequest(BaseModel):
    shift_id:  str
    user_ids:  List[str]
    channel:   Optional[str] = "Phone"


class ListShiftUsersRequest(BaseModel):
    shift_id:           str
    page:               int = 1
    per_page:           int = 20
    radius:             Optional[float] = None
    order_by:           Optional[str]   = None
    sort:               Optional[str]   = "asc"
    county_multiple:    Optional[list]  = None
    user_type_multiple: Optional[list]  = None
    excluded:           Optional[int]   = None
    in_pool:            Optional[int]   = None
    search:             Optional[str]   = None
    gender_id:          Optional[str]   = None
    gender_multiple:    Optional[list]  = None
    visa_type_id:       Optional[str]   = None
    group_id:           Optional[str]   = None
    qqi_status_number:      Optional[int]  = None
    user_sub_type_multiple: Optional[list] = None


class ListMultiShiftUsersRequest(BaseModel):
    shift_ids:          list                    # shifts._id strings
    group_id:           Optional[str]   = None  # also check shifts_group_pool
    page:               int = 1
    per_page:           int = 20
    radius:             Optional[float] = None
    order_by:           Optional[str]   = None
    sort:               Optional[str]   = "asc"
    county_multiple:    Optional[list]  = None
    user_type_multiple: Optional[list]  = None
    excluded:           Optional[int]   = None
    in_pool:            Optional[int]   = None  # 1 = in pool for ANY shift or the group
    gender_id:          Optional[str]   = None
    gender_multiple:    Optional[list]  = None
    qqi_status_number:      Optional[int]  = None
    user_sub_type_multiple: Optional[list] = None
    visa_type_id:           Optional[str]  = None
    search:                 Optional[str]  = None


# ─────────────────────────────────────────────────────────────────────────────
# Shared Ops-region county map
# ─────────────────────────────────────────────────────────────────────────────

OPS_GROUPS: dict = {
    "Ops 1": ["Dublin", "Kildare", "Laois", "Louth", "Meath", "Monaghan", "Offaly", "Westmeath"],
    "Ops 2": ["Cork", "Waterford", "Kerry", "Limerick", "Tipperary", "Clare"],
    "Ops 3": ["Galway", "Sligo", "Mayo", "Donegal", "Leitrim", "Longford", "Roscommon", "Cavan"],
    "Ops 4": ["Carlow", "Wicklow", "Wexford", "Kilkenny"],
}

_ALL_OPS_COUNTIES = [cn for grp in OPS_GROUPS.values() for cn in grp]

_CHANNEL_MAP = {
    "phone":     "Phone",
    "whatsapp":  "WhatsApp",
    "email":     "Email",
    "sms":       "SMS",
}


# ─────────────────────────────────────────────────────────────────────────────
# Helper — resolve Ops county name from a shift doc (3-tier fallback)
# ─────────────────────────────────────────────────────────────────────────────

async def _resolve_shift_county(shift_doc: dict, db) -> str:
    """
    Returns the plain county name for *shift_doc*, trying three sources
    in priority order:
      Tier 1 – client_county field
      Tier 2 – county_id → county collection lookup
      Tier 3 – parse the location string for a token matching a known county
    """
    # Tier 1
    t1 = (shift_doc.get("client_county") or "").strip()
    if t1:
        return t1

    # Tier 2
    cid = shift_doc.get("county_id")
    if cid and ObjectId.is_valid(str(cid)):
        co_doc = await db["county"].find_one({"_id": ObjectId(str(cid))}, {"name": 1})
        if co_doc and co_doc.get("name"):
            return co_doc["name"].strip()

    # Tier 3
    loc = (shift_doc.get("location") or "").strip()
    if loc:
        for token in reversed([t.strip() for t in loc.split(",")]):
            if any(token.lower() == cn.lower() for cn in _ALL_OPS_COUNTIES):
                return token

    return ""


async def _resolve_ops_county_ids(county_name: str, db) -> tuple[Optional[str], list, list, list]:
    """
    Given a plain county name (e.g. "Meath"), returns:
      (ops_group_name, ops_county_names, ops_county_ids, unmatched_names)

    Flow:
      1. Find which Ops group the county belongs to (case-insensitive match).
      2. For every county name in that group, look up its _id in the county
         collection using a case-insensitive exact match.
      3. Return the list of ObjectId strings — these go straight into
         county_multiple so the downstream /list and /list-multi endpoints
         filter users by county _id.

    Returns four empty/None values when county_name is not in any Ops group.
    unmatched_names holds any Ops group county names that were NOT found in
    the county collection (useful for debugging missing reference data).
    """
    ops_group: Optional[str] = None
    county_names: list = []

    # Step 1 — find Ops group
    for group_name, counties in OPS_GROUPS.items():
        for cn in counties:
            if cn.lower() == county_name.lower():
                ops_group    = group_name
                county_names = counties
                break
        if ops_group:
            break

    if not county_names:
        return None, [], [], []

    # Step 2 — resolve each county name → _id from the county collection
    # Use a single $or query with case-insensitive exact anchored regex per name.
    patterns = [{"name": {"$regex": f"^{cn}$", "$options": "i"}} for cn in county_names]
    county_docs = await db["county"].find({"$or": patterns}, {"_id": 1, "name": 1}).to_list(None)

    # Build resolved map: lowercase name → ObjectId string
    resolved: dict = {doc["name"].lower(): str(doc["_id"]) for doc in county_docs}

    county_ids:      list = list(resolved.values())
    unmatched_names: list = [cn for cn in county_names if cn.lower() not in resolved]

    if unmatched_names:
        logger.warning(
            f"[ops-county-resolve] county collection missing entries for: {unmatched_names} "
            f"(ops_group={ops_group})"
        )

    return ops_group, county_names, county_ids, unmatched_names


async def _resolve_sequence(db, sequence_id: Optional[str], steps: list) -> tuple[str, str]:
    """
    Resolves sequence_id to a valid (sequence_id_str, sequence_name).
    Falls back to the first active sequence when the supplied ID is missing or invalid.
    Raises HTTPException(404) when no sequences exist at all.
    """
    if sequence_id and ObjectId.is_valid(sequence_id):
        seq_doc = await db["sequences"].find_one({"_id": ObjectId(sequence_id)})
        if seq_doc:
            return sequence_id, seq_doc.get("name", "—")
        steps.append({
            "step":    "sequence_resolve",
            "warning": f"sequence_id {sequence_id} not found — falling back to first active sequence",
        })

    seq_doc = await db["sequences"].find_one({"is_active": True}, sort=[("sort_order", 1)])
    if not seq_doc:
        seq_doc = await db["sequences"].find_one({}, sort=[("sort_order", 1)])
    if not seq_doc:
        raise HTTPException(status_code=404, detail="No sequences found in database")

    return str(seq_doc["_id"]), seq_doc.get("name", "—")


# ── GET /shift-users/run-outreach ─────────────────────────────────────────────
# Orchestrates the full outreach-prep flow for a shift in a single call:
#   Step 1 — /shift-users/list      → fetch eligible candidates
#   Step 2 — /shift-users/bulk      → add all candidates to shift pool
#   Step 3 — /outreach/detail       → preview round info + pool composition
#   Step 4 — /outreach/create       → create outreach record + enable calls
#
# Usage: GET /shift-users/run-outreach?shift_id=<shifts._id>
#             &sequence_id=<sequences._id>   (optional — first active seq used if omitted)
#             &channel=Phone                 (optional — default Phone)
#             &per_page=3000                 (optional — candidates to pull)
#             &dry_run=true                  (optional — skip steps 3 & 4, preview only)
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/run-outreach",
    summary="[Orchestrator] List → Bulk → Outreach detail → Outreach create for a shift",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("10/minute")
async def run_outreach_orchestrator(
    request:     Request,
    shift_id:    str           = Query(...,  description="shifts._id"),
    sequence_id: Optional[str] = Query(None, description="sequences._id — first active sequence used if omitted"),
    channel:     str           = Query("Phone", description="Phone | WhatsApp | Email | SMS"),
    per_page:    int           = Query(3000,    description="Max candidates to pull from /list"),
    dry_run:     bool          = Query(False,   description="If true, skip outreach/detail and outreach/create"),
):
    """
    Single endpoint that executes the full outreach prep flow:

    1. shift-users/list    — fetches candidate users for the shift
    2. shift-users/bulk    — upserts all candidates into the shift pool
    3. outreach/detail     — previews the pool + round configuration
    4. outreach/create     — creates the outreach record and enables calls

    Only shift_id is required. sequence_id is auto-resolved to the first
    active sequence when not supplied. Passing dry_run=true stops after
    step 2 so you can inspect the pool before committing.
    """
    db    = _get_db()
    steps: list = []   # audit trail returned in the response

    # ── Validate shift_id ────────────────────────────────────────────────────
    if not ObjectId.is_valid(shift_id):
        raise HTTPException(status_code=422, detail=f"Invalid shift_id: {shift_id}")
    shift_oid = ObjectId(shift_id)

    shift = await db["shifts"].find_one(
        {"_id": shift_oid},
        {"_id": 1, "name": 1, "shift_code": 1, "shift_id": 1,
         "date": 1, "user_type": 1, "status": 1,
         "client_county": 1, "location": 1, "county_id": 1, "radius": 1},
    )
    if not shift:
        raise HTTPException(status_code=404, detail=f"Shift {shift_id} not found")

    # ── Normalise channel ────────────────────────────────────────────────────
    channel = _CHANNEL_MAP.get(channel.lower(), "Phone")

    # ── Resolve sequence ─────────────────────────────────────────────────────
    resolved_sequence_id, sequence_name = await _resolve_sequence(db, sequence_id, steps)
    seq_oid = ObjectId(resolved_sequence_id)  # noqa: F841  (kept for symmetry / future use)

    # ── Resolve Ops county info ──────────────────────────────────────────────
    shift_county_name = await _resolve_shift_county(shift, db)
    shift_ops_group, ops_county_names, ops_county_ids, ops_unmatched = await _resolve_ops_county_ids(
        shift_county_name, db
    )

    # ── Extract radius from shift doc (None = no distance filter) ─────────────────────
    _raw_radius = shift.get("radius")
    shift_radius: Optional[float] = None
    if _raw_radius is not None:
        try:
            shift_radius = float(_raw_radius)
        except (TypeError, ValueError):
            logger.warning(f"[run-outreach] shift.radius={_raw_radius!r} is not numeric — ignoring")

    logger.info(
        f"[run-outreach] shift_id={shift_id} client_county='{shift.get('client_county')}' "
        f"resolved_county='{shift_county_name}' ops_group={shift_ops_group} "
        f"county_ids_count={len(ops_county_ids)} unmatched={ops_unmatched} "
        f"radius={shift_radius}"
    )

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1 — shift-users/list
    # ─────────────────────────────────────────────────────────────────────────
    from app.routers.shift_users import list_shift_users_paginated

    list_payload = ListShiftUsersRequest(
        shift_id=shift_id,
        page=1,
        per_page=min(per_page, _MAX_SCAN),
        county_multiple=ops_county_ids if ops_county_ids else None,
        radius=shift_radius,
    )

    try:
        list_result = await list_shift_users_paginated(request, list_payload)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"[Step 1 list] {e.detail}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"[Step 1 list] {str(e)}")

    candidate_users = list_result.get("data") or []
    user_ids        = [u["id"] for u in candidate_users if u.get("id")]

    steps.append({
        "step":                    "1_list",
        "endpoint":                "POST /shift-users/list",
        "shift_county":            shift_county_name or None,
        "ops_group":               shift_ops_group,
        "ops_counties":            ops_county_names,
        "ops_county_ids":          ops_county_ids,
        "ops_county_ids_count":    len(ops_county_ids),
        "ops_counties_unmatched":  ops_unmatched,
        "radius_km":               shift_radius,
        "candidates_found":        len(candidate_users),
        "total_in_db":             list_result.get("total", 0),
        "shift_user_type":         list_result.get("shift_user_type"),
    })

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2 — shift-users/bulk
    # ─────────────────────────────────────────────────────────────────────────
    from app.routers.shift_users import add_users_to_shift_bulk

    bulk_result_data: dict = {}

    if not user_ids:
        steps.append({
            "step":     "2_bulk",
            "endpoint": "POST /shift-users/bulk",
            "skipped":  True,
            "reason":   "No candidates returned from list",
        })
    else:
        bulk_payload = AddUsersToShiftRequest(
            shift_id=shift_id,
            user_ids=user_ids,
            channel=channel,
        )
        try:
            bulk_result      = await add_users_to_shift_bulk(request, bulk_payload)
            bulk_result_data = bulk_result.get("data") or {}
        except HTTPException as e:
            raise HTTPException(status_code=e.status_code, detail=f"[Step 2 bulk] {e.detail}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"[Step 2 bulk] {str(e)}")

        steps.append({
            "step":              "2_bulk",
            "endpoint":          "POST /shift-users/bulk",
            "users_submitted":   len(user_ids),
            "inserted":          bulk_result_data.get("inserted", 0),
            "updated_existing":  bulk_result_data.get("skipped_duplicate", 0),
            "removed_from_pool": bulk_result_data.get("removed", 0),
            "skipped_missing":   bulk_result_data.get("skipped_missing_user", 0),
        })

    # ── Early exit for dry_run ────────────────────────────────────────────────
    if dry_run:
        return {
            "success":       True,
            "dry_run":       True,
            "shift_id":      shift_id,
            "shift_code":    shift.get("shift_code"),
            "shift_name":    shift.get("name"),
            "shift_county":  shift_county_name or None,
            "ops_group":     shift_ops_group,
            "sequence_id":   resolved_sequence_id,
            "sequence_name": sequence_name,
            "channel":       channel,
            "message":       "Dry run complete — steps 3 (outreach/detail) and 4 (outreach/create) were skipped",
            "steps":         steps,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3 — outreach/detail  (preview — no DB write)
    # ─────────────────────────────────────────────────────────────────────────
    from app.routers.outreach import (
        outreach_detail  as _outreach_detail,
        create_outreach  as _create_outreach,
        OutreachDetailRequest,
    )

    outreach_payload = OutreachDetailRequest(
        sequence_id=resolved_sequence_id,
        shift_id=shift_id,
    )

    detail_data: dict = {}
    try:
        detail_result = await _outreach_detail(request, outreach_payload)
        detail_data   = detail_result
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"[Step 3 outreach/detail] {e.detail}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"[Step 3 outreach/detail] {str(e)}")

    pool_info    = (detail_data.get("data") or {}).get("pool") or {}
    round_number = detail_data.get("round_number", 1)

    steps.append({
        "step":         "3_outreach_detail",
        "endpoint":     "POST /outreach/detail",
        "round_number": round_number,
        "is_first":     detail_data.get("is_first"),
        "pool":         pool_info,
    })

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4 — outreach/create  (writes outreach doc + shifts_users)
    # ─────────────────────────────────────────────────────────────────────────
    create_data: dict = {}
    try:
        create_result = await _create_outreach(request, outreach_payload)
        create_data   = create_result
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"[Step 4 outreach/create] {e.detail}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"[Step 4 outreach/create] {str(e)}")

    su_summary = create_data.get("shifts_users_update") or {}

    steps.append({
        "step":                    "4_outreach_create",
        "endpoint":                "POST /outreach/create",
        "outreach_id":             (create_data.get("data") or {}).get("id"),
        "round_number":            create_data.get("round_number"),
        "shifts_users_updated":    su_summary.get("updated", 0),
        "shifts_users_skipped":    su_summary.get("skipped", 0),
    })

    # ── Final summary ─────────────────────────────────────────────────────────
    return {
        "success":       True,
        "dry_run":       False,
        "shift_id":      shift_id,
        "shift_code":    shift.get("shift_code"),
        "shift_name":    shift.get("name"),
        "shift_county":  shift_county_name or None,
        "ops_group":     shift_ops_group,
        "sequence_id":   resolved_sequence_id,
        "sequence_name": sequence_name,
        "channel":       channel,
        "round_number":  create_data.get("round_number"),
        "outreach_id":   (create_data.get("data") or {}).get("id"),
        "message": (
            f"Round {create_data.get('round_number')} outreach created — "
            f"{len(candidate_users)} candidates listed"
            + (f" (Ops group: {shift_ops_group})" if shift_ops_group else "")
            + f", {bulk_result_data.get('inserted', 0) + bulk_result_data.get('skipped_duplicate', 0)} in pool"
            + f", {su_summary.get('updated', 0)} shifts_users enabled"
        ),
        "pool":     pool_info,
        "steps":    steps,
        "outreach": create_data.get("data"),
    }


# ── GET /shift-users/run-group-outreach ───────────────────────────────────────
# Orchestrates the full MULTI-SHIFT outreach prep flow in a single call:
#   Step 1 — /shifts-group/add-shifts    → create/update group with provided shift_ids
#   Step 2 — /shift-users/list-multi     → fetch candidates across all shifts
#   Step 3 — /shifts-group/pool/add      → add all candidates to the group pool
#   Step 4 — /outreach-group/create      → create outreach record + enable calls
#
# Usage: GET /shift-users/run-group-outreach
#             ?shifts=<id1>,<id2>,<id3>   (comma-separated shifts._id, required)
#             &group_id=<id>              (optional — new group created if omitted)
#             &group_name=My+Group        (optional — name for new group)
#             &sequence_id=<id>           (optional — first active seq used if omitted)
#             &channel=Phone              (optional — Phone|WhatsApp|Email|SMS)
#             &per_page=3000              (optional)
#             &dry_run=true              (optional — skip step 4)
# ─────────────────────────────────────────────────────────────────────────────

@router.get(
    "/run-group-outreach",
    summary="[Orchestrator] Add-shifts → List-multi → Pool/add → Outreach-group/create",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("10/minute")
async def run_group_outreach_orchestrator(
    request:     Request,
    shifts:      str           = Query(...,  description="Comma-separated shifts._id values"),
    group_id:    Optional[str] = Query(None, description="Existing shifts_group._id — new group created if omitted"),
    group_name:  Optional[str] = Query(None, description="Name for the group (used on creation)"),
    sequence_id: Optional[str] = Query(None, description="sequences._id — first active sequence used if omitted"),
    channel:     str           = Query("Phone", description="Phone | WhatsApp | Email | SMS"),
    per_page:    int           = Query(3000,    description="Max candidates to pull from list-multi"),
    dry_run:     bool          = Query(False,   description="If true, skip outreach-group/create"),
):
    """
    Single GET endpoint that executes the full multi-shift outreach prep flow.

    Shifts are passed as a comma-separated query string:
        ?shifts=6a33dcf7b1dbb4d723fae422,6b44ed08c2ecc5e834gbf533

    Steps executed in order:
      1. shifts-group/add-shifts   — create or update a shift group
      2. shift-users/list-multi    — fetch all eligible candidates across all shifts
      3. shifts-group/pool/add     — add candidates to the group pool
      4. outreach-group/create     — create the outreach record + enable calls

    dry_run=true stops after step 3 so you can inspect the pool first.
    """
    db    = _get_db()
    steps: list = []

    # ── Parse & validate shift_ids ────────────────────────────────────────────
    raw_ids = [s.strip() for s in shifts.split(",") if s.strip()]
    if not raw_ids:
        raise HTTPException(status_code=422, detail="shifts query param must contain at least one shift_id")

    invalid_ids = [s for s in raw_ids if not ObjectId.is_valid(s)]
    if invalid_ids:
        raise HTTPException(status_code=422, detail=f"Invalid shift_id(s): {invalid_ids}")

    shift_oids = [ObjectId(s) for s in raw_ids]

    # Verify all shifts exist
    found_shifts = await db["shifts"].find(
        {"_id": {"$in": shift_oids}},
        {"_id": 1, "name": 1, "shift_code": 1, "client_county": 1,
         "location": 1, "county_id": 1, "user_type": 1, "radius": 1},
    ).to_list(len(shift_oids))

    found_ids = {str(s["_id"]) for s in found_shifts}
    missing   = [s for s in raw_ids if s not in found_ids]
    if missing:
        raise HTTPException(status_code=404, detail=f"Shifts not found: {missing}")

    # ── Normalise channel ─────────────────────────────────────────────────────
    channel = _CHANNEL_MAP.get(channel.lower(), "Phone")

    # ── Resolve sequence ──────────────────────────────────────────────────────
    resolved_sequence_id, sequence_name = await _resolve_sequence(db, sequence_id, steps)

    # ── Ops region county filter (based on first shift's county) ─────────────
    first_shift = found_shifts[0]
    shift_county_name = await _resolve_shift_county(first_shift, db)
    shift_ops_group, ops_county_names, ops_county_ids, ops_unmatched = await _resolve_ops_county_ids(
        shift_county_name, db
    )

    # ── Extract radius from first shift doc (None = no distance filter) ────────────────
    _raw_radius = first_shift.get("radius")
    shift_radius: Optional[float] = None
    if _raw_radius is not None:
        try:
            shift_radius = float(_raw_radius)
        except (TypeError, ValueError):
            logger.warning(f"[run-group-outreach] first shift radius={_raw_radius!r} is not numeric — ignoring")

    logger.info(
        f"[run-group-outreach] shifts={raw_ids} client_county='{first_shift.get('client_county')}' "
        f"resolved_county='{shift_county_name}' ops_group={shift_ops_group} "
        f"county_ids_count={len(ops_county_ids)} unmatched={ops_unmatched} "
        f"radius={shift_radius}"
    )

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 1 — shifts-group/add-shifts
    # ─────────────────────────────────────────────────────────────────────────
    from app.routers.shifts_group import (
        add_shifts_to_group     as _add_shifts,
        add_staff_to_group_pool as _pool_add,
        AddShiftsRequest,
        GroupPoolAddRequest,
    )

    add_shifts_payload = AddShiftsRequest(
        group_id=group_id or None,
        group_name=group_name or None,
        shift_ids=raw_ids,
    )

    try:
        add_shifts_result = await _add_shifts(request, add_shifts_payload)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"[Step 1 add-shifts] {e.detail}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"[Step 1 add-shifts] {str(e)}")

    resolved_group_id   = add_shifts_result.get("group_id")
    resolved_group_name = add_shifts_result.get("group_name")

    steps.append({
        "step":        "1_add_shifts",
        "endpoint":    "POST /shifts-group/add-shifts",
        "action":      add_shifts_result.get("action"),   # "created" or "updated"
        "group_id":    resolved_group_id,
        "group_name":  resolved_group_name,
        "shift_count": add_shifts_result.get("shift_count"),
    })

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 2 — shift-users/list-multi
    # ─────────────────────────────────────────────────────────────────────────
    from app.routers.shift_users import list_shift_users_multi

    list_multi_payload = ListMultiShiftUsersRequest(
        shift_ids=raw_ids,
        group_id=resolved_group_id,
        page=1,
        per_page=min(per_page, _MAX_SCAN),
        county_multiple=ops_county_ids if ops_county_ids else None,
        radius=shift_radius,
    )

    try:
        list_result = await list_shift_users_multi(request, list_multi_payload)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"[Step 2 list-multi] {e.detail}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"[Step 2 list-multi] {str(e)}")

    candidate_users = list_result.get("data") or []
    user_ids        = [u["id"] for u in candidate_users if u.get("id")]

    steps.append({
        "step":                   "2_list_multi",
        "endpoint":               "POST /shift-users/list-multi",
        "shift_county":           shift_county_name or None,
        "ops_group":              shift_ops_group,
        "ops_counties":           ops_county_names,
        "ops_county_ids":         ops_county_ids,
        "ops_county_ids_count":   len(ops_county_ids),
        "ops_counties_unmatched": ops_unmatched,
        "radius_km":              shift_radius,
        "candidates_found":       len(candidate_users),
        "total_in_db":            list_result.get("total", 0),
        "shift_user_types":       list_result.get("shift_user_types"),
    })

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 3 — shifts-group/pool/add
    # ─────────────────────────────────────────────────────────────────────────
    pool_result_data: dict = {}

    if not user_ids:
        steps.append({
            "step":     "3_pool_add",
            "endpoint": "POST /shifts-group/pool/add",
            "skipped":  True,
            "reason":   "No candidates returned from list-multi",
        })
    else:
        pool_payload = GroupPoolAddRequest(
            group_id=resolved_group_id,
            user_ids=user_ids,
            channel=channel,
        )
        try:
            pool_result      = await _pool_add(request, pool_payload)
            pool_result_data = pool_result
        except HTTPException as e:
            raise HTTPException(status_code=e.status_code, detail=f"[Step 3 pool/add] {e.detail}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"[Step 3 pool/add] {str(e)}")

        steps.append({
            "step":             "3_pool_add",
            "endpoint":         "POST /shifts-group/pool/add",
            "users_submitted":  len(user_ids),
            "inserted":         pool_result_data.get("inserted", 0),
            "updated_existing": pool_result_data.get("skipped", 0),
        })

    # ── Early exit for dry_run ────────────────────────────────────────────────
    if dry_run:
        return {
            "success":       True,
            "dry_run":       True,
            "shift_ids":     raw_ids,
            "group_id":      resolved_group_id,
            "group_name":    resolved_group_name,
            "shift_county":  shift_county_name or None,
            "ops_group":     shift_ops_group,
            "sequence_id":   resolved_sequence_id,
            "sequence_name": sequence_name,
            "channel":       channel,
            "message":       "Dry run complete — step 4 (outreach-group/create) was skipped",
            "steps":         steps,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # STEP 4 — outreach-group/create
    # ─────────────────────────────────────────────────────────────────────────
    from app.routers.outreach_group import (
        create_group_outreach as _create_group_outreach,
        GroupOutreachRequest,
    )

    outreach_payload = GroupOutreachRequest(
        sequence_id=resolved_sequence_id,
        group_id=resolved_group_id,
    )

    try:
        outreach_result = await _create_group_outreach(request, outreach_payload)
    except HTTPException as e:
        raise HTTPException(status_code=e.status_code, detail=f"[Step 4 outreach-group/create] {e.detail}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"[Step 4 outreach-group/create] {str(e)}")

    outreach_data = outreach_result.get("data") or {}
    round_number  = outreach_result.get("round_number")

    steps.append({
        "step":                   "4_outreach_group_create",
        "endpoint":               "POST /outreach-group/create",
        "outreach_id":            outreach_data.get("id"),
        "round_number":           round_number,
        "shifts_group_users_set": outreach_result.get("shifts_group_users_update", {}).get("inserted", 0),
    })

    # ── Final summary ─────────────────────────────────────────────────────────
    return {
        "success":       True,
        "dry_run":       False,
        "shift_ids":     raw_ids,
        "group_id":      resolved_group_id,
        "group_name":    resolved_group_name,
        "shift_county":  shift_county_name or None,
        "ops_group":     shift_ops_group,
        "sequence_id":   resolved_sequence_id,
        "sequence_name": sequence_name,
        "channel":       channel,
        "round_number":  round_number,
        "outreach_id":   outreach_data.get("id"),
        "message": (
            f"Round {round_number} group outreach created — "
            f"{len(raw_ids)} shifts, "
            f"{len(candidate_users)} candidates listed"
            + (f" (Ops group: {shift_ops_group})" if shift_ops_group else "")
            + f", {pool_result_data.get('inserted', 0) + pool_result_data.get('skipped', 0)} in group pool"
        ),
        "steps":    steps,
        "outreach": outreach_data,
    }
