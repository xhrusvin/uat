import logging
from datetime import datetime, timezone

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/outreach", tags=["Outreach"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize(doc: dict) -> dict:
    result = {}
    for k, v in doc.items():
        key = "id" if k == "_id" else k
        if isinstance(v, ObjectId):
            result[key] = str(v)
        elif hasattr(v, "isoformat"):
            result[key] = v.isoformat()
        else:
            result[key] = v
    return result


# ── Schema ────────────────────────────────────────────────────────────────────

class OutreachDetailRequest(BaseModel):
    sequence_id: str   # sequences._id
    shift_id:    str   # shifts._id


# ── POST /outreach/detail ─────────────────────────────────────────────────────

@router.post(
    "/detail",
    summary="Get outreach details before starting — shows pool, plan, pause config",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def outreach_detail(request: Request, payload: OutreachDetailRequest):
    """
    Body: { "sequence_id": "<sequence _id>", "shift_id": "<shift _id>" }

    Returns:
    - sequence name
    - pool composition (staff count, phone, whatsapp, email)
    - plan (rounds, staff per round, delay)
    - pause_on config
    - round_number (1 if first outreach for this shift)
    """
    db = _get_db()

    # ── Validate IDs ──────────────────────────────────────────────────────────
    if not ObjectId.is_valid(payload.sequence_id):
        raise HTTPException(status_code=422, detail="Invalid sequence_id")
    if not ObjectId.is_valid(payload.shift_id):
        raise HTTPException(status_code=422, detail="Invalid shift_id")

    seq_oid   = ObjectId(payload.sequence_id)
    shift_oid = ObjectId(payload.shift_id)

    # ── Fetch sequence ────────────────────────────────────────────────────────
    sequence = await db["sequences"].find_one({"_id": seq_oid})
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")

    # ── Fetch shift ───────────────────────────────────────────────────────────
    shift = await db["shifts"].find_one({"_id": shift_oid})
    if not shift:
        raise HTTPException(status_code=404, detail="Shift not found")

    # ── Count existing outreach rounds for this shift ─────────────────────────
    outreach_count = await db["outreach"].count_documents({"shift_id": shift_oid})
    round_number   = outreach_count + 1
    is_first       = outreach_count == 0

    # ── Pool composition from shifts_users (exclude already-assigned) ────────
    no_outreach_filter = {
        "shift_id": shift_oid,
        "$or": [{"outreach_id": {"$exists": False}}, {"outreach_id": None}],
    }
    total_staff = await db["shifts_users"].count_documents(no_outreach_filter)
    phone_count = await db["shifts_users"].count_documents({
        **no_outreach_filter,
        "call_enabled": {"$gt": 0},
    })
    # whatsapp and email are placeholders until those fields are added
    whatsapp_count = 0
    email_count    = 0

    pool_summary = (
        f"{total_staff} staff · phone {phone_count}, "
        f"WhatsApp {whatsapp_count}, email {email_count}"
    )

    # ── Plan — default: 3 rounds of 6 staff, 90s delay ───────────────────────
    rounds_per_plan    = 3
    staff_per_round    = 6
    delay_seconds      = 90
    plan_summary = (
        f"{rounds_per_plan} rounds of {staff_per_round} staff, "
        f"{delay_seconds}s delay between"
    )

    # ── Pause on ─────────────────────────────────────────────────────────────
    pause_on = "First Available Staff"

    # ── Build message ─────────────────────────────────────────────────────────
    if is_first:
        message = (
            "Round 1 will begin contacting staff immediately. "
            "The sequence pauses automatically when someone becomes available."
        )
    else:
        message = (
            f"Round {round_number} will begin contacting staff immediately. "
            "The sequence pauses automatically when someone becomes available."
        )

    return {
        "success":      True,
        "round_number": round_number,
        "is_first":     is_first,
        "message":      message,
        "data": {
            "sequence":   sequence.get("name", "—"),
            "sequence_id": payload.sequence_id,
            "shift_id":   payload.shift_id,
            "pool": {
                "total_staff":   total_staff,
                "phone":         phone_count,
                "whatsapp":      whatsapp_count,
                "email":         email_count,
                "summary":       pool_summary,
            },
            "plan": {
                "rounds":         rounds_per_plan,
                "staff_per_round": staff_per_round,
                "delay_seconds":  delay_seconds,
                "summary":        plan_summary,
            },
            "pause_on": pause_on,
        },
    }


# ── POST /outreach/start ──────────────────────────────────────────────────────

@router.post(
    "/start",
    summary="Start outreach — creates an outreach record",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def start_outreach(request: Request, payload: OutreachDetailRequest):
    """
    Body: { "sequence_id": "<sequence _id>", "shift_id": "<shift _id>" }
    Creates an outreach document and returns it with round_number.
    """
    db = _get_db()

    if not ObjectId.is_valid(payload.sequence_id):
        raise HTTPException(status_code=422, detail="Invalid sequence_id")
    if not ObjectId.is_valid(payload.shift_id):
        raise HTTPException(status_code=422, detail="Invalid shift_id")

    seq_oid   = ObjectId(payload.sequence_id)
    shift_oid = ObjectId(payload.shift_id)

    sequence = await db["sequences"].find_one({"_id": seq_oid})
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")

    shift = await db["shifts"].find_one({"_id": shift_oid})
    if not shift:
        raise HTTPException(status_code=404, detail="Shift not found")

    # Determine round number
    outreach_count = await db["outreach"].count_documents({"shift_id": shift_oid})
    round_number   = outreach_count + 1

    now = datetime.now(timezone.utc)
    doc = {
        "shift_id":    shift_oid,
        "sequence_id": seq_oid,
        "round_number": round_number,
        "status":      "active",
        "pause_on":    "first_available",
        "started_at":  now,
        "paused_at":   None,
        "ended_at":    None,
        "created_at":  now,
        "updated_at":  now,
    }

    result = await db["outreach"].insert_one(doc)
    doc["_id"] = result.inserted_id

    logger.info(f"Outreach started: shift={payload.shift_id} round={round_number} seq={payload.sequence_id}")

    return {
        "success":      True,
        "round_number": round_number,
        "message":      f"Round {round_number} outreach started",
        "data":         _serialize(doc),
    }


# ── POST /outreach/create ──────────────────────────────────────────────────────

@router.post(
    "/create",
    summary="Create outreach and update shifts_users with outreach_id + call_enabled",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def create_outreach(request: Request, payload: OutreachDetailRequest):
    """
    Body: { "sequence_id": "<sequence _id>", "shift_id": "<shift _id>" }

    1. Creates an outreach document in the outreach collection.
    2. For each shifts_users record where shift_id matches:
       - If outreach_id is missing → set outreach_id + call_enabled = 1
       - If outreach_id already exists → leave it unchanged
    Returns the outreach record + update summary.
    """
    db = _get_db()

    if not ObjectId.is_valid(payload.sequence_id):
        raise HTTPException(status_code=422, detail="Invalid sequence_id")
    if not ObjectId.is_valid(payload.shift_id):
        raise HTTPException(status_code=422, detail="Invalid shift_id")

    seq_oid   = ObjectId(payload.sequence_id)
    shift_oid = ObjectId(payload.shift_id)

    # Validate sequence and shift exist
    sequence = await db["sequences"].find_one({"_id": seq_oid})
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")

    shift = await db["shifts"].find_one({"_id": shift_oid})
    if not shift:
        raise HTTPException(status_code=404, detail="Shift not found")

    # Round number
    outreach_count = await db["outreach"].count_documents({"shift_id": shift_oid})
    round_number   = outreach_count + 1
    now = datetime.now(timezone.utc)

    # Create outreach document
    doc = {
        "shift_id":       shift_oid,
        "sequence_id":    seq_oid,
        "round_number":   round_number,
        "status":         "active",
        "outreach_status": 1,
        "pause_on":       "first_available",
        "started_at":     now,
        "paused_at":      None,
        "ended_at":       None,
        "created_at":     now,
        "updated_at":     now,
    }
    result = await db["outreach"].insert_one(doc)
    outreach_oid = result.inserted_id
    doc["_id"]   = outreach_oid

    # Update shifts_users:
    # - where outreach_id is missing → set outreach_id + call_enabled = 1
    # - where outreach_id already exists → skip
    updated = await db["shifts_users"].update_many(
        {
            "shift_id":   shift_oid,
            "$or": [
                {"outreach_id": {"$exists": False}},
                {"outreach_id": None},
            ],
        },
        {
            "$set": {
                "outreach_id":  outreach_oid,
                "call_enabled": 1,
                "updated_at":   now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            }
        }
    )

    skipped = await db["shifts_users"].count_documents({
        "shift_id":   shift_oid,
        "outreach_id": {"$exists": True, "$ne": None, "$ne": outreach_oid},
    })

    logger.info(
        f"Outreach created: id={outreach_oid} shift={payload.shift_id} "
        f"round={round_number} updated={updated.modified_count} skipped={skipped}"
    )

    return {
        "success":      True,
        "round_number": round_number,
        "message":      f"Round {round_number} outreach created",
        "data":         _serialize(doc),
        "shifts_users_update": {
            "updated":  updated.modified_count,
            "skipped":  skipped,
        },
    }


# ── POST /outreach/pause ──────────────────────────────────────────────────────

class PauseOutreachRequest(BaseModel):
    shift_id: str


@router.post(
    "/pause",
    summary="Pause outreach for a shift",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def pause_outreach(request: Request, payload: PauseOutreachRequest):
    """
    Body: { "shift_id": "<shift _id>" }

    1. Finds the latest active outreach for the shift.
    2. Sets outreach.outreach_status = 2 (Paused), paused_at = now.
    3. For shifts_users where call_processed != 1 → set call_enabled = 0.
    """
    db = _get_db()

    if not ObjectId.is_valid(payload.shift_id):
        raise HTTPException(status_code=422, detail="Invalid shift_id")

    shift_oid = ObjectId(payload.shift_id)

    # Find the latest active outreach for this shift
    outreach = await db["outreach"].find_one(
        {"shift_id": shift_oid, "outreach_status": 1},
        sort=[("created_at", -1)],
    )
    if not outreach:
        raise HTTPException(
            status_code=404,
            detail="No active (Live) outreach found for this shift"
        )

    now = datetime.now(timezone.utc)

    # Update outreach status to Paused (2)
    await db["outreach"].update_one(
        {"_id": outreach["_id"]},
        {"$set": {
            "outreach_status": 2,
            "paused_at":       now,
            "updated_at":      now,
        }}
    )

    # Set call_enabled = 0 for shifts_users where call_processed != 1
    result = await db["shifts_users"].update_many(
        {
            "shift_id":      shift_oid,
            "call_processed": {"$ne": 1},
        },
        {"$set": {
            "call_enabled": 0,
            "updated_at":   now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        }}
    )

    logger.info(
        f"Outreach paused: shift={payload.shift_id} "
        f"outreach={outreach['_id']} disabled={result.modified_count}"
    )

    return {
        "success":       True,
        "message":       "Outreach paused",
        "outreach_id":   str(outreach["_id"]),
        "shift_id":      payload.shift_id,
        "outreach_status":      2,
        "outreach_status_text": "Paused",
        "shifts_users_updated": result.modified_count,
    }
