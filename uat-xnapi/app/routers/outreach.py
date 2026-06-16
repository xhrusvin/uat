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

    # ── Pool composition from shifts_users ───────────────────────────────────
    total_staff = await db["shifts_users"].count_documents({"shift_id": shift_oid})
    phone_count = await db["shifts_users"].count_documents({
        "shift_id":    shift_oid,
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
