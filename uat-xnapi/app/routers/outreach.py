import logging
import httpx
from typing import Optional
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

    # Copy from shifts_pool to shifts_users
    # Skip only if user already in shifts_users with availability == 1
    pool_docs = await db["shifts_pool"].find({"shift_id": shift_oid}).to_list(length=5000)

    inserted_count = 0
    skipped        = 0
    for pd in pool_docs:
        user_oid_pool = pd.get("user_id")
        if not user_oid_pool:
            continue
        # Skip only if already exists with availability == 1
        exists = await db["shifts_users"].find_one({
            "shift_id":    shift_oid,
            "user_id":     user_oid_pool,
            "availability": 1,
        })
        if exists:
            skipped += 1
            continue
        su_doc = {
            "user_id":            user_oid_pool,
            "shift_id":           shift_oid,
            "outreach_id":        outreach_oid,
            "assigned_at":        now,
            "availability":       6,
            "call_enabled":       1,
            "call_processed":     0,
            "call_processed_at":  now,
            "conversation_id":    None,
            "agent_id":           None,
            "call_status":        0,
            "call_summary_title": None,
            "ended_at":           None,
            "started_at":         None,
            "updated_at":         now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        }
        await db["shifts_users"].insert_one(su_doc)
        inserted_count += 1

    class _Result:
        def __init__(self, n): self.modified_count = n
    updated = _Result(inserted_count)

    # Get counts for activity log
    available_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "availability": {"$gt": 0},
    })
    declined_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "availability": {"$ne": 1},
    })
    no_reply_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "call_processed": 0, "call_enabled": 1,
    })

    # Save activity log
    activity_doc = {
        "activity_type": "round_started",
        "shift_id":      shift_oid,
        "outreach_id":   outreach_oid,
        "sequence_id":   seq_oid,
        "metadata": {
            "sequence_id":   str(seq_oid),
            "shift_id":      payload.shift_id,
            "outreach_id":   str(outreach_oid),
            "round_number":  round_number,
            "available":     available_count,
            "declined":      declined_count,
            "no_reply":      no_reply_count,
            "call_enabled_set": updated.modified_count,
            "summary":       f"Round {round_number} started · {available_count} available, {declined_count} declined, {no_reply_count} no-reply",
        },
        "created_at": now,
    }
    try:
        await db["activities"].insert_one(activity_doc)
    except Exception as e:
        logger.error(f"Activity log error: {e}")

    logger.info(
        f"Outreach created: id={outreach_oid} shift={payload.shift_id} "
        f"round={round_number} updated={updated.modified_count} skipped={skipped}"
    )

    # Re-serialize safely
    safe_doc = {
        "id":             str(doc["_id"]),
        "shift_id":       str(doc["shift_id"]),
        "sequence_id":    str(doc["sequence_id"]),
        "round_number":   doc["round_number"],
        "outreach_status": doc["outreach_status"],
        "status":         doc["status"],
        "started_at":     doc["started_at"].isoformat() if doc.get("started_at") else None,
        "created_at":     doc["created_at"].isoformat() if doc.get("created_at") else None,
    }

    return {
        "success":      True,
        "round_number": round_number,
        "message":      f"Round {round_number} outreach created",
        "data":         safe_doc,
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

    # Get counts for activity log
    available_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "availability": {"$gt": 0},
    })
    declined_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "availability": {"$ne": 1},
    })
    no_reply_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "call_processed": 0, "call_enabled": 0,
    })

    # Save activity log
    seq_oid = outreach.get("sequence_id")
    round_number = outreach.get("round_number", 1)
    activity_doc = {
        "activity_type": "round_paused",
        "shift_id":      shift_oid,
        "outreach_id":   outreach["_id"],
        "metadata": {
            "sequence_id":   str(seq_oid) if seq_oid else None,
            "shift_id":      payload.shift_id,
            "outreach_id":   str(outreach["_id"]),
            "round_number":  round_number,
            "available":     available_count,
            "declined":      declined_count,
            "no_reply":      no_reply_count,
            "summary":       f"Round {round_number} paused · {available_count} available, {declined_count} declined, {no_reply_count} no-reply",
        },
        "created_at": now,
    }
    if seq_oid:
        activity_doc["sequence_id"] = seq_oid
    await db["activities"].insert_one(activity_doc)

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


# ── POST /outreach/restart ────────────────────────────────────────────────────

@router.post(
    "/restart",
    summary="Restart a paused outreach — re-enables call_enabled and logs activity",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def restart_outreach(request: Request, payload: PauseOutreachRequest):
    """
    Body: { "shift_id": "<shift _id>" }

    1. Finds the latest paused (status=2) outreach for the shift.
    2. Sets outreach_status = 1 (Live), paused_at = null.
    3. Sets call_enabled = 1 for shifts_users where outreach_id matches.
    4. Saves a round_started activity log.
    """
    db = _get_db()

    if not ObjectId.is_valid(payload.shift_id):
        raise HTTPException(status_code=422, detail="Invalid shift_id")

    shift_oid = ObjectId(payload.shift_id)

    # Find the latest paused outreach for this shift
    outreach = await db["outreach"].find_one(
        {"shift_id": shift_oid, "outreach_status": 2},
        sort=[("created_at", -1)],
    )
    if not outreach:
        raise HTTPException(
            status_code=404,
            detail="No paused outreach found for this shift"
        )

    now = datetime.now(timezone.utc)

    # ── Sync shifts_pool → shifts_users for this outreach ─────────────────────
    outreach_oid = outreach["_id"]

    # Current pool user_ids
    pool_docs = await db["shifts_pool"].find({"shift_id": shift_oid}, {"user_id": 1}).to_list(5000)
    pool_user_ids = {str(pd["user_id"]) for pd in pool_docs if pd.get("user_id")}

    # Current shifts_users user_ids for this outreach
    su_docs = await db["shifts_users"].find(
        {"shift_id": shift_oid, "outreach_id": outreach_oid},
        {"user_id": 1}
    ).to_list(5000)
    su_user_ids = {str(su["user_id"]) for su in su_docs if su.get("user_id")}

    # Add new users (in pool but not in shifts_users)
    added = 0
    for pd in pool_docs:
        uid_str = str(pd.get("user_id", ""))
        if uid_str and uid_str not in su_user_ids:
            # Skip if availability == 1 in any existing shifts_users record
            exists_avail = await db["shifts_users"].find_one({
                "shift_id":    shift_oid,
                "user_id":     pd["user_id"],
                "availability": 1,
            })
            if exists_avail:
                continue
            await db["shifts_users"].insert_one({
                "user_id":            pd["user_id"],
                "shift_id":           shift_oid,
                "outreach_id":        outreach_oid,
                "assigned_at":        now,
                "availability":       6,
                "call_enabled":       1,
                "call_processed":     0,
                "call_processed_at":  now,
                "conversation_id":    None,
                "agent_id":           None,
                "call_status":        0,
                "call_summary_title": None,
                "ended_at":           None,
                "started_at":         None,
                "updated_at":         now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            })
            added += 1

    # Remove users no longer in pool (in shifts_users but not in pool) where availability != 1
    removed = 0
    for su in su_docs:
        uid_str = str(su.get("user_id", ""))
        if uid_str and uid_str not in pool_user_ids:
            # Only remove if not already available (availability != 1)
            full_su = await db["shifts_users"].find_one({"_id": su["_id"]})
            if full_su and full_su.get("availability") != 1:
                await db["shifts_users"].delete_one({"_id": su["_id"]})
                removed += 1
    await db["outreach"].update_one(
        {"_id": outreach["_id"]},
        {"$set": {
            "outreach_status": 1,
            "paused_at":       None,
            "updated_at":      now,
        }}
    )

    # Re-enable call_enabled = 1 for shifts_users with this outreach_id
    result = await db["shifts_users"].update_many(
        {"outreach_id": outreach["_id"]},
        {"$set": {
            "call_enabled": 1,
            "updated_at":   now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        }}
    )

    # Get counts for activity log
    available_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "availability": {"$gt": 0},
    })
    declined_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "availability": {"$ne": 1},
    })
    no_reply_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "call_processed": 0, "call_enabled": 1,
    })

    # Save activity log
    seq_oid      = outreach.get("sequence_id")
    round_number = outreach.get("round_number", 1)
    activity_doc = {
        "activity_type": "round_started",
        "shift_id":      shift_oid,
        "outreach_id":   outreach["_id"],
        "metadata": {
            "sequence_id":      str(seq_oid) if seq_oid else None,
            "shift_id":         payload.shift_id,
            "outreach_id":      str(outreach["_id"]),
            "round_number":     round_number,
            "available":        available_count,
            "declined":         declined_count,
            "no_reply":         no_reply_count,
            "call_enabled_set": result.modified_count,
            "summary":          f"Round {round_number} restarted · {available_count} available, {declined_count} declined, {no_reply_count} no-reply",
        },
        "created_at": now,
    }
    if seq_oid:
        activity_doc["sequence_id"] = seq_oid
    await db["activities"].insert_one(activity_doc)

    logger.info(
        f"Outreach restarted: shift={payload.shift_id} "
        f"outreach={outreach['_id']} re-enabled={result.modified_count}"
    )

    return {
        "success":              True,
        "message":              "Outreach restarted",
        "outreach_id":          str(outreach["_id"]),
        "shift_id":             payload.shift_id,
        "outreach_status":      1,
        "outreach_status_text": "Live",
        "shifts_users_updated": result.modified_count,
        "pool_sync": {
            "added":   added,
            "removed": removed,
        },
    }


class EndOutreachRequest(BaseModel):
    shift_id:       str
    end_reason_id:  Optional[str] = None   # outreach_end_reasons._id (optional)
    end_reason_text: Optional[str] = None  # free text override


# ── POST /outreach/end ────────────────────────────────────────────────────────

@router.post(
    "/end",
    summary="End the current outreach round for a shift",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def end_outreach(request: Request, payload: EndOutreachRequest):
    """
    Body: { "shift_id": "<shift _id>", "end_reason_id": "<optional>", "end_reason_text": "<optional>" }

    Rules:
    - Can only end an outreach that is Live (1) or Paused (2).
    - Cannot end if outreach_status is already Completed (10).
    - Can only end once per round — a second end requires a new round to exist.
    - Sets outreach_status = 3 (Ended), ended_at = now.
    - Sets call_enabled = 0 for shifts_users where call_processed = 0.
    - Logs round_ended activity.
    """
    db = _get_db()

    if not ObjectId.is_valid(payload.shift_id):
        raise HTTPException(status_code=422, detail="Invalid shift_id")

    shift_oid = ObjectId(payload.shift_id)

    # Find the latest outreach for this shift
    latest = await db["outreach"].find_one(
        {"shift_id": shift_oid},
        sort=[("created_at", -1)],
    )
    if not latest:
        raise HTTPException(status_code=404, detail="No outreach found for this shift")

    current_status = latest.get("outreach_status", 0)

    # Cannot end if already Completed
    if current_status == 10:
        raise HTTPException(
            status_code=409,
            detail="Outreach is already Completed and cannot be ended"
        )

    # Cannot end if already Ended (3) — a new round must exist first
    if current_status == 3:
        raise HTTPException(
            status_code=409,
            detail="Outreach round already ended. Create a new round before ending again"
        )

    # Can only end Live (1) or Paused (2)
    if current_status not in (1, 2):
        raise HTTPException(
            status_code=409,
            detail=f"Outreach cannot be ended from status {current_status}"
        )

    now = datetime.now(timezone.utc)

    # Set outreach to Ended (3)
    await db["outreach"].update_one(
        {"_id": latest["_id"]},
        {"$set": {
            "outreach_status": 3,
            "ended_at":        now,
            "updated_at":      now,
        }}
    )

    # Disable unprocessed shifts_users for this shift
    result = await db["shifts_users"].update_many(
        {
            "shift_id":      shift_oid,
            "call_processed": 0,
        },
        {"$set": {
            "call_enabled": 0,
            "updated_at":   now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        }}
    )

    # Get counts for activity log
    available_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "availability": {"$gt": 0},
    })
    declined_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "availability": {"$ne": 1},
    })
    no_reply_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "call_processed": 0,
    })

    # Resolve end reason
    end_reason_label = payload.end_reason_text or None
    if payload.end_reason_id and ObjectId.is_valid(payload.end_reason_id):
        reason_doc = await db["outreach_end_reasons"].find_one(
            {"_id": ObjectId(payload.end_reason_id)}, {"reason": 1}
        )
        if reason_doc:
            end_reason_label = reason_doc.get("reason")

    # Also store reason on outreach doc
    if end_reason_label:
        await db["outreach"].update_one(
            {"_id": latest["_id"]},
            {"$set": {"end_reason": end_reason_label, "end_reason_id": payload.end_reason_id}}
        )

    # Save activity log
    seq_oid      = latest.get("sequence_id")
    round_number = latest.get("round_number", 1)
    activity_doc = {
        "activity_type": "round_ended",
        "shift_id":      shift_oid,
        "outreach_id":   latest["_id"],
        "metadata": {
            "sequence_id":    str(seq_oid) if seq_oid else None,
            "shift_id":       payload.shift_id,
            "outreach_id":    str(latest["_id"]),
            "round_number":   round_number,
            "available":      available_count,
            "declined":       declined_count,
            "no_reply":       no_reply_count,
            "call_disabled":  result.modified_count,
            "end_reason":     end_reason_label,
            "summary":        f"Round {round_number} ended · {available_count} available, {declined_count} declined, {no_reply_count} no-reply",
        },
        "created_at": now,
    }
    if seq_oid:
        activity_doc["sequence_id"] = seq_oid
    await db["activities"].insert_one(activity_doc)

    logger.info(
        f"Outreach ended: shift={payload.shift_id} "
        f"outreach={latest['_id']} disabled={result.modified_count}"
    )

    return {
        "success":              True,
        "message":              f"Round {round_number} ended",
        "outreach_id":          str(latest["_id"]),
        "shift_id":             payload.shift_id,
        "outreach_status":      3,
        "outreach_status_text": "Ended",
        "end_reason":           end_reason_label,
        "shifts_users_updated": result.modified_count,
    }


# ── POST /outreach/complete ───────────────────────────────────────────────────

@router.post(
    "/complete",
    summary="Mark outreach as completed (outreach_status = 10)",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def complete_outreach(request: Request, payload: PauseOutreachRequest):
    """
    Body: { "shift_id": "<shift _id>" }

    Rules:
    - Can only complete from Live (1), Paused (2), or Ended (3).
    - Already Completed (10) returns 409.
    - Sets outreach_status = 10 (Completed), ended_at = now.
    - Sets call_enabled = 0 for shifts_users where call_processed = 0.
    - Logs round_completed activity.
    """
    db = _get_db()

    if not ObjectId.is_valid(payload.shift_id):
        raise HTTPException(status_code=422, detail="Invalid shift_id")

    shift_oid = ObjectId(payload.shift_id)

    # Find the latest outreach for this shift
    latest = await db["outreach"].find_one(
        {"shift_id": shift_oid},
        sort=[("created_at", -1)],
    )
    if not latest:
        raise HTTPException(status_code=404, detail="No outreach found for this shift")

    current_status = latest.get("outreach_status", 0)

    if current_status == 10:
        raise HTTPException(status_code=409, detail="Outreach is already Completed")

    if current_status not in (1, 2, 3):
        raise HTTPException(
            status_code=409,
            detail=f"Outreach cannot be completed from status {current_status}"
        )

    now = datetime.now(timezone.utc)

    # Set outreach to Completed (10)
    await db["outreach"].update_one(
        {"_id": latest["_id"]},
        {"$set": {
            "outreach_status": 10,
            "ended_at":        now,
            "updated_at":      now,
        }}
    )

    # Disable unprocessed shifts_users for this shift
    result = await db["shifts_users"].update_many(
        {
            "shift_id":       shift_oid,
            "call_processed": 0,
        },
        {"$set": {
            "call_enabled": 0,
            "updated_at":   now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        }}
    )

    # Get counts for activity log
    available_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "availability": {"$gt": 0},
    })
    declined_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "availability": {"$ne": 1},
    })
    no_reply_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "call_processed": 0,
    })

    # Save activity log
    seq_oid      = latest.get("sequence_id")
    round_number = latest.get("round_number", 1)
    activity_doc = {
        "activity_type": "round_completed",
        "shift_id":      shift_oid,
        "outreach_id":   latest["_id"],
        "metadata": {
            "sequence_id":   str(seq_oid) if seq_oid else None,
            "shift_id":      payload.shift_id,
            "outreach_id":   str(latest["_id"]),
            "round_number":  round_number,
            "available":     available_count,
            "declined":      declined_count,
            "no_reply":      no_reply_count,
            "call_disabled": result.modified_count,
            "summary":       f"Round {round_number} completed · {available_count} available, {declined_count} declined, {no_reply_count} no-reply",
        },
        "created_at": now,
    }
    if seq_oid:
        activity_doc["sequence_id"] = seq_oid
    await db["activities"].insert_one(activity_doc)

    logger.info(
        f"Outreach completed: shift={payload.shift_id} "
        f"outreach={latest['_id']} disabled={result.modified_count}"
    )

    return {
        "success":              True,
        "message":              f"Round {round_number} completed",
        "outreach_id":          str(latest["_id"]),
        "shift_id":             payload.shift_id,
        "outreach_status":      10,
        "outreach_status_text": "Completed",
        "shifts_users_updated": result.modified_count,
    }


# ── POST /outreach/detail ─────────────────────────────────────────────────────

class OutreachDetailIdRequest(BaseModel):
    outreach_id: str


@router.post(
    "/detail",
    summary="Get full outreach details including shifts_users",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def get_outreach_detail(request: Request, payload: OutreachDetailIdRequest):
    """
    Body: { "outreach_id": "<outreach._id>" }
    Returns outreach document enriched with:
    - sequence name
    - shift details
    - all shifts_users records with user info
    - counts
    """
    db = _get_db()

    if not ObjectId.is_valid(payload.outreach_id):
        raise HTTPException(status_code=422, detail="Invalid outreach_id")

    outreach_oid = ObjectId(payload.outreach_id)
    outreach = await db["outreach"].find_one({"_id": outreach_oid})
    if not outreach:
        raise HTTPException(status_code=404, detail="Outreach not found")

    STATUS_TEXT = {0: "Not Started", 1: "Live", 2: "Paused", 3: "Ended", 10: "Completed"}

    # Resolve sequence name
    seq_name = None
    seq_oid  = outreach.get("sequence_id")
    if seq_oid:
        seq = await db["sequences"].find_one({"_id": seq_oid}, {"name": 1})
        if seq:
            seq_name = seq.get("name")

    # Resolve shift name
    shift_oid  = outreach.get("shift_id")
    shift_info = None
    if shift_oid:
        sh = await db["shifts"].find_one(
            {"_id": shift_oid},
            {"name": 1, "shift_code": 1, "location": 1, "date": 1,
             "start_time": 1, "end_time": 1, "user_type": 1, "shift_timing": 1}
        )
        if sh:
            shift_info = {
                "shift_id":    str(shift_oid),
                "name":        sh.get("name") or sh.get("shift_code"),
                "location":    sh.get("location"),
                "date":        sh["date"].isoformat() if sh.get("date") and hasattr(sh["date"], "isoformat") else str(sh.get("date", "")),
                "start_time":  sh.get("start_time"),
                "end_time":    sh.get("end_time"),
                "shift_timing": sh.get("shift_timing"),
                "user_type":   sh.get("user_type"),
            }

    # Fetch all shifts_users for this outreach
    su_docs = await db["shifts_users"].find(
        {"outreach_id": outreach_oid},
        {"user_id": 1, "availability": 1, "call_enabled": 1, "call_processed": 1,
         "call_processed_at": 1, "call_status": 1, "assigned_at": 1, "updated_at": 1}
    ).to_list(length=1000)

    # Batch user lookup
    user_oids = [su["user_id"] for su in su_docs if su.get("user_id") and ObjectId.is_valid(str(su.get("user_id", "")))]
    user_map: dict = {}
    if user_oids:
        async for u in db["users"].find(
            {"_id": {"$in": user_oids}},
            {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
             "xn_user_id": 1, "designation": 1, "rating": 1}
        ):
            user_map[str(u["_id"])] = u

    shifts_users_list = []
    for su in su_docs:
        uid_str = str(su.get("user_id", ""))
        u = user_map.get(uid_str, {})
        shifts_users_list.append({
            "id":              str(su["_id"]),
            "user_id":         uid_str,
            "xn_user_id":      u.get("xn_user_id"),
            "name":            " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() or "—",
            "email":           u.get("email"),
            "phone":           u.get("phone"),
            "designation":     u.get("designation"),
            "rating":          u.get("rating"),
            "availability":    su.get("availability"),
            "call_enabled":    su.get("call_enabled"),
            "call_processed":  su.get("call_processed"),
            "call_status":     su.get("call_status"),
            "call_processed_at": su["call_processed_at"].isoformat() if su.get("call_processed_at") and hasattr(su["call_processed_at"], "isoformat") else None,
            "assigned_at":     su["assigned_at"].isoformat() if su.get("assigned_at") and hasattr(su["assigned_at"], "isoformat") else None,
        })

    # Counts
    total     = len(shifts_users_list)
    available = sum(1 for s in shifts_users_list if s["availability"] == 1)
    pending   = sum(1 for s in shifts_users_list if s["call_enabled"] == 1 and s["call_processed"] == 0)
    processed = sum(1 for s in shifts_users_list if s["call_processed"] == 1)

    o_status = outreach.get("outreach_status", 0)

    return {
        "success": True,
        "data": {
            "id":                   str(outreach["_id"]),
            "shift_id":             str(shift_oid) if shift_oid else None,
            "sequence_id":          str(seq_oid) if seq_oid else None,
            "sequence_name":        seq_name,
            "round_number":         outreach.get("round_number"),
            "outreach_status":      o_status,
            "outreach_status_text": STATUS_TEXT.get(o_status, "Not Started"),
            "end_reason":           outreach.get("end_reason"),
            "started_at":           outreach["started_at"].isoformat() if outreach.get("started_at") and hasattr(outreach["started_at"], "isoformat") else None,
            "paused_at":            outreach["paused_at"].isoformat() if outreach.get("paused_at") and hasattr(outreach["paused_at"], "isoformat") else None,
            "ended_at":             outreach["ended_at"].isoformat() if outreach.get("ended_at") and hasattr(outreach["ended_at"], "isoformat") else None,
            "created_at":           outreach["created_at"].isoformat() if outreach.get("created_at") and hasattr(outreach["created_at"], "isoformat") else None,
            "shift":                shift_info,
            "counts": {
                "total":     total,
                "available": available,
                "pending":   pending,
                "processed": processed,
            },
            "shifts_users": shifts_users_list,
        },
    }


def _format_call_time(dt) -> str:
    """Format call_processed_at as Today HH:MM, Yesterday HH:MM, or D Mon HH:MM."""
    if not dt:
        return None
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    if hasattr(dt, "tzinfo") and dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    time_str = dt.strftime("%H:%M")
    diff_days = (now.date() - dt.date()).days
    if diff_days == 0:
        return f"Today {time_str}"
    elif diff_days == 1:
        return f"Yesterday {time_str}"
    else:
        return dt.strftime("%-d %b %H:%M")


# ── POST /outreach/staff_list ─────────────────────────────────────────────────

class OutreachStaffListRequest(BaseModel):
    outreach_id: str


@router.post(
    "/staff_list",
    summary="Get outreach record with full shifts_users list",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def outreach_staff_list(request: Request, payload: OutreachStaffListRequest):
    """
    Body: { "outreach_id": "<outreach._id>" }
    Returns outreach doc + all shifts_users enriched with user details + counts.
    """
    db = _get_db()

    if not ObjectId.is_valid(payload.outreach_id):
        raise HTTPException(status_code=422, detail="Invalid outreach_id")

    outreach_oid = ObjectId(payload.outreach_id)
    outreach = await db["outreach"].find_one({"_id": outreach_oid})
    if not outreach:
        raise HTTPException(status_code=404, detail="Outreach not found")

    STATUS_TEXT = {0: "Not Started", 1: "Live", 2: "Paused", 3: "Ended", 10: "Completed"}

    # Sequence name
    seq_name = None
    seq_oid  = outreach.get("sequence_id")
    if seq_oid:
        seq = await db["sequences"].find_one({"_id": seq_oid}, {"name": 1})
        if seq:
            seq_name = seq.get("name")

    # Shift info
    shift_oid  = outreach.get("shift_id")
    shift_info = None
    if shift_oid:
        sh = await db["shifts"].find_one(
            {"_id": shift_oid},
            {"name": 1, "shift_code": 1, "location": 1, "date": 1,
             "start_time": 1, "end_time": 1, "user_type": 1, "shift_timing": 1}
        )
        if sh:
            shift_info = {
                "shift_id":    str(shift_oid),
                "name":        sh.get("name") or sh.get("shift_code"),
                "location":    sh.get("location"),
                "date":        sh["date"].isoformat() if sh.get("date") and hasattr(sh["date"], "isoformat") else str(sh.get("date", "")),
                "start_time":  sh.get("start_time"),
                "end_time":    sh.get("end_time"),
                "shift_timing": sh.get("shift_timing"),
                "user_type":   sh.get("user_type"),
            }

    # Fetch shifts_users for this outreach
    su_docs = await db["shifts_users"].find(
        {"outreach_id": outreach_oid},
        {"user_id": 1, "availability": 1, "call_enabled": 1, "call_processed": 1,
         "call_processed_at": 1, "call_status": 1, "assigned_at": 1, "flag": 1}
    ).to_list(length=2000)

    # Batch user lookup
    user_oids = [
        ObjectId(str(su["user_id"])) for su in su_docs
        if su.get("user_id") and ObjectId.is_valid(str(su.get("user_id", "")))
    ]
    user_map: dict = {}
    if user_oids:
        async for u in db["users"].find(
            {"_id": {"$in": user_oids}},
            {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
             "xn_user_id": 1, "designation": 1, "rating": 1}
        ):
            user_map[str(u["_id"])] = u

    AVAILABILITY_TEXT = {
        1: "Available",
        0: "Not Available",
        3: "Voicemail",
        4: "Call Not Attended",
        6: "Call Not Triggered",
    }

    shifts_users_list = []
    for su in su_docs:
        uid_str = str(su.get("user_id", ""))
        u = user_map.get(uid_str, {})
        shifts_users_list.append({
            "id":              str(su["_id"]),
            "user_id":         uid_str,
            "xn_user_id":      u.get("xn_user_id"),
            "name":            " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() or "—",
            "email":           u.get("email"),
            "phone":           u.get("phone"),
            "designation":     u.get("designation"),
            "rating":          u.get("rating"),
            "availability":       su.get("availability"),
            "availability_text":  AVAILABILITY_TEXT.get(su.get("availability"), "Unknown"),
            "call_enabled":       su.get("call_enabled"),
            "channel":            "Phone",
            "call_processed":     su.get("call_processed"),
            "call_processed_text": "Sent" if su.get("call_processed") == 1 else "Queued",
            "start_time":         _format_call_time(su.get("call_processed_at")) if su.get("call_processed_at") and hasattr(su.get("call_processed_at"), "date") else None,
            "flag":               su.get("flag", 0),
            "call_status":       su.get("call_status"),
            "call_processed_at": su["call_processed_at"].isoformat() if su.get("call_processed_at") and hasattr(su["call_processed_at"], "isoformat") else None,
            "assigned_at":       su["assigned_at"].isoformat() if su.get("assigned_at") and hasattr(su["assigned_at"], "isoformat") else None,
        })

    total     = len(shifts_users_list)
    available = sum(1 for s in shifts_users_list if s["availability"] == 1)
    pending   = sum(1 for s in shifts_users_list if s["call_enabled"] == 1 and s["call_processed"] == 0)
    processed = sum(1 for s in shifts_users_list if s["call_processed"] == 1)

    o_status = outreach.get("outreach_status", 0)

    return {
        "success": True,
        "data": {
            "id":                   str(outreach["_id"]),
            "shift_id":             str(shift_oid) if shift_oid else None,
            "sequence_id":          str(seq_oid) if seq_oid else None,
            "sequence_name":        seq_name,
            "round_number":         outreach.get("round_number"),
            "outreach_status":      o_status,
            "outreach_status_text": STATUS_TEXT.get(o_status, "Not Started"),
            "end_reason":           outreach.get("end_reason"),
            "started_at":           outreach["started_at"].isoformat() if outreach.get("started_at") and hasattr(outreach["started_at"], "isoformat") else None,
            "paused_at":            outreach["paused_at"].isoformat() if outreach.get("paused_at") and hasattr(outreach["paused_at"], "isoformat") else None,
            "ended_at":             outreach["ended_at"].isoformat() if outreach.get("ended_at") and hasattr(outreach["ended_at"], "isoformat") else None,
            "created_at":           outreach["created_at"].isoformat() if outreach.get("created_at") and hasattr(outreach["created_at"], "isoformat") else None,
            "shift":                shift_info,
            "counts": {
                "total":     total,
                "available": available,
                "pending":   pending,
                "processed": processed,
            },
            "shifts_users": shifts_users_list,
        },
    }


# ── POST /outreach/flag ───────────────────────────────────────────────────────

class FlagStaffRequest(BaseModel):
    outreach_id:     str
    shifts_users_id: str
    flag:            int = 1   # 1 = flagged, 0 = unflag


@router.post(
    "/flag",
    summary="Flag or unflag a staff member in shifts_users for a specific outreach",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def flag_staff(request: Request, payload: FlagStaffRequest):
    """
    Body: { "outreach_id": "...", "shifts_users_id": "...", "flag": 1 }
    Sets shifts_users.flag = 1 (flagged) or 0 (unflagged)
    for the record matching both _id and outreach_id.
    """
    db = _get_db()

    if not ObjectId.is_valid(payload.outreach_id):
        raise HTTPException(status_code=422, detail="Invalid outreach_id")
    if not ObjectId.is_valid(payload.shifts_users_id):
        raise HTTPException(status_code=422, detail="Invalid shifts_users_id")

    outreach_oid = ObjectId(payload.outreach_id)
    su_oid       = ObjectId(payload.shifts_users_id)

    doc = await db["shifts_users"].find_one({
        "_id":        su_oid,
        "outreach_id": outreach_oid,
    }, {"_id": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="shifts_users record not found for this outreach")

    now = datetime.now(timezone.utc)
    await db["shifts_users"].update_one(
        {"_id": su_oid},
        {"$set": {"flag": payload.flag, "updated_at": now.strftime("%Y-%m-%dT%H:%M:%S+00:00")}}
    )

    return {
        "success":         True,
        "message":         "Flagged" if payload.flag else "Unflagged",
        "outreach_id":     payload.outreach_id,
        "shifts_users_id": payload.shifts_users_id,
        "flag":            payload.flag,
    }


# ── POST /outreach/remove_staff ───────────────────────────────────────────────

class RemoveStaffRequest(BaseModel):
    outreach_id:     str
    shifts_users_id: str


@router.post(
    "/remove_staff",
    summary="Remove a staff member from an outreach (deletes shifts_users record)",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def remove_staff_from_outreach(request: Request, payload: RemoveStaffRequest):
    """
    Body: { "outreach_id": "...", "shifts_users_id": "..." }
    Deletes the shifts_users record that matches both _id and outreach_id.
    Guards: cannot remove from a Completed (10) outreach.
    """
    db = _get_db()

    if not ObjectId.is_valid(payload.outreach_id):
        raise HTTPException(status_code=422, detail="Invalid outreach_id")
    if not ObjectId.is_valid(payload.shifts_users_id):
        raise HTTPException(status_code=422, detail="Invalid shifts_users_id")

    outreach_oid = ObjectId(payload.outreach_id)
    su_oid       = ObjectId(payload.shifts_users_id)

    # Validate outreach exists and is not completed
    outreach = await db["outreach"].find_one({"_id": outreach_oid}, {"outreach_status": 1})
    if not outreach:
        raise HTTPException(status_code=404, detail="Outreach not found")
    if outreach.get("outreach_status") == 10:
        raise HTTPException(status_code=409, detail="Cannot remove staff from a Completed outreach")

    # Find the shifts_users record
    su_doc = await db["shifts_users"].find_one({
        "_id":        su_oid,
        "outreach_id": outreach_oid,
    }, {"_id": 1, "user_id": 1})
    if not su_doc:
        raise HTTPException(status_code=404, detail="Staff record not found for this outreach")

    await db["shifts_users"].delete_one({"_id": su_oid})

    return {
        "success":         True,
        "message":         "Staff removed from outreach",
        "outreach_id":     payload.outreach_id,
        "shifts_users_id": payload.shifts_users_id,
        "user_id":         str(su_doc.get("user_id", "")),
    }


# ── POST /outreach/transcription ──────────────────────────────────────────────

class TranscriptionRequest(BaseModel):
    shift_id: str
    user_id:  str


@router.post(
    "/transcription",
    summary="Get AI call transcription for a staff member on a shift",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def get_transcription(request: Request, payload: TranscriptionRequest):
    """
    Body: { "shift_id": "...", "user_id": "..." }
    Fetches conversation turns from shift_booking_conv collection.
    """
    db = _get_db()

    conv = await db["shift_booking_conv"].find_one(
        {"shift_id": payload.shift_id, "user_id": payload.user_id},
    )
    if not conv:
        raise HTTPException(status_code=404, detail="No conversation found for this shift/user")

    # Serialize
    def _fmt(dt):
        return dt.isoformat() if dt and hasattr(dt, "isoformat") else None

    turns = []
    for turn in conv.get("turns", []):
        turns.append({
            "role":    turn.get("role"),
            "message": turn.get("message") or turn.get("text"),
            "ts":      _fmt(turn.get("ts")),
        })

    return {
        "success": True,
        "data": {
            "id":                          str(conv["_id"]),
            "shift_id":                    payload.shift_id,
            "user_id":                     payload.user_id,
            "elevenlabs_conversation_id":  conv.get("elevenlabs_conversation_id"),
            "started_at":                  _fmt(conv.get("started_at")),
            "ended_at":                    _fmt(conv.get("ended_at")),
            "turns":                       turns,
            "has_audio":                   bool(conv.get("elevenlabs_conversation_id")),
        },
    }


# ── POST /outreach/transcription/audio ───────────────────────────────────────

@router.post(
    "/transcription/audio",
    summary="Get audio URL for an AI call transcription",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def get_transcription_audio(request: Request, payload: TranscriptionRequest):
    """
    Body: { "shift_id": "...", "user_id": "..." }
    Returns a proxied audio stream from ElevenLabs using the stored conversation ID.
    """
    import os
    from fastapi.responses import StreamingResponse

    db = _get_db()

    conv = await db["shift_booking_conv"].find_one(
        {"shift_id": payload.shift_id, "user_id": payload.user_id},
        {"elevenlabs_conversation_id": 1}
    )
    if not conv:
        raise HTTPException(status_code=404, detail="No conversation found")

    el_conv_id = conv.get("elevenlabs_conversation_id")
    if not el_conv_id:
        raise HTTPException(status_code=404, detail="No audio available for this conversation")

    api_key = settings.ELEVENLABS_API_KEY or ""

    url = f"https://api.elevenlabs.io/v1/convai/conversations/{el_conv_id}/audio"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers={"xi-api-key": api_key})

    if resp.status_code != 200:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Failed to fetch audio from ElevenLabs: {resp.text[:200]} | api_key_used: {api_key[:8]}..."
        )

    return StreamingResponse(
        iter([resp.content]),
        media_type="audio/mpeg",
        headers={"Content-Disposition": "inline; filename=call_audio.mp3"},
    )
