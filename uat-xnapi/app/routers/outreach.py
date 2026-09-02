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

    # ── Pool composition from shifts_pool (staff added via /shift-users/bulk) ─
    total_staff    = await db["shifts_pool"].count_documents({"shift_id": shift_oid, "selected": {"$ne": 0}})
    phone_count    = await db["shifts_pool"].count_documents({"shift_id": shift_oid, "selected": {"$ne": 0}, "channel": {"$in": ["Phone", None, ""]}})
    whatsapp_count = await db["shifts_pool"].count_documents({"shift_id": shift_oid, "selected": {"$ne": 0}, "channel": "WhatsApp"})
    email_count    = await db["shifts_pool"].count_documents({"shift_id": shift_oid, "selected": {"$ne": 0}, "channel": "Email"})
    no_channel     = await db["shifts_pool"].count_documents({"shift_id": shift_oid, "selected": {"$ne": 0}, "channel": {"$exists": False}})
    phone_count   += no_channel
    # whatsapp and email are placeholders until those fields are added
    pool_summary = (
        f"{total_staff} staff · phone {phone_count}, "
        f"WhatsApp {whatsapp_count}, email {email_count}"
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
            "pause_on": pause_on,
            "date_one": 0,
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

    # Round number — check if latest outreach is Ended (3), if so reset all and start round 1
    latest_outreach = await db["outreach"].find_one(
        {"shift_id": shift_oid}, sort=[("created_at", -1)]
    )
    now = datetime.now(timezone.utc)

    outreach_count = await db["outreach"].count_documents({"shift_id": shift_oid})
    round_number   = outreach_count + 1

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
    pool_docs = await db["shifts_pool"].find({"shift_id": shift_oid, "selected": {"$ne": 0}}).to_list(length=5000)

    # ── Sort pool users based on sequence name ────────────────────────────────
    seq_name = (sequence.get("name") or "").strip().lower()

    if pool_docs:
        user_oids_pool = [pd["user_id"] for pd in pool_docs if pd.get("user_id")]

        if "previously worked here" in seq_name:
            # Sort by number of prior shifts at this shift's client
            client_id = shift.get("client_id")
            prior_map: dict = {}
            if client_id:
                async for s in db["shifts"].find(
                    {"client_id": client_id, "staff_email": {"$exists": True, "$ne": None}},
                    {"staff_email": 1}
                ):
                    prior_map[s.get("staff_email", "")] = prior_map.get(s.get("staff_email", ""), 0) + 1
            # Get emails for pool users
            email_map: dict = {}
            async for u in db["users"].find(
                {"_id": {"$in": user_oids_pool}}, {"email": 1}
            ):
                email_map[str(u["_id"])] = u.get("email", "")
            pool_docs.sort(
                key=lambda pd: prior_map.get(email_map.get(str(pd.get("user_id", "")), ""), 0),
                reverse=True
            )

        elif "by rating" in seq_name or "by favourites" in seq_name:
            # Sort by users.rating descending
            rating_map: dict = {}
            async for u in db["users"].find(
                {"_id": {"$in": user_oids_pool}}, {"rating": 1}
            ):
                rating_map[str(u["_id"])] = u.get("rating") or 0
            pool_docs.sort(
                key=lambda pd: rating_map.get(str(pd.get("user_id", "")), 0),
                reverse=True
            )

        elif "by distance" in seq_name:
            # Sort by distance ascending using client location
            client_loc = None
            if shift.get("client_id"):
                cl = await db["clients"].find_one(
                    {"_id": shift["client_id"]}, {"latitude": 1, "longitude": 1}
                )
                if cl and cl.get("latitude") and cl.get("longitude"):
                    client_loc = (float(cl["latitude"]), float(cl["longitude"]))

            if client_loc:
                from app.routers.staff import _haversine_km as _hav_o, _user_coords as _uc_o
                loc_map: dict = {}
                async for u in db["users"].find(
                    {"_id": {"$in": user_oids_pool}},
                    {"location": 1, "latitude": 1, "longitude": 1}
                ):
                    coords = _uc_o(u)
                    if coords:
                        dist = _hav_o(client_loc[0], client_loc[1], coords[0], coords[1])
                        loc_map[str(u["_id"])] = dist
                    else:
                        loc_map[str(u["_id"])] = 99999
                pool_docs.sort(
                    key=lambda pd: loc_map.get(str(pd.get("user_id", "")), 99999)
                )

    inserted_count = 0
    skipped        = 0
    call_order     = 1
    for pd in pool_docs:
        user_oid_pool = pd.get("user_id")
        if not user_oid_pool:
            continue
        # Skip confirmed (1) or not available (0)
        exists_skip = await db["shifts_users"].find_one({
            "shift_id":    shift_oid,
            "user_id":     user_oid_pool,
            "availability": {"$in": [0, 1]},
        })
        if exists_skip:
            skipped += 1
            continue

        # Check if exists with any other status — update for new outreach round
        _ch = pd.get("channel", "Phone") or "Phone"
        _avail_init = 7 if _ch in ("WhatsApp", "Email") else 6
        exists_any = await db["shifts_users"].find_one({
            "shift_id": shift_oid,
            "user_id":  user_oid_pool,
        })
        if exists_any:
            await db["shifts_users"].update_one(
                {"_id": exists_any["_id"]},
                {"$set": {
                    "outreach_id":        outreach_oid,
                    "channel":            _ch,
                    "assigned_at":        now,
                    "availability":       _avail_init,
                    "call_enabled":       1,
                    "call_processed":     0,
                    "call_processed_at":  now,
                    "call_status":        0,
                    "call_summary_title": None,
                    "ended_at":           None,
                    "started_at":         None,
                    "call_order":         call_order,
                    "updated_at":         now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                }}
            )
            inserted_count += 1
            call_order     += 1
            continue
        su_doc = {
            "user_id":            user_oid_pool,
            "shift_id":           shift_oid,
            "outreach_id":        outreach_oid,
            "channel":            _ch,
            "assigned_at":        now,
            "availability":       _avail_init,
            "call_enabled":       1,
            "call_processed":     0,
            "call_processed_at":  now,
            "conversation_id":    None,
            "agent_id":           None,
            "call_status":        0,
            "call_summary_title": None,
            "ended_at":           None,
            "started_at":         None,
            "call_order":         call_order,
            "updated_at":         now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        }
        await db["shifts_users"].insert_one(su_doc)
        inserted_count += 1
        call_order     += 1

    class _Result:
        def __init__(self, n): self.modified_count = n
    updated = _Result(inserted_count)

    # Get counts for activity log
    available_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "outreach_id": outreach_oid, "availability": 1,
    })
    declined_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "outreach_id": outreach_oid, "availability": 0,
    })
    no_reply_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "outreach_id": outreach_oid, "availability": {"$in": [3, 4, 6, 7, 8]},
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
        "shift_id": shift_oid, "outreach_id": outreach_oid, "availability": 1,
    })
    declined_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "outreach_id": outreach_oid, "availability": 0,
    })
    no_reply_count = await db["shifts_users"].count_documents({
        "shift_id": shift_oid, "outreach_id": outreach_oid, "availability": {"$in": [3, 4, 6, 7, 8]},
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
            "id":              str(su.get("user_id", su["_id"])),
            "user_id":         uid_str,
            "xn_user_id":      u.get("xn_user_id"),
            "name":            " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() or "—",
            "email":           u.get("email"),
            "phone":           u.get("phone"),
            "designation":     u.get("designation"),
            "rating":          u.get("rating"),
            "availability":    su.get("availability"),
            "availability_text": {0:"Not Available",1:"Available",3:"Voicemail",4:"Call Not Attended",5:"In Call",6:"Call Not Triggered",7:"Not Sent",8:"No Response"}.get(su.get("availability"), "Unknown"),
            "call_enabled":    su.get("call_enabled"),
            "call_processed":      su.get("call_processed"),
            "call_processed_text": "Sent" if su.get("call_processed") == 1 else "Queued",
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
    """Format call_processed_at as Today HH:MM, Yesterday HH:MM, or D Mon HH:MM (Ireland time)."""
    if not dt:
        return None
    from datetime import datetime, timezone
    try:
        from zoneinfo import ZoneInfo
        irl_tz = ZoneInfo("Europe/Dublin")
    except Exception:
        import os
        irl_tz = timezone.utc  # fallback
    now = datetime.now(irl_tz)
    if hasattr(dt, "tzinfo") and dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_irl  = dt.astimezone(irl_tz)
    time_str = dt_irl.strftime("%H:%M")
    diff_days = (now.date() - dt_irl.date()).days
    if diff_days == 0:
        return f"Today {time_str}"
    elif diff_days == 1:
        return f"Yesterday {time_str}"
    else:
        return dt_irl.strftime("%-d %b %H:%M")


# ── POST /outreach/staff_list ─────────────────────────────────────────────────

class OutreachStaffListRequest(BaseModel):
    outreach_id: str
    shift_id:    Optional[str] = None
    page:        int = 1
    per_page:    int = 20


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
    is_group_outreach = False
    if not outreach:
        # Try group outreach collection
        outreach = await db["outreach_shift_group"].find_one({"_id": outreach_oid})
        if outreach:
            is_group_outreach = True
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

    # Fetch shifts_users — check shifts_users first, fallback to shifts_group_users
    _su_projection = {"user_id": 1, "availability": 1, "call_enabled": 1, "call_processed": 1,
                      "call_processed_at": 1, "call_status": 1, "assigned_at": 1, "flag": 1, "channel": 1,
                      "shift_id": 1, "outreach_id": 1, "conversation_id": 1, "ignored": 1,
                      "customer_feedback": 1, "availability_details": 1, "wa_phone": 1}
    su_docs = await db["shifts_users"].find(
        {"outreach_id": outreach_oid}, _su_projection
    ).to_list(length=2000)

    if not su_docs:
        su_docs = await db["shifts_group_users"].find(
            {"outreach_id": outreach_oid}, _su_projection
        ).to_list(length=2000)

    # Batch user lookup — include all fields needed for available_staff structure
    user_oids = [
        ObjectId(str(su["user_id"])) for su in su_docs
        if su.get("user_id") and ObjectId.is_valid(str(su.get("user_id", "")))
    ]
    user_map: dict = {}
    if user_oids:
        async for u in db["users"].find(
            {"_id": {"$in": user_oids}},
            {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
             "xn_user_id": 1, "designation": 1, "rating": 1,
             "county": 1, "county_id": 1, "tags": 1, "location": 1,
             "visa_hours_used": 1, "visa_hours_total": 1}
        ):
            user_map[str(u["_id"])] = u

    county_id_strs = list({str(u.get("county_id","")) for u in user_map.values() if u.get("county_id")})
    county_name_map: dict = {}
    if county_id_strs:
        async for c in db["county"].find(
            {"_id": {"$in": [ObjectId(i) for i in county_id_strs if ObjectId.is_valid(i)]}},
            {"name": 1}
        ):
            county_name_map[str(c["_id"])] = c.get("name", "")

    AVAILABILITY_TEXT = {
        1: "Available", 0: "Not Available", 3: "Voicemail",
        4: "Call Not Attended", 5: "Ongoing Call", 6: "Call Not Triggered", 7: "Not Sent", 8: "No Response",
    }

    # Get shift info for client coords + shift label
    shift_oid_for_staff = outreach.get("shift_id")
    shift_doc_for_staff = None
    client_lat_s = client_lng_s = None
    shift_label_s = placed_at_s = ""
    shift_staff_email = None
    shift_staff_id    = None
    if shift_oid_for_staff:
        shift_doc_for_staff = await db["shifts"].find_one(
            {"_id": shift_oid_for_staff},
            {"name": 1, "shift_code": 1, "date": 1, "start_time": 1,
             "end_time": 1, "user_type": 1, "shift_timing": 1, "client_id": 1,
             "staff_email": 1, "staff_id": 1}
        )
        if shift_doc_for_staff:
            shift_staff_email = shift_doc_for_staff.get("staff_email")
            shift_staff_id    = str(shift_doc_for_staff.get("staff_id", ""))
        if shift_doc_for_staff:
            sd = shift_doc_for_staff
            date_str = sd["date"].strftime("%d/%m/%Y") if sd.get("date") and hasattr(sd["date"], "strftime") else ""
            shift_label_s = f"{sd.get('user_type') or sd.get('shift_timing') or ''} • {date_str} • {sd.get('start_time','')} – {sd.get('end_time','')}"
            cid = sd.get("client_id")
            if cid:
                cl = await db["clients"].find_one({"xn_client_id": cid}, {"name": 1, "title": 1, "latitude": 1, "longitude": 1})
                if cl:
                    placed_at_s  = cl.get("name") or cl.get("title") or "—"
                    client_lat_s = cl.get("latitude")
                    client_lng_s = cl.get("longitude")

    from app.routers.staff import _haversine_km as _hav_s, _user_coords as _uc_s

    # Prior shifts lookup helper
    shift_client_id_s = shift_doc_for_staff.get("client_id") if shift_doc_for_staff else None
    prior_client_shift_ids_s = []
    if shift_client_id_s:
        prior_client_shift_ids_s = await db["shifts"].distinct("_id", {"client_id": shift_client_id_s})

    shifts_users_list = []
    # Get shift user_type for designation filtering
    _shift_user_type_filter = (shift_info.get("user_type") or "").strip().lower() if shift_info else ""

    for su in su_docs:
        uid_str = str(su.get("user_id", ""))
        u = user_map.get(uid_str, {})

        # Skip if user designation doesn't match shift user_type
        if _shift_user_type_filter:
            _user_desig = (u.get("designation") or "").strip().lower()
            if _user_desig and _user_desig != _shift_user_type_filter:
                continue

        avail_val    = su.get("availability")

        # For group outreach — resolve avail_val from availability_details per outreach shift_id
        if is_group_outreach and su.get("availability_details"):
            _details_map = {str(ad.get("shift_id","")): ad.get("availability") for ad in su["availability_details"]}
            # Use payload shift_id if provided, else check all entries
            _lookup_shift_id = payload.shift_id or ""
            if _lookup_shift_id and _lookup_shift_id in _details_map:
                avail_val = _details_map[_lookup_shift_id]
            elif not _lookup_shift_id:
                # No shift_id — show overall: Available only if all Yes
                _avails = [v for v in _details_map.values() if v is not None]
                if _avails:
                    if all(a == 1 for a in _avails):
                        avail_val = 1
                    elif 0 in _avails:
                        avail_val = 0
                    # else keep top-level
        raw_oid_su   = su.get("outreach_id")

        # Prior shifts here
        prior_here = 0
        if prior_client_shift_ids_s and su.get("user_id"):
            prior_here = await db["shifts_users"].count_documents({
                "user_id":  su["user_id"],
                "shift_id": {"$in": prior_client_shift_ids_s},
                "availability": 1,
            })

        # Last contacted
        last_contacted = None
        last_su_lc = await db["shifts_users"].find_one(
            {"user_id": su.get("user_id"), "call_processed_at": {"$ne": None}},
            sort=[("call_processed_at", -1)], projection={"call_processed_at": 1}
        )
        if last_su_lc and last_su_lc.get("call_processed_at"):
            from datetime import timezone as _tz2
            lc = last_su_lc["call_processed_at"]
            if hasattr(lc, "tzinfo") and lc.tzinfo is None:
                lc = lc.replace(tzinfo=_tz2.utc)
            diff = int((datetime.now(_tz2.utc) - lc).total_seconds())
            if diff < 60:       last_contacted = "just now"
            elif diff < 3600:   last_contacted = f"{diff//60} minute{'s' if diff//60!=1 else ''} ago"
            elif diff < 86400:  last_contacted = f"{diff//3600} hour{'s' if diff//3600!=1 else ''} ago"
            else:               last_contacted = f"{diff//86400} day{'s' if diff//86400!=1 else ''} ago"

        # Staff tags
        raw_tags = u.get("tags") or []
        staff_tags = [
            {"id": str(t.get("id","")), "name": t.get("name","")} if isinstance(t, dict)
            else {"id": "", "name": str(t)} for t in raw_tags
        ]

        # Visa hours static
        visa_hours_remaining = "8/24"

        # Distance
        distance_km = None
        if client_lat_s is not None and client_lng_s is not None:
            ucoords = _uc_s(u)
            if ucoords:
                distance_km = _hav_s(float(client_lat_s), float(client_lng_s), ucoords[0], ucoords[1])

        # Response + call_details from conversation
        response_text = response_time_s = None
        call_details = None
        conv_s = await db["shift_booking_conv"].find_one(
            {"shift_id": str(shift_oid_for_staff) if shift_oid_for_staff else "", "user_id": uid_str},
            {"turns": 1, "started_at": 1, "ended_at": 1, "elevenlabs_conversation_id": 1,
             "round_number": 1, "phone": 1, "duration_seconds": 1, "confidence": 1}
        )
        if conv_s:
            for turn in reversed(conv_s.get("turns") or []):
                if turn.get("role") in ("user", "human") and turn.get("message"):
                    response_text = turn["message"]
                    ts = turn.get("ts")
                    if ts and hasattr(ts, "strftime"):
                        response_time_s = ts.strftime("%H:%M")
                    break
            started   = conv_s.get("started_at")
            ended_cv  = conv_s.get("ended_at")
            dur_s     = conv_s.get("duration_seconds")
            if not dur_s and started and ended_cv:
                dur_s = int((ended_cv - started).total_seconds())
            placed_time_s  = started.strftime("%H:%M:%S") if started and hasattr(started, "strftime") else None
            round_num_s    = conv_s.get("round_number", 1)
            phone_used_s   = conv_s.get("phone") or u.get("phone")
            ai_heard_s     = None
            confidence_s   = conv_s.get("confidence")
            for turn in reversed(conv_s.get("turns") or []):
                if turn.get("role") in ("user", "human") and turn.get("message"):
                    t_ts = turn.get("ts")
                    t_time_s = t_ts.strftime("%H:%M") if t_ts and hasattr(t_ts, "strftime") else None
                    conf_pct = f"{int(confidence_s * 100)}% confidence" if confidence_s else None
                    parts = [f'"{turn["message"]}"']
                    if t_time_s: parts.append(f"at {t_time_s}")
                    if conf_pct: parts.append(f"· {conf_pct}")
                    ai_heard_s = " ".join(parts)
                    break
            call_details = {
                "called_via": f"{phone_used_s} (phone)" if phone_used_s else "Phone",
                "placed_at":  f"{placed_time_s} · Round {round_num_s}" if placed_time_s else None,
                "duration":   f"{dur_s} seconds" if dur_s else None,
                "ai_heard":   ai_heard_s,
            }

        shifts_users_list.append({
            "id":                  str(su.get("user_id", su["_id"])),
            "user_id":             uid_str,
            "xn_user_id":          u.get("xn_user_id"),
            "name":                " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() or "—",
            "email":               u.get("email"),
            "phone":               u.get("phone"),
            "designation":         u.get("designation"),
            "rating":              u.get("rating"),
            "county":              u.get("county") or county_name_map.get(str(u.get("county_id", ""))) or None,
            "county_id":           str(u["county_id"]) if u.get("county_id") else None,
            "abcd":                0,
            "prior_shifts_here":   prior_here,
            "last_contacted":      last_contacted,
            "staff_tags":          staff_tags,
            "visa_hours_remaining": visa_hours_remaining,
            "channel":             su.get("channel") or "Phone",
            "response_text":       response_text,
            "response_time":       response_time_s,
            "availability":        avail_val,
            "availability_text":   AVAILABILITY_TEXT.get(avail_val, "Unknown"),
            "availability_details": su.get("availability_details") or [],
            "call_enabled":        su.get("call_enabled"),
            "call_processed":      su.get("call_processed"),
            "call_processed_text": "Sent" if su.get("call_processed") == 1 else "Queued",
            "start_time":          _format_call_time(su.get("call_processed_at")) if su.get("call_processed_at") and hasattr(su.get("call_processed_at"), "date") else None,
            "flag":                su.get("flag", 0),
            "ignored":             su.get("ignored", 0),
            "confirmed":           1 if str(uid_str) == shift_staff_id or u.get("email") == shift_staff_email else 0,
            "customer_feedback":   su.get("customer_feedback"),
            "call_status":         su.get("call_status"),
            "call_processed_at":   su["call_processed_at"].isoformat() if su.get("call_processed_at") and hasattr(su["call_processed_at"], "isoformat") else None,
            "assigned_at":         su["assigned_at"].isoformat() if su.get("assigned_at") and hasattr(su["assigned_at"], "isoformat") else None,
            "shift_id":            str(su.get("shift_id", "")) if su.get("shift_id") else None,
            "outreach_id":         str(raw_oid_su) if raw_oid_su else None,
            "conversation_id":     su.get("conversation_id"),
            "distance_km":         distance_km,
            "call_details":        call_details,
            "confirm": {
                "staff_label":       f"{' '.join(filter(None, [u.get('first_name',''), u.get('last_name','')])).strip()} · ★ {u.get('rating') or '—'} · {prior_here} prior shifts here",
                "prior_shifts_here": prior_here,
                "rating":            u.get("rating"),
                "shift":             shift_label_s,
                "placed_at":         placed_at_s,
                "confirmed_by":      None,
            },
        })

    total     = len(shifts_users_list)
    available = sum(1 for s in shifts_users_list if s["availability"] == 1)
    pending   = sum(1 for s in shifts_users_list if s["call_enabled"] == 1 and s["call_processed"] == 0)
    processed = sum(1 for s in shifts_users_list if s["call_processed"] == 1)


    # Apply pagination
    _skip = (payload.page - 1) * payload.per_page
    shifts_users_paginated = shifts_users_list[_skip: _skip + payload.per_page]

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
            "is_group_outreach":    is_group_outreach,
            "wa_phone":             su.get("wa_phone", ""),
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
            "total":     total,
            "page":      payload.page,
            "per_page":  payload.per_page,
            "shifts_users": shifts_users_paginated,
        },
    }


# ── POST /outreach/flag ───────────────────────────────────────────────────────

class FlagStaffRequest(BaseModel):
    outreach_id: str
    staff_id:    str             # users._id (shifts_users.user_id)
    reason:      Optional[str] = None   # e.g. "actually_declined"
    notes:       Optional[str] = None   # optional AI training notes


# ── Flag reasons list ─────────────────────────────────────────────────────────

FLAG_REASONS = [
    {
        "id":          "actually_declined",
        "title":       "Actually Declined",
        "description": "They said no, even if it sounded ambiguous",
    },
    {
        "id":          "unclear_follow_up",
        "title":       "Unclear · needs follow-up",
        "description": "Response was ambiguous; ops should call back",
    },
    {
        "id":          "available_with_conditions",
        "title":       "Available but with conditions",
        "description": 'e.g. "yes if I can leave early"',
    },
]


@router.get(
    "/flag-reasons",
    summary="Get list of flag reasons",
    dependencies=[Depends(verify_api_key)],
)
async def get_flag_reasons(request: Request):
    return {"success": True, "data": FLAG_REASONS}


@router.post(
    "/flag",
    summary="Flag or unflag a staff member in shifts_users for a specific outreach",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def flag_staff(request: Request, payload: FlagStaffRequest):
    """
    Body: { "outreach_id": "...", "staff_id": "...", "reason": "...", "notes": "..." }
    Looks up shifts_users by outreach_id + user_id, sets flag=1 with reason and notes.
    """
    db = _get_db()

    if not ObjectId.is_valid(payload.outreach_id):
        raise HTTPException(status_code=422, detail="Invalid outreach_id")
    if not ObjectId.is_valid(payload.staff_id):
        raise HTTPException(status_code=422, detail="Invalid staff_id")

    outreach_oid = ObjectId(payload.outreach_id)
    user_oid     = ObjectId(payload.staff_id)

    su = await db["shifts_users"].find_one({
        "outreach_id": outreach_oid,
        "user_id":     user_oid,
    }, {"_id": 1})
    if not su:
        raise HTTPException(status_code=404, detail="shifts_users record not found for this outreach and staff")

    now = datetime.now(timezone.utc)
    update_fields = {
        "flag":            1,
        "flag_reason":     payload.reason,
        "flag_notes":      payload.notes,
        "flag_staff_id":   payload.staff_id,
        "updated_at":      now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
    }

    # Resolve reason label
    reason_label = next(
        (r["title"] for r in FLAG_REASONS if r["id"] == payload.reason), payload.reason
    )
    update_fields["flag_reason_text"] = reason_label

    await db["shifts_users"].update_one({"_id": su["_id"]}, {"$set": update_fields})

    return {
        "success":     True,
        "message":     "Flagged",
        "outreach_id": payload.outreach_id,
        "staff_id":    payload.staff_id,
        "reason":      payload.reason,
        "reason_text": reason_label,
        "notes":       payload.notes,
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
    shift_id:        str
    user_id:         str
    outreach_id:     Optional[str] = None
    conversation_id: Optional[str] = None   # shifts_users.conversation_id — use directly if provided


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

    query: dict = {"shift_id": payload.shift_id, "user_id": payload.user_id}
    if payload.outreach_id:
        query["outreach_id"] = payload.outreach_id

    # If conversation_id provided, look up directly by elevenlabs_conversation_id
    if payload.conversation_id:
        query = {"elevenlabs_conversation_id": payload.conversation_id}

    conv = await db["shift_booking_conv"].find_one(query)
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
            "outreach_id":                 str(conv["outreach_id"]) if conv.get("outreach_id") else None,
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

    audio_query: dict = {"shift_id": payload.shift_id, "user_id": payload.user_id}
    if payload.outreach_id:
        audio_query["outreach_id"] = payload.outreach_id

    # If conversation_id provided use directly
    if payload.conversation_id:
        audio_query = {"elevenlabs_conversation_id": payload.conversation_id}

    conv = await db["shift_booking_conv"].find_one(
        audio_query,
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


# ── POST /outreach/confirm-transcription ─────────────────────────────────────

class ConfirmTranscriptionRequest(BaseModel):
    conversation_id: str   # requested_confirm.elevenlabs_conversation_id


@router.post(
    "/confirm-transcription",
    summary="Get transcription for a confirm call",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("60/minute")
async def get_confirm_transcription(request: Request, payload: ConfirmTranscriptionRequest):
    db = _get_db()

    # Find in requested_confirm_call_conversations by elevenlabs_conversation_id
    conv = await db["requested_confirm_call_conversations"].find_one(
        {"elevenlabs_conversation_id": payload.conversation_id}
    )
    if not conv:
        raise HTTPException(status_code=404, detail="No conversation found for this conversation_id")

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
            "id":                         str(conv["_id"]),
            "conversation_id":            conv.get("elevenlabs_conversation_id"),
            "shift_id":                   conv.get("shift_id"),
            "shift_code":                 conv.get("shift_code"),
            "shift_date":                 conv.get("shift_date"),
            "start_time":                 conv.get("start_time"),
            "end_time":                   conv.get("end_time"),
            "client_name":                conv.get("client_name"),
            "location":                   conv.get("location"),
            "user_type":                  conv.get("user_type"),
            "name":                       conv.get("name"),
            "phone":                      conv.get("phone"),
            "designation":                conv.get("designation"),
            "call_sid":                   conv.get("call_sid"),
            "started_at":                 _fmt(conv.get("started_at")),
            "ended_at":                   _fmt(conv.get("ended_at")),
            "turns":                      turns,
            "has_audio":                  bool(conv.get("elevenlabs_conversation_id")),
        },
    }


# ── POST /outreach/confirm-transcription/audio ───────────────────────────────

@router.post(
    "/confirm-transcription/audio",
    summary="Get audio for a confirm call",
    dependencies=[Depends(verify_api_key)],
)
@limiter.limit("30/minute")
async def get_confirm_transcription_audio(request: Request, payload: ConfirmTranscriptionRequest):
    import os
    from fastapi.responses import StreamingResponse

    el_conv_id = payload.conversation_id
    if not el_conv_id:
        raise HTTPException(status_code=400, detail="conversation_id is required")

    # Verify conversation exists
    conv = await _get_db()["requested_confirm_call_conversations"].find_one(
        {"elevenlabs_conversation_id": el_conv_id}, {"_id": 1}
    )
    if not conv:
        raise HTTPException(status_code=404, detail="No conversation found for this conversation_id")

    api_key = settings.ELEVENLABS_API_KEY or ""
    url     = f"https://api.elevenlabs.io/v1/convai/conversations/{el_conv_id}/audio"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers={"xi-api-key": api_key})

    if resp.status_code != 200:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Failed to fetch audio: {resp.text[:200]}"
        )

    return StreamingResponse(
        iter([resp.content]),
        media_type="audio/mpeg",
        headers={"Content-Disposition": f"attachment; filename=confirm_{el_conv_id}.mp3"}
    )


# ── GET /outreach/email-list ──────────────────────────────────────────────────

@router.get(
    "/email-list",
    summary="List email outreach records with response status",
    dependencies=[Depends(verify_api_key)],
)
async def email_list(
    request: Request,
    shift_id:    Optional[str] = None,
    outreach_id: Optional[str] = None,
    page:        int = 1,
    per_page:    int = 20,
):
    db   = _get_db()
    skip = (page - 1) * per_page

    q: dict = {"channel": "Email"}
    if shift_id and ObjectId.is_valid(shift_id):
        q["shift_id"] = ObjectId(shift_id)
    if outreach_id and ObjectId.is_valid(outreach_id):
        q["outreach_id"] = ObjectId(outreach_id)

    total = await db["shifts_users"].count_documents(q)
    docs  = await db["shifts_users"].find(q).sort("assigned_at", -1).skip(skip).limit(per_page).to_list(per_page)

    # Build user map
    user_oids = [d["user_id"] for d in docs if d.get("user_id")]
    user_map: dict = {}
    async for u in db["users"].find({"_id": {"$in": user_oids}}, {"first_name": 1, "last_name": 1, "email": 1}):
        user_map[str(u["_id"])] = u

    AVAIL = {0: "Not Available", 1: "Available", 3: "Voicemail", 4: "Call Not Attended",
             5: "In Call", 6: "Call Not Triggered", 7: "Not Sent"}

    def _iso(v): return v.isoformat() if v and hasattr(v, "isoformat") else None

    results = []
    for d in docs:
        u   = user_map.get(str(d.get("user_id", "")), {})
        av  = d.get("availability")
        results.append({
            "id":               str(d["_id"]),
            "shift_id":         str(d.get("shift_id", "")),
            "outreach_id":      str(d.get("outreach_id", "")) if d.get("outreach_id") else None,
            "user_id":          str(d.get("user_id", "")),
            "name":             f"{u.get('first_name','')} {u.get('last_name','')}".strip(),
            "email":            u.get("email"),
            "channel":          d.get("channel", "Email"),
            "availability":     av,
            "availability_text": AVAIL.get(av, "Unknown"),
            "call_processed":   d.get("call_processed", 0),
            "email_sent":       d.get("email_sent", 0),
            "email_sent_at":    _iso(d.get("email_sent_at")),
            "response_text":    d.get("response_text"),
            "responded_at":     _iso(d.get("responded_at")),
            "assigned_at":      _iso(d.get("assigned_at")),
        })

    return {"success": True, "total": total, "page": page, "per_page": per_page, "data": results}


# ── GET /outreach/email-responses ─────────────────────────────────────────────

@router.get(
    "/email-responses",
    summary="List email responses (users who replied Yes/No)",
    dependencies=[Depends(verify_api_key)],
)
async def email_responses(
    request: Request,
    shift_id:    Optional[str] = None,
    outreach_id: Optional[str] = None,
    answer:      Optional[str] = None,  # yes / no
    page:        int = 1,
    per_page:    int = 20,
):
    db   = _get_db()
    skip = (page - 1) * per_page

    q: dict = {"channel": "Email", "responded_at": {"$exists": True}}
    if shift_id and ObjectId.is_valid(shift_id):
        q["shift_id"] = ObjectId(shift_id)
    if outreach_id and ObjectId.is_valid(outreach_id):
        q["outreach_id"] = ObjectId(outreach_id)
    if answer == "yes":
        q["availability"] = 1
    elif answer == "no":
        q["availability"] = 0

    total = await db["shifts_users"].count_documents(q)
    docs  = await db["shifts_users"].find(q).sort("responded_at", -1).skip(skip).limit(per_page).to_list(per_page)

    user_oids = [d["user_id"] for d in docs if d.get("user_id")]
    user_map: dict = {}
    async for u in db["users"].find({"_id": {"$in": user_oids}}, {"first_name": 1, "last_name": 1, "email": 1}):
        user_map[str(u["_id"])] = u

    def _iso(v): return v.isoformat() if v and hasattr(v, "isoformat") else None

    results = []
    for d in docs:
        u  = user_map.get(str(d.get("user_id", "")), {})
        av = d.get("availability")
        results.append({
            "id":            str(d["_id"]),
            "shift_id":      str(d.get("shift_id", "")),
            "outreach_id":   str(d.get("outreach_id", "")) if d.get("outreach_id") else None,
            "user_id":       str(d.get("user_id", "")),
            "name":          f"{u.get('first_name','')} {u.get('last_name','')}".strip(),
            "email":         u.get("email"),
            "response":      "yes" if av == 1 else "no" if av == 0 else "unknown",
            "response_text": d.get("response_text"),
            "responded_at":  _iso(d.get("responded_at")),
            "email_sent_at": _iso(d.get("email_sent_at")),
        })

    return {"success": True, "total": total, "page": page, "per_page": per_page, "data": results}


# ── GET /outreach/email-detail ────────────────────────────────────────────────

class EmailDetailRequest(BaseModel):
    user_id:     Optional[str] = None
    shift_id:    Optional[str] = None
    outreach_id: Optional[str] = None


@router.post(
    "/email-detail",
    summary="Get sent email content and user response by user_id/shift_id/outreach_id",
    dependencies=[Depends(verify_api_key)],
)
async def email_detail(request: Request, payload: EmailDetailRequest):
    user_id     = payload.user_id
    shift_id    = payload.shift_id
    outreach_id = payload.outreach_id
    db = _get_db()

    q: dict = {"channel": "Email"}
    if user_id and ObjectId.is_valid(user_id):
        q["user_id"] = ObjectId(user_id)
    if outreach_id and ObjectId.is_valid(outreach_id):
        q["outreach_id"] = ObjectId(outreach_id)

    if not q or q == {"channel": "Email"}:
        raise HTTPException(status_code=422, detail="Provide at least one of: user_id, shift_id, outreach_id")

    # For regular outreach — also filter by shift_id
    q_regular = dict(q)
    if shift_id and ObjectId.is_valid(shift_id):
        q_regular["shift_id"] = ObjectId(shift_id)

    su = await db["shifts_users"].find_one(q_regular, sort=[("assigned_at", -1)])
    is_group = False
    if not su:
        # Try shifts_group_users — no shift_id filter (group users don't have shift_id)
        su = await db["shifts_group_users"].find_one(q, sort=[("assigned_at", -1)])
        if su:
            is_group = True
    if not su:
        raise HTTPException(status_code=404, detail="No email record found")

    # Get user details
    u = {}
    if su.get("user_id"):
        u = await db["users"].find_one({"_id": su["user_id"]}, {"first_name": 1, "last_name": 1, "email": 1}) or {}

    # Get shift details — for group outreach fetch all shifts in group
    s = {}
    all_shifts = []
    if is_group and su.get("group_id"):
        sg = await db["shifts_group"].find_one({"_id": su["group_id"]}, {"shift_ids": 1})
        if sg and sg.get("shift_ids"):
            async for sh in db["shifts"].find(
                {"_id": {"$in": sg["shift_ids"]}},
                {"shift_code": 1, "date": 1, "start_time": 1, "end_time": 1,
                 "client_name": 1, "location": 1, "user_type": 1, "unit": 1,
                 "shift_timing": 1, "client_county": 1, "rate": 1}
            ):
                all_shifts.append(sh)
            if all_shifts:
                s = all_shifts[0]
    elif su.get("shift_id"):
        s = await db["shifts"].find_one({"_id": su["shift_id"]},
            {"shift_code": 1, "date": 1, "start_time": 1, "end_time": 1,
             "client_name": 1, "location": 1, "user_type": 1, "unit": 1,
             "shift_timing": 1, "client_county": 1, "rate": 1}) or {}
        all_shifts = [s] if s else []

    AVAIL = {0: "Not Available", 1: "Available", 3: "Voicemail",
             4: "Call Not Attended", 6: "Call Not Triggered", 7: "Not Sent"}

    try:
        from zoneinfo import ZoneInfo as _ZI2
        _irl = _ZI2("Europe/Dublin")
    except Exception:
        from datetime import timezone as _tz2
        _irl = _tz2.utc
    def _iso(v):
        if not v or not hasattr(v, "isoformat"):
            return None
        if hasattr(v, "tzinfo") and v.tzinfo is None:
            from datetime import timezone
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(_irl).isoformat()

    av = su.get("availability")

    # For group outreach — resolve av from availability_details
    if is_group:
        details = su.get("availability_details") or []
        if shift_id:
            # Specific shift — get av for that shift, fallback to top-level
            for ad in details:
                if str(ad.get("shift_id","")) == shift_id:
                    av = ad.get("availability", av)
                    break
            # Filter all_shifts to just this shift
            if all_shifts:
                filtered = [sh for sh in all_shifts if str(sh.get("_id","")) == shift_id]
                if filtered:
                    all_shifts = filtered
                    s = filtered[0]
        else:
            # No specific shift — use first availability_details entry if exists, else top-level
            if details:
                av = details[0].get("availability", av)

    # Reconstruct sent email content
    base_url   = "https://uat.expresshealth.ie"
    first_name = u.get("first_name", "")
    su_id      = str(su["_id"])
    shift_date = str(s.get("date", ""))
    try:
        from datetime import datetime as _dt
        formatted_date = _dt.strptime(shift_date.split(" ")[0], "%Y-%m-%d").strftime("%A, %d %B %Y")
    except Exception:
        formatted_date = shift_date

    email_status  = su.get("email_status", "")
    email_opened  = su.get("email_opened", 0)
    email_clicked = su.get("email_clicked", 0)
    email_bounced = su.get("email_bounced", 0)

    STATUS_ICON = {
        "delivered":   "📬 Delivered",
        "opened":      "👁️ Opened",
        "clicked":     "🖱️ Clicked",
        "soft_bounce": "⚠️ Soft Bounce",
        "hard_bounce": "❌ Hard Bounce",
        "unsubscribed":"🚫 Unsubscribed",
    }
    status_label = STATUS_ICON.get(email_status, "📤 Sent" if su.get("email_sent") else "⏳ Pending")
    name      = f"{u.get('first_name','')} {u.get('last_name','')}".strip()
    email_to  = u.get("email", "")
    sent_at   = _iso(su.get("email_sent_at")) or _iso(su.get("assigned_at")) or ""
    responded = _iso(su.get("responded_at")) or ""
    response_text = su.get("response_text", "")
    # For group per-shift — override response_text from availability_details
    if is_group and shift_id:
        for ad in (su.get("availability_details") or []):
            if str(ad.get("shift_id","")) == shift_id:
                if ad.get("availability") == 1:
                    response_text = "Yes, I'm available."
                elif ad.get("availability") == 0:
                    response_text = "No, thanks."
                break
    elif is_group and not shift_id:
        # Multi-shift group — don't show single response_text, use per-shift bubbles
        response_text = ""
    # Build availability_details map for quick lookup
    avail_details_map = {}
    for ad in (su.get("availability_details") or []):
        avail_details_map[str(ad.get("shift_id", ""))] = ad

    answer_label = "✅ Yes, I'm available" if av == 1 else "❌ No, thanks" if av == 0 else None

    # Sent email bubble content
    def _shift_row_html(sh, fd, shift_avail=None):
        _avail_badge = ""
        if shift_avail == 1:
            _avail_badge = '<span style="background:#dcfce7;color:#166534;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;margin-left:6px;">✅ Available</span>'
        elif shift_avail == 0:
            _avail_badge = '<span style="background:#fee2e2;color:#991b1b;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:600;margin-left:6px;">❌ Not Available</span>'
        elif shift_avail == 8:
            _avail_badge = '<span style="background:#f3f4f6;color:#6b7280;padding:2px 8px;border-radius:10px;font-size:11px;margin-left:6px;">⏳ No Response</span>'
        return f"""
          <tr><td style="padding:6px 12px;color:#6b7280;width:40%">📍 Facility</td><td style="padding:6px 12px;font-weight:600;color:#111827;">{sh.get('client_name','') or sh.get('location','')}{_avail_badge}</td></tr>
          <tr><td style="padding:6px 12px;color:#6b7280">👩\u200d⚕️ Role</td><td style="padding:6px 12px;font-weight:600;color:#111827;">{sh.get('user_type','')}</td></tr>
          <tr><td style="padding:6px 12px;color:#6b7280">📅 Date</td><td style="padding:6px 12px;font-weight:600;color:#111827;">{fd}</td></tr>
          <tr><td style="padding:6px 12px;color:#6b7280">🕐 Time</td><td style="padding:6px 12px;font-weight:600;color:#111827;">{sh.get('start_time','')} – {sh.get('end_time','')}</td></tr>"""

    if is_group and len(all_shifts) > 1:
        shift_rows_html = ""
        for i, sh in enumerate(all_shifts):
            _fd = formatted_date
            try:
                from datetime import datetime as _dtt2
                _fd = _dtt2.strptime(str(sh.get("date","")).split("T")[0], "%Y-%m-%d").strftime("%A, %d %B %Y")
            except Exception:
                pass
            _sh_avail = avail_details_map.get(str(sh.get("_id","")), {}).get("availability")
            shift_rows_html += f'<tr><td colspan="2" style="padding:6px 12px;background:#f4f3ef;font-size:11px;font-weight:700;color:#27237c;">SHIFT {i+1} OF {len(all_shifts)}</td></tr>'
            shift_rows_html += _shift_row_html(sh, _fd, _sh_avail)
        intro_text = f"We're reaching out to check your availability for the following <strong>{len(all_shifts)} shifts</strong>."
        avail_text = "Are you available for these shifts?"
    else:
        shift_rows_html = _shift_row_html(s, formatted_date, av) if s else ""
        intro_text = f"We're reaching out to check your availability for an upcoming shift at <strong>{s.get('client_name','')}</strong>."
        avail_text = "Are you available for this shift?"

    email_bubble = f"""
      <div style="font-size:13px;line-height:1.6;color:#374151;">
        <p style="margin:0 0 10px;">Hi <strong>{first_name}</strong>,</p>
        <p style="margin:0 0 10px;">{intro_text}</p>
        <table style="width:100%;background:#f3f4f6;border-radius:6px;border:1px solid #e5e7eb;font-size:12px;margin:10px 0;border-collapse:collapse;">
          <tr><td style="padding:6px 12px;color:#6b7280;width:40%">👤 Staff</td><td style="padding:6px 12px;font-weight:600;color:#111827;">{name}</td></tr>
          {shift_rows_html}
        </table>
        <p style="margin:10px 0 6px;font-weight:600;text-align:center;">{avail_text}</p>
        <div style="text-align:center;">
          <span style="display:inline-block;background:#1e7a38;color:#fff;padding:6px 14px;border-radius:4px;font-size:12px;margin:2px;">✅ Yes, I'm available</span>
          <span style="display:inline-block;background:#dc2626;color:#fff;padding:6px 14px;border-radius:4px;font-size:12px;margin:2px;">❌ No, thanks</span>
        </div>
      </div>"""

    email_reply      = su.get("email_reply", "")
    email_reply_from = su.get("email_reply_from", "")
    email_reply_at   = _iso(su.get("email_reply_at")) or ""
    staff_comment    = su.get("staff_comment", "")
    comment_at       = _iso(su.get("comment_at")) or ""

    response_bubble = ""
    if is_group and len(all_shifts) > 1 and avail_details_map:
        # Show per-shift response bubbles
        for sh in all_shifts:
            sh_id = str(sh.get("_id", ""))
            ad    = avail_details_map.get(sh_id)
            if not ad:
                continue
            _av   = ad.get("availability")
            _time = ad.get("responded_at", "")
            _label = "✅ Yes, I'm available" if _av == 1 else "❌ No, thanks" if _av == 0 else None
            if _label:
                _facility = sh.get("client_name", "") or sh.get("location", "")
                _color    = "#1e7a38" if _av == 1 else "#dc2626"
                response_bubble += f"""
    <div style="display:flex;justify-content:flex-end;margin-bottom:12px;">
      <div>
        <div style="font-size:11px;color:#9ca3af;text-align:right;margin-bottom:4px;">{_facility}</div>
        <div style="background:{_color};color:#fff;border-radius:18px 18px 4px 18px;padding:10px 16px;max-width:340px;font-size:13px;font-weight:600;">
          {_label}
        </div>
        <div style="text-align:right;font-size:11px;color:#9ca3af;margin-top:3px;">{name} · {str(_time)[:16]}</div>
      </div>
      <div style="width:36px;height:36px;border-radius:50%;background:{_color};color:#fff;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;margin-left:8px;flex-shrink:0;">
        {(name[0] if name else 'U').upper()}
      </div>
    </div>"""
    elif response_text or email_reply:
        display_text = answer_label or response_text or email_reply
        display_time = responded or email_reply_at
        reply_label  = "📧 Replied via email" if email_reply and not response_text else ""
        response_bubble = f"""
    <!-- Response bubble -->
    <div style="display:flex;justify-content:flex-end;margin-bottom:20px;">
      <div>
        {"<div style='font-size:11px;color:#9ca3af;text-align:right;margin-bottom:4px;'>"+reply_label+"</div>" if reply_label else ""}
        <div style="background:#1e7a38;color:#fff;border-radius:18px 18px 4px 18px;padding:12px 18px;max-width:340px;font-size:14px;font-weight:600;white-space:pre-wrap;">
          {display_text}
        </div>
        <div style="text-align:right;font-size:11px;color:#9ca3af;margin-top:4px;">{name} · {display_time.replace("T", " ")[:16] if display_time else ''}</div>
      </div>
      <div style="width:36px;height:36px;border-radius:50%;background:#1e7a38;color:#fff;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;margin-left:8px;flex-shrink:0;">
        {(name[0] if name else 'U').upper()}
      </div>
    </div>"""

    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Email Chat – {name}</title>
</head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Inter,'Helvetica Neue',Arial,sans-serif;">

<div style="max-width:600px;margin:0 auto;">

  <!-- Header -->
  <div style="background:#1e7a38;color:#fff;padding:16px 20px;display:flex;align-items:center;gap:12px;">
    <div style="width:40px;height:40px;border-radius:50%;background:rgba(255,255,255,0.2);display:flex;align-items:center;justify-content:center;font-weight:700;font-size:16px;">{(name[0] if name else 'U').upper()}</div>
    <div>
      <div style="font-weight:700;font-size:15px;">{name}</div>
      <div style="font-size:12px;opacity:0.85;">{email_to}</div>
    </div>
    <div style="margin-left:auto;font-size:12px;opacity:0.8;background:{'rgba(255,255,255,0.2)' if av==1 else 'rgba(220,38,38,0.5)' if av==0 else 'rgba(255,255,255,0.1)'};padding:4px 10px;border-radius:12px;">
      {AVAIL.get(av, 'Pending')}
    </div>
  </div>

  <!-- Chat area -->
  <div style="padding:20px;min-height:300px;background:#f9fafb;">

    <!-- Sent email bubble -->
    <div style="display:flex;margin-bottom:20px;gap:10px;">
      <div style="width:36px;height:36px;border-radius:50%;background:#e5e7eb;color:#374151;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:13px;flex-shrink:0;">XH</div>
      <div style="flex:1;">
        <div style="font-size:11px;color:#9ca3af;margin-bottom:6px;">Xpress Health · {sent_at.replace("T", " ")[:16] if sent_at else 'Sent'}</div>
        <div style="background:#fff;border:1px solid #e5e7eb;border-radius:4px 18px 18px 18px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,0.05);">
          {email_bubble}
        </div>
      </div>
    </div>

    {response_bubble if response_bubble else '''
    <div style="text-align:center;color:#9ca3af;font-size:13px;padding:20px;">
      No response yet
    </div>'''}

    {f"""
    <div style="display:flex;justify-content:flex-end;margin-bottom:20px;">
      <div>
        <div style="font-size:11px;color:#9ca3af;text-align:right;margin-bottom:4px;">&#x1F4AC; Staff Comment</div>
        <div style="background:#f59e0b;color:#fff;border-radius:18px 18px 4px 18px;padding:12px 18px;max-width:340px;font-size:14px;white-space:pre-wrap;">{staff_comment}</div>
        <div style="text-align:right;font-size:11px;color:#9ca3af;margin-top:4px;">{name} · {comment_at.replace("T", " ")[:16] if comment_at else ''}</div>
      </div>
      <div style="width:36px;height:36px;border-radius:50%;background:#f59e0b;color:#fff;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:14px;margin-left:8px;flex-shrink:0;">{(name[0] if name else 'U').upper()}</div>
    </div>""" if staff_comment else ""}

  </div>

  <!-- Footer -->
  <div style="background:#fff;border-top:1px solid #e5e7eb;padding:12px 20px;font-size:12px;color:#6b7280;text-align:center;display:flex;justify-content:center;gap:16px;flex-wrap:wrap;">
    <span>{status_label}</span>
    {'<span>👁️ Opened</span>' if email_opened else ''}
    {'<span>🖱️ Clicked</span>' if email_clicked else ''}
    {'<span>⚠️ Bounced</span>' if email_bounced else ''}
    <span>🔖 {s.get('shift_code','')}</span>
  </div>

</div>
</body>
</html>"""

    from fastapi.responses import HTMLResponse as _HR
    return _HR(content=html)


# ── POST /outreach/brevo-webhook ──────────────────────────────────────────────

@router.post(
    "/brevo-webhook",
    summary="Brevo email event webhook (open, click, bounce etc)",
    include_in_schema=False,
)
async def brevo_webhook(request: Request):
    """
    Receives email events from Brevo (Sendinblue).
    Configure in Brevo → Settings → Webhooks → URL: /xnapi/outreach/brevo-webhook
    Events: delivered, opened, clicked, soft_bounce, hard_bounce, unsubscribed
    """
    db  = _get_db()
    now = datetime.now(timezone.utc)

    try:
        body = await request.json()
    except Exception:
        body = {}

    # Brevo sends array or single event
    events = body if isinstance(body, list) else [body]

    for event in events:
        event_type = event.get("event", "")
        email      = event.get("email", "")
        # X-Shift-Id header we set when sending
        shifts_users_id = (
            event.get("X-Shift-Id") or
            event.get("x-shift-id") or
            (event.get("headers") or {}).get("X-Shift-Id")
        )

        logger.info(f"[BREVO] event={event_type} email={email} su_id={shifts_users_id}")

        if not shifts_users_id or not ObjectId.is_valid(shifts_users_id):
            # Try to find by email
            if email:
                su = await db["shifts_users"].find_one(
                    {"email_sent": 1, "email_status": {"$ne": "hard_bounce"}},
                    sort=[("email_sent_at", -1)]
                )
                if su:
                    shifts_users_id = str(su["_id"])

        if not shifts_users_id or not ObjectId.is_valid(shifts_users_id):
            continue

        update: dict = {"email_status": event_type, "updated_at": now}

        if event_type == "opened":
            update["email_opened"]    = 1
            update["email_opened_at"] = now
        elif event_type == "clicked":
            update["email_clicked"]    = 1
            update["email_clicked_at"] = now
        elif event_type in ("soft_bounce", "hard_bounce"):
            update["email_bounced"]    = 1
            update["email_bounce_type"] = event_type
        elif event_type == "delivered":
            update["email_delivered"] = 1

        await db["shifts_users"].update_one(
            {"_id": ObjectId(shifts_users_id)},
            {"$set": update}
        )

    return {"success": True, "processed": len(events)}


# ── POST /outreach/brevo-inbound ─────────────────────────────────────────────

@router.post(
    "/brevo-inbound",
    summary="Brevo inbound email reply webhook",
    include_in_schema=False,
)
async def brevo_inbound(request: Request):
    """
    Receives inbound email replies from Brevo.
    Set Reply-To: reply+{shifts_users_id}@yourdomain.com when sending.
    Configure in Brevo → Inbound Parsing → Webhook URL: /xnapi/outreach/brevo-inbound
    """
    db  = _get_db()
    now = datetime.now(timezone.utc)

    try:
        # Brevo sends multipart form data for inbound
        form = await request.form()
        payload = {}
        for k in form:
            payload[k] = form[k]
    except Exception:
        try:
            payload = await request.json()
        except Exception:
            payload = {}

    logger.info(f"[BREVO INBOUND] payload keys: {list(payload.keys())}")

    # Extract reply-to address to get shifts_users_id
    to_full    = payload.get("To", "") or payload.get("to", "")
    from_email = payload.get("From", "") or payload.get("from", "")
    subject    = payload.get("Subject", "") or payload.get("subject", "")
    text_body  = payload.get("TextBody", "") or payload.get("text", "") or payload.get("RawTextBody", "")
    html_body  = payload.get("HtmlBody", "") or payload.get("html", "")
    reply_text = text_body.strip() or html_body.strip()

    # Parse shifts_users_id from reply+ address e.g. reply+6a7db363@domain.com
    shifts_users_id = None
    import re as _re
    match = _re.search(r'reply\+([a-f0-9]{24})', to_full, _re.IGNORECASE)
    if match:
        shifts_users_id = match.group(1)

    # Save raw inbound log
    await db["email_inbound_logs"].insert_one({
        "to":              to_full,
        "from":            from_email,
        "subject":         subject,
        "body":            reply_text[:2000],
        "shifts_users_id": shifts_users_id,
        "received_at":     now,
        "raw":             {k: str(v)[:500] for k, v in payload.items()},
    })

    if shifts_users_id and ObjectId.is_valid(shifts_users_id):
        await db["shifts_users"].update_one(
            {"_id": ObjectId(shifts_users_id)},
            {"$set": {
                "email_reply":      reply_text[:2000],
                "email_reply_from": from_email,
                "email_reply_at":   now,
                "updated_at":       now,
            }}
        )
        logger.info(f"[BREVO INBOUND] ✓ Saved reply for su_id={shifts_users_id} from={from_email}")
    else:
        logger.warning(f"[BREVO INBOUND] Could not find shifts_users_id in To: {to_full}")

    return {"success": True}


# ── POST /outreach/whatsapp-detail ────────────────────────────────────────────

class WhatsAppDetailRequest(BaseModel):
    phone:       Optional[str] = None
    user_id:     Optional[str] = None
    shift_id:    Optional[str] = None
    outreach_id: Optional[str] = None
    page:        int = 1
    per_page:    int = 20

@router.post(
    "/whatsapp-detail",
    summary="Get WhatsApp chat view for a staff member",
    dependencies=[Depends(verify_api_key)],
)
async def whatsapp_detail(request: Request, payload: WhatsAppDetailRequest):
    from fastapi.responses import HTMLResponse as _HR2
    db  = _get_db()

    # Find by phone (most direct) or other filters
    su = None
    if payload.phone:
        _phone_clean = payload.phone.replace("+", "").replace(" ", "").replace("-", "").strip()
        su = await db["shifts_users"].find_one(
            {"wa_phone": _phone_clean},
            sort=[("wa_sent_at", -1)]
        )
        if not su:
            su = await db["shifts_group_users"].find_one(
                {"wa_phone": _phone_clean},
                sort=[("wa_sent_at", -1)]
            )

    if not su:
        q: dict = {"channel": "WhatsApp"}
        if payload.user_id and ObjectId.is_valid(payload.user_id):
            q["user_id"] = ObjectId(payload.user_id)
        if payload.shift_id and ObjectId.is_valid(payload.shift_id):
            q["shift_id"] = ObjectId(payload.shift_id)
        if payload.outreach_id and ObjectId.is_valid(payload.outreach_id):
            q["outreach_id"] = ObjectId(payload.outreach_id)

        if len(q) > 1:
            su = await db["shifts_users"].find_one(q, sort=[("assigned_at", -1)])
            if not su:
                su = await db["shifts_group_users"].find_one(q, sort=[("assigned_at", -1)])

    if not su:
        raise HTTPException(status_code=404, detail="No WhatsApp record found")

    # User info
    u = {}
    if su.get("user_id"):
        u = await db["users"].find_one({"_id": su["user_id"]}, {"first_name": 1, "last_name": 1, "phone": 1}) or {}
    name  = f"{u.get('first_name','')} {u.get('last_name','')}".strip()
    phone = u.get("phone", "")

    # Shift info
    s = {}
    if su.get("shift_id"):
        s = await db["shifts"].find_one({"_id": su["shift_id"]},
            {"shift_code": 1, "date": 1, "start_time": 1, "end_time": 1,
             "client_name": 1, "user_type": 1, "client_county": 1}) or {}

    # WATI messages — search by wa_phone or user_id
    _wa_phone = su.get("wa_phone") or (payload.phone.replace("+","").replace(" ","").strip() if payload.phone else "")
    _skip = (payload.page - 1) * payload.per_page
    _total_msgs = await db["wati_messages"].count_documents(
        {"$or": [{"phone": _wa_phone}, {"user_id": su.get("user_id")}]} if _wa_phone else {"user_id": su.get("user_id")}
    )
    wati_msgs = await db["wati_messages"].find(
        {"$or": [
            {"phone": _wa_phone},
            {"user_id": su.get("user_id")},
        ]} if _wa_phone else {"user_id": su.get("user_id")},
        sort=[("timestamp", -1)]
    ).skip(_skip).limit(payload.per_page).to_list(length=payload.per_page)

    try:
        from zoneinfo import ZoneInfo as _ZI3
        _irl3 = _ZI3("Europe/Dublin")
    except Exception:
        from datetime import timezone as _tz3
        _irl3 = _tz3.utc

    def _fmt_time(v):
        if not v: return ""
        if hasattr(v, "tzinfo") and v.tzinfo is None:
            from datetime import timezone
            v = v.replace(tzinfo=timezone.utc)
        v_local = v.astimezone(_irl3)
        now_local = datetime.now(_irl3)
        time_str = v_local.strftime("%H:%M")
        diff_days = (now_local.date() - v_local.date()).days
        if diff_days == 0:
            return f"Today {time_str}"
        elif diff_days == 1:
            return f"Yesterday {time_str}"
        else:
            return v_local.strftime("%-d %b %H:%M")

    def _fmt_date(v):
        if not v: return ""
        if hasattr(v, "tzinfo") and v.tzinfo is None:
            from datetime import timezone
            v = v.replace(tzinfo=timezone.utc)
        return v.astimezone(_irl3).strftime("%d %b %Y")

    AVAIL = {0:"Not Available",1:"Available",7:"Not Sent",8:"No Response"}
    av    = su.get("availability", 7)

        # Build chat bubbles — collect all as (timestamp, html) then sort
    _all_bubbles = []

    # Sent template message bubble
    wa_sent_at = su.get("wa_sent_at")
    if su.get("wa_sent") and wa_sent_at:
        _shift_text = ""
        if s:
            _shift_text = f"<br><small style='color:#aaa;'>{s.get('client_name','') or s.get('location','')} · {s.get('user_type','')} · {s.get('start_time','')}–{s.get('end_time','')}</small>"
        _tmpl_html = f"""
    <div style="display:flex;gap:8px;margin-bottom:16px;">
      <div style="width:32px;height:32px;border-radius:50%;background:#25D366;color:#fff;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;flex-shrink:0;">XH</div>
      <div>
        <div style="font-size:10px;color:#aaa;margin-bottom:4px;">Xpress Health · {_fmt_time(wa_sent_at)}</div>
        <div style="background:#fff;border:1px solid #e5e7eb;border-radius:4px 18px 18px 18px;padding:12px 16px;max-width:320px;font-size:14px;line-height:1.5;color:#111;">
          Hi <strong>{u.get('first_name','')}</strong> 👋<br>
          We're checking your availability for the following shift:{_shift_text}<br><br>
          <em>Are you available for this shift?</em>
        </div>
        <div style="display:flex;gap:6px;margin-top:6px;">
          <span style="background:#f0fdf4;border:1px solid #bbf7d0;color:#166534;padding:5px 14px;border-radius:20px;font-size:12px;font-weight:600;">✓ Yes, I'm available</span>
          <span style="background:#fff0f0;border:1px solid #fecaca;color:#991b1b;padding:5px 14px;border-radius:20px;font-size:12px;font-weight:600;">✕ No, thanks</span>
        </div>
      </div>
    </div>"""
        _all_bubbles.append((wa_sent_at, _tmpl_html))

    # WATI message history
    for msg in wati_msgs:
        is_inbound = msg.get("type") == "inbound" or msg.get("direction") == "inbound"
        msg_text   = msg.get("text") or msg.get("body") or msg.get("message", "")
        msg_ts     = msg.get("timestamp") or msg.get("created_at")
        msg_time   = _fmt_time(msg_ts)
        btn_text   = (msg.get("button_reply") or {}).get("title") or msg.get("button_text", "")
        display    = btn_text or msg_text

        if not display:
            continue

        if is_inbound:
            _b = f"""
    <div style="display:flex;justify-content:flex-end;margin-bottom:12px;">
      <div>
        <div style="background:#DCF8C6;border-radius:18px 18px 4px 18px;padding:10px 16px;max-width:280px;font-size:14px;color:#111;">{display}</div>
        <div style="text-align:right;font-size:10px;color:#aaa;margin-top:3px;">{name} · {msg_time}</div>
      </div>
      <div style="width:32px;height:32px;border-radius:50%;background:#25D366;color:#fff;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;margin-left:8px;flex-shrink:0;">{(name[0] if name else 'U').upper()}</div>
    </div>"""
        else:
            _b = f"""
    <div style="display:flex;gap:8px;margin-bottom:12px;">
      <div style="width:32px;height:32px;border-radius:50%;background:#25D366;color:#fff;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;flex-shrink:0;">XH</div>
      <div>
        <div style="background:#fff;border:1px solid #e5e7eb;border-radius:4px 18px 18px 18px;padding:10px 16px;max-width:280px;font-size:14px;color:#111;">{display}</div>
        <div style="font-size:10px;color:#aaa;margin-top:3px;">Xpress Health · {msg_time}</div>
      </div>
    </div>"""
        _all_bubbles.append((msg_ts, _b))

    # User response from shifts_users (fallback when no WATI messages)
    if su.get("response_text") and not wati_msgs:
        _resp_ts   = su.get("responded_at") or datetime.now(timezone.utc)
        _resp_time = _fmt_time(_resp_ts)
        _color     = "#DCF8C6" if av == 1 else "#fff0f0"
        _b = f"""
    <div style="display:flex;justify-content:flex-end;margin-bottom:12px;">
      <div>
        <div style="background:{_color};border-radius:18px 18px 4px 18px;padding:10px 16px;max-width:280px;font-size:14px;color:#111;">{su.get('response_text')}</div>
        <div style="text-align:right;font-size:10px;color:#aaa;margin-top:3px;">{name} · {_resp_time}</div>
      </div>
      <div style="width:32px;height:32px;border-radius:50%;background:#25D366;color:#fff;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:700;margin-left:8px;flex-shrink:0;">{(name[0] if name else 'U').upper()}</div>
    </div>"""
        _all_bubbles.append((_resp_ts, _b))

    # Sort all bubbles by timestamp — latest message last
    _all_bubbles.sort(key=lambda x: x[0] if x[0] else datetime.min.replace(tzinfo=timezone.utc))
    bubbles_html = "".join(b[1] for b in _all_bubbles)
