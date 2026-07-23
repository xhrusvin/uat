import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
limiter = Limiter(key_func=get_remote_address)
router = APIRouter(prefix="/outreach-group", tags=["Outreach Shift Group"])

STATUS_TEXT = {
    0:  "Not Started",
    1:  "Live",
    2:  "Paused",
    3:  "Ended",
    10: "Completed",
}


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


# ── Schemas ───────────────────────────────────────────────────────────────────

class GroupOutreachRequest(BaseModel):
    sequence_id: str
    group_id:    str


class GroupOutreachActionRequest(BaseModel):
    group_id: str


class GroupOutreachEndRequest(BaseModel):
    group_id:        str
    end_reason_id:   Optional[str] = None
    end_reason_text: Optional[str] = None


# ── Helper ────────────────────────────────────────────────────────────────────

async def _get_group_or_404(db, group_id: str):
    if not ObjectId.is_valid(group_id):
        raise HTTPException(status_code=422, detail="Invalid group_id")
    group = await db["shifts_group"].find_one({"_id": ObjectId(group_id)})
    if not group:
        raise HTTPException(status_code=404, detail="Shift group not found")
    return group


# ── POST /outreach-group/detail ───────────────────────────────────────────────

@router.post("/detail", summary="Preview group outreach before starting",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("60/minute")
async def group_outreach_detail(request: Request, payload: GroupOutreachRequest):
    """
    Body: { "sequence_id": "...", "group_id": "..." }
    Returns sequence name, pool composition, plan and pause config.
    """
    db = _get_db()
    if not ObjectId.is_valid(payload.sequence_id):
        raise HTTPException(status_code=422, detail="Invalid sequence_id")

    group    = await _get_group_or_404(db, payload.group_id)
    sequence = await db["sequences"].find_one({"_id": ObjectId(payload.sequence_id)})
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")

    group_oid = ObjectId(payload.group_id)
    outreach_count = await db["outreach_shift_group"].count_documents({"group_id": group_oid})
    round_number   = outreach_count + 1
    is_first       = outreach_count == 0

    total_staff = await db["shifts_group_pool"].count_documents({"group_id": group_oid})
    phone_count = total_staff  # all via phone for now
    pool_summary = f"{total_staff} staff · phone {phone_count}, WhatsApp 0, email 0"

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
            "sequence":    sequence.get("name", "—"),
            "sequence_id": payload.sequence_id,
            "group_id":    payload.group_id,
            "group_name":  group.get("name"),
            "pool": {
                "total_staff": total_staff,
                "phone":       phone_count,
                "whatsapp":    0,
                "email":       0,
                "summary":     pool_summary,
            },
            "plan": {
                "rounds":          3,
                "staff_per_round": 6,
                "delay_seconds":   90,
                "summary":         "3 rounds of 6 staff, 90s delay between",
            },
            "pause_on": "First Available Staff",
        },
    }


# ── POST /outreach-group/create ───────────────────────────────────────────────

@router.post("/create", summary="Create group outreach and populate shifts_group_users",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def create_group_outreach(request: Request, payload: GroupOutreachRequest):
    """
    Body: { "sequence_id": "...", "group_id": "..." }
    Creates outreach_shift_group doc, copies shifts_group_pool → shifts_group_users.
    Skips users with availability == 1.
    """
    db = _get_db()
    if not ObjectId.is_valid(payload.sequence_id):
        raise HTTPException(status_code=422, detail="Invalid sequence_id")

    group    = await _get_group_or_404(db, payload.group_id)
    sequence = await db["sequences"].find_one({"_id": ObjectId(payload.sequence_id)})
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")

    group_oid = ObjectId(payload.group_id)
    seq_oid   = ObjectId(payload.sequence_id)
    now = datetime.now(timezone.utc)

    # Check if latest outreach is Ended (3) — if so reset all and start fresh round 1
    latest = await db["outreach_shift_group"].find_one(
        {"group_id": group_oid}, sort=[("created_at", -1)]
    )

    if latest and latest.get("outreach_status") == 3:
        # Delete all previous outreach records for this group
        await db["outreach_shift_group"].delete_many({"group_id": group_oid})
        # Delete all shifts_group_users for this group (except availability==1)
        await db["shifts_group_users"].delete_many({
            "group_id":    group_oid,
            "availability": {"$ne": 1},
        })
        round_number = 1
    else:
        outreach_count = await db["outreach_shift_group"].count_documents({"group_id": group_oid})
        round_number   = outreach_count + 1

    doc = {
        "group_id":       group_oid,
        "sequence_id":    seq_oid,
        "round_number":   round_number,
        "outreach_status": 1,
        "status":         "active",
        "pause_on":       "first_available",
        "started_at":     now,
        "paused_at":      None,
        "ended_at":       None,
        "created_at":     now,
        "updated_at":     now,
    }
    result   = await db["outreach_shift_group"].insert_one(doc)
    oid      = result.inserted_id
    doc["_id"] = oid

    # Copy shifts_group_pool → shifts_group_users (skip availability == 1)
    pool_docs = await db["shifts_group_pool"].find({"group_id": group_oid}).to_list(5000)
    inserted_count = skipped = 0
    for pd in pool_docs:
        user_oid = pd.get("user_id")
        if not user_oid:
            continue
        exists = await db["shifts_group_users"].find_one({
            "group_id":    group_oid,
            "user_id":     user_oid,
            "availability": 1,
        })
        if exists:
            skipped += 1
            continue
        await db["shifts_group_users"].insert_one({
            "user_id":            user_oid,
            "group_id":           group_oid,
            "outreach_id":        oid,
            "assigned_at":        now,
            "availability":       6,
            "call_enabled":       1,
            "call_processed":     0,
            "call_processed_at":  now,
            "conversation_id":    None,
            "call_status":        0,
            "updated_at":         now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        })
        inserted_count += 1

    # Activity log
    await db["activities"].insert_one({
        "activity_type": "round_started",
        "group_id":      group_oid,
        "outreach_id":   oid,
        "metadata": {
            "round_number":     round_number,
            "sequence_id":      str(seq_oid),
            "call_enabled_set": inserted_count,
        },
        "created_at": now,
    })

    return {
        "success":      True,
        "round_number": round_number,
        "message":      f"Round {round_number} group outreach created",
        "data": {
            "id":              str(oid),
            "group_id":        payload.group_id,
            "sequence_id":     payload.sequence_id,
            "round_number":    round_number,
            "outreach_status": 1,
            "status":          "active",
            "started_at":      now.isoformat(),
            "created_at":      now.isoformat(),
        },
        "shifts_group_users_update": {"inserted": inserted_count, "skipped": skipped},
    }


# ── POST /outreach-group/pause ────────────────────────────────────────────────

@router.post("/pause", summary="Pause group outreach",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def pause_group_outreach(request: Request, payload: GroupOutreachActionRequest):
    db = _get_db()
    group_oid = ObjectId(payload.group_id) if ObjectId.is_valid(payload.group_id) else None
    if not group_oid:
        raise HTTPException(status_code=422, detail="Invalid group_id")

    outreach = await db["outreach_shift_group"].find_one(
        {"group_id": group_oid, "outreach_status": 1}, sort=[("created_at", -1)]
    )
    if not outreach:
        raise HTTPException(status_code=404, detail="No active (Live) group outreach found")

    now = datetime.now(timezone.utc)
    await db["outreach_shift_group"].update_one(
        {"_id": outreach["_id"]},
        {"$set": {"outreach_status": 2, "paused_at": now, "updated_at": now}}
    )
    result = await db["shifts_group_users"].update_many(
        {"group_id": group_oid, "call_processed": {"$ne": 1}},
        {"$set": {"call_enabled": 0, "updated_at": now.strftime("%Y-%m-%dT%H:%M:%S+00:00")}}
    )

    avail   = await db["shifts_group_users"].count_documents({"group_id": group_oid, "availability": 1})
    declined = await db["shifts_group_users"].count_documents({"group_id": group_oid, "availability": {"$in": [0,3,4]}})
    no_reply = await db["shifts_group_users"].count_documents({"group_id": group_oid, "availability": 6})

    await db["activities"].insert_one({
        "activity_type": "round_paused",
        "group_id":      group_oid,
        "outreach_id":   outreach["_id"],
        "metadata": {
            "round_number": outreach.get("round_number", 1),
            "available": avail, "declined": declined, "no_reply": no_reply,
            "summary": f"Round {outreach.get('round_number',1)} paused · {avail} available, {declined} declined, {no_reply} no-reply",
        },
        "created_at": now,
    })

    return {
        "success": True, "message": "Group outreach paused",
        "outreach_id": str(outreach["_id"]), "group_id": payload.group_id,
        "outreach_status": 2, "outreach_status_text": "Paused",
        "shifts_group_users_updated": result.modified_count,
    }


# ── POST /outreach-group/restart ──────────────────────────────────────────────

@router.post("/restart", summary="Restart paused group outreach",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def restart_group_outreach(request: Request, payload: GroupOutreachActionRequest):
    db = _get_db()
    group_oid = ObjectId(payload.group_id) if ObjectId.is_valid(payload.group_id) else None
    if not group_oid:
        raise HTTPException(status_code=422, detail="Invalid group_id")

    outreach = await db["outreach_shift_group"].find_one(
        {"group_id": group_oid, "outreach_status": 2}, sort=[("created_at", -1)]
    )
    if not outreach:
        raise HTTPException(status_code=404, detail="No paused group outreach found")

    now = datetime.now(timezone.utc)

    # Sync pool → shifts_group_users
    pool_docs = await db["shifts_group_pool"].find({"group_id": group_oid}, {"user_id": 1}).to_list(5000)
    pool_ids  = {str(p["user_id"]) for p in pool_docs if p.get("user_id")}
    su_docs   = await db["shifts_group_users"].find(
        {"group_id": group_oid, "outreach_id": outreach["_id"]}, {"user_id": 1}
    ).to_list(5000)
    su_ids = {str(su["user_id"]) for su in su_docs if su.get("user_id")}

    added = removed = 0
    for p in pool_docs:
        uid = str(p.get("user_id",""))
        if uid and uid not in su_ids:
            exists_avail = await db["shifts_group_users"].find_one({"group_id": group_oid, "user_id": p["user_id"], "availability": 1})
            if not exists_avail:
                await db["shifts_group_users"].insert_one({
                    "user_id": p["user_id"], "group_id": group_oid,
                    "outreach_id": outreach["_id"], "assigned_at": now,
                    "availability": 6, "call_enabled": 1, "call_processed": 0,
                    "call_processed_at": now, "conversation_id": None,
                    "call_status": 0, "updated_at": now.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
                })
                added += 1

    for su in su_docs:
        uid = str(su.get("user_id",""))
        if uid and uid not in pool_ids:
            full = await db["shifts_group_users"].find_one({"_id": su["_id"]})
            if full and full.get("availability") != 1:
                await db["shifts_group_users"].delete_one({"_id": su["_id"]})
                removed += 1

    await db["outreach_shift_group"].update_one(
        {"_id": outreach["_id"]},
        {"$set": {"outreach_status": 1, "paused_at": None, "updated_at": now}}
    )
    result = await db["shifts_group_users"].update_many(
        {"outreach_id": outreach["_id"]},
        {"$set": {"call_enabled": 1, "updated_at": now.strftime("%Y-%m-%dT%H:%M:%S+00:00")}}
    )

    await db["activities"].insert_one({
        "activity_type": "round_started",
        "group_id": group_oid, "outreach_id": outreach["_id"],
        "metadata": {"round_number": outreach.get("round_number",1), "restarted": True},
        "created_at": now,
    })

    return {
        "success": True, "message": "Group outreach restarted",
        "outreach_id": str(outreach["_id"]), "group_id": payload.group_id,
        "outreach_status": 1, "outreach_status_text": "Live",
        "shifts_group_users_updated": result.modified_count,
        "pool_sync": {"added": added, "removed": removed},
    }


# ── POST /outreach-group/end ──────────────────────────────────────────────────

@router.post("/end", summary="End group outreach",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def end_group_outreach(request: Request, payload: GroupOutreachEndRequest):
    db = _get_db()
    group_oid = ObjectId(payload.group_id) if ObjectId.is_valid(payload.group_id) else None
    if not group_oid:
        raise HTTPException(status_code=422, detail="Invalid group_id")

    latest = await db["outreach_shift_group"].find_one({"group_id": group_oid}, sort=[("created_at", -1)])
    if not latest:
        raise HTTPException(status_code=404, detail="No group outreach found")

    status = latest.get("outreach_status", 0)
    if status == 10:
        raise HTTPException(status_code=409, detail="Group outreach is already Completed")
    if status == 3:
        raise HTTPException(status_code=409, detail="Already ended. Create a new round first")
    if status not in (1, 2):
        raise HTTPException(status_code=409, detail=f"Cannot end from status {status}")

    now = datetime.now(timezone.utc)

    end_reason_label = payload.end_reason_text or None
    if payload.end_reason_id and ObjectId.is_valid(payload.end_reason_id):
        reason_doc = await db["outreach_end_reasons"].find_one({"_id": ObjectId(payload.end_reason_id)}, {"reason": 1})
        if reason_doc:
            end_reason_label = reason_doc.get("reason")

    if end_reason_label:
        await db["outreach_shift_group"].update_one(
            {"_id": latest["_id"]}, {"$set": {"end_reason": end_reason_label}}
        )

    await db["outreach_shift_group"].update_one(
        {"_id": latest["_id"]},
        {"$set": {"outreach_status": 3, "ended_at": now, "updated_at": now}}
    )
    result = await db["shifts_group_users"].update_many(
        {"group_id": group_oid, "call_processed": 0},
        {"$set": {"call_enabled": 0, "updated_at": now.strftime("%Y-%m-%dT%H:%M:%S+00:00")}}
    )

    avail    = await db["shifts_group_users"].count_documents({"group_id": group_oid, "availability": 1})
    declined = await db["shifts_group_users"].count_documents({"group_id": group_oid, "availability": {"$in": [0,3,4]}})
    no_reply = await db["shifts_group_users"].count_documents({"group_id": group_oid, "call_processed": 0})
    rn       = latest.get("round_number", 1)

    await db["activities"].insert_one({
        "activity_type": "round_ended",
        "group_id": group_oid, "outreach_id": latest["_id"],
        "metadata": {
            "round_number": rn, "available": avail,
            "declined": declined, "no_reply": no_reply,
            "end_reason": end_reason_label,
            "summary": f"Round {rn} ended · {avail} available, {declined} declined, {no_reply} no-reply",
        },
        "created_at": now,
    })

    return {
        "success": True, "message": f"Round {rn} ended",
        "outreach_id": str(latest["_id"]), "group_id": payload.group_id,
        "outreach_status": 3, "outreach_status_text": "Ended",
        "end_reason": end_reason_label,
        "shifts_group_users_updated": result.modified_count,
    }


# ── POST /outreach-group/complete ─────────────────────────────────────────────

@router.post("/complete", summary="Complete group outreach (status 10)",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def complete_group_outreach(request: Request, payload: GroupOutreachActionRequest):
    db = _get_db()
    group_oid = ObjectId(payload.group_id) if ObjectId.is_valid(payload.group_id) else None
    if not group_oid:
        raise HTTPException(status_code=422, detail="Invalid group_id")

    latest = await db["outreach_shift_group"].find_one({"group_id": group_oid}, sort=[("created_at", -1)])
    if not latest:
        raise HTTPException(status_code=404, detail="No group outreach found")

    status = latest.get("outreach_status", 0)
    if status == 10:
        raise HTTPException(status_code=409, detail="Group outreach is already Completed")
    if status not in (1, 2, 3):
        raise HTTPException(status_code=409, detail=f"Cannot complete from status {status}")

    now = datetime.now(timezone.utc)
    await db["outreach_shift_group"].update_one(
        {"_id": latest["_id"]},
        {"$set": {"outreach_status": 10, "ended_at": now, "updated_at": now}}
    )
    result = await db["shifts_group_users"].update_many(
        {"group_id": group_oid, "call_processed": 0},
        {"$set": {"call_enabled": 0, "updated_at": now.strftime("%Y-%m-%dT%H:%M:%S+00:00")}}
    )

    avail    = await db["shifts_group_users"].count_documents({"group_id": group_oid, "availability": 1})
    declined = await db["shifts_group_users"].count_documents({"group_id": group_oid, "availability": {"$in": [0,3,4]}})
    no_reply = await db["shifts_group_users"].count_documents({"group_id": group_oid, "call_processed": 0})
    rn       = latest.get("round_number", 1)

    await db["activities"].insert_one({
        "activity_type": "round_completed",
        "group_id": group_oid, "outreach_id": latest["_id"],
        "metadata": {
            "round_number": rn, "available": avail,
            "declined": declined, "no_reply": no_reply,
            "summary": f"Round {rn} completed · {avail} available, {declined} declined, {no_reply} no-reply",
        },
        "created_at": now,
    })

    return {
        "success": True, "message": f"Round {rn} completed",
        "outreach_id": str(latest["_id"]), "group_id": payload.group_id,
        "outreach_status": 10, "outreach_status_text": "Completed",
        "shifts_group_users_updated": result.modified_count,
    }


# ── POST /outreach-group/staff_list ──────────────────────────────────────────

class GroupOutreachIdRequest(BaseModel):
    outreach_id: str


@router.post("/staff_list", summary="Get group outreach with full shifts_group_users list",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("60/minute")
async def group_outreach_staff_list(request: Request, payload: GroupOutreachIdRequest):
    db = _get_db()
    if not ObjectId.is_valid(payload.outreach_id):
        raise HTTPException(status_code=422, detail="Invalid outreach_id")

    outreach_oid = ObjectId(payload.outreach_id)
    outreach = await db["outreach_shift_group"].find_one({"_id": outreach_oid})
    if not outreach:
        raise HTTPException(status_code=404, detail="Group outreach not found")

    seq_name = None
    seq_oid  = outreach.get("sequence_id")
    if seq_oid:
        seq = await db["sequences"].find_one({"_id": seq_oid}, {"name": 1})
        if seq:
            seq_name = seq.get("name")

    group_oid   = outreach.get("group_id")
    group_info  = None
    if group_oid:
        gr = await db["shifts_group"].find_one({"_id": group_oid}, {"name": 1, "shift_ids": 1})
        if gr:
            group_info = {"group_id": str(group_oid), "name": gr.get("name"), "shift_count": len(gr.get("shift_ids") or [])}

    su_docs = await db["shifts_group_users"].find(
        {"outreach_id": outreach_oid},
        {"user_id": 1, "availability": 1, "call_enabled": 1, "call_processed": 1,
         "call_processed_at": 1, "call_status": 1, "assigned_at": 1, "flag": 1,
         "group_id": 1, "outreach_id": 1, "conversation_id": 1,
         "availability_details": 1}
    ).to_list(length=2000)

    user_oids = [ObjectId(str(su["user_id"])) for su in su_docs if su.get("user_id") and ObjectId.is_valid(str(su.get("user_id","")))]
    user_map: dict = {}
    if user_oids:
        async for u in db["users"].find(
            {"_id": {"$in": user_oids}},
            {"first_name": 1, "last_name": 1, "email": 1, "phone": 1,
             "xn_user_id": 1, "designation": 1, "rating": 1,
             "county": 1, "county_id": 1, "tags": 1, "location": 1}
        ):
            user_map[str(u["_id"])] = u

    AVAIL_TEXT = {1: "Available", 0: "Not Available", 3: "Voicemail", 4: "Call Not Attended", 6: "Call Not Triggered"}

    from app.routers.staff import _user_coords as _uc_g
    from app.routers.outreach import _format_call_time as _fct_g

    staff_list = []
    for su in su_docs:
        uid_str  = str(su.get("user_id", ""))
        u        = user_map.get(uid_str, {})
        avail    = su.get("availability")
        raw_oid  = su.get("outreach_id")

        # Staff tags
        staff_tags = [
            {"id": str(t.get("id","")), "name": t.get("name","")} if isinstance(t, dict)
            else {"id": "", "name": str(t)} for t in (u.get("tags") or [])
        ]

        # Last contacted
        last_contacted = None
        lc_su = await db["shifts_group_users"].find_one(
            {"user_id": su.get("user_id"), "call_processed_at": {"$ne": None}},
            sort=[("call_processed_at", -1)], projection={"call_processed_at": 1}
        )
        if lc_su and lc_su.get("call_processed_at"):
            lc_dt = lc_su["call_processed_at"]
            if hasattr(lc_dt, "tzinfo") and lc_dt.tzinfo is None:
                lc_dt = lc_dt.replace(tzinfo=timezone.utc)
            diff = int((datetime.now(timezone.utc) - lc_dt).total_seconds())
            if diff < 60:       last_contacted = "just now"
            elif diff < 3600:   last_contacted = f"{diff//60} minute{'s' if diff//60!=1 else ''} ago"
            elif diff < 86400:  last_contacted = f"{diff//3600} hour{'s' if diff//3600!=1 else ''} ago"
            else:               last_contacted = f"{diff//86400} day{'s' if diff//86400!=1 else ''} ago"

        # Response + call_details
        response_text = response_time_g = None
        call_details  = None
        conv_g = await db["shift_booking_conv"].find_one(
            {"user_id": uid_str},
            {"turns": 1, "started_at": 1, "ended_at": 1,
             "elevenlabs_conversation_id": 1, "round_number": 1,
             "phone": 1, "duration_seconds": 1, "confidence": 1}
        )
        if conv_g:
            for turn in reversed(conv_g.get("turns") or []):
                if turn.get("role") in ("user", "human") and turn.get("message"):
                    response_text = turn["message"]
                    ts = turn.get("ts")
                    if ts and hasattr(ts, "strftime"):
                        response_time_g = ts.strftime("%H:%M")
                    break
            started_g = conv_g.get("started_at")
            ended_g   = conv_g.get("ended_at")
            dur_g     = conv_g.get("duration_seconds")
            if not dur_g and started_g and ended_g:
                dur_g = int((ended_g - started_g).total_seconds())
            pt = started_g.strftime("%H:%M:%S") if started_g and hasattr(started_g, "strftime") else None
            rn_g = conv_g.get("round_number", 1)
            ph_g = conv_g.get("phone") or u.get("phone")
            ai_h = None
            conf_g = conv_g.get("confidence")
            for turn in reversed(conv_g.get("turns") or []):
                if turn.get("role") in ("user", "human") and turn.get("message"):
                    t_ts = turn.get("ts")
                    t_t  = t_ts.strftime("%H:%M") if t_ts and hasattr(t_ts, "strftime") else None
                    cp   = f"{int(conf_g * 100)}% confidence" if conf_g else None
                    parts = [f'"{turn["message"]}"']
                    if t_t: parts.append(f"at {t_t}")
                    if cp:  parts.append(f"· {cp}")
                    ai_h = " ".join(parts)
                    break
            call_details = {
                "called_via": f"{ph_g} (phone)" if ph_g else "Phone",
                "placed_at":  f"{pt} · Round {rn_g}" if pt else None,
                "duration":   f"{dur_g} seconds" if dur_g else None,
                "ai_heard":   ai_h,
            }

        staff_list.append({
            "id":                  str(su["_id"]),
            "user_id":             uid_str,
            "xn_user_id":          u.get("xn_user_id"),
            "name":                " ".join(filter(None, [u.get("first_name",""), u.get("last_name","")])).strip() or "—",
            "email":               u.get("email"),
            "phone":               u.get("phone"),
            "designation":         u.get("designation"),
            "rating":              u.get("rating"),
            "county":              u.get("county"),
            "county_id":           str(u["county_id"]) if u.get("county_id") else None,
            "last_contacted":      last_contacted,
            "staff_tags":          staff_tags,
            "visa_hours_remaining": "8/24",
            "channel":             "Phone",
            "response_text":       response_text,
            "response_time":       response_time_g,
            "availability":        avail,
            "availability_text":   AVAIL_TEXT.get(avail, "Unknown"),
            "call_enabled":        su.get("call_enabled"),
            "call_processed":      su.get("call_processed"),
            "call_processed_text": "Sent" if su.get("call_processed") == 1 else "Queued",
            "start_time":          _fct_g(su.get("call_processed_at")) if su.get("call_processed_at") and hasattr(su.get("call_processed_at"), "date") else None,
            "flag":                su.get("flag", 0),
            "call_status":         su.get("call_status"),
            "call_processed_at":   su["call_processed_at"].isoformat() if su.get("call_processed_at") and hasattr(su["call_processed_at"], "isoformat") else None,
            "assigned_at":         su["assigned_at"].isoformat() if su.get("assigned_at") and hasattr(su["assigned_at"], "isoformat") else None,
            "group_id":            str(su.get("group_id","")) if su.get("group_id") else None,
            "outreach_id":         str(raw_oid) if raw_oid else None,
            "conversation_id":     su.get("conversation_id"),
            "call_details":        call_details,
            "availability_details": su.get("availability_details") or [],
        })

    total     = len(staff_list)
    available = sum(1 for s in staff_list if s["availability"] == 1)
    pending   = sum(1 for s in staff_list if s["call_enabled"] == 1 and s["call_processed"] == 0)
    processed = sum(1 for s in staff_list if s["call_processed"] == 1)
    o_status  = outreach.get("outreach_status", 0)

    return {
        "success": True,
        "data": {
            "id":                   str(outreach["_id"]),
            "group_id":             str(group_oid) if group_oid else None,
            "sequence_id":          str(seq_oid) if seq_oid else None,
            "sequence_name":        seq_name,
            "round_number":         outreach.get("round_number"),
            "outreach_status":      o_status,
            "outreach_status_text": STATUS_TEXT.get(o_status, "Not Started"),
            "group":                group_info,
            "counts": {"total": total, "available": available, "pending": pending, "processed": processed},
            "shifts_group_users":   staff_list,
        },
    }


# ── POST /outreach-group/flag ─────────────────────────────────────────────────

class GroupFlagRequest(BaseModel):
    outreach_id:        str
    shifts_group_users_id: str
    flag:               int = 1


@router.post("/flag", summary="Flag or unflag a staff member in shifts_group_users",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("60/minute")
async def flag_group_staff(request: Request, payload: GroupFlagRequest):
    db = _get_db()
    if not ObjectId.is_valid(payload.outreach_id):
        raise HTTPException(status_code=422, detail="Invalid outreach_id")
    if not ObjectId.is_valid(payload.shifts_group_users_id):
        raise HTTPException(status_code=422, detail="Invalid shifts_group_users_id")

    su_oid = ObjectId(payload.shifts_group_users_id)
    doc = await db["shifts_group_users"].find_one(
        {"_id": su_oid, "outreach_id": ObjectId(payload.outreach_id)}, {"_id": 1}
    )
    if not doc:
        raise HTTPException(status_code=404, detail="Record not found for this outreach")

    now = datetime.now(timezone.utc)
    await db["shifts_group_users"].update_one(
        {"_id": su_oid},
        {"$set": {"flag": payload.flag, "updated_at": now.strftime("%Y-%m-%dT%H:%M:%S+00:00")}}
    )
    return {
        "success": True, "message": "Flagged" if payload.flag else "Unflagged",
        "outreach_id": payload.outreach_id,
        "shifts_group_users_id": payload.shifts_group_users_id,
        "flag": payload.flag,
    }


# ── POST /outreach-group/transcription ───────────────────────────────────────



# ── POST /outreach-group/transcription ───────────────────────────────────────

class GroupTranscriptionV2Request(BaseModel):
    conversation_id: str   # shifts_group_users.conversation_id (unique)


@router.post("/transcription", summary="Get AI call transcription for a group staff member",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("60/minute")
async def group_transcription_v2(request: Request, payload: GroupTranscriptionV2Request):
    """
    Body: { "conversation_id": "conv_..." }
    Fetches from shift_booking_bulk_conv collection.
    """
    db = _get_db()

    su_doc = await db["shifts_group_users"].find_one(
        {"conversation_id": payload.conversation_id}
    )

    availability_details = []
    if su_doc:
        avail_list = su_doc.get("availability_details") or []
        if isinstance(avail_list, list):
            availability_details = avail_list

    conv = await db["shift_booking_bulk_conv"].find_one(
        {"elevenlabs_conversation_id": payload.conversation_id}
    )
    if not conv:
        raise HTTPException(status_code=404, detail="No conversation found")

    def _fmt(dt):
        return dt.isoformat() if dt and hasattr(dt, "isoformat") else None

    turns = [
        {"role": t.get("role"), "message": t.get("message") or t.get("text"), "ts": _fmt(t.get("ts"))}
        for t in conv.get("turns", [])
    ]

    return {
        "success": True,
        "data": {
            "id":                          str(conv["_id"]),
            "conversation_id":             payload.conversation_id,
            "elevenlabs_conversation_id":  conv.get("elevenlabs_conversation_id"),
            "user_id":                     str(su_doc["user_id"]) if su_doc and su_doc.get("user_id") else None,
            "group_id":                    str(su_doc["group_id"]) if su_doc and su_doc.get("group_id") else None,
            "started_at":                  _fmt(conv.get("started_at")),
            "ended_at":                    _fmt(conv.get("ended_at")),
            "turns":                       turns,
            "has_audio":                   bool(conv.get("elevenlabs_conversation_id")),
            "availability_details":        availability_details,
        },
    }


# ── POST /outreach-group/transcription/audio ─────────────────────────────────

@router.post("/transcription/audio", summary="Stream audio for a group staff call",
             dependencies=[Depends(verify_api_key)])
@limiter.limit("30/minute")
async def group_transcription_audio_v2(request: Request, payload: GroupTranscriptionV2Request):
    """
    Body: { "conversation_id": "conv_..." }
    Fetches from shift_booking_bulk_conv and streams MP3 from ElevenLabs.
    """
    from fastapi.responses import StreamingResponse
    db = _get_db()

    conv = await db["shift_booking_bulk_conv"].find_one(
        {"elevenlabs_conversation_id": payload.conversation_id},
        {"elevenlabs_conversation_id": 1}
    )
    if not conv:
        raise HTTPException(status_code=404, detail="No conversation found")

    el_id = conv.get("elevenlabs_conversation_id")
    if not el_id:
        raise HTTPException(status_code=404, detail="No audio available")

    api_key = settings.ELEVENLABS_API_KEY or ""
    url     = f"https://api.elevenlabs.io/v1/convai/conversations/{el_id}/audio"

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers={"xi-api-key": api_key})

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code,
            detail=f"ElevenLabs error: {resp.text[:200]} | key: {api_key[:8]}...")

    return StreamingResponse(
        iter([resp.content]),
        media_type="audio/mpeg",
        headers={"Content-Disposition": "inline; filename=call_audio.mp3"},
    )
