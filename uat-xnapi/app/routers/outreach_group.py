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
    outreach_count = await db["outreach_shift_group"].count_documents({"group_id": group_oid})
    round_number   = outreach_count + 1
    now = datetime.now(timezone.utc)

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
