import logging
from datetime import datetime, timezone
from typing import Optional, List
from bson import ObjectId

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.core.config import settings
from app.core.security import verify_api_key

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/wati", tags=["WATI WhatsApp"])


def _get_db():
    from app.db.database import _client
    return _client[settings.MONGODB_DB]


def _serialize_msg(doc: dict) -> dict:
    return {
        "id":               str(doc["_id"]),
        "broadcast_id":     str(doc.get("broadcast_id", "")),
        "user_id":          str(doc.get("user_id", "")) if doc.get("user_id") else None,
        "phone":            doc.get("phone"),
        "name":             doc.get("name"),
        "message":          doc.get("message"),
        "template_name":    doc.get("template_name"),
        "status":           doc.get("status", "pending"),
        "wati_message_id":  doc.get("wati_message_id"),
        "response":         doc.get("response"),
        "response_at":      doc["response_at"].isoformat() if doc.get("response_at") and hasattr(doc["response_at"], "isoformat") else None,
        "sent_at":          doc["sent_at"].isoformat() if doc.get("sent_at") and hasattr(doc["sent_at"], "isoformat") else None,
        "created_at":       doc["created_at"].isoformat() if doc.get("created_at") and hasattr(doc["created_at"], "isoformat") else None,
    }


# ── POST /wati/broadcast ──────────────────────────────────────────────────────

class BroadcastRequest(BaseModel):
    message:       str
    template_name: Optional[str] = None
    user_ids:      Optional[List[str]] = None  # if None → send to all Enabled users
    designation:   Optional[str] = None         # filter by designation
    county:        Optional[str] = None         # filter by county


@router.post("/broadcast", summary="Send WhatsApp message to users", dependencies=[Depends(verify_api_key)])
async def broadcast_message(request: Request, payload: BroadcastRequest):
    import httpx as _httpx
    db  = _get_db()
    now = datetime.now(timezone.utc)

    if not settings.WATI_API_URL or not settings.WATI_API_TOKEN:
        raise HTTPException(status_code=503, detail="WATI not configured. Set WATI_API_URL and WATI_API_TOKEN in .env")

    # Build user query
    user_filter: dict = {"status": "Enabled", "phone": {"$exists": True, "$ne": None}}
    if payload.user_ids:
        valid_oids = [ObjectId(i) for i in payload.user_ids if ObjectId.is_valid(i)]
        user_filter["_id"] = {"$in": valid_oids}
    if payload.designation:
        user_filter["designation"] = payload.designation
    if payload.county:
        user_filter["county"] = {"$regex": payload.county, "$options": "i"}

    users = await db["users"].find(user_filter, {"_id": 1, "first_name": 1, "last_name": 1, "phone": 1}).to_list(5000)
    if not users:
        raise HTTPException(status_code=404, detail="No users found matching criteria")

    # Create broadcast record
    broadcast_doc = {
        "message":       payload.message,
        "template_name": payload.template_name,
        "total":         len(users),
        "sent":          0,
        "failed":        0,
        "created_at":    now,
        "updated_at":    now,
        "status":        "sending",
    }
    broadcast_result = await db["wati_broadcasts"].insert_one(broadcast_doc)
    broadcast_id     = broadcast_result.inserted_id

    # Send to each user
    wati_url     = f"{settings.WATI_API_URL.rstrip('/')}/api/v1/sendSessionMessage"
    wati_headers = {
        "Authorization": f"Bearer {settings.WATI_API_TOKEN}",
        "Content-Type":  "application/json",
    }

    sent = failed = 0
    async with _httpx.AsyncClient(timeout=15.0) as client:
        for u in users:
            phone     = (u.get("phone") or "").replace(" ", "").replace("-", "").replace("+", "")
            full_name = f"{u.get('first_name','')} {u.get('last_name','')}".strip()
            if not phone:
                continue

            msg_doc = {
                "broadcast_id": broadcast_id,
                "user_id":      u["_id"],
                "phone":        phone,
                "name":         full_name,
                "message":      payload.message,
                "template_name": payload.template_name,
                "status":       "pending",
                "created_at":   now,
            }
            msg_result = await db["wati_messages"].insert_one(msg_doc)
            msg_id     = msg_result.inserted_id

            try:
                body = {"whatsappNumber": phone, "messageText": payload.message}
                if payload.template_name:
                    wati_url = f"{settings.WATI_API_URL.rstrip('/')}/api/v1/sendTemplateMessage"
                    body = {"whatsappNumber": phone, "template_name": payload.template_name}

                resp = await client.post(wati_url, json=body, headers=wati_headers)
                wati_body = resp.json() if resp.content else {}
                wati_msg_id = wati_body.get("id") or wati_body.get("messageId")

                await db["wati_messages"].update_one(
                    {"_id": msg_id},
                    {"$set": {
                        "status":          "sent" if resp.status_code == 200 else "failed",
                        "wati_message_id": wati_msg_id,
                        "sent_at":         datetime.now(timezone.utc),
                        "wati_response":   wati_body,
                    }}
                )
                if resp.status_code == 200:
                    sent += 1
                else:
                    failed += 1
            except Exception as e:
                await db["wati_messages"].update_one({"_id": msg_id}, {"$set": {"status": "error", "error": str(e)}})
                failed += 1

    await db["wati_broadcasts"].update_one(
        {"_id": broadcast_id},
        {"$set": {"sent": sent, "failed": failed, "status": "done", "updated_at": datetime.now(timezone.utc)}}
    )

    return {
        "success":      True,
        "broadcast_id": str(broadcast_id),
        "total":        len(users),
        "sent":         sent,
        "failed":       failed,
    }


# ── GET /wati/broadcasts ──────────────────────────────────────────────────────

@router.get("/broadcasts", summary="List broadcast history", dependencies=[Depends(verify_api_key)])
async def list_broadcasts(request: Request, page: int = 1, per_page: int = 20):
    db    = _get_db()
    skip  = (page - 1) * per_page
    total = await db["wati_broadcasts"].count_documents({})
    docs  = await db["wati_broadcasts"].find({}).sort("created_at", -1).skip(skip).limit(per_page).to_list(per_page)
    data  = []
    for d in docs:
        data.append({
            "id":            str(d["_id"]),
            "message":       d.get("message"),
            "template_name": d.get("template_name"),
            "total":         d.get("total", 0),
            "sent":          d.get("sent", 0),
            "failed":        d.get("failed", 0),
            "status":        d.get("status"),
            "created_at":    d["created_at"].isoformat() if d.get("created_at") and hasattr(d["created_at"], "isoformat") else None,
        })
    return {"success": True, "total": total, "page": page, "per_page": per_page, "data": data}


# ── GET /wati/messages ────────────────────────────────────────────────────────

@router.get("/messages", summary="List messages for a broadcast", dependencies=[Depends(verify_api_key)])
async def list_messages(request: Request, broadcast_id: str, page: int = 1, per_page: int = 50):
    db = _get_db()
    if not ObjectId.is_valid(broadcast_id):
        raise HTTPException(status_code=422, detail="Invalid broadcast_id")
    bid   = ObjectId(broadcast_id)
    skip  = (page - 1) * per_page
    total = await db["wati_messages"].count_documents({"broadcast_id": bid})
    docs  = await db["wati_messages"].find({"broadcast_id": bid}).sort("created_at", 1).skip(skip).limit(per_page).to_list(per_page)
    return {"success": True, "total": total, "page": page, "per_page": per_page, "data": [_serialize_msg(d) for d in docs]}


# ── POST /wati/webhook ────────────────────────────────────────────────────────

@router.post("/webhook", summary="WATI webhook — receive message responses", include_in_schema=False)
async def wati_webhook(request: Request):
    """Receives inbound WhatsApp messages from WATI webhook."""
    db  = _get_db()
    now = datetime.now(timezone.utc)
    try:
        body = await request.json()
    except Exception:
        body = {}

    logger.info(f"[WATI webhook] {body}")

    phone   = str(body.get("waId") or body.get("whatsappNumber") or "").replace("+", "")
    text    = body.get("text") or body.get("body") or ""
    msg_id  = body.get("id") or body.get("messageId")
    event   = body.get("eventType") or body.get("type", "message")

    # Save raw webhook
    await db["wati_webhook_logs"].insert_one({
        "phone":      phone,
        "text":       text,
        "event":      event,
        "wati_id":    msg_id,
        "raw":        body,
        "received_at": now,
    })

    # Match to last sent message for this phone and update with response
    if phone and text:
        last_msg = await db["wati_messages"].find_one(
            {"phone": {"$regex": phone[-9:]}, "status": "sent"},
            sort=[("sent_at", -1)]
        )
        if last_msg:
            await db["wati_messages"].update_one(
                {"_id": last_msg["_id"]},
                {"$set": {"response": text, "response_at": now, "status": "replied"}}
            )
            logger.info(f"[WATI webhook] matched response from {phone}: {text}")

    return {"success": True}


# ── GET /wati/responses ───────────────────────────────────────────────────────

@router.get("/responses", summary="List inbound webhook responses", dependencies=[Depends(verify_api_key)])
async def list_responses(request: Request, page: int = 1, per_page: int = 50):
    db    = _get_db()
    skip  = (page - 1) * per_page
    total = await db["wati_webhook_logs"].count_documents({})
    docs  = await db["wati_webhook_logs"].find({}).sort("received_at", -1).skip(skip).limit(per_page).to_list(per_page)
    data  = []
    for d in docs:
        data.append({
            "id":          str(d["_id"]),
            "phone":       d.get("phone"),
            "text":        d.get("text"),
            "event":       d.get("event"),
            "received_at": d["received_at"].isoformat() if d.get("received_at") and hasattr(d["received_at"], "isoformat") else None,
        })
    return {"success": True, "total": total, "page": page, "per_page": per_page, "data": data}
