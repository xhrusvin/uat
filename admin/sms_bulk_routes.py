"""
sms_bulk_routes.py
Bulk SMS sending via Telnyx Messaging API.
Mirrors the structure of whatsapp_bulk_routes.py.

Env vars used:
    TELNYX_API_KEY          – Bearer token for Telnyx REST API
    TELNYX_FROM_NUMBER         – The Telnyx messaging number (E.164, e.g. +353...)
                              NOTE: distinct from TELNYX_CALLER_ID used for voice
"""

from datetime import datetime
from bson import ObjectId
import pandas as pd
import requests
import os
import traceback
import threading

from flask import request, jsonify, render_template
from database import db
from . import admin_bp
from admin.views import admin_required

# ── Telnyx config ────────────────────────────────────────────────────────────
TELNYX_API_KEY  = os.environ.get("TELNYX_API_KEY", "")
TELNYX_FROM_NUMBER = os.environ.get("TELNYX_FROM_NUMBER", "")   # separate from CALLER_ID
TELNYX_MESSAGES_URL = "https://api.telnyx.com/v2/messages"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalise_phone(raw: str) -> str:
    """Strip spaces/dashes; prepend + if missing, assume E.164-ish input."""
    phone = raw.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")
    if phone and not phone.startswith("+"):
        phone = "+" + phone
    return phone


def _sms_campaigns_col():
    return db.sms_bulk_campaigns


def _sms_messages_col():
    return db.sms_bulk_messages


def _serialize_msg(row):
    row["_id"] = str(row["_id"])
    if row.get("campaign_id"):
        row["campaign_id"] = str(row["campaign_id"])
    for field in ("created_at", "sent_at"):
        if row.get(field) and isinstance(row[field], datetime):
            row[field] = row[field].isoformat()
    row.pop("telnyx_response", None)
    row.pop("traceback", None)
    return row


def _send_sms(to: str, body: str) -> dict:
    """
    Send a single SMS via Telnyx Messaging API.
    Returns {"success": bool, "message_id": str|None, "error": str|None}.
    """
    payload = {
        "from": TELNYX_FROM_NUMBER,
        "to":   to,
        "text": body,
        "type": "SMS",
    }
    try:
        resp = requests.post(
            TELNYX_MESSAGES_URL,
            headers={
                "Authorization": f"Bearer {TELNYX_API_KEY}",
                "Content-Type":  "application/json",
            },
            json=payload,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        msg_id = data.get("data", {}).get("id")
        return {"success": True, "message_id": msg_id, "error": None, "raw": data}
    except requests.HTTPError as exc:
        return {"success": False, "message_id": None, "error": str(exc), "raw": exc.response.text if exc.response else ""}
    except Exception as exc:
        return {"success": False, "message_id": None, "error": str(exc), "raw": ""}


# ── Background worker ─────────────────────────────────────────────────────────

def _process_sms_campaign(campaign_id: ObjectId, batch_size: int = 200):
    """
    Called in a daemon thread. Fetches pending messages for the campaign,
    sends each via Telnyx, and updates the DB record.
    """
    pending = list(
        _sms_messages_col().find(
            {"campaign_id": campaign_id, "status": "pending"}
        ).limit(batch_size)
    )

    sent_count = failed_count = 0

    for msg in pending:
        phone     = msg.get("phone", "")
        body      = msg.get("message_body", "")
        msg_id    = msg["_id"]

        result = _send_sms(phone, body)

        update = {
            "status":   "sent"    if result["success"] else "failed",
            "sent_at":  datetime.utcnow() if result["success"] else None,
            "telnyx_message_id": result.get("message_id"),
            "error":    result.get("error"),
            "telnyx_response": str(result.get("raw", "")),
        }
        _sms_messages_col().update_one({"_id": msg_id}, {"$set": update})

        if result["success"]:
            sent_count += 1
        else:
            failed_count += 1

    _sms_campaigns_col().update_one(
        {"_id": campaign_id},
        {
            "$inc": {"sent": sent_count, "failed": failed_count},
            "$set": {"status": "processed", "updated_at": datetime.utcnow()},
        },
    )


# ── Routes ────────────────────────────────────────────────────────────────────

@admin_bp.route("/sms_bulk")
@admin_required
def sms_bulk():
    return render_template("admin/sms_bulk.html")


# ── 1. Search users by name / phone ──────────────────────────────────────────

@admin_bp.route("/sms_bulk/search_users")
@admin_required
def sms_bulk_search_users():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify({"success": True, "users": []})

    try:
        regex = {"$regex": q, "$options": "i"}
        cursor = db.users.find(
            {
                "$or": [
                    {"first_name": regex},
                    {"last_name":  regex},
                    {"email":      regex},
                    {"phone":      regex},
                ]
            },
            {"first_name": 1, "last_name": 1, "email": 1, "phone": 1}
        ).limit(20)

        users = []
        for u in cursor:
            users.append({
                "id":    str(u["_id"]),
                "name":  f"{u.get('first_name','')} {u.get('last_name','')}".strip(),
                "email": u.get("email", ""),
                "phone": u.get("phone", ""),
            })

        return jsonify({"success": True, "users": users})
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


# ── 2. Send SMS to selected users (manual search) ────────────────────────────

@admin_bp.route("/sms_bulk/send_selected", methods=["POST"])
@admin_required
def sms_bulk_send_selected():
    body_data = request.get_json(silent=True) or {}
    recipients = body_data.get("recipients", [])   # [{phone, name}]
    message    = body_data.get("message", "").strip()

    if not recipients:
        return jsonify({"success": False, "error": "No recipients provided"}), 400
    if not message:
        return jsonify({"success": False, "error": "Message body required"}), 400

    campaign_id = _sms_campaigns_col().insert_one({
        "type":         "manual_search",
        "message":      message,
        "status":       "queued",
        "total":        len(recipients),
        "sent":         0,
        "failed":       0,
        "created_at":   datetime.utcnow(),
    }).inserted_id

    docs = []
    for r in recipients:
        phone = _normalise_phone(str(r.get("phone", "")))
        if not phone:
            continue
        docs.append({
            "campaign_id":   campaign_id,
            "campaign_name": "manual_search",
            "phone":         phone,
            "name":          r.get("name", ""),
            "message_body":  message,
            "status":        "pending",
            "created_at":    datetime.utcnow(),
        })

    if docs:
        _sms_messages_col().insert_many(docs)

    threading.Thread(
        target=_process_sms_campaign,
        args=(campaign_id,),
        daemon=True,
    ).start()

    return jsonify({
        "success":     True,
        "campaign_id": str(campaign_id),
        "queued":      len(docs),
    })


# ── 3. Preview Excel upload ───────────────────────────────────────────────────

@admin_bp.route("/sms_bulk/preview", methods=["POST"])
@admin_required
def sms_bulk_preview():
    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file uploaded"}), 400

    df = pd.read_excel(request.files["file"], dtype=str).fillna("")

    phone_col = None
    for col in df.columns:
        if col.lower().strip() in ["phone", "mobile", "mobile number", "phone number"]:
            phone_col = col
            break

    if not phone_col:
        return jsonify({"success": False, "error": "Phone column not found"}), 400

    rows, errors = [], []
    for idx, row in df.iterrows():
        phone = str(row.get(phone_col, "")).strip()
        if not phone:
            errors.append({"row": idx + 2, "reason": "Missing phone"})
            continue
        rows.append({
            "phone": _normalise_phone(phone),
            "name":  str(row.get("name", row.get("Name", ""))).strip(),
        })

    return jsonify({
        "success": True,
        "total":   len(df),
        "valid":   len(rows),
        "invalid": len(errors),
        "rows":    rows[:100],
        "errors":  errors,
    })


# ── 4. Send bulk SMS from Excel upload ───────────────────────────────────────

@admin_bp.route("/sms_bulk/send", methods=["POST"])
@admin_required
def sms_bulk_send():
    message = request.form.get("message", "").strip()
    if not message:
        return jsonify({"success": False, "error": "message required"}), 400
    if "file" not in request.files:
        return jsonify({"success": False, "error": "file required"}), 400

    df = pd.read_excel(request.files["file"], dtype=str).fillna("")

    phone_col = None
    name_col  = None
    for col in df.columns:
        cl = col.lower().strip()
        if cl in ["phone", "mobile", "mobile number", "phone number"]:
            phone_col = col
        if cl == "name":
            name_col = col

    if not phone_col:
        return jsonify({"success": False, "error": "Phone column not found"}), 400

    campaign_id = _sms_campaigns_col().insert_one({
        "type":       "excel_upload",
        "message":    message,
        "status":     "queued",
        "total":      len(df),
        "sent":       0,
        "failed":     0,
        "created_at": datetime.utcnow(),
    }).inserted_id

    docs = []
    for _, row in df.iterrows():
        phone = str(row.get(phone_col, "")).strip()
        if not phone:
            continue
        name  = str(row.get(name_col, "")).strip() if name_col else ""

        # Allow {{name}} merge tag in message body
        body = message.replace("{{name}}", name) if name else message

        docs.append({
            "campaign_id":   campaign_id,
            "campaign_name": "excel_upload",
            "phone":         _normalise_phone(phone),
            "name":          name,
            "message_body":  body,
            "status":        "pending",
            "created_at":    datetime.utcnow(),
        })

    if docs:
        _sms_messages_col().insert_many(docs)

    threading.Thread(
        target=_process_sms_campaign,
        args=(campaign_id,),
        daemon=True,
    ).start()

    return jsonify({
        "success":     True,
        "campaign_id": str(campaign_id),
        "queued":      len(docs),
    })


# ── 5. Campaign status ────────────────────────────────────────────────────────

@admin_bp.route("/sms_bulk/status/<campaign_id>")
@admin_required
def sms_bulk_status(campaign_id):
    campaign = _sms_campaigns_col().find_one({"_id": ObjectId(campaign_id)})
    if not campaign:
        return jsonify({"success": False, "error": "Campaign not found"}), 404

    campaign["_id"] = str(campaign["_id"])
    if isinstance(campaign.get("created_at"), datetime):
        campaign["created_at"] = campaign["created_at"].isoformat()
    if isinstance(campaign.get("updated_at"), datetime):
        campaign["updated_at"] = campaign["updated_at"].isoformat()

    return jsonify({"success": True, "campaign": campaign})


# ── 6. Message list for a campaign ───────────────────────────────────────────

@admin_bp.route("/sms_bulk/messages/<campaign_id>")
@admin_required
def sms_bulk_messages(campaign_id):
    rows = list(
        _sms_messages_col().find(
            {"campaign_id": ObjectId(campaign_id)},
            {"phone": 1, "name": 1, "status": 1, "error": 1, "message_body": 1}
        )
    )
    for row in rows:
        row["_id"] = str(row["_id"])

    return jsonify({"success": True, "messages": rows})


# ── 7. Inbound / status webhook from Telnyx ──────────────────────────────────

@admin_bp.route("/sms_bulk/webhook", methods=["POST"])
def sms_bulk_webhook():
    """
    Telnyx posts delivery receipts here.
    Register this URL in your Telnyx Messaging Profile as the
    'Status Callback URL'.
    """
    data = request.get_json(silent=True) or {}
    event_type = data.get("data", {}).get("event_type", "")

    if event_type in ("message.finalized",):
        payload    = data["data"]["payload"]
        msg_id     = payload.get("id")
        to_status  = payload.get("to", [{}])[0].get("status", "")

        telnyx_to_internal = {
            "delivered":  "sent",
            "sent":       "sent",
            "failed":     "failed",
            "undelivered":"failed",
        }
        internal_status = telnyx_to_internal.get(to_status)

        if msg_id and internal_status:
            _sms_messages_col().update_one(
                {"telnyx_message_id": msg_id},
                {"$set": {"status": internal_status, "updated_at": datetime.utcnow()}}
            )

    return "", 200