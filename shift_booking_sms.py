# shift_booking_sms.py
# Sends SMS via Telnyx (api.telnyx.com) for shifts where channel == 'SMS'
# Mirrors shift_booking_whatsapp.py logic exactly
# Processes up to 10 pending messages per trigger call

import logging
import os
import threading
import requests as _req
from flask import current_app, jsonify, request
from bson import ObjectId
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ALLOWED_START_HOUR = 8    # SMS-friendly window — daytime only
ALLOWED_END_HOUR   = 21
BATCH_SIZE         = 10

# ── Telnyx credentials (set in environment) ────────────────────────────────────
# TELNYX_API_KEY       your API key from Telnyx Mission Control Portal
# TELNYX_FROM_NUMBER   your Telnyx number in E.164 format e.g. +353894618556
#
# Webhook — configure in Telnyx Mission Control Portal:
#   Messaging Profiles → your profile → Webhook URL:
#       https://yourapp.com/telnyx/webhook
#   (one URL handles both inbound messages AND delivery receipts)
# ──────────────────────────────────────────────────────────────────────────────

TELNYX_API_BASE = "https://api.telnyx.com/v2"


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def is_within_call_window():
    now     = datetime.utcnow()
    hour    = now.hour
    allowed = ALLOWED_START_HOUR <= hour < ALLOWED_END_HOUR
    log.info(f"[SMS TIME CHECK] {now.strftime('%Y-%m-%d %H:%M:%S UTC')} → Hour {hour} → Allowed: {allowed}")
    return allowed, now


def _check_and_end_outreach(app, shift_id: str, outreach_id: str, collection="shifts_users"):
    """End outreach automatically when all records in the batch have been processed."""
    try:
        shift_oid    = ObjectId(shift_id)    if shift_id    and ObjectId.is_valid(shift_id)    else None
        outreach_oid = ObjectId(outreach_id) if outreach_id and ObjectId.is_valid(outreach_id) else None
        if not shift_oid or not outreach_oid:
            return
        db_col    = getattr(app.db, collection)
        total     = db_col.count_documents({"shift_id": shift_oid, "outreach_id": outreach_oid})
        processed = db_col.count_documents({"shift_id": shift_oid, "outreach_id": outreach_oid, "call_processed": 1})
        if total > 0 and processed >= total:
            app.db.outreach.update_one(
                {"_id": outreach_oid, "outreach_status": {"$nin": [3, 10]}},
                {"$set": {
                    "outreach_status": 3,
                    "ended_at":        datetime.utcnow(),
                    "updated_at":      datetime.utcnow(),
                    "end_reason":      "all_sms_processed",
                }}
            )
            log.info(f"[SMS END CHECK] Outreach {outreach_id} ended — all {total} records processed in {collection}")
    except Exception as e:
        log.error(f"[SMS END CHECK] Error: {e}")


def _format_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(str(date_str).split(" ")[0].split("T")[0], "%Y-%m-%d")
        return dt.strftime("%A, %d %B %Y")
    except Exception:
        return str(date_str)


def _clean_phone(phone: str) -> str:
    """Return E.164 format with leading + (Telnyx requires +353...)."""
    cleaned = phone.replace(" ", "").replace("-", "").strip()
    if not cleaned.startswith("+"):
        cleaned = "+" + cleaned
    return cleaned


def _build_sms_body(first_name: str, shift_doc: dict) -> str:
    """
    Compose the SMS text. Aim for under 160 chars (1 segment).
    Adjust wording to match your team's copy.
    """
    facility = shift_doc.get("client_name") or shift_doc.get("location") or "the facility"
    county   = shift_doc.get("client_county") or "Ireland"
    unit     = shift_doc.get("unit") or "-"
    date_str = _format_date(shift_doc.get("date", ""))
    start    = shift_doc.get("start_time") or "TBC"
    end      = shift_doc.get("end_time")   or "TBC"
    _rate    = shift_doc.get("rate", "")
    rate     = "REG" if not _rate or str(_rate) in ("0", "0.0", "") else str(_rate)

    return (
        f"Hi {first_name}, shift available – Co. {county}\n"
        f"Facility: {facility}\n"
        f"Unit: {unit}\n"
        f"Date: {date_str}\n"
        f"Time: {start} – {end}\n"
        f"Rate: {rate}\n"
        f"If you are available, please request via the app"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Telnyx outbound send
# ─────────────────────────────────────────────────────────────────────────────

def _send_telnyx_sms(app, record, shift_doc, phone, first_name, su_id, collection="shifts_users"):
    """
    POST https://api.telnyx.com/v2/messages
    {
        "from": "+353...",
        "to":   "+353...",
        "text": "..."
    }
    Telnyx returns:
    {
        "data": {
            "id": "<message-uuid>",
            "to": [{"phone_number": "...", "status": "queued"}],
            ...
        }
    }
    """
    db_col = getattr(app.db, collection)
    try:
        api_key     = os.getenv("TELNYX_API_KEY", "")
        from_number = os.getenv("TELNYX_FROM_NUMBER", "")

        if not api_key or not from_number:
            log.error("[SMS] TELNYX_API_KEY or TELNYX_FROM_NUMBER not set")
            db_col.update_one({"_id": su_id}, {"$set": {"sms_error": "Missing TELNYX_API_KEY or TELNYX_FROM_NUMBER"}})
            return

        phone_clean = _clean_phone(phone)
        body        = _build_sms_body(first_name, shift_doc)

        payload = {
            "from": from_number,
            "to":   phone_clean,
            "text": body,
        }

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        }

        log.info(f"[SMS] Sending to {phone_clean}")
        resp = _req.post(
            f"{TELNYX_API_BASE}/messages",
            json=payload,
            headers=headers,
            timeout=20,
        )

        try:
            resp_data = resp.json()
        except ValueError:
            resp_data = {"raw_response": resp.text}

        if resp.status_code in (200, 201):
            msg_data   = resp_data.get("data", {})
            message_id = msg_data.get("id", "")
            log.info(f"[SMS] ✓ Sent to {phone_clean} — message_id={message_id}")

            db_col.update_one(
                {"_id": su_id},
                {"$set": {
                    "sms_sent":       1,
                    "sms_sent_at":    datetime.utcnow(),
                    "sms_message_id": message_id,
                    "sms_phone":      phone_clean,
                    "availability":   8,          # 8 = Sent, awaiting reply
                    "updated_at":     datetime.utcnow(),
                }}
            )
        else:
            err = f"{resp.status_code}: {resp.text[:300]}"
            log.error(f"[SMS] ✗ Failed {phone_clean}: {err}")
            db_col.update_one({"_id": su_id}, {"$set": {"sms_error": err}})

    except Exception as e:
        log.error(f"[SMS] ✗ Exception for {phone}: {e}")
        db_col.update_one({"_id": su_id}, {"$set": {"sms_error": str(e)}})


# ─────────────────────────────────────────────────────────────────────────────
# Shift doc builder  (identical to WhatsApp version)
# ─────────────────────────────────────────────────────────────────────────────

def _get_shift_doc(app, record):
    shift_id  = record.get("shift_id")
    group_id  = record.get("group_id")
    shift_doc = {}

    if shift_id:
        s = app.db.shifts.find_one({"_id": shift_id})
        if s:
            client = None
            if s.get("client_id"):
                client = app.db.clients.find_one(
                    {"xn_client_id": str(s["client_id"])},
                    {"county": 1}
                )
            shift_doc = {
                "client_name":   s.get("client_name", "") or s.get("location", ""),
                "location":      s.get("location", ""),
                "client_county": s.get("client_county", "") or (client.get("county", "") if client else ""),
                "date":          str(s.get("date", "")),
                "start_time":    s.get("start_time", ""),
                "end_time":      s.get("end_time", ""),
                "rate":          s.get("rate", ""),
                "unit":          s.get("unit", ""),
            }
    elif group_id:
        sg = app.db.shifts_group.find_one({"_id": group_id}, {"shift_ids": 1})
        if sg and sg.get("shift_ids"):
            s = app.db.shifts.find_one({"_id": sg["shift_ids"][0]})
            if s:
                shift_doc = {
                    "client_name":   s.get("client_name", "") or s.get("location", ""),
                    "location":      s.get("location", ""),
                    "client_county": s.get("client_county", ""),
                    "date":          str(s.get("date", "")),
                    "start_time":    s.get("start_time", ""),
                    "end_time":      s.get("end_time", ""),
                    "rate":          s.get("rate", ""),
                    "unit":          s.get("unit", ""),
                }
    return shift_doc


# ─────────────────────────────────────────────────────────────────────────────
# Route registration
# ─────────────────────────────────────────────────────────────────────────────

def register_shift_booking_sms_routes(app):

    def _process_batch(query, collection_name):
        allowed, server_time = is_within_call_window()
        response_base = {
            "server_time":    server_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
            "collection":     collection_name,
        }

        if not allowed:
            return jsonify({**response_base, "status": "outside_hours"}), 200

        db_col  = getattr(app.db, collection_name)
        records = list(db_col.find(query, sort=[("assigned_at", 1)], limit=BATCH_SIZE))

        if not records:
            return jsonify({**response_base, "status": "no_pending",
                            "message": f"No pending SMS in {collection_name}"}), 200

        triggered = []
        for record in records:
            su_id   = record["_id"]
            user_id = record.get("user_id")

            user = None
            if user_id:
                user = app.db.users.find_one(
                    {"_id": user_id},
                    {"phone": 1, "first_name": 1, "last_name": 1}
                )

            if not user or not user.get("phone"):
                log.warning(f"[SMS] No phone for su_id={su_id}")
                continue

            phone      = user["phone"]
            first_name = user.get("first_name", "")
            full_name  = f"{first_name} {user.get('last_name', '')}".strip()

            # Mark processed before background send (availability=7 = "Processing")
            result = db_col.update_one(
                {"_id": su_id},
                {"$set": {
                    "call_processed":    1,
                    "call_processed_at": datetime.utcnow(),
                    "availability":      7,
                    "updated_at":        datetime.utcnow(),
                }}
            )
            if result.modified_count == 0:
                continue   # already claimed by another worker

            shift_doc = _get_shift_doc(app, record)

            threading.Thread(
                target=_send_telnyx_sms,
                args=(current_app._get_current_object(), record, shift_doc,
                      phone, first_name, su_id, collection_name),
                daemon=True
            ).start()

            # Auto-end outreach if all records done
            shift_id    = str(record.get("shift_id", ""))
            outreach_id = str(record.get("outreach_id", ""))
            threading.Thread(
                target=_check_and_end_outreach,
                args=(current_app._get_current_object(), shift_id, outreach_id, collection_name),
                daemon=True
            ).start()

            triggered.append({
                "su_id":      str(su_id),
                "user_id":    str(user_id),
                "staff_name": full_name,
                "phone":      phone,
            })

        return jsonify({
            **response_base,
            "status":       "triggered",
            "triggered":    len(triggered),
            "batch_size":   BATCH_SIZE,
            "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "data":         triggered,
        }), 200

    # ── Regular shifts_users ──────────────────────────────────────────────────
    @app.route('/shift_booking_sms', methods=['GET'])
    def shift_booking_sms():
        user_id_param = request.args.get('user_id')
        if user_id_param:
            query = {"user_id": ObjectId(user_id_param), "call_processed": 0, "channel": "SMS"}
        else:
            query = {"call_processed": 0, "call_enabled": 1, "channel": "SMS"}
        return _process_batch(query, "shifts_users")

    # ── Debug ─────────────────────────────────────────────────────────────────
    @app.route('/debug-shift-booking-sms')
    def debug_shift_booking_sms():
        allowed, now = is_within_call_window()
        pending_su  = app.db.shifts_users.count_documents(
            {"call_processed": 0, "call_enabled": 1, "channel": "SMS"}
        )
        pending_sgu = app.db.shifts_group_users.count_documents(
            {"call_processed": 0, "call_enabled": 1, "channel": "SMS"}
        )
        return jsonify({
            "debug":           "shift_booking_sms.py loaded (Telnyx)",
            "server_time":     now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "batch_size":      BATCH_SIZE,
            "pending_regular": pending_su,
            "pending_group":   pending_sgu,
            "telnyx_api":      TELNYX_API_BASE,
            "from_number":     os.getenv("TELNYX_FROM_NUMBER", "not set"),
            "api_key_set":     bool(os.getenv("TELNYX_API_KEY")),
        })


# ─────────────────────────────────────────────────────────────────────────────
# Telnyx webhook  (handles BOTH inbound messages AND delivery receipts)
# Configure ONE URL in Telnyx Mission Control Portal:
#   Messaging Profiles → your profile → Inbound webhook URL
#       https://yourapp.com/telnyx/webhook
#
# Telnyx V2 webhook envelope:
# {
#   "data": {
#     "event_type": "message.received" | "message.sent" | "message.finalized",
#     "id":         "<event-uuid>",
#     "occurred_at":"2024-01-01T12:00:00Z",
#     "payload": {
#       "id":        "<message-uuid>",
#       "direction": "inbound" | "outbound",
#       "from":      {"phone_number": "+353..."},
#       "to":        [{"phone_number": "+353...", "status": "delivered"}],
#       "text":      "YES",
#       "type":      "SMS"
#     }
#   }
# }
# ─────────────────────────────────────────────────────────────────────────────

def register_telnyx_webhook_routes(app):

    @app.route('/telnyx/webhook', methods=['POST'])
    def telnyx_webhook():
        try:
            body = request.get_json(silent=True) or {}
        except Exception:
            body = {}

        log.info(f"[TELNYX WEBHOOK] Raw: {body}")

        # ── Unwrap Telnyx V2 envelope ─────────────────────────────────────────
        data       = body.get("data", {})
        event_type = data.get("event_type", "")
        payload    = data.get("payload", {})

        message_id = payload.get("id", "")
        direction  = payload.get("direction", "")       # "inbound" | "outbound"

        # from is a dict: {"phone_number": "+353..."}
        from_obj = payload.get("from") or {}
        phone    = (from_obj.get("phone_number") or "").replace("+", "").strip()

        # to is a list: [{"phone_number": "...", "status": "..."}]
        to_list     = payload.get("to") or []
        to_statuses = [t.get("status", "") for t in to_list]   # for DLR events

        text = (payload.get("text") or "").strip()
        now  = datetime.utcnow()

        # ── Persist every event ───────────────────────────────────────────────
        msg_doc = {
            "event_type": event_type,
            "message_id": message_id,
            "direction":  direction,
            "phone":      phone,
            "text":       text,
            "to_statuses":to_statuses,
            "raw":        {k: str(v)[:500] for k, v in body.items()},
            "timestamp":  now,
        }

        user = None
        if phone:
            user = app.db.users.find_one(
                {"phone": {"$regex": phone[-9:] if len(phone) >= 9 else phone, "$options": "i"}},
                {"_id": 1}
            )
        if user:
            msg_doc["user_id"] = user["_id"]

        app.db.telnyx_messages.insert_one(msg_doc)

        # ── Delivery receipt (message.finalized / message.sent) ───────────────
        # Telnyx fires "message.finalized" with final delivery status on outbound
        if event_type in ("message.finalized", "message.sent") and direction == "outbound":
            final_status = to_statuses[0] if to_statuses else "unknown"
            log.info(f"[TELNYX WEBHOOK] DLR — message_id={message_id} status={final_status}")

            # Locate the record by sms_message_id
            su         = None
            collection = "shifts_users"

            su = app.db.shifts_group_users.find_one({"sms_message_id": message_id})
            if su:
                collection = "shifts_group_users"
            else:
                su = app.db.shifts_users.find_one({"sms_message_id": message_id})

            if su:
                db_col = getattr(app.db, collection)
                db_col.update_one(
                    {"_id": su["_id"]},
                    {"$set": {
                        "sms_dlr_status": final_status,
                        "sms_dlr_at":     now,
                        "updated_at":     now,
                    }}
                )
                log.info(f"[TELNYX WEBHOOK] ✓ DLR saved — su_id={su['_id']} status={final_status}")

            return {"success": True, "event": event_type, "status": final_status}, 200

        # ── Only process inbound messages from here ───────────────────────────
        if event_type != "message.received" or direction != "inbound":
            log.info(f"[TELNYX WEBHOOK] Skipping event: {event_type} direction={direction}")
            return {"success": True, "message": f"Skipped: {event_type}"}, 200

        if not phone:
            log.warning("[TELNYX WEBHOOK] No sender phone")
            return {"success": True, "message": "No phone"}, 200

        # ── Parse YES / NO ────────────────────────────────────────────────────
        reply_lower = text.lower()
        avail = None
        if reply_lower in ("yes", "y", "1") or "yes" in reply_lower or "available" in reply_lower:
            avail = 1
        elif reply_lower in ("no", "n", "0") or "no" in reply_lower or "not available" in reply_lower:
            avail = 0

        # ── Non-standard reply → store as customer_feedback ──────────────────
        if avail is None:
            log.info(f"[TELNYX WEBHOOK] Non-standard reply — storing as customer_feedback. phone={phone} text={text!r}")

            _fb_su         = None
            _fb_collection = "shifts_users"

            if user:
                _fb_sgu = app.db.shifts_group_users.find_one(
                    {"user_id": user["_id"], "sms_sent": 1},
                    sort=[("sms_sent_at", -1)]
                )
                _fb_su_reg = app.db.shifts_users.find_one(
                    {"user_id": user["_id"], "sms_sent": 1},
                    sort=[("sms_sent_at", -1)]
                )

                if _fb_sgu and _fb_su_reg:
                    sgu_time = _fb_sgu.get("sms_sent_at") or datetime.min
                    reg_time = _fb_su_reg.get("sms_sent_at") or datetime.min
                    if sgu_time >= reg_time:
                        _fb_su = _fb_sgu
                        _fb_collection = "shifts_group_users"
                    else:
                        _fb_su = _fb_su_reg
                elif _fb_sgu:
                    _fb_su = _fb_sgu
                    _fb_collection = "shifts_group_users"
                elif _fb_su_reg:
                    _fb_su = _fb_su_reg

            if _fb_su:
                _fb_col = getattr(app.db, _fb_collection)
                _fb_col.update_one(
                    {"_id": _fb_su["_id"]},
                    {"$set": {
                        "customer_feedback":    text,
                        "customer_feedback_at": now,
                        "updated_at":           now,
                    }}
                )
                log.info(f"[TELNYX WEBHOOK] ✓ Saved customer_feedback on {_fb_collection} {_fb_su['_id']}")
                return {
                    "success":    True,
                    "message":    "Stored as customer_feedback",
                    "collection": _fb_collection,
                    "su_id":      str(_fb_su["_id"]),
                }, 200

            log.warning(f"[TELNYX WEBHOOK] No record found to attach feedback for phone={phone}")
            return {"success": True, "message": "No actionable response, no record found"}, 200

        # ── Resolve shifts_users / shifts_group_users record ──────────────────
        # Match priority:
        # 1. sms_phone  (most reliable — set at send time)
        # 2. user_id    (fallback)
        su         = None
        collection = "shifts_users"

        # 1. Match by sms_phone
        su = app.db.shifts_group_users.find_one(
            {"sms_phone": f"+{phone}", "sms_sent": 1},
            sort=[("sms_sent_at", -1)]
        )
        if su:
            collection = "shifts_group_users"
            log.info(f"[TELNYX WEBHOOK] Matched sms_phone → shifts_group_users {su['_id']}")
        else:
            su = app.db.shifts_users.find_one(
                {"sms_phone": f"+{phone}", "sms_sent": 1},
                sort=[("sms_sent_at", -1)]
            )
            if su:
                log.info(f"[TELNYX WEBHOOK] Matched sms_phone → shifts_users {su['_id']}")

        # 2. Fallback by user_id
        if not su and user:
            su = app.db.shifts_group_users.find_one(
                {"user_id": user["_id"], "sms_sent": 1},
                sort=[("sms_sent_at", -1)]
            )
            if su:
                collection = "shifts_group_users"
                log.info(f"[TELNYX WEBHOOK] Fallback user_id → shifts_group_users {su['_id']}")
            else:
                su = app.db.shifts_users.find_one(
                    {"user_id": user["_id"], "sms_sent": 1},
                    sort=[("sms_sent_at", -1)]
                )
                if su:
                    log.info(f"[TELNYX WEBHOOK] Fallback user_id → shifts_users {su['_id']}")

        if not su:
            log.warning(f"[TELNYX WEBHOOK] No record found for phone={phone}")
            return {"success": True, "message": "No record found"}, 200

        # ── Update availability ───────────────────────────────────────────────
        log.info(f"[TELNYX WEBHOOK] Updating su_id={su['_id']} in {collection} → availability={avail}")
        db_col  = getattr(app.db, collection)
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        _set_fields = {
            "availability":  avail,
            "response_text": "Yes, I'm available." if avail == 1 else "No, thanks.",
            "response_time": now_str,
            "responded_at":  now,
            "updated_at":    now,
            "sms_response":  text,
        }

        result = db_col.update_one({"_id": su["_id"]}, {"$set": _set_fields})
        log.info(f"[TELNYX WEBHOOK] ✓ Updated {result.modified_count} record(s) → availability={avail}")

        # Auto-end outreach
        threading.Thread(
            target=_check_and_end_outreach,
            args=(app, str(su.get("shift_id", "")), str(su.get("outreach_id", "")), collection),
            daemon=True
        ).start()

        return {
            "success":      True,
            "availability": avail,
            "collection":   collection,
            "su_id":        str(su["_id"]),
        }, 200