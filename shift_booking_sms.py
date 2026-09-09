# shift_booking_sms.py
# Sends SMS messages via Telinex for shifts where channel == 'SMS'
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

ALLOWED_START_HOUR = 8   # SMS-friendly window (daytime only)
ALLOWED_END_HOUR   = 21
BATCH_SIZE         = 10

# ── Telinex credentials (set in environment) ───────────────────────────────────
# TELINEX_API_URL      e.g. https://api.telinex.com
# TELINEX_API_KEY      your API key / bearer token
# TELINEX_SENDER_ID    your registered sender ID / number
# TELINEX_WEBHOOK_SECRET  optional secret for webhook validation


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


def _build_sms_body(first_name, shift_doc) -> str:
    """
    Compose the SMS text body.
    Keep it short — SMS bodies should ideally stay under 160 chars per segment.
    Adjust wording to match whatever copy your team uses.
    """
    facility = shift_doc.get("client_name") or shift_doc.get("location") or "the facility"
    county   = shift_doc.get("client_county") or "Ireland"
    unit     = shift_doc.get("unit") or "-"
    date_str = _format_date(shift_doc.get("date", ""))
    start    = shift_doc.get("start_time") or "TBC"
    end      = shift_doc.get("end_time")   or "TBC"
    _rate    = shift_doc.get("rate", "")
    rate     = "REG" if not _rate or str(_rate) in ("0", "0.0", "") else str(_rate)

    body = (
        f"Hi {first_name}, shift available – Co. {county}\n"
        f"Facility: {facility}\n"
        f"Unit: {unit}\n"
        f"Date: {date_str}\n"
        f"Time: {start} – {end}\n"
        f"Rate: {rate}\n"
        f"Reply YES or NO"
    )
    return body


def _clean_phone(phone: str) -> str:
    """Normalise to E.164 digits only (no +, spaces or dashes)."""
    return phone.replace("+", "").replace(" ", "").replace("-", "").strip()


# ─────────────────────────────────────────────────────────────────────────────
# Telinex SMS send
# ─────────────────────────────────────────────────────────────────────────────

def _send_telinex_sms(app, record, shift_doc, phone, first_name, su_id, collection="shifts_users"):
    """Send an SMS via the Telinex API and update the DB record."""
    db_col = getattr(app.db, collection)
    try:
        telinex_url    = (os.getenv("TELINEX_API_URL") or "").rstrip("/")
        telinex_key    = os.getenv("TELINEX_API_KEY", "")
        sender_id      = os.getenv("TELINEX_SENDER_ID", "ExpressHealth")

        if not telinex_url or not telinex_key:
            log.error("[SMS] TELINEX_API_URL or TELINEX_API_KEY not set")
            return

        phone_clean = _clean_phone(phone)
        body        = _build_sms_body(first_name, shift_doc)

        # ── Telinex send-message payload ──────────────────────────────────────
        # Adjust field names below if Telinex uses different keys.
        # Common Telinex REST payload:
        #   POST /api/sms/send
        #   { "to": "353...", "from": "SenderID", "message": "..." }
        payload = {
            "to":      phone_clean,
            "from":    sender_id,
            "message": body,
            # Optional: include a reference ID so the webhook can match replies
            "reference": str(su_id),
        }

        headers = {
            "Authorization": f"Bearer {telinex_key}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        }

        send_url = f"{telinex_url}/api/sms/send"
        log.info(f"[SMS] Sending to {phone_clean} via {send_url}")

        resp = _req.post(send_url, json=payload, headers=headers, timeout=20)

        if resp.status_code in (200, 201):
            log.info(f"[SMS] ✓ Sent to {phone_clean}")
            try:
                resp_data = resp.json()
            except ValueError:
                resp_data = {"raw_response": resp.text}

            # Telinex typically returns a message ID — adjust key as needed
            message_id = (
                resp_data.get("messageId")
                or resp_data.get("message_id")
                or resp_data.get("id")
                or ""
            )

            db_col.update_one(
                {"_id": su_id},
                {"$set": {
                    "sms_sent":       1,
                    "sms_sent_at":    datetime.utcnow(),
                    "sms_message_id": str(message_id),
                    "sms_phone":      phone_clean,
                    "availability":   8,           # 8 = "Sent, awaiting reply"
                    "updated_at":     datetime.utcnow(),
                }}
            )
        else:
            log.error(f"[SMS] ✗ Failed {phone_clean}: {resp.status_code} {resp.text[:200]}")
            db_col.update_one(
                {"_id": su_id},
                {"$set": {"sms_error": f"{resp.status_code}: {resp.text[:200]}"}}
            )

    except Exception as e:
        log.error(f"[SMS] ✗ Exception for {phone}: {e}")
        db_col.update_one(
            {"_id": su_id},
            {"$set": {"sms_error": str(e)}}
        )


# ─────────────────────────────────────────────────────────────────────────────
# Shift doc builder  (identical to WhatsApp version)
# ─────────────────────────────────────────────────────────────────────────────

def _get_shift_doc(app, record):
    shift_id = record.get("shift_id")
    group_id = record.get("group_id")
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

            # Mark processed before background send (availability=7 = "Not Sent yet")
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
                continue   # already processed by another worker

            shift_doc = _get_shift_doc(app, record)

            threading.Thread(
                target=_send_telinex_sms,
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

    # ── Regular shifts_users (single-shift outreach) ──────────────────────────
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
            "debug":           "shift_booking_sms.py loaded",
            "server_time":     now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "batch_size":      BATCH_SIZE,
            "pending_regular": pending_su,
            "pending_group":   pending_sgu,
            "telinex_url":     os.getenv("TELINEX_API_URL", "not set"),
            "sender_id":       os.getenv("TELINEX_SENDER_ID", "not set"),
        })


# ─────────────────────────────────────────────────────────────────────────────
# Telinex inbound SMS webhook
# ─────────────────────────────────────────────────────────────────────────────

def register_telinex_webhook_routes(app):

    @app.route('/telinex/webhook', methods=['POST'])
    def telinex_webhook():
        """
        Telinex delivers inbound SMS (replies) to this endpoint.
        Configure in Telinex dashboard → Settings → Inbound Webhook URL:
            https://yourapp.com/telinex/webhook

        Expected payload (Telinex standard MO format — adjust if different):
        {
            "messageId":  "abc123",
            "from":       "353871234567",
            "to":         "353894618556",
            "message":    "YES",
            "reference":  "<su_id from outbound>",
            "receivedAt": "2025-01-01T12:00:00Z"
        }
        """
        try:
            data = request.get_json(silent=True) or {}
        except Exception:
            data = {}

        log.info(f"[TELINEX WEBHOOK] Received: {data}")

        # ── Extract core fields ───────────────────────────────────────────────
        # Normalise key names — Telinex may use camelCase or snake_case
        phone      = (data.get("from") or data.get("sender") or "").replace("+", "").strip()
        message_id = data.get("messageId") or data.get("message_id") or ""
        reference  = data.get("reference") or data.get("ref") or ""   # su_id we sent
        body       = (data.get("message") or data.get("text") or data.get("body") or "").strip()
        received_at = data.get("receivedAt") or data.get("received_at") or ""

        now = datetime.utcnow()

        # ── Persist raw inbound to DB ─────────────────────────────────────────
        msg_doc = {
            "phone":       phone,
            "message_id":  message_id,
            "reference":   reference,
            "body":        body,
            "direction":   "inbound",
            "channel":     "SMS",
            "raw":         {k: str(v)[:500] for k, v in data.items()},
            "timestamp":   now,
        }

        # Try to link to a user
        user = None
        if phone:
            user = app.db.users.find_one(
                {"phone": {"$regex": phone[-9:] if len(phone) >= 9 else phone, "$options": "i"}},
                {"_id": 1}
            )
        if user:
            msg_doc["user_id"] = user["_id"]

        app.db.telinex_messages.insert_one(msg_doc)

        if not phone:
            log.warning("[TELINEX WEBHOOK] No sender phone in payload")
            return {"success": True, "message": "No phone"}, 200

        # ── Parse YES / NO ────────────────────────────────────────────────────
        reply_text = body.lower().strip()
        avail = None
        if reply_text in ("yes", "y", "1") or "yes" in reply_text or "available" in reply_text:
            avail = 1
        elif reply_text in ("no", "n", "0") or "no" in reply_text or "not available" in reply_text:
            avail = 0

        # ── Non-standard reply → store as customer_feedback ──────────────────
        if avail is None:
            log.info(f"[TELINEX WEBHOOK] Non-standard reply — storing as customer_feedback. phone={phone} body={body!r}")

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
                        "customer_feedback":    body,
                        "customer_feedback_at": now,
                        "updated_at":           now,
                    }}
                )
                log.info(f"[TELINEX WEBHOOK] ✓ Saved customer_feedback on {_fb_collection} {_fb_su['_id']}")
                return {
                    "success":    True,
                    "message":    "Stored as customer_feedback",
                    "collection": _fb_collection,
                    "su_id":      str(_fb_su["_id"]),
                }, 200

            log.warning(f"[TELINEX WEBHOOK] No record found to attach feedback for phone={phone}")
            return {"success": True, "message": "No actionable response, no record found"}, 200

        # ── Resolve the shifts_users / shifts_group_users record ──────────────
        su         = None
        collection = "shifts_users"

        # 1. Prefer match by reference (su_id we embedded in outbound reference field)
        if reference and ObjectId.is_valid(reference):
            ref_oid = ObjectId(reference)
            su = app.db.shifts_group_users.find_one({"_id": ref_oid, "sms_sent": 1})
            if su:
                collection = "shifts_group_users"
                log.info(f"[TELINEX WEBHOOK] Matched by reference → shifts_group_users {su['_id']}")
            else:
                su = app.db.shifts_users.find_one({"_id": ref_oid, "sms_sent": 1})
                if su:
                    log.info(f"[TELINEX WEBHOOK] Matched by reference → shifts_users {su['_id']}")

        # 2. Fallback: match by sms_phone
        if not su:
            su = app.db.shifts_group_users.find_one(
                {"sms_phone": phone, "sms_sent": 1},
                sort=[("sms_sent_at", -1)]
            )
            if su:
                collection = "shifts_group_users"
                log.info(f"[TELINEX WEBHOOK] Fallback sms_phone → shifts_group_users {su['_id']}")
            else:
                su = app.db.shifts_users.find_one(
                    {"sms_phone": phone, "sms_sent": 1},
                    sort=[("sms_sent_at", -1)]
                )
                if su:
                    log.info(f"[TELINEX WEBHOOK] Fallback sms_phone → shifts_users {su['_id']}")

        # 3. Fallback: match by user_id
        if not su and user:
            su = app.db.shifts_group_users.find_one(
                {"user_id": user["_id"], "sms_sent": 1},
                sort=[("sms_sent_at", -1)]
            )
            if su:
                collection = "shifts_group_users"
                log.info(f"[TELINEX WEBHOOK] Fallback user_id → shifts_group_users {su['_id']}")
            else:
                su = app.db.shifts_users.find_one(
                    {"user_id": user["_id"], "sms_sent": 1},
                    sort=[("sms_sent_at", -1)]
                )
                if su:
                    log.info(f"[TELINEX WEBHOOK] Fallback user_id → shifts_users {su['_id']}")

        if not su:
            log.warning(f"[TELINEX WEBHOOK] No record found for phone={phone}")
            return {"success": True, "message": "No record found"}, 200

        # ── Update availability ───────────────────────────────────────────────
        log.info(f"[TELINEX WEBHOOK] Updating su_id={su['_id']} in {collection} → availability={avail}")
        db_col  = getattr(app.db, collection)
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        _set_fields = {
            "availability":  avail,
            "response_text": "Yes, I'm available." if avail == 1 else "No, thanks.",
            "response_time": now_str,
            "responded_at":  now,
            "updated_at":    now,
            "sms_response":  body,
        }

        result = db_col.update_one({"_id": su["_id"]}, {"$set": _set_fields})
        log.info(f"[TELINEX WEBHOOK] ✓ Updated {result.modified_count} record(s) → availability={avail}")

        # Auto-end outreach after reply
        _wh_shift_id    = str(su.get("shift_id", ""))
        _wh_outreach_id = str(su.get("outreach_id", ""))
        threading.Thread(
            target=_check_and_end_outreach,
            args=(app, _wh_shift_id, _wh_outreach_id, collection),
            daemon=True
        ).start()

        return {
            "success":    True,
            "availability": avail,
            "collection": collection,
            "su_id":      str(su["_id"]),
        }, 200

    # ── Delivery receipt (optional — Telinex DLR) ─────────────────────────────
    @app.route('/telinex/dlr', methods=['POST'])
    def telinex_dlr():
        """
        Telinex delivery receipt webhook.
        Configure in Telinex dashboard → Settings → DLR Webhook URL:
            https://yourapp.com/telinex/dlr

        Typical payload:
        {
            "messageId": "abc123",
            "status":    "DELIVERED",   // DELIVERED | FAILED | PENDING
            "reference": "<su_id>",
            "timestamp": "2025-01-01T12:01:00Z"
        }
        """
        try:
            data = request.get_json(silent=True) or {}
        except Exception:
            data = {}

        log.info(f"[TELINEX DLR] {data}")

        message_id = data.get("messageId") or data.get("message_id") or ""
        status     = (data.get("status") or "").upper()
        reference  = data.get("reference") or ""

        if not reference and not message_id:
            return {"success": True, "message": "No reference"}, 200

        # Locate the record
        su         = None
        collection = "shifts_users"

        if reference and ObjectId.is_valid(reference):
            ref_oid = ObjectId(reference)
            su = app.db.shifts_group_users.find_one({"_id": ref_oid})
            if su:
                collection = "shifts_group_users"
            else:
                su = app.db.shifts_users.find_one({"_id": ref_oid})

        if not su and message_id:
            su = app.db.shifts_group_users.find_one({"sms_message_id": message_id})
            if su:
                collection = "shifts_group_users"
            else:
                su = app.db.shifts_users.find_one({"sms_message_id": message_id})

        if not su:
            log.warning(f"[TELINEX DLR] No record for reference={reference} messageId={message_id}")
            return {"success": True}, 200

        db_col = getattr(app.db, collection)
        db_col.update_one(
            {"_id": su["_id"]},
            {"$set": {
                "sms_dlr_status": status,
                "sms_dlr_at":     datetime.utcnow(),
                "updated_at":     datetime.utcnow(),
            }}
        )
        log.info(f"[TELINEX DLR] ✓ su_id={su['_id']} status={status}")
        return {"success": True, "status": status}, 200