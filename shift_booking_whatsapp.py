# shift_booking_whatsapp.py
# Sends WhatsApp messages via WATI for shifts where channel == 'WhatsApp'
# Processes up to 10 pending messages per trigger call
import logging
import os
import requests as _req
from flask import current_app, jsonify, request
from bson import ObjectId
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ALLOWED_START_HOUR = 0
ALLOWED_END_HOUR   = 23
BATCH_SIZE         = 10
WATI_TEMPLATE_NAME = "shift_call_new"


def is_within_call_window():
    now     = datetime.utcnow()
    hour    = now.hour
    allowed = ALLOWED_START_HOUR <= hour < ALLOWED_END_HOUR
    log.info(f"[WA TIME CHECK] {now.strftime('%Y-%m-%d %H:%M:%S UTC')} → Hour {hour} → Allowed: {allowed}")
    return allowed, now


def _format_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(str(date_str).split(" ")[0].split("T")[0], "%Y-%m-%d")
        return dt.strftime("%A, %d %B %Y")
    except Exception:
        return str(date_str)


def _format_day(date_str: str) -> str:
    try:
        dt = datetime.strptime(str(date_str).split(" ")[0].split("T")[0], "%Y-%m-%d")
        return dt.strftime("%A")
    except Exception:
        return ""


def _send_wati_whatsapp(app, record, shift_doc, phone, first_name, su_id, collection="shifts_users"):
    """Send WhatsApp message via WATI API."""
    try:
        wati_url   = (os.getenv("WATI_API_ENDPOINT") or os.getenv("WATI_API_URL", "")).rstrip("/")
        wati_token = os.getenv("WATI_ACCESS_TOKEN") or os.getenv("WATI_API_TOKEN", "")

        if not wati_url or not wati_token:
            log.error("[WA] WATI_API_ENDPOINT or WATI_ACCESS_TOKEN not set")
            return

        # Clean phone — remove + and spaces
        phone_clean = phone.replace("+", "").replace(" ", "").replace("-", "").strip()

        facility = shift_doc.get("client_name", "") or shift_doc.get("location", "")
        county   = shift_doc.get("client_county", "") or ""
        date_str = _format_date(shift_doc.get("date", ""))
        day_str  = _format_day(shift_doc.get("date", ""))
        start    = shift_doc.get("start_time", "")
        end      = shift_doc.get("end_time", "")
        _rate    = shift_doc.get("rate", "")
        rate     = "REG" if not _rate or str(_rate) in ("0", "0.0", "") else str(_rate)

        # WATI template parameters — order matches template placeholders
        parameters = [
            {"name": "name",     "value": first_name},
            {"name": "facility", "value": facility},
            {"name": "county",   "value": county},
            {"name": "day",      "value": day_str},
            {"name": "date",     "value": date_str},
            {"name": "start",    "value": start},
            {"name": "end",      "value": end},
            {"name": "rate",     "value": rate},
        ]

        payload = {
            "template_name": WATI_TEMPLATE_NAME,
            "broadcast_name": f"shift_{str(su_id)}",
            "parameters": parameters,
        }

        headers = {
            "Authorization": f"Bearer {wati_token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        }

        # Build URL — if endpoint already has /api in it, use as-is
        _wati_send_url = f"{wati_url}/api/v1/sendTemplateMessage?whatsappNumber={phone_clean}" if "/api" not in wati_url else f"{wati_url}/v1/sendTemplateMessage?whatsappNumber={phone_clean}"
        resp = _req.post(
            _wati_send_url,
            json=payload,
            headers=headers,
            timeout=20,
        )

        if resp.status_code == 200:
            log.info(f"[WA] ✓ Sent to {phone_clean}")
            resp_data = resp.json()
            db_col = getattr(app.db, collection)
            db_col.update_one(
                {"_id": su_id},
                {"$set": {
                    "wa_sent":          1,
                    "wa_sent_at":       datetime.utcnow(),
                    "wa_message_id":    resp_data.get("id", ""),
                    "wa_conversation_id": resp_data.get("conversationId", ""),
                    "availability":     8,
                }}
            )
        else:
            log.error(f"[WA] ✗ Failed {phone_clean}: {resp.status_code} {resp.text[:200]}")
            db_col = getattr(app.db, collection)
            db_col.update_one(
                {"_id": su_id},
                {"$set": {"wa_error": f"{resp.status_code}: {resp.text[:200]}"}}
            )

    except Exception as e:
        log.error(f"[WA] ✗ Exception for {phone}: {e}")
        db_col = getattr(app.db, collection)
        db_col.update_one(
            {"_id": su_id},
            {"$set": {"wa_error": str(e)}}
        )


def _get_shift_doc(app, record):
    """Build shift_doc from shifts collection."""
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
                "client_name":  s.get("client_name", "") or s.get("location", ""),
                "location":     s.get("location", ""),
                "client_county": s.get("client_county", "") or (client.get("county", "") if client else ""),
                "date":         str(s.get("date", "")),
                "start_time":   s.get("start_time", ""),
                "end_time":     s.get("end_time", ""),
                "rate":         s.get("rate", ""),
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
                }
    return shift_doc


def register_shift_booking_whatsapp_routes(app):

    def _process_batch(query, collection_name):
        """Fetch and process up to BATCH_SIZE pending WhatsApp records."""
        allowed, server_time = is_within_call_window()
        response_base = {
            "server_time":    server_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
            "collection":     collection_name,
        }

        if not allowed:
            return jsonify({**response_base, "status": "outside_hours"}), 200

        db_col   = getattr(app.db, collection_name)
        records  = list(db_col.find(query, sort=[("assigned_at", 1)], limit=BATCH_SIZE))

        if not records:
            return jsonify({**response_base, "status": "no_pending",
                            "message": f"No pending WhatsApp in {collection_name}"}), 200

        triggered = []
        for record in records:
            su_id    = record["_id"]
            user_id  = record.get("user_id")

            user = None
            if user_id:
                user = app.db.users.find_one(
                    {"_id": user_id},
                    {"phone": 1, "first_name": 1, "last_name": 1}
                )

            if not user or not user.get("phone"):
                log.warning(f"[WA] No phone for su_id={su_id}")
                continue

            phone      = user["phone"]
            first_name = user.get("first_name", "")
            full_name  = f"{first_name} {user.get('last_name','')}".strip()

            # Mark processed + set availability=7 (Not Sent) before background send
            result = db_col.update_one(
                {"_id": su_id},
                {"$set": {"call_processed": 1, "call_processed_at": datetime.utcnow(),
                           "availability": 7, "updated_at": datetime.utcnow()}}
            )
            if result.modified_count == 0:
                continue

            shift_doc = _get_shift_doc(app, record)

            import threading
            threading.Thread(
                target=_send_wati_whatsapp,
                args=(current_app._get_current_object(), record, shift_doc,
                      phone, first_name, su_id, collection_name),
                daemon=True
            ).start()

            triggered.append({
                "su_id":             str(su_id),
                "user_id":           str(user_id),
                "staff_name":        full_name,
                "phone":             phone,
                "wa_message_id":     record.get("wa_message_id", ""),
                "wa_conversation_id": record.get("wa_conversation_id", ""),
                "availability":      record.get("availability", 7),
                "wa_response":       record.get("wa_response", ""),
                "responded_at":      str(record.get("responded_at", "")),
            })

        return jsonify({
            **response_base,
            "status":       "triggered",
            "triggered":    len(triggered),
            "batch_size":   BATCH_SIZE,
            "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "data":         triggered,
        }), 200

    # ── Regular shifts_users (single shift outreach) ──────────────────────────
    @app.route('/shift_booking_whatsapp', methods=['GET'])
    def shift_booking_whatsapp():
        user_id_param = request.args.get('user_id')
        if user_id_param:
            query = {"user_id": ObjectId(user_id_param), "call_processed": 0, "channel": "WhatsApp"}
        else:
            query = {"call_processed": 0, "call_enabled": 1, "channel": "WhatsApp"}
        return _process_batch(query, "shifts_users")

    # ── Group shifts_group_users (group outreach) ─────────────────────────────
    @app.route('/shift_group_booking_whatsapp', methods=['GET'])
    def shift_group_booking_whatsapp():
        user_id_param = request.args.get('user_id')
        if user_id_param:
            query = {"user_id": ObjectId(user_id_param), "call_processed": 0, "channel": "WhatsApp"}
        else:
            query = {"call_processed": 0, "call_enabled": 1, "channel": "WhatsApp"}
        return _process_batch(query, "shifts_group_users")

    # ── Debug ─────────────────────────────────────────────────────────────────
    @app.route('/debug-shift-booking-whatsapp')
    def debug_shift_booking_whatsapp():
        allowed, now = is_within_call_window()
        pending_su  = app.db.shifts_users.count_documents(
            {"call_processed": 0, "call_enabled": 1, "channel": "WhatsApp"}
        )
        pending_sgu = app.db.shifts_group_users.count_documents(
            {"call_processed": 0, "call_enabled": 1, "channel": "WhatsApp"}
        )
        return jsonify({
            "debug":            "shift_booking_whatsapp.py loaded",
            "server_time":      now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "template":         WATI_TEMPLATE_NAME,
            "batch_size":       BATCH_SIZE,
            "pending_regular":  pending_su,
            "pending_group":    pending_sgu,
            "wati_url":         os.getenv("WATI_API_URL", "not set"),
        })


def register_wati_webhook_routes(app):

    @app.route('/wati/webhook', methods=['POST'])
    def wati_webhook():
        """
        WATI sends button click responses here.
        Configure in WATI dashboard → Settings → Webhook URL:
        https://uat.expresshealth.ie/wati/webhook
        """
        try:
            data = request.get_json(silent=True) or {}
        except Exception:
            data = {}

        log.info(f"[WATI WEBHOOK] {data}")

        # Save raw event to wati_messages collection
        phone       = (data.get("waId") or data.get("phone") or "").replace("+", "").strip()
        event_type  = data.get("eventType", "") or data.get("type", "")
        msg_type    = data.get("type", "")
        # WATI sends button replies at top level (not nested in waMessage)
        btn_reply   = data.get("buttonReply") or {}
        btn_text    = btn_reply.get("text", "") or btn_reply.get("title", "")
        text        = data.get("text") or ""
        # Also check nested waMessage
        wa_message  = data.get("waMessage") or {}
        if not btn_text:
            nested_btn = wa_message.get("buttonReply") or {}
            btn_text   = nested_btn.get("text", "") or nested_btn.get("title", "")
        if not text:
            text = (wa_message.get("text") or wa_message.get("body") or "")
        direction   = "inbound" if (msg_type == "button" or event_type == "message") else "outbound"

        # Save to DB
        msg_doc = {
            "phone":        phone,
            "event_type":   event_type,
            "direction":    direction,
            "text":         btn_text or text,
            "button_text":  btn_text,
            "button_reply": btn_reply,
            "raw":          {k: str(v)[:500] for k, v in data.items()},
            "timestamp":    datetime.utcnow(),
        }

        # Link to user if phone matches
        user = app.db.users.find_one(
            {"phone": {"$regex": phone[-9:] if len(phone) >= 9 else phone, "$options": "i"}},
            {"_id": 1}
        )
        if user:
            msg_doc["user_id"] = user["_id"]

        app.db.wati_messages.insert_one(msg_doc)

        # Determine availability from button clicked
        btn_text_l  = btn_text.strip().lower()
        text_l      = text.strip().lower()
        combined    = btn_text_l or text_l
        avail = None
        if "yes" in combined or "available" in combined:
            avail = 1
        elif "no" in combined or "thanks" in combined:
            avail = 0

        if avail is None or not phone:
            return {"success": True, "message": "No actionable response"}, 200

        now = datetime.utcnow()

        # Find shifts_users by user already resolved above
        if not user:
            log.warning(f"[WATI WEBHOOK] No user found for phone {phone}")
            return {"success": True}, 200

        # Find most recent shifts_users for this user with wa_sent=1
        conversation_id = data.get("conversationId", "")

        # Find shifts_users by conversationId — most reliable
        su         = None
        collection = "shifts_users"
        if conversation_id:
            su = app.db.shifts_users.find_one(
                {"wa_conversation_id": conversation_id},
                sort=[("wa_sent_at", -1)]
            )
            if not su:
                su = app.db.shifts_group_users.find_one(
                    {"wa_conversation_id": conversation_id},
                    sort=[("wa_sent_at", -1)]
                )
                if su:
                    collection = "shifts_group_users"

        # Fallback — find by user phone + wa_sent
        if not su and user:
            su = app.db.shifts_users.find_one(
                {"user_id": user["_id"], "wa_sent": 1},
                sort=[("wa_sent_at", -1)]
            )
            if not su:
                su = app.db.shifts_group_users.find_one(
                    {"user_id": user["_id"], "wa_sent": 1},
                    sort=[("wa_sent_at", -1)]
                )
                if su:
                    collection = "shifts_group_users"

        if not su:
            log.warning(f"[WATI WEBHOOK] No pending shifts_users found for phone {phone}")
            return {"success": True}, 200

        db_col = getattr(app.db, collection)
        db_col.update_one(
            {"_id": su["_id"]},
            {"$set": {
                "availability":  avail,
                "response_text": "Yes, I'm available." if avail == 1 else "No, thanks.",
                "response_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                "responded_at":  now,
                "updated_at":    now,
                "wa_response":   button_text or text,
            }}
        )

        log.info(f"[WATI WEBHOOK] ✓ {phone} → availability={avail} in {collection}")
        return {"success": True, "availability": avail, "collection": collection}, 200
