# shift_booking_whatsapp.py
# Sends WhatsApp messages via WATI for shifts where channel == 'WhatsApp'
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

ALLOWED_START_HOUR = 1
ALLOWED_END_HOUR   = 23
BATCH_SIZE         = 10
WATI_TEMPLATE_NAME = "shift_kiran"


def is_within_call_window():
    now     = datetime.utcnow()
    hour    = now.hour
    allowed = ALLOWED_START_HOUR <= hour < ALLOWED_END_HOUR
    log.info(f"[WA TIME CHECK] {now.strftime('%Y-%m-%d %H:%M:%S UTC')} → Hour {hour} → Allowed: {allowed}")
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
                {"$set": {"outreach_status": 3, "ended_at": datetime.utcnow(),
                          "updated_at": datetime.utcnow(), "end_reason": "all_wa_processed"}}
            )
            log.info(f"[WA END CHECK] Outreach {outreach_id} ended — all {total} records processed in {collection}")
    except Exception as e:
        log.error(f"[WA END CHECK] Error: {e}")
 
 


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
        unit     = shift_doc.get("unit", "") or ""
        date_str = _format_date(shift_doc.get("date", ""))
        start    = shift_doc.get("start_time", "")
        end      = shift_doc.get("end_time", "")
        _rate    = shift_doc.get("rate", "")
        rate     = "REG" if not _rate or str(_rate) in ("0", "0.0", "") else str(_rate)

        # WATI template parameters — order matches template placeholders
        # Template: shift_kiran
        # Shift Availability – Co. {{county}}
        # Facility: {{facility}}
        # Unit: {{unit}}
        # Date: {{date}}
        # Time: {{from_time}} – {{to_time}}
        # Rate: {{rate}}
        def _nz(v, fallback):
            s = str(v or "").strip()
            return s if s else fallback

        parameters = [
            {"name": "county",    "value": _nz(county, "Ireland")},
            {"name": "facility",  "value": _nz(facility, "the facility")},
            {"name": "unit",      "value": _nz(unit, "-")},
            {"name": "date",      "value": _nz(date_str, "TBC")},
            {"name": "from_time", "value": _nz(start, "TBC")},
            {"name": "to_time",   "value": _nz(end, "TBC")},
            {"name": "rate",      "value": _nz(rate, "REG")},
        ]
        log.info(f"[GROUP WA] params={parameters} phone={phone_clean}")

        payload = {
            "template_name": WATI_TEMPLATE_NAME,
            "broadcast_name": f"shift_{str(su_id)}",
            "parameters": parameters,
            "channel_number": "353894618556"
        }

        headers = {
            "Authorization": f"Bearer {wati_token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        }
        

        # Build URL — if endpoint already has /api in it, use as-is
        _wati_send_url = f"{wati_url}/api/v2/sendTemplateMessage?whatsappNumber={phone_clean}" if "/api" not in wati_url else f"{wati_url}/v1/sendTemplateMessage?whatsappNumber={phone_clean}"
        resp = _req.post(
            _wati_send_url,
            json=payload,
            headers=headers,
            timeout=20,
        )

        

        

        if resp.status_code == 200:
           log.info(f"[WA] ✓ Sent to {phone_clean}")

           try:
               resp_data = resp.json()
           except ValueError:
               resp_data = {"raw_response": resp.text}

           # Get localMessageId from WATI response
           local_message_id = ""

           receivers = resp_data.get("receivers", [])
           
           if receivers:
               local_message_id = resp_data["receivers"][0]["localMessageId"]

           log.info(
               f"[WA] localMessageId={local_message_id} "
               f"phone={phone_clean}"
           )

           db_col = getattr(app.db, collection)

           db_col.update_one(
        {"_id": su_id},
        {"$set": {
            "wa_sent": 1,
            "wa_sent_at": datetime.utcnow(),
            "wa_message_id": resp_data.get("id", ""),
            "wa_conversation_id": resp_data.get("conversationId", ""),
            "localMessageId": local_message_id,
            "wa_phone": phone_clean,
            "availability": 8,
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

#             response = _send_wati_whatsapp(
#     current_app._get_current_object(),
#     record,
#     shift_doc,
#     phone,
#     first_name,
#     su_id,
#     collection_name
# )
#             return jsonify(response), 200
            
            threading.Thread(
                target=_send_wati_whatsapp,
                args=(current_app._get_current_object(), record, shift_doc,
                      phone, first_name, su_id, collection_name),
                daemon=True
            ).start()

            # ── Auto-end outreach if all records processed ────────────────
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

        # Extract shift_index from broadcast payload — e.g. group_shift_{su_id}_1
        _broadcast_link_id = ""
        _shift_index       = None
        try:
            import json as _json
            _payload_str = btn_reply.get("payload", "") or ""
            if _payload_str:
                _p = _json.loads(_payload_str) if isinstance(_payload_str, str) else _payload_str
                _broadcast_link_id = str(_p.get("BroadcastLinkId", ""))
        except Exception:
            pass
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

        # Only process availability when it's a button click (type=button or has buttonReply)
                # Also treat explicit reply events; text may arrive in other fields
        reply_text = (
            btn_text
            or text
            or data.get("replyText")
            or data.get("buttonText")
            or (wa_message.get("text") if isinstance(wa_message, dict) else "")
            or (wa_message.get("body") if isinstance(wa_message, dict) else "")
            or ""
        )
        reply_text = str(reply_text).strip()

        is_button_event = (
            msg_type == "button"
            or bool(btn_reply)
            or bool(reply_text)
            or event_type in (
                "message",
                "ctaButtonClicked",
                "CTA Button Clicked",
                "sentMessageREPLIED_v2",
                "button",
            )
        )

        if not is_button_event:
            log.info(f"[WATI WEBHOOK] Skipping non-button event: eventType={event_type} type={msg_type}")
            return {"success": True, "message": f"Skipped event: {event_type}"}, 200

        # Determine availability from button clicked
        combined = (reply_text or btn_text or text or "").strip().lower()
        avail = None
        if "yes" in combined or "i'm available" in combined or "im available" in combined:
            avail = 1
        elif "no" in combined or "not available" in combined or "thanks" in combined:
            avail = 0

        if not phone:
            log.info(f"[WATI WEBHOOK] No phone. eventType={event_type}")
            return {"success": True, "message": "No phone"}, 200

        if avail is None:
            log.info(f"[WATI WEBHOOK] Non-standard reply — storing as customer_feedback. phone={phone} reply_text={reply_text!r}")

            _fb_su = None
            _fb_collection = "shifts_users"

            if user:
                _fb_sgu = app.db.shifts_group_users.find_one(
                    {"user_id": user["_id"], "wa_sent": 1},
                    sort=[("wa_sent_at", -1)]
                )
                _fb_su_reg = app.db.shifts_users.find_one(
                    {"user_id": user["_id"], "wa_sent": 1},
                    sort=[("wa_sent_at", -1)]
                )

                # Pick whichever was sent more recently
                if _fb_sgu and _fb_su_reg:
                    sgu_time = _fb_sgu.get("wa_sent_at") or datetime.min
                    reg_time = _fb_su_reg.get("wa_sent_at") or datetime.min
                    if sgu_time >= reg_time:
                        _fb_su = _fb_sgu
                        _fb_collection = "shifts_group_users"
                    else:
                        _fb_su = _fb_su_reg
                        _fb_collection = "shifts_users"
                elif _fb_sgu:
                    _fb_su = _fb_sgu
                    _fb_collection = "shifts_group_users"
                elif _fb_su_reg:
                    _fb_su = _fb_su_reg
                    _fb_collection = "shifts_users"

            if _fb_su:
                _fb_col = getattr(app.db, _fb_collection)
                _fb_col.update_one(
                    {"_id": _fb_su["_id"]},
                    {"$set": {
                        "customer_feedback": combined,
                        "customer_feedback_at": datetime.utcnow(),
                        "updated_at": datetime.utcnow(),
                    }}
                )
                log.info(f"[WATI WEBHOOK] ✓ Saved customer_feedback on {_fb_collection} {_fb_su['_id']}")
                return {"success": True, "message": "Stored as customer_feedback", "collection": _fb_collection, "su_id": str(_fb_su["_id"])}, 200

            log.warning(f"[WATI WEBHOOK] No record found to attach feedback for phone={phone}")
            return {"success": True, "message": "No actionable response, no record found"}, 200

        now = datetime.utcnow()
        log.info(f"[WATI WEBHOOK] Processing — phone={phone} avail={avail} btn={btn_text} text={text}")

        # Find shifts_users by user already resolved above
        if not user:
            log.warning(f"[WATI WEBHOOK] No user found for phone {phone}")
            return {"success": True}, 200

        log.info(f"[WATI WEBHOOK] User found: {user['_id']}")


        su         = None
        collection = "shifts_users"
        local_message_id = None

        # ---------- Option 2: Get localMessageId via BroadcastLinkId ----------
        if _broadcast_link_id:
            original_msg = app.db.wati_messages.find_one(
                {
                    "event_type": "templateMessageSent_v2",
                    "raw.id": _broadcast_link_id
                },
                sort=[("timestamp", -1)]
            )
            if original_msg:
                local_message_id = (
                    original_msg.get("raw", {}).get("localMessageId")
                    or original_msg.get("localMessageId")
                )
                log.info(f"[WATI WEBHOOK] Found localMessageId={local_message_id} from BroadcastLinkId={_broadcast_link_id}")

        # ---------- 1. Prefer match by localMessageId ----------
        if local_message_id:
            su = app.db.shifts_group_users.find_one(
                {"localMessageId": local_message_id, "wa_sent": 1}
            )
            if su:
                collection = "shifts_group_users"
                log.info(f"[WATI WEBHOOK] Matched by localMessageId in shifts_group_users → {su['_id']}")
            else:
                su = app.db.shifts_users.find_one(
                    {"localMessageId": local_message_id, "wa_sent": 1}
                )
                if su:
                    log.info(f"[WATI WEBHOOK] Matched by localMessageId in shifts_users → {su['_id']}")

        # ---------- 2. Fallback by phone ----------
        if not su:
            su = app.db.shifts_group_users.find_one(
                {"wa_phone": phone, "wa_sent": 1},
                sort=[("wa_sent_at", -1)]
            )
            if su:
                collection = "shifts_group_users"
                log.info(f"[WATI WEBHOOK] Fallback wa_phone → shifts_group_users {su['_id']}")
            else:
                su = app.db.shifts_users.find_one(
                    {"wa_phone": phone, "wa_sent": 1},
                    sort=[("wa_sent_at", -1)]
                )
                if su:
                    log.info(f"[WATI WEBHOOK] Fallback wa_phone → shifts_users {su['_id']}")

        # ---------- 3. Fallback by user_id ----------
        if not su and user:
            su = app.db.shifts_group_users.find_one(
                {"user_id": user["_id"], "wa_sent": 1},
                sort=[("wa_sent_at", -1)]
            )
            if su:
                collection = "shifts_group_users"
                log.info(f"[WATI WEBHOOK] Fallback user_id → shifts_group_users {su['_id']}")
            else:
                su = app.db.shifts_users.find_one(
                    {"user_id": user["_id"], "wa_sent": 1},
                    sort=[("wa_sent_at", -1)]
                )
                if su:
                    log.info(f"[WATI WEBHOOK] Fallback user_id → shifts_users {su['_id']}")


        if not su:
            log.warning(f"[WATI WEBHOOK] No record found for phone={phone} user={user['_id']}")
            return {"success": True}, 200

        log.info(f"[WATI WEBHOOK] Updating su_id={su['_id']} in {collection} → availability={avail}")
        db_col = getattr(app.db, collection)
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")

        _set_fields = {
            "availability":  avail,
            "response_text": "Yes, I'm available." if avail == 1 else "No, thanks.",
            "response_time": now_str,
            "responded_at":  now,
            "updated_at":    now,
            "wa_response":   btn_text or text,
        }

                # For group outreach — match by localMessageId
        if collection == "shifts_group_users" and local_message_id:
            existing    = su.get("availability_details") or []
            new_details = list(existing)
            clicked_shift_id = None

            for ad in existing:
                ad_local_id = ad.get("local_message_id") or ad.get("localMessageId", "")
                if ad_local_id and ad_local_id == local_message_id:
                    clicked_shift_id = ad.get("shift_id")
                    log.info(f"[WATI WEBHOOK] Matched shift_id={clicked_shift_id} via localMessageId={local_message_id}")
                    break

            if clicked_shift_id:
                for ad in new_details:
                    if str(ad.get("shift_id", "")) == str(clicked_shift_id):
                        ad["availability"] = avail
                        ad["responded_at"] = now_str
                        break
                _set_fields["availability_details"] = new_details
                log.info(f"[WATI WEBHOOK] Updated shift_id={clicked_shift_id} → availability={avail}")
            else:
                log.info(f"[WATI WEBHOOK] No localMessageId match for {local_message_id} — skipping availability_details")

        result = db_col.update_one({"_id": su["_id"]}, {"$set": _set_fields})
        log.info(f"[WATI WEBHOOK] ✓ Updated {result.modified_count} record(s) → availability={avail}")

        # ── Auto-end outreach after webhook response ──────────────────
        _wh_shift_id    = str(su.get("shift_id", ""))
        _wh_outreach_id = str(su.get("outreach_id", ""))
        threading.Thread(
            target=_check_and_end_outreach,
            args=(app, _wh_shift_id, _wh_outreach_id, collection),
            daemon=True
        ).start()

        return {"success": True, "availability": avail, "collection": collection, "su_id": str(su["_id"])}, 200