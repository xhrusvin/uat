# shift_group_booking_whatsapp.py
# Sends WhatsApp messages via WATI for GROUP shifts (shifts_group_users collection)
# Processes up to 10 pending messages per trigger call
import logging
import os
import requests as _req
import threading
from flask import current_app, jsonify, request
from bson import ObjectId
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ALLOWED_START_HOUR = 1
ALLOWED_END_HOUR   = 23
BATCH_SIZE         = 10
WATI_TEMPLATE_NAME = "shift_call_new"


def is_within_call_window():
    now     = datetime.utcnow()
    hour    = now.hour
    allowed = ALLOWED_START_HOUR <= hour < ALLOWED_END_HOUR
    log.info(f"[GROUP WA TIME CHECK] {now.strftime('%Y-%m-%d %H:%M:%S UTC')} → Hour {hour} → Allowed: {allowed}")
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


def _send_wati_whatsapp_sync(app, shift_doc, phone, first_name, su_id, shift_index=0, shift_id="", delay=0):
    """Send WhatsApp synchronously and return WATI response."""
    try:
        if delay:
            import time as _time
            _time.sleep(delay)

        wati_url   = (os.getenv("WATI_API_ENDPOINT") or os.getenv("WATI_API_URL", "")).rstrip("/")
        wati_token = os.getenv("WATI_ACCESS_TOKEN") or os.getenv("WATI_API_TOKEN", "")

        if not wati_url or not wati_token:
            log.error("[GROUP WA] WATI_API_ENDPOINT or WATI_ACCESS_TOKEN not set")
            return None

        phone_clean = phone.replace("+", "").replace(" ", "").replace("-", "").strip()
        facility = shift_doc.get("client_name", "") or shift_doc.get("location", "")
        county   = shift_doc.get("client_county", "") or ""
        date_str = _format_date(shift_doc.get("date", ""))
        day_str  = _format_day(shift_doc.get("date", ""))
        start    = shift_doc.get("start_time", "")
        end      = shift_doc.get("end_time", "")
        _rate    = shift_doc.get("rate", "")
        rate     = "REG" if not _rate or str(_rate) in ("0", "0.0", "") else str(_rate)

        parameters = [
            {"name": "name",     "value": first_name or "there"},
            {"name": "facility", "value": facility or "the facility"},
            {"name": "county",   "value": county or "Ireland"},
            {"name": "day",      "value": day_str or "Today"},
            {"name": "date",     "value": date_str or "TBC"},
            {"name": "start",    "value": start or "TBC"},
            {"name": "end",      "value": end or "TBC"},
            {"name": "rate",     "value": rate or "REG"},
        ]

        payload = {
            "template_name":  WATI_TEMPLATE_NAME,
            "broadcast_name": f"group_shift_{str(su_id)}_{shift_index}",
            "parameters":     parameters,
        }
        headers = {
            "Authorization": f"Bearer {wati_token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        }
        _wati_send_url = f"{wati_url}/api/v2/sendTemplateMessage?whatsappNumber={phone_clean}"

        resp = _req.post(_wati_send_url, json=payload, headers=headers, timeout=20)
        resp_data = resp.json() if resp.status_code == 200 else {}
        _inner    = resp_data.get("data") or resp_data
        _receiver = (_inner.get("receivers") or [{}])[0] if _inner.get("receivers") else {}
        local_msg_id_log = _receiver.get("localMessageId", "") or resp_data.get("local_message_id", "")
        log.info(f"[GROUP WA] shift_index={shift_index} status={resp.status_code} localMessageId={local_msg_id_log}")

        if resp.status_code == 200:
            local_msg_id = _receiver.get("localMessageId", "")
            wati_id      = ""
            # Update base fields on first send only
            if shift_index == 0:
                app.db.shifts_group_users.update_one(
                    {"_id": su_id},
                    {"$set": {
                        "wa_sent":            1,
                        "wa_sent_at":         datetime.utcnow(),
                        "wa_message_id":      wati_id,
                        "wa_conversation_id": resp_data.get("conversationId", ""),
                        "wa_phone":           phone_clean,
                        "availability":       8,
                    }}
                )
            else:
                app.db.shifts_group_users.update_one(
                    {"_id": su_id},
                    {"$set": {"wa_sent": 1, "availability": 8}}
                )
            # Push to availability_details with localMessageId for per-shift tracking
            app.db.shifts_group_users.update_one(
                {"_id": su_id},
                {"$push": {
                    "availability_details": {
                        "shift_id":       shift_id,
                        "shift_index":    shift_index,
                        "localMessageId": local_msg_id,
                        "availability":   8,
                        "responded_at":   None,
                        "sent_at":        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                }}
            )
            log.info(f"[GROUP WA] ✓ Sent shift_index={shift_index} local_message_id={local_msg_id}")
            return resp_data
        else:
            log.error(f"[GROUP WA] ✗ Failed shift_index={shift_index}: {resp.status_code} {resp.text[:200]}")
            app.db.shifts_group_users.update_one(
                {"_id": su_id},
                {"$set": {"wa_error": f"shift_{shift_index}: {resp.status_code}: {resp.text[:200]}"}}
            )
            return None
    except Exception as e:
        log.error(f"[GROUP WA] ✗ Exception shift_index={shift_index}: {e}")
        return None
        wati_url   = (os.getenv("WATI_API_ENDPOINT") or os.getenv("WATI_API_URL", "")).rstrip("/")
        wati_token = os.getenv("WATI_ACCESS_TOKEN") or os.getenv("WATI_API_TOKEN", "")

        if not wati_url or not wati_token:
            log.error("[GROUP WA] WATI_API_ENDPOINT or WATI_ACCESS_TOKEN not set")
            return

        phone_clean = phone.replace("+", "").replace(" ", "").replace("-", "").strip()

        facility = shift_doc.get("client_name", "") or shift_doc.get("location", "")
        county   = shift_doc.get("client_county", "") or ""
        date_str = _format_date(shift_doc.get("date", ""))
        day_str  = _format_day(shift_doc.get("date", ""))
        start    = shift_doc.get("start_time", "")
        end      = shift_doc.get("end_time", "")
        unit     = shift_doc.get("unit", "") or ""
        _rate    = shift_doc.get("rate", "")
        rate     = "REG" if not _rate or str(_rate) in ("0", "0.0", "") else str(_rate)

        # Template: shift_call_new
        parameters = [
            {"name": "name",     "value": first_name or "there"},
            {"name": "facility", "value": facility or "the facility"},
            {"name": "county",   "value": county or "Ireland"},
            {"name": "day",      "value": day_str or "Today"},
            {"name": "date",     "value": date_str or "TBC"},
            {"name": "start",    "value": start or "TBC"},
            {"name": "end",      "value": end or "TBC"},
            {"name": "rate",     "value": rate or "REG"},
        ]

        # Unique broadcast name per shift to avoid WATI deduplication
        payload = {
            "template_name":  WATI_TEMPLATE_NAME,
            "broadcast_name": f"group_shift_{str(su_id)}_{shift_index}",
            "parameters":     parameters,
        }

        headers = {
            "Authorization": f"Bearer {wati_token}",
            "Content-Type":  "application/json",
            "Accept":        "application/json",
        }

        _wati_send_url = f"{wati_url}/api/v2/sendTemplateMessage?whatsappNumber={phone_clean}"

        resp = _req.post(_wati_send_url, json=payload, headers=headers, timeout=20)

        if resp.status_code == 200:
            log.info(f"[GROUP WA] ✓ Sent to {phone_clean} shift_index={shift_index}")
            resp_data = resp.json()
            wati_id   = resp_data.get("id", "")

            # Update base fields
            app.db.shifts_group_users.update_one(
                {"_id": su_id},
                {"$set": {
                    "wa_sent":            1,
                    "wa_sent_at":         datetime.utcnow(),
                    "wa_message_id":      wati_id,
                    "wa_conversation_id": resp_data.get("conversationId", ""),
                    "wa_phone":           phone_clean,
                    "availability":       8,
                }}
            )
            # Push shift record separately
            app.db.shifts_group_users.update_one(
                {"_id": su_id},
                {"$push": {
                    "wa_sent_shifts": {
                        "shift_id":       shift_id,
                        "shift_index":    shift_index,
                        "broadcast_name": f"group_shift_{str(su_id)}_{shift_index}",
                        "wati_id":        wati_id,
                        "sent_at":        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                }}
            )
            log.info(f"[GROUP WA] ✓ Saved wa_sent_shifts for shift_id={shift_id} wati_id={wati_id}")
        else:
            log.error(f"[GROUP WA] ✗ Failed {phone_clean}: {resp.status_code} {resp.text[:200]}")
            app.db.shifts_group_users.update_one(
                {"_id": su_id},
                {"$set": {"wa_error": f"{resp.status_code}: {resp.text[:200]}"}}
            )

    except Exception as e:
        log.error(f"[GROUP WA] ✗ Exception for {phone}: {e}")
        app.db.shifts_group_users.update_one(
            {"_id": su_id},
            {"$set": {"wa_error": str(e)}}
        )


def _get_shift_doc(app, record):
    """Build shift_doc from first shift in the group."""
    group_id = record.get("group_id")
    shift_id = record.get("shift_id")
    shift_doc = {}

    if group_id:
        sg = app.db.shifts_group.find_one({"_id": group_id}, {"shift_ids": 1})
        if sg and sg.get("shift_ids"):
            s = app.db.shifts.find_one({"_id": sg["shift_ids"][0]})
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
                    "unit":          s.get("unit") or "",
                    "user_type":     s.get("user_type", ""),
                    "rate":          s.get("rate", ""),
                }
    elif shift_id:
        s = app.db.shifts.find_one({"_id": shift_id})
        if s:
            shift_doc = {
                "client_name":   s.get("client_name", "") or s.get("location", ""),
                "location":      s.get("location", ""),
                "client_county": s.get("client_county", ""),
                "date":          str(s.get("date", "")),
                "start_time":    s.get("start_time", ""),
                "end_time":      s.get("end_time", ""),
                "unit":          s.get("unit") or "",
                "user_type":     s.get("user_type", ""),
                "rate":          s.get("rate", ""),
            }
    return shift_doc


def register_shift_group_booking_whatsapp_routes(app):

    @app.route('/shift_group_booking_whatsapp', methods=['GET'], endpoint='shift_group_booking_whatsapp_route')
    def shift_group_booking_whatsapp():
        allowed, server_time = is_within_call_window()
        user_id_param = request.args.get('user_id')

        response_base = {
            "server_time":    server_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
            "collection":     "shifts_group_users",
        }

        if not allowed:
            return jsonify({**response_base, "status": "outside_hours"}), 200

        if user_id_param:
            query = {"user_id": ObjectId(user_id_param), "call_processed": 0, "channel": "WhatsApp"}
        else:
            query = {"call_processed": 0, "call_enabled": 1, "channel": "WhatsApp"}

        records = list(app.db.shifts_group_users.find(
            query, sort=[("assigned_at", 1)], limit=BATCH_SIZE
        ))

        if not records:
            return jsonify({**response_base, "status": "no_pending",
                            "message": "No pending WhatsApp in shifts_group_users"}), 200

        triggered = []
        for record in records:
            su_id   = record["_id"]
            user_id = record.get("user_id")

            user = None
            if user_id:
                user = app.db.users.find_one(
                    {"_id": user_id},
                    {"phone": 1, "first_name": 1, "last_name": 1, "designation": 1}
                )

            if not user or not user.get("phone"):
                log.warning(f"[GROUP WA] No phone for su_id={su_id}")
                continue

            phone      = user["phone"]
            first_name = user.get("first_name", "")
            full_name  = f"{first_name} {user.get('last_name', '')}".strip()

            # Check designation matches shift user_type
            user_designation = (user.get("designation") or "").strip().lower()
            shift_doc = _get_shift_doc(app, record)
            shift_user_type = (shift_doc.get("user_type") or "").strip().lower()
            if user_designation and shift_user_type and user_designation != shift_user_type:
                log.warning(f"[GROUP WA] Skipping {phone} — designation '{user_designation}' != shift user_type '{shift_user_type}'")
                continue

            # Build shift_docs — one per shift in group
            group_id_rec = record.get("group_id")
            shift_docs    = []
            shift_id_list = []
            if group_id_rec:
                sg = app.db.shifts_group.find_one({"_id": group_id_rec}, {"shift_ids": 1})
                if sg and sg.get("shift_ids"):
                    for _sid in sg["shift_ids"]:
                        _s = app.db.shifts.find_one({"_id": _sid})
                        if _s:
                            _stype = (_s.get("user_type") or "").strip().lower()
                            if user_designation and _stype and user_designation != _stype:
                                continue
                            _client = None
                            if _s.get("client_id"):
                                _client = app.db.clients.find_one(
                                    {"xn_client_id": str(_s["client_id"])}, {"county": 1}
                                )
                            shift_docs.append({
                                "client_name":   _s.get("client_name", "") or _s.get("location", ""),
                                "location":      _s.get("location", ""),
                                "client_county": _s.get("client_county", "") or (_client.get("county", "") if _client else ""),
                                "date":          str(_s.get("date", "")),
                                "start_time":    _s.get("start_time", ""),
                                "end_time":      _s.get("end_time", ""),
                                "unit":          _s.get("unit") or "",
                                "user_type":     _s.get("user_type", ""),
                                "rate":          _s.get("rate", ""),
                            })
                            shift_id_list.append(str(_sid))
            if not shift_docs:
                shift_docs    = [shift_doc]
                shift_id_list = [str(record.get("group_id", ""))]

            log.info(f"[GROUP WA] Sending {len(shift_docs)} messages to {phone} for su_id={su_id}")

            def _send_all_shifts(app_obj, shift_docs_list, shift_id_list_, phone_, first_name_, su_id_):
                for _i, (_sdoc, _sid) in enumerate(zip(shift_docs_list, shift_id_list_)):
                    if _i > 0:
                        import time as _t
                        _t.sleep(5)
                    _send_wati_whatsapp_sync(app_obj, _sdoc, phone_, first_name_, su_id_, _i, _sid)

            threading.Thread(
                target=_send_all_shifts,
                args=(current_app._get_current_object(), shift_docs, shift_id_list,
                      phone, first_name, su_id),
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

    @app.route('/debug-shift-group-booking-whatsapp', endpoint='debug_shift_group_booking_whatsapp_route')
    def debug_shift_group_booking_whatsapp():
        allowed, now = is_within_call_window()
        pending = app.db.shifts_group_users.count_documents(
            {"call_processed": 0, "call_enabled": 1, "channel": "WhatsApp"}
        )
        return jsonify({
            "debug":        "shift_group_booking_whatsapp.py loaded",
            "server_time":  now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "template":     WATI_TEMPLATE_NAME,
            "batch_size":   BATCH_SIZE,
            "pending":      pending,
            "wati_url":     os.getenv("WATI_API_ENDPOINT", "not set"),
        })
