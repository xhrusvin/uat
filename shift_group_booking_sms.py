# shift_group_booking_sms.py
# Sends SMS via Telnyx for GROUP shifts (shifts_group_users collection) where channel == 'SMS'
# Mirrors shift_group_booking_whatsapp.py logic exactly
# Processes up to 10 pending messages per trigger call

import logging
import os
import threading
import time as _time
import requests as _req
from flask import current_app, jsonify, request
from bson import ObjectId
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ALLOWED_START_HOUR = 8    # SMS-friendly window — daytime only
ALLOWED_END_HOUR   = 21
BATCH_SIZE         = 10

TELNYX_API_BASE = "https://api.telnyx.com/v2"

# ── Telnyx credentials (set in environment) ────────────────────────────────────
# TELNYX_API_KEY        your API key from Telnyx Mission Control Portal
# TELNYX_FROM_NUMBER    your Telnyx number in E.164 format e.g. +353894618556
# ──────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def is_within_call_window():
    now     = datetime.utcnow()
    hour    = now.hour
    allowed = ALLOWED_START_HOUR <= hour < ALLOWED_END_HOUR
    log.info(f"[GROUP SMS TIME CHECK] {now.strftime('%Y-%m-%d %H:%M:%S UTC')} → Hour {hour} → Allowed: {allowed}")
    return allowed, now


def _format_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(str(date_str).split(" ")[0].split("T")[0], "%Y-%m-%d")
        return dt.strftime("%A, %d %B %Y")
    except Exception:
        return str(date_str)


def _clean_phone(phone: str) -> str:
    """Return E.164 with leading + (Telnyx requires +353...)."""
    cleaned = phone.replace(" ", "").replace("-", "").strip()
    if not cleaned.startswith("+"):
        cleaned = "+" + cleaned
    return cleaned


def _build_sms_body(first_name: str, shift_doc: dict, shift_index: int = 0) -> str:
    """
    Compose the SMS text body.
    Matches the same fields as the WATI template: county, facility, unit, date, from_time, to_time, rate.
    Aim for under 160 chars per segment.
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
        f"Reply YES or NO"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Telnyx outbound send — synchronous, one shift at a time
# (mirrors _send_wati_whatsapp_sync exactly)
# ─────────────────────────────────────────────────────────────────────────────

def _send_telnyx_sms_sync(app, shift_doc, phone, first_name, su_id,
                           shift_index=0, shift_id="", delay=0):
    """
    Send one SMS via Telnyx and update shifts_group_users.
    Returns the Telnyx response dict on success, None on failure.

    POST https://api.telnyx.com/v2/messages
    { "from": "+353...", "to": "+353...", "text": "..." }

    Response:
    { "data": { "id": "<message-uuid>", "to": [{"phone_number":"...","status":"queued"}], ... } }
    """
    try:
        if delay:
            _time.sleep(delay)

        api_key     = os.getenv("TELNYX_API_KEY", "")
        from_number = os.getenv("TELNYX_FROM_NUMBER", "")

        if not api_key or not from_number:
            log.error("[GROUP SMS] TELNYX_API_KEY or TELNYX_FROM_NUMBER not set")
            return None

        phone_clean = _clean_phone(phone)
        body        = _build_sms_body(first_name, shift_doc, shift_index)

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

        log.info(f"[GROUP SMS] shift_index={shift_index} to={phone_clean}")
        resp = _req.post(
            f"{TELNYX_API_BASE}/messages",
            json=payload,
            headers=headers,
            timeout=20,
        )

        try:
            resp_data = resp.json()
        except ValueError:
            resp_data = {}

        log.info(f"[GROUP SMS] shift_index={shift_index} status={resp.status_code}")

        if resp.status_code in (200, 201):
            msg_data   = resp_data.get("data", {})
            message_id = msg_data.get("id", "")

            log.info(
                f"[GROUP SMS] ✓ shift_index={shift_index} "
                f"message_id={message_id} phone={phone_clean}"
            )

            # Build availability_details entry — mirrors WA structure exactly
            detail_entry = {
                "shift_id":       shift_id,
                "shift_index":    shift_index,
                "sms_message_id": message_id,
                "sms_phone":      phone_clean,
                "availability":   8,
                "responded_at":   None,
                "sent_at":        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            }

            update_ops = {
                "$set": {
                    "sms_sent":      1,
                    "availability":  8,
                },
                "$push": {
                    "availability_details": detail_entry
                }
            }

            # Extra top-level fields on the first message only (mirrors WA exactly)
            if shift_index == 0:
                update_ops["$set"].update({
                    "sms_sent_at":    datetime.utcnow(),
                    "sms_message_id": message_id,
                    "sms_phone":      phone_clean,
                    "updated_at":     datetime.utcnow(),
                })

            update_result = app.db.shifts_group_users.update_one(
                {"_id": su_id},
                update_ops
            )
            log.info(
                f"[GROUP SMS] ✓ DB updated shift_index={shift_index} "
                f"matched={update_result.matched_count} "
                f"modified={update_result.modified_count}"
            )

            # ── Auto-end outreach when this was the last pending SMS ──────────
            try:
                _rec = app.db.shifts_group_users.find_one(
                    {"_id": su_id}, {"outreach_id": 1, "group_id": 1}
                )
                outreach_id = _rec.get("outreach_id") if _rec else None
                group_id    = _rec.get("group_id")    if _rec else None

                if outreach_id and group_id:
                    def _check_and_end_sms():
                        try:
                            # Count remaining unprocessed SMS records for this outreach
                            pending = app.db.shifts_group_users.count_documents({
                                "outreach_id":   outreach_id,
                                "channel":       "SMS",
                                "call_processed": 0,
                            })
                            if pending > 0:
                                return  # still more to send

                            # Only auto-end if outreach is still Live / Paused
                            outreach = app.db.outreach_shift_group.find_one({
                                "_id": outreach_id,
                                "outreach_status": {"$in": [1, 2]}
                            })
                            if not outreach:
                                return

                            now = datetime.utcnow()
                            res = app.db.outreach_shift_group.update_one(
                                {"_id": outreach_id},
                                {"$set": {
                                    "outreach_status": 3,
                                    "status":          "ended",
                                    "ended_at":        now,
                                    "updated_at":      now,
                                    "end_reason":      "All SMS messages sent",
                                }}
                            )
                            if res.modified_count:
                                # Disable remaining pending staff
                                app.db.shifts_group_users.update_many(
                                    {"group_id": group_id, "call_processed": 0},
                                    {"$set": {
                                        "call_enabled": 0,
                                        "updated_at":   now,
                                    }}
                                )
                                rn = outreach.get("round_number", 1)
                                app.db.activities.insert_one({
                                    "activity_type": "round_ended",
                                    "group_id":      group_id,
                                    "outreach_id":   outreach_id,
                                    "metadata": {
                                        "round_number": rn,
                                        "end_reason":   "All SMS messages sent",
                                        "auto_ended":   True,
                                        "summary":      f"Round {rn} auto-ended · all SMS messages sent",
                                    },
                                    "created_at": now,
                                })
                                log.info(
                                    f"[SMS AUTO-END] outreach_shift_group {outreach_id} "
                                    f"→ outreach_status=3, status='ended' "
                                    f"(last SMS sent for group {group_id})"
                                )
                        except Exception as e:
                            log.error(f"[SMS AUTO-END] failed for outreach {outreach_id}: {e}")

                    threading.Thread(target=_check_and_end_sms, daemon=True).start()

            except Exception as auto_err:
                log.warning(f"[SMS AUTO-END] outer check failed: {auto_err}")

            return resp_data

        else:
            err = f"shift_{shift_index}: {resp.status_code}: {resp.text[:300]}"
            log.error(f"[GROUP SMS] ✗ Failed shift_index={shift_index}: {err}")
            app.db.shifts_group_users.update_one(
                {"_id": su_id},
                {"$set": {"sms_error": err}}
            )
            return None

    except Exception as e:
        log.error(f"[GROUP SMS] ✗ Exception shift_index={shift_index}: {e}")
        app.db.shifts_group_users.update_one(
            {"_id": su_id},
            {"$set": {"sms_error": str(e)}}
        )
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Shift doc builder  (identical to WA version)
# ─────────────────────────────────────────────────────────────────────────────

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
                        {"xn_client_id": str(s["client_id"])}, {"county": 1}
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


# ─────────────────────────────────────────────────────────────────────────────
# Route registration
# ─────────────────────────────────────────────────────────────────────────────

def register_shift_group_booking_sms_routes(app):

    @app.route('/shift_group_booking_sms', methods=['GET'], endpoint='shift_group_booking_sms_route')
    def shift_group_booking_sms():
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
            query = {"user_id": ObjectId(user_id_param), "call_processed": 0, "channel": "SMS"}
        else:
            query = {"call_processed": 0, "call_enabled": 1, "channel": "SMS"}

        records = list(app.db.shifts_group_users.find(
            query, sort=[("assigned_at", 1)], limit=BATCH_SIZE
        ))

        if not records:
            return jsonify({**response_base, "status": "no_pending",
                            "message": "No pending SMS in shifts_group_users"}), 200

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
                log.warning(f"[GROUP SMS] No phone for su_id={su_id}")
                continue

            phone            = user["phone"]
            first_name       = user.get("first_name", "")
            full_name        = f"{first_name} {user.get('last_name', '')}".strip()
            user_designation = (user.get("designation") or "").strip().lower()

            # ── Build shift_docs — one per matching shift in the group ─────────
            # Mirrors WA: filters by designation vs shift user_type
            group_id_rec  = record.get("group_id")
            shift_docs    = []
            shift_id_list = []

            if group_id_rec:
                sg = app.db.shifts_group.find_one({"_id": group_id_rec}, {"shift_ids": 1})
                if sg and sg.get("shift_ids"):
                    for _sid in sg["shift_ids"]:
                        _s = app.db.shifts.find_one({"_id": _sid})
                        if not _s:
                            continue
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
                log.warning(f"[GROUP SMS] No shifts match designation '{user_designation}' for su_id={su_id} — skipping")
                continue

            log.info(f"[GROUP SMS] Sending {len(shift_docs)} SMS to {phone} for su_id={su_id}")

            # Mark processed + clear availability_details before background send
            result = app.db.shifts_group_users.update_one(
                {"_id": su_id},
                {"$set": {
                    "call_processed":       1,
                    "call_processed_at":    datetime.utcnow(),
                    "availability":         7,
                    "updated_at":           datetime.utcnow(),
                    "availability_details": [],   # reset — populated per-shift in thread
                }}
            )
            if result.modified_count == 0:
                continue   # already claimed by another worker

            # ── Thread: send one SMS per shift with a gap between each ─────────
            def _send_all_shifts(app_obj, shift_docs_list, shift_id_list_, phone_, first_name_, su_id_):
                try:
                    log.info(f"[GROUP SMS] Thread started — {len(shift_docs_list)} shifts for {phone_}")
                    for _i, (_sdoc, _sid) in enumerate(zip(shift_docs_list, shift_id_list_)):
                        if _i > 0:
                            _time.sleep(10)  # gap between messages, same as WA version
                        log.info(f"[GROUP SMS] Sending shift {_i + 1}/{len(shift_docs_list)} shift_id={_sid}")
                        _send_telnyx_sms_sync(app_obj, _sdoc, phone_, first_name_, su_id_, _i, _sid)
                except Exception as _e:
                    log.error(f"[GROUP SMS] Thread error: {_e}", exc_info=True)

            threading.Thread(
                target=_send_all_shifts,
                args=(current_app._get_current_object(), shift_docs, shift_id_list,
                      phone, first_name, su_id),
                daemon=True
            ).start()

            triggered.append({
                "su_id":        str(su_id),
                "user_id":      str(user_id),
                "staff_name":   full_name,
                "phone":        phone,
                "shifts_count": len(shift_docs),
                "shift_ids":    shift_id_list,
            })

        return jsonify({
            **response_base,
            "status":       "triggered",
            "triggered":    len(triggered),
            "batch_size":   BATCH_SIZE,
            "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "data":         triggered,
        }), 200

    @app.route('/debug-shift-group-booking-sms', endpoint='debug_shift_group_booking_sms_route')
    def debug_shift_group_booking_sms():
        allowed, now = is_within_call_window()
        pending = app.db.shifts_group_users.count_documents(
            {"call_processed": 0, "call_enabled": 1, "channel": "SMS"}
        )
        return jsonify({
            "debug":        "shift_group_booking_sms.py loaded (Telnyx)",
            "server_time":  now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "batch_size":   BATCH_SIZE,
            "pending":      pending,
            "telnyx_api":   TELNYX_API_BASE,
            "from_number":  os.getenv("TELNYX_FROM_NUMBER", "not set"),
            "api_key_set":  bool(os.getenv("TELNYX_API_KEY")),
        })