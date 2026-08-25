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

ALLOWED_START_HOUR = 0
ALLOWED_END_HOUR   = 23
BATCH_SIZE         = 10
WATI_TEMPLATE_NAME = "shift_call_new"

# When True, a failed send resets call_processed to 0 so the record is retried
# on the next trigger. When False, failures stay marked processed.
RETRY_ON_FAILURE   = True


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


def _clean(value) -> str:
    """
    Meta/WATI reject template parameter values that are blank, contain newlines
    or tabs, or contain 4+ consecutive spaces. Collapse all whitespace runs to a
    single space and coerce everything to str (ints/datetimes from Mongo would
    otherwise serialize as non-strings and be rejected).
    """
    if value is None:
        return ""
    return " ".join(str(value).split())


def _p(name: str, value, fallback: str) -> dict:
    """Build one WATI parameter, guaranteeing a clean non-empty string value."""
    return {"name": name, "value": _clean(value) or fallback}


def _wati_env():
    """Return (url, token) from either env var naming convention."""
    wati_url   = (os.getenv("WATI_API_ENDPOINT") or os.getenv("WATI_API_URL", "")).rstrip("/")
    wati_token = os.getenv("WATI_ACCESS_TOKEN") or os.getenv("WATI_API_TOKEN", "")
    return wati_url, wati_token


def _wati_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }


def _mark_failure(app, su_id, error_text, parameters=None):
    """Record a send failure and optionally release the record for retry."""
    update = {
        "wa_error":      error_text,
        "wa_errored_at": datetime.utcnow(),
    }
    if parameters is not None:
        update["wa_error_params"] = parameters
    if RETRY_ON_FAILURE:
        update["call_processed"] = 0
    app.db.shifts_group_users.update_one({"_id": su_id}, {"$set": update})


def _send_wati_whatsapp(app, record, shift_doc, phone, first_name, su_id):
    """Send WhatsApp message via WATI API and save result to shifts_group_users."""
    parameters = None
    try:
        wati_url, wati_token = _wati_env()

        if not wati_url or not wati_token:
            log.error("[GROUP WA] WATI_API_ENDPOINT or WATI_ACCESS_TOKEN not set")
            _mark_failure(app, su_id, "WATI credentials not configured")
            return

        phone_clean = _clean(phone).replace("+", "").replace(" ", "").replace("-", "")

        facility = shift_doc.get("client_name", "") or shift_doc.get("location", "")
        county   = shift_doc.get("client_county", "") or ""
        date_str = _format_date(shift_doc.get("date", ""))
        start    = shift_doc.get("start_time", "")
        end      = shift_doc.get("end_time", "")
        unit     = shift_doc.get("unit", "") or ""

        _rate = _clean(shift_doc.get("rate", ""))
        rate  = "REG" if _rate in ("", "0", "0.0", "0.00") else _rate

        parameters = [
            _p("county",   county,                      "Ireland"),
            _p("name",     first_name,                  "there"),
            _p("facility", facility,                    "the facility"),
            _p("unit",     unit,                        "General"),
            _p("date",     date_str,                    "TBC"),
            _p("start",    start,                       "TBC"),
            _p("end",      end,                         "TBC"),
            _p("role",     shift_doc.get("user_type"),  "Nurse"),
            _p("rate",     rate,                        "REG"),
        ]

        payload = {
            "template_name":  WATI_TEMPLATE_NAME,
            "broadcast_name": f"group_shift_{str(su_id)}",
            "parameters":     parameters,
        }

        _wati_send_url = (
            f"{wati_url}/api/v1/sendTemplateMessage?whatsappNumber={phone_clean}"
            if "/api" not in wati_url else
            f"{wati_url}/v1/sendTemplateMessage?whatsappNumber={phone_clean}"
        )

        resp = _req.post(_wati_send_url, json=payload,
                         headers=_wati_headers(wati_token), timeout=20)

        # WATI returns HTTP 200 with result=false for template errors, so check both.
        resp_data = {}
        try:
            resp_data = resp.json() or {}
        except Exception:
            pass

        ok = resp.status_code == 200 and resp_data.get("result") is not False

        if ok:
            log.info(f"[GROUP WA] ✓ Sent to {phone_clean}")
            app.db.shifts_group_users.update_one(
                {"_id": su_id},
                {"$set": {
                    "wa_sent":            1,
                    "wa_sent_at":         datetime.utcnow(),
                    "wa_message_id":      resp_data.get("id", ""),
                    "wa_conversation_id": resp_data.get("conversationId", ""),
                    "wa_phone":           phone_clean,
                    "availability":       8,
                }}
            )
        else:
            err = f"{resp.status_code}: {resp.text[:200]}"
            log.error(f"[GROUP WA] ✗ Failed {phone_clean}: {err} | params={parameters}")
            _mark_failure(app, su_id, err, parameters)

    except Exception as e:
        log.error(f"[GROUP WA] ✗ Exception for {phone}: {e}")
        _mark_failure(app, su_id, str(e), parameters)


def _get_shift_doc(app, record):
    """Build shift_doc from first shift in the group."""
    group_id  = record.get("group_id")
    shift_id  = record.get("shift_id")
    shift_doc = {}

    def _build(s, client=None):
        return {
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
                shift_doc = _build(s, client)
    elif shift_id:
        s = app.db.shifts.find_one({"_id": shift_id})
        if s:
            shift_doc = _build(s)

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
        skipped   = []
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
                skipped.append({"su_id": str(su_id), "reason": "no_phone"})
                continue

            phone      = user["phone"]
            first_name = user.get("first_name", "")
            full_name  = f"{first_name} {user.get('last_name', '')}".strip()

            # Check designation matches shift user_type
            user_designation = (user.get("designation") or "").strip().lower()
            shift_doc        = _get_shift_doc(app, record)
            shift_user_type  = (shift_doc.get("user_type") or "").strip().lower()
            if user_designation and shift_user_type and user_designation != shift_user_type:
                log.warning(f"[GROUP WA] Skipping {phone} — designation "
                            f"'{user_designation}' != shift user_type '{shift_user_type}'")
                skipped.append({"su_id": str(su_id), "reason": "designation_mismatch"})
                continue

            if not shift_doc:
                log.warning(f"[GROUP WA] No shift doc resolved for su_id={su_id}")
                skipped.append({"su_id": str(su_id), "reason": "no_shift_doc"})
                continue

            # Mark processed + availability=7 (Not Sent). Clear any stale error.
            result = app.db.shifts_group_users.update_one(
                {"_id": su_id, "call_processed": 0},
                {"$set": {"call_processed": 1, "call_processed_at": datetime.utcnow(),
                          "availability": 7, "updated_at": datetime.utcnow()},
                 "$unset": {"wa_error": "", "wa_error_params": "", "wa_errored_at": ""}}
            )
            if result.modified_count == 0:
                continue

            threading.Thread(
                target=_send_wati_whatsapp,
                args=(current_app._get_current_object(), record, shift_doc,
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
            "skipped":      len(skipped),
            "batch_size":   BATCH_SIZE,
            "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "data":         triggered,
            "skipped_data": skipped,
        }), 200

    @app.route('/debug-shift-group-booking-whatsapp', endpoint='debug_shift_group_booking_whatsapp_route')
    def debug_shift_group_booking_whatsapp():
        allowed, now = is_within_call_window()
        pending = app.db.shifts_group_users.count_documents(
            {"call_processed": 0, "call_enabled": 1, "channel": "WhatsApp"}
        )
        errored = app.db.shifts_group_users.count_documents(
            {"channel": "WhatsApp", "wa_error": {"$exists": True}}
        )
        return jsonify({
            "debug":        "shift_group_booking_whatsapp.py loaded",
            "server_time":  now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "call_allowed": allowed,
            "template":     WATI_TEMPLATE_NAME,
            "batch_size":   BATCH_SIZE,
            "pending":      pending,
            "errored":      errored,
            "retry_on_failure": RETRY_ON_FAILURE,
            "wati_url":     os.getenv("WATI_API_ENDPOINT", "not set"),
        })

    @app.route('/debug-wati-template', endpoint='debug_wati_template_route')
    def debug_wati_template():
        """
        Fetch the live template definition from WATI so you can compare the
        placeholders it declares against the parameter names this file sends.
        A mismatch here is the usual cause of the generic
        'Check your template, it cannot have typos or blank text' 400.
        """
        wati_url, wati_token = _wati_env()
        if not wati_url or not wati_token:
            return jsonify({"error": "WATI credentials not configured"}), 500

        url = (f"{wati_url}/api/v1/getMessageTemplates?pageSize=100"
               if "/api" not in wati_url else
               f"{wati_url}/v1/getMessageTemplates?pageSize=100")

        try:
            r = _req.get(url, headers=_wati_headers(wati_token), timeout=20)
            data = r.json() if r.status_code == 200 else {}
        except Exception as e:
            return jsonify({"error": str(e)}), 500

        templates = data.get("messageTemplates", []) or []
        match = next((t for t in templates
                      if t.get("elementName") == WATI_TEMPLATE_NAME), None)

        sent_names = ["county", "name", "facility", "unit",
                      "date", "start", "end", "role", "rate"]

        if not match:
            return jsonify({
                "template":         WATI_TEMPLATE_NAME,
                "found":            False,
                "available":        [t.get("elementName") for t in templates],
                "we_send":          sent_names,
            })

        expected = [c.get("paramName") or c.get("text")
                    for c in (match.get("customParams") or [])]

        return jsonify({
            "template":     WATI_TEMPLATE_NAME,
            "found":        True,
            "status":       match.get("status"),
            "expects":      expected,
            "we_send":      sent_names,
            "missing":      [n for n in expected if n and n not in sent_names],
            "extra":        [n for n in sent_names if expected and n not in expected],
            "body":         (match.get("body") or "")[:500],
        })
