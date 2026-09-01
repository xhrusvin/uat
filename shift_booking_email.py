# shift_booking_email.py
import threading
import logging
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from flask import current_app, jsonify, request
from bson import ObjectId
from datetime import datetime
from bson import json_util
import json
from flask import jsonify

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ALLOWED_START_HOUR = 1
ALLOWED_END_HOUR   = 23
BATCH_SIZE         = 50


def is_within_call_window():
    now     = datetime.utcnow()
    hour    = now.hour
    allowed = ALLOWED_START_HOUR <= hour < ALLOWED_END_HOUR
    log.info(f"[TIME CHECK] {now.strftime('%Y-%m-%d %H:%M:%S UTC')} → Hour {hour} → Allowed: {allowed}")
    return allowed, now


def _check_and_end_outreach(app, shift_id: str, outreach_id: str):
    try:
        shift_oid    = ObjectId(shift_id)    if shift_id    and ObjectId.is_valid(shift_id)    else None
        outreach_oid = ObjectId(outreach_id) if outreach_id and ObjectId.is_valid(outreach_id) else None
        if not shift_oid or not outreach_oid:
            return
        total     = app.db.shifts_users.count_documents({"shift_id": shift_oid, "outreach_id": outreach_oid})
        processed = app.db.shifts_users.count_documents({"shift_id": shift_oid, "outreach_id": outreach_oid, "call_processed": 1})
        if total > 0 and processed >= total:
            app.db.outreach.update_one(
                {"_id": outreach_oid, "outreach_status": {"$nin": [3, 10]}},
                {"$set": {"outreach_status": 3, "ended_at": datetime.utcnow(),
                          "updated_at": datetime.utcnow(), "end_reason": "all_emails_processed"}}
            )
    except Exception as e:
        log.error(f"[END CHECK] Error: {e}")


def _format_date(date_str: str) -> str:
    """Convert '2026-08-25 00:00:00' → 'Monday, 25 August 2026'"""
    try:
        dt = datetime.strptime(str(date_str).split(" ")[0], "%Y-%m-%d")
        return dt.strftime("%A, %d %B %Y")
    except Exception:
        return str(date_str)


def _build_email_html(first_name, shift, base_url, shifts_users_id, staff_name=''):
    """Load and render shift booking email from HTML template file."""
    facility        = shift.get("client_name", "") or shift.get("location", "")
    address         = shift.get("client_address", "")
    county          = shift.get("client_county", "")
    lat             = shift.get("client_lat", "")
    lng             = shift.get("client_lng", "")
    date_str        = _format_date(shift.get("date", ""))
    start_time      = shift.get("start_time", "")
    end_time        = shift.get("end_time", "")
    user_type       = shift.get("user_type", "")
    unit            = shift.get("unit", "") or ""
    shift_type      = shift.get("shift_type", "") or shift.get("shift_timing", "") or ""
    shift_preference= shift.get("shift_preference", "") or "—"
    su_id           = str(shifts_users_id)
    logo_url        = "https://uat.expresshealth.ie/static/image/logo.png"
    map_url         = f"https://www.google.com/maps?q={lat},{lng}" if lat and lng else ""

    yes_url      = f"{base_url}/shift_booking_email/respond/{su_id}?answer=yes"
    no_url       = f"{base_url}/shift_booking_email/respond/{su_id}?answer=no"
    details_url  = f"{base_url}/shift_booking_email/respond/{su_id}?answer=details"
    comments_url = f"{base_url}/shift_booking_email/respond/{su_id}?answer=comments"

    # Shift timing icon
    st_lower = shift_type.lower()
    if "night" in st_lower:
        timing_icon = "🌙"
    elif "morning" in st_lower or "eve" in st_lower:
        timing_icon = "🌅"
    else:
        timing_icon = "☀️"

    # Subject and intro
    county_display = county or address or "your area"
    email_subject  = f"Shift Availability Request – Co. {county_display}" if county_display else f"Shift Availability Request – {facility}"
    email_intro    = f"Hope you're well! Please see the shift available in <strong>{county_display}</strong>. Tap Yes if you're free, or No if you can't make it."

    # Load template file
    import os as _os
    template_path = _os.path.join(
        _os.path.dirname(_os.path.abspath(__file__)),
        "templates", "booking", "shifts_booking_email.html"
    )
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            html = f.read()
    except FileNotFoundError:
        log.error(f"[EMAIL] Template not found: {template_path}")
        raise FileNotFoundError(f"Email template missing: {template_path}")

    html = html.replace("{{first_name}}", first_name)
    html = html.replace("{{staff_name}}", staff_name or first_name)
    html = html.replace("{{facility}}", facility)
    html = html.replace("{{address}}", address)
    html = html.replace("{{county}}", county)
    html = html.replace("{{county_uppercase}}", county.upper() if county else "")
    _rate_val = shift.get("rate", "")
    html = html.replace("{{rate}}", "REG" if not _rate_val or str(_rate_val) in ("0","0.0","") else str(_rate_val))
    html = html.replace("{{county_name}}", f"Co. {county}" if county else facility)
    html = html.replace("{{shift_count}}", "1")
    html = html.replace("{{premium_badge}}", "")
    html = html.replace("{{shift_date}}", date_str)
    html = html.replace("{{shift_timing_icon}}", timing_icon)
    html = html.replace("{{shift_type}}", shift_type)
    html = html.replace("{{start_time}}", start_time)
    html = html.replace("{{end_time}}", end_time)
    html = html.replace("{{unit}}", unit)
    unit_line = f'<br><span style="color:#777777;"><strong>Unit:</strong> {unit}</span>' if unit else ""
    html = html.replace("{{unit_line}}", unit_line)
    html = html.replace("{{user_type}}", user_type)
    html = html.replace("{{date_str}}", date_str)
    html = html.replace("{{shift_preference}}", shift_preference)
    html = html.replace("{{map_url}}", map_url)
    html = html.replace("{{yes_url}}", yes_url)
    html = html.replace("{{no_url}}", no_url)
    html = html.replace("{{details_url}}", details_url)
    html = html.replace("{{comments_url}}", comments_url)
    html = html.replace("{{logo_url}}", logo_url)
    html = html.replace("{{base_url}}", base_url)
    html = html.replace("{{email_subject}}", email_subject)
    html = html.replace("{{email_preview}}", "")
    html = html.replace("{{email_intro}}", email_intro)

    return html


def _send_shift_email(app, record, shift_doc, to_email, first_name, shifts_users_id):
    """Send shift booking email via SMTP."""
    try:
        # Safety check — skip if designation doesn't match shift user_type
        _designation  = (record.get("designation") or "").strip().lower()
        _shift_type   = (shift_doc.get("user_type") or "").strip().lower()
        if _designation and _shift_type and _designation != _shift_type:
            log.warning(f"[EMAIL] Blocked send to {to_email} — designation '{_designation}' != shift user_type '{_shift_type}'")
            return
        base_url = os.getenv("APP_BASE_URL", "https://uat.expresshealth.ie")
        html     = _build_email_html(first_name, shift_doc, base_url, shifts_users_id, staff_name=f"{first_name} {record.get('last_name', '')}".strip())

        su_id_str = str(shifts_users_id)
        msg = MIMEMultipart("alternative")
        _county_s = shift_doc.get('client_county','') or shift_doc.get('location','')
        msg["Subject"]    = f"Shift Availability Request – Co. {_county_s}" if _county_s else f"Shift Availability Request – {shift_doc.get('client_name','')}"
        msg["From"]       = f"{os.getenv('SHIFT_SMTP_FROM_NAME','XpressHealth')} <{os.getenv('SHIFT_FROM_EMAIL','')}>"
        msg["X-Shift-Id"] = su_id_str  # Track back to shifts_users._id
        _reply_to = os.getenv("SHIFT_REPLY_TO_EMAIL", "")
        if not _reply_to:
            reply_domain = os.getenv("SHIFT_REPLY_DOMAIN", "uat.expresshealth.ie")
            _reply_to = f"reply+{su_id_str}@{reply_domain}"
        msg["Reply-To"] = _reply_to
        msg["To"]      = to_email

        cc  = os.getenv("SHIFT_CC_EMAIL",  "")
        bcc = os.getenv("SHIFT_BCC_EMAIL", "")
        if cc:  msg["Cc"]  = cc
        if bcc: msg["Bcc"] = bcc

        msg.attach(MIMEText(html, "html"))

        recipients = [to_email]
        if cc:  recipients += [e.strip() for e in cc.split(",") if e.strip()]
        if bcc: recipients += [e.strip() for e in bcc.split(",") if e.strip()]

        smtp_host = os.getenv("SHIFT_SMTP_HOST", "smtp.gmail.com")
        smtp_port = int(os.getenv("SHIFT_SMTP_PORT", 587))
        smtp_user = os.getenv("SHIFT_SMTP_USER", "")
        smtp_pass = os.getenv("SHIFT_SMTP_PASSWORD", "")

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(msg["From"], recipients, msg.as_string())

        log.info(f"[EMAIL] ✓ Sent to {to_email}")
        result = app.db.shifts_users.update_one(
            {"_id": shifts_users_id},
            {"$set": {
                "email_sent":       1,
                "email_sent_at":    datetime.utcnow(),
                "email_status":     "delivered",
                "email_message_id": su_id_str,
                "availability":     8,
            }}
        )
        log.info(f"[EMAIL] DB update matched={result.matched_count} modified={result.modified_count} for {su_id_str}")

    except Exception as e:
        log.error(f"[EMAIL] ✗ Failed to send to {to_email}: {e}")
        app.db.shifts_users.update_one(
            {"_id": shifts_users_id},
            {"$set": {"email_error": str(e)}}
        )


def register_shift_booking_email_routes(app):

    @app.route('/shift_booking_email', methods=['GET'])
    # ?limit=N overrides BATCH_SIZE for this call
    def shift_booking_email():
        allowed, server_time = is_within_call_window()
        user_id_param = request.args.get('user_id')

        response_base = {
            "server_time":    server_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
        }

        if not allowed:
            return jsonify({**response_base, "status": "outside_hours",
                            "message": "Emails only allowed during allowed hours."}), 200

        if user_id_param:
            query = {"user_id": ObjectId(user_id_param), "call_processed": 0, "channel": "Email"}
        else:
            query = {"call_processed": 0, "call_enabled": 1, "channel": "Email"}

        _limit  = BATCH_SIZE
        records = list(app.db.shifts_users.find(query, sort=[("assigned_at", 1)], limit=_limit))

        if not records:
            return jsonify({**response_base, "status": "no_pending", "message": "No pending emails."}), 200

        triggered = []
        for record in records:
            shifts_users_id = record["_id"]
            shift_id        = str(record.get("shift_id",    ""))
            outreach_id     = str(record.get("outreach_id", ""))
            user_id         = str(record.get("user_id",     ""))

            user = None
            if user_id and ObjectId.is_valid(user_id):
                user = app.db.users.find_one(
                    {"_id": ObjectId(user_id)},
                    {"email": 1, "first_name": 1, "last_name": 1, "designation": 1}
                )

            if not user or not user.get("email"):
                continue

            email      = user.get("email")
            first_name = user.get("first_name", "")
            last_name  = user.get("last_name",  "")
            full_name  = f"{first_name} {last_name}".strip()

            result = app.db.shifts_users.update_one(
                {"_id": shifts_users_id},
                {"$set": {"call_processed": 1, "call_processed_at": datetime.utcnow(),
                          "updated_at": datetime.utcnow()}}
            )
            if result.modified_count == 0:
                continue

            shift_doc = {}
            if shift_id and ObjectId.is_valid(shift_id):
                s = app.db.shifts.find_one({"_id": ObjectId(shift_id)})
                if s:
                    client = None
                    if s.get("client_id"):
                        client = app.db.clients.find_one(
                            {"xn_client_id": str(s["client_id"])},
                            {"address": 1, "county": 1, "latitude": 1, "longitude": 1}
                        )
                    shift_doc = {
                        "id":             str(s["_id"]),
                        "shift_code":     s.get("shift_code") or s.get("name", ""),
                        "date":           str(s.get("date", "")),
                        "start_time":     s.get("start_time", ""),
                        "end_time":       s.get("end_time", ""),
                        "client_name":    s.get("client_name", ""),
                        "location":       s.get("location", ""),
                        "user_type":          s.get("user_type", ""),
                        "unit":               s.get("unit") or "",
                        "shift_type":         s.get("shift_timing") or s.get("shift_type") or "",
                        "shift_preference":   ", ".join([sp.get("name","") for sp in (s.get("shift_preferences") or []) if sp.get("name")]) or "—",
                        "client_address": client.get("address", "") if client else "",
                        "client_county":  s.get("client_county", "") or (client.get("county", "") if client else ""),
                        "client_lat":     client.get("latitude", "") if client else "",
                        "client_lng":     client.get("longitude", "") if client else "",
                    }

            record["email"]       = email
            record["first_name"]  = first_name
            record["last_name"]   = last_name
            record["designation"] = user.get("designation", "")
            record["full_name"]   = full_name

            threading.Thread(
                target=_send_shift_email,
                args=(current_app._get_current_object(), record, shift_doc, email, first_name, shifts_users_id),
                daemon=True
            ).start()

            threading.Thread(
                target=_check_and_end_outreach,
                args=(current_app._get_current_object(), shift_id, outreach_id),
                daemon=True
            ).start()

            triggered.append({
                "shifts_users_id": str(shifts_users_id),
                "shift_id":        shift_id,
                "outreach_id":     outreach_id,
                "user_id":         user_id,
                "staff_name":      full_name,
                "email":           email,
            })

        return jsonify({
            **response_base,
            "status":       "triggered",
            "triggered":    len(triggered),
            "batch_size":   _limit,
            "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "data":         triggered,
        }), 200



    @app.route('/shift_booking_email/respond/<shifts_users_id>', methods=['GET'])
    def shift_booking_email_respond(shifts_users_id):
        answer = request.args.get('answer', '').lower()

        try:
            obj_id = ObjectId(shifts_users_id)
        except Exception:
            return "<h2>Invalid link.</h2>", 400

        record = app.db.shifts_users.find_one({"_id": obj_id})
        if not record:
            return "<h2>Record not found.</h2>", 404

        shift_id    = str(record.get("shift_id", ""))
        outreach_id = str(record.get("outreach_id", ""))
        shift_doc   = {}
        if shift_id and ObjectId.is_valid(shift_id):
            s = app.db.shifts.find_one({"_id": ObjectId(shift_id)})
            if s:
                shift_doc = {
                    "client_name":       s.get("client_name", ""),
                    "date":              _format_date(str(s.get("date", ""))),
                    "start_time":        s.get("start_time", ""),
                    "end_time":          s.get("end_time", ""),
                    "location":          s.get("location", ""),
                    "user_type":         s.get("user_type", ""),
                    "unit":              s.get("unit") or "—",
                    "shift_type":        s.get("shift_timing") or s.get("shift_type") or "—",
                    "shift_preference":  ", ".join([sp.get("name","") for sp in (s.get("shift_preferences") or []) if sp.get("name")]) or "—",
                }

        base_url = os.getenv("APP_BASE_URL", "https://uat.expresshealth.ie")
        yes_url  = f"{base_url}/shift_booking_email/respond/{shifts_users_id}?answer=yes"
        no_url   = f"{base_url}/shift_booking_email/respond/{shifts_users_id}?answer=no"

        if answer == "yes":
            _now = datetime.utcnow()
            app.db.shifts_users.update_one(
                {"_id": obj_id},
                {"$set": {
                    "availability":   1,
                    "response_text":  "Yes, I'm available.",
                    "response_time":  _now.strftime("%Y-%m-%d %H:%M:%S"),
                    "responded_at":   _now,
                    "updated_at":     _now,
                }}
            )

            # ── Activity log: staff_available ─────────────────────────────
            try:
                _shift_oid    = ObjectId(shift_id)    if shift_id    and ObjectId.is_valid(shift_id)    else None
                _outreach_oid = ObjectId(outreach_id) if outreach_id and ObjectId.is_valid(outreach_id) else None

                # Get counts for activity log
                _log_query = {"shift_id": _shift_oid} if _shift_oid else {}
                if _outreach_oid:
                    _log_query["outreach_id"] = _outreach_oid
                available_count = app.db.shifts_users.count_documents({**_log_query, "availability": 1})
                declined_count  = app.db.shifts_users.count_documents({**_log_query, "availability": 0})
                no_reply_count  = app.db.shifts_users.count_documents({**_log_query, "availability": {"$in": [3, 4, 6, 7, 8]}})

                # Get user name
                _user_name = ""
                _user_oid  = record.get("user_id")
                if _user_oid:
                    _u = app.db.users.find_one({"_id": _user_oid}, {"first_name": 1, "last_name": 1})
                    if _u:
                        _user_name = f"{_u.get('first_name', '')} {_u.get('last_name', '')}".strip()

                # Get round number from outreach
                _round_number = 1
                if _outreach_oid:
                    _outreach_doc = app.db.outreach.find_one({"_id": _outreach_oid}, {"round_number": 1, "sequence_id": 1})
                    if _outreach_doc:
                        _round_number = _outreach_doc.get("round_number", 1)

                _seq_oid = _outreach_doc.get("sequence_id") if _outreach_doc else None

                activity_doc = {
                    "activity_type": "staff_available",
                    "shift_id":      _shift_oid,
                    "outreach_id":   _outreach_oid,
                    "metadata": {
                        "sequence_id":   str(_seq_oid) if _seq_oid else None,
                        "shift_id":      shift_id,
                        "outreach_id":   outreach_id,
                        "round_number":  _round_number,
                        "user_id":       str(record.get("user_id", "")),
                        "user_name":     _user_name,
                        "channel":       "Email",
                        "response":      "yes",
                        "available":     available_count,
                        "declined":      declined_count,
                        "no_reply":      no_reply_count,
                        "summary":       f"{_user_name or 'Staff'} marked available via email · {available_count} available, {declined_count} declined, {no_reply_count} no-reply",
                    },
                    "created_at": _now,
                }
                if _seq_oid:
                    activity_doc["sequence_id"] = _seq_oid
                app.db.activities.insert_one(activity_doc)
            except Exception as _log_err:
                log.error(f"[EMAIL RESPOND] Activity log error: {_log_err}")

            threading.Thread(
                target=_check_and_end_outreach,
                args=(app, shift_id, outreach_id), daemon=True
            ).start()
            html = f"""<html><body style="font-family:Arial;max-width:500px;margin:40px auto;text-align:center;color:#333">
  <h2 style="color:#1e7a38">&#x2705; Your availability has been recorded</h2>
  <p>We've marked you as <strong>available</strong> for this shift.</p>
  <div style="background:#f9f9f9;border-left:4px solid #1e7a38;padding:16px;border-radius:6px;text-align:left;margin:20px 0">
    <p>&#x1F4CD; {shift_doc.get('client_name')}, {shift_doc.get('location')}</p>
    <p>&#x1F4C5; {shift_doc.get('date')}</p>
    <p>&#x1F550; {shift_doc.get('start_time')} &#x2013; {shift_doc.get('end_time')}</p>
  </div>
  <p>We'll let you know as soon as the facility confirms the booking.</p>
  <p style="color:#aaa;font-size:12px">Xpress Health</p>
</body></html>"""

        elif answer == "no":
            _now = datetime.utcnow()
            app.db.shifts_users.update_one(
                {"_id": obj_id},
                {"$set": {
                    "availability":  0,
                    "response_text": "No, thanks.",
                    "response_time": _now.strftime("%Y-%m-%d %H:%M:%S"),
                    "responded_at":  _now,
                    "updated_at":    _now,
                }}
            )
            html = """<html><body style="font-family:Arial;max-width:500px;margin:40px auto;text-align:center;color:#333">
  <h2>&#x1F44D; No problem!</h2>
  <p>Thanks for letting us know. We'll reach out for future shifts.</p>
  <p style="color:#aaa;font-size:12px">Xpress Health</p>
</body></html>"""

        elif answer == "details":
            comments_url = f"{base_url}/shift_booking_email/respond/{shifts_users_id}?answer=comments"
            html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Inter,Arial,sans-serif;">
<div style="max-width:500px;margin:0 auto;">
  <div style="height:5px;background:linear-gradient(90deg,#016ab2 0%,#009540 100%);"></div>
  <div style="background:#fff;padding:24px 28px;">
    <img src="https://uat.expresshealth.ie/static/image/logo.png" alt="Xpress Health" width="130" style="display:block;margin-bottom:20px;">
    <hr style="border:none;border-top:1px solid #e5e7eb;margin:0 0 20px;">
    <h2 style="color:#111827;font-size:17px;margin:0 0 16px;">Shift Details</h2>
    <table width="100%" style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:8px;border-collapse:collapse;overflow:hidden;">
      <tr><td colspan="2" style="background:#1e7a38;padding:10px 16px;font-size:12px;font-weight:700;color:#fff;text-transform:uppercase;letter-spacing:0.5px;">Shift Information</td></tr>
      <tr><td style="padding:10px 16px;font-size:13px;color:#6b7280;width:45%;">📍 Facility</td><td style="padding:10px 16px;font-size:13px;font-weight:600;color:#111827;">{shift_doc.get('client_name')}</td></tr>
      <tr style="background:#fff;"><td style="padding:8px 16px;font-size:13px;color:#6b7280;">👩‍⚕️ Role</td><td style="padding:8px 16px;font-size:13px;font-weight:600;color:#111827;">{shift_doc.get('user_type')}</td></tr>
      <tr><td style="padding:8px 16px;font-size:13px;color:#6b7280;">📅 Date</td><td style="padding:8px 16px;font-size:13px;font-weight:600;color:#111827;">{shift_doc.get('date')}</td></tr>
      <tr style="background:#fff;"><td style="padding:8px 16px;font-size:13px;color:#6b7280;">🕐 Time</td><td style="padding:8px 16px;font-size:13px;font-weight:600;color:#111827;">{shift_doc.get('start_time')} – {shift_doc.get('end_time')}</td></tr>
      <tr><td style="padding:8px 16px;font-size:13px;color:#6b7280;">📌 Address</td><td style="padding:8px 16px;font-size:13px;font-weight:600;color:#111827;">{shift_doc.get('location')}</td></tr>
    </table>
    <p style="font-weight:700;text-align:center;color:#111827;margin:20px 0 12px;">Are you available for this shift?</p>
    <div style="text-align:center;margin-bottom:16px;">
      <a href="{yes_url}" style="display:inline-block;background:#1e7a38;color:#fff;padding:12px 22px;border-radius:6px;text-decoration:none;font-weight:700;font-size:14px;margin:4px;">✅ Yes, I'm available</a>
      <a href="{no_url}"  style="display:inline-block;background:#dc2626;color:#fff;padding:12px 22px;border-radius:6px;text-decoration:none;font-weight:700;font-size:14px;margin:4px;">❌ No, thanks</a>
      <a href="{comments_url}" style="display:inline-block;background:#f59e0b;color:#fff;padding:12px 22px;border-radius:6px;text-decoration:none;font-weight:700;font-size:14px;margin:4px;">💬 Add Comments</a>
    </div>
  </div>
  <div style="height:5px;background:linear-gradient(90deg,#016ab2 0%,#009540 100%);"></div>
  <div style="background:#f9fafb;text-align:center;padding:16px;font-size:11px;color:#9ca3af;">© 2026 Xpress Health</div>
</div>
</body></html>"""

        elif answer == "comments":
            existing_comment = record.get("staff_comment", "")
            html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Inter,Arial,sans-serif;">
<div style="max-width:500px;margin:0 auto;">
  <div style="height:5px;background:linear-gradient(90deg,#016ab2 0%,#009540 100%);"></div>
  <div style="background:#fff;padding:24px 28px;">
    <img src="https://uat.expresshealth.ie/static/image/logo.png" alt="Xpress Health" width="130" style="display:block;margin-bottom:20px;">
    <hr style="border:none;border-top:1px solid #e5e7eb;margin:0 0 20px;">
    <h2 style="color:#111827;font-size:17px;margin:0 0 8px;">Add Comments</h2>
    <p style="font-size:13px;color:#6b7280;margin:0 0 16px;">Leave a message for the Xpress Health bookings team regarding this shift.</p>
    {f'<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:6px;padding:12px 16px;margin-bottom:16px;font-size:13px;color:#166534;"><strong>Previous comment:</strong> {existing_comment}</div>' if existing_comment else ''}
    <form method="POST" action="{base_url}/shift_booking_email/respond/{shifts_users_id}">
      <input type="hidden" name="answer" value="save_comment">
      <textarea name="comment" rows="5" placeholder="Type your comment here..." style="width:100%;padding:12px;border:1px solid #d1d5db;border-radius:6px;font-size:14px;font-family:inherit;resize:vertical;box-sizing:border-box;"></textarea>
      <button type="submit" style="width:100%;margin-top:12px;background:#f59e0b;color:#fff;padding:13px;border:none;border-radius:6px;font-size:14px;font-weight:700;cursor:pointer;">Submit Comment</button>
    </form>
  </div>
  <div style="height:5px;background:linear-gradient(90deg,#016ab2 0%,#009540 100%);"></div>
</div>
</body></html>"""

        else:
            html = "<h2>Invalid response.</h2>"

        return html, 200


    @app.route('/shift_booking_email/respond/<shifts_users_id>', methods=['POST'])
    def shift_booking_email_respond_post(shifts_users_id):
        answer  = request.form.get('answer', '')
        comment = request.form.get('comment', '').strip()

        try:
            obj_id = ObjectId(shifts_users_id)
        except Exception:
            return "<h2>Invalid link.</h2>", 400

        if answer == "save_comment" and comment:
            app.db.shifts_users.update_one(
                {"_id": obj_id},
                {"$set": {
                    "staff_comment":     comment,
                    "customer_feedback": comment,
                    "comment_at":        datetime.utcnow(),
                    "updated_at":        datetime.utcnow(),
                }}
            )
            # Also update requested_confirm.customer_feedback for same shift+user
            su_record = app.db.shifts_users.find_one({"_id": obj_id}, {"shift_id": 1, "user_id": 1, "outreach_id": 1})
            if su_record:
                app.db.requested_confirm.update_many(
                    {
                        "shift_id":   su_record.get("shift_id"),
                        "staff_id":   su_record.get("user_id"),
                        "outreach_id": su_record.get("outreach_id"),
                    },
                    {"$set": {"customer_feedback": comment, "updated_at": datetime.utcnow()}}
                )
            html = f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Inter,Arial,sans-serif;">
<div style="max-width:500px;margin:0 auto;">
  <div style="height:5px;background:linear-gradient(90deg,#016ab2 0%,#009540 100%);"></div>
  <div style="background:#fff;padding:32px 28px;text-align:center;">
    <img src="https://uat.expresshealth.ie/static/image/logo.png" alt="Xpress Health" width="130" style="display:block;margin:0 auto 20px;">
    <div style="font-size:40px;margin-bottom:16px;">💬</div>
    <h2 style="color:#111827;font-size:18px;margin:0 0 10px;">Comment Submitted!</h2>
    <p style="font-size:14px;color:#6b7280;margin:0 0 20px;">Thank you. Your comment has been sent to the Xpress Health bookings team.</p>
    <div style="background:#f9fafb;border:1px solid #e5e7eb;border-radius:6px;padding:14px 18px;text-align:left;font-size:13px;color:#374151;">
      <strong>Your comment:</strong><br>{comment}
    </div>
  </div>
  <div style="height:5px;background:linear-gradient(90deg,#016ab2 0%,#009540 100%);"></div>
</div>
</body></html>"""
        else:
            html = "<h2>No comment provided.</h2>"

        return html, 200


    @app.route('/debug-shift-booking-email')
    def debug_shift_booking_email():
        allowed, now = is_within_call_window()
        return jsonify({
            "debug":          "shift_booking_email.py loaded",
            "server_time":    now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
        })
