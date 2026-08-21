# shift_group_booking_email.py
# Sends outreach emails for GROUP shifts (shifts_group_users collection)
# Processes up to 5 pending emails per trigger call
import threading
import logging
import smtplib
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from flask import current_app, jsonify, request
from bson import ObjectId
from datetime import datetime
from flask import jsonify

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ALLOWED_START_HOUR = 0
ALLOWED_END_HOUR   = 23
BATCH_SIZE         = 5   # emails per trigger


def is_within_call_window():
    now     = datetime.utcnow()
    hour    = now.hour
    allowed = ALLOWED_START_HOUR <= hour < ALLOWED_END_HOUR
    log.info(f"[GROUP EMAIL TIME CHECK] {now.strftime('%Y-%m-%d %H:%M:%S UTC')} → Hour {hour} → Allowed: {allowed}")
    return allowed, now


def _format_date(date_str: str) -> str:
    try:
        dt = datetime.strptime(str(date_str).split(" ")[0], "%Y-%m-%d")
        return dt.strftime("%A, %d %B %Y")
    except Exception:
        return str(date_str)


def _build_email_html(first_name, shifts_list, base_url, shifts_users_id, staff_name='', county=''):
    """Load and render GROUP shift booking email with multiple shift rows."""
    import os as _os

    su_id        = str(shifts_users_id)
    logo_url     = "https://uat.expresshealth.ie/static/image/logo.png"
    county_upper = county.upper() if county else ""
    comments_url = f"{base_url}/shift_group_booking_email/respond/{su_id}?answer=comments"
    details_url  = f"{base_url}/shift_group_booking_email/respond/{su_id}?answer=details"

    # Group shifts by client_county from shifts data
    from collections import OrderedDict as _OD
    county_groups = _OD()
    for shift in shifts_list:
        c = (shift.get("client_county") or shift.get("county") or county or "Other").strip()
        if c not in county_groups:
            county_groups[c] = []
        county_groups[c].append(shift)

    # Build one table per county
    rows_html = ""
    for _county, _shifts in county_groups.items():
        _county_upper = _county.upper() if _county else ""
        _county_rows = ""
        for shift in _shifts:
            facility   = shift.get("client_name", "") or shift.get("location", "")
            unit       = shift.get("unit", "") or ""
            unit_td    = f"<br><small style='color:#777;'><strong>Unit:</strong> {unit}</small>" if unit else ""
            date_str   = _format_date(shift.get("date", ""))
            start_time = shift.get("start_time", "")
            end_time   = shift.get("end_time", "")
            shift_type = shift.get("shift_type", "") or shift.get("shift_timing", "") or ""
            _rate_raw  = shift.get("rate", "")
            rate       = "REG" if not _rate_raw or str(_rate_raw) in ("0", "0.0", "") else str(_rate_raw)
            yes_url    = f"{base_url}/shift_group_booking_email/respond/{su_id}?answer=yes&shift_id={str(shift.get('id',''))}"
            _county_rows += (
                f"<tr>"
                f"<td style='padding:12px 10px;border-bottom:1px solid #eee;font-size:13px;white-space:nowrap;vertical-align:middle;'>{date_str}</td>"
                f"<td style='padding:12px 10px;border-bottom:1px solid #eee;font-size:13px;white-space:nowrap;vertical-align:middle;'>{shift_type}<br>{start_time}–{end_time}</td>"
                f"<td style='padding:12px 10px;border-bottom:1px solid #eee;font-size:13px;vertical-align:middle;line-height:1.4;'><strong>{facility}</strong>{unit_td}</td>"
                f"<td style='padding:12px 10px;border-bottom:1px solid #eee;font-size:13px;white-space:nowrap;vertical-align:middle;text-align:center;'>{rate}</td>"
                f"<td style='padding:12px 10px;border-bottom:1px solid #eee;vertical-align:middle;text-align:center;'>"
                f"<a href='{yes_url}' target='_blank' style='display:inline-block;background:#168124;color:#fff;text-decoration:none;font-size:13px;font-weight:700;padding:8px 18px;border-radius:5px;white-space:nowrap;'>✓ Yes</a>"
                f"</td></tr>"
            )
        rows_html += (
            f"<table width='100%' cellspacing='0' cellpadding='0' border='0' style='width:100%;border:1px solid #ddd;border-collapse:collapse;margin-bottom:16px;'>"
            f"<tr><td colspan='5' style='padding:12px 14px;background:#f4f3ef;border-bottom:1px solid #ddd;font-size:14px;font-weight:700;color:#27237c;'>CO. {_county_upper} &nbsp;—&nbsp; {len(_shifts)} shift(s)</td></tr>"
            f"<tr style='background:#fafafa;'>"
            f"<td style='padding:10px;border-bottom:1px solid #ddd;font-size:11px;font-weight:700;color:#666;text-transform:uppercase;'>Date</td>"
            f"<td style='padding:10px;border-bottom:1px solid #ddd;font-size:11px;font-weight:700;color:#666;text-transform:uppercase;'>Time</td>"
            f"<td style='padding:10px;border-bottom:1px solid #ddd;font-size:11px;font-weight:700;color:#666;text-transform:uppercase;'>Location</td>"
            f"<td style='padding:10px;border-bottom:1px solid #ddd;font-size:11px;font-weight:700;color:#666;text-transform:uppercase;text-align:center;'>Rate</td>"
            f"<td style='padding:10px;border-bottom:1px solid #ddd;font-size:11px;font-weight:700;color:#666;text-transform:uppercase;text-align:center;'>Response</td>"
            f"</tr>{_county_rows}</table>"
        )

    # Load template
    template_path = _os.path.join(
        _os.path.dirname(_os.path.abspath(__file__)),
        "templates", "booking", "shifts_group_booking_email.html"
    )
    try:
        with open(template_path, "r", encoding="utf-8") as f:
            html = f.read()
    except FileNotFoundError:
        log.error(f"[GROUP EMAIL] Template not found: {template_path}")
        raise FileNotFoundError(f"Email template missing: {template_path}")

    html = html.replace("{{staff_name}}", staff_name or first_name)
    html = html.replace("{{county}}", county)
    html = html.replace("{{county_uppercase}}", county_upper)
    html = html.replace("{{shift_count}}", str(len(shifts_list)))
    html = html.replace("{{shift_rows}}", rows_html)
    html = html.replace("{{details_url}}", details_url)
    html = html.replace("{{comments_url}}", comments_url)
    html = html.replace("{{logo_url}}", logo_url)
    html = html.replace("{{base_url}}", base_url)

    return html



def _send_group_shift_email(app, record, shifts_list, to_email, first_name, su_id, county=""):
    """Send group shift booking email via SMTP."""
    try:
        base_url  = os.getenv("APP_BASE_URL", "https://uat.expresshealth.ie")
        last_name = record.get("last_name", "")
        html      = _build_email_html(first_name, shifts_list, base_url, su_id,
                                       staff_name=f"{first_name} {last_name}".strip(), county=county)

        su_id_str = str(su_id)
        msg = MIMEMultipart("alternative")
        # Build subject with all unique counties
        _all_counties = list(dict.fromkeys([
            (s.get("client_county") or "").strip()
            for s in shifts_list if s.get("client_county")
        ]))
        if _all_counties:
            _counties_str = ", ".join(_all_counties)
            msg["Subject"] = f"Shift Availability Request – Co. {_counties_str}"
        else:
            msg["Subject"] = f"Shift Availability Request – Co. {county}" if county else "Shift Availability Request – Xpress Health"
        msg["From"]       = f"{os.getenv('SHIFT_SMTP_FROM_NAME', 'XpressHealth')} <{os.getenv('SHIFT_FROM_EMAIL', '')}>"
        msg["X-Shift-Id"] = su_id_str
        reply_domain      = os.getenv("SHIFT_REPLY_DOMAIN", "uat.expresshealth.ie")
        msg["Reply-To"]   = f"reply+{su_id_str}@{reply_domain}"
        msg["To"]         = to_email

        cc  = os.getenv("SHIFT_CC_EMAIL", "")
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

        log.info(f"[GROUP EMAIL] ✓ Sent to {to_email}")
        app.db.shifts_group_users.update_one(
            {"_id": su_id},
            {"$set": {
                "email_sent":       1,
                "email_sent_at":    datetime.utcnow(),
                "email_status":     "delivered",
                "email_message_id": su_id_str,
                "availability":     8,
            }}
        )

    except Exception as e:
        log.error(f"[GROUP EMAIL] ✗ Failed to send to {to_email}: {e}")
        app.db.shifts_group_users.update_one(
            {"_id": su_id},
            {"$set": {"email_error": str(e)}}
        )


def register_shift_group_booking_email_routes(app):

    @app.route('/shift_group_booking_email', methods=['GET'])
    def shift_group_booking_email():
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

        # Fetch up to BATCH_SIZE pending records from shifts_group_users
        if user_id_param:
            query = {"user_id": ObjectId(user_id_param), "call_processed": 0, "channel": "Email"}
        else:
            query = {"call_processed": 0, "call_enabled": 1, "channel": "Email"}

        records = list(app.db.shifts_group_users.find(
            query, sort=[("assigned_at", 1)], limit=BATCH_SIZE
        ))

        if not records:
            return jsonify({**response_base, "status": "no_pending",
                            "message": "No pending group emails."}), 200

        triggered = []
        for record in records:
            su_id      = record["_id"]
            shift_id   = str(record.get("shift_id", ""))
            outreach_id= str(record.get("outreach_id", ""))
            user_id    = str(record.get("user_id", ""))

            user = None
            if user_id and ObjectId.is_valid(user_id):
                user = app.db.users.find_one(
                    {"_id": ObjectId(user_id)},
                    {"email": 1, "first_name": 1, "last_name": 1, "designation": 1}
                )

            if not user or not user.get("email"):
                log.warning(f"[GROUP EMAIL] No user/email for su_id={su_id}")
                continue

            email      = user["email"]
            first_name = user.get("first_name", "")
            last_name  = user.get("last_name", "")
            full_name  = f"{first_name} {last_name}".strip()

            # Check user designation matches shift user_type
            user_designation = user.get("designation", "").strip().lower()
            # Get user_type from group's first shift
            _shift_user_type = ""
            _group_id_check  = record.get("group_id")
            if _group_id_check:
                _sg_check = app.db.shifts_group.find_one({"_id": _group_id_check}, {"shift_ids": 1})
                if _sg_check and _sg_check.get("shift_ids"):
                    _s_check = app.db.shifts.find_one({"_id": _sg_check["shift_ids"][0]}, {"user_type": 1})
                    if _s_check:
                        _shift_user_type = (_s_check.get("user_type") or "").strip().lower()
            if user_designation and _shift_user_type and user_designation != _shift_user_type:
                log.warning(f"[GROUP EMAIL] Skipping {email} — designation '{user_designation}' != shift user_type '{_shift_user_type}'")
                continue

            # Mark as processed
            result = app.db.shifts_group_users.update_one(
                {"_id": su_id},
                {"$set": {"call_processed": 1, "call_processed_at": datetime.utcnow(),
                           "updated_at": datetime.utcnow()}}
            )
            if result.modified_count == 0:
                continue

            # Build shifts_list — all shifts from the group
            shifts_list = []
            county      = ""
            group_id    = record.get("group_id")
            if group_id:
                sg = app.db.shifts_group.find_one({"_id": group_id}, {"shift_ids": 1})
                if sg and sg.get("shift_ids"):
                    for sid in sg["shift_ids"]:
                        s = app.db.shifts.find_one({"_id": sid})
                        if s:
                            client = None
                            if s.get("client_id"):
                                client = app.db.clients.find_one(
                                    {"xn_client_id": str(s["client_id"])},
                                    {"address": 1, "county": 1, "latitude": 1, "longitude": 1}
                                )
                            c = s.get("client_county", "") or (client.get("county", "") if client else "")
                            if not county and c:
                                county = c
                            shifts_list.append({
                                "id":               str(s["_id"]),
                                "date":             str(s.get("date", "")),
                                "start_time":       s.get("start_time", ""),
                                "end_time":         s.get("end_time", ""),
                                "client_name":      s.get("client_name", "") or s.get("location", ""),
                                "location":         s.get("location", ""),
                                "user_type":        s.get("user_type", ""),
                                "unit":             s.get("unit") or "",
                                "shift_type":       s.get("shift_timing") or s.get("shift_type") or "",
                                "rate":             "REG" if not s.get("rate") or str(s.get("rate","")) in ("0","0.0") else str(s.get("rate","")),
                                "client_county":    c,
                            })
            # Fallback — single shift_id on record
            if not shifts_list and shift_id and ObjectId.is_valid(shift_id):
                s = app.db.shifts.find_one({"_id": ObjectId(shift_id)})
                if s:
                    county = s.get("client_county", "")
                    shifts_list.append({
                        "id":          str(s["_id"]),
                        "date":        str(s.get("date", "")),
                        "start_time":  s.get("start_time", ""),
                        "end_time":    s.get("end_time", ""),
                        "client_name": s.get("client_name", "") or s.get("location", ""),
                        "location":    s.get("location", ""),
                        "user_type":   s.get("user_type", ""),
                        "unit":        s.get("unit") or "",
                        "shift_type":  s.get("shift_timing") or s.get("shift_type") or "",
                        "rate":        "REG" if not s.get("rate") or str(s.get("rate","")) in ("0","0.0") else str(s.get("rate","")),
                        "client_county": county,
                    })

            record["email"]      = email
            record["first_name"] = first_name
            record["last_name"]  = last_name

            threading.Thread(
                target=_send_group_shift_email,
                args=(current_app._get_current_object(), record, shifts_list, email, first_name, su_id, county),
                daemon=True
            ).start()

            triggered.append({
                "shifts_group_users_id": str(su_id),
                "user_id":               user_id,
                "staff_name":            full_name,
                "email":                 email,
            })

        return jsonify({
            **response_base,
            "status":       "triggered",
            "triggered":    len(triggered),
            "batch_size":   BATCH_SIZE,
            "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "data":         triggered,
        }), 200


    @app.route('/shift_group_booking_email/respond/<su_id>', methods=['GET'])
    def shift_group_booking_email_respond(su_id):
        answer = request.args.get('answer', '').lower()

        try:
            obj_id = ObjectId(su_id)
        except Exception:
            return "<h2>Invalid link.</h2>", 400

        record = app.db.shifts_group_users.find_one({"_id": obj_id})
        if not record:
            return "<h2>Record not found.</h2>", 404

        # Get shift info
        shift_doc = {}
        group_id  = record.get("group_id")
        if group_id:
            sg = app.db.shifts_group.find_one({"_id": group_id}, {"shift_ids": 1})
            if sg and sg.get("shift_ids"):
                s = app.db.shifts.find_one({"_id": sg["shift_ids"][0]})
                if s:
                    shift_doc = {
                        "client_name": s.get("client_name", ""),
                        "date":        _format_date(str(s.get("date", ""))),
                        "start_time":  s.get("start_time", ""),
                        "end_time":    s.get("end_time", ""),
                        "location":    s.get("location", ""),
                        "user_type":   s.get("user_type", ""),
                        "unit":        s.get("unit") or "—",
                        "shift_type":  s.get("shift_timing") or s.get("shift_type") or "—",
                    }

        base_url  = os.getenv("APP_BASE_URL", "https://uat.expresshealth.ie")
        yes_url   = f"{base_url}/shift_group_booking_email/respond/{su_id}?answer=yes"
        no_url    = f"{base_url}/shift_group_booking_email/respond/{su_id}?answer=no"
        # shift_id from URL param — for per-shift response tracking
        clicked_shift_id = request.args.get('shift_id', '')

        if answer == "yes":
            _now       = datetime.utcnow()
            _avail_new = 1
            _set_fields = {
                "response_time": _now.strftime("%Y-%m-%d %H:%M:%S"),
                "responded_at":  _now,
                "updated_at":    _now,
            }
            # Only set top-level response_text if no specific shift
            if not clicked_shift_id:
                _set_fields["availability"]  = _avail_new
                _set_fields["response_text"] = "Yes, I'm available."
            update_op = {"$set": _set_fields}

            if clicked_shift_id:
                # Check if this shift already in availability_details
                existing = record.get("availability_details") or []
                already_exists = any(str(ad.get("shift_id","")) == clicked_shift_id for ad in existing)
                if already_exists:
                    # Update existing entry
                    new_details = []
                    for ad in existing:
                        if str(ad.get("shift_id","")) == clicked_shift_id:
                            new_details.append({
                                "shift_id":     clicked_shift_id,
                                "availability": _avail_new,
                                "responded_at": _now.strftime("%Y-%m-%d %H:%M:%S"),
                            })
                        else:
                            new_details.append(ad)
                    _set_fields["availability_details"] = new_details
                else:
                    # Push new entry
                    update_op["$push"] = {
                        "availability_details": {
                            "shift_id":     clicked_shift_id,
                            "availability": _avail_new,
                            "responded_at": _now.strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    }
                # Only set top-level availability=1 if ALL shifts responded Yes
                # Otherwise keep as 8 (No Response) until all replied
            else:
                _set_fields["availability"] = _avail_new

            app.db.shifts_group_users.update_one({"_id": obj_id}, update_op)

            # Find shift details for confirmation page
            _conf_shift = {}
            if clicked_shift_id and ObjectId.is_valid(clicked_shift_id):
                _s = app.db.shifts.find_one({"_id": ObjectId(clicked_shift_id)},
                    {"client_name": 1, "location": 1, "date": 1, "start_time": 1, "end_time": 1})
                if _s:
                    _conf_shift = {
                        "client_name": _s.get("client_name", ""),
                        "location":    _s.get("location", ""),
                        "date":        _format_date(str(_s.get("date", ""))),
                        "start_time":  _s.get("start_time", ""),
                        "end_time":    _s.get("end_time", ""),
                    }
            elif shift_doc:
                _conf_shift = shift_doc

            html = f"""<html><body style="font-family:Arial;max-width:500px;margin:40px auto;text-align:center;color:#333">
  <h2 style="color:#1e7a38">&#x2705; Great, you're confirmed!</h2>
  <p>We've marked you as <strong>available</strong> for this shift.</p>
  <div style="background:#f9f9f9;border-left:4px solid #1e7a38;padding:16px;border-radius:6px;text-align:left;margin:20px 0">
    <p>&#x1F4CD; {_conf_shift.get('client_name','')}, {_conf_shift.get('location','')}</p>
    <p>&#x1F4C5; {_conf_shift.get('date','')}</p>
    <p>&#x1F550; {_conf_shift.get('start_time','')} &#x2013; {_conf_shift.get('end_time','')}</p>
  </div>
  <p>We'll let you know as soon as the facility confirms the booking.</p>
  <p style="color:#aaa;font-size:12px">Xpress Health</p>
</body></html>"""

        elif answer == "no":
            _now       = datetime.utcnow()
            _avail_new = 0
            _resp_text = "No, thanks."
            _set_fields = {
                "response_time": _now.strftime("%Y-%m-%d %H:%M:%S"),
                "responded_at":  _now,
                "updated_at":    _now,
            }
            if not clicked_shift_id:
                _set_fields["availability"]  = _avail_new
                _set_fields["response_text"] = "No, thanks."
            update_op = {"$set": _set_fields}
            if clicked_shift_id:
                existing = record.get("availability_details") or []
                already_exists = any(str(ad.get("shift_id","")) == clicked_shift_id for ad in existing)
                if already_exists:
                    new_details = []
                    for ad in existing:
                        if str(ad.get("shift_id","")) == clicked_shift_id:
                            new_details.append({"shift_id": clicked_shift_id, "availability": _avail_new, "responded_at": _now.strftime("%Y-%m-%d %H:%M:%S")})
                        else:
                            new_details.append(ad)
                    _set_fields["availability_details"] = new_details
                else:
                    update_op["$push"] = {"availability_details": {"shift_id": clicked_shift_id, "availability": _avail_new, "responded_at": _now.strftime("%Y-%m-%d %H:%M:%S")}}
            else:
                _set_fields["availability"] = _avail_new
            app.db.shifts_group_users.update_one({"_id": obj_id}, update_op)
            html = """<html><body style="font-family:Arial;max-width:500px;margin:40px auto;text-align:center;color:#333">
  <h2>&#x1F44D; No problem!</h2>
  <p>Thanks for letting us know. We'll reach out for future shifts.</p>
  <p style="color:#aaa;font-size:12px">Xpress Health</p>
</body></html>"""

        elif answer == "details":
            comments_url = f"{base_url}/shift_group_booking_email/respond/{su_id}?answer=comments"
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
      <a href="{no_url}" style="display:inline-block;background:#dc2626;color:#fff;padding:12px 22px;border-radius:6px;text-decoration:none;font-weight:700;font-size:14px;margin:4px;">❌ No, thanks</a>
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
    <form method="POST" action="{base_url}/shift_group_booking_email/respond/{su_id}">
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


    @app.route('/shift_group_booking_email/respond/<su_id>', methods=['POST'])
    def shift_group_booking_email_respond_post(su_id):
        answer  = request.form.get('answer', '')
        comment = request.form.get('comment', '').strip()

        try:
            obj_id = ObjectId(su_id)
        except Exception:
            return "<h2>Invalid link.</h2>", 400

        if answer == "save_comment" and comment:
            app.db.shifts_group_users.update_one(
                {"_id": obj_id},
                {"$set": {
                    "staff_comment":     comment,
                    "customer_feedback": comment,
                    "comment_at":        datetime.utcnow(),
                    "updated_at":        datetime.utcnow(),
                }}
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


    @app.route('/debug-shift-group-booking-email')
    def debug_shift_group_booking_email():
        allowed, now = is_within_call_window()
        return jsonify({
            "debug":          "shift_group_booking_email.py loaded",
            "server_time":    now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
            "batch_size":     BATCH_SIZE,
            "collection":     "shifts_group_users",
        })