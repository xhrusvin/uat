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

ALLOWED_START_HOUR = 0
ALLOWED_END_HOUR   = 23


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


def _build_email_html(first_name, shift, base_url, shifts_users_id):
    """Load and render shift booking email from HTML template file."""
    facility   = shift.get("client_name", "")
    address    = shift.get("client_address", "")
    county     = shift.get("client_county", "")
    lat        = shift.get("client_lat", "")
    lng        = shift.get("client_lng", "")
    date_str   = _format_date(shift.get("date", ""))
    start_time = shift.get("start_time", "")
    end_time   = shift.get("end_time", "")
    user_type  = shift.get("user_type", "")
    su_id      = str(shifts_users_id)
    logo_url   = "https://uat.expresshealth.ie/static/image/logo.png"
    map_url    = f"https://www.google.com/maps?q={lat},{lng}" if lat and lng else ""

    yes_url     = f"{base_url}/shift_booking_email/respond/{su_id}?answer=yes"
    no_url      = f"{base_url}/shift_booking_email/respond/{su_id}?answer=no"
    details_url = f"{base_url}/shift_booking_email/respond/{su_id}?answer=details"

    # Load template file
    template_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "templates", "booking", "shifts_booking_email.html"
    )

    try:
        with open(template_path, "r", encoding="utf-8") as f:
            html = f.read()
    except FileNotFoundError:
        log.error(f"[EMAIL] Template not found: {template_path}")
        raise FileNotFoundError(f"Email template missing: {template_path}")

    html = html.replace("{{first_name}}", first_name)
    html = html.replace("{{facility}}", facility)
    html = html.replace("{{address}}", address)
    html = html.replace("{{county}}", county)
    html = html.replace("{{date_str}}", date_str)
    html = html.replace("{{start_time}}", start_time)
    html = html.replace("{{end_time}}", end_time)
    html = html.replace("{{user_type}}", user_type)
    html = html.replace("{{map_url}}", map_url)
    html = html.replace("{{yes_url}}", yes_url)
    html = html.replace("{{no_url}}", no_url)
    html = html.replace("{{details_url}}", details_url)
    html = html.replace("{{logo_url}}", logo_url)
    html = html.replace("{{base_url}}", base_url)

    return html


def _send_shift_email(app, record, shift_doc, to_email, first_name, shifts_users_id):
    """Send shift booking email via SMTP."""
    try:
        base_url = os.getenv("APP_BASE_URL", "https://uat.expresshealth.ie")
        html     = _build_email_html(first_name, shift_doc, base_url, shifts_users_id)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Shift Available – {shift_doc.get('client_name','')} on {_format_date(shift_doc.get('date',''))}"
        msg["From"]    = f"{os.getenv('SHIFT_SMTP_FROM_NAME','XpressHealth')} <{os.getenv('SHIFT_FROM_EMAIL','')}>"
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
        app.db.shifts_users.update_one(
            {"_id": shifts_users_id},
            {"$set": {"email_sent": 1, "email_sent_at": datetime.utcnow()}}
        )

    except Exception as e:
        log.error(f"[EMAIL] ✗ Failed to send to {to_email}: {e}")
        app.db.shifts_users.update_one(
            {"_id": shifts_users_id},
            {"$set": {"email_error": str(e)}}
        )


def register_shift_booking_email_routes(app):

    @app.route('/shift_booking_email', methods=['GET'])
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

        record = app.db.shifts_users.find_one(query, sort=[("assigned_at", 1)])

        if not record:
            return jsonify({**response_base, "status": "no_pending", "message": "No pending emails."}), 200

        if record.get("call_processed") == 1 and not user_id_param:
            return jsonify({**response_base, "status": "already_processed", "message": "Already processed."}), 200

        shifts_users_id = record["_id"]
        shift_id        = str(record.get("shift_id",    ""))
        outreach_id     = str(record.get("outreach_id", ""))
        user_id         = str(record.get("user_id",     ""))

        user = None
        if user_id and ObjectId.is_valid(user_id):
            user = app.db.users.find_one(
                {"_id": ObjectId(user_id)},
                {"email": 1, "first_name": 1, "last_name": 1}
            )

        if not user:
            return jsonify({**response_base, "status": "no_user",
                            "message": "User not found.", "shifts_users_id": str(shifts_users_id)}), 200

        email      = user.get("email")
        first_name = user.get("first_name", "")
        last_name  = user.get("last_name",  "")
        full_name  = f"{first_name} {last_name}".strip()

        if not email:
            return jsonify({**response_base, "status": "no_email",
                            "message": "No email found.", "shifts_users_id": str(shifts_users_id)}), 200

        result = app.db.shifts_users.update_one(
            {"_id": shifts_users_id},
            {"$set": {"call_processed": 1, "call_processed_at": datetime.utcnow(),
                      "updated_at": datetime.utcnow()}}
        )
        if result.modified_count == 0:
            return jsonify({**response_base, "status": "failed", "message": "Failed to update record."}), 500

        shift_doc = {}
        if shift_id and ObjectId.is_valid(shift_id):
            s = app.db.shifts.find_one({"_id": ObjectId(shift_id)})
            if s:
                # Join client data
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
                    "user_type":      s.get("user_type", ""),
                    "client_address": client.get("address", "") if client else "",
                    "client_county":  client.get("county", "") if client else "",
                    "client_lat":     client.get("latitude", "") if client else "",
                    "client_lng":     client.get("longitude", "") if client else "",
                }

        record["email"]      = email
        record["first_name"] = first_name
        record["last_name"]  = last_name
        record["full_name"]  = full_name

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

        return jsonify({
            **response_base,
            "status":          "triggered",
            "shifts_users_id": str(shifts_users_id),
            "shift_id":        shift_id,
            "outreach_id":     outreach_id,
            "user_id":         user_id,
            "staff_name":      full_name,
            "email":           email,
            "triggered_at":    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
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
                    "client_name": s.get("client_name", ""),
                    "date":        _format_date(str(s.get("date", ""))),
                    "start_time":  s.get("start_time", ""),
                    "end_time":    s.get("end_time", ""),
                    "location":    s.get("location", ""),
                    "user_type":   s.get("user_type", ""),
                    "shift_code":  s.get("shift_code", ""),
                }

        base_url = os.getenv("APP_BASE_URL", "https://uat.expresshealth.ie")
        yes_url  = f"{base_url}/shift_booking_email/respond/{shifts_users_id}?answer=yes"
        no_url   = f"{base_url}/shift_booking_email/respond/{shifts_users_id}?answer=no"

        if answer == "yes":
            app.db.shifts_users.update_one(
                {"_id": obj_id},
                {"$set": {"availability": 1, "response_text": "Yes, I'm available.",
                           "responded_at": datetime.utcnow(), "updated_at": datetime.utcnow()}}
            )
            threading.Thread(
                target=_check_and_end_outreach,
                args=(app, shift_id, outreach_id), daemon=True
            ).start()
            html = f"""<html><body style="font-family:Arial;max-width:500px;margin:40px auto;text-align:center;color:#333">
  <h2 style="color:#1e7a38">&#x2705; Great, you're confirmed!</h2>
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
            app.db.shifts_users.update_one(
                {"_id": obj_id},
                {"$set": {"availability": 0, "response_text": "No, thanks.",
                           "responded_at": datetime.utcnow(), "updated_at": datetime.utcnow()}}
            )
            html = """<html><body style="font-family:Arial;max-width:500px;margin:40px auto;text-align:center;color:#333">
  <h2>&#x1F44D; No problem!</h2>
  <p>Thanks for letting us know. We'll reach out for future shifts.</p>
  <p style="color:#aaa;font-size:12px">Xpress Health</p>
</body></html>"""

        elif answer == "details":
            html = f"""<html><body style="font-family:Arial;max-width:500px;margin:40px auto;color:#333">
  <h2 style="color:#1e7a38">Shift Details</h2>
  <div style="background:#f9f9f9;border-left:4px solid #1e7a38;padding:16px;border-radius:6px;margin:16px 0">
    <p>&#x1F4CD; <strong>{shift_doc.get('client_name')}</strong>, {shift_doc.get('location')}</p>
    <p>&#x1F4C5; {shift_doc.get('date')}</p>
    <p>&#x1F550; {shift_doc.get('start_time')} &#x2013; {shift_doc.get('end_time')}</p>
    <p>&#x1F469;&#x200D;&#x2695;&#xFE0F; {shift_doc.get('user_type')}</p>
    <p>&#x1F516; {shift_doc.get('shift_code')}</p>
  </div>
  <p style="font-weight:bold;text-align:center">Are you available for this shift?</p>
  <div style="text-align:center;margin:20px 0">
    <a href="{yes_url}" style="background:#1e7a38;color:#fff;padding:12px 24px;border-radius:6px;text-decoration:none;font-weight:bold;margin:6px;display:inline-block">Yes, I'm available</a>
    <a href="{no_url}"  style="background:#e74c3c;color:#fff;padding:12px 24px;border-radius:6px;text-decoration:none;font-weight:bold;margin:6px;display:inline-block">No, thanks</a>
  </div>
  <p style="color:#aaa;font-size:12px;text-align:center">Xpress Health</p>
</body></html>"""

        else:
            html = "<h2>Invalid response.</h2>"

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