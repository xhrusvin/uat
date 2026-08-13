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
    """Build shift booking email HTML with Yes/No/More Details buttons."""
    facility   = shift.get("client_name", "")
    location   = shift.get("location", facility)
    date_str   = _format_date(shift.get("date", ""))
    start_time = shift.get("start_time", "")
    end_time   = shift.get("end_time", "")
    user_type  = shift.get("user_type", "")
    shift_code = shift.get("shift_code", "")
    su_id      = str(shifts_users_id)

    yes_url     = f"{base_url}/shift_booking_email/respond/{su_id}?answer=yes"
    no_url      = f"{base_url}/shift_booking_email/respond/{su_id}?answer=no"
    details_url = f"{base_url}/shift_booking_email/respond/{su_id}?answer=details"

    return f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  body {{ font-family: Arial, sans-serif; background:#f4f4f4; margin:0; padding:0; }}
  .container {{ max-width:520px; margin:30px auto; background:#fff; border-radius:10px; overflow:hidden; box-shadow:0 2px 8px rgba(0,0,0,0.1); }}
  .header {{ background:#1e7a38; color:#fff; padding:24px 30px; }}
  .header h2 {{ margin:0; font-size:20px; }}
  .body {{ padding:28px 30px; color:#333; }}
  .body p {{ font-size:15px; line-height:1.6; }}
  .shift-card {{ background:#f9f9f9; border-left:4px solid #1e7a38; border-radius:6px; padding:16px 20px; margin:20px 0; }}
  .shift-card p {{ margin:6px 0; font-size:14px; }}
  .shift-card .icon {{ margin-right:6px; }}
  .btn-row {{ text-align:center; margin:24px 0 8px; }}
  .btn {{ display:inline-block; padding:12px 24px; border-radius:6px; text-decoration:none; font-weight:bold; font-size:14px; margin:6px 4px; }}
  .btn-yes {{ background:#1e7a38; color:#fff; }}
  .btn-no  {{ background:#e74c3c; color:#fff; }}
  .btn-details {{ background:#f0f0f0; color:#333; border:1px solid #ccc; }}
  .footer {{ text-align:center; padding:16px; color:#aaa; font-size:12px; border-top:1px solid #eee; }}
</style>
</head>
<body>
<div class="container">
  <div class="header">
    <h2>Shift Available 🏥</h2>
  </div>
  <div class="body">
    <p>Hi <strong>{first_name}</strong> 👋</p>
    <p>We're reaching out from <strong>Xpress Health</strong> to check your availability for an upcoming shift.</p>
    <p>Please review the shift details below and let us know if you're available. Your quick response helps us confirm staffing as soon as possible.</p>
    <div class="shift-card">
      <p><span class="icon">📍</span><strong>{facility}</strong>, {location}</p>
      <p><span class="icon">📅</span>{date_str}</p>
      <p><span class="icon">🕐</span>{start_time} – {end_time}</p>
      <p><span class="icon">👩‍⚕️</span>{user_type}</p>
      <p><span class="icon">🔖</span>{shift_code}</p>
    </div>
    <p style="text-align:center; font-weight:bold; font-size:15px;">Are you available for this shift?</p>
    <div class="btn-row">
      <a href="{yes_url}" class="btn btn-yes">✅ Yes, I'm available</a>
      <a href="{no_url}"  class="btn btn-no">❌ No, thanks</a>
      <a href="{details_url}" class="btn btn-details">ℹ️ More details</a>
    </div>
  </div>
  <div class="footer">
    Xpress Health · This email was sent by our automated system. Please do not reply directly.
  </div>
</div>
</body>
</html>
"""


def _send_shift_email(app, record, shift_doc, to_email, first_name, shifts_users_id):
    """Send shift booking email via SMTP."""
    try:
        base_url = os.getenv("APP_BASE_URL", "https://uat.expresshealth.ie")
        html     = _build_email_html(first_name, shift_doc, base_url, shifts_users_id)

        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"Shift Available – {shift_doc.get('client_name','')} on {_format_date(shift_doc.get('date',''))}"
        msg["From"]    = f"{os.getenv('SMTP_FROM_NAME','XpressHealth')} <{os.getenv('FROM_EMAIL','')}>"
        msg["To"]      = to_email

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

        log.info(f"[EMAIL] ✓ Sent to {to_email}")

        # Update shifts_users with email sent status
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

    # ------------------------------------------------------------------
    # 1. AUTO-TRIGGER: GET /shift_booking_email
    # ------------------------------------------------------------------
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

        # Mark as processed
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
                shift_doc = {
                    "id":          str(s["_id"]),
                    "shift_code":  s.get("shift_code") or s.get("name", ""),
                    "date":        str(s.get("date", "")),
                    "start_time":  s.get("start_time", ""),
                    "end_time":    s.get("end_time", ""),
                    "client_name": s.get("client_name", ""),
                    "location":    s.get("location", ""),
                    "user_type":   s.get("user_type", ""),
                }

        record["email"]      = email
        record["first_name"] = first_name
        record["last_name"]  = last_name
        record["full_name"]  = full_name

        # Send email in background
        threading.Thread(
            target=_send_shift_email,
            args=(current_app._get_current_object(), record, shift_doc, email, first_name, shifts_users_id),
            daemon=True
        ).start()

        # Check if all processed → end outreach
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

    # ------------------------------------------------------------------
    # 2. RESPONSE HANDLER: GET /shift_booking_email/respond/<su_id>
    # ------------------------------------------------------------------
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
            html = f"""
<html><body style="font-family:Arial;max-width:500px;margin:40px auto;text-align:center;color:#333">
  <h2 style="color:#1e7a38">✅ Great, you're confirmed!</h2>
  <p>We've marked you as <strong>available</strong> for this shift.</p>
  <div style="background:#f9f9f9;border-left:4px solid #1e7a38;padding:16px;border-radius:6px;text-align:left;margin:20px 0">
    <p>📍 {shift_doc.get('client_name')}, {shift_doc.get('location')}</p>
    <p>📅 {shift_doc.get('date')}</p>
    <p>🕐 {shift_doc.get('start_time')} – {shift_doc.get('end_time')}</p>
  </div>
  <p>We'll let you know as soon as the facility confirms the booking. You'll receive a confirmation with all the shift details.</p>
  <p style="color:#aaa;font-size:12px">Xpress Health Automated System</p>
</body></html>"""

        elif answer == "no":
            app.db.shifts_users.update_one(
                {"_id": obj_id},
                {"$set": {"availability": 0, "response_text": "No, thanks.",
                           "responded_at": datetime.utcnow(), "updated_at": datetime.utcnow()}}
            )
            html = """
<html><body style="font-family:Arial;max-width:500px;margin:40px auto;text-align:center;color:#333">
  <h2>👍 No problem!</h2>
  <p>Thanks for letting us know. We'll reach out for future shifts.</p>
  <p style="color:#aaa;font-size:12px">Xpress Health Automated System</p>
</body></html>"""

        elif answer == "details":
            html = f"""
<html><body style="font-family:Arial;max-width:500px;margin:40px auto;color:#333">
  <h2 style="color:#1e7a38">Shift Details</h2>
  <div style="background:#f9f9f9;border-left:4px solid #1e7a38;padding:16px;border-radius:6px;margin:16px 0">
    <p>📍 <strong>{shift_doc.get('client_name')}</strong>, {shift_doc.get('location')}</p>
    <p>📅 {shift_doc.get('date')}</p>
    <p>🕐 {shift_doc.get('start_time')} – {shift_doc.get('end_time')}</p>
    <p>👩‍⚕️ {shift_doc.get('user_type')}</p>
    <p>🔖 {shift_doc.get('shift_code')}</p>
  </div>
  <p style="font-weight:bold;text-align:center">Are you available for this shift?</p>
  <div style="text-align:center;margin:20px 0">
    <a href="{yes_url}" style="background:#1e7a38;color:#fff;padding:12px 24px;border-radius:6px;text-decoration:none;font-weight:bold;margin:6px">✅ Yes, I'm available</a>
    <a href="{no_url}"  style="background:#e74c3c;color:#fff;padding:12px 24px;border-radius:6px;text-decoration:none;font-weight:bold;margin:6px">❌ No, thanks</a>
  </div>
  <p style="color:#aaa;font-size:12px;text-align:center">Xpress Health Automated System</p>
</body></html>"""

        else:
            html = "<h2>Invalid response.</h2>"

        return html, 200

    # ------------------------------------------------------------------
    # 3. DEBUG
    # ------------------------------------------------------------------
    @app.route('/debug-shift-booking-email')
    def debug_shift_booking_email():
        allowed, now = is_within_call_window()
        return jsonify({
            "debug":          "shift_booking_email.py loaded",
            "server_time":    now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
        })