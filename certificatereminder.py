# certificate_reminder_call.py

from flask import request, jsonify
from dotenv import load_dotenv
import os
import re
import threading
import traceback
from threading import Thread
import requests
from requests.exceptions import HTTPError
import urllib
from datetime import datetime, time
import pytz
from flask import current_app

# ── Telnyx setup ──────────────────────────────────────────────────────
CALLER_ID            = os.getenv('TELNYX_CALLER_ID')
TELNYX_CONNECTION_ID = os.getenv('TELNYX_CONNECTION_ID')
BASE_URL             = os.getenv('BASE_URL', 'https://app.expresshealth.ie').rstrip('/')

# Dublin timezone
DUBLIN_TZ = pytz.timezone('Europe/Dublin')


# ── Phone number normalisation ────────────────────────────────────────

def normalize_e164(phone: str) -> str:
    """
    Strip all non-digit characters except a leading '+'.
    Returns a clean E.164 string, e.g. '+916282908578'.

    Handles:
      '+91 6282908578'   → '+916282908578'
      '+91-628-290-8578' → '+916282908578'
      '+91 (628) 290 8578' → '+916282908578'
      '00916282908578'   → '+916282908578'  (converts leading 00 → +)
      ' +916282908578 '  → '+916282908578'  (strips surrounding whitespace)
    """
    if not phone:
        return ""

    # Strip surrounding whitespace (including non-breaking spaces \xa0)
    phone = phone.strip().replace('\xa0', '').replace('\u200b', '')

    # Convert leading '00' international prefix to '+'
    if phone.startswith('00'):
        phone = '+' + phone[2:]

    # Keep only digits and the leading '+'
    cleaned = re.sub(r'[^\d+]', '', phone)

    # Ensure there is exactly one '+' at the start
    if not cleaned.startswith('+'):
        cleaned = '+' + cleaned

    return cleaned


def is_valid_e164(phone: str) -> bool:
    """
    Validate that the number matches E.164: + followed by 7–15 digits.
    Covers local (7-digit) to international (15-digit) numbers.
    """
    return bool(re.match(r'^\+\d{7,15}$', phone))


# ── Core call function ────────────────────────────────────────────────

def make_certificate_reminder_call(app, phone: str, reminder_doc: dict, reminder_object_id):
    """
    Fire a Telnyx call for a certificate reminder record.

    reminder_doc fields used:
      - phone
      - name
      - certificates_needed  (list)
      - xn_user_id
      - user_ref_id
    """
    # ── Normalise & validate the destination number ───────────────────
    e164_phone = normalize_e164(phone)

    if not is_valid_e164(e164_phone):
        msg = f"Invalid phone number '{phone}' → normalised to '{e164_phone}' — skipping call."
        print(f"[CERT REMINDER] {msg}")
        try:
            with app.app_context():
                app.db.certificate_reminder_calls.update_one(
                    {"_id": reminder_object_id},
                    {"$set": {
                        "call_status": "failed",
                        "call_error":  msg,
                        "updated_at":  datetime.utcnow(),
                    }}
                )
        except Exception:
            pass
        return

    # ── Normalise the caller-ID as well ──────────────────────────────
    caller_id_raw = os.getenv('TELNYX_CALLER_ID', '')
    caller_id     = normalize_e164(caller_id_raw) if caller_id_raw else ''

    params = urllib.parse.urlencode(
        {
            "name":                reminder_doc.get("name", ""),
            "phone":               e164_phone,
            "xn_user_id":          reminder_doc.get("xn_user_id", ""),
            "user_ref_id":         reminder_doc.get("user_ref_id", ""),
            "certificates_needed": ",".join(reminder_doc.get("certificates_needed", [])),
        },
        doseq=True,
    )

    try:
        with app.app_context():
            connection_id = os.getenv('TELNYX_CONNECTION_ID')
            socket_uri    = os.getenv('SOCKET_URI_CERT_REMINDER',
                                      os.getenv('SOCKET_URI_FOLLOWUP', ''))

            # Debug log — shows exactly what is sent to Telnyx
            print(
                f"[CERT REMINDER] Dialling → "
                f"To={e164_phone!r}  From={caller_id!r}  "
                f"name={reminder_doc.get('name', '')!r}"
            )

            response = requests.post(
                f"https://api.telnyx.com/v2/texml/calls/{connection_id}",
                headers={
                    "Authorization": f"Bearer {os.getenv('TELNYX_API_KEY')}",
                    "Content-Type":  "application/json",
                },
                json={
                    "To":             e164_phone,
                    "From":           caller_id,
                    "Url":            f"{socket_uri}?{params}",
                    "StatusCallback": f"{BASE_URL}/call/completed",
                },
            )
            try:
                response.raise_for_status()
            except HTTPError as http_err:
                # ── Capture the full Telnyx error response ────────────────
                status_code = response.status_code
                raw_body    = response.text          # always available
                try:
                    telnyx_detail = response.json()  # structured if JSON
                except Exception:
                    telnyx_detail = None

                error_detail = {
                    "type":          "http_error",
                    "status_code":   status_code,
                    "exception":     str(http_err),
                    "response_body": raw_body,
                    "telnyx_detail": telnyx_detail,
                    "request_to":    e164_phone,
                    "request_from":  caller_id,
                }
                print(
                    f"[CERT REMINDER] HTTP {status_code} for {e164_phone}: "
                    f"{raw_body[:500]}"
                )
                try:
                    with app.app_context():
                        app.db.certificate_reminder_calls.update_one(
                            {"_id": reminder_object_id},
                            {"$set": {
                                "call_status": "failed",
                                "call_error":  error_detail,
                                "updated_at":  datetime.utcnow(),
                            }}
                        )
                except Exception:
                    pass
                return

            data = response.json()
            print(f"[CERT REMINDER] Call initiated: {data.get('call_sid')} → {e164_phone}")

            # Mark as called in certificate_reminder_calls
            app.db.certificate_reminder_calls.update_one(
                {"_id": reminder_object_id},
                {"$set": {
                    "call_status":  "triggered",
                    "triggered_at": datetime.utcnow(),
                    "updated_at":   datetime.utcnow(),
                }}
            )

    except Exception as e:
        # ── Non-HTTP failures (connection error, missing env var, etc.) ──
        tb = traceback.format_exc()
        print(f"[CERT REMINDER] Call failed for {e164_phone}: {e}\n{tb}")
        error_detail = {
            "type":      "exception",
            "exception": str(e),
            "traceback": tb,
        }
        try:
            with app.app_context():
                app.db.certificate_reminder_calls.update_one(
                    {"_id": reminder_object_id},
                    {"$set": {
                        "call_status": "failed",
                        "call_error":  error_detail,
                        "updated_at":  datetime.utcnow(),
                    }}
                )
        except Exception:
            pass


# ── Background scheduler ──────────────────────────────────────────────

def schedule_certificate_reminder_calls(app):
    """
    Background thread: polls certificate_reminder_calls every 60 seconds
    and fires calls for all pending records — only during Dublin business hours.
    """
    def runner():
        while True:
            try:
                with app.app_context():
                    now_dublin = datetime.now(DUBLIN_TZ)
                    curr_time  = now_dublin.time()
                    is_hours   = time(8, 0) <= curr_time <= time(20, 0)

                    print(
                        f"[CERT REMINDER Scheduler] {now_dublin.strftime('%Y-%m-%d %H:%M')} "
                        f"Dublin | Business hours: {is_hours}"
                    )

                    if not is_hours:
                        threading.Event().wait(60)
                        continue

                    pending = list(
                        current_app.db.certificate_reminder_calls.find(
                            {"call_status": "pending"},
                            sort=[("created_at", 1)],
                        )
                    )

                    if not pending:
                        print("[CERT REMINDER Scheduler] No pending reminders.")
                    else:
                        print(f"[CERT REMINDER Scheduler] {len(pending)} pending — dispatching.")

                    for reminder in pending:
                        phone = reminder.get("phone")
                        if not phone:
                            continue

                        Thread(
                            target=make_certificate_reminder_call,
                            args=(app, phone, reminder, reminder["_id"]),
                            daemon=True,
                        ).start()

            except Exception as e:
                print(f"[CERT REMINDER Scheduler Error] {e}")

            threading.Event().wait(60)

    Thread(target=runner, daemon=True).start()
    print("[CERT REMINDER Scheduler] Started.")


def init_certificate_reminder_scheduler(app):
    """Call once during app initialisation."""
    schedule_certificate_reminder_calls(app)