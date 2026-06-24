# certificate_reminder_call.py

from flask import request, jsonify
from dotenv import load_dotenv
import os
import threading
from threading import Thread
import requests
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


# ── Core call function (replaces make_ai_call) ────────────────────────

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
    params = urllib.parse.urlencode(
        {
            "name":                 reminder_doc.get("name", ""),
            "phone":                phone,
            "xn_user_id":           reminder_doc.get("xn_user_id", ""),
            "user_ref_id":          reminder_doc.get("user_ref_id", ""),
            "certificates_needed":  ",".join(reminder_doc.get("certificates_needed", [])),
        },
        doseq=True,
    )

    try:
        with app.app_context():
            e164_phone    = phone.replace(" ", "")
            connection_id = os.getenv('TELNYX_CONNECTION_ID')
            socket_uri    = os.getenv('SOCKET_URI_CERT_REMINDER',
                                      os.getenv('SOCKET_URI_FOLLOWUP', ''))

            response = requests.post(
                f"https://api.telnyx.com/v2/texml/calls/{connection_id}",
                headers={
                    "Authorization": f"Bearer {os.getenv('TELNYX_API_KEY')}",
                    "Content-Type":  "application/json",
                },
                json={
                    "To":             e164_phone,
                    "From":           CALLER_ID.replace(" ", ""),
                    "Url":            f"{socket_uri}?{params}",
                    "StatusCallback": f"{BASE_URL}/call/completed",
                },
            )
            response.raise_for_status()
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
        print(f"[CERT REMINDER] Call failed for {phone}: {e}")
        # Mark as failed so it can be retried
        try:
            with app.app_context():
                app.db.certificate_reminder_calls.update_one(
                    {"_id": reminder_object_id},
                    {"$set": {
                        "call_status":  "failed",
                        "call_error":   str(e),
                        "updated_at":   datetime.utcnow(),
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
                    now_utc    = datetime.utcnow()
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