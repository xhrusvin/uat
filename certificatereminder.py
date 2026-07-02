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
from datetime import datetime, time, timedelta
import pytz
from flask import current_app

# ── Telnyx setup ──────────────────────────────────────────────────────
CALLER_ID            = os.getenv('TELNYX_CALLER_ID')
TELNYX_CONNECTION_ID = os.getenv('TELNYX_CONNECTION_ID')
BASE_URL             = os.getenv('BASE_URL', 'https://app.expresshealth.ie').rstrip('/')

# Dublin timezone
DUBLIN_TZ = pytz.timezone('Europe/Dublin')

# ── Retry config ──────────────────────────────────────────────────────
# Any call_outcome other than this is treated as non-success → schedule retry
SUCCESS_OUTCOME   = "Call Success"
RETRY_DELAY_HOURS = 1        # hours between retry attempts
MAX_RETRIES       = 10       # stop retrying after this many attempts

# ── Batch config ──────────────────────────────────────────────────────
BATCH_SIZE        = 10       # calls per batch
BATCH_INTERVAL_S  = 180      # seconds between batches (3 minutes)


# ── Phone number normalisation ────────────────────────────────────────

def normalize_e164(phone: str) -> str:
    """
    Strip all non-digit characters except a leading '+'.
    Returns a clean E.164 string, e.g. '+916282908578'.
    """
    if not phone:
        return ""
    phone = phone.strip().replace('\xa0', '').replace('\u200b', '')
    if phone.startswith('00'):
        phone = '+' + phone[2:]
    cleaned = re.sub(r'[^\d+]', '', phone)
    if not cleaned.startswith('+'):
        cleaned = '+' + cleaned
    return cleaned


def is_valid_e164(phone: str) -> bool:
    return bool(re.match(r'^\+\d{7,15}$', phone))


# ── Core call function ────────────────────────────────────────────────

def make_certificate_reminder_call(app, phone: str, reminder_doc: dict, reminder_object_id):
    """
    Fire a Telnyx call for a certificate reminder record.

    reminder_doc fields used:
      - phone
      - name
      - certificates_needed  (list — e.g. ["PCC"], ["Garda Vetting"], ["Occupational Certificate"], or any combination)
      - xn_user_id
      - user_ref_id
    """
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

            print(
                f"[CERT REMINDER] Dialling → "
                f"To={e164_phone!r}  From={caller_id!r}  "
                f"name={reminder_doc.get('name', '')!r}  "
                f"retry={reminder_doc.get('retry_count', 0)}"
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
                status_code   = response.status_code
                raw_body      = response.text
                try:
                    telnyx_detail = response.json()
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

            # Mark as triggered — outcome will be updated by the callback
            app.db.certificate_reminder_calls.update_one(
                {"_id": reminder_object_id},
                {"$set": {
                    "call_status":       "triggered",
                    "last_triggered_at": datetime.utcnow(),
                    "updated_at":        datetime.utcnow(),
                }}
            )

    except Exception as e:
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


# ── Call outcome handler (called from your /call/completed webhook) ───

def handle_certificate_reminder_outcome(app, reminder_id, outcome: str):
    """
    Called when ElevenLabs/Telnyx posts back the call result.
    - outcome == "Call Success"  → mark completed, clear retry fields.
    - anything else              → store outcome as status, schedule a retry
                                   unless MAX_RETRIES has been reached.

    Wire this into your existing /call/completed route, e.g.:
        from certificatereminder import handle_certificate_reminder_outcome
        handle_certificate_reminder_outcome(app, reminder_id, outcome)
    """
    now = datetime.utcnow()
    col = app.db.certificate_reminder_calls

    try:
        obj_id   = reminder_id if not isinstance(reminder_id, str) else __import__('bson').ObjectId(reminder_id)
        reminder = col.find_one({"_id": obj_id})
        if not reminder:
            print(f"[CERT REMINDER] outcome callback: reminder {reminder_id} not found")
            return

        if outcome == SUCCESS_OUTCOME:
            col.update_one(
                {"_id": obj_id},
                {"$set": {
                    "call_status":  "completed",
                    "call_outcome": outcome,
                    "updated_at":   now,
                },
                "$unset": {"retry_after": ""}}
            )
            print(f"[CERT REMINDER] {reminder.get('name')} → completed (Call Success)")
            return

        # Non-success outcome — voicemail, no-answer, etc.
        retry_count = reminder.get("retry_count", 0) + 1

        if retry_count > MAX_RETRIES:
            col.update_one(
                {"_id": obj_id},
                {"$set": {
                    "call_status":  "max_retries_reached",
                    "call_outcome": outcome,
                    "retry_count":  retry_count - 1,   # don't increment past max
                    "updated_at":   now,
                }}
            )
            print(
                f"[CERT REMINDER] {reminder.get('name')} → max retries reached "
                f"(outcome={outcome!r})"
            )
            return

        retry_after = now + timedelta(hours=RETRY_DELAY_HOURS)

        # Use outcome string directly as status label (e.g. "Voicemail Detected")
        # truncated to 64 chars for safety
        status_label = outcome[:64] if outcome else "non_success"

        col.update_one(
            {"_id": obj_id},
            {"$set": {
                "call_status":  status_label,
                "call_outcome": outcome,
                "retry_count":  retry_count,
                "retry_after":  retry_after,
                "updated_at":   now,
            }}
        )
        print(
            f"[CERT REMINDER] {reminder.get('name')} → outcome={outcome!r}, "
            f"retry {retry_count}/{MAX_RETRIES} scheduled at {retry_after.strftime('%H:%M UTC')}"
        )

    except Exception as e:
        print(f"[CERT REMINDER] handle_outcome error for {reminder_id}: {e}")


# ── Background scheduler ──────────────────────────────────────────────

def schedule_certificate_reminder_calls(app):
    """
    Background thread — runs continuously, waking every 60 s.

    On each wake-up it processes ALL eligible reminders in batches of
    BATCH_SIZE (default 10), pausing BATCH_INTERVAL_S (default 180 s =
    3 min) between batches.  After all batches are done the thread sleeps
    until the next 60-second tick.

    Eligible reminders are:
      1. call_status == "pending"   (fresh imports, resets)
      2. call_status not in terminal set AND retry_after <= now
         (voicemail / non-success retries whose wait window has elapsed)

    Only runs during Dublin business hours (08:00–20:00).
    """
    TERMINAL_STATUSES = {"completed", "failed", "max_retries_reached"}

    def _fetch_eligible(db):
        now = datetime.utcnow()
        return list(
            db.certificate_reminder_calls.find(
                {"$or": [
                    {"call_status": "pending"},
                    {"call_status": {"$nin": list(TERMINAL_STATUSES) + ["pending", "triggered"]},
                     "retry_after": {"$lte": now}},
                ]},
                sort=[("created_at", 1)],
            )
        )

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

                    eligible = _fetch_eligible(current_app.db)

                    if not eligible:
                        print("[CERT REMINDER Scheduler] No eligible reminders.")
                        threading.Event().wait(60)
                        continue

                    total = len(eligible)
                    print(
                        f"[CERT REMINDER Scheduler] {total} eligible — "
                        f"dispatching in batches of {BATCH_SIZE} "
                        f"({BATCH_INTERVAL_S}s gap)."
                    )

                    for batch_start in range(0, total, BATCH_SIZE):
                        batch = eligible[batch_start: batch_start + BATCH_SIZE]
                        batch_num = batch_start // BATCH_SIZE + 1

                        print(
                            f"[CERT REMINDER Scheduler] Batch {batch_num}: "
                            f"firing {len(batch)} call(s)."
                        )

                        for reminder in batch:
                            phone = reminder.get("phone")
                            if not phone:
                                continue

                            # Mark as triggered immediately so a concurrent
                            # scheduler wake-up won't double-fire it
                            current_app.db.certificate_reminder_calls.update_one(
                                {"_id": reminder["_id"]},
                                {"$set": {
                                    "call_status": "triggered",
                                    "updated_at":  datetime.utcnow(),
                                }}
                            )

                            Thread(
                                target=make_certificate_reminder_call,
                                args=(app, phone, reminder, reminder["_id"]),
                                daemon=True,
                            ).start()

                        # Pause between batches (but not after the last one)
                        if batch_start + BATCH_SIZE < total:
                            print(
                                f"[CERT REMINDER Scheduler] Waiting {BATCH_INTERVAL_S}s "
                                f"before next batch…"
                            )
                            threading.Event().wait(BATCH_INTERVAL_S)

            except Exception as e:
                print(f"[CERT REMINDER Scheduler Error] {e}")

            threading.Event().wait(60)

    Thread(target=runner, daemon=True).start()
    print("[CERT REMINDER Scheduler] Started.")


def init_certificate_reminder_scheduler(app):
    """Call once during app initialisation."""
    schedule_certificate_reminder_calls(app)
