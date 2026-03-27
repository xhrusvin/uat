# shiftbookingcall_telnyx.py

from flask import current_app
from datetime import datetime, time
from threading import Thread
import threading
import pytz
import urllib
import os
import requests

# Load environment variables
TELNYX_API_KEY = os.getenv('TELNYX_API_KEY')
TELNYX_CONNECTION_ID = os.getenv('TELNYX_CONNECTION_ID')
TELNYX_FROM_NUMBER = os.getenv('TELNYX_CALLER_ID')   # ← Must be set!
BASE_URL = os.getenv('BASE_URL', 'https://app.expresshealth.ie').rstrip('/')

# Dublin timezone
DUBLIN_TZ = pytz.timezone('Europe/Dublin')

# Validate critical Telnyx settings at import time
if not TELNYX_API_KEY:
    print("WARNING: TELNYX_API_KEY is not set!")
if not TELNYX_CONNECTION_ID:
    print("WARNING: TELNYX_CONNECTION_ID is not set!")
if not TELNYX_FROM_NUMBER:
    print("ERROR: TELNYX_FROM_NUMBER is not set! Calls will fail.")

def generate_texml(user_doc: dict):
    """Generate TeXML to connect call to WebSocket stream (bidirectional RTP)"""
    from xml.etree.ElementTree import Element, SubElement, tostring
    import xml.dom.minidom as minidom

    response = Element('Response')
    connect = SubElement(response, 'Connect')

    stream = SubElement(connect, 'Stream')
    stream.set('url', f"wss://{BASE_URL.replace('https://', '')}/wss")
    stream.set('bidirectionalMode', 'rtp')
    stream.set('track', 'both_tracks')

    rough_string = tostring(response, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")


def make_shiftbooking_ai_call(app, phone: str, user_doc: dict, user_object_id, shift_id):
    """Initiate AI shift booking call using Telnyx TeXML"""
    print(f'[Shift Booking] Initiating Telnyx AI call to {phone}')

    if not TELNYX_FROM_NUMBER:
        print(f"[Shift Booking] ERROR: TELNYX_FROM_NUMBER is not configured. Cannot call {phone}")
        return

    if not TELNYX_CONNECTION_ID:
        print(f"[Shift Booking] ERROR: TELNYX_CONNECTION_ID is not configured.")
        return

    params_dict = user_doc.copy()
    params_dict['shift_id'] = str(shift_id)

    params = urllib.parse.urlencode(params_dict, doseq=True)
    texml_fetch_url = f'{BASE_URL}/shiftbookinguat?{params}'

    try:
        with app.app_context():
            # Clean phone numbers safely
            e164_phone = phone.replace(" ", "").strip()
            from_number = TELNYX_FROM_NUMBER.replace(" ", "").strip()

            if not e164_phone.startswith('+'):
                e164_phone = '+' + e164_phone

            response = requests.post(
                f"https://api.telnyx.com/v2/texml/calls/{TELNYX_CONNECTION_ID}",
                headers={
                    "Authorization": f"Bearer {TELNYX_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "To": e164_phone,
                    "From": from_number,
                    "Url": texml_fetch_url,
                    "StatusCallback": f"{BASE_URL}/call/completed",
                    "StatusCallbackMethod": "POST"
                },
                timeout=15
            )
            response.raise_for_status()
            data = response.json()

            call_id = data.get('call_sid') or data.get('id') or "unknown"
            print(f"[Shift Booking] Telnyx call initiated successfully. Call ID: {call_id} for {e164_phone}")

            # Mark as sent
            app.db.users.update_one(
                {"_id": user_object_id},
                {"$set": {"call_sent": 1, "updated_at": datetime.utcnow()}}
            )
            print(f"call_sent = 1 for user {user_object_id}")

    except requests.exceptions.RequestException as e:
        print(f"[Shift Booking] Telnyx API request failed for {phone}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                print(f"Response body: {e.response.text}")
            except:
                pass
    except Exception as e:
        print(f"[Shift Booking] Unexpected error calling {phone}: {e}")


def make_followup_ai_call(phone: str, user_doc: dict, user_id):
    """Initiate follow-up AI call"""
    print(f'[Follow-up] Initiating Telnyx follow-up call to {phone}')

    if not TELNYX_FROM_NUMBER or not TELNYX_CONNECTION_ID:
        print(f"[Follow-up] ERROR: Telnyx configuration missing. Cannot call {phone}")
        return

    params = urllib.parse.urlencode(user_doc, doseq=True)
    texml_fetch_url = f'{BASE_URL}/shiftbookinguat?{params}'

    try:
        e164_phone = phone.replace(" ", "").strip()
        from_number = TELNYX_FROM_NUMBER.replace(" ", "").strip()

        if not e164_phone.startswith('+'):
            e164_phone = '+' + e164_phone

        response = requests.post(
            f"https://api.telnyx.com/v2/texml/calls/{TELNYX_CONNECTION_ID}",
            headers={
                "Authorization": f"Bearer {TELNYX_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "To": e164_phone,
                "From": from_number,
                "Url": texml_fetch_url,
                "StatusCallback": f"{BASE_URL}/call/completed",
                "StatusCallbackMethod": "POST"
            },
            timeout=15
        )
        response.raise_for_status()
        data = response.json()
        call_id = data.get('call_sid') or data.get('id') or "unknown"
        print(f"[Follow-up] Telnyx call initiated. Call ID: {call_id} for {e164_phone}")

    except Exception as e:
        print(f"[Follow-up] Error calling {phone}: {e}")


def schedule_followup_calls(app):
    """Background thread for follow-up calls"""
    def runner():
        while True:
            try:
                with app.app_context():
                    now_utc = datetime.utcnow()
                    now_dublin = datetime.now(DUBLIN_TZ)
                    current_time = now_dublin.time()
                    is_business_hours = time(8, 0) <= current_time <= time(20, 0)

                    print(f"[Follow-up Scheduler] Checking at {now_dublin.strftime('%Y-%m-%d %H:%M')} | Business hours: {is_business_hours}")

                    query = {
                        "next_follow_up_at": {"$lte": now_utc},
                        "follow_up_sent": {"$ne": 1},
                        "phone": {"$exists": True},
                        "call_sent": 1
                    }

                    users_due = list(current_app.db.users.find(query))

                    if users_due:
                        print(f"[Follow-up] Found {len(users_due)} user(s) due.")
                    else:
                        if is_business_hours:
                            print("[Follow-up] No users due right now.")

                    for user in users_due:
                        phone = user.get("phone")
                        user_id = user["_id"]

                        if not phone:
                            continue

                        user_doc = {
                            "first_name": user.get("first_name", ""),
                            "last_name": user.get("last_name", ""),
                            "email": user.get("email", ""),
                            "phone": phone,
                            "country": user.get("country", ""),
                            "designation": user.get("designation", "")
                        }

                        if is_business_hours:
                            Thread(
                                target=make_followup_ai_call,
                                args=(phone, user_doc, user_id),
                                daemon=True
                            ).start()
                        else:
                            print(f"[Follow-up] User {phone} due, but outside business hours.")

            except Exception as e:
                print(f"[Follow-up Scheduler Error] {e}")

            threading.Event().wait(60)

    thread = Thread(target=runner, daemon=True)
    thread.start()
    print("[Follow-up Scheduler] Started successfully with Telnyx.")


def init_followup_scheduler(app):
    schedule_followup_calls(app)