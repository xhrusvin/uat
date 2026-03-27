# shiftbookingcall_telnyx.py

from flask import current_app
from datetime import datetime, time
from threading import Thread
import threading
import pytz
import urllib
import os

# Load environment variables
TELNYX_API_KEY = os.getenv('TELNYX_API_KEY')
TELNYX_FROM_NUMBER = os.getenv('TELNYX_FROM_NUMBER')      # Your Telnyx phone number (e.g. +353...)
BASE_URL = os.getenv('BASE_URL', 'https://app.expresshealth.ie').rstrip('/')

# Telnyx Client
import telnyx
telnyx.api_key = TELNYX_API_KEY

# Dublin timezone
DUBLIN_TZ = pytz.timezone('Europe/Dublin')

def generate_texml(user_doc: dict):
    """Generate TeXML to connect call to WebSocket stream (bidirectional)"""
    from xml.etree.ElementTree import Element, SubElement, tostring
    import xml.dom.minidom as minidom

    response = Element('Response')
    connect = SubElement(response, 'Connect')

    stream = SubElement(connect, 'Stream')
    stream.set('url', f"wss://{BASE_URL.replace('https://', '')}/wss")
    stream.set('bidirectionalMode', 'rtp')      # Important: enables sending audio back
    stream.set('track', 'both_tracks')         # or 'inbound_track' if you prefer

    # Pretty print XML
    rough_string = tostring(response, 'utf-8')
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent="  ")

def make_shiftbooking_ai_call(app, phone: str, user_doc: dict, user_object_id, shift_id):
    """Initiate AI call using Telnyx and mark call_sent = 1 on success"""
    print('Initiating Telnyx AI call for', phone)

    params_dict = user_doc.copy()
    params_dict['shift_id'] = str(shift_id)        # convert ObjectId to string

    # Encode parameters to pass to your TeXML endpoint
    params = urllib.parse.urlencode(params_dict, doseq=True)
    texml_fetch_url = f'{BASE_URL}/shiftbookinguat?{params}'

    try:
        with app.app_context():
            # Create outbound call using TeXML
            call = telnyx.Call.create(
                to=phone,
                from_=TELNYX_FROM_NUMBER,
                connection_id=None,                    # Optional: if using specific connection
                # For TeXML outbound, you can use texml_app_id or let the outbound profile handle it
                # Alternatively, use webhook_url if you prefer webhook-based control
                webhook_url=texml_fetch_url,           # Telnyx will GET/POST this URL to fetch TeXML
                # You can also specify a pre-configured TeXML Application ID if preferred
            )

            print(f"Telnyx AI Call initiated successfully. Call Control ID: {call.id} for {phone}")

            # Mark as sent in MongoDB
            app.db.users.update_one(
                {"_id": user_object_id},
                {"$set": {"call_sent": 1, "updated_at": datetime.utcnow()}}
            )
            print(f"call_sent = 1 for user {user_object_id}")

    except Exception as e:
        print(f"Telnyx Call failed for {phone}: {e}")
        # Do NOT mark as sent if call failed

def make_followup_ai_call(phone: str, user_doc: dict, user_id):
    """Similar function for follow-up calls (you can merge with above if preferred)"""
    print(f"[Follow-up] Initiating Telnyx follow-up call to {phone}")

    params = urllib.parse.urlencode(user_doc, doseq=True)
    texml_fetch_url = f'https://app.expresshealth.ie/shiftbookinguat?{params}'

    try:
        call = telnyx.Call.create(
            to=phone,
            from_=TELNYX_FROM_NUMBER,
            webhook_url=texml_fetch_url,
        )
        print(f"[Follow-up] Telnyx call initiated. ID: {call.id}")
    except Exception as e:
        print(f"[Follow-up] Telnyx call failed for {phone}: {e}")

def schedule_followup_calls(app):
    """Background thread: check for due follow-ups every 60 seconds"""
    def runner():
        while True:
            try:
                with app.app_context():
                    now_utc = datetime.utcnow()
                    now_dublin = datetime.now(DUBLIN_TZ)
                    current_time = now_dublin.time()
                    is_business_hours = time(8, 0) <= current_time <= time(20, 0)

                    print(f"[Follow-up Scheduler] Checking at {now_dublin.strftime('%Y-%m-%d %H:%M')} Dublin time | Business hours: {is_business_hours}")

                    query = {
                        "next_follow_up_at": {"$lte": now_utc},
                        "follow_up_sent": {"$ne": 1},
                        "phone": {"$exists": True},
                        "call_sent": 1
                    }

                    users_due = list(current_app.db.users.find(query))

                    if users_due:
                        print(f"[Follow-up] Found {len(users_due)} user(s) due for follow-up.")
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

# Call this once during app initialization
def init_followup_scheduler(app):
    schedule_followup_calls(app)