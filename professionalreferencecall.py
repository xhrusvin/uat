# compliancedocumentcall.py

from flask import current_app
from twilio.rest import Client as TwilioClient
from datetime import datetime, time
from threading import Thread
import threading
import pytz
import urllib
import os

# Load environment variables (assuming .env is loaded in main app)
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
CALLER_ID = os.getenv('TWILIO_CALLER_ID')
BASE_URL = os.getenv('BASE_URL', 'https://app.expresshealth.ie').rstrip('/')

# Twilio Client (will be used inside app context)
twilio_client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

# Dublin timezone
DUBLIN_TZ = pytz.timezone('Europe/Dublin')

def generate_twiml(user_doc: dict):
    """Generate TwiML to connect call to WebSocket stream"""
    from twilio.twiml.voice_response import VoiceResponse, Connect, Stream
    resp = VoiceResponse()
    connect = Connect()
    connect.stream(url="wss://app.expresshealth.ie/wss")
    resp.append(connect)
    return str(resp)

def make_professional_reference_ai_call(app, phone: str, user_doc: dict, user_object_id):
    """Initiate AI call and mark call_sent = 1 on success"""
    print('calls rusvin')
    params = urllib.parse.urlencode(user_doc, doseq=True)
    try:
        with app.app_context():
            call = twilio_client.calls.create(
                to=phone,
                from_=CALLER_ID,
                url=f'https://app.expresshealth.ie/voice3?{params}'
            )
            print(f"AI Call initiated: {call.sid} for {phone}")

            # Mark as sent
            app.db.users.update_one(
                {"_id": user_object_id},
                {"$set": {"call_sent": 1, "updated_at": datetime.utcnow()}}
            )
            print(f"call_sent = 1 for user {user_object_id}")

    except Exception as e:
        print(f"Call failed: {e}")
        # Do NOT mark as sent if call failed

def schedule_professional_reference_call_calls(app):
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

                    # Find users ready for follow-up
                    query = {
                        "next_follow_up_at": {"$lte": now_utc},
                        "follow_up_sent": {"$ne": 1},  # Not already sent
                        "phone": {"$exists": True},
                        "call_sent": 1  # Optional: only users who had initial call
                    }

                    users_due = list(current_app.db.users.find(query))

                    if not users_due:
                        if is_business_hours:
                            print("[Follow-up] No users due for follow-up right now.")
                    else:
                        print(f"[Follow-up] Found {len(users_due)} user(s) due for follow-up.")

                    for user in users_due:
                        phone = user.get("phone")
                        user_id = user["_id"]

                        if not phone:
                            continue

                        # Build minimal user_doc for TwiML params (same as registration)
                        user_doc = {
                            "first_name": user.get("first_name", ""),
                            "last_name": user.get("last_name", ""),
                            "email": user.get("email", ""),
                            "phone": phone,
                            "country": user.get("country", ""),
                            "designation": user.get("designation", "")
                        }

                        if is_business_hours:
                            # Call immediately in separate thread
                            Thread(
                                target=make_professional_reference_ai_call,
                                args=(phone, user_doc, user_id),
                                daemon=True
                            ).start()
                        else:
                            print(f"[Follow-up] User {phone} is due, but outside business hours. Will try tomorrow.")

            except Exception as e:
                print(f"[Follow-up Scheduler Error] {e}")

            # Wait 60 seconds before next check
            threading.Event().wait(60)

    # Start the background thread
    thread = Thread(target=runner, daemon=True)
    thread.start()
    print("[Follow-up Scheduler] Started successfully.")

# Call this once during app initialization
def init_followup_scheduler(app):
    schedule_followup_calls(app)