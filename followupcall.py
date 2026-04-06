# followupcall.py

from flask import request, jsonify, session
from telnyx import Telnyx                        # replaces: from twilio.rest import Client
from dotenv import load_dotenv
import os
import threading
from threading import Thread
import requests
import bcrypt
import re
import urllib
from datetime import datetime, time
import pytz
from flask import current_app

# Load environment variables (assuming .env is loaded in main app)
# Telnyx setup
telnyx_client = Telnyx(api_key=os.getenv('TELNYX_API_KEY'))
CALLER_ID = os.getenv('TELNYX_CALLER_ID')
TELNYX_CONNECTION_ID = os.getenv('TELNYX_CONNECTION_ID')  # required by Telnyx
BASE_URL = os.getenv('BASE_URL', 'https://app.expresshealth.ie').rstrip('/')


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

def make_followup_ai_call(app, phone: str, user_doc: dict, user_object_id):
    params = urllib.parse.urlencode(user_doc, doseq=True)
    try:
        with app.app_context():
            e164_phone = phone.replace(" ", "")
            connection_id = os.getenv('TELNYX_CONNECTION_ID')
            
            response = requests.post(
                f"https://api.telnyx.com/v2/texml/calls/{connection_id}",
                headers={
                    "Authorization": f"Bearer {os.getenv('TELNYX_API_KEY')}",
                    "Content-Type": "application/json"
                },
                json={
                    "To": e164_phone,
                    "From": CALLER_ID.replace(" ", ""),
                    "Url": f'https://app.expresshealth.ie/voice1_uat?{params}',
                    "StatusCallback": f'https://app.expresshealth.ie/call/completed'
                }
            )
            response.raise_for_status()
            data = response.json()
            print(f"TeXML call initiated: {data['call_sid']} for {e164_phone}")

            app.db.users.update_one(
                {"_id": user_object_id},
                {"$set": {"call_sent": 1, "updated_at": datetime.utcnow()}}
            )
    except Exception as e:
        print(f"Call failed: {e}")

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
                                target=make_followup_ai_call,
                                args=(app, phone, user_doc, user_id),  # ✅
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