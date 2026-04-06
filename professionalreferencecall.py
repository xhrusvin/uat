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

# Call this once during app initialization
def init_followup_scheduler(app):
    schedule_followup_calls(app)