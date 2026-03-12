# gemini_call/call_service.py

from twilio.rest import Client
import os
from dotenv import load_dotenv

from flask import url_for

load_dotenv()

TWILIO_ACCOUNT_SID    = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN     = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER   = os.getenv("TWILIO_CALLER_ID")

if not all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER]):
    raise RuntimeError("Missing Twilio credentials")

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


def make_reminder_call(to_number: str):
    """
    Start outbound call → points to voice-webhook (conversation begins there)
    """
    try:
        call = client.calls.create(
            to=to_number,
            from_=TWILIO_PHONE_NUMBER,
            url=url_for('gemini_call.voice_webhook', _external=True, _scheme='https'),   # ← critical: full public URL
            # Optional: add status_callback for logging
            # status_callback=url_for('gemini_call.call_status', _external=True),
            # status_callback_event=['initiated', 'answered', 'completed']
        )
        return call.sid
    except Exception as e:
        print(f"Twilio call failed: {e}")
        raise