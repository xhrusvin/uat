# compliancedocumentcall.py

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

def make_professional_reference_ai_call(app, phone: str, user_doc: dict, ref_id: str):
    params_dict = {}

    # Add ref_id FIRST (as requested)
    if ref_id:
        params_dict["ref_id"] = ref_id

    # Add user document fields (handle ObjectId and other non-serializable types)
    for key, value in user_doc.items():
                if isinstance(value, ObjectId):
                    params_dict[key] = str(value)
                elif isinstance(value, datetime):
                    params_dict[key] = value.isoformat()
                else:
                    params_dict[key] = value

    # Convert to query string
    query_string = urllib.parse.urlencode(params_dict, doseq=True)
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
                    "Url": f'https://app.expresshealth.ie/voice3_uat?{query_string}',
                    "StatusCallback": f'https://app.expresshealth.ie/call/completed'
                }
            )
            response.raise_for_status()
            data = response.json()
            print(f"TeXML call initiated: {data['call_sid']} for {e164_phone}")

    except Exception as e:
        print(f"Call failed: {e}")

