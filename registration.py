# registration.py
from flask import request, jsonify, session
import telnyx                          # replaces: from twilio.rest import Client
from dotenv import load_dotenv
import os
import threading
import bcrypt
import re
import urllib
from datetime import datetime, time
import pytz
from flask import current_app

load_dotenv()

# Telnyx setup
telnyx.api_key = os.getenv('TELNYX_API_KEY')   # replaces: TwilioClient(SID, TOKEN)
CALLER_ID = os.getenv('TELNYX_CALLER_ID')
TELNYX_CONNECTION_ID = os.getenv('TELNYX_CONNECTION_ID')  # required by Telnyx
BASE_URL = os.getenv('BASE_URL', 'https://app.expresshealth.ie').rstrip('/')

# ElevenLabs
AGENT_ID = os.getenv('ELEVENLABS_AGENT_ID')
EL_API_KEY = os.getenv('ELEVENLABS_API_KEY')

# Allowed designations
ALLOWED_DESIGNATIONS = [
    "Nurse", "Healthcare Assistant", "RPN", "Pharmacist", "Pharmacy Technician",
    "Multi-Task Assistant", "Cleaner", "Radiographer", "Cardiac Physiologist",
    "Chef", "Housekeeping", "Occupational Therapists", "Physiotherapist",
    "Speech And Language Therapist", "Podiatrists", "Support Worker",
    "Admin Assistant", "Test", "Social Care Worker", "Anaesthetic Rn",
    "Midwives", "Psychologists", "Kitchen Assistant"
]

# Global queue: (phone, user_doc, user_id)
CALL_QUEUE = []

def schedule_calls(app):
    """Background thread: call queued users after 8:00 AM Ireland time"""
    def runner():
        while True:
            try:
                now = datetime.now(pytz.timezone('Europe/Dublin'))
                current_time = now.time()

                if current_time >= time(11, 0):
                    with app.app_context():
                        while CALL_QUEUE:
                            phone, user_doc, user_id = CALL_QUEUE.pop(0)
                            print(f"[Scheduler] Calling queued: {phone}")
                            make_ai_call(app, phone, user_doc, user_id)
            except Exception as e:
                print(f"[Scheduler Error] {e}")
            threading.Event().wait(60)

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()

def generate_twiml(user_doc: dict):
    # TeXML syntax is identical to TwiML — no library needed
    return '''<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://app.expresshealth.ie/wss"/>
  </Connect>
</Response>'''

def make_ai_call(app, phone: str, user_doc: dict, user_object_id):
    params = urllib.parse.urlencode(user_doc, doseq=True)
    try:
        with app.app_context():
            call = telnyx.Call.create(
                to=phone,
                from_=CALLER_ID,
                connection_id=TELNYX_CONNECTION_ID,           # new — required
                webhook_url=f'{BASE_URL}/voice?{params}',     # replaces: url=
                webhook_url_method='POST'
            )
            print(f"Telnyx call initiated: {call.call_control_id} for {phone}")  # .sid → .call_control_id

            app.db.users.update_one(
                {"_id": user_object_id},
                {"$set": {"call_sent": 1, "updated_at": datetime.utcnow()}}
            )
    except Exception as e:
        print(f"Call failed: {e}")

# === MAIN ROUTE REGISTRATION ===
def register_registration_routes(app):
    @app.route('/api/register', methods=['POST'])
    def api_register():
        try:
            data = request.get_json()
            required = ['first_name', 'last_name', 'email', 'password', 'confirm_password',
                        'phone', 'country', 'designation']
            if not all(k in data for k in required):
                return jsonify({"error": "Missing fields"}), 400

            if data['password'] != data['confirm_password']:
                return jsonify({"error": "Passwords do not match"}), 400

            if data['country'] not in ['Ireland', 'UK', 'Northern Ireland', 'Australia']:
                return jsonify({"error": "Select Country"}), 400

            if data['designation'] not in ALLOWED_DESIGNATIONS:
                return jsonify({"error": "Invalid designation"}), 400

            if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', data['email']):
                return jsonify({"error": "Invalid email"}), 400

            if not re.match(r'^\+\d{1,3}\s\d{7,15}$', data['phone']):
                return jsonify({"error": "Invalid phone format"}), 400

            users = app.db.users
            if users.find_one({"$or": [{"email": data['email']}, {"phone": data['phone']}]}):
                return jsonify({"error": "Already registered"}), 400

            hashed = bcrypt.hashpw(data['password'].encode(), bcrypt.gensalt()).decode()

            user_doc = {
                "first_name": data['first_name'],
                "last_name": data['last_name'],
                "email": data['email'],
                "phone": data['phone'],
                "country": data['country'],
                "designation": data['designation'],
                "password": hashed,
                "call_sent": 0,  # Default
                "created_at": datetime.utcnow().isoformat() + 'Z'
            }

            # Insert user
            result = users.insert_one(user_doc)
            user_id = str(result.inserted_id)

            if data['country'] != 'Ireland':
                app.db.users.update_one(
                    {"_id": result.inserted_id},
                    {"$set": {"call_sent": 1, "updated_at": datetime.utcnow()}}
                )

            # Session
            session['user_id'] = user_id
            session['email'] = data['email']
            session['name'] = f"{data['first_name']} {data['last_name']}"

            # === Admin toggle ===
            settings = app.db.settings.find_one({"_id": "global"})
            allow_call = settings.get("allow_registration_call", False) if settings else False

            # === RESTRICT CALL TO IRELAND ONLY ===
            if data['country'] != 'Ireland':
                allow_call = False  # No call for UK, NI, Australia, etc.

            # === Dublin time check ===
            now = datetime.now(pytz.timezone('Europe/Dublin'))
            current_time = now.time()
            is_business_hours = time(8, 0) <= current_time <= time(20, 0)

            # === Final call decision ===
            message = "Success! Registration complete."
            call_scheduled = False

            if allow_call and user_doc["call_sent"] == 0:
                if is_business_hours:
                    # Call now
                    threading.Thread(
                        target=make_ai_call,
                        args=(app, data['phone'], user_doc, result.inserted_id),
                        daemon=True
                    ).start()
                    message = "Success! AI calling you now..."
                    call_scheduled = True
                else:
                    # Queue for tomorrow
                    #CALL_QUEUE.append((data['phone'], user_doc, result.inserted_id))
                    message = "Success! We'll call you tomorrow after 8:00 AM."
                    call_scheduled = True
            else:
                if not allow_call:
                    message = "Success! Registration complete. Call disabled by admin."
                elif user_doc["call_sent"] == 1:
                    message = "Success! Registration complete. (Call already sent)"

            return jsonify({
                "message": message,
                "user_id": user_id,
                "call_scheduled": call_scheduled
            }), 201

        except Exception as e:
            print(f"Registration error: {e}")
            return jsonify({"error": "Server error"}), 500

    @app.route("/call/completed", methods=["POST"])
    def call_completed():
        data = request.get_json()   # Telnyx sends JSON, not form data
        print("Call completed:", data)
        return "", 200

# Auto-start scheduler when module is imported
# Ensure this runs only once (e.g., in app factory)
def init_scheduler(app):
    schedule_calls(app)