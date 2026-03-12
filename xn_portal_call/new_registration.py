# xn_portal_call/new_registration.py
# FULLY STANDALONE – No app.py needed, no current_app, everything inside

from flask import Blueprint, request, jsonify
import requests
from datetime import datetime
import pytz
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

# ==================== BLUEPRINT ====================
bp = Blueprint('registrations', __name__, url_prefix='')

# ==================== MONGO DB (INSIDE THIS FILE) ====================
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')
XN_PORTAL_BASE_URL = os.getenv('XN_PORTAL_BASE_URL')

if not MONGO_URI or not DB_NAME:
    raise ValueError("MONGO_URI and DB_NAME must be set in .env")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
users_collection = db['users']   # ← direct access, no app.py needed

# ==================== API CONFIG ====================
API_URL = f"{XN_PORTAL_BASE_URL}admin/recruitments/staff-list"
API_KEY = os.getenv("XN_PORTAL_API_KEY")
HEADERS = {
    "Api-key": API_KEY,
    "X-App-Country": "ie"
}
TRIGGER_KEY = os.getenv("TRIGGER_KEY", "1234")
MAX_LEADS = 10
#BLOCKED_DESIGNATIONS = {"admin assistant", "support worker", "pharmacy technician", "pharmacist" }
BLOCKED_DESIGNATIONS = {}

# ==================== HELPERS ====================
def parse_date(date_str):
    if not date_str:
        return datetime(1970, 1, 1)
    for fmt in ("%d %b %Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(date_str).split()[0], fmt)
        except:
            continue
    return datetime(1970, 1, 1)

# ==================== MAIN ROUTE ====================
@bp.route('/api/new_registrations')
def new_registrations():
    key = request.args.get('key')

    if key != TRIGGER_KEY:
        return jsonify({"success": False, "message": "Invalid key"}), 401

    if not API_KEY:
        return jsonify({"success": False, "message": "Missing XN_PORTAL_API_KEY in .env"}), 500

    try:
        print(f"[{datetime.now(pytz.timezone('Europe/Dublin'))}] Starting new registrations import...")

        # Get first page only (latest records)
        resp = requests.get(
            API_URL,
            headers=HEADERS,
            params={"page": 1, "per_page": 100},
            timeout=600
        )
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            return jsonify({"success": False, "message": "API access denied or failed"}), 403

        users = data["data"]["data"]
        print(f"Fetched {len(users)} records")

        # Sort newest first
        users.sort(key=lambda x: parse_date(x.get("registered_date")), reverse=True)
        latest = users[:MAX_LEADS]

        inserted = skipped_exists = skipped_not_irish = skipped_no_email = skipped_blocked = 0
        now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)

        for user in latest:
            email = str(user.get("email", "")).strip().lower()
            if not email or "@" not in email:
                skipped_no_email += 1
                continue

            if users_collection.find_one({"email": email}):
                skipped_exists += 1
                continue

            xn_user_id = str(user.get("_id", "")).strip()
            if not xn_user_id:
                continue

            phone_raw = str(user.get("phone_number", "")).strip()
            phone = phone_raw.replace(" ", "").replace("-", "")

            MY_PHONE_NUMBERS = {
                "+917034526952",
                "+917012932756",
                "+919496329819",
                "+916282908578",
                "+917994804516",
                "+919633392501"
            }
            # if not (phone.startswith("+353") or phone in MY_PHONE_NUMBERS):
            #     skipped_not_irish += 1
            #     continue

            raw_designation = user.get("user_type", "")
            designation = str(raw_designation).strip().lower()
            if any(blocked in designation for blocked in BLOCKED_DESIGNATIONS):
                skipped_blocked += 1
                continue

            # EXACT SAME FORMAT FROM YOUR OLD PLAYWRIGHT SCRIPT
            user_doc = {
                "xn_user_id": xn_user_id,
                "email": email,
                "first_name": str(user.get("name", "").split()[0] if user.get("name") else "Unknown").strip() or "Unknown",
                "last_name": " ".join(str(user.get("name", "")).split()[1:]).strip() or " ",
                "phone": phone,
                "designation": str(raw_designation).strip() or "unknown",
                "country": str(user.get("country", "Ireland")).strip() or "Ireland",
                "call_sent": 0,
                "created_at": now_utc.isoformat(),
                "updated_at": now_utc.isoformat(),
                # "call_sent": 1,
                # "garda_email_sent": 1,
                # "follow_up_sent": 1,
                # "missed_call_email_sent": 1,
            }

            users_collection.insert_one(user_doc)
            print(f"Inserted → {email} | {phone}")
            inserted += 1

        return jsonify({
            "success": True,
            "message": "Success",
            "inserted": inserted,
            "skipped": {
                "already_exists": skipped_exists,
                "not_+353": skipped_not_irish,
                "invalid_email": skipped_no_email,
                "blocked_role": skipped_blocked
            },
            "processed": len(latest),
            "timestamp": datetime.now(pytz.timezone('Europe/Dublin')).strftime("%Y-%m-%d %H:%M:%S")
        })

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"success": False, "message": str(e)}), 500