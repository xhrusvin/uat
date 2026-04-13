# follow_up_call.py
import email
import threading
import logging
from flask import current_app, jsonify
from bson import ObjectId
from professionalreferencecall import make_professional_reference_ai_call
from datetime import datetime
from bson import json_util
import requests
from dotenv import load_dotenv
import os

load_dotenv()
# --------------------------------------------------
# Logging setup
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --------------------------------------------------
# Time window (UTC)
# Example: 08:00 – 20:00 UTC
# --------------------------------------------------
ALLOWED_START_HOUR = 0
ALLOWED_END_HOUR = 23

def create_app():
    app = Flask(__name__)

    # Load config from environment variables
    app.config['XN_PORTAL_BASE_URL'] = os.getenv('XN_PORTAL_BASE_URL')
    app.config['XN_PORTAL_API_KEY'] = os.getenv('XN_PORTAL_API_KEY')
    app.config['XN_APP_COUNTRY']   = os.getenv('XN_APP_COUNTRY')

    # Optional: Add default values or validation
    if not app.config['XN_PORTAL_BASE_URL']:
        print("Warning: XN_PORTAL_BASE_URL is not set in .env")

    # Register your routes here
    register_professional_reference_call_routes(app)

    return app

def serialize_doc(doc):
    if not doc:
        return None
    doc = dict(doc)  # make a copy
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])
    # Convert any other datetime fields if needed
    for key, value in list(doc.items()):
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
    return doc

def is_within_call_window():
    """
    Return (allowed: bool, now: datetime)
    """
    now = datetime.utcnow()
    hour = now.hour
    allowed = ALLOWED_START_HOUR <= hour < ALLOWED_END_HOUR

    log.info(
        f"[TIME CHECK] {now.strftime('%Y-%m-%d %H:%M:%S UTC')} | "
        f"Hour={hour} | Allowed={allowed}"
    )
    return allowed, now


def register_professional_reference_call_routes(app):
    """
    Registers compliance document call routes
    """


    @app.route('/professional_reference_call', methods=['GET'])
    def auto_professional_reference_call():
        allowed, server_time = is_within_call_window()

        response_base = {
          "server_time": server_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
          "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
          "call_allowed": allowed
       }

        if not allowed:
            return jsonify({
                **response_base,
                "status": "outside_hours",
                "message": "Calls only allowed from 8:00 AM to 8:00 PM UTC."
            }), 200

          # === Within allowed time → proceed ===
          # Find users where follow-up is due: follow_up_sent is 0 (or missing) AND next_follow_up_at <= now (if exists)
        current_time = datetime.utcnow() 
        query = {
            "is_admin": {"$ne": True},
            "xn_user_id": {"$ne": None},
            #"call_sent": {"$ne": 0},
            #"follow_up_sent": {"$ne": 0},  # 0 or missing
            #"xn_user_id": "69452f8cf84265e6fd0a11b9",
            #"next_follow_up_at": {"$lte": current_time},
            "email": "rusvin@xpresshealth.ie"
            }

        user = app.db.users.find_one(
          query,
          sort=[("next_follow_up_at", 1)]  # Oldest due first (ascending)
          )

        if not user:
            return jsonify({
                **response_base,
                "status": "no_pending",
                "message": "No users need a follow-up call at this time."
            }), 200

        xn_user_id = user.get("xn_user_id")
        if not xn_user_id:
            return jsonify({
                **response_base,
                "status": "missing_xn_user_id",
                "message": "xn_user_id is missing for this user"
            }), 200

        # ====================== CALL XN PORTAL API ======================
        try:
            xn_base_url = current_app.config.get('XN_PORTAL_BASE_URL')
            api_key     = current_app.config.get('XN_PORTAL_API_KEY')
            app_country = current_app.config.get('XN_APP_COUNTRY')

            if not xn_base_url or not api_key or not app_country:
                return jsonify({
                    **response_base,
                    "status": "config_error",
                    "message": "XN Portal configuration is missing"
                }), 500

            url = f"{xn_base_url.rstrip('/')}/ai/recruitments/detail"

            headers = {
                "Api-Key": api_key,
                "X-App-Country": app_country,
                "Content-Type": "application/json"
            }

            payload = {
                "_id": str(xn_user_id)   # ensure it's string
            }

            response = requests.get(
                url,
                headers=headers,
                json=payload,      # sends as JSON body (even for GET)
                timeout=30
            )

            # Log the status for debugging
            log.info(f"XN Portal API call - Status: {response.status_code} | URL: {url}")

            if response.status_code == 200:
                xn_data = response.json()
                status = "success"
            else:
                xn_data = response.text
                status = "failed"

        except requests.exceptions.RequestException as e:
            log.error(f"XN Portal API request failed: {e}")
            xn_data = None
            status = "request_error"

        # ====================== FINAL RESPONSE ======================
        return jsonify({
            **response_base,
            "xn_user_id": str(xn_user_id),
            "status": status,
            "xn_portal_response": xn_data
        }), 200

        user_id = user["_id"]

         # Prevent double-triggering (in case of concurrent requests)
        update_result = app.db.users.update_one(
          {
            "_id": user_id
          },
          {
            "$set": {
                "follow_up_sent": 1,
                "updated_at": datetime.utcnow()
            }
          }
        )

        if update_result.modified_count == 0:
             return jsonify({
            **response_base,
            "status": "already_triggered_or_failed",
            "message": "Follow-up already triggered or no longer eligible."
         }), 200

        # Trigger background follow-up AI call
        threading.Thread(
           target=make_professional_reference_ai_call,
           args=(current_app._get_current_object(), user.get("phone"), user, user_id),
           daemon=True
         ).start()

        next_follow_up_str = (
            user.get("next_follow_up_at").strftime("%Y-%m-%d %H:%M:%S UTC")
            if user.get("next_follow_up_at")
            else "unknown"
            )

        created_at_str = (
            user.get("created_at").strftime("%Y-%m-%d %H:%M:%S UTC")
            if isinstance(user.get("created_at"), datetime)
            else "unknown"
         )

        return jsonify({
           **response_base,
           "status": "triggered",
            "user_id": str(user_id),
           "phone": user.get("phone"),
            "name": f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
            "created_at": created_at_str,
            "next_follow_up_at": next_follow_up_str,
            "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
            "message": "Professional reference call triggered successfully."
          }), 200

    
