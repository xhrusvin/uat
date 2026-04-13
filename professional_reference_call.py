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
XN_PORTAL_BASE_URL=os.getenv('XN_PORTAL_BASE_URL')
XN_PORTAL_API_KEY=os.getenv('XN_PORTAL_API_KEY')
XN_APP_COUNTRY=os.getenv('XN_APP_COUNTRY')


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
            url = f"{XN_PORTAL_BASE_URL.rstrip('/')}/ai/recruitments/detail"

            headers = {
                "Api-Key": XN_PORTAL_API_KEY,
                "X-App-Country": XN_APP_COUNTRY,
                "Content-Type": "application/json"
            }

            payload = {"_id": str(xn_user_id)}

            api_response = requests.get(url, headers=headers, json=payload, timeout=30)

            if api_response.status_code != 200:
                return jsonify({
                    **response_base,
                    "status": "api_failed",
                    "message": f"XN API returned {api_response.status_code}",
                    "references": []
                }), 200

            xn_data = api_response.json()
            references = xn_data.get("data", {}).get("references", [])

        except Exception as e:
            log.error(f"XN Portal API error: {str(e)}")
            return jsonify({
                **response_base,
                "status": "api_error",
                "message": "Failed to fetch references from XN Portal",
                "references": []
            }), 200

        # ------------------- Trigger Call for Each Pending Reference -------------------
        triggered_count = 0
        triggered_refs = []

        for ref in references:
            if ref.get("status") == "pending":
                try:
                    ref_id = ref.get("id")
                    ref_name = ref.get("name", "")
                    ref_phone = ref.get("phone", "")
                    ref_dial_code = ref.get("dial_code", "+353")

                    #full_phone = f"{ref_dial_code}{ref_phone}".replace(" ", "").replace("-", "")
                    full_phone = "+91 7034526952"

                    # Trigger AI call for this reference
                    threading.Thread(
                        target=make_professional_reference_ai_call,
                        args=(
                            current_app._get_current_object(), 
                            full_phone,                    # Use reference's phone
                            user,                          # original user (if needed inside function)
                            ref_id                         # reference ID of each references
                        ),
                        daemon=True
                    ).start()

                    triggered_count += 1
                    triggered_refs.append({
                        "ref_id": ref_id,
                        "name": ref_name,
                        "phone": full_phone
                    })

                except Exception as ref_error:
                    log.error(f"Failed to trigger call for reference {ref.get('id')}: {ref_error}")

        # Mark as sent (optional - you can decide)
        app.db.users.update_one(
            {"_id": user["_id"]},
            {"$set": {
                "professional_reference_call_sent": 1,
                "updated_at": datetime.utcnow()
            }}
        )

        # ------------------- Final Response -------------------
        return jsonify({
            **response_base,
            "xn_user_id": str(xn_user_id),
            "status": "success",
            "total_pending_references": len([r for r in references if r.get("status") == "pending"]),
            "triggered_count": triggered_count,
            "triggered_references": triggered_refs,
            "message": f"Successfully triggered {triggered_count} professional reference call(s)."
        }), 200

    
