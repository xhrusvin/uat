# follow_up_call.py
import threading
import logging
from flask import current_app, jsonify
from bson import ObjectId
from compliancedocumentcall import make_compliance_document_ai_call
from datetime import datetime
import requests
import os


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

XN_PORTAL_BASE_URL = os.getenv("XN_PORTAL_BASE_URL")


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


def register_compliance_doc_call_routes(app):
    """
    Registers compliance document call routes
    """


    @app.route('/compliance_document_call', methods=['GET'])
    def auto_compliance_document_call():
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
            #"xn_user_id": {"$ne": None},
            #"call_sent": {"$ne": 0},
            #"follow_up_sent": {"$ne": 0},  # 0 or missing
            #"compliance_documents_status": {"$ne": 1},
            "xn_user_id": "69e7340f5f14105609094fb1",
            "email": "juhi@xpresshealth.ie"
            }

        user = app.db.users.find_one(
          query,
          sort=[("compliance_documents_status", 1)]  # Oldest due first (ascending)
          )

        if not user:
        # Optional: fallback message if no follow-up due
          return jsonify({
              **response_base,
              "status": "no_pending",
            "message": "No users need a compliance document call at this time."
          }), 200

        user_id = user["_id"]
        xn_user_id = user.get("xn_user_id")

        # === Fetch document list from external API ===
        api_url = XN_PORTAL_BASE_URL.rstrip("/") + "/ai/recruitments/user-document-list"
        headers = {
         "Api-Key": os.getenv("XN_PORTAL_API_KEY"),      # Replace with env var in production
          "X-App-Country": os.getenv("XN_APP_COUNTRY")    # Replace with env var in production
        }
        payload = {"_id": str(xn_user_id)}

        try:
             response = requests.get(api_url, params=payload, headers=headers, timeout=10)
             response.raise_for_status()
             api_data = response.json()
        except Exception as e:
          return jsonify({
            **response_base,
            "status": "api_error",
            "message": f"Failed to fetch document list: {str(e)}",
            "user_id": str(user_id)
        }), 200

        if not api_data.get("success") or "data" not in api_data:
             return jsonify({
               **response_base,
               "status": "api_error",
               "message": "Invalid or unsuccessful response from document API.",
               "user_id": str(user_id)
              }), 200

        documents = api_data["data"]
        pending_count = sum(1 for doc in documents if doc.get("status") == "pending")
        has_pending = pending_count > 0

        # === Only proceed if there are pending documents ===
        if not has_pending:
           return jsonify({
            **response_base,
            "status": "no_action_needed",
            "message": "All compliance documents are complete. No call required.",
            "user_id": str(user_id),
            "pending_documents_count": 0
        }), 200

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
           target=make_compliance_document_ai_call,
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
            "message": "Compliance document call triggered successfully."
          }), 200

    
