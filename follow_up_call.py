# follow_up_call.py
import threading
import logging
from flask import current_app, jsonify
from bson import ObjectId
from followupcall import make_followup_ai_call
from datetime import datetime

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


def register_follow_up_call_routes(app):
    """
    Registers follow-up call routes
    """

    # --------------------------------------------------
    # 1. AUTO FOLLOW-UP CALL
    # --------------------------------------------------
    @app.route('/follow_up_call1', methods=['GET'])
    def auto_follow_up_call1():
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
        query = {
            "is_admin": {"$ne": True},
            "$or": [
                {"call_sent": 1},
                {"call_sent": {"$exists": False}}
            ]
        }

        user = app.db.users.find_one(
            query,
            sort=[("created_at", -1)]
        )

        if not user:
            return jsonify({
                **response_base,
                "status": "no_pending",
                "message": "No users need a call (within allowed hours)."
            }), 200

        user_id = user["_id"]

        if user.get("call_sent") == 2:
            return jsonify({
                **response_base,
                "status": "already_sent",
                "message": "Call already triggered."
            }), 200

        # Mark as sent
        result = app.db.users.update_one(
            {"_id": user_id},
            {"$set": {"call_sent": 1, "updated_at": datetime.utcnow()}}
        )

        if result.modified_count == 0:
            return jsonify({
                **response_base,
                "status": "failed",
                "message": "Failed to update user."
            }), 500

        # Trigger background call
        threading.Thread(
            target=make_followup_ai_call,
            args=(current_app._get_current_object(), user.get("phone"), user, user_id),
            daemon=True
        ).start()

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
            "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        }), 200


    @app.route('/follow_up_call', methods=['GET'])
    def auto_follow_up_call():
        allowed, server_time = is_within_call_window()
        user_id = request.args.get('user_id')

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

        if user_id:
            query = {
                "_id": ObjectId(user_id),
                "is_admin": {"$ne": True},
            }
        else:
          
            query = {
            "is_admin": {"$ne": True},
            #"xn_user_id": {"$ne": None},
            #"call_sent": {"$ne": 0},
            #"follow_up_sent": {"$ne": 1},  # 0 or missing
            "email": {"$exists": True},
            #"email": "juhi@xpresshealth.ie",
            "email": "juhi@xpresshealth.ie",
            #"next_follow_up_at": {"$lte": current_time}
            }

        user = app.db.users.find_one(
          query,
          sort=[("next_follow_up_at", 1)]  # Oldest due first (ascending)
          )
        
        # query = {
        #     "is_admin": {"$ne": True},
        #     "call_sent": {"$ne": 0},
        #     "name" : "Akhil K",
        #     }
        
        # user = app.db.users.find_one(
        #   query,
        #   sort=[("last_call_at", 1)],
        #   )


        if not user:
        # Optional: fallback message if no follow-up due
          return jsonify({
              **response_base,
              "status": "no_pending",
            "message": "No users need a follow-up call at this time."
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
           target=make_followup_ai_call,
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
            "message": "Follow-up call triggered successfully."
          }), 200

    # --------------------------------------------------
    # 2. MANUAL FOLLOW-UP CALL
    # --------------------------------------------------
    @app.route('/follow_up_call/trigger/<user_id>', methods=['POST'])
    def manual_follow_up_call(user_id):
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
                "message": "Manual follow-up calls blocked outside allowed hours."
            }), 403

        try:
            obj_id = ObjectId(user_id)
        except Exception:
            return jsonify({"status": "error", "message": "Invalid user ID"}), 400

        user = app.db.users.find_one(
            {"_id": obj_id},
            {
                "first_name": 1,
                "last_name": 1,
                "phone": 1,
                "created_at": 1,
                "followup_call_sent": 1,
                "is_admin": 1
            }
        )

        if not user:
            return jsonify({"status": "error", "message": "User not found"}), 404

        if user.get("is_admin"):
            return jsonify({"status": "error", "message": "Cannot call admin"}), 403

        if user.get("followup_call_sent") == 1:
            return jsonify({
                **response_base,
                "status": "info",
                "message": "Follow-up call already sent."
            }), 200

        # ----------------------------------------------
        # Mark + trigger
        # ----------------------------------------------
        app.db.users.update_one(
            {"_id": obj_id},
            {
                "$set": {
                    "followup_call_sent": 1,
                    "followup_called_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow()
                }
            }
        )

        threading.Thread(
            target=make_followup_ai_call,
            args=(current_app._get_current_object(), user.get("phone"), user, user_id),
            daemon=True
        ).start()

        created_at_str = (
            user.get("created_at").strftime("%Y-%m-%d %H:%M:%S UTC")
            if isinstance(user.get("created_at"), datetime)
            else "unknown"
        )

        return jsonify({
            **response_base,
            "status": "followup_triggered",
            "user_id": str(obj_id),
            "phone": user["phone"],
            "name": f"{user.get('first_name','')} {user.get('last_name','')}".strip(),
            "created_at": created_at_str
        }), 200

    # --------------------------------------------------
    # 3. DEBUG ENDPOINT
    # --------------------------------------------------
    @app.route('/debug-follow-up-call')
    def debug_follow_up():
        allowed, now = is_within_call_window()
        return jsonify({
            "debug": "follow_up_call.py loaded",
            "server_time": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed": allowed,
            "note": "Follow-up calls only run during allowed hours."
        })
