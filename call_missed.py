# call_missed.py
import threading
import logging
from flask import current_app, jsonify, request
from bson import ObjectId
from registration import make_ai_call
from datetime import datetime


# Logging setup
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Time window (UTC): 8:00 AM to 8:00 PM
ALLOWED_START_HOUR = 0   # 08:00 UTC
ALLOWED_END_HOUR = 23    # 20:00 UTC


def is_within_call_window():
    """Return True if current UTC hour is between 8:00 AM and 8:00 PM."""
    now = datetime.utcnow()
    hour = now.hour
    allowed = ALLOWED_START_HOUR <= hour < ALLOWED_END_HOUR
    log.info(f"[TIME CHECK] Server time: {now.strftime('%Y-%m-%d %H:%M:%S UTC')} → "
             f"Hour {hour} → Call allowed: {allowed}")
    return allowed, now


def register_missed_call_routes(app):
    """Register /call_missed → auto-trigger only 8 AM – 8 PM UTC."""

    # --------------------------------------------------------------
    # 1. AUTO-TRIGGER: /call_missed (TIME-RESTRICTED)
    # --------------------------------------------------------------
    @app.route('/call_missed', methods=['GET'])
    def call_missed_page():
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
        if user_id:
            query = {
                "_id": ObjectId(user_id)
            }
        else:
            query = {
            "is_admin": {"$ne": True},
            "$or": [
                {"call_sent": 0},
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
        if user.get("call_sent") == 1:
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
            target=make_ai_call,
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

    # --------------------------------------------------------------
    # 2. MANUAL TRIGGER: /call_missed/trigger/<user_id> (ALSO TIME-RESTRICTED)
    # --------------------------------------------------------------
    @app.route('/call_missed/trigger/<user_id>', methods=['POST'])
    def call_missed_trigger(user_id):
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
                "message": "Manual calls blocked outside 8:00 AM – 8:00 PM UTC."
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
                "call_sent": 1,
                "is_admin": 1
            }
        )

        if not user:
            return jsonify({"status": "error", "message": "User not found"}), 404

        if user.get("is_admin"):
            return jsonify({"status": "error", "message": "Cannot call admin"}), 403

        if user.get("call_sent") == 1:
            return jsonify({
                **response_base,
                "status": "info",
                "message": "Call already sent"
            }), 200

        # Mark and trigger
        app.db.users.update_one(
            {"_id": obj_id},
            {"$set": {"call_sent": 1, "updated_at": datetime.utcnow()}}
        )

        threading.Thread(
            target=make_ai_call,
            args=(current_app._get_current_object(), user["phone"], user, obj_id),
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
            "user_id": str(obj_id),
            "phone": user["phone"],
            "name": f"{user.get('first_name','')} {user.get('last_name','')}".strip(),
            "created_at": created_at_str
        }), 200

    # --------------------------------------------------------------
    # 3. DEBUG: Show current server time + allowed status
    # --------------------------------------------------------------
    @app.route('/debug-call-missed')
    def debug():
        allowed, now = is_within_call_window()
        return jsonify({
            "debug": "call_missed.py loaded",
            "server_time": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed": allowed,
            "note": "Auto-call only runs during allowed hours."
        })