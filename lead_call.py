# lead_call.py
import threading
import logging
from flask import current_app, jsonify
from bson import ObjectId
from leadcall import make_ai_call
from datetime import datetime

# Logging setup
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Time window (UTC): 8:00 AM to 8:00 PM
ALLOWED_START_HOUR = 00   # 08:00 UTC
ALLOWED_END_HOUR = 23    # 20:00 UTC


def is_within_call_window():
    """Return True if current UTC hour is between 8:00 AM and 8:00 PM."""
    now = datetime.utcnow()
    hour = now.hour
    allowed = ALLOWED_START_HOUR <= hour < ALLOWED_END_HOUR
    log.info(f"[TIME CHECK] Server time: {now.strftime('%Y-%m-%d %H:%M:%S UTC')} → "
             f"Hour {hour} → Lead call allowed: {allowed}")
    return allowed, now


def register_lead_call_routes(app):
    """Register /lead-calls/auto and /lead-calls/trigger/<lead_id> — time-restricted auto-call for leads."""

    # --------------------------------------------------------------
    # 1. AUTO-TRIGGER: /lead-calls/auto (TIME-RESTRICTED) → calls oldest uncalled lead
    # --------------------------------------------------------------
    @app.route('/lead-calls/auto', methods=['GET'])
    def lead_auto_call():
        # ---- 1. Check global setting ----
        settings = app.db.settings.find_one({"_id": "global"}) or {}
        if not settings.get("enable_lead_call", True):
            return jsonify({
                "status": "disabled",
                "message": "Lead calling is disabled in system settings."
            }), 403

        # ---- 2. Check time window ----
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
                "message": "Lead calls only allowed from 8:00 AM to 8:00 PM UTC."
            }), 200

        # Find oldest lead that hasn't been called
        query = {
            "$or": [
                {"call_initiated": {"$exists": False}},
                {"call_initiated": {"$ne": 1}}
            ]
        }

        lead = app.db.leads.find_one(
            query,
            sort=[("uploaded_at", 1)]  # oldest first
        )

        if not lead:
            return jsonify({
                **response_base,
                "status": "no_pending",
                "message": "No leads need a call right now."
            }), 200

        lead_id = lead["_id"]

        if lead.get("call_initiated") == 1:
            return jsonify({
                **response_base,
                "status": "already_sent",
                "message": "This lead was already called."
            }), 200

        # Mark as called
        result = app.db.leads.update_one(
            {"_id": lead_id},
            {"$set": {"call_initiated": 1, "call_initiated_at": datetime.utcnow()}}
        )

        if result.modified_count == 0:
            return jsonify({
                **response_base,
                "status": "failed",
                "message": "Failed to mark lead as called."
            }), 500

        # Trigger background call
        threading.Thread(
            target=make_ai_call,
            args=(current_app._get_current_object(), lead.get("phone_number") or lead.get("phone"), lead, lead_id),
            daemon=True
        ).start()

        uploaded_at_str = (
            lead.get("uploaded_at").strftime("%Y-%m-%d %H:%M:%S UTC")
            if isinstance(lead.get("uploaded_at"), datetime)
            else "unknown"
        )

        return jsonify({
            **response_base,
            "status": "triggered",
            "lead_id": str(lead_id),
            "name": lead.get("name") or "Unknown",
            "phone": lead.get("phone_number") or lead.get("phone") or "N/A",
            "uploaded_at": uploaded_at_str,
            "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        }), 200

    # --------------------------------------------------------------
    # 2. MANUAL TRIGGER: /lead-calls/trigger/<lead_id> (TIME-RESTRICTED)
    # --------------------------------------------------------------
    @app.route('/lead-calls/trigger/<lead_id>', methods=['POST'])
    def lead_call_trigger(lead_id):
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
                "message": "Manual lead calls blocked outside 8 AM – 8 PM UTC."
            }), 403

        try:
            obj_id = ObjectId(lead_id)
        except Exception:
            return jsonify({"status": "error", "message": "Invalid lead ID"}), 400

        lead = app.db.leads.find_one({"_id": obj_id})

        if not lead:
            return jsonify({"status": "error", "message": "Lead not found"}), 404

        if lead.get("call_initiated") == 1:
            return jsonify({
                **response_base,
                "status": "already_called",
                "message": "Call already initiated for this lead"
            }), 200

        # Mark and trigger
        app.db.leads.update_one(
            {"_id": obj_id},
            {"$set": {"call_initiated": 1, "call_initiated_at": datetime.utcnow()}}
        )

        threading.Thread(
            target=make_ai_call,
            args=(current_app._get_current_object(), user.get("phone"), user, user_id),
            daemon=True
        ).start()

        uploaded_at_str = (
            lead.get("uploaded_at").strftime("%Y-%m-%d %H:%M:%S UTC")
            if isinstance(lead.get("uploaded_at"), datetime)
            else "unknown"
        )

        return jsonify({
            **response_base,
            "status": "triggered",
            "lead_id": str(obj_id),
            "name": lead.get("name") or "Unknown",
            "phone": lead.get("phone_number") or lead.get("phone") or "N/A",
            "uploaded_at": uploaded_at_str
        }), 200

    # --------------------------------------------------------------
    # 3. DEBUG: Check time + status
    # --------------------------------------------------------------
    @app.route('/debug-lead-call')
    def debug_lead_call():
        allowed, now = is_within_call_window()
        return jsonify({
            "debug": "lead_call.py loaded",
            "server_time": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed": allowed,
            "note": "Auto-call only runs during allowed hours."
        })