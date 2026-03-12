# shift_booking_call.py
import threading
import logging
from flask import current_app, jsonify
from bson import ObjectId
from shiftbookingcall import make_shiftbooking_ai_call
from datetime import datetime, time
import time

# --------------------------------------------------
# Logging setup
# --------------------------------------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --------------------------------------------------
# Time window (UTC) - adjust as needed
# Example: 00:00 – 23:00 UTC = whole day
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


def register_shift_booking_call_routes(app):
    """
    Registers follow-up call routes
    """

    # --------------------------------------------------
    # 2. AUTO SHIFT BOOKING CALL - BATCH OF UP TO 10
    # --------------------------------------------------
    @app.route('/shift_booking_call', methods=['GET'])
    def auto_shift_booking_call():
        allowed, server_time = is_within_call_window()
        db = current_app.db

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

        current_time = datetime.utcnow()
        today_start = datetime(current_time.year, current_time.month, current_time.day)

        # Find up to 10 oldest eligible assignments
        eligible_assignments = list(db.shifts_users.find({
            "call_enabled": 1,
            "call_processed": {"$ne": 1},          # prevent re-processing
            "shift_id": {
                "$in": db.shifts.find(
                    {
                        "is_active": True,
                        "date": {"$gte": today_start}
                    },
                    {"_id": 1}
                ).distinct("_id")
            }
        }).sort("assigned_at", 1).limit(10))

        if not eligible_assignments:
            return jsonify({
                **response_base,
                "status": "no_pending",
                "message": "No staff with call_enabled=1 found (or shift inactive/expired)."
            }), 200

        triggered = []
        skipped = []

        for assignment in eligible_assignments:
            shift_id = assignment["shift_id"]
            user_id = assignment["user_id"]

            user = db.users.find_one(
                {"_id": user_id},
                {
                    "first_name": 1,
                    "last_name": 1,
                    "phone": 1,
                    "created_at": 1,
                    "is_admin": 1
                }
            )

            if not user:
                skipped.append({
                    "assignment_id": str(assignment.get("_id")),
                    "user_id": str(user_id),
                    "reason": "user_not_found"
                })
                continue

            if user.get("is_admin"):
                skipped.append({
                    "assignment_id": str(assignment.get("_id")),
                    "user_id": str(user_id),
                    "reason": "admin_skipped"
                })
                continue

            if user.get("call_sent") == 1:
                skipped.append({
                    "assignment_id": str(assignment.get("_id")),
                    "user_id": str(user_id),
                    "reason": "call_already_sent"
                })
                continue

            # Mark as processed BEFORE starting call (important!)
            db.shifts_users.update_one(
                {"_id": assignment["_id"]},
                {"$set": {
                    "call_processed": 1,
                    "availability": 4, # 4 = not attended yet
                    "call_processed_at": current_time
                }}
            )

            # Optional: small delay to avoid rate-limiting your telephony provider
            # time.sleep(0.4)   # uncomment if needed

            # Trigger the AI call in background
            threading.Thread(
                target=make_shiftbooking_ai_call,
                args=(current_app._get_current_object(), user.get("phone"), user, str(user_id), str(shift_id)),
                daemon=True
            ).start()

            name = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip() or "Unknown"
            created_at_str = (
                user.get("created_at").strftime("%Y-%m-%d %H:%M:%S UTC")
                if user.get("created_at") and isinstance(user.get("created_at"), datetime)
                else "unknown"
            )

            triggered.append({
                "user_id": str(user_id),
                "shift_id": str(shift_id),
                "phone": user.get("phone"),
                "name": name,
                "created_at": created_at_str
            })

        triggered_count = len(triggered)

        return jsonify({
            **response_base,
            "status": "processed",
            "found": len(eligible_assignments),
            "triggered_count": triggered_count,
            "triggered": triggered,
            "skipped_count": len(skipped),
            "skipped": skipped if skipped else None,
            "message": f"Processed {len(eligible_assignments)} assignment(s), triggered {triggered_count} call(s)"
        }), 200

   
    # --------------------------------------------------
    # 4. DEBUG ENDPOINT
    # --------------------------------------------------
    @app.route('/debug-shift-booking-call')
    def debug_shift_booking():
        allowed, now = is_within_call_window()
        return jsonify({
            "debug": "shift_booking.py loaded",
            "server_time": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed": allowed,
            "note": "Follow-up calls only run during allowed hours."
        })