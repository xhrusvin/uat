# shift_booking_bulk_call.py
import threading
import logging
from flask import current_app, jsonify, request
from bson import ObjectId
from shiftbookingcallbulk import make_shiftbooking_ai_call_bulk
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

ALLOWED_START_HOUR = 0
ALLOWED_END_HOUR   = 23


def is_within_call_window():
    now     = datetime.utcnow()
    hour    = now.hour
    allowed = ALLOWED_START_HOUR <= hour < ALLOWED_END_HOUR
    log.info(f"[TIME CHECK] {now.strftime('%Y-%m-%d %H:%M:%S UTC')} → Hour {hour} → Allowed: {allowed}")
    return allowed, now


def _check_and_end_group_outreach(app, group_id: str, outreach_id: str):
    """
    Check if all shifts_group_users for this group+outreach have call_processed == 1.
    If yes, set outreach_shift_group.outreach_status = 3 (Ended).
    """
    try:
        group_oid    = ObjectId(group_id)    if group_id    and ObjectId.is_valid(group_id)    else None
        outreach_oid = ObjectId(outreach_id) if outreach_id and ObjectId.is_valid(outreach_id) else None

        if not group_oid or not outreach_oid:
            log.warning(f"[END CHECK] Invalid ids: group={group_id} outreach={outreach_id}")
            return

        total = app.db.shifts_group_users.count_documents({
            "group_id":    group_oid,
            "outreach_id": outreach_oid,
        })
        processed = app.db.shifts_group_users.count_documents({
            "group_id":    group_oid,
            "outreach_id": outreach_oid,
            "call_processed": 1,
        })

        log.info(f"[END CHECK] group={group_id} outreach={outreach_id} processed={processed}/{total}")

        if total > 0 and processed >= total:
            result = app.db.outreach_shift_group.update_one(
                {"_id": outreach_oid, "outreach_status": {"$nin": [3, 10]}},
                {"$set": {
                    "outreach_status": 3,
                    "ended_at":        datetime.utcnow(),
                    "updated_at":      datetime.utcnow(),
                    "end_reason":      "all_calls_processed",
                }}
            )
            if result.modified_count:
                log.info(f"[END CHECK] ✓ Group outreach {outreach_id} ended — all {total} calls processed")
            else:
                log.info(f"[END CHECK] Group outreach {outreach_id} already ended or completed")
        else:
            log.info(f"[END CHECK] Not all calls processed yet ({processed}/{total})")

    except Exception as e:
        log.error(f"[END CHECK] Error: {e}")


def _get_shift_details(app, shift_id: str) -> dict:
    """Get shift details as string-safe dict."""
    if not shift_id or not ObjectId.is_valid(shift_id):
        return {}
    try:
        s = app.db.shifts.find_one({"_id": ObjectId(shift_id)})
        if not s:
            return {}
        return {
            "id":          str(s["_id"]),
            "shift_code":  s.get("shift_code") or s.get("name", ""),
            "date":        str(s.get("date", "")),
            "start_time":  s.get("start_time", ""),
            "end_time":    s.get("end_time", ""),
            "client_name": s.get("client_name", ""),
            "location":    s.get("location", ""),
            "user_type":   s.get("user_type", ""),
        }
    except Exception as e:
        log.warning(f"[SHIFT LOOKUP] {e}")
        return {}


def register_shift_booking_call_bulk_routes(app):

    # ------------------------------------------------------------------
    # 1. AUTO-TRIGGER: GET /shift_booking_bulk_call
    # ------------------------------------------------------------------
    @app.route('/shift_booking_bulk_call', methods=['GET'])
    def shift_booking_bulk_call():
        allowed, server_time = is_within_call_window()
        user_id_param = request.args.get('user_id')

        response_base = {
            "server_time":    server_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
        }

        if not allowed:
            return jsonify({**response_base, "status": "outside_hours",
                            "message": "Calls only allowed during allowed hours."}), 200

        # Find next unprocessed shifts_group_users record
        if user_id_param:
            query = {"user_id": ObjectId(user_id_param), "call_processed": 0, "call_enabled": 1}
        else:
            query = {"call_processed": 0, "call_enabled": 1}

        record = app.db.shifts_group_users.find_one(query, sort=[("assigned_at", 1)])

        if not record:
            return jsonify({**response_base, "status": "no_pending",
                            "message": "No pending bulk calls."}), 200

        shifts_group_users_id = record["_id"]
        group_id    = str(record.get("group_id",    ""))
        outreach_id = str(record.get("outreach_id", ""))
        user_id     = str(record.get("user_id",     ""))

        # Get user details
        user = None
        if user_id and ObjectId.is_valid(user_id):
            user = app.db.users.find_one(
                {"_id": ObjectId(user_id)},
                {"phone": 1, "first_name": 1, "last_name": 1}
            )

        if not user:
            return jsonify({**response_base, "status": "no_user",
                            "message": "User not found.",
                            "shifts_group_users_id": str(shifts_group_users_id)}), 200

        phone      = user.get("phone")
        first_name = user.get("first_name", "")
        last_name  = user.get("last_name",  "")
        full_name  = f"{first_name} {last_name}".strip()

        if not phone:
            return jsonify({**response_base, "status": "no_phone",
                            "message": "No phone found.",
                            "shifts_group_users_id": str(shifts_group_users_id)}), 200

        # Mark as processed
        result = app.db.shifts_group_users.update_one(
            {"_id": shifts_group_users_id},
            {"$set": {"call_processed": 1, "call_processed_at": datetime.utcnow(),
                      "updated_at": datetime.utcnow()}}
        )
        if result.modified_count == 0:
            return jsonify({**response_base, "status": "failed",
                            "message": "Failed to update record."}), 500

        # Get shift details from availability_details
        shift_id   = ""
        shift_doc  = {}
        avail_details = record.get("availability_details") or []
        if avail_details:
            first_avail = avail_details[0] if isinstance(avail_details, list) else avail_details
            shift_id = str(first_avail.get("shift_id", ""))
            if shift_id:
                shift_doc = _get_shift_details(app, shift_id)

        record["phone"] = phone
        record["first_name"] = first_name
        record["last_name"] = last_name
        record["full_name"] = full_name

        # Trigger bulk call in background
        threading.Thread(
            target=make_shiftbooking_ai_call_bulk,
            args=(current_app._get_current_object(), phone, record, shifts_group_users_id, shift_doc),
            daemon=True
        ).start()

        # Check if all calls processed → end group outreach
        threading.Thread(
            target=_check_and_end_group_outreach,
            args=(current_app._get_current_object(), group_id, outreach_id),
            daemon=True
        ).start()

        return jsonify({
            **response_base,
            "status":                 "triggered",
            "shifts_group_users_id":  str(shifts_group_users_id),
            "group_id":               group_id,
            "outreach_id":            outreach_id,
            "user_id":                user_id,
            "staff_name":             full_name,
            "phone":                  phone,
            "shift_id":               shift_id,
            "triggered_at":           datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        }), 200

    # ------------------------------------------------------------------
    # 2. MANUAL TRIGGER: POST /shift_booking_bulk_call/trigger/<id>
    # ------------------------------------------------------------------
    @app.route('/shift_booking_bulk_call/trigger/<shifts_group_users_id>', methods=['POST'])
    def shift_booking_bulk_call_trigger(shifts_group_users_id):
        allowed, server_time = is_within_call_window()

        response_base = {
            "server_time":    server_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
        }

        if not allowed:
            return jsonify({**response_base, "status": "outside_hours",
                            "message": "Manual calls blocked outside allowed hours."}), 403

        try:
            obj_id = ObjectId(shifts_group_users_id)
        except Exception:
            return jsonify({"status": "error", "message": "Invalid shifts_group_users_id"}), 400

        record = app.db.shifts_group_users.find_one({"_id": obj_id})
        if not record:
            return jsonify({"status": "error", "message": "Record not found"}), 404

        if record.get("call_processed") == 1:
            return jsonify({**response_base, "status": "info",
                            "message": "Call already processed"}), 200

        group_id    = str(record.get("group_id",    ""))
        outreach_id = str(record.get("outreach_id", ""))
        user_id     = str(record.get("user_id",     ""))

        user = None
        if user_id and ObjectId.is_valid(user_id):
            user = app.db.users.find_one(
                {"_id": ObjectId(user_id)},
                {"phone": 1, "first_name": 1, "last_name": 1}
            )

        if not user:
            return jsonify({**response_base, "status": "no_user",
                            "message": "User not found."}), 200

        phone     = user.get("phone")
        full_name = f"{user.get('first_name','')} {user.get('last_name','')}".strip()

        if not phone:
            return jsonify({**response_base, "status": "no_phone",
                            "message": "No phone found."}), 200

        shift_id  = ""
        shift_doc = {}
        avail_details = record.get("availability_details") or []
        if avail_details:
            first_avail = avail_details[0] if isinstance(avail_details, list) else avail_details
            shift_id = str(first_avail.get("shift_id", ""))
            if shift_id:
                shift_doc = _get_shift_details(app, shift_id)

        app.db.shifts_group_users.update_one(
            {"_id": obj_id},
            {"$set": {"call_processed": 1, "call_processed_at": datetime.utcnow(),
                      "updated_at": datetime.utcnow()}}
        )

        threading.Thread(
            target=make_shiftbooking_ai_call_bulk,
            args=(current_app._get_current_object(), phone, record, obj_id, shift_doc),
            daemon=True
        ).start()

        threading.Thread(
            target=_check_and_end_group_outreach,
            args=(current_app._get_current_object(), group_id, outreach_id),
            daemon=True
        ).start()

        return jsonify({
            **response_base,
            "status":                "triggered",
            "shifts_group_users_id": shifts_group_users_id,
            "group_id":              group_id,
            "outreach_id":           outreach_id,
            "user_id":               user_id,
            "staff_name":            full_name,
            "phone":                 phone,
            "shift_id":              shift_id,
            "triggered_at":          datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        }), 200

    # ------------------------------------------------------------------
    # 3. DEBUG
    # ------------------------------------------------------------------
    @app.route('/debug-shift-booking-bulk-call')
    def debug_shift_booking_bulk_call():
        allowed, now = is_within_call_window()
        return jsonify({
            "debug":          "shift_booking_bulk_call.py loaded",
            "server_time":    now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
        })