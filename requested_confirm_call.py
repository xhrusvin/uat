# requested_confirm_call.py
import threading
import logging
from flask import current_app, jsonify, request
from bson import ObjectId
from requestedconfirmcall import make_requested_confirm_call
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


def _get_staff_user(app, record):
    """Join requested_confirm.staff_id → users._id → phone, first_name, last_name."""
    staff_id = record.get("staff_id")
    if not staff_id:
        return None
    try:
        return app.db.users.find_one(
            {"_id": ObjectId(str(staff_id))},
            {"phone": 1, "first_name": 1, "last_name": 1}
        )
    except Exception as e:
        log.warning(f"[USER JOIN] staff_id={staff_id}: {e}")
    return None


def _serialize_shift(shift):
    """Convert shift doc ObjectIds and datetimes to strings."""
    if not shift:
        return None
    result = {}
    for k, v in shift.items():
        key = "id" if k == "_id" else k
        if isinstance(v, ObjectId):
            result[key] = str(v)
        elif isinstance(v, datetime):
            result[key] = v.strftime("%Y-%m-%d %H:%M:%S UTC")
        elif isinstance(v, list):
            result[key] = [
                str(i) if isinstance(i, ObjectId)
                else i.strftime("%Y-%m-%d %H:%M:%S UTC") if isinstance(i, datetime)
                else i for i in v
            ]
        else:
            result[key] = v
    return result


def _get_shift_for_record(app, record):
    """Join requested_confirm.shift_id → shifts._id, return string-safe dict."""
    shift_id = record.get("shift_id")
    if not shift_id:
        return None
    try:
        return _serialize_shift(app.db.shifts.find_one({"_id": ObjectId(str(shift_id))}))
    except Exception as e:
        log.warning(f"[SHIFT JOIN] shift_id={shift_id}: {e}")
    return None


def register_requested_confirm_call_routes(app):

    # ------------------------------------------------------------------
    # 1. AUTO-TRIGGER: GET /requested_confirm_call
    # ------------------------------------------------------------------
    @app.route('/requested_confirm_call', methods=['GET'])
    def requested_confirm_call():
        allowed, server_time = is_within_call_window()
        user_id_param = request.args.get('user_id')

        response_base = {
            "server_time":    server_time.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
        }

        if not allowed:
            return jsonify({**response_base, "status": "outside_hours",
                            "message": "Calls only allowed from 8:00 AM to 8:00 PM UTC."}), 200

        query = (
            {"staff_id": ObjectId(user_id_param)}
            if user_id_param else
            {"$or": [{"call_sent": 0}, {"call_sent": {"$exists": False}}]}
        )

        record = app.db.requested_confirm.find_one(query, sort=[("confirmed_at", -1)])

        if not record:
            return jsonify({**response_base, "status": "no_pending",
                            "message": "No pending confirmation calls."}), 200

        if record.get("call_sent") == 1 and not user_id_param:
            return jsonify({**response_base, "status": "already_sent",
                            "message": "Call already triggered."}), 200

        confirm_id = record["_id"]

        # ── Join users via staff_id ───────────────────────────────
        staff_user = _get_staff_user(app, record)
        return jsonify(staff_user)
        if not staff_user:
            return jsonify({**response_base, "status": "no_user",
                            "message": "Staff user not found.",
                            "confirm_id": str(confirm_id),
                            "staff_id": str(record.get("staff_id", ""))}), 200

        phone      = staff_user.get("phone")
        first_name = staff_user.get("first_name", "")
        last_name  = staff_user.get("last_name", "")
        full_name  = f"{first_name} {last_name}".strip() or record.get("staff_name", "")

        if not phone:
            return jsonify({**response_base, "status": "no_phone",
                            "message": "No phone found for staff member.",
                            "confirm_id": str(confirm_id),
                            "staff_id": str(record.get("staff_id", ""))}), 200

        # ── Fetch shift ───────────────────────────────────────────
        shift = _get_shift_for_record(app, record)

        # Mark as sent
        result = app.db.requested_confirm.update_one(
            {"_id": confirm_id},
            {"$set": {"call_sent": 1, "call_sent_at": datetime.utcnow(), "updated_at": datetime.utcnow()}}
        )

        if result.modified_count == 0:
            return jsonify({**response_base, "status": "failed",
                            "message": "Failed to update record."}), 500

        threading.Thread(
            target=make_requested_confirm_call,
            args=(current_app._get_current_object(), phone, record, confirm_id, shift),
            daemon=True
        ).start()

        return jsonify({
            **response_base,
            "status":       "triggered",
            "confirm_id":   str(confirm_id),
            "shift_id":     str(record.get("shift_id", "")),
            "staff_id":     str(record.get("staff_id", "")),
            "staff_name":   full_name,
            "first_name":   first_name,
            "last_name":    last_name,
            "phone":        phone,
            "shift_code":   record.get("shift_code"),
            "client_name":  record.get("client_name"),
            "confirmed_at": record["confirmed_at"].strftime("%Y-%m-%d %H:%M:%S UTC")
                            if isinstance(record.get("confirmed_at"), datetime) else "unknown",
            "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        }), 200

    # ------------------------------------------------------------------
    # 2. MANUAL TRIGGER: POST /requested_confirm_call/trigger/<confirm_id>
    # ------------------------------------------------------------------
    @app.route('/requested_confirm_call/trigger/<confirm_id>', methods=['POST'])
    def requested_confirm_call_trigger(confirm_id):
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
            obj_id = ObjectId(confirm_id)
        except Exception:
            return jsonify({"status": "error", "message": "Invalid confirm_id"}), 400

        record = app.db.requested_confirm.find_one({"_id": obj_id})
        if not record:
            return jsonify({"status": "error", "message": "Record not found"}), 404

        if record.get("call_sent") == 1:
            return jsonify({**response_base, "status": "info",
                            "message": "Call already sent"}), 200

        # ── Join users via staff_id ───────────────────────────────
        staff_user = _get_staff_user(app, record)
        if not staff_user:
            return jsonify({**response_base, "status": "no_user",
                            "message": "Staff user not found."}), 200

        phone      = staff_user.get("phone")
        first_name = staff_user.get("first_name", "")
        last_name  = staff_user.get("last_name", "")
        full_name  = f"{first_name} {last_name}".strip() or record.get("staff_name", "")

        if not phone:
            return jsonify({**response_base, "status": "no_phone",
                            "message": "No phone found for staff member."}), 200

        # ── Fetch shift ───────────────────────────────────────────
        shift = _get_shift_for_record(app, record)

        app.db.requested_confirm.update_one(
            {"_id": obj_id},
            {"$set": {"call_sent": 1, "call_sent_at": datetime.utcnow(), "updated_at": datetime.utcnow()}}
        )

        threading.Thread(
            target=make_requested_confirm_call,
            args=(current_app._get_current_object(), phone, record, obj_id, shift),
            daemon=True
        ).start()

        return jsonify({
            **response_base,
            "status":       "triggered",
            "confirm_id":   str(obj_id),
            "shift_id":     str(record.get("shift_id", "")),
            "staff_id":     str(record.get("staff_id", "")),
            "staff_name":   full_name,
            "first_name":   first_name,
            "last_name":    last_name,
            "phone":        phone,
            "shift_code":   record.get("shift_code"),
            "client_name":  record.get("client_name"),
            "confirmed_at": record["confirmed_at"].strftime("%Y-%m-%d %H:%M:%S UTC")
                            if isinstance(record.get("confirmed_at"), datetime) else "unknown",
            "triggered_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        }), 200

    # ------------------------------------------------------------------
    # 3. DEBUG: GET /debug-requested-confirm-call
    # ------------------------------------------------------------------
    @app.route('/debug-requested-confirm-call')
    def debug_requested_confirm_call():
        allowed, now = is_within_call_window()
        return jsonify({
            "debug":          "requested_confirm_call.py loaded",
            "server_time":    now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 - {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
            "note":           "Auto-call only runs during allowed hours.",
        })
