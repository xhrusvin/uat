# webhook.py   (placed in root, next to app.py)
from flask import Blueprint, request, jsonify, current_app
from bson import ObjectId
from datetime import datetime
import pytz
import os

webhook_bp = Blueprint('webhook', __name__, url_prefix='/api/webhook')

# Secure key from .env
WEBHOOK_KEY = os.getenv('WEBHOOK_SECRET_KEY')  # Set this in your .env

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def validate_date(date_str):
    if date_str is None:
        return None
    if not isinstance(date_str, str):
        return None, "schedule_call must be string or null"
    try:
        dt = datetime.strptime(date_str.strip(), DATE_FORMAT)
        return dt.strftime(DATE_FORMAT), None
    except ValueError:
        return None, f"Invalid format. Use: {DATE_FORMAT}"

@webhook_bp.route('/schedule-call/<user_id>', methods=['POST'])
def schedule_call(user_id):
    # === Security: API Key Check ===
    provided_key = request.headers.get('X-API-Key') or request.args.get('key')
    if not WEBHOOK_KEY or provided_key != WEBHOOK_KEY:
        return jsonify({"error": "Unauthorized – missing or invalid API key"}), 401

    if not request.is_json:
        return jsonify({"error": "JSON required"}), 400

    data = request.get_json() or {}
    schedule_raw = data.get("schedule_call")

    if not ObjectId.is_valid(user_id):
        return jsonify({"error": "Invalid user_id"}), 400

    schedule_str, error_msg = validate_date(schedule_raw)
    if error_msg:
        return jsonify({"error": error_msg}), 400

    # Access DB from the running Flask app
    users = current_app.db['users']

    result = users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": {
            "schedule_call": schedule_str,
            "updated_at": datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat()
        }}
    )

    if result.matched_count == 0:
        return jsonify({"error": "User not found"}), 404

    return jsonify({
        "success": True,
        "user_id": user_id,
        "schedule_call": schedule_str or None,
        "message": "Schedule updated"
    }), 200