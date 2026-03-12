# admin.py
from flask import (
    redirect, url_for, flash, current_app, jsonify,
    request, session, render_template, Response
)
from functools import wraps
from . import admin_bp
from datetime import datetime, timedelta
import bcrypt
from pytz import utc
import pytz
from bson import ObjectId
from bson.binary import Binary
import os
import asyncio
import aiohttp
import threading
import re
from pymongo.errors import OperationFailure
import requests


now_utc = datetime.now(pytz.UTC)


XN_PORTAL_BASE_URL = os.getenv('XN_PORTAL_BASE_URL')
NEXT_FOLLOW_UP_MINUTES = 2
NEXT_FOLLOW_UP_HOURS = 24

current_time = datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M")

def to_str(value):
    """
    Convert any value to string.
    - None → "null" or "" (your choice)
    - int/float → str without scientific notation
    - bool → "true"/"false" or "True"/"False"
    - Already string → return as-is
    """
    if value is None:
        return ""  # or return "null" if you prefer
    return str(value).strip()

def run_async(coro):
    """
    Safely runs an async coroutine in a new event loop (thread-safe).
    This avoids the "asyncio.run() cannot be called from a running event loop" error.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# ==================================================================
# HELPER: Normalize password hash from any MongoDB storage type
# ==================================================================
def normalize_stored_hash(stored_hash):
    """
    Safely convert stored password hash to bytes, regardless of storage format:
    - str → encode to UTF-8 bytes
    - bson.Binary → convert to bytes
    - bytearray → convert to bytes
    - bytes → return as-is
    - anything else → return None (invalid)
    """
    if isinstance(stored_hash, str):
        try:
            return stored_hash.encode('utf-8')
        except (UnicodeEncodeError, AttributeError):
            return None
    elif isinstance(stored_hash, (bytes, bytearray, Binary)):
        return bytes(stored_hash)
    else:
        return None


# ------------------------------------------------------------------
# ADMIN LOGIN ROUTE (API + PAGE)
# ------------------------------------------------------------------
@admin_bp.route('/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'GET':
        return render_template('admin/login.html')

    data = request.get_json()
    email = data.get('email')
    password = data.get('password')

    if not email or not password:
        return jsonify({"error": "Email and password required"}), 400

    user = current_app.db.users.find_one({"email": email})
    if not user or not user.get('is_admin'):
        return jsonify({"error": "Invalid credentials"}), 401

    # === ROBUST HASH VERIFICATION (same as change_password) ===
    normalized_hash = normalize_stored_hash(user.get('password'))
    if normalized_hash is None or not bcrypt.checkpw(password.encode('utf-8'), normalized_hash):
        return jsonify({"error": "Invalid credentials"}), 401

    # Set session
    session['user_id'] = str(user['_id'])
    session['email'] = user['email']
    session['is_admin'] = True
    session['name'] = f"{user.get('first_name','')} {user.get('last_name','')}".strip()

    return jsonify({"message": "Admin login successful"}), 200


# ------------------------------------------------------------------
# PROTECTED ROUTES DECORATOR
# ------------------------------------------------------------------
def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session or not session.get('is_admin'):
            return redirect(url_for('admin.admin_login'))
        return f(*args, **kwargs)
    return decorated


# ------------------------------------------------------------------
# DASHBOARD
# ------------------------------------------------------------------
@admin_bp.route('/')
@admin_required
def dashboard():
    total_users = current_app.db.users.count_documents({"is_admin": {"$ne": True}})
    recent_users = list(current_app.db.users.find({"is_admin": {"$ne": True}}).sort("created_at", -1).limit(10))

    now = datetime.now(utc)
    seven_days_ago = now - timedelta(days=7)
    new_registrations = 0

    for user in recent_users:
        created_at = user.get('created_at')
        if isinstance(created_at, str):
            try:
                if created_at.endswith('Z'):
                    created_at = created_at.replace('Z', '+00:00')
                created_at = datetime.fromisoformat(created_at)
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=utc)
                else:
                    created_at = created_at.astimezone(utc)
            except Exception:
                continue
        elif isinstance(created_at, datetime):
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=utc)
            else:
                created_at = created_at.astimezone(utc)
        else:
            continue

        if created_at >= seven_days_ago:
            new_registrations += 1

    active_shifts = 25
    pending_approvals = 3
    top_designations = list(current_app.db.users.aggregate([
        {"$match": {"is_admin": {"$ne": True}}},
        {"$group": {"_id": "$designation", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 5}
    ]))

    return render_template('admin/dashboard.html',
                           current_time=current_time,
                           total_users=total_users,
                           active_shifts=active_shifts,
                           new_registrations=new_registrations,
                           pending_approvals=pending_approvals,
                           recent_users=recent_users,
                           top_designations=top_designations)


# ------------------------------------------------------------------
# OTHER PROTECTED ROUTES
# ------------------------------------------------------------------
@admin_bp.route('/shifts')
@admin_required
def shifts():
    return render_template('admin/shifts.html')


@admin_bp.route('/reports')
@admin_required
def reports():
    return render_template('admin/reports.html')


@admin_bp.route('/users')
@admin_required
def users():
    page = int(request.args.get('page', 1))
    per_page = 10
    search = request.args.get('search', '').strip()

    query = {"is_admin": {"$ne": True}}
    if search:
        query["email"] = {"$regex": search, "$options": "i"}

    total = current_app.db.users.count_documents(query)
    users_list = list(
        current_app.db.users.find(query)
        .sort("created_at", -1)
        .skip((page - 1) * per_page)
        .limit(per_page)
    )

    for u in users_list:
        u['call_sent'] = u.get('call_sent', 1)
        u['garda_email_sent_status'] = "Sent" if u.get('garda_email_sent') == 1 else "No"
        u['missed_call_email_sent_status'] = "Sent" if u.get('missed_call_email_sent') == 1 else "No"
        created = u.get('created_at')
        if isinstance(created, datetime):
            u['created_at_formatted'] = created.strftime('%d %b %Y')
            u['created_at_time'] = created.strftime('%H:%M')
        elif isinstance(created, str):
            try:
                dt = datetime.fromisoformat(created.replace('Z', '+00:00'))
                u['created_at_formatted'] = dt.strftime('%d %b %Y')
                u['created_at_time'] = dt.strftime('%H:%M')
            except:
                u['created_at_formatted'] = '—'
                u['created_at_time'] = ''
        else:
            u['created_at_formatted'] = '—'
            u['created_at_time'] = ''

    return render_template('admin/users.html',
                           users=users_list,
                           page=page,
                           total=total,
                           per_page=per_page,
                           search=search)


@admin_bp.route('/change_password', methods=['GET', 'POST'])
@admin_required
def change_password():
    if request.method == 'POST':
        current_password = (request.form.get('current_password') or "").strip()
        new_password = (request.form.get('new_password') or "").strip()
        confirm_password = (request.form.get('confirm_password') or "").strip()

        if not all([current_password, new_password, confirm_password]):
            flash("All fields are required.", "danger")
            return redirect(url_for('admin.change_password'))

        if new_password != confirm_password:
            flash("New passwords do not match.", "danger")
            return redirect(url_for('admin.change_password'))

        if len(new_password) < 6:
            flash("New password must be at least 6 characters.", "danger")
            return redirect(url_for('admin.change_password'))

        user = current_app.db.users.find_one({"_id": ObjectId(session['user_id'])})
        if not user:
            flash("User not found.", "danger")
            return redirect(url_for('admin.change_password'))

        # === SAME ROBUST VERIFICATION AS LOGIN ===
        normalized_hash = normalize_stored_hash(user.get('password'))
        if normalized_hash is None:
            flash("Invalid password format in database.", "danger")
            return redirect(url_for('admin.change_password'))

        if not bcrypt.checkpw(current_password.encode('utf-8'), normalized_hash):
            flash("Current password is incorrect.", "danger")
            return redirect(url_for('admin.change_password'))

        # Generate new hash (always bytes)
        new_hash = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt())

        update_data = {
            "password": new_hash,
            "updated_at": datetime.utcnow(),
            "phone": user.get('phone', ''),           # ensure phone exists
            "call_sent": user.get('call_sent', 1)     # ensure call_sent exists
        }

        result = current_app.db.users.update_one(
            {"_id": ObjectId(session['user_id'])},
            {"$set": update_data}
        )

        if result.modified_count == 1:
            flash("Password changed successfully!", "success")
            return redirect(url_for('admin.dashboard'))
        else:
            flash("Failed to update password.", "danger")

    return render_template('admin/change_password.html')


@admin_bp.route('/settings', methods=['GET', 'POST'])
@admin_required
def settings():
    settings_doc = current_app.db.settings.find_one({"_id": "global"})
    
    if request.method == 'POST':
        allow_registration_call = request.form.get('allow_registration_call') == 'on'
        enable_support_agent = request.form.get('enable_support_agent') == 'on'
        enable_lead_call = request.form.get('enable_lead_call') == 'on'
        enable_follow_up_call = request.form.get('enable_follow_up_call') == 'on'
        enable_follow_up_call_bot4 = request.form.get('enable_follow_up_call_bot4') == 'on'

        current_app.db.settings.update_one(
            {"_id": "global"},
            {"$set": {
                "allow_registration_call": allow_registration_call,
                "enable_support_agent": enable_support_agent,
                "enable_lead_call": enable_lead_call,
                "enable_follow_up_call": enable_follow_up_call,
                "enable_follow_up_call_bot4": enable_follow_up_call_bot4,
                "updated_at": datetime.utcnow()
            }},
            upsert=True
        )

        flash("Settings updated successfully!", "success")
        return redirect(url_for('admin.settings'))

    return render_template('admin/settings.html', settings=settings_doc)


@admin_bp.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('admin.admin_login'))


@admin_bp.route('/api/delete_user', methods=['POST'])
@admin_required
def delete_user():
    data = request.get_json()
    user_id = data.get('user_id')

    if not user_id or not ObjectId.is_valid(user_id):
        return jsonify({"error": "Invalid user ID"}), 400

    user = current_app.db.users.find_one({"_id": ObjectId(user_id)})
    if user and user.get('is_admin'):
        return jsonify({"error": "Cannot delete admin user"}), 403

    result = current_app.db.users.delete_one({"_id": ObjectId(user_id)})
    if result.deleted_count == 1:
        return jsonify({"success": True, "message": "User deleted"}), 200
    else:
        return jsonify({"error": "User not found"}), 404


# ==================================================================
# TRANSCRIPTIONS – LIST FINISHED CALLS
# ==================================================================
def _format_conv(conv):
    tz_utc = pytz.UTC
    conv_id = str(conv.get('_id', ''))
    conv['conv_id'] = conv_id

    # === SAFE USER EXTRACTION ===
    user = {}
    user_info = conv.get('user_info') or []
    if isinstance(user_info, list) and len(user_info) > 0:
        user = user_info[0]
    elif isinstance(user_info, dict):
        user = user_info

    # Safely extract first_name and last_name as strings
    def safe_str(val, default=""):
        if val is None:
            return default
        if isinstance(val, str):
            return val.strip()
        if isinstance(val, (int, float)):
            return str(val).strip()
        if isinstance(val, datetime):
            return ""  # ignore datetime values
        return str(val).strip() if hasattr(val, '__str__') else default

    first_name = safe_str(user.get('first_name'))
    last_name  = safe_str(user.get('last_name'))

    full_name = " ".join(filter(None, [first_name, last_name])).strip()
    conv['name'] = full_name or "Unknown User"
    conv['designation'] = safe_str(user.get('designation'), '-')
    conv['country'] = safe_str(user.get('country'), '-')

    # === DATE FORMATTING (also make safe) ===
    try:
        if conv.get('started_at'):
            if isinstance(conv['started_at'], str):
                # Handle ISO strings with/without Z
                dt = conv['started_at'].replace('Z', '+00:00') if 'Z' in conv['started_at'] else conv['started_at']
                conv['started_at'] = datetime.fromisoformat(dt).astimezone(tz_utc).strftime('%Y-%m-%d %H:%M:%S')
            else:
                conv['started_at'] = conv['started_at'].astimezone(tz_utc).strftime('%Y-%m-%d %H:%M:%S')
        else:
            conv['started_at'] = '—'
    except Exception:
        conv['started_at'] = 'Invalid'

    try:
        ended_at = conv.get('ended_at')
        if ended_at:
            if isinstance(ended_at, str):
                dt = ended_at.replace('Z', '+00:00') if 'Z' in ended_at else ended_at
                conv['ended_at'] = datetime.fromisoformat(dt).astimezone(tz_utc).strftime('%Y-%m-%d %H:%M:%S')
            else:
                conv['ended_at'] = ended_at.astimezone(tz_utc).strftime('%Y-%m-%d %H:%M:%S')
        else:
            conv['ended_at'] = 'Ongoing'
    except Exception:
        conv['ended_at'] = 'Invalid'

    # === TURNS ===
    formatted_turns = []
    for turn in conv.get('turns', []):
        try:
            time_str = turn['ts'].astimezone(tz_utc).strftime('%H:%M:%S') if turn.get('ts') else '—'
        except Exception:
            time_str = '—'
        formatted_turns.append({
            'role': turn.get('role', 'unknown'),
            'text': turn.get('text', ''),
            'time': time_str
        })
    conv['turns'] = formatted_turns

    # === ELEVENLABS ID ===
    elevenlabs_id = conv.get('elevenlabs_conversation_id', '')
    conv['elevenlabs_conversation_id'] = elevenlabs_id
    conv['has_audio'] = bool(elevenlabs_id)

    return {
        'conv_id': conv_id,
        'phone': conv.get('phone', ''),
        'name': conv['name'],
        'designation': conv['designation'],
        'country': conv['country'],
        'started_at': conv['started_at'],
        'ended_at': conv['ended_at'],
        'turns': conv['turns'],
        'elevenlabs_conversation_id': elevenlabs_id,
        'has_audio': bool(elevenlabs_id)
    }



def safe_regex_pattern(text: str) -> str:
    """Escape only the characters MongoDB treats as regex metachars"""
    if not text:
        return ""
    meta = r'^$.*+?()[]{}|'
    escaped = ''
    for c in text:
        if c in meta:
            escaped += '\\' + c
        else:
            escaped += c
    return escaped


@admin_bp.route('/transcriptions')
@admin_required
def transcriptions():
    page = int(request.args.get('page', 1))
    per_page = 10
    search = request.args.get('search', '').strip()

    pipeline = [
        {"$sort": {"started_at": -1}},
        {"$lookup": {
            "from": "users",
            "localField": "phone",
            "foreignField": "phone",
            "as": "user_info"
        }},
        # Unwind user_info so we can work with fields easily (optional but makes $match simpler)
        {"$unwind": {"path": "$user_info", "preserveNullAndEmptyArrays": True}},
    ]

    if search:
        # 1. Phone search – safely escaped so +91 works perfectly
        phone_pattern = safe_regex_pattern(search)

        # 2. Name search – split into tokens and search each part in first_name OR last_name
        tokens = [t.strip() for t in search.split() if len(t.strip()) >= 2]
        name_or_conditions = []
        for token in tokens:
            token_pattern = safe_regex_pattern(token)
            name_or_conditions.extend([
                {"user_info.first_name": {"$regex": token_pattern, "$options": "i"}},
                {"user_info.last_name":  {"$regex": token_pattern, "$options": "i"}},
            ])

        # 3. Full concatenated name search (covers "Rus Vin" → "Rusvin")
        full_name_pattern = safe_regex_pattern(search)
        full_name_condition = {
            "$expr": {
                "$regexMatch": {
                    "input": {
                        "$concat": [
                            {"$ifNull": ["$user_info.first_name", ""]}, " ",
                            {"$ifNull": ["$user_info.last_name", ""]}
                        ]
                    },
                    "regex": full_name_pattern,
                    "options": "i"
                }
            }
        }

        # Combine everything
        final_or = [{"phone": {"$regex": phone_pattern, "$options": "i"}}]
        if name_or_conditions:
            final_or.extend(name_or_conditions)
        final_or.append(full_name_condition)

        pipeline.append({"$match": {"$or": final_or}})

    # Total count
    count_pipeline = pipeline + [{"$count": "total"}]
    total_result = list(current_app.db.conversations.aggregate(count_pipeline, allowDiskUse=True))
    total = total_result[0]["total"] if total_result else 0

    # Pagination + final fields
    result_pipeline = pipeline + [
        {"$skip": (page - 1) * per_page},
        {"$limit": per_page},
        {"$project": {
            "_id": 1,
            "phone": 1,
            "started_at": 1,
            "ended_at": 1,
            "turns": 1,
            "user_info": 1,
            "elevenlabs_conversation_id": 1
        }}
    ]

    raw_convs = list(current_app.db.conversations.aggregate(result_pipeline, allowDiskUse=True))
    convs = [_format_conv(c) for c in raw_convs]

    return render_template(
        'admin/transcriptions.html',
        convs=convs,
        page=page,
        total=total,
        per_page=per_page,
        search=search
    )

@admin_bp.route('/conversation/<conv_id>/audio')
@admin_required
def get_conversation_audio(conv_id):
    #try:
        conv = current_app.db.conversations.find_one({"_id": ObjectId(conv_id)})
        if not conv:
            return "Conversation not found", 404

        el_id = conv.get("elevenlabs_conversation_id")
        if not el_id:
            current_app.logger.info(f"Conv {conv_id} has no ElevenLabs ID")
            return "Audio not generated for this call", 404

        url = f"https://api.elevenlabs.io/v1/convai/conversations/{el_id}/audio"
        api_key = os.getenv("ELEVENLABS_API_KEY")
        if not api_key:
            current_app.logger.error("ELEVENLABS_API_KEY not set")
            return "Server configuration error", 500

        async def fetch_audio():
            async with aiohttp.ClientSession() as session:
                headers = {"xi-api-key": api_key}
                async with session.get(url, headers=headers) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        current_app.logger.warning(f"ElevenLabs error {resp.status}: {text}")
                        return None
                    return await resp.read()

        # This is the safe way to run async code in sync Flask view
        audio_bytes = run_async(fetch_audio())

        if not audio_bytes:
            return "Audio not available (may still be processing)", 404

        return Response(
            audio_bytes,
            mimetype='audio/mpeg',
            headers={
                'Content-Disposition': f'attachment; filename="call_{conv_id}.mp3"',
                'Cache-Control': 'no-cache',
            }
        )

    # except Exception as e:
    #     current_app.logger.error(f"Error fetching audio for {conv_id}: {e}", exc_info=True)
    #     return "Internal server error", 500

# ==================================================================
# BRIEF SUMMARY – LATEST CALL (NO ID NEEDED)
# ==================================================================
@admin_bp.route('/api/brief-summary')
@admin_required
def api_latest_brief_summary():
    try:
        # Find latest completed call that hasn't had data collected yet
        latest_conv = current_app.db.conversations.find_one({
            "elevenlabs_conversation_id": {"$exists": True, "$ne": None, "$ne": ""},
            "ended_at": {"$ne": "Ongoing", "$ne": None},
            "data_collected": {"$ne": 1}  # ← Only fetch if not already done
        }, sort=[("ended_at", -1)])

        # If no new call → return last saved one (or empty)
        if not latest_conv:
            # Fall back to most recent with saved data
            fallback = current_app.db.conversations.find_one({
                "data_collected": 1
            }, sort=[("ended_at", -1)])

            if fallback and fallback.get("collected_lead_data"):
                saved = fallback["collected_lead_data"]
                saved["from_cache"] = True
                saved["note"] = "Already processed earlier"
                return jsonify(saved), 200
            else:
                return jsonify({
                    "message": "No completed calls with lead data yet",
                    "collected_data": {}
                }), 200

        el_id = latest_conv["elevenlabs_conversation_id"]
        conv_id = latest_conv["_id"]

        # Fetch from ElevenLabs
        api_key = os.getenv("ELEVENLABS_API_KEY")
        if not api_key:
            return jsonify({"error": "Missing ELEVENLABS_API_KEY"}), 500

        import requests
        url = f"https://api.elevenlabs.io/v1/convai/conversations/{el_id}"
        resp = requests.get(url, headers={"xi-api-key": api_key}, timeout=25)

        if resp.status_code != 200:
            current_app.logger.warning(f"ElevenLabs {resp.status_code} for {el_id}: {resp.text}")
            return jsonify({
                "message": "Analysis not ready yet",
                "elevenlabs_id": el_id,
                "collected_data": {}
            }), 202

        data = resp.json()
        analysis = data.get("analysis") or {}
        dcr = analysis.get("data_collection_results") or {}

        # Build clean collected data
        collected_data = {}
        schedule_call_value = None

        for field_id, item in dcr.items():
            value = item.get("value")
            collected_data[field_id] = value
            if field_id == "schedule_call":
                schedule_call_value = value

        # Extract caller name
        caller_name = "Unknown Caller"
        user_info = latest_conv.get("user_info")
        if user_info:
            if isinstance(user_info, list) and user_info:
                u = user_info[0]
            elif isinstance(user_info, dict):
                u = user_info
            else:
                u = {}
            caller_name = " ".join(filter(None, [
                u.get("first_name", "").strip(),
                u.get("last_name", "").strip()
            ])) or "Unknown Caller"

        # Final result to return
        result = {
            "elevenlabs_conversation_id": el_id,
            "internal_conv_id": str(conv_id),
            "phone": latest_conv.get("phone"),
            "caller_name": caller_name,
            "ended_at": latest_conv.get("ended_at"),
            "summary_title": analysis.get("call_summary_title"),
            "collected_data": collected_data,
            "schedule_call": schedule_call_value,
            "total_fields": len(collected_data),
            "from_cache": False
        }

        # SAVE TO DB so we never fetch again
        update_data = {
            "data_collected": 1,
            "collected_lead_data": result,           # full structured data
            "collected_at": datetime.utcnow(),
            "collected_lead_summary": analysis.get("call_summary_title"),
            "schedule_call_requested": bool(schedule_call_value)
        }

        current_app.db.conversations.update_one(
            {"_id": conv_id},
            {"$set": update_data}
        )

        current_app.logger.info(f"Collected lead data saved for conv {conv_id} | Schedule call: {schedule_call_value}")

        return jsonify(result), 200

    except Exception as e:
        current_app.logger.error(f"api_latest_brief_summary CRASH: {e}", exc_info=True)
        return jsonify({
            "error": "Server error",
            "details": str(e),
            "collected_data": {}
        }), 500



@admin_bp.route('/api/brief-summary-conv')
def api_brief_summary_cov():
    conv_id = request.args.get('conv_id')
    collection = request.args.get('collection')
    if not conv_id:
        return jsonify({"error": "Missing conv_id parameter"}), 400
    
    try:
        # Step 1: Find the user by last_elevenlabs_conversation_id
        user = current_app.db.users.find_one({
            "last_elevenlabs_conversation_id": conv_id
        })

        if not user:
            return jsonify({
                "message": "No user found with this conversation ID",
                "collected_data": {}
            }), 404

        xn_user_id = user.get("xn_user_id")
        if not xn_user_id:
            return jsonify({
                "message": "User found but missing xn_user_id",
                "collected_data": {}
            }), 404

        first_name = user.get("first_name", "").strip()
        last_name = user.get("last_name", "").strip()
        caller_name = " ".join(filter(None, [first_name, last_name])) or "Unknown Caller"
        phone = user.get("phone")

        # --- FIX: Initialize conversation as None upfront ---
        conversation = None

        # Step 2: Determine which collection to query
        if collection in (None, "1", 1):  # Accept None, "1", or 1
            conversation = current_app.db.conversations.find_one({
                "elevenlabs_conversation_id": conv_id,
                "ended_at": {"$ne": "Ongoing", "$exists": True, "$ne": None}
            })
        elif collection in ("2", 2):
            conversation = current_app.db.follow_up_conv.find_one({
                "elevenlabs_conversation_id": conv_id,
                "ended_at": {"$ne": "Ongoing", "$exists": True, "$ne": None}
            })

        # --- Critical: If conversation not found or not ended, return early ---
        if not conversation:
            return jsonify({
                "message": "Conversation not found or not completed",
                "collected_data": {}
            }), 404

        # === NEW BLOCK START ===
        email_message = ""
        if collection in (None, "1", 1):
            garda_url = f"https://expresshealth.ie/lead-registration/garda-vetting-email?id={user['_id']}"
            try:
                import requests
                garda_resp = requests.get(garda_url, timeout=100)
                if garda_resp.status_code in (200, 201):
                    current_app.logger.info(f"Garda vetting email request successful for user {user['_id']}")
                    email_message = garda_resp.text
                else:
                    current_app.logger.warning(
                        f"Garda vetting email request failed {garda_resp.status_code} for user {user['_id']}: {garda_resp.text}"
                    )
                    email_message = garda_resp.text
            except Exception as garda_err:
                current_app.logger.error(f"Garda vetting email request exception for user {user['_id']}: {garda_err}")
                email_message = str(garda_err)
        # === NEW BLOCK END ===

        el_id = conv_id

        # Fetch from ElevenLabs
        api_key = os.getenv("ELEVENLABS_API_KEY")
        if not api_key:
            return jsonify({"error": "Missing ELEVENLABS_API_KEY"}), 500

        import requests
        url = f"https://api.elevenlabs.io/v1/convai/conversations/{el_id}"
        resp = requests.get(url, headers={"xi-api-key": api_key}, timeout=25)

       





        if resp.status_code != 200:
            current_app.logger.warning(f"ElevenLabs {resp.status_code} for {el_id}: {resp.text}")
            return jsonify({
                "message": "Analysis not ready yet or error fetching from ElevenLabs",
                "elevenlabs_id": el_id,
                "collected_data": {}
            }), 202

        data = resp.json()
        analysis = data.get("analysis") or {}
        dcr = analysis.get("data_collection_results") or {}


         #fetch current details
        if xn_user_id:
            details_url = f"{XN_PORTAL_BASE_URL}ai/recruitments/detail"
            details_payload = {
                "_id": str(xn_user_id)
            }

            details_headers = {
                "Api-Key": os.getenv("XN_PORTAL_API_KEY"),         # Set this in env
                "X-App-Country": os.getenv("XN_APP_COUNTRY"),       # Set this in env
                "Content-Type": "application/json"
            }

            details_resp = requests.get(details_url, json=details_payload, headers=details_headers, timeout=25)

            details_resp.raise_for_status()   

            try:
              api_data = details_resp.json()
            except ValueError:
                # Response was not JSON → handle gracefully
                return jsonify({
                    "success": False,
                    "message": "Invalid JSON response from server",
                    "xn_user_id": xn_user_id,
                    "collected_data": {},
                    "errors": ["Response is not valid JSON"]
                }), 500   
            user_data = api_data.get("data")

            if user_data is not None:
                # Store to variable (you can use it later if needed)
                collected_user_info = user_data       # ← here is your variable

                response_body = {
                 "success": True,
                 "message": api_data.get("message", "Recruitment detail fetched"),
                 "data": collected_user_info,      # ← full user object here (most useful)
                 "xn_user_id": xn_user_id,
                 "collected_data": {},             # or put collected_user_info here if needed
                 "status_code": api_data.get("status_code", details_resp.status_code),
                 "errors": []
                  }
                status_code = 200
            else:
                # "data" was missing or null
                response_body = {
                    "success": False,
                    "message": "User data not found in response",
                    "xn_user_id": xn_user_id,
                    "collected_data": {},
                    "errors": ["'data' field is missing or null"]
                }
                status_code = 404   # or 200 — depends on your API contract
                return jsonify(response_body), status_code

            

        # Get current user data to check what's already present
        current_user_data = current_app.db.users.find_one({"_id": user["_id"]})
        
        # Build collected data
        collected_data = {}
        schedule_call_value = ""
        full_address = ""
        dob = ""
        eir_code = ""
        years_experience_ireland = ""
        location_in_ireland = ""
        company_name = ""
        company_phone = ""
        job_title = ""
        travel_mode = ""
        last_company_experience_year = ""
        masters = ""
        visa_type = ""
        previous_work_county = ""
        pps_number = ""
        uniform_size = ""
        tuberculosis_vaccine = ""
        hepatitis_antibody = ""
        mmr_vaccine = ""
        covid_19_vaccine = ""
        gender = ""

        for field_id, item in dcr.items():
          raw_value = item.get("value")
          value = to_str(raw_value)  # Convert EVERYTHING to clean string early

          collected_data[field_id] = value # Store as string or None

          if field_id == "schedule_call":
            schedule_call_value = value

          if field_id == "gender":
            gender = value

          if field_id == "full_address" and value:
            full_address = value

          if field_id == "dob" and value:
            dob = value

          if field_id == "eir_code" and value:
            eir_code = value

          if field_id == "years_experience_ireland" and value:
            years_experience_ireland = value

          if field_id == "location_in_ireland" and value:
            location_in_ireland = value

          if field_id == "last_employer_name" and value:
            company_name = value 

          if field_id == "employer_phone_number" and value:
            company_phone = value 

          if field_id == "last_job_title" and value:
            job_title = value

          if field_id == "commute_plan" and value:
            travel_mode = value  

          if field_id == "employment_duration_years" and value:
            last_company_experience_year = value

          if field_id == "masters" and value:
            masters = value

          if field_id == "visa_type" and value:
            visa_type = value

          if field_id == "previous_work_county" and value:
            previous_work_county = value

          if field_id == "pps_number" and value:
            pps_number = value

          if field_id == "uniform_size" and value:
            uniform_size = value

          if field_id == "tuberculosis_vaccine" and value:
            tuberculosis_vaccine = value

          if field_id == "hepatitis_b_antibodies" and value:
            hepatitis_antibody = value

          if field_id == "mmr_varicella_vaccination" and value:
            mmr_vaccine = value

          if field_id == "covid_vaccination" and value:
            covid_19_vaccine = value


        next_follow_up_at = now_utc + timedelta(hours=24)
        next_compliance_document_at = now_utc + timedelta(hours=56)
        next_professional_reference_at = now_utc + timedelta(hours=240)
        

        update_fields = {
          "next_follow_up_at": next_follow_up_at,
          "next_compliance_document_at": next_compliance_document_at,
          "next_professional_reference_at": next_professional_reference_at,
          "follow_up_sent": 1 if collection in ("2", 2) else 0,
          "updated_at": now_utc
        }

    

        update_result = current_app.db.users.update_one(
        {"_id": user["_id"]},
        {"$set": update_fields}
        )

        if update_result.matched_count == 0:
           current_app.logger.warning("No user found for follow-up update")

        if update_result.modified_count > 0:
           current_app.logger.info(f"User data updated with selective fields: ")

        # Final result
        result = {
            "elevenlabs_conversation_id": el_id,
            "internal_conv_id": str(conversation["_id"]),
            "xn_user_id": xn_user_id,
            "phone": phone,
            "caller_name": caller_name,
            "ended_at": conversation.get("ended_at"),
            "summary_title": analysis.get("call_summary_title"),
            "collected_data": collected_data,
            "schedule_call": schedule_call_value,
            "total_fields": len(collected_data),
            "from_cache": False,
            "address_update_status": "not_triggered",
            "email_status": email_message
        }

        # === UPDATE EXTERNAL API IF ADDRESS FOUND ===
        if xn_user_id and user_data is not None:
            update_url = f"{XN_PORTAL_BASE_URL}ai/recruitments/update"
    
            update_payload = {
                "_id": str(xn_user_id),
            }
    
             # Helper: only add field if we have a real value AND current value is missing/empty
            def should_update(current_val, new_val):
             if not new_val:  # new_val is "", None, False, 0, etc.
               return False
             # Treat various "empty" representations as missing
             if current_val in (None, "", "null", "None", [], {}):
               return True
             return False
    
           # ────────────────────────────────────────────────
           # Add fields conditionally
           # ────────────────────────────────────────────────
            # ────────────────────────────────────────────────
            # Update ONLY if the field is currently None in user_data
            # ────────────────────────────────────────────────
            if user_data.get("gender") is None:
                update_payload["gender"] = gender
    
            if user_data.get("dob") is None:
                update_payload["dob"] = dob
    
            if user_data.get("county_id") is None:
                update_payload["county_id"] = location_in_ireland
    
            if user_data.get("eir_code") is None:
                update_payload["eir_code"] = eir_code
    
            if user_data.get("address") is None:
                update_payload["address"] = full_address
    
            if user_data.get("experience_year") is None:
                update_payload["experience_year"] = years_experience_ireland
    
            if user_data.get("masters") is None:
                update_payload["masters"] = masters
    
            if user_data.get("travel_mode") is None:
                update_payload["travel_mode"] = travel_mode
    
            if user_data.get("company_name") is None:
                update_payload["company_name"] = company_name
    
            if user_data.get("job_title") is None:
                update_payload["job_title"] = job_title
    
            if user_data.get("company_phone") is None:
                update_payload["company_phone"] = company_phone
    
            if user_data.get("last_company_experience_year") is None:
                update_payload["last_company_experience_year"] = last_company_experience_year
    
            if user_data.get("company_county_id") is None:
                update_payload["company_county_id"] = previous_work_county
    
            if user_data.get("pps_number") is None:
                update_payload["pps_number"] = pps_number
    
            if user_data.get("visa_type_id") is None:
                update_payload["visa_type_id"] = visa_type
    
            if user_data.get("uniform_size") is None:
                update_payload["uniform_size"] = uniform_size
    
            if user_data.get("tuberculosis_vaccine") is None:
                update_payload["tuberculosis_vaccine"] = tuberculosis_vaccine
    
            if user_data.get("hepatitis_antibody") is None:
                update_payload["hepatitis_antibody"] = hepatitis_antibody
    
            if user_data.get("mmr_vaccine") is None:
                update_payload["mmr_vaccine"] = mmr_vaccine
    
            if user_data.get("covid_19_vaccine") is None:
                update_payload["covid_19_vaccine"] = covid_19_vaccine
             # ────────────────────────────────────────────────
             # Only send request if we actually have something to update
             # (besides _id)
             # ────────────────────────────────────────────────
            
            update_headers = {
                     "Api-Key": os.getenv("XN_PORTAL_API_KEY"),
                     "X-App-Country": os.getenv("XN_APP_COUNTRY"),
                     "Content-Type": "application/json"
                 }

        try:
           
            update_resp = requests.get(           # ← most update endpoints use POST, not GET
                update_url,
                json=update_payload,
                headers=update_headers,
                timeout=10
                )
            
            if update_resp.status_code in (200, 201, 204):
                current_app.logger.info(
                    f"Selective update successful for xn_user_id {xn_user_id} "
                    f"({len(update_payload)-1} fields)"
                )
                result["address_update_status"] = {
                    "status": "success",
                    "updated_fields": list(update_payload.keys())[1:],  # exclude _id
                    "response": update_resp.json() if update_resp.text else {}
                }
            else:
                current_app.logger.warning(
                    f"Update failed {update_resp.status_code} for {xn_user_id}: {update_resp.text}"
                )
                result["address_update_status"] = {
                    "status": "failed",
                    "code": update_resp.status_code,
                    "message": update_resp.text[:200]
                }

        except Exception as exc:
            current_app.logger.error(f"Update request exception: {exc}", exc_info=True)
            result["address_update_status"] = {
                "status": "request_error",
                "error": str(exc)
            }
        else:
            result["address_update_status"] = update_resp.json() if update_resp.text else {}

        if not xn_user_id or not user_data:
            result["address_update_status"] = "skipped_missing_xn_user_id_or_user_data"

        current_app.logger.info(f"No address found in collected_data for conv {el_id}")

        current_app.logger.info(f"Brief summary fetched for EL conv {el_id} (xn_user_id: {xn_user_id}) | Schedule call: {schedule_call_value}")

        return jsonify(result), 200

    except Exception as e:
        current_app.logger.error(f"api_brief_summary_cov CRASH: {e}", exc_info=True)
        return jsonify({
            "error": "Server error",
            "details": str(e),
            "collected_data": {}
        }), 500


@admin_bp.route('/elevenlabs/api/conversation/<conversation_id>')
@admin_required
def elevenlabs_api_proxy(conversation_id):
    api_key = os.getenv("ELEVENLABS_API_KEY")
    if not api_key:
        return jsonify({"error": "API key missing"}), 500

    url = f"https://api.elevenlabs.io/v1/convai/conversations/{conversation_id}"
    headers = {"xi-api-key": api_key}

    try:
        import requests
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": "ElevenLabs API error", "details": resp.text}), resp.status_code
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500