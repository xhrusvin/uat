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
import pandas as pd
from io import BytesIO


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
    joined_from = request.args.get('joined_from', '').strip()
    joined_to = request.args.get('joined_to', '').strip()
    
    sort_order = request.args.get('sort', 'desc')
    sort_direction = -1 if sort_order == 'desc' else 1

    query = {"is_admin": {"$ne": True}}

    if search:
        # === SAFE ESCAPING FOR ALL REGEX FIELDS ===
        escaped_search = re.escape(search)   # This escapes +, *, ?, ., (, ), [, ], etc.

        regex_pattern = {"$regex": escaped_search, "$options": "i"}

        # Full name search using $regexMatch (also needs escaping)
        full_name_condition = {
            "$expr": {
                "$regexMatch": {
                    "input": {
                        "$concat": [
                            {"$ifNull": ["$first_name", ""]},
                            " ",
                            {"$ifNull": ["$last_name", ""]}
                        ]
                    },
                    "regex": escaped_search,      # Use the escaped version here too
                    "options": "i"
                }
            }
        }

        query["$or"] = [
            {"email": regex_pattern},
            {"phone": regex_pattern},
            {"first_name": regex_pattern},
            {"last_name": regex_pattern},
            full_name_condition
        ]

    # Date range filter (unchanged)
    if joined_from or joined_to:
        date_filter = {}
        try:
            if joined_from:
                from_dt = datetime.strptime(joined_from, '%Y-%m-%d')
                date_filter["$gte"] = from_dt.replace(hour=0, minute=0, second=0, microsecond=0)

            if joined_to:
                to_dt = datetime.strptime(joined_to, '%Y-%m-%d')
                date_filter["$lte"] = to_dt.replace(hour=23, minute=59, second=59, microsecond=999999)

            if date_filter:
                query["created_at"] = date_filter
        except ValueError:
            pass

    total = current_app.db.users.count_documents(query)

    users_list = list(
        current_app.db.users.find(query)
        .sort("created_at", sort_direction)
        .skip((page - 1) * per_page)
        .limit(per_page)
    )

    # Formatting loop (unchanged - keep your existing code here)
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

    # Fetch last conversation _id using elevenlabs_conversation_id match
    elevenlabs_ids = [
        u.get('last_elevenlabs_conversation_id')
        for u in users_list
        if u.get('last_elevenlabs_conversation_id')
    ]

    conv_by_elevenlabs_id = {}
    if elevenlabs_ids:
        for doc in current_app.db.conversations.find(
            {"elevenlabs_conversation_id": {"$in": elevenlabs_ids}},
            {"_id": 1, "elevenlabs_conversation_id": 1}
        ):
            el_key = doc.get("elevenlabs_conversation_id")
            if el_key:
                conv_by_elevenlabs_id[el_key] = str(doc["_id"])

    # Attach last_conv_id to each user
    for u in users_list:
        el_id = u.get("last_elevenlabs_conversation_id") or ""
        u["last_conv_id"] = conv_by_elevenlabs_id.get(el_id, "")

    return render_template('admin/users.html',
                           users=users_list,
                           page=page,
                           total=total,
                           per_page=per_page,
                           search=search,
                           joined_from=joined_from,
                           joined_to=joined_to,
                           sort=sort_order)

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
    conv['email'] = safe_str(user.get('email'), '—')          # ← ADD THIS LINE
    conv['designation'] = safe_str(user.get('designation'), '-')
    conv['country'] = safe_str(user.get('country'), '-')
    conv['call_status'] = safe_str(conv.get('call_status'), '—')

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
        'email': conv.get('email', '—'),
        'designation': conv['designation'],
        'call_status': conv['call_status'],
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
    date_range = request.args.get('date_range', '').strip()

    pre_match = {}
    post_match = None
    name_search_active = False

    # ====================== DATE RANGE FILTER ======================
    date_filter = {}
    if date_range:
        try:
            parts = [p.strip() for p in date_range.split('to')]
            if len(parts) == 2:
                from_date = datetime.strptime(parts[0], '%Y-%m-%d').replace(tzinfo=utc)
                to_date = datetime.strptime(parts[1], '%Y-%m-%d') \
                            .replace(hour=23, minute=59, second=59, microsecond=999999, tzinfo=utc)
                
                date_filter = {"started_at": {"$gte": from_date, "$lte": to_date}}
            elif len(parts) == 1:
                single_date = datetime.strptime(parts[0], '%Y-%m-%d').replace(tzinfo=utc)
                date_filter = {"started_at": {"$gte": single_date, "$lt": single_date + timedelta(days=1)}}
        except Exception as e:
            current_app.logger.warning(f"Invalid date_range: {date_range} | {e}")

    # ====================== SEARCH FILTER (Phone + Name) ======================
    if search:
        phone_pattern = safe_regex_pattern(search)

        # Check if search looks like a phone number (simple heuristic)
        is_phone_like = re.match(r'^\+?[\d\s\-\(\)]+$', search) is not None

        if is_phone_like:
            # Phone-only search → can be done early (before lookup)
            pre_match = {"phone": {"$regex": phone_pattern, "$options": "i"}}
        else:
            # Name search → must be done after $lookup
            name_search_active = True

        # Always prepare post_match for name search (safer approach)
        tokens = [t.strip() for t in search.split() if len(t.strip()) >= 2]
        name_or_conditions = []
        for token in tokens:
            tp = safe_regex_pattern(token)
            name_or_conditions.extend([
                {"user_info.first_name": {"$regex": tp, "$options": "i"}},
                {"user_info.last_name": {"$regex": tp, "$options": "i"}},
            ])

        full_name_condition = {
            "$expr": {
                "$regexMatch": {
                    "input": {"$concat": [
                        {"$ifNull": ["$user_info.first_name", ""]}, " ",
                        {"$ifNull": ["$user_info.last_name", ""]}
                    ]},
                    "regex": safe_regex_pattern(search),
                    "options": "i"
                }
            }
        }

        post_match = {
            "$or": [
                {"phone": {"$regex": phone_pattern, "$options": "i"}},   # always allow phone match after lookup too
                *name_or_conditions,
                full_name_condition
            ]
        }

    # ====================== MERGE DATE + PHONE FILTER ======================
    if date_filter:
        if pre_match:
            pre_match.update(date_filter)
        else:
            pre_match = date_filter

    # ── Build Aggregation Pipeline ──
    pipeline = []

    # 1. Early filtering (Date + Phone)
    if pre_match:
        pipeline.append({"$match": pre_match})

    # 2. Sort by newest calls
    pipeline.append({"$sort": {"started_at": -1}})

    # 3. Lookup user info (needed for name search)
    pipeline.append({
        "$lookup": {
            "from": "users",
            "localField": "phone",
            "foreignField": "phone",
            "as": "user_info"
        }
    })

    # 4. Unwind
    pipeline.append({
        "$unwind": {"path": "$user_info", "preserveNullAndEmptyArrays": True}
    })

    # 5. Post-lookup filtering (Name search)
    if post_match:
        pipeline.append({"$match": post_match})

    # 6. Project only needed fields
    pipeline.append({
        "$project": {
            "_id": 1,
            "phone": 1,
            "started_at": 1,
            "ended_at": 1,
            "call_status": 1,
            "elevenlabs_conversation_id": 1,
            "user_info.first_name": 1,
            "user_info.last_name": 1,
            "user_info.email": 1,
            "user_info.designation": 1,
            "user_info.country": 1,
        }
    })

    # 7. Facet for total count + pagination
    facet_pipeline = pipeline + [
        {
            "$facet": {
                "total": [{"$count": "count"}],
                "results": [
                    {"$skip": (page - 1) * per_page},
                    {"$limit": per_page}
                ]
            }
        }
    ]

    facet_result = list(current_app.db.conversations.aggregate(facet_pipeline, allowDiskUse=True))[0]

    total = facet_result["total"][0]["count"] if facet_result.get("total") else 0
    raw_convs = facet_result.get("results", [])

    convs = [_format_conv(c) for c in raw_convs]

    return render_template(
        'admin/transcriptions.html',
        convs=convs,
        page=page,
        total=total,
        per_page=per_page,
        search=search,
        date_range=date_range
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
        experience_month = ""
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
        right_to_work_ireland = ""


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
            years_experience_ireland = int(float(value))

          if field_id == "county" and value:
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

          if field_id == "right_to_work_ireland" and value:
            right_to_work_ireland = value

          if field_id == "employment_duration_months" and value:
            try:
             # Convert to float first (handles "2.0", "6.5", etc.)
             num = float(str(value).strip())
             # Convert to int (drops decimal part)
             experience_month = int(num)
             # Optional: prevent negative numbers
             if experience_month < 0:
                experience_month = 0
            except (ValueError, TypeError):
             experience_month = 0

        now_utc = datetime.now(pytz.UTC)
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

            if user_data.get("experience_month") is None:
                update_payload["experience_month"] = experience_month
    
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

            if user_data.get("permission_to_work") is None:
                update_payload["permission_to_work"] = right_to_work_ireland
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

@admin_bp.route('/elevenlabs/api/conversation/<conversation_id>/summary')
@admin_required
def elevenlabs_summary_proxy(conversation_id):
    api_key = os.getenv("ELEVENLABS_API_KEY")
    if not api_key:
        return jsonify({"error": "API key missing"}), 500

    url = f"https://api.elevenlabs.io/v1/convai/conversations/{conversation_id}"
    headers = {"xi-api-key": api_key}

    COUNTY_FIELDS  = {"location_in_ireland", "previous_work_county"}
    GENDER_FIELDS  = {"gender"}
    VISA_FIELDS    = {"visa_type"}
    UNIFORM_FIELDS = {"uniform_size"}
    BOOLEAN_FIELDS = {
       "right_to_work_ireland",
       "covid_vaccination",
       "hepatitis_b_antibodies",
       "mmr_varicella_vaccination",
     }

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": "ElevenLabs API error", "details": resp.text}), resp.status_code

        data = resp.json()
        analysis = data.get("analysis") or {}
        dcr = analysis.get("data_collection_results") or {}

        # ── Pre-load counties (_id → name) ──
        county_map = {}
        try:
            for c in current_app.db.county.find({}, {"_id": 1, "name": 1}):
                county_map[str(c["_id"])] = c["name"]
        except Exception as e:
            current_app.logger.warning(f"Could not load counties: {e}")

        # ── Pre-load genders (_id → name) ──
        gender_map = {}
        try:
            for g in current_app.db.genders.find({}, {"_id": 1, "name": 1}):
                gender_map[str(g["_id"])] = g["name"]
        except Exception as e:
            current_app.logger.warning(f"Could not load genders: {e}")

        # ── Pre-load visa types (_id → name) ──
        visa_map = {}
        try:
            for v in current_app.db.visa_types.find({}, {"_id": 1, "name": 1}):
                visa_map[str(v["_id"])] = v["name"]
        except Exception as e:
            current_app.logger.warning(f"Could not load visa types: {e}")

        # ── Pre-load uniform sizes (id integer → name) ──
        # Collection uses `id` field (int) as lookup key, not `_id`
        uniform_map = {}
        try:
            for u in current_app.db.uniform_sizes.find({}, {"id": 1, "name": 1}):
                if "id" in u:
                    uniform_map[str(u["id"])] = u["name"]   # key as string for safe comparison
        except Exception as e:
            current_app.logger.warning(f"Could not load uniform sizes: {e}")

        # ── Build structured display rows ──
        rows = []
        for key, item in dcr.items():
            field_id = item.get("data_collection_id", key)
            value    = item.get("value")

            display_value = None

            # === BOOLEAN FIELD HANDLING (YES / NO) ===
            if field_id in BOOLEAN_FIELDS:
              if str(value) == "1":
               display_value = "Yes"
              elif str(value) == "0":
               display_value = "No"
              else:
               display_value = "—"

            if field_id in COUNTY_FIELDS and value:
                display_value = county_map.get(str(value))

            elif field_id in GENDER_FIELDS and value:
                display_value = gender_map.get(str(value))

            elif field_id in VISA_FIELDS and value:
                display_value = visa_map.get(str(value))


            elif field_id in UNIFORM_FIELDS and value:
                # ElevenLabs returns the integer id (e.g. 6), match against `id` field
                display_value = uniform_map.get(str(value))

            # Fall back to raw value if no match or not a lookup field
            if display_value is None:
                display_value = str(value) if value is not None else "—"

            rows.append({
                "id":      field_id,
                "label":   field_id.replace("_", " ").title(),
                "value":   display_value,
                "is_null": value is None,
            })

            call_status_item = dcr.get("call_status") or {}
            call_status = call_status_item.get("value") or ""

        return jsonify({
            "call_summary": analysis.get("call_summary_title") or "",
            "call_status" : call_status,
            "rows":         rows,
            "total":        len(rows)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@admin_bp.route('/preview_excel_import', methods=['POST'])
@admin_required
def preview_excel_import():
    if 'excel_file' not in request.files:
        return jsonify({"success": False, "message": "No file uploaded"}), 400

    file = request.files['excel_file']
    if not file.filename.endswith('.xlsx'):
        return jsonify({"success": False, "message": "Only .xlsx files allowed"}), 400

    try:
        df = pd.read_excel(BytesIO(file.read()), dtype=str)
        df = df.fillna('')  # replace NaN with empty string

        # Convert to list of dicts with uppercase keys normalized
        records = []
        for _, row in df.iterrows():
            record = {k.strip().upper(): v.strip() if isinstance(v, str) else v for k, v in row.items()}
            records.append(record)

        return jsonify({
            "success": True,
            "data": records[:500]   # limit preview to first 500 rows
        })

    except Exception as e:
        return jsonify({"success": False, "message": f"Error reading Excel: {str(e)}"}), 400


# ────────────────────────────────────────────────
#  ACTUAL IMPORT – save to users collection
# ────────────────────────────────────────────────
@admin_bp.route('/import_excel_users', methods=['POST'])
@admin_required
def import_excel_users():
    if 'excel_file' not in request.files:
        return jsonify({"success": False, "message": "No file uploaded"}), 400

    file = request.files['excel_file']
    try:
        # Read Excel - treat everything as string to avoid type issues
        df = pd.read_excel(BytesIO(file.read()), dtype=str)
        df = df.fillna('')  # NaN → empty string

        inserted = 0
        skipped  = 0
        now = datetime.utcnow()

        # Normalize column names (upper case, strip spaces)
        columns = {col.strip().upper(): col for col in df.columns}

        for _, row in df.iterrows():
            # Get email & phone — most important deduplication keys
            email = str(row.get(columns.get('EMAIL', 'EMAIL'), '')).strip().lower()
            phone = str(row.get(columns.get('PHONE NUMBER', 'PHONE NUMBER'), '')).strip()

            if not email or '@' not in email:
                skipped += 1
                continue

            # Skip if user already exists (by email or phone)
            exists = current_app.db.users.find_one({
                "$or": [
                    {"email": email},
                    {"phone": phone}
                ]
            })
            if exists:
                skipped += 1
                continue

            # ────────────────────────────────────────────────
            # Build document — only include fields we find in Excel
            # ────────────────────────────────────────────────
            doc = {
                "email": email,
                "phone": phone,
                "created_at": now,
                "updated_at": now,
                "is_admin": False,
                # Default values like your sample
                "call_sent": 1,
                "garda_email_sent": 1,
                "follow_up_sent": 1,
                "missed_call_email_sent": 1,
                "missed_call_email_sent_at": now,   # or None — your choice
                "status": "Enabled",
            }

            # ───── Name splitting ─────
            full_name = str(row.get(columns.get('NAME', 'NAME'), '')).strip()
            if full_name:
                name_parts = full_name.split(maxsplit=1)
                doc["first_name"] = name_parts[0]
                doc["last_name"]  = name_parts[1] if len(name_parts) > 1 else ""
                doc["name"] = full_name  # store full name too (like your sample)

            # ───── Designation / User Type ─────
            if 'USER TYPE' in columns:
                designation = str(row.get(columns['USER TYPE'], '')).strip()
                if designation:
                    doc["designation"] = designation

            # ───── Status ─────
            if 'STATUS' in columns:
                status = str(row.get(columns['STATUS'], '')).strip()
                if status:
                    doc["status"] = status

            # ───── Location / County ─────
            if 'LOCATION' in columns:
                loc = str(row.get(columns['LOCATION'], '')).strip()
                if loc:
                    doc["location"] = loc

            # ───── Region / Country ─────
            if 'REGION' in columns:
                region = str(row.get(columns['REGION'], '')).strip()
                if region and region != 'IRELAND':  # sometimes region = nationality
                    doc["country"] = region
                elif region:
                    doc["region"] = region

            # ───── EIR Code ─────
            if 'EIR CODE' in columns:
                eir = str(row.get(columns['EIR CODE'], '')).strip()
                if eir and eir != 'N/A':
                    doc["eir_code"] = eir

            # ───── PPS Number ─────
            if 'PPS NUMBER' in columns:
                pps = str(row.get(columns['PPS NUMBER'], '')).strip()
                if pps and pps != 'N/A':
                    doc["pps_number"] = pps

            # ───── Completion % ─────
            if 'COMPLETION PERCENTAGE' in columns:
                comp = str(row.get(columns['COMPLETION PERCENTAGE'], '0')).strip()
                if comp.isdigit() or '.' in comp:
                    try:
                        doc["completion_percentage"] = int(float(comp))
                    except:
                        pass

            # ───── Stage ─────
            if 'STAGE' in columns:
                stage = str(row.get(columns['STAGE'], 'Register')).strip()
                if stage:
                    doc["stage"] = stage

            # ───── Recruiter ─────
            if 'RECRUITER' in columns:
                rec = str(row.get(columns['RECRUITER'], '')).strip()
                if rec:
                    doc["recruiter"] = rec

            # ───── Registration Date (optional) ─────
            if 'REGISTRATION DATE' in columns:
                reg_date_str = str(row.get(columns['REGISTRATION DATE'], '')).strip()
                if reg_date_str:
                    try:
                        # Try common formats: 25 Feb 2026, 24-02-2026, etc.
                        dt = pd.to_datetime(reg_date_str, errors='coerce', dayfirst=True)
                        if pd.notna(dt):
                            doc["registration_date"] = dt.to_pydatetime()
                    except:
                        pass

            # Insert the new user
            current_app.db.users.insert_one(doc)
            inserted += 1

        return jsonify({
            "success": True,
            "inserted": inserted,
            "skipped": skipped,
            "total_processed": inserted + skipped,
            "message": f"Imported {inserted} new users. {skipped} skipped (duplicates or invalid)."
        })

    except Exception as e:
        import traceback
        return jsonify({
            "success": False,
            "message": f"Import failed: {str(e)}",
            "trace": traceback.format_exc()[:800]
        }), 500

@admin_bp.route('/api/conversation/<conv_id>/transcript')
@admin_required
def get_conversation_transcript(conv_id):
    """Lightweight endpoint — returns only transcript turns for a single conversation."""
    if not ObjectId.is_valid(conv_id):
        return jsonify({"error": "Invalid conversation ID"}), 400

    conv = current_app.db.conversations.find_one(
        {"_id": ObjectId(conv_id)},
        {"turns": 1, "phone": 1, "started_at": 1, "ended_at": 1}  # only fetch what we need
    )

    if not conv:
        return jsonify({"error": "Conversation not found"}), 404

    tz_utc = pytz.UTC
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

    return jsonify({
        "conv_id": conv_id,
        "turns": formatted_turns,
        "turn_count": len(formatted_turns)
    }), 200