# admin/transcriptions_reminder.py
from flask import (
    render_template, request, jsonify, current_app,
    redirect, url_for, flash, Response
)
from bson import ObjectId
from datetime import datetime
import os
import aiohttp
import asyncio
from pytz import utc
import requests
import pytz
import re

from .views import admin_bp, admin_required

now_utc = datetime.now(pytz.UTC)


# ===============================
# Helpers
# ===============================
def run_async(coro):
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            return future.result(timeout=30)
        return loop.run_until_complete(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)


async def fetch_audio_reminder(url, api_key):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={"xi-api-key": api_key}) as resp:
            if resp.status != 200:
                return None
            return await resp.read()


def _format_reminder_conv(conv):
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

    def safe_str(val, default=""):
        if val is None:
            return default
        if isinstance(val, str):
            return val.strip()
        if isinstance(val, (int, float)):
            return str(val).strip()
        if isinstance(val, datetime):
            return ""
        return str(val).strip() if hasattr(val, '__str__') else default

    first_name = safe_str(user.get('first_name'))
    last_name  = safe_str(user.get('last_name'))
    full_name  = " ".join(filter(None, [first_name, last_name])).strip()

    conv['name']        = full_name or "Unknown User"
    conv['email']       = safe_str(user.get('email'), '—')
    conv['designation'] = safe_str(user.get('designation'), '-')
    conv['country']     = safe_str(user.get('country'), '-')
    conv['county']      = safe_str(user.get('county'), '-')
    conv['call_status'] = safe_str(conv.get('call_status'), '—')

    # Certificates needed — stored as a list on the reminder doc
    certs = conv.get('certificates_needed', [])
    conv['certificates_needed'] = certs if isinstance(certs, list) else []

    created_at   = user.get('created_at')
    onboarded_at = user.get('onboarded_at')

    if isinstance(created_at, datetime):
        created_at = created_at.isoformat()
    if isinstance(onboarded_at, datetime):
        onboarded_at = onboarded_at.isoformat()

    conv['created_at']   = created_at
    conv['onboarded']    = user.get('onboarded', 0)
    conv['onboarded_at'] = onboarded_at

    # === DATE FORMATTING ===
    try:
        if conv.get('started_at'):
            if isinstance(conv['started_at'], str):
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

    elevenlabs_id = conv.get('elevenlabs_conversation_id', '')
    conv['elevenlabs_conversation_id'] = elevenlabs_id
    conv['has_audio'] = bool(elevenlabs_id)

    return {
        'conv_id':                    conv_id,
        'phone':                      conv.get('phone', ''),
        'name':                       conv['name'],
        'email':                      conv.get('email', '—'),
        'designation':                conv['designation'],
        'call_status':                conv['call_status'],
        'country':                    conv['country'],
        'county':                     conv['county'],
        'certificates_needed':        conv['certificates_needed'],
        'started_at':                 conv['started_at'],
        'ended_at':                   conv['ended_at'],
        'turns':                      conv['turns'],
        'elevenlabs_conversation_id': elevenlabs_id,
        'created_at':                 conv.get('created_at'),
        'onboarded':                  conv.get('onboarded', 0),
        'onboarded_at':               conv.get('onboarded_at'),
        'has_audio':                  bool(elevenlabs_id),
    }


def safe_regex_pattern_reminder(text: str) -> str:
    if not text:
        return ""
    meta = r'^$.*+?()[]{}|'
    escaped = ''
    for c in text:
        escaped += ('\\' + c) if c in meta else c
    return escaped


# ===============================
# REMINDER CALLS LIST
# ===============================
@admin_bp.route("/reminder_tr")
@admin_required
def reminder_tr():
    page       = int(request.args.get('page', 1))
    per_page   = 10
    search     = request.args.get('search', '').strip()
    date_range = request.args.get('date_range', '').strip()
    designation = request.args.get('designation', '').strip()
    county     = request.args.get('county', '').strip()

    pre_match  = {}
    post_match = None

    # ── Designation filter ──
    if designation:
        dr = safe_regex_pattern_reminder(designation)
        cond = {"user_info.designation": {"$regex": dr, "$options": "i"}}
        post_match = {"$and": [post_match, cond]} if post_match else cond

    # ── County filter ──
    if county:
        cr = safe_regex_pattern_reminder(county)
        cond = {"user_info.county": {"$regex": cr, "$options": "i"}}
        post_match = {"$and": [post_match, cond]} if post_match else cond

    # ── Date range filter ──
    date_filter = {}
    if date_range:
        try:
            parts = [p.strip() for p in date_range.split('to')]
            if len(parts) == 2:
                from_date = datetime.strptime(parts[0], '%Y-%m-%d').replace(tzinfo=utc)
                to_date   = datetime.strptime(parts[1], '%Y-%m-%d').replace(
                    hour=23, minute=59, second=59, microsecond=999999, tzinfo=utc)
                date_filter = {"started_at": {"$gte": from_date, "$lte": to_date}}
        except Exception as e:
            current_app.logger.warning(f"Invalid date_range: {date_range} | {e}")

    # ── Search filter (phone + name) ──
    if search:
        phone_pattern  = safe_regex_pattern_reminder(search)
        is_phone_like  = re.match(r'^\+?[\d\s\-\(\)]+$', search) is not None

        if is_phone_like:
            pre_match = {"phone": {"$regex": phone_pattern, "$options": "i"}}

        tokens = [t.strip() for t in search.split() if len(t.strip()) >= 2]
        name_or_conditions = []
        for token in tokens:
            tp = safe_regex_pattern_reminder(token)
            name_or_conditions.extend([
                {"user_info.first_name": {"$regex": tp, "$options": "i"}},
                {"user_info.last_name":  {"$regex": tp, "$options": "i"}},
            ])

        full_name_condition = {
            "$expr": {
                "$regexMatch": {
                    "input": {"$concat": [
                        {"$ifNull": ["$user_info.first_name", ""]}, " ",
                        {"$ifNull": ["$user_info.last_name",  ""]}
                    ]},
                    "regex":   safe_regex_pattern_reminder(search),
                    "options": "i"
                }
            }
        }
        post_match = {
            "$or": [
                {"phone": {"$regex": phone_pattern, "$options": "i"}},
                *name_or_conditions,
                full_name_condition
            ]
        }

    if date_filter:
        pre_match.update(date_filter) if pre_match else pre_match.update(date_filter)
        if not pre_match:
            pre_match = date_filter

    # ── Aggregation pipeline ──
    pipeline = []
    if pre_match:
        pipeline.append({"$match": pre_match})

    pipeline.append({"$sort": {"started_at": -1}})

    # Lookup user info from users collection via phone
    pipeline.append({
        "$lookup": {
            "from":         "users",
            "localField":   "phone",
            "foreignField": "phone",
            "as":           "user_info"
        }
    })
    pipeline.append({
        "$unwind": {"path": "$user_info", "preserveNullAndEmptyArrays": True}
    })

    if post_match:
        pipeline.append({"$match": post_match})

    pipeline.append({
        "$project": {
            "_id":                         1,
            "phone":                       1,
            "started_at":                  1,
            "ended_at":                    1,
            "call_status":                 1,
            "certificates_needed":         1,
            "elevenlabs_conversation_id":  1,
            "user_info.first_name":        1,
            "user_info.last_name":         1,
            "user_info.email":             1,
            "user_info.designation":       1,
            "user_info.country":           1,
            "user_info.county":            1,
            "user_info.created_at":        1,
            "user_info.onboarded":         1,
            "user_info.onboarded_at":      1,
        }
    })

    facet_pipeline = pipeline + [
        {
            "$facet": {
                "total":   [{"$count": "count"}],
                "results": [
                    {"$skip":  (page - 1) * per_page},
                    {"$limit": per_page}
                ]
            }
        }
    ]

    # ── Query against certificate_reminder_calls collection ──
    facet_result = list(
        current_app.db.certificate_reminder_calls.aggregate(facet_pipeline, allowDiskUse=True)
    )[0]

    total     = facet_result["total"][0]["count"] if facet_result.get("total") else 0
    raw_convs = facet_result.get("results", [])
    convs     = [_format_reminder_conv(c) for c in raw_convs]

    designations = sorted(list(filter(None, current_app.db.users.distinct("designation"))))
    counties     = sorted(list(filter(None, current_app.db.users.distinct("county"))))

    return render_template(
        'admin/transcriptions_reminder.html',
        convs=convs,
        page=page,
        total=total,
        per_page=per_page,
        search=search,
        date_range=date_range,
        designation=designation,
        county=county,
        designations=designations,
        counties=counties,
    )


# ===============================
# AUDIO ENDPOINT
# ===============================
@admin_bp.route('/reminder_tr/<conv_id>/audio')
@admin_required
def get_reminder_tr_audio(conv_id):
    conv = current_app.db.certificate_reminder_calls.find_one({"_id": ObjectId(conv_id)})
    if not conv:
        return "Conversation not found", 404

    el_id = conv.get("elevenlabs_conversation_id")
    if not el_id:
        return "Audio not generated for this call", 404

    url     = f"https://api.elevenlabs.io/v1/convai/conversations/{el_id}/audio"
    api_key = os.getenv("ELEVENLABS_API_KEY")
    if not api_key:
        return "Server configuration error", 500

    async def fetch_audio():
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers={"xi-api-key": api_key}) as resp:
                if resp.status != 200:
                    return None
                return await resp.read()

    audio_bytes = run_async(fetch_audio())
    if not audio_bytes:
        return "Audio not available (may still be processing)", 404

    return Response(
        audio_bytes,
        mimetype='audio/mpeg',
        headers={
            'Content-Disposition': f'attachment; filename="reminder_{conv_id}.mp3"',
            'Cache-Control':       'no-cache',
        }
    )


# ===============================
# ELEVENLABS SUMMARY PROXY
# ===============================
@admin_bp.route('/elevenlabs/api/reminder_tr/<conversation_id>/summary')
@admin_required
def elevenlabs_summary_proxy_reminder_tr(conversation_id):
    api_key = os.getenv("ELEVENLABS_API_KEY")
    if not api_key:
        return jsonify({"error": "API key missing"}), 500

    url     = f"https://api.elevenlabs.io/v1/convai/conversations/{conversation_id}"
    headers = {"xi-api-key": api_key}

    COUNTY_FIELDS  = {"county", "previous_work_county"}
    GENDER_FIELDS  = {"gender"}
    VISA_FIELDS    = {"visa_type"}
    UNIFORM_FIELDS = {"uniform_size"}
    TRAVEL_FIELDS  = {"commute_plan"}
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

        data     = resp.json()
        analysis = data.get("analysis") or {}
        dcr      = analysis.get("data_collection_results") or {}

        # ── Pre-load lookup maps ──
        county_map = {}
        try:
            for c in current_app.db.county.find({}, {"_id": 1, "name": 1}):
                county_map[str(c["_id"])] = c["name"]
        except Exception as e:
            current_app.logger.warning(f"Could not load counties: {e}")

        gender_map = {}
        try:
            for g in current_app.db.genders.find({}, {"_id": 1, "name": 1}):
                gender_map[str(g["_id"])] = g["name"]
        except Exception as e:
            current_app.logger.warning(f"Could not load genders: {e}")

        visa_map = {}
        try:
            for v in current_app.db.visa_types.find({}, {"_id": 1, "name": 1}):
                visa_map[str(v["_id"])] = v["name"]
        except Exception as e:
            current_app.logger.warning(f"Could not load visa types: {e}")

        uniform_map = {}
        try:
            for u in current_app.db.uniform_sizes.find({}, {"id": 1, "name": 1}):
                if "id" in u:
                    uniform_map[str(u["id"])] = u["name"]
        except Exception as e:
            current_app.logger.warning(f"Could not load uniform sizes: {e}")

        travel_map = {}
        try:
            for t in current_app.db.travel_mode.find({}, {"id": 1, "name": 1}):
                if "id" in t:
                    travel_map[str(t["id"])] = t["name"]
        except Exception as e:
            current_app.logger.warning(f"Could not load travel modes: {e}")

        rows = []
        for key, item in dcr.items():
            field_id = item.get("data_collection_id", key)
            value    = item.get("value")

            display_value = None

            if field_id in BOOLEAN_FIELDS:
                if str(value) == "1":
                    display_value = "Yes"
                elif str(value) == "0":
                    display_value = "No"
                else:
                    display_value = "—"
            elif field_id in COUNTY_FIELDS and value:
                display_value = county_map.get(str(value))
            elif field_id in GENDER_FIELDS and value:
                display_value = gender_map.get(str(value))
            elif field_id in VISA_FIELDS and value:
                display_value = visa_map.get(str(value))
            elif field_id in UNIFORM_FIELDS and value:
                display_value = uniform_map.get(str(value))
            elif field_id in TRAVEL_FIELDS and value:
                display_value = travel_map.get(str(value))

            if display_value is None:
                display_value = str(value) if value is not None else "—"

            rows.append({
                "id":      field_id,
                "label":   field_id.replace("_", " ").title(),
                "value":   display_value,
                "is_null": value is None,
            })

        call_status_item = dcr.get("call_status") or {}
        call_status      = call_status_item.get("value") or ""

        return jsonify({
            "call_summary": analysis.get("call_summary_title") or "",
            "call_status":  call_status,
            "rows":         rows,
            "total":        len(rows),
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ===============================
# RAW ELEVENLABS API PROXY
# ===============================
@admin_bp.route('/elevenlabs/api/reminder_tr/<conversation_id>')
@admin_required
def elevenlabs_reminder_api_proxy(conversation_id):
    api_key = os.getenv("ELEVENLABS_API_KEY")
    if not api_key:
        return jsonify({"error": "API key missing"}), 500

    url     = f"https://api.elevenlabs.io/v1/convai/conversations/{conversation_id}"
    headers = {"xi-api-key": api_key}

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return jsonify({"error": "ElevenLabs API error", "details": resp.text}), resp.status_code
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ===============================
# TRANSCRIPT ENDPOINT
# ===============================
@admin_bp.route('/api/reminder_tr/<conv_id>/transcript')
@admin_required
def get_reminder_transcript(conv_id):
    if not ObjectId.is_valid(conv_id):
        return jsonify({"error": "Invalid conversation ID"}), 400

    conv = current_app.db.certificate_reminder_calls.find_one(
        {"_id": ObjectId(conv_id)},
        {"turns": 1, "phone": 1, "started_at": 1, "ended_at": 1}
    )
    if not conv:
        return jsonify({"error": "Conversation not found"}), 404

    tz_utc          = pytz.UTC
    formatted_turns = []
    for turn in conv.get('turns', []):
        try:
            time_str = turn['ts'].astimezone(tz_utc).strftime('%H:%M:%S') if turn.get('ts') else '—'
        except Exception:
            time_str = '—'
        formatted_turns.append({
            'role': turn.get('role', 'unknown'),
            'text': turn.get('text', ''),
            'time': time_str,
        })

    return jsonify({
        "conv_id":    conv_id,
        "turns":      formatted_turns,
        "turn_count": len(formatted_turns),
    }), 200
