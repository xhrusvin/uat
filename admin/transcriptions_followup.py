# admin/website_leads.py
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
import pytz

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


async def fetch_audio(url, api_key):
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers={"xi-api-key": api_key}) as resp:
            if resp.status != 200:
                return None
            return await resp.read()

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


# ===============================
# WEBSITE LEADS LIST
# ===============================
@admin_bp.route("/followup_tr")
@admin_required
def followup_tr():
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
    total_result = list(current_app.db.follow_up_conv.aggregate(count_pipeline, allowDiskUse=True))
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

    raw_convs = list(current_app.db.follow_up_conv.aggregate(result_pipeline, allowDiskUse=True))
    convs = [_format_conv(c) for c in raw_convs]

    return render_template(
        'admin/transcriptions_follwoup.html',
        convs=convs,
        page=page,
        total=total,
        per_page=per_page,
        search=search
    )

# ===============================
# FETCH TRANSCRIPT (MODAL)
# ===============================
@admin_bp.route('/followup_tr/<conv_id>/audio')
@admin_required
def get_followup_tr_audio(conv_id):
    #try:
        conv = current_app.db.follow_up_conv.find_one({"_id": ObjectId(conv_id)})
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

# ===============================
# AUDIO ENDPOINT
# ===============================
@admin_bp.route('/elevenlabs/api/followup_tr/<conversation_id>')
@admin_required
def elevenlabs_follow_up_api_proxy(conversation_id):
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
