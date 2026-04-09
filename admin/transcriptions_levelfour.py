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
@admin_bp.route("/level_four_tr")
@admin_required
def level_four_tr():
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

    facet_result = list(current_app.db.level_four_cov.aggregate(facet_pipeline, allowDiskUse=True))[0]

    total = facet_result["total"][0]["count"] if facet_result.get("total") else 0
    raw_convs = facet_result.get("results", [])

    convs = [_format_conv(c) for c in raw_convs]

    return render_template(
        'admin/transcriptions_levelfour.html',
        convs=convs,
        page=page,
        total=total,
        per_page=per_page,
        search=search,
        date_range=date_range
    )

# ===============================
# FETCH TRANSCRIPT (MODAL)
# ===============================
@admin_bp.route('/level_four_tr/<conv_id>/audio')
@admin_required
def get_level_four_tr_audio(conv_id):
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
@admin_bp.route('/elevenlabs/api/level_four_tr/<conversation_id>')
@admin_required
def elevenlabs_level_four_tr_api_proxy(conversation_id):
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
