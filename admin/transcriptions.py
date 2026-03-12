# admin/transcriptions.py
from flask import render_template, request, current_app, session, redirect, url_for, Response
from functools import wraps
import pytz
from datetime import datetime
from dotenv import load_dotenv
import os
import asyncio
import aiohttp
import threading
from bson import ObjectId

load_dotenv()
from . import admin_bp


# ------------------------------------------------------------------
# ADMIN REQUIRED DECORATOR
# ------------------------------------------------------------------
def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session or not session.get('is_admin'):
            return redirect(url_for('admin.admin_login'))
        return f(*args, **kwargs)
    return decorated


# ------------------------------------------------------------------
# HELPER: Run async function safely from sync Flask view
# ------------------------------------------------------------------
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


# ------------------------------------------------------------------
# HELPER: Format conversation for template
# ------------------------------------------------------------------
def _format_conv(conv):
    tz_utc = pytz.UTC

    conv['conv_id'] = str(conv.get('_id', ''))
    conv['elevenlabs_conversation_id'] = conv.get('elevenlabs_conversation_id')

    if conv.get('started_at'):
        conv['started_at'] = conv['started_at'].astimezone(tz_utc).strftime('%Y-%m-%d %H:%M:%S')
    if conv.get('ended_at'):
        conv['ended_at'] = conv['ended_at'].astimezone(tz_utc).strftime('%Y-%m-%d %H:%M:%S')
    else:
        conv['ended_at'] = 'Ongoing'

    user = (conv.get('user_info') or [{}])[0]
    conv['name'] = f"{user.get('first_name','')} {user.get('last_name','')}".strip() or "Unknown"
    conv['designation'] = user.get('designation', '-')
    conv['country'] = user.get('country', '-')

    for turn in conv.get('turns', []):
        if turn.get('ts'):
            turn['time'] = turn['ts'].astimezone(tz_utc).strftime('%H:%M:%S')
        else:
            turn['time'] = '—'

    return conv


# ------------------------------------------------------------------
# ROUTE – GET CONVERSATION AUDIO (FIXED!)
# ------------------------------------------------------------------
@admin_bp.route('/conversation/<conv_id>/audio')
@admin_required
def get_conversation_audio(conv_id):
    try:
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

    except Exception as e:
        current_app.logger.error(f"Error fetching audio for {conv_id}: {e}", exc_info=True)
        return "Internal server error", 500


# ------------------------------------------------------------------
# ROUTE – LIST FINISHED TRANSCRIPTIONS
# ------------------------------------------------------------------
@admin_bp.route('/transcriptions')
@admin_required
def transcriptions():
    print("TRANSCRIPTIONS ROUTE HIT!")  # DEBUG

    page = int(request.args.get('page', 1))
    per_page = 10
    search = request.args.get('search', '').strip()

    # Base pipeline
    pipeline = [
        {"$sort": {"started_at": -1}},
        {"$lookup": {
            "from": "users",
            "localField": "phone",
            "foreignField": "phone",
            "as": "user_info"
        }},
        {"$unwind": {"path": "$user_info", "preserveNullAndEmptyArrays": True}},
    ]

    # Optional search by phone
    if search:
        pipeline.insert(0, {"$match": {"phone": {"$regex": search, "$options": "i"}}})

    # Total count
    total_pipeline = pipeline + [{"$count": "total"}]
    total_result = list(current_app.db.conversations.aggregate(total_pipeline))
    total = total_result[0]["total"] if total_result else 0

    # Paginated results
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

    cursor = current_app.db.conversations.aggregate(result_pipeline)
    convs = [_format_conv(c) for c in cursor]

    return render_template(
        'admin/transcriptions.html',
        convs=convs,
        page=page,
        total=total,
        per_page=per_page,
        search=search
    )