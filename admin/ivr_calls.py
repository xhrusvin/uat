# admin/website_leads.py
from flask import (
    render_template, request, jsonify, current_app,
    redirect, url_for, flash, Response
)
from bson import ObjectId
from datetime import datetime, timezone, timedelta
import os
import aiohttp
import asyncio

from .views import admin_bp, admin_required


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


# ===============================
# WEBSITE LEADS LIST
# ===============================
@admin_bp.route("/ivr-calls")
@admin_required
def ivr_calls():
    page       = int(request.args.get("page", 1))
    per_page   = 25
    search     = request.args.get("search", "").strip()
    reason     = request.args.get("reason", "")
    from_date  = request.args.get("from_date", "").strip()
    to_date    = request.args.get("to_date", "").strip()

    query = {}

    if search:
        query["$or"] = [
            {"call_summary_title": {"$regex": search, "$options": "i"}},
            {"call_reason":       {"$regex": search, "$options": "i"}},
            {"phone":             {"$regex": search, "$options": "i"}},
        ]

    if reason:
        query["call_reason"] = reason

    # ── Date range filter on updated_at ───────────────────────────────
    if from_date or to_date:
       date_query = {}

    try:
        utc = timezone.utc

        if from_date or to_date:
         date_query = {}

         if from_date:
            date_query["$gte"] = f"{from_date}T00:00:00"

         if to_date:
            date_query["$lte"] = f"{to_date}T23:59:59.999999"

         if date_query:
            query["updated_at"] = date_query

    except ValueError as ve:
        flash(f"Invalid date format: {ve}. Use YYYY-MM-DD.", "danger")
    except Exception as e:
        flash(f"Date filter error: {str(e)}", "danger")
            # Or silently ignore - your choice

    # Rest of your pipeline stays exactly the same
    pipeline = [
        {"$match": query},
        {
            "$lookup": {
                "from": "ivr_calls_conv",
                "localField": "conversation_id",
                "foreignField": "conversation_id",
                "as": "conversation"
            }
        },
        {
            "$unwind": {
                "path": "$conversation",
                "preserveNullAndEmptyArrays": True
            }
        },
        {"$sort": {"updated_at": -1}},   # ← Changed to sort by updated_at (newest first)
    ]

    total = len(list(current_app.db.ivr_calls.aggregate(pipeline)))

    leads = list(
        current_app.db.ivr_calls.aggregate(
            pipeline + [
                {"$skip": (page - 1) * per_page},
                {"$limit": per_page},
            ]
        )
    )

    total_pages = (total + per_page - 1) // per_page

    return render_template(
        "admin/iver_call_list.html",
        leads=leads,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        search=search,
        reason=reason,
        from_date=from_date,    # pass back for input value
        to_date=to_date,
    )


# ===============================
# FETCH TRANSCRIPT (MODAL)
# ===============================
@admin_bp.route("/ivr-calls/conversation/<conv_id>")
@admin_required
def ivr_calls_conversation(conv_id):
    conv = current_app.db.ivr_calls_conv.find_one(
        {"conversation_id": conv_id}
    )

    if not conv:
        return jsonify({"error": "Conversation not found"}), 404

    return jsonify({
        "conversation_id": conv.get("conversation_id"),
        "call_successful": conv.get("call_successful"),
        "call_summary_title": conv.get("call_summary_title"),
        "transcript": conv.get("transcript", []),
        "stored_at": conv.get("stored_at"),
    })


# ===============================
# AUDIO ENDPOINT
# ===============================
@admin_bp.route("/ivr-calls/conversation/<conv_id>/audio")
@admin_required
def ivr_calls_audio(conv_id):
    api_key = os.getenv("ELEVENLABS_API_KEY")
    if not api_key:
        return "Missing ElevenLabs API key", 500

    url = f"https://api.elevenlabs.io/v1/convai/conversations/{conv_id}/audio"
    audio = run_async(fetch_audio(url, api_key))

    if not audio:
        return "Audio not available yet", 404

    return Response(
        audio,
        mimetype="audio/mpeg",
        headers={
            "Content-Disposition": f'attachment; filename="{conv_id}.mp3"'
        },
    )
@admin_bp.route('/ivr_elevenlabs/api/conversation/<conversation_id>')
@admin_required
def ivr_elevenlabs_api_proxy(conversation_id):
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