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
from datetime import timedelta

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
        


def update_registration_status():
    db = current_app.db

    one_min_ago = datetime.utcnow() - timedelta(minutes=1)

    pipeline = [
        {
            "$match": {
                "email": {"$ne": None},
                "email_sent": 1,
                "email_sent_at": {"$lte": one_min_ago},
                "registration_completed": {"$ne": 1}
            }
        },
        {
            "$lookup": {
                "from": "users",
                "let": {"lead_email": {"$toLower": "$email"}},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": [
                                    {"$toLower": "$email"},
                                    "$$lead_email"
                                ]
                            }
                        }
                    }
                ],
                "as": "matched_user"
            }
        },
        {
            "$match": {
                "matched_user.0": {"$exists": True}
            }
        },
        {
            "$set": {
                "registration_completed": 1,
                "registration_completed_at": datetime.utcnow()
            }
        },
        {
            "$project": {
                "matched_user": 0
            }
        },
        {
            "$merge": {
                "into": "website_leads",
                "on": "_id",
                "whenMatched": "merge",
                "whenNotMatched": "discard"
            }
        }
    ]

    db.website_leads.aggregate(pipeline)


# ===============================
# WEBSITE LEADS LIST
# ===============================
@admin_bp.route("/website-leads")
@admin_required
def website_leads_list():

    update_registration_status()

    page = int(request.args.get("page", 1))
    per_page = 25
    search = request.args.get("search", "").strip()
    status = request.args.get("status", "")

    query = {}

    if search:
        query["$or"] = [
            {"name": {"$regex": search, "$options": "i"}},
            {"email": {"$regex": search, "$options": "i"}},
            {"phone": {"$regex": search, "$options": "i"}},
        ]

    if status:
        query["call_successful"] = status

    pipeline = [
        {"$match": query},
        {
            "$lookup": {
                "from": "website_leads_conv",
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
        {
            "$addFields": {
                "email_sent_label": {
                    "$cond": [
                        {"$eq": ["$email_sent", 1]},
                        "Yes",
                        "Pending"
                    ]
                },
                "link_clicked_label": {
                    "$cond": [
                        {"$eq": ["$link_clicked", 1]},
                        "Yes",
                        "No"
                    ]
                },
                "registration_completed_label": {
                    "$cond": [
                        {"$eq": ["$registration_completed", 1]},
                        "Yes",
                        "No"
                    ]
                }
            }
        },
        {"$sort": {"updated_at": -1}},
        ]


    total = len(list(current_app.db.website_leads.aggregate(pipeline)))

    leads = list(
        current_app.db.website_leads.aggregate(
            pipeline + [
                {"$skip": (page - 1) * per_page},
                {"$limit": per_page},
            ]
        )
    )

    total_pages = (total + per_page - 1) // per_page

    return render_template(
        "admin/website_leads_list.html",
        leads=leads,
        total=total,
        page=page,
        per_page=per_page,
        total_pages=total_pages,
        search=search,
        status=status,
    )


# ===============================
# FETCH TRANSCRIPT (MODAL)
# ===============================
@admin_bp.route("/website-leads/conversation/<conv_id>")
@admin_required
def website_lead_conversation(conv_id):
    conv = current_app.db.website_leads_conv.find_one(
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
@admin_bp.route("/website-leads/conversation/<conv_id>/audio")
@admin_required
def website_lead_audio(conv_id):
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
