# booking/shift_users.py
from flask import Blueprint, render_template, request, jsonify, url_for
from bson import ObjectId
from datetime import datetime

from database import db
from .models.shift import Shift
from .models.shift_user import ShiftUser
from .models.client import Client

shift_model     = Shift(db.shifts)
shift_user_model = ShiftUser(db.shifts_users)
client_model    = Client(db.clients)
import requests
from flask import current_app
from flask import send_file
import io

from . import bp





@bp.route('/assignments')
def all_assignments_list():

    page = int(request.args.get('page', 1))
    per_page = 25
    q = request.args.get('q', '').strip()
    date_str = request.args.get('date', '').strip()

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    pipeline = []

    # 🔹 1. Join shifts
    pipeline.extend([
        {
            "$lookup": {
                "from": "shifts",
                "localField": "shift_id",
                "foreignField": "_id",
                "as": "shift"
            }
        },
        {"$unwind": {"path": "$shift", "preserveNullAndEmptyArrays": True}},

        # 🔹 2. Join clients
        {
            "$lookup": {
                "from": "clients",
                "let": {"client_id_str": "$shift.client_id"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$eq": [{"$toString": "$_id"}, "$$client_id_str"]
                            }
                        }
                    }
                ],
                "as": "client"
            }
        },
        {"$unwind": {"path": "$client", "preserveNullAndEmptyArrays": True}},

        # 🔹 3. Join users
        {
            "$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user"
            }
        },
        {"$unwind": {"path": "$user", "preserveNullAndEmptyArrays": True}},
        {"$match": {"user._id": {"$exists": True}}},
    ])


    if date_str:
        try:
            selected_date = datetime.strptime(date_str, "%Y-%m-%d")
            next_day = selected_date.replace(hour=23, minute=59, second=59)
            pipeline.append({
                "$match": {
                    "shift.date": {
                        "$gte": selected_date,
                        "$lte": next_day
                    }
                }
            })
        except ValueError:
            pass  # invalid date → no date filter
    else:
        # Default behavior: only future + today
        pipeline.append({
            "$match": {
                "shift.date": {"$gte": today}
            }
        })

    # 🔍 Global Search
    if q:
        pipeline.append({
            "$match": {
                "$or": [
                    {"agent_id": {"$regex": q, "$options": "i"}},
                    {"conversation_id": {"$regex": q, "$options": "i"}},
                    {"user.name": {"$regex": q, "$options": "i"}},
                    {"user.first_name": {"$regex": q, "$options": "i"}},
                    {"user.last_name": {"$regex": q, "$options": "i"}},
                    {"client.name": {"$regex": q, "$options": "i"}},
                    {"client.county": {"$regex": q, "$options": "i"}},
                    {"shift.shift_xn_id": {"$regex": q, "$options": "i"}},
                ]
            }
        })

    # 📅 Date Filter
    if date_str:
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            next_day = date_obj.replace(hour=23, minute=59, second=59)

            pipeline.append({
                "$match": {
                    "shift.date": {
                        "$gte": date_obj,
                        "$lte": next_day
                    }
                }
            })
        except ValueError:
            pass

    # Availability Filter
    avail_filter = request.args.get('avail', '').strip()

    if avail_filter == "unknown":
      pipeline.append({
        "$match": {
            "$or": [
                {"availability": {"$exists": False}},
                {"availability": None},
                {"availability": {"$nin": [0, 1, 3, 4]}}
            ]
        }
    })

    elif avail_filter in ["0", "1", "3", "4"]:
      pipeline.append({
          "$match": {"availability": int(avail_filter)}
      })

    # 🔄 Sort + Pagination
    pipeline.extend([
        {"$sort": {"assigned_at": -1}},
        #{"$sort": {"shift.date": 1}},
        {"$skip": (page - 1) * per_page},
        {"$limit": per_page},

        # 📦 Projection
        {
            "$project": {
                "assignment_id": "$_id",
                "shift_id": 1,
                "user_id": 1,
                "created_at": 1,
                "availability": 1,

                "shift_name": {"$ifNull": ["$shift.name", "—"]},
                "shift_date": "$shift.date",
                "shift_start_time": "$shift.start_time",
                "shift_end_time": "$shift.end_time",
                "shift_xn_id": "$shift.shift_xn_id",

                "client_name": {"$ifNull": ["$client.name", "—"]},
                "client_county": {"$ifNull": ["$client.county", "—"]},

                "staff_name": {
                    "$ifNull": [
                        "$user.name",
                        {
                            "$trim": {
                                "input": {
                                    "$concat": [
                                        {"$ifNull": ["$user.first_name", ""]},
                                        " ",
                                        {"$ifNull": ["$user.last_name", ""]}
                                    ]
                                }
                            }
                        }
                    ]
                },

                "staff_email": {"$ifNull": ["$user.email", "—"]},
                "staff_phone": {"$ifNull": ["$user.phone", "—"]},
                "assigned_at": 1,
            }
        }
    ])

    assignments = list(db.shifts_users.aggregate(pipeline))

    # 🕒 Format Dates
    for a in assignments:
        if a.get('created_at') and isinstance(a['created_at'], datetime):
            a['created_at_formatted'] = a['created_at'].strftime('%Y-%m-%d %H:%M')

        if a.get('shift_date') and isinstance(a['shift_date'], datetime):
            a['shift_date_formatted'] = a['shift_date'].strftime('%d %b %Y')

    # 🔢 Count (remove skip, limit, project)
    count_pipeline = pipeline[:-3]
    count_pipeline.append({"$count": "total"})

    count_result = list(db.shifts_users.aggregate(count_pipeline))
    total = count_result[0]["total"] if count_result else 0

    pages = (total + per_page - 1) // per_page if total > 0 else 1

    return render_template(
        'booking/assignments_list.html',
        assignments=assignments,
        page=page,
        pages=pages,
        total=total,
        q=q,
    )

@bp.route('/shift-users')
def shift_users_list():
    """
    Original view: list shifts with assignment summary
    """
    page = int(request.args.get('page', 1))
    per_page = 12
    q = request.args.get('q', '').strip()
    client_id_str = request.args.get('client_id')

    query = {}
    if q:
        query["$or"] = [
            {"name": {"$regex": q, "$options": "i"}},
            {"location": {"$regex": q, "$options": "i"}},
        ]
    if client_id_str:
        try:
            query["client_id"] = ObjectId(client_id_str)
        except:
            pass

    pipeline = [
        {"$match": query},
        {"$sort": {"date": -1, "created_at": -1}},
        {"$skip": (page - 1) * per_page},
        {"$limit": per_page},
        {"$lookup": {
            "from": "shifts_users",
            "localField": "_id",
            "foreignField": "shift_id",
            "as": "assignments"
        }},
        {"$addFields": {
            "assigned_count": {"$size": "$assignments"}
        }},
        {"$lookup": {
    "from": "clients",
    "let": {"client_id_obj": {"$toObjectId": "$shift.client_id"}},  # may fail if string is invalid
    "pipeline": [
        {"$match": {"$expr": {"$eq": ["$_id", "$$client_id_obj"]}}},
    ],
    "as": "client"
}},
{"$unwind": {"path": "$client", "preserveNullAndEmptyArrays": True}},
        {"$project": {
            "_id": 1,
            "name": 1,
            "date": 1,
            "start_time": 1,
            "end_time": 1,
            "location": 1,
            "client_name": {
            "$cond": [
                {"$eq": ["$client_id", None]},
                "No client",
                {"$ifNull": ["$client.name", "— (client deleted)"]}
            ]
        },
            "client_type": "$client.type",
            "assigned_count": 1,
            "sample_users": {"$slice": ["$assignments", 3]}
        }}
    ]

    shifts = list(db.shifts.aggregate(pipeline))

    for s in shifts:
        if s.get('start_time') and isinstance(s['start_time'], datetime):
            s['start_time_formatted'] = s['start_time'].strftime('%I:%M %p')
        if s.get('end_time') and isinstance(s['end_time'], datetime):
            s['end_time_formatted'] = s['end_time'].strftime('%I:%M %p')

    total = db.shifts.count_documents(query)
    pages = (total + per_page - 1) // per_page

    clients = list(db.clients.find({"is_active": True}).sort("name", 1).limit(300))

    return render_template(
        'booking/shift_users.html',
        shifts=shifts,
        page=page,
        pages=pages,
        total=total,
        per_page=per_page,
        q=q,
        client_id=client_id_str,
        clients=clients,
    )

from flask import jsonify, current_app, request
from bson import ObjectId
import os
import aiohttp
import asyncio
from functools import partial

# Helper to run async code in sync Flask view (you probably already have something like this)
def run_async(coro):
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)


@bp.route('/assignment-conversation', methods=['GET'])
def get_assignment_conversation():
    shift_id = request.args.get('shift_id')
    user_id = request.args.get('user_id')

    conv = db.shift_booking_conv.find_one(
        {"shift_id": shift_id, "user_id": user_id},
        {"_id": 0}
    )

    if not conv:
        return jsonify({"error": "No conversation found"}), 404

    # format timestamps
    if conv.get("started_at"):
        conv["started_at"] = conv["started_at"].isoformat()
    if conv.get("ended_at"):
        conv["ended_at"] = conv["ended_at"].isoformat()

    for turn in conv.get("turns", []):
        if turn.get("ts"):
            turn["ts"] = turn["ts"].isoformat()

    return jsonify({
        "conversation": conv,
        "audio_url": url_for(
            "booking.get_assignment_audio",  # ✅ correct
            shift_id=shift_id,
            user_id=user_id
        )
    })

@bp.route('/assignment-conversation/audio', methods=['GET'])
def get_assignment_audio():
    shift_id = request.args.get('shift_id')
    user_id = request.args.get('user_id')

    conv = db.shift_booking_conv.find_one(
        {"shift_id": shift_id, "user_id": user_id}
    )

    if not conv:
        return jsonify({"error": "No conversation found"}), 404

    el_conv_id = conv.get("elevenlabs_conversation_id")
    if not el_conv_id:
        return jsonify({"error": "No audio available"}), 404

    api_key = os.getenv("ELEVENLABS_API_KEY")
    url = f"https://api.elevenlabs.io/v1/convai/conversations/{el_conv_id}/audio"

    resp = requests.get(url, headers={"xi-api-key": api_key})

    if resp.status_code != 200:
        return jsonify({"error": "Failed to fetch audio"}), 400

    return send_file(
        io.BytesIO(resp.content),
        mimetype="audio/mpeg",
        as_attachment=False
    )



