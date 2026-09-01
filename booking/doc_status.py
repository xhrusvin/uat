# booking/doc_status.py
from flask import request, jsonify
from bson import ObjectId
from datetime import datetime
from database import db

from . import bp
from admin.views import admin_required


@bp.route('/doc-statuses')
def doc_statuses():
    page = int(request.args.get('page', 1))
    search = request.args.get('search', '').strip()
    per_page = 10

    # Find the most recent synced_at timestamp
    latest = db.documents_new.find_one(
        {"url_status": 1},
        sort=[("synced_at", -1)],
        projection={"synced_at": 1}
    )
    latest_synced_at = latest["synced_at"] if latest else None

    # Query documents matching that latest sync and url_status=1
    query = {"url_status": 1}
    if latest_synced_at:
        query["synced_at"] = latest_synced_at
    if search:
        query["$or"] = [
            {"title":  {"$regex": search, "$options": "i"}},
            {"url":    {"$regex": search, "$options": "i"}},
        ]

    total = db.documents_new.count_documents(query)
    pages = (total + per_page - 1) // per_page if per_page else 1

    docs = list(
        db.documents_new.find(query)
        .sort("created_at", -1)
        .skip((page - 1) * per_page)
        .limit(per_page)
    )
    for d in docs:
        for key, val in d.items():
            if isinstance(val, ObjectId):
                d[key] = str(val)
            elif isinstance(val, datetime):
                d[key] = val.isoformat()

    return jsonify({
        "doc_statuses": docs,
        "page": page,
        "total": total,
        "per_page": per_page,
        "pages": pages,
        "search": search
    })