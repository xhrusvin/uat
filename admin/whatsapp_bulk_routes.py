from datetime import datetime
from bson import ObjectId
import pandas as pd

from flask import request, jsonify, render_template
from database import db
from . import admin_bp
from admin.views import admin_required
from .whatsapp_wati import (
    _normalise_phone,
    _send_session_message
)
import os
import traceback


WATI_API_ENDPOINT  = os.environ.get("WATI_API_ENDPOINT", "").rstrip("/")
WATI_ACCESS_TOKEN  = os.environ.get("WATI_ACCESS_TOKEN", "")


def _bulk_campaigns_col():
    return db.whatsapp_bulk_campaigns


def _bulk_messages_col():
    return db.whatsapp_bulk_messages


def _serialize_msg(row):
    """Convert a message document to a JSON-safe dict."""
    row["_id"] = str(row["_id"])
    if row.get("campaign_id"):
        row["campaign_id"] = str(row["campaign_id"])
    # Serialize all datetime fields to ISO strings
    for field in ("created_at", "sent_at"):
        if row.get(field) and isinstance(row[field], datetime):
            row[field] = row[field].isoformat()
    # Drop large internal fields not needed by the UI
    row.pop("wati_response", None)
    row.pop("traceback", None)
    return row


@admin_bp.route("/whatsapp_wati/bulk")
@admin_required
def whatsapp_bulk():
    return render_template("admin/whatsapp_bulk.html")


@admin_bp.route("/whatsapp_wati/bulk/preview", methods=["POST"])
@admin_required
def whatsapp_bulk_preview():
    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file uploaded"}), 400

    df = pd.read_excel(request.files["file"])

    phone_column = None

    for col in df.columns:
        if col.lower().strip() in [
            "phone", "mobile", "mobile number", "phone number", "whatsapp"
        ]:
            phone_column = col
            break

    if not phone_column:
        return jsonify({"success": False, "error": "Phone column not found"}), 400

    rows = []
    errors = []

    for idx, row in df.iterrows():
        phone = str(row.get(phone_column, "")).strip()
        if not phone:
            errors.append({"row": idx + 2, "reason": "Missing phone"})
            continue
        rows.append({"phone": _normalise_phone(phone)})

    return jsonify({
        "success": True,
        "total": len(df),
        "valid": len(rows),
        "invalid": len(errors),
        "rows": rows[:100],
        "errors": errors
    })


@admin_bp.route("/whatsapp_wati/bulk/send", methods=["POST"])
@admin_required
def whatsapp_bulk_send():
    template_name = request.form.get("template_name")

    print(os.getenv("WATI_API_ENDPOINT"))
    print(os.getenv("WATI_ACCESS_TOKEN"))

    if not template_name:
        return jsonify({"success": False, "error": "template_name required"}), 400

    if "file" not in request.files:
        return jsonify({"success": False, "error": "file required"}), 400
    
    import json

    parameter_config = json.loads(
        request.form.get(
            "parameter_config",
            "[]"
        )
    )
    
    print("=" * 80)
    print("PARAMETER CONFIG")
    print(parameter_config)
    print("=" * 80)

    df = pd.read_excel(request.files["file"])

    campaign_id = _bulk_campaigns_col().insert_one({
        "template_name": template_name,
        "parameter_config": parameter_config,
        "status": "queued",
        "total": len(df),
        "sent": 0,
        "failed": 0,
        "created_at": datetime.utcnow()
    }).inserted_id

    phone_column = None
    name_column  = None

    for col in df.columns:
        if col.lower().strip() == "phone":
            phone_column = col
        if col.lower().strip() == "name":
            name_column = col

    if not phone_column:
        return jsonify({"success": False, "error": "Phone column not found"}), 400

    docs = []

    for _, row in df.iterrows():
        
        row_data = {}

        for col in df.columns:

            value = row.get(col)

            if pd.isna(value):
                value = ""

            row_data[col] = str(value).strip()
        
        phone = str(row.get(phone_column, "")).strip()
        if not phone:
            continue

        name = ""
        if name_column:
            name = str(row.get(name_column, "")).strip()

        docs.append({
            "campaign_id":   campaign_id,
            "campaign_name": template_name,
            "phone":         _normalise_phone(phone),
            "name":          name,
            "row_data":     row_data,
            "status":        "pending",
            "created_at":    datetime.utcnow()
        })

    if docs:
        _bulk_messages_col().insert_many(docs)

    return jsonify({
        "success": True,
        "campaign_id": str(campaign_id),
        "queued": len(docs)
    })


@admin_bp.route("/whatsapp_wati/bulk/status/<campaign_id>")
@admin_required
def whatsapp_bulk_status(campaign_id):
    campaign = _bulk_campaigns_col().find_one({"_id": ObjectId(campaign_id)})

    if not campaign:
        return jsonify({"success": False, "error": "Campaign not found"}), 404

    campaign["_id"] = str(campaign["_id"])
    if isinstance(campaign.get("created_at"), datetime):
        campaign["created_at"] = campaign["created_at"].isoformat()

    return jsonify({"success": True, "campaign": campaign})


@admin_bp.route("/whatsapp_wati/bulk/process/<campaign_id>")
@admin_required
def whatsapp_bulk_process(campaign_id):
    from bson import ObjectId
    from .whatsapp_bulk_worker import process_bulk_messages

    process_bulk_messages(ObjectId(campaign_id), batch_size=100)

    return jsonify({"success": True})


@admin_bp.route("/whatsapp_wati/bulk/messages/<campaign_id>")
@admin_required
def whatsapp_bulk_messages(campaign_id):
    rows = list(
        _bulk_messages_col().find(
            {"campaign_id": ObjectId(campaign_id)},
            {"phone": 1, "name": 1, "status": 1, "error": 1}
        )
    )

    for row in rows:
        row["_id"] = str(row["_id"])

    return jsonify({"success": True, "messages": rows})


@admin_bp.route("/whatsapp_wati/bulk/conversations")
@admin_required
def whatsapp_bulk_conversations():
    conversations = list(
        _bulk_messages_col().aggregate([
            {"$sort": {"created_at": -1}},
            {
                "$group": {
                    "_id": "$phone",
                    "name":            {"$first": "$name"},
                    "last_message_at": {"$first": "$created_at"},
                    "total_messages":  {"$sum": 1},
                    "last_status":     {"$first": "$status"},
                    "last_message_text": {"$first": "$message_text"},
                }
            },
            {"$sort": {"last_message_at": -1}}
        ])
    )

    for row in conversations:
        row["phone"] = row.pop("_id")
        if isinstance(row.get("last_message_at"), datetime):
            row["last_message_at"] = row["last_message_at"].isoformat()

    return jsonify({"success": True, "conversations": conversations})


@admin_bp.route("/whatsapp_wati/bulk/conversation/<phone>")
@admin_required
def whatsapp_bulk_conversation(phone):
    rows = list(
        _bulk_messages_col()
        .find({"phone": phone})
        .sort("created_at", 1)
    )

    return jsonify({
        "success": True,
        "messages": [_serialize_msg(row) for row in rows]
    })
