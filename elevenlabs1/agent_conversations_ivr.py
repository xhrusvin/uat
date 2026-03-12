# elevenlabs/agent_conversations.py
# FULLY STANDALONE – Root-level module
# PURPOSE:
# - website_leads: store ONLY real lead data from data_collection_results
# - website_leads_conv: store ONLY minimal transcript for UI display
# - DEFENSIVE against misconfigured ElevenLabs schemas

from flask import Blueprint, request, jsonify
import requests
import os
from dotenv import load_dotenv
from datetime import datetime
import pytz
from pymongo import MongoClient

load_dotenv()

# ==================== BLUEPRINT ====================
bp = Blueprint("elevenlabs_bp_ivr", __name__)

# ==================== CONFIG ====================
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY")
TRIGGER_KEY = os.getenv("ADMIN_API_KEY", "1234")

ELEVENLABS_CONV_API = "https://api.elevenlabs.io/v1/convai/conversations"

HEADERS = {
    "xi-api-key": ELEVENLABS_API_KEY,
    "Content-Type": "application/json"
}

# ==================== MONGO DB ====================
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

if not MONGO_URI or not DB_NAME:
    raise ValueError("MONGO_URI and DB_NAME must be set in .env")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

leads_collection = db["ivr_calls"]          # CLEAN CRM DATA
conversations_collection = db["ivr_calls_conv"]  # UI TRANSCRIPTS ONLY

# ==================== CONSTANTS ====================
CALL_STATUS_VALUES = {
    "voice call detected",
    "call success",
    "user busy",
    "no required experience"
}

# ==================== HELPERS ====================
def extract_minimal_transcript(transcript):
    """Extract only fields required to display transcript in UI"""
    clean = []
    for turn in transcript:
        role = turn.get("role")
        message = turn.get("message") or turn.get("original_message")
        time_secs = turn.get("time_in_call_secs")
        interrupted = turn.get("interrupted", False)

        if role and message:
            clean.append({
                "role": role,
                "message": message.strip(),
                "time_in_call_secs": time_secs,
                "interrupted": interrupted
            })
    return clean


def extract_data_collection_map(data_collection_results):
    """
    Convert data_collection_results into:
    { data_collection_id: value }
    """
    extracted = {}

    if not isinstance(data_collection_results, dict):
        return extracted

    for item in data_collection_results.values():
        if not isinstance(item, dict):
            continue

        dc_id = item.get("data_collection_id")
        value = item.get("value")

        if dc_id and value is not None:
            extracted[dc_id] = value

    return extracted


def is_call_status_value(value):
    """Detect call-status text mistakenly placed in lead fields"""
    if not isinstance(value, str):
        return False
    return value.strip().lower() in CALL_STATUS_VALUES


# ==================== ROUTE ====================
@bp.route("/api/elevenlabs/agent/conversations_ivr", methods=["GET", "POST"])
def sync_agent_conversations_ivr():
    key = request.args.get("key")
    agent_id = request.args.get("agent_id")
    limit = int(request.args.get("limit", 20))

    page = 1  # Always latest only

    if key != TRIGGER_KEY:
        return jsonify({"success": False, "message": "Invalid key"}), 401

    if not ELEVENLABS_API_KEY:
        return jsonify({"success": False, "message": "Missing ELEVENLABS_API_KEY"}), 500

    if not agent_id:
        return jsonify({"success": False, "message": "agent_id is required"}), 400

    try:
        # 1️⃣ Fetch conversation list
        resp = requests.get(
            ELEVENLABS_CONV_API,
            headers=HEADERS,
            params={"agent_id": agent_id, "page": page, "limit": limit},
            timeout=60
        )
        resp.raise_for_status()
        data = resp.json()


        conversations = data.get("conversations", [])
        processed = inserted = updated = skipped = 0
        now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)

        for conv in conversations:
            conversation_id = conv.get("conversation_id")
            if not conversation_id:
                skipped += 1
                continue

            try:
                # 2️⃣ Fetch full conversation
                detail_resp = requests.get(
                    f"{ELEVENLABS_CONV_API}/{conversation_id}",
                    headers=HEADERS,
                    timeout=60
                )
                detail_resp.raise_for_status()
                full_details = detail_resp.json()

                analysis = full_details.get("analysis", {})
                transcript = full_details.get("transcript", [])
                dc_results = analysis.get("data_collection_results", {})

                # ==================== DATA COLLECTION ====================
                dc_map = extract_data_collection_map(dc_results)

                name_val = dc_map.get("name")
                email_val = dc_map.get("email")
                phone_val = dc_map.get("phone_number")
                position_val = dc_map.get("position")
                call_reason_val = dc_map.get("call_reason")

                

                start_time_unix_secs = conv.get("start_time_unix_secs")
                call_duration_secs = conv.get("call_duration_secs")

                stored_at = (
                  datetime.fromtimestamp(start_time_unix_secs, tz=pytz.UTC).isoformat()
                  if start_time_unix_secs
                  else now_utc.isoformat()
                ) 

                ended_at = (
                  datetime.fromtimestamp(start_time_unix_secs + call_duration_secs, tz=pytz.UTC).isoformat()
                  if call_duration_secs
                  else now_utc.isoformat()
                )   

                # ==================== website_leads ====================
                lead_doc = {
                    "conversation_id": conversation_id,
                    "agent_id": agent_id,

                    # Lead details (ONLY if real)
                    "name": name_val,
                    "email": email_val.lower() if isinstance(email_val, str) else None,
                    "phone": phone_val,
                    "position": position_val,
                    "call_reason": call_reason_val,

                    # Call outcome
                    "call_successful": analysis.get("call_successful"),
                    "call_summary_title": analysis.get("call_summary_title"),
                    "call_status": 1,

                    "source": "elevenlabs_convai",
                    "updated_at": stored_at,
                    "started_at":stored_at,
                    "ended_at": ended_at,

                }

                result = leads_collection.update_one(
                    {"conversation_id": conversation_id},
                    {
                        "$set": lead_doc,
                        "$setOnInsert": {"created_at": now_utc.isoformat()}
                    },
                    upsert=True
                )

                if result.upserted_id:
                    inserted += 1
                else:
                    updated += 1

                # ==================== website_leads_conv ====================
                conv_doc = {
                    "conversation_id": conversation_id,
                    "agent_id": agent_id,
                    "call_successful": analysis.get("call_successful"),
                    "call_summary_title": analysis.get("call_summary_title"),
                    "status": full_details.get("status"),
                    "transcript": extract_minimal_transcript(transcript),
                    "stored_at": now_utc.isoformat()
                }

                conversations_collection.update_one(
                    {"conversation_id": conversation_id},
                    {"$set": conv_doc},
                    upsert=True
                )

                processed += 1

            except Exception as e:
                print(f"Failed processing {conversation_id}: {e}")
                skipped += 1

        return jsonify({
            "success": True,
            "agent_id": agent_id,
            "processed": processed,
            "inserted_leads": inserted,
            "updated_leads": updated,
            "skipped": skipped,
            "timestamp": now_utc.isoformat()
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "message": "Failed to sync ElevenLabs conversations",
            "error": str(e)
        }), 500
