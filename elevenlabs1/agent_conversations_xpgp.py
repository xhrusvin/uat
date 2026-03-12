# elevenlabs/agent_conversations.py
# UPDATED: Uses SAME email templates & functions as xpressgp/customer_booking_form.py
# Sends identical customer + admin emails with same details (falls back to None/"Not provided" if missing)

from flask import Blueprint, request, jsonify, render_template
import requests
import os
import random
from dotenv import load_dotenv
from datetime import datetime
import pytz
from pymongo import MongoClient

# Import the shared email functions from the booking form module
from xpressgp.customer_booking_form import (
    send_customer_confirmation,
    send_admin_notification,
    send_email  # optional, only if you want to reuse directly
)

load_dotenv()

# ==================== BLUEPRINT ====================
bp = Blueprint("elevenlabs_bp_xpgp", __name__)

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
CC_EMAIL = os.getenv("XP_GP_CC_EMAIL")
BCC_EMAIL = os.getenv("XP_GP_BCC_EMAIL")

if not MONGO_URI or not DB_NAME:
    raise ValueError("MONGO_URI and DB_NAME must be set in .env")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

leads_collection = db["xpress_gp"]                  # CLEAN CRM DATA
conversations_collection = db["xpress_gp_conv"]     # UI TRANSCRIPTS ONLY

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
def generate_reference_id():
    """Generate a random 5-digit numeric reference ID"""
    return ''.join(random.choices('0123456789', k=5))    

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


# ==================== ROUTE ====================
@bp.route("/api/elevenlabs/agent/conversations_xpgp", methods=["GET", "POST"])
def sync_agent_conversations_xpgp():
    key = request.args.get("key") 
    agent_id = request.args.get("agent_id")
    limit = int(request.args.get("limit", 1))

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

                reference_id = generate_reference_id()

                analysis = full_details.get("analysis", {})
                transcript = full_details.get("transcript", [])
                dc_results = analysis.get("data_collection_results", {})

                # ==================== DATA COLLECTION ====================
                dc_map = extract_data_collection_map(dc_results)

                name_val = dc_map.get("full_name") or None
                email_val = dc_map.get("email")
                phone_val = dc_map.get("phone_number") or None
                preferred_date_val = dc_map.get("preferred_date") or None
                preferred_time_val = dc_map.get("preferred_time") or None
                date_of_birth_val = dc_map.get("date_of_birth") or None
                pps_number_val = dc_map.get("pps_number") or None

                # Format appointment info for email (same style as form)
                appointment_info = None
                if preferred_date_val and preferred_time_val:
                    try:
                        time_obj = datetime.strptime(preferred_time_val, '%H:%M')
                        formatted_time = time_obj.strftime('%I:%M %p').lstrip('0')
                        appointment_info = f"{preferred_date_val} at {formatted_time}"
                    except:
                        appointment_info = f"{preferred_date_val} at {preferred_time_val}"
                elif preferred_date_val:
                    appointment_info = f"{preferred_date_val}"

                start_time_unix_secs = conv.get("start_time_unix_secs")
                call_duration_secs = conv.get("call_duration_secs")

                stored_at = (
                    datetime.fromtimestamp(start_time_unix_secs, tz=pytz.UTC).isoformat()
                    if start_time_unix_secs
                    else now_utc.isoformat()
                )

                ended_at = (
                    datetime.fromtimestamp(start_time_unix_secs + call_duration_secs, tz=pytz.UTC).isoformat()
                    if start_time_unix_secs and call_duration_secs
                    else now_utc.isoformat()
                )

                # ==================== LEAD DOCUMENT ====================
                lead_doc = {
                    "conversation_id": conversation_id,
                    "agent_id": agent_id,

                    "name": name_val,
                    "email": email_val.lower() if isinstance(email_val, str) else None,
                    "phone": phone_val,
                    "preferred_date": preferred_date_val,
                    "preferred_time": preferred_time_val,
                    "date_of_birth": date_of_birth_val,
                    "pps_number": pps_number_val,

                    "call_successful": analysis.get("call_successful"),
                    "call_summary_title": analysis.get("call_summary_title"),
                    "call_status": 1,

                    "source": "elevenlabs_convai",
                    "updated_at": stored_at,
                    "started_at": stored_at,
                    "ended_at": ended_at,
                }

                # ==================== UPSERT LEAD ====================
                result = leads_collection.update_one(
                    {"conversation_id": conversation_id},
                    {
                        "$set": lead_doc,
                        "$setOnInsert": {"created_at": now_utc.isoformat()}
                    },
                    upsert=True
                )

                is_new_lead = result.upserted_id is not None

                if is_new_lead:
                    inserted += 1

                    # =============== SEND EMAILS USING SAME FUNCTIONS & TEMPLATES ===============
                    # We reuse the exact same send_customer_confirmation and send_admin_notification
                    # from the booking form – they use the same templates:
                    #   email/xpress_gp_customer_confirmation.html
                    #   email/xpress_gp_admin_notification.html

                    mongo_id = str(result.upserted_id)  # ObjectId as string for email template

                    # Customer email (only if email exists)
                    customer_success = False
                    if email_val:
                        customer_success = send_customer_confirmation(
                            full_name=name_val or "Valued Customer",
                            email=email_val,
                            mongo_id=mongo_id,
                            registration_id_input="Voice Call",  # to show source
                            health_concern=analysis.get("call_summary_title") or "General consultation requested via voice agent",
                            appointment_info=appointment_info or "We will contact you shortly",
                            reference_id=reference_id,
                        )
                    else:
                        print(f"New lead {conversation_id} has no email – customer email skipped")

                    if email_val:
                        admin_success = send_admin_notification(
                        full_name=name_val or "Not provided",
                        dob=date_of_birth_val or "Not provided (voice call)",  # no DOB from voice agent
                        phone=phone_val or "Not provided",
                        email=email_val or "Not provided",
                        pps_number=pps_number_val or "Not provided",
                        followup="Not provided",
                        health_concern=analysis.get("call_summary_title") or "General consultation via voice agent",
                        registration_id_input=f"Voice Call ({conversation_id})",
                        appointment_info=appointment_info or "No specific time requested",
                        mongo_id=mongo_id,
                        reference_id=reference_id,
                        bcc=BCC_EMAIL
                    )
                    else:
                        print(f"New lead {conversation_id} has no email – customer email skipped")

                    if email_val and not customer_success:
                        print(f"Customer email failed for {conversation_id}")
                    if not admin_success:
                        print(f"Admin email failed for {conversation_id}")

                else:
                    updated += 1

                # ==================== CONVERSATION TRANSCRIPT ====================
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