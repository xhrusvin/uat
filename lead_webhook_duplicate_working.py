# lead_webhook_duplicate.py

import logging
import requests
from flask import request, jsonify
from datetime import datetime
from bson.objectid import ObjectId

# Logging setup
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --------------------------------------------------------------
# CONFIGURATION - USE ENV VARS IN PRODUCTION!
# --------------------------------------------------------------
import os

PAGE_ACCESS_TOKEN = os.getenv("FB_PAGE_ACCESS_TOKEN")
GRAPH_API_VERSION = "v22.0"
TARGET_CAMPAIGN_ID = "120228734485040662"  # Only this campaign (Nurse)

# Helper functions (copied from fb_lead_fetcher for consistency)
def get_field(values_list):
    return values_list[0].strip() if values_list else None

def get_country_id(raw_phone):
    if not raw_phone:
        return 250

    cleaned = "".join(c for c in raw_phone if c.isdigit() or c == "+")
    if cleaned.startswith("+"):
        cleaned = cleaned[1:]

    if cleaned.startswith("353"):      # Ireland
        return 1
    if cleaned.startswith("44"):       # UK
        return 2
    if cleaned.startswith("61"):       # Australia
        return 3
    return 250

def convert_mongo_ids(doc):
    """Recursively convert ObjectId → str in any MongoDB document."""
    if isinstance(doc, dict):
        return {k: convert_mongo_ids(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [convert_mongo_ids(item) for item in doc]
    if isinstance(doc, ObjectId):
        return str(doc)
    return doc


def register_lead_webhook_duplicate_routes(app):
    """
    Duplicate webhook that:
    - Receives real-time lead notifications
    - Fetches full lead data including campaign_id and field_data
    - Saves into db.leads (same collection as fetcher)
    - Only saves if campaign_id == 120235638775730662 (Nurse campaign)
    - Applies same cleaning and structure as fb_lead_fetcher.py
    - Still logs raw webhook to lead_webhooks for debugging
    """

    leads_collection = app.db.leads          # Main leads collection
    webhook_collection = app.db.lead_webhooks  # For raw debugging

    # --------------------------------------------------------------
    # 1. VERIFY WEBHOOK
    # --------------------------------------------------------------
    @app.route('/lead/webhook_dub', methods=['GET'])
    def verify_webhook_duplicate():
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")

        VERIFY_TOKEN = os.getenv("FB_WEBHOOK_VERIFY_TOKEN", "1234")

        if mode == "subscribe" and token == VERIFY_TOKEN:
            log.info("[WEBHOOK DUB] Verification successful ✔")
            return challenge, 200

        log.warning("[WEBHOOK DUB] Verification failed ❌")
        return jsonify({"error": "Invalid verification token"}), 403

    # --------------------------------------------------------------
    # 2. RECEIVE WEBHOOK & PROCESS LEADS
    # --------------------------------------------------------------
    @app.route('/lead/webhook_dub', methods=['POST'])
    def receive_webhook_duplicate():
        try:
            payload = request.get_json(force=True)
        except Exception:
            return jsonify({"status": "error", "message": "Invalid JSON"}), 400

        log.info(f"[WEBHOOK DUB] Received payload: {payload}")

        processed_leads = []
        saved_count = 0
        skipped_count = 0
        wrong_campaign_count = 0

        for entry in payload.get("entry", []):
            for change in entry.get("changes", []):
                if change.get("field") != "leadgen":
                    continue

                value = change.get("value", {})
                leadgen_id = value.get("leadgen_id")
                if not leadgen_id:
                    continue

                # Fetch full lead with campaign_id and field_data
                url = f"https://graph.facebook.com/{GRAPH_API_VERSION}/{leadgen_id}"
                params = {
                    "access_token": PAGE_ACCESS_TOKEN,
                    "fields": "campaign_id,field_data,created_time"
                }

                try:
                    response = requests.get(url, params=params, timeout=15)
                    response.raise_for_status()
                    lead_data = response.json()
                except requests.RequestException as e:
                    error_text = getattr(e.response, "text", str(e))
                    log.error(f"[WEBHOOK DUB] Failed to fetch lead {leadgen_id}: {error_text}")
                    lead_data = {"error": str(e)}

                # Extract campaign_id (it's directly in the lead object)
                campaign_id = lead_data.get("campaign_id")
                if str(campaign_id) != TARGET_CAMPAIGN_ID:
                    wrong_campaign_count += 1
                    processed_leads.append({
                        "leadgen_id": leadgen_id,
                        "reason": "wrong_campaign",
                        "campaign_id": campaign_id
                    })
                    continue  # Skip saving

                # Parse field data
                field_data = {item["name"].lower(): item["values"] for item in lead_data.get("field_data", [])}

                full_name = get_field(field_data.get("full_name")) or get_field(field_data.get("name"))
                email = get_field(field_data.get("email"))
                raw_phone = get_field(field_data.get("phone_number"))
                province = get_field(field_data.get("province")) or get_field(field_data.get("location")) or "Unknown"

                if not full_name:
                    skipped_count += 1
                    processed_leads.append({
                        "leadgen_id": leadgen_id,
                        "reason": "missing_name"
                    })
                    continue

                phone = raw_phone.replace("+", "") if raw_phone else None
                country_id = get_country_id(raw_phone)

                doc = {
                    "fb_lead_id": leadgen_id,
                    "name": full_name,
                    "email_id": email.lower() if email else None,
                    "phone_number": phone,
                    "user_type": "Nurse",
                    "location": province.title(),
                    "country": country_id,
                    "call_initiated": 0,
                    "uploaded_at": datetime.utcnow(),
                    "source_file": "fb_leads_webhook",
                    "call_initiated_at": None,
                    "campaign_id": campaign_id
                }

                try:
                    result = leads_collection.update_one(
                        {"fb_lead_id": leadgen_id},
                        {"$setOnInsert": doc},
                        upsert=True
                    )
                    if result.upserted_id:
                        saved_count += 1
                        log.info(f"[WEBHOOK DUB] SAVED NURSE → {full_name} | {email or '-'} | {phone or '-'} | Country: {country_id}")
                    else:
                        skipped_count += 1
                except Exception as e:
                    log.error(f"[WEBHOOK DUB] DB Error saving {leadgen_id}: {e}")
                    skipped_count += 1

                processed_leads.append({
                    "leadgen_id": leadgen_id,
                    "saved": result.upserted_id is not None,
                    "name": full_name
                })

        # Save raw webhook payload for debugging (as before)
        record = {
            "raw_payload": payload,
            "processed_leads": processed_leads,
            "received_at": datetime.utcnow(),
            "source_ip": request.remote_addr,
            "stats": {
                "saved": saved_count,
                "skipped_duplicate_or_error": skipped_count,
                "wrong_campaign": wrong_campaign_count,
                "total_received": len(processed_leads)
            }
        }
        webhook_collection.insert_one(record)

        return jsonify({
            "status": "success",
            "message": "Real-time leads processed",
            "saved_new_leads": saved_count,
            "skipped_or_duplicate": skipped_count,
            "filtered_wrong_campaign": wrong_campaign_count,
            "total_processed": len(processed_leads)
        }), 200

    # --------------------------------------------------------------
    # 3. DEBUG ENDPOINT
    # --------------------------------------------------------------
    @app.route('/lead/webhook/debug_dub', methods=['GET'])
    def debug_webhook_duplicate():
        last = webhook_collection.find_one(sort=[("_id", -1)])
        last_clean = convert_mongo_ids(last) if last else None

        return jsonify({
            "status": "webhook_duplicate_active",
            "last_entry": last_clean,
            "server_time_utc": datetime.utcnow().isoformat() + "Z"
        }), 200