# fb_lead_fetcher.py
import os
import logging
import requests
from datetime import datetime
from flask import jsonify, request

log = logging.getLogger(__name__)

# Config
FB_ACCESS_TOKEN = os.getenv("FB_PAGE_ACCESS_TOKEN")
GRAPH_VERSION = "v20.0"

# Allowed campaigns and their corresponding user types
ALLOWED_CAMPAIGNS = {                  # Original Theatre Nurse campaign
    "120228337783440662": "Nurse",                   # Additional Nurse campaign
    "120228353185220662": "Health Care Assistant",  # HCA campaign
}

# Collections
forms_collection = None
leads_collection = None


def init_fb_lead_fetcher(db):
    global forms_collection, leads_collection
    forms_collection = db.fb_form_ids
    leads_collection = db.leads

    index_name = "fb_lead_id_unique_when_present"
    existing_indexes = [idx["name"] for idx in leads_collection.list_indexes()]

    if index_name not in existing_indexes:
        try:
            leads_collection.create_index(
                "fb_lead_id",
                name=index_name,
                unique=True,
                partialFilterExpression={
                    "fb_lead_id": {"$exists": True, "$ne": None, "$type": "string"}
                }
            )
            log.info("[FB LEAD FETCHER] Unique partial index created on fb_lead_id")
        except Exception as e:
            if "already exists" not in str(e).lower():
                log.warning(f"[FB LEAD FETCHER] Could not create index: {e}")
    else:
        log.info("[FB LEAD FETCHER] Index already exists")

    log.info("[FB LEAD FETCHER] Ready – processing leads from specified campaigns only")


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


def fetch_leads_for_form(form_id, form_name):
    url = f"https://graph.facebook.com/{GRAPH_VERSION}/{form_id}/leads"
    params = {"access_token": FB_ACCESS_TOKEN, "limit": 100, "fields": "campaign_id,field_data"}

    inserted = skipped = filtered_out = 0

    while url:
        try:
            resp = requests.get(url, params=params, timeout=40)
            resp.raise_for_status()
            data = resp.json()

            for lead in data.get("data", []):
                lead_id = lead["id"]
                campaign_id = str(lead.get("campaign_id") or lead.get("ad_id"))

                # CRITICAL FILTER: Only process allowed campaigns
                if campaign_id not in ALLOWED_CAMPAIGNS:
                    filtered_out += 1
                    continue

                user_type = ALLOWED_CAMPAIGNS[campaign_id]

                field_data = {item["name"].lower(): item["values"] for item in lead.get("field_data", [])}

                full_name = get_field(field_data.get("full_name")) or get_field(field_data.get("name"))
                email = get_field(field_data.get("email"))
                raw_phone = get_field(field_data.get("phone_number"))
                province = get_field(field_data.get("province")) or get_field(field_data.get("location")) or "Unknown"

                if not full_name:
                    skipped += 1
                    continue

                phone = raw_phone.replace("+", "") if raw_phone else None
                country_id = get_country_id(raw_phone)

                doc = {
                    "fb_lead_id": lead_id,
                    "name": full_name,
                    "email_id": email.lower() if email else None,
                    "phone_number": phone,
                    "user_type": user_type,
                    "location": province.title(),
                    "country": country_id,
                    "call_initiated": 0,
                    "uploaded_at": datetime.utcnow(),
                    "source_file": "fb_leads",
                    "call_initiated_at": None,
                    "campaign_id": campaign_id
                }

                try:
                    result = leads_collection.update_one(
                        {"fb_lead_id": lead_id},
                        {"$setOnInsert": doc},
                        upsert=True
                    )
                    if result.upserted_id:
                        inserted += 1
                        log.info(f"SAVED {user_type.upper()} → {full_name} | {email or '-'} | {phone or '-'} | Country ID: {country_id} | Campaign: {campaign_id}")
                    else:
                        skipped += 1
                except Exception as e:
                    log.error(f"DB Error {lead_id}: {e}")
                    skipped += 1

            url = data.get("paging", {}).get("next")
            params = {} if url else params

        except Exception as e:
            log.error(f"Error fetching form {form_id}: {e}")
            break

    log.info(f"Form {form_name}: Inserted={inserted}, Skipped/Duplicate={skipped}, Filtered Out (wrong campaign)={filtered_out}")
    return inserted, skipped + filtered_out


def fetch_all_fb_leads():
    total_inserted = total_skipped = forms_count = 0

    for form in forms_collection.find({"status": "ACTIVE"}):
        form_id = form["form_id"]
        form_name = form.get("name", "Unknown")
        log.info(f"Fetching leads → {form_name} ({form_id})")

        ins, sk = fetch_leads_for_form(form_id, form_name)
        total_inserted += ins
        total_skipped += sk
        forms_count += 1

        import time
        time.sleep(1)

    return {
        "forms_processed": forms_count,
        "leads_inserted": total_inserted,
        "leads_skipped_or_filtered": total_skipped
    }


def register_fb_lead_routes(app):
    init_fb_lead_fetcher(app.db)

    @app.route("/admin/fetch-fb-leads", methods=["GET"])
    def fetch_fb_leads_endpoint():
        key = request.args.get("key") or request.headers.get("X-API-Key")
        if key != os.getenv("ADMIN_API_KEY"):
            return jsonify({"error": "Unauthorized"}), 401

        try:
            stats = fetch_all_fb_leads()
            return jsonify({
                "status": "success",
                "message": "Facebook leads synced (Nurse & Health Care Assistant campaigns only)",
                "stats": stats,
                "run_at": datetime.utcnow().isoformat() + "Z"
            }), 200
        except Exception as e:
            log.exception("Fetch FB leads failed")
            return jsonify({"error": str(e)}), 500