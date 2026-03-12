# fb_form_sync.py
import os
import logging
import requests
from datetime import datetime
from flask import jsonify, request

log = logging.getLogger(__name__)

# ──────────────────────────────── CONFIG ────────────────────────────────
# Put these in your .env or config
# FB_PAGE_ID = "XpressHealthcareireland"
FB_PAGE_ID = "xpressgp"

FB_ACCESS_TOKEN = os.getenv("FB_PAGE_ACCESS_TOKEN")  # MUST be long-lived Page token

# Will be set when you register routes
fb_forms_collection = None


def init_fb_form_sync(db):
    """Call this once at app startup"""
    global fb_forms_collection
    fb_forms_collection = db.fb_form_ids
    
    # Unique index on form_id so we never duplicate
    fb_forms_collection.create_index("form_id", unique=True)
    log.info("[FB FORMS SYNC] Module initialized – collection: fb_form_ids")


def fetch_and_store_all_forms():
    """Core function – fetches ALL the magic happens here"""
    if not FB_ACCESS_TOKEN:
        raise ValueError("FB_PAGE_ACCESS_TOKEN environment variable is missing")

    url = f"https://graph.facebook.com/v20.0/{FB_PAGE_ID}/leadgen_forms"
    params = {
        "access_token": FB_ACCESS_TOKEN,
        "fields": "id,name,locale,status,created_time",
        "limit": 100
    }

    inserted = 0
    updated = 0
    total = 0

    while url:
        try:
            log.info(f"[FB FORMS] Fetching → {url}")
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            for form in data.get("data", []):
                total += 1
                form_id = form["id"]

                doc = {
                    "form_id": form_id,
                    "name": form.get("name"),
                    "locale": form.get("locale"),
                    "status": form.get("status"),
                    "created_time": form.get("created_time"),
                    "page_id": FB_PAGE_ID,
                    "last_synced_at": datetime.utcnow()
                }

                result = fb_forms_collection.update_one(
                    {"form_id": form_id},
                    {"$set": doc},
                    upsert=True
                )

                if result.upserted_id:
                    inserted += 1
                    log.info(f"[FB FORMS] New form → {form_id} | {doc['name']}")
                elif result.modified_count:
                    updated += 1

            # Pagination
            url = data.get("paging", {}).get("next")

        except requests.exceptions.RequestException as e:
            log.error(f"[FB FORMS] API error: {e}")
            break
        except Exception as e:
            log.error(f"[FB FORMS] Unexpected error: {e}")
            break

    log.info(f"[FB FORMS] Sync finished – Total: {total} | New: {inserted} | Updated: {updated}")
    return {"total": total, "new": inserted, "updated": updated}


# ──────────────────────────────── ROUTE ────────────────────────────────
def register_fb_form_sync_routes(app):
    """Call this in your main app factory / startup"""

    init_fb_form_sync(app.db)  # ← important: pass the same db instance

    @app.route('/admin/sync-fb-forms', methods=['GET'])
    def sync_fb_forms_endpoint():
        # Simple auth – change to your own (e.g. Flask-Login, API key, etc.)
        api_key = request.args.get('key') or request.headers.get('X-API-Key')
        if api_key != os.getenv("ADMIN_API_KEY", "supersecret123"):
            return jsonify({"error": "Unauthorized"}), 401

        try:
            stats = fetch_and_store_all_forms()
            return jsonify({
                "status": "success",
                "message": "Facebook Lead Forms synced successfully",
                "stats": stats,
                "synced_at": datetime.utcnow().isoformat() + "Z"
            }), 200
        except Exception as e:
            log.exception("[FB FORMS] Sync failed")
            return jsonify({"error": str(e)}), 500