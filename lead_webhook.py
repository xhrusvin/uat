# lead_webhook.py
import logging
from flask import request, jsonify
from datetime import datetime
from bson.objectid import ObjectId

# Logging setup
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def convert_mongo_ids(doc):
    """Recursively convert ObjectId → str in any MongoDB document."""
    if isinstance(doc, dict):
        return {k: convert_mongo_ids(v) for k, v in doc.items()}
    if isinstance(doc, list):
        return [convert_mongo_ids(item) for item in doc]
    if isinstance(doc, ObjectId):
        return str(doc)
    return doc


def register_lead_webhook_routes(app):
    """
    Registers webhook routes for receiving leads
    and storing them into a new MongoDB collection : lead_webhooks
    """

    collection = app.db.lead_webhooks   # NEW COLLECTION

    # --------------------------------------------------------------
    # 1️⃣ VERIFY WEBHOOK (FACEBOOK MODE)
    # --------------------------------------------------------------
    @app.route('/lead/webhook', methods=['GET'])
    def verify_webhook():
        """
        Facebook sends a GET request to verify:
        /lead/webhook?hub.mode=subscribe&hub.verify_token=XXX&hub.challenge=YYY
        """

        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")

        VERIFY_TOKEN = "1234"   # <-- Change this

        if mode == "subscribe" and token == VERIFY_TOKEN:
            log.info("[WEBHOOK] Verification success ✔")
            return challenge, 200

        log.warning("[WEBHOOK] Verification failed ❌")
        return jsonify({"error": "Invalid verification token"}), 403

    # --------------------------------------------------------------
    # 2️⃣ WEBHOOK RECEIVER – STORE JSON INTO DB
    # --------------------------------------------------------------
    @app.route('/lead/webhook', methods=['POST'])
    def receive_webhook():
        """
        Receives POST JSON from Facebook or any webhook source
        Stores full JSON payload inside lead_webhooks collection
        """

        try:
            payload = request.get_json(force=True)
        except Exception as e:
            return jsonify({"status": "error", "message": "Invalid JSON"}), 400

        log.info(f"[WEBHOOK] Incoming Data: {payload}")

        record = {
            "payload": payload,
            "received_at": datetime.utcnow(),
            "source_ip": request.remote_addr
        }

        # Insert into MongoDB
        result = collection.insert_one(record)

        return jsonify({
            "status": "success",
            "message": "Webhook saved",
            "id": str(result.inserted_id)
        }), 200

  
    # --------------------------------------------------------------
    # 3️⃣ DEBUG ROUTE
    # --------------------------------------------------------------
    @app.route('/lead/webhook/debug', methods=['GET'])
    def debug_webhook():
        last = collection.find_one(sort=[("_id", -1)])

        # Convert ObjectId → string-safe JSON
        last_clean = convert_mongo_ids(last) if last else None

        return jsonify({
        "status": "webhook_module_loaded",
        "last_entry": last_clean,
        "server_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        }), 200


