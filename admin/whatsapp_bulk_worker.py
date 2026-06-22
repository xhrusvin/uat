
from datetime import datetime

from database import db
from .whatsapp_wati import _send_template_message
import traceback


def process_bulk_messages(campaign_id, batch_size=500):
    campaigns = db.whatsapp_bulk_campaigns
    messages = db.whatsapp_bulk_messages

    pending = list(
        messages.find({"status": "pending", "campaign_id": campaign_id}).limit(batch_size)
    )

    for msg in pending:
        campaign = campaigns.find_one({"_id": msg["campaign_id"]})
        template_name = campaign.get("template_name", "new_chat_v1") if campaign else "new_chat_v1"

        try:
            parameters = [
                {
                    "name": "name",
                    "value": msg.get("name", "")
                }
            ]

            result = _send_template_message(
                msg["phone"],
                template_name,
                parameters
            )

            # Build a human-readable snapshot of the message that was sent
            param_map = {p["name"]: p["value"] for p in parameters}
            name_val = param_map.get("name", "").strip()
            if name_val:
                message_text = f"Hi {name_val}! (via template: {template_name})"
            else:
                message_text = f"Template: {template_name}"

            messages.update_one(
                {"_id": msg["_id"]},
                {
                    "$set": {
                        "status": "sent",
                        "sent_at": datetime.utcnow(),
                        "wati_response": result,
                        "template_name": template_name,
                        "template_params": param_map,
                        "message_text": message_text,
                    }
                }
            )

            campaigns.update_one(
                {"_id": msg["campaign_id"]},
                {"$inc": {"sent": 1}}
            )

        except Exception as e:

            print("FAILED PHONE:", msg["phone"])
            print("ERROR:", str(e))
            print(traceback.format_exc())

            messages.update_one(
                {"_id": msg["_id"]},
                {
                    "$set": {
                        "status": "failed",
                        "error": str(e),
                        "traceback": traceback.format_exc()
                    }
                }
            )

            campaigns.update_one(
                {"_id": msg["campaign_id"]},
                {"$inc": {"failed": 1}}
            )
