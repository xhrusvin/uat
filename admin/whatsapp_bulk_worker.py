from datetime import datetime

from database import db
from .whatsapp_wati import _send_template_message
import traceback


def process_bulk_messages(campaign_id, batch_size=1000):
    campaigns = db.whatsapp_bulk_campaigns
    messages = db.whatsapp_bulk_messages

    # Campaign is the same for every message in this batch — read it once.
    campaign = campaigns.find_one({"_id": campaign_id}) or {}
    template_name = campaign.get("template_name", "new_chat_v1")
    parameter_config = campaign.get("parameter_config", [])

    pending = list(
        messages.find({"status": "pending", "campaign_id": campaign_id}).limit(batch_size)
    )

    for msg in pending:

        try:
            parameters = []

            for item in parameter_config:

                param_name = item.get("name")
                param_type = item.get("type")
                param_value = item.get("value")

                if not param_name:
                    continue

                # "user" and "excel" both resolve against row_data, which is
                # populated from the user document or the spreadsheet row.
                if param_type in ("user", "excel"):

                    value = (
                        msg.get("row_data", {})
                        .get(param_value, "")
                    )

                else:

                    value = param_value or ""

                parameters.append({
                    "name": param_name,
                    "value": str(value)
                })

            result = _send_template_message(
                msg["phone"],
                template_name,
                parameters
            )

            print("WATI RESPONSE")
            print(result)

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
