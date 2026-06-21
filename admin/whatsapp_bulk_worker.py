
from datetime import datetime

from database import db
from .whatsapp_wati import _send_template_message
import traceback


def process_bulk_messages( campaign_id, batch_size=500 ):
    campaigns = db.whatsapp_bulk_campaigns
    messages = db.whatsapp_bulk_messages

    pending = list(
        messages.find({"status": "pending", "campaign_id": campaign_id}).limit(batch_size)
    )

    for msg in pending:
        campaign = campaigns.find_one(
            {"_id": msg["campaign_id"]}
        )

        try:
            parameters = [
                {
                    "name": "name",
                    "value": msg.get("name", "")
                }
            ]

            result = _send_template_message(
                msg["phone"],
                "new_chat_v1",
                parameters
            )

            messages.update_one(
                {"_id": msg["_id"]},
                {
                    "$set": {
                        "status": "sent",
                        "sent_at": datetime.utcnow(),
                        "wati_response": result
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
