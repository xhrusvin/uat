import os
import json
from unittest import result
import requests
from datetime import datetime
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import MongoClient
from google import genai
import base64
import logging
from datetime import datetime, UTC
import re

import json



load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

geminiclient = genai.Client(api_key=GEMINI_API_KEY)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

DOC_TYPE_MAP = {
    "INFECTION_PREVENTION_CONTROL_CERTIFICATE": "IPC",
    "SAFEGUARDING_ADULTS_AT_RISK": "SAFEGUARDING_ADULTS",
    "QQI_LEVEL_5_OR_EQUIVALENT": "QQI_LEVEL_5",
    "CPR/BLS": "CPR_BLS",
    "MANUAL_AND_PEOPLE_HANDLING_DOCUMENTS": "MANUAL_HANDLING",
    "GARDA_VETTING_DOCUMENT": "GARDA_VETTING",
    "EMPLOYMENT_CONTRACT_SIGNED": "EMPLOYMENT_CONTRACT",
    "THE_OPEN_DISCLOSURE": "OPEN_DISCLOSURE",
    "CPI/_MAPA/PMAV": "CPI_MAPA_PMAV",
    "Nmbi Qualification": "NBMI",
    "Covid Certificate": "COVID",
    "Medication Management": "MEDICATION",
    "Fire Safety": "FIRE_SAFETY",
    "IPC": "IPC",
    "Hand Hygiene": "HAND_HYGIENE",
    "Children First": "CHILDREN_FIRST",
    "Safeguarding Adults": "SAFEGUARDING_ADULTS",
    "CV": "CV",
    "CPR/BLS": "CPR_BLS",
    "Manual Handling": "MANUAL_HANDLING",
    "Garda Vetting": "GARDA_VETTING",
    "PPE": "PPE",
    "Employment Contract": "EMPLOYMENT_CONTRACT",
    "Open Disclosure": "OPEN_DISCLOSURE",
    "CPI/MAPA/PMAV": "CPI_MAPA_PMAV",
    "Cyber Security": "CYBER_SECURITY",
    "GDPR": "GDPR",
    "HACCP/FOOD_SAFETY": "HACCP_FOOD_SAFETY",
    "CPR/BLS": "CPR_BLS",
    "CPI/_MAPA/PMAV": "CPI_MAPA_PMAV",
    "POLICE_CLEARANCE_CERTIFICATE_(_FROM_COUNTRY_OF_BIRTH_)": "POLICE_CLEARANCE"
}



def log_stats():
    pending = db.documents.count_documents({"status": "pending"})
    processing = db.documents.count_documents({"status": "processing"})
    completed = db.documents.count_documents({"status": "completed"})
    failed = db.documents.count_documents({"status": "failed"})

    logging.info(
        f"[STATS] Pending: {pending} | Processing: {processing} | Completed: {completed} | Failed: {failed}"
    )

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def safe_json_parse(text):
    try:
        return json.loads(text)
    except:
        return {
            "is_valid": False,
            "status": "invalid",
            "failed_reason": "Invalid AI response"
        }


def get_prompt(document_type_code):
    return db.prompts.find_one(
        {
            "document_type_code": document_type_code,
            "is_active": True
        },
        sort=[("version", -1)]
    )


def get_pending_failed_count():
    return db.documents.count_documents({
        "status": {"$in": ["pending", "failed"]}
    })


# def extract_json(text):
#     try:
#         # extract JSON block
#         match = re.search(r'\{.*\}', text, re.DOTALL)
#         if match:
#             return match.group(0)
#         return text
#     except:
#         return text



# def extract_json(text):
#     try:
#         # 🔥 Remove markdown code blocks
#         text = text.replace("```json", "").replace("```", "").strip()

#         # 🔥 Extract JSON object
#         match = re.search(r'\{.*\}', text, re.DOTALL)
#         if match:
#             return match.group(0)

#         return text
#     except Exception as e:
#         logging.error(f"[EXTRACT JSON ERROR] {e}")
#         return text



def extract_json(text):
    try:
        text = text.strip()

        # Remove markdown
        if text.startswith("```"):
            text = text.split("```")[1]

        # Find first valid JSON object
        start = text.find("{")
        end = text.rfind("}")

        if start != -1 and end != -1:
            candidate = text[start:end+1]

            # Validate JSON BEFORE returning
            json.loads(candidate)

            return candidate

        return text

    except Exception as e:
        logging.error(f"[EXTRACT JSON ERROR] {e}")
        return text

def call_gemini(doc_url, prompt_text):
    try:
        resp = requests.get(doc_url, timeout=20)
        resp.raise_for_status()

        file_bytes = resp.content

        file_size_kb = len(file_bytes) / 1024
        logging.info(f"[FILE] Downloaded: {doc_url} | Size: {file_size_kb:.2f} KB")

        response = geminiclient.models.generate_content(
            model="gemini-2.5-flash",
            contents=[
                {
                    "role": "user",
                    "parts": [
                        {"text": prompt_text},
                        {
                            "inline_data": {
                                "mime_type": "application/pdf",
                                "data": base64.b64encode(file_bytes).decode("utf-8")
                            }
                        }
                    ]
                }
            ]
        )

        result = response.text or ""

        logging.info(f"[GEMINI RESPONSE RAW]\n{result[:1000]}")

        cleaned = extract_json(result)

        logging.info(f"[GEMINI CLEANED JSON]\n{cleaned}")

        return cleaned

    except Exception as e:
        import traceback
        logging.error(f"[GEMINI ERROR] {str(e)}")
        logging.error(traceback.format_exc())

        return json.dumps({
            "is_valid": False,
            "status": "invalid",
            "failed_reason": f"Processing error: {str(e)}"
        })

# ─────────────────────────────────────────────
# MAIN WORKER
# ─────────────────────────────────────────────
def process_documents(limit=10):

    # logging.info(f"[DEBUG] DB Name: {DB_NAME}")
    # logging.info(f"[DEBUG] Collections: {db.list_collection_names()}")
    total_docs = db.documents.count_documents({})
    logging.info(f"[DEBUG] Total documents in DB: {total_docs}")
    sample_docs = list(db.documents.find({}, {"status": 1}).limit(5))
    logging.info(f"[DEBUG] Sample statuses: {sample_docs}")
    logging.info("===== WORKER CYCLE START =====")

    docs = db.documents.find({
        "status": {"$in": ["pending", "failed"]}
    }).limit(limit)

    log_stats()

    for doc in docs:
        try:
            print(f"Processing: {doc['_id']}")
            logging.info(f"[START] Processing document: {doc['_id']} | Type: {doc['document_type_code']}")
            remaining = get_pending_failed_count()
            logging.info(f"[QUEUE] Documents remaining (pending + failed): {remaining}")

            start_time = datetime.now(UTC)

            # ── LOCK
            # updated = db.documents.update_one(
            #     {"_id": doc["_id"], "status": {"$ne": "processing"}},
            #     {"$set": {"status": "processing"}}
            # )

            updated = db.documents.update_one(
                {"_id": doc["_id"], "status": {"$in": ["pending", "failed"]}},
                {"$set": {"status": "processing", "updated_at": datetime.now(UTC)}}
            )

            if updated.modified_count == 0:
                continue

            # ── USER
            user = db.users.find_one({"_id": doc["user_id"]})
            if not user:
                raise Exception("User not found")

            # ── PROMPT
            # prompt_doc = get_prompt(doc["document_type_code"])
            raw_type = doc["document_type_code"]

            mapped_type = DOC_TYPE_MAP.get(
                raw_type,
                raw_type.upper().replace(" ", "_").replace("/", "_")
            )

            logging.info(f"[MAPPING] {raw_type} → {mapped_type}")

            prompt_doc = get_prompt(mapped_type)



            if not prompt_doc:
                raise Exception("Prompt not found")

            today = datetime.now(UTC).strftime("%Y-%m-%d")

            if prompt_doc:
                final_prompt = (
                    prompt_doc["prompt_text"]
                    .replace("{today}", today)
                    .replace("{fullName}", user.get("full_name", ""))
                )
            else:
                final_prompt = "Validate this document"

            logging.info(f"[PROMPT] Using prompt for {mapped_type}:\n{final_prompt[:500]}")

            # ── GEMINI CALL
            logging.info("[GEMINI REQUEST] Sending request to Gemini...")
            result_text = call_gemini(doc["file_url"], final_prompt)
            parsed = safe_json_parse(result_text)

            logging.info(f"[PARSED RESULT] {json.dumps(parsed, indent=2)}")

            status = "success" if parsed.get("is_valid") else "failed"

            logging.info(f"[DECISION] Status: {status}")

            # ── SAVE VALIDATION
            validation_id = db.validations.insert_one({
                "user_id": doc["user_id"],
                "document_id": doc["_id"],
                "document_type_code": mapped_type,
                "prompt": {
                    "id": prompt_doc["_id"],
                    "version": prompt_doc["version"]
                },
                "status": status,
                "result": parsed,
                "failed_reason": parsed.get("failed_reason"),
                "raw_response": parsed,
                "validated_at": datetime.now(UTC)
            }).inserted_id

            # ── UPDATE DOCUMENT
            db.documents.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "status": "completed",
                        "last_validation_id": validation_id,
                        "updated_at": datetime.now(UTC)
                    }
                }
            )

            # ── UPDATE USER SNAPSHOT
            db.users.update_one(
                {"_id": doc["user_id"]},
                {
                    "$set": {
                        f"document_status.{doc['document_type_code']}": {
                            "status": parsed.get("status", "unknown"),
                            "last_updated": datetime.now(UTC),
                            "validation_id": validation_id
                        }
                    }
                }
            )

            print(f"Done: {doc['_id']}")
            logging.info(f"[SUCCESS] {doc['_id']} processed successfully")
            processing_time = int((datetime.now(UTC) - start_time).total_seconds() * 1000) / 1000
            print(f"Processing time: {processing_time} s")
            print("================================ \n\n")

        except Exception as e:
            print(f"Error: {e}")
            logging.error(f"[ERROR] {doc['_id']} failed with error: {e}")

            db.documents.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "status": "failed",
                        "error": str(e),
                        "updated_at": datetime.now(UTC)
                    }
                }
            )


