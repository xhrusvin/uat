import os
import json
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
PROMPTS_COLLECTION = os.getenv("PROMPTS_COLLECTION")
DOCUMENT_TYPES_COLLECTION = os.getenv("DOCUMENT_TYPES_COLLECTION")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]



def convert_bson(doc):
    """Convert $oid and $date to Mongo types"""
    if isinstance(doc, dict):
        if "$oid" in doc:
            return ObjectId(doc["$oid"])
        if "$date" in doc:
            return datetime.fromisoformat(doc["$date"].replace("Z", ""))
        return {k: convert_bson(v) for k, v in doc.items()}

    elif isinstance(doc, list):
        return [convert_bson(i) for i in doc]

    return doc



def load_json(filename):
    with open(filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    return convert_bson(data)




def upsert_data(collection_name, data):
    collection = db[collection_name]

    for doc in data:
        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": doc},
            upsert=True
        )

    print(f"Upserted {len(data)} records into {collection_name}")




if __name__ == "__main__":
    prompts_data = load_json("xpress_health_uat.prompts.json")
    document_types_data = load_json("xpress_health_uat.document_types.json")

    upsert_data(PROMPTS_COLLECTION, prompts_data)
    upsert_data(DOCUMENT_TYPES_COLLECTION, document_types_data)

    print("Done.")