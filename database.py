# database.py  (top-level, same level as app.py)
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME   = os.getenv("DB_NAME")

if not MONGO_URI or not DB_NAME:
    raise ValueError("MONGO_URI and DB_NAME must be set in .env")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
references_collection = db["users"]