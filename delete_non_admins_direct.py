# delete_non_admins_direct.py
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
users = db['users']

# DIRECT DELETE — NO CONFIRMATION!
deleted = users.delete_many({
    "$or": [
        {"is_admin": {"$ne": True}},
        {"is_admin": {"$exists": False}}
    ]
}).deleted_count

print(f"DELETED {deleted} non-admin users permanently.")
print(f"Remaining admins: {users.count_documents({'is_admin': True})}")