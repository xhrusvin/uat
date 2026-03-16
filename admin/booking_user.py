# Run once
from pymongo import MongoClient
import bcrypt
import os
from dotenv import load_dotenv
load_dotenv()

client = MongoClient(os.getenv('MONGO_URI'))
db = client[os.getenv('DB_NAME')]

password = "admin123"
hashed = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())

db.users.update_one(
    {"email": "booking@admin.com"},
    {"$set": {
        "email": "booking@admin.com",
        "password": hashed,
        "first_name": "Admin",
        "last_name": "User",
        "is_admin": True,
        "phone": "",
        "call_sent": 0
    }},
    upsert=True
)
print("Admin created: booking@admin.com / admin123")