from pymongo import MongoClient
import json

# Configuration
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "xpress_health_uat"
COLLECTION = "live_staffs"
JSON_FILE = "removed_staff.json"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION]

# Load JSON
with open(JSON_FILE, "r", encoding="utf-8") as f:
    users = json.load(f)

matched = 0
modified = 0
not_found = []

for user in users:
    email = user.get("email")
    if not email:
        continue

    result = collection.update_one(
        {"email": email},
        {"$set": {"special": 1}}
    )

    matched += result.matched_count
    modified += result.modified_count

    if result.matched_count == 0:
        not_found.append(email)

print("=" * 50)
print(f"Total records in JSON : {len(users)}")
print(f"Matched in MongoDB    : {matched}")
print(f"Updated              : {modified}")
print(f"Not found            : {len(not_found)}")

if not_found:
    print("\nEmails not found:")
    for email in not_found:
        print(email)

client.close()