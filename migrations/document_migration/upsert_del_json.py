import json
from copy import deepcopy
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "xpress_health_uat"
COLLECTION = "live_staffs"

JSON_FILE = "deleted_staff.json"

client = MongoClient(MONGO_URI)
collection = client[DB_NAME][COLLECTION]


def merge_missing_fields(existing, incoming):
    """
    Recursively copy only missing fields from incoming into existing.
    Existing values are NEVER overwritten.
    """
    changed = False

    for key, value in incoming.items():

        # Key missing entirely
        if key not in existing:
            existing[key] = deepcopy(value)
            changed = True
            continue

        # Both dictionaries -> recurse
        if isinstance(existing[key], dict) and isinstance(value, dict):
            if merge_missing_fields(existing[key], value):
                changed = True

        # Arrays are left untouched.
        # Existing scalar values are also left untouched.

    return changed


with open(JSON_FILE, "r", encoding="utf-8") as f:
    staffs = json.load(f)

if isinstance(staffs, dict):
    staffs = [staffs]

inserted = 0
updated = 0
unchanged = 0

for staff in staffs:

    email = staff.get("email")
    if not email:
        continue

    existing = collection.find_one({"email": email})

    # Doesn't exist -> insert
    if existing is None:
        collection.insert_one(staff)
        inserted += 1
        continue

    # Merge missing fields
    merged = deepcopy(existing)

    if merge_missing_fields(merged, staff):

        collection.replace_one(
            {"_id": existing["_id"]},
            merged
        )

        updated += 1
    else:
        unchanged += 1

print(f"Inserted : {inserted}")
print(f"Updated  : {updated}")
print(f"Unchanged: {unchanged}")

client.close()