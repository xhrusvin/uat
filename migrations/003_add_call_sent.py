# migrations/002_add_call_sent.py
from datetime import datetime

def up(db):
    """Add call_sent field to all users (default 0)"""
    result = db.users.update_many(
        {"call_sent": {"$exists": False}},
        {"$set": {"call_sent": 0, "updated_at": datetime.utcnow()}}
    )
    print(f"Migration 002: Added call_sent=0 to {result.modified_count} users.")

    # Also ensure new users get the field automatically via collection validation (optional)
    try:
        db.command({
            "collMod": "users",
            "validator": {
                "$jsonSchema": {
                    "bsonType": "object",
                    "required": ["email", "phone", "call_sent"],
                    "properties": {
                        "call_sent": {
                            "bsonType": "int",
                            "enum": [0, 1],
                            "description": "0 = call not sent, 1 = call sent"
                        }
                    }
                }
            }
        })
        print("Migration 002: Updated collection validator to require call_sent.")
    except Exception as e:
        print(f"Migration 002: Validator update skipped (may already exist): {e}")

def down(db):
    """Remove call_sent field"""
    result = db.users.update_many(
        {},
        {"$unset": {"call_sent": ""}, "$set": {"updated_at": datetime.utcnow()}}
    )
    print(f"Migration 002 down: Removed call_sent from {result.modified_count} users.")