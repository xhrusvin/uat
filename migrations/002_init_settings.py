# migrations/001_init_settings.py
from datetime import datetime

def up(db):
    """Create default settings if not exists"""
    settings = db.settings.find_one({"_id": "global"})
    if not settings:
        default_settings = {
            "_id": "global",
            "allow_registration_call": True,
            "enable_support_agent": True,   # NEW FIELD
            "enable_lead_call": True,   # NEW FIELD
            "enable_follow_up_call": True,   # NEW FIELD
            "enable_follow_up_call_bot4": True,   # NEW FIELD
            "maintenance_mode": False,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        db.settings.insert_one(default_settings)
        print("Migration 001: Settings document created with enable_support_agent.")
    else:
        # Add missing field if not exists
        if "enable_support_agent" not in settings:
            db.settings.update_one(
                {"_id": "global"},
                {"$set": {"enable_support_agent": True}}
            )
            print("Migration 001: Added enable_support_agent field.")
        else:
            print("Migration 001: Settings already up to date.")

def down(db):
    result = db.settings.update_one(
        {"_id": "global"},
        {"$unset": {"enable_support_agent": ""}}
    )
    print(f"Migration 001 down: enable_support_agent removed.")