from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime
from dotenv import load_dotenv

# --- اتصال (Connection) ---

load_dotenv()


MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


# --- Helper: create collection with validator ---
def create_collection(name, validator):
    try:
        db.create_collection(name)
        db.command("collMod", name, validator=validator)
        print(f"{name} created with validator")
    except Exception as e:
        print(f"{name} exists or error: {e}")


# -----------------------------
# document_types
# -----------------------------
create_collection("document_types", {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["code", "name", "is_active"],
        "properties": {
            "code": {"bsonType": "string"},
            "name": {"bsonType": "string"},
            "description": {"bsonType": ["string", "null"]},
            "is_active": {"bsonType": "bool"},
            "created_at": {"bsonType": "date"}
        }
    }
})

db.document_types.create_index([("code", ASCENDING)], unique=True)


# -----------------------------
# prompts
# -----------------------------
create_collection("prompts", {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["document_type_code", "version", "prompt_text"],
        "properties": {
            "document_type_code": {"bsonType": "string"},
            "version": {"bsonType": "int"},
            "prompt_text": {"bsonType": "string"},
            "variables": {
                "bsonType": ["array"],
                "items": {"bsonType": "string"}
            },
            "is_active": {"bsonType": "bool"},
            "created_at": {"bsonType": "date"}
        }
    }
})

db.prompts.create_index(
    [("document_type_code", ASCENDING), ("version", DESCENDING)],
    unique=True
)


# -----------------------------
# documents
# -----------------------------
create_collection("documents", {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["user_id", "document_type_code", "status"],
        "properties": {
            "user_id": {"bsonType": "objectId"},
            "document_type_code": {"bsonType": "string"},
            "file_url": {"bsonType": ["string", "null"]},
            "extracted_text": {"bsonType": ["string", "null"]},
            "status": {
                "enum": ["pending", "processing", "completed", "failed"]
            },
            "last_validation_id": {"bsonType": ["objectId", "null"]},
            "hash": {"bsonType": ["string", "null"]},
            "uploaded_at": {"bsonType": "date"},
            "updated_at": {"bsonType": "date"}
        }
    }
})

db.documents.create_index([("user_id", ASCENDING), ("document_type_code", ASCENDING)])
db.documents.create_index([("status", ASCENDING)])
db.documents.create_index([("hash", ASCENDING)])


# -----------------------------
# validations
# -----------------------------
create_collection("validations", {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["user_id", "document_id", "document_type_code", "status"],
        "properties": {
            "user_id": {"bsonType": "objectId"},
            "document_id": {"bsonType": "objectId"},
            "document_type_code": {"bsonType": "string"},
            "prompt": {
                "bsonType": "object",
                "properties": {
                    "id": {"bsonType": "objectId"},
                    "version": {"bsonType": "int"}
                }
            },
            "status": {
                "enum": ["success", "failed", "processing"]
            },
            "result": {"bsonType": ["object", "null"]},
            "failed_reason": {"bsonType": ["string", "null"]},
            "error": {"bsonType": ["string", "null"]},
            "raw_response": {"bsonType": ["object", "null"]},
            "validated_at": {"bsonType": "date"},
            "processing_time_ms": {"bsonType": ["int", "null"]}
        }
    }
})

db.validations.create_index([("user_id", ASCENDING)])
db.validations.create_index([("document_id", ASCENDING)])
db.validations.create_index([("status", ASCENDING)])


# -----------------------------
# user_status
# -----------------------------
create_collection("user_status", {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["user_id", "document_type_code"],
        "properties": {
            "user_id": {"bsonType": "objectId"},
            "document_type_code": {"bsonType": "string"},
            "status": {
                "enum": ["valid", "invalid", "missing", "expired", "unknown"]
            },
            "latest_validation_id": {"bsonType": "objectId"},
            "updated_at": {"bsonType": "date"}
        }
    }
})

db.user_status.create_index(
    [("user_id", ASCENDING), ("document_type_code", ASCENDING)],
    unique=True
)


# -----------------------------
# user_document_status
# -----------------------------
create_collection("user_document_status", {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["user_id", "document_type_code", "status"],
        "properties": {
            "user_id": {"bsonType": "objectId"},
            "document_type_code": {"bsonType": "string"},
            "status": {
                "enum": ["valid", "invalid", "missing", "expired", "unknown"]
            },
            "latest_validation_id": {"bsonType": ["objectId", "null"]},
            "source": {
                "enum": ["ai", "manual"]
            },
            "last_updated": {"bsonType": "date"}
        }
    }
})


# -----------------------------
# Seed Data (prompts)
# -----------------------------
db.prompts.insert_many([
    {
        "document_type_code": "ATTACH_ANY_OTHER_CERTIFICATE",
        "version": 1,
        "prompt_text": "Validate this document and return JSON with is_valid, status, failed_reason.",
        "is_active": True,
        "created_at": datetime.utcnow()
    },
    {
        "document_type_code": "POLICE_CLEARANCE",
        "version": 1,
        "prompt_text": "Check if police clearance is valid and not expired. Return JSON.",
        "is_active": True,
        "created_at": datetime.utcnow()
    }
])

print("Setup completed successfully!")