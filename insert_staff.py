#!/usr/bin/env python3
"""
insert_staff.py
Inserts staff records from Mismatched_Staff.xlsx into MongoDB live_staffs collection.
Skips records where the email already exists.

Requirements:
    pip3 install pandas openpyxl pymongo
"""

import sys
from datetime import datetime, timezone

try:
    import pandas as pd
except ImportError:
    print("Missing dependency: pip3 install pandas openpyxl")
    sys.exit(1)

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, OperationFailure
except ImportError:
    print("Missing dependency: pip3 install pymongo")
    sys.exit(1)

# ─── CONFIG ──────────────────────────────────────────────────────────────────

MONGO_URI = "mongodb://localhost:27017"   # Change if using auth: mongodb://user:pass@host:port
DATABASE  = "xpress_health_uat"          # Change to your actual DB name
COLLECTION = "live_staffs"
EXCEL_FILE = "Mismatched_Staff.xlsx"      # Path to the Excel file

# ─────────────────────────────────────────────────────────────────────────────


def load_excel(path: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(path)
        print(f"Loaded {len(df)} rows from '{path}'")
        return df
    except FileNotFoundError:
        print(f"ERROR: File not found: {path}")
        sys.exit(1)
    except Exception as e:
        print(f"ERROR reading Excel: {e}")
        sys.exit(1)


def connect_mongo(uri: str, db_name: str):
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        print(f"Connected to MongoDB at {uri}")
        return client[db_name]
    except ConnectionFailure as e:
        print(f"ERROR: Could not connect to MongoDB: {e}")
        sys.exit(1)


def build_document(row: pd.Series) -> dict | None:
    email     = str(row.get("Email ID", "")).strip()
    agency_id = str(row.get("Agency ID or Reference", "")).strip()
    name      = str(row.get("Name", "")).strip()
    category  = str(row.get("Staff Category", "")).strip()

    if not email or email.lower() == "nan":
        return None

    if agency_id.lower() == "nan":
        agency_id = ""

    return {
        "email": email,
        "employee_code": agency_id,
        "created_at": datetime.now(timezone.utc),
        "user_type": category,
        "section_1_personal_details": {
            "full_name": name,
            "previous_names": "",
            "date_of_birth": "",
            "address": "",
            "eircode_postcode": None,
            "mobile_number": "",
            "email_address": email,
            "pps_number": "",
            "nationality": "",
            "work_permit_visa_status": {
                "permission_to_work": "",
                "visa_type": ""
            },
            "nmbi_pin_number": ""
        }
    }


def run(excel_path: str, mongo_uri: str, db_name: str, collection_name: str):
    df         = load_excel(excel_path)
    db         = connect_mongo(mongo_uri, db_name)
    collection = db[collection_name]

    inserted  = 0
    skipped_no_email  = 0
    skipped_exists    = 0
    errors    = 0

    print(f"\nProcessing {len(df)} rows into '{db_name}.{collection_name}' ...\n")

    for idx, row in df.iterrows():
        doc = build_document(row)

        if doc is None:
            name = str(row.get("Name", "")).strip()
            print(f"  [SKIP] Row {idx + 2}: No email — {name}")
            skipped_no_email += 1
            continue

        email = doc["email"]

        try:
            if collection.find_one({"email": email}):
                print(f"  [EXISTS] {email}")
                skipped_exists += 1
            else:
                collection.insert_one(doc)
                print(f"  [INSERTED] {email} — {doc['section_1_personal_details']['full_name']}")
                inserted += 1
        except OperationFailure as e:
            print(f"  [ERROR] {email}: {e}")
            errors += 1

    print(f"""
─────────────────────────────
  Done.
  Inserted  : {inserted}
  Skipped (email exists) : {skipped_exists}
  Skipped (no email)     : {skipped_no_email}
  Errors    : {errors}
─────────────────────────────""")


if __name__ == "__main__":
    run(EXCEL_FILE, MONGO_URI, DATABASE, COLLECTION)
