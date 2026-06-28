from datetime import datetime
import pandas as pd
from pymongo import MongoClient

# ==========================
# CONFIGURATION
# ==========================
MONGO_URI = "mongodb://localhost:27017/"  # Change if needed
DB_NAME = "xpress_health_uat"
COLLECTION_NAME = "live_staffs"

EXCEL_FILE = "merged_output.xlsx"

# ==========================
# CONNECT TO MONGODB
# ==========================
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# ==========================
# READ EXCEL
# ==========================
df = pd.read_excel(EXCEL_FILE)

updated = 0
not_found = 0
errors = 0

for _, row in df.iterrows():
    try:
        email = str(row["email"]).strip().lower()

        interview_url = row.get("interview_form_url")
        consent_url = row.get("consent_form_url")

        update = {
            "$set": {
                # Interview Form
                "if_doc_type": "Interview Form",
                "if_doc_url": interview_url,
                "if_document_name": "Interview Form",
                "if_fetched": pd.notna(interview_url),
                "if_fetched_at": datetime.utcnow(),
                "if_note": "uploaded successfully",

                # Consent Form
                "consent_doc_type": "Consent Form",
                "consent_doc_url": consent_url,
                "consent_document_name": "Consent Form",
                "consent_fetched": pd.notna(consent_url),
                "consent_fetched_at": datetime.utcnow(),
                "consent_note": "uploaded successfully",
            }
        }

        result = collection.update_one(
            {"email": email},
            update
        )

        if result.matched_count:
            updated += 1
            print(f"✓ Updated {email}")
        else:
            not_found += 1
            print(f"✗ User not found: {email}")

    except Exception as e:
        errors += 1
        print(f"Error processing {row.get('email')}: {e}")

print("\n========== SUMMARY ==========")
print(f"Updated    : {updated}")
print(f"Not Found  : {not_found}")
print(f"Errors     : {errors}")