from datetime import datetime
import pandas as pd
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "xpress_health_uat"
COLLECTION = "live_staffs"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION]

df = pd.read_excel("merged_output.xlsx")

# Remove leading/trailing spaces from column names
df.columns = df.columns.str.strip()

updated = 0
not_found = 0

for _, row in df.iterrows():
    email = str(row["Email"]).strip().lower()

    consent_name = row["Consent Form"]
    consent_url = row["Consent Form URL"]

    interview_name = row["Interview Form"]
    interview_url = row["Interview Form URL"]

    result = collection.update_one(
        {"email": email},
        {
            "$set": {
                # Consent Form
                "consent_doc_type": "Consent Form",
                "consent_document_name": consent_name,
                "consent_doc_url": consent_url,
                "consent_fetched": True,
                "consent_fetched_at": datetime.utcnow(),
                "consent_note": "uploaded successfully",

                # Interview Form
                "intf_doc_type": "Interview Form",
                "intf_document_name": interview_name,
                "intf_doc_url": interview_url,
                "intf_fetched": True,
                "intf_fetched_at": datetime.utcnow(),
                "intf_note": "uploaded successfully",
            }
        }
    )

    if result.matched_count:
        updated += 1
        print(f"✓ Updated {email}")
    else:
        not_found += 1
        print(f"✗ Not found: {email}")

print(f"\nUpdated: {updated}")
print(f"Not Found: {not_found}")