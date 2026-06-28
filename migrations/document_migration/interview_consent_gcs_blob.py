from pymongo import MongoClient
import pandas as pd

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "xpress_health_uat"
COLLECTION = "live_staffs"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION]

df = pd.read_excel("file_mapping.xlsx")
df.columns = df.columns.str.strip()

updated = 0
not_found = 0

for _, row in df.iterrows():
    email = str(row["Email"]).strip().lower()

    consent_file = str(row["Consent Form"]).strip()
    interview_file = str(row["Interview Form"]).strip()

    consent_blob = f"consent_form/{consent_file}" if consent_file else None
    interview_blob = f"interview_form/{interview_file}" if interview_file else None

    result = collection.update_one(
        {"email": email},
        {
            "$set": {
                "consent_gcs_blob": consent_blob,
                "interview_gcs_blob": interview_blob,
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