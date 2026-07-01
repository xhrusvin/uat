from pymongo import MongoClient
import pandas as pd
import re

# -----------------------------
# Configuration
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"  # Change if needed
DB_NAME = "xpress_health_uat"
COLLECTION = "live_staffs"

INPUT_FILE = "Review_sheet.xlsx"
OUTPUT_FILE = "output_with_email.xlsx"

PHONE_COLUMN = "Numbers" 


def normalize_phone(phone):
    """
    Remove spaces, +, -, brackets, etc.
    Example:
        +353 892294012 -> 353892294012
        +91-98765 43210 -> 919876543210
    """
    if pd.isna(phone):
        return None
    return re.sub(r"\D", "", str(phone))


# -----------------------------
# MongoDB
# -----------------------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION]

print("Loading phone numbers from MongoDB...")

phone_to_email = {}

for doc in collection.find(
    {},
    {
        "email": 1,
        "section_1_personal_details.mobile_number": 1,
    },
):
    details = doc.get("section_1_personal_details", {})

    # In case the field is stored as a list
    if isinstance(details, list):
        if not details:
            continue
        details = details[0]

    phone = normalize_phone(details.get("mobile_number"))
    email = doc.get("email", "")

    if phone:
        phone_to_email[phone] = email

print(f"Loaded {len(phone_to_email)} phone numbers.")

# -----------------------------
# Read Excel
# -----------------------------
df = pd.read_excel(INPUT_FILE)

df.columns = df.columns.str.strip()

emails = []

for phone in df[PHONE_COLUMN]:
    normalized = normalize_phone(phone)
    emails.append(phone_to_email.get(normalized, ""))

df["email"] = emails

df.to_excel(OUTPUT_FILE, index=False)

matched = (df["email"] != "").sum()

print(f"Matched {matched} of {len(df)} records.")
print(f"Saved to {OUTPUT_FILE}")