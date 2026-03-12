from playwright.sync_api import sync_playwright
import os
import glob
from datetime import datetime
import time
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import pytz

# Load environment variables
load_dotenv()

# ================= CONFIGURATION =================
USERNAME = "rusvin@xpresshealth.ie"
PASSWORD = "Rusvin@123"

LOGIN_URL = "https://phase-three.xpresshealthapp.ie/login"
TARGET_URL = "https://phase-three.xpresshealthapp.ie/recruitment/all?start_date=2025-05-14&end_date=2026-05-14&page=1&per_page=50"

DOWNLOAD_FOLDER = "recruitment_downloads"
EXPORT_BUTTON_CLASS = "btn btn-outlined md flex items-center rounded-md gap-x-md justify-center transition-all font-medium relative pointer-events-all"

# MongoDB
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
users_collection = db['users']

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
IS_HEADLESS = not os.getenv("DISPLAY", None)  # Headless if no display

# ================================================

def delete_old_exports(keep_path):
    pattern = os.path.join(DOWNLOAD_FOLDER, "recruitment_export_*.xlsx")
    old_files = glob.glob(pattern)
    deleted = 0
    for file_path in old_files:
        if os.path.abspath(file_path) != os.path.abspath(keep_path):
            try:
                os.remove(file_path)
                print(f"Deleted old file: {os.path.basename(file_path)}")
                deleted += 1
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")
    if deleted == 0:
        print("No old files to delete.")

def download_excel():
    final_path = None
    with sync_playwright() as p:
        print(f"Launching browser in {'headless' if IS_HEADLESS else 'headed'} mode...")
        browser = p.chromium.launch(headless=IS_HEADLESS, args=["--no-sandbox", "--disable-setuid-sandbox", "--start-maximized"])
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        try:
            print("Opening login page...")
            page.goto(LOGIN_URL, wait_until="networkidle")
            page.fill("input[name='user_name'], input[type='text']", USERNAME)
            page.fill("input[name='password'], input[type='password']", PASSWORD)

            with page.expect_navigation(wait_until="networkidle"):
                page.click("button[type='submit'], button:has-text('Login')")

            if "login" in page.url.lower():
                print("Login failed!")
                return None

            print("Login successful!")
            print("Navigating to recruitment page...")
            page.goto(TARGET_URL, wait_until="networkidle")
            page.wait_for_selector("text=Recruitment", timeout=15000)

            print("Clicking Export button...")
            export_button = page.locator(f".{EXPORT_BUTTON_CLASS.replace(' ', '.')}").first
            with page.expect_download(timeout=60000) as download_info:
                export_button.click(force=True)
                time.sleep(4)  # small buffer

            download = download_info.value
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            final_name = f"recruitment_export_{timestamp}.xlsx"
            final_path = os.path.join(DOWNLOAD_FOLDER, final_name)
            download.save_as(final_path)
            print(f"Downloaded: {final_path}")

        except Exception as e:
            print(f"Error during automation: {e}")
            page.screenshot(path="error_debug.png")
        finally:
            browser.close()

    return final_path

def clean_and_insert_only_new_irish_users(file_path):
    if not file_path or not os.path.exists(file_path):
        print("No file to process.")
        return

    print(f"\nProcessing file: {os.path.basename(file_path)}")
    df = pd.read_excel(file_path)

    # Normalize column names
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    print(f"Columns found: {list(df.columns)}")

    # Column mapping
    column_map = {
        'first_name': ['first_name', 'name', 'firstname', 'first'],
        'last_name':  ['last_name', 'surname', 'lastname', 'last'],
        'email':      ['email', 'email_address', 'e-mail'],
        'phone':      ['phone', 'mobile', 'phone_number', 'contact', 'telephone'],
        'designation':['user_type', 'designation', 'role', 'job_title', 'position'],
        'country':    ['region', 'country', 'location'],
    }

    mapped = {}
    for std, variants in column_map.items():
        for v in variants:
            if v in df.columns:
                mapped[std] = v
                break
        else:
            mapped[std] = None

    # Required: email and phone
    if not mapped['email']:
        print("No email column found. Cannot proceed.")
        return

    # Take only the FIRST 10 rows (latest assumed to be at the top)
    df = df.head(10).copy()
    print(f"Taking only the latest 10 records from Excel.")

    inserted = skipped_no_phone = skipped_not_irish = skipped_exists = skipped_blocked_role = 0
    now_utc = datetime.utcnow().replace(tzinfo=pytz.UTC)

    # Roles to block (case-insensitive)
    BLOCKED_DESIGNATIONS = {"admin assistant", "support worker", "pharmacy technician"}

    for idx, row in df.iterrows():
        # Extract and clean email
        raw_email = row.get(mapped['email'], '')
        email = str(raw_email).strip().lower() if pd.notna(raw_email) else ""
        if not email or '@' not in email:
            print(f"Row {idx+2}: Invalid or missing email → skipped")
            skipped_no_phone += 1
            continue

        # Check if user already exists
        if users_collection.find_one({"email": email}):
            print(f"Row {idx+2}: {email} already exists → skipped (no update)")
            skipped_exists += 1
            continue

        # Extract and validate phone: must start with +353
        raw_phone = row.get(mapped['phone'], '') if mapped['phone'] else ''
        phone_str = str(raw_phone).strip().replace(" ", "").replace("-", "")
        if not phone_str.startswith('+353'):
            print(f"Row {idx+2}: Phone {phone_str or 'missing'} does not start with +353 → skipped")
            skipped_not_irish += 1
            continue

        # === NEW: Block Admin Assistant & Support Worker ===
        raw_designation = row.get(mapped['designation'], "") if mapped['designation'] else ""
        designation_clean = str(raw_designation).strip().lower()
        if any(blocked in designation_clean for blocked in BLOCKED_DESIGNATIONS):
            print(f"Row {idx+2}: Designation '{raw_designation}' is blocked (Admin Assistant / Support Worker) → skipped")
            skipped_blocked_role += 1
            continue
        # ================================================

        # Build clean user document
        user_doc = {
            "email": email,
            "first_name": str(row.get(mapped['first_name'], "") or "").strip() or "Unknown",
            "last_name": str(row.get(mapped['last_name'], "") or "").strip() or " ",
            "phone": phone_str,
            "designation": str(raw_designation).strip() or "unknown",
            "country": str(row.get(mapped['country'], "") or "Ireland").strip() or "Ireland",
            "call_sent": 0,
            "created_at": now_utc.isoformat(),
            "updated_at": now_utc.isoformat(),
        }

        # Insert only (no update)
        try:
            users_collection.insert_one(user_doc)
            print(f"Row {idx+2}: Inserted → {email} | {phone_str} | {designation_clean.title()}")
            inserted += 1
        except Exception as e:
            print(f"Row {idx+2}: Failed to insert {email}: {e}")
            skipped_exists += 1

    print("\n" + "="*60)
    print(f"SUMMARY:")
    print(f"   Inserted (new +353 users):          {inserted}")
    print(f"   Skipped (already exists):           {skipped_exists}")
    print(f"   Skipped (not +353 phone):           {skipped_not_irish}")
    print(f"   Skipped (invalid email):            {skipped_no_phone}")
    print(f"   Skipped (blocked designation):      {skipped_blocked_role}")
    print("="*60)

def main():
    latest_file = download_excel()
    if not latest_file:
        print("Download failed. Exiting.")
        return

    print("\nCleaning up old export files...")
    delete_old_exports(latest_file)

    clean_and_insert_only_new_irish_users(latest_file)

    print("\n" + "="*80)
    print("FIRST 10 ROWS FROM LATEST EXCEL (for reference)")
    print("="*80)
    df_preview = pd.read_excel(latest_file).head(10)
    print(df_preview[['Email', 'Phone', 'First Name', 'Last Name'] if 'Email' in df_preview.columns else df_preview.columns[:4]].to_string(index=True))
    print(f"\nTotal rows in file: {len(pd.read_excel(latest_file))}")

if __name__ == "__main__":
    main()