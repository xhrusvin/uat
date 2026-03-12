# scrap_users.py
import threading
from flask import jsonify, request, current_app
from datetime import datetime
import pytz
import subprocess

# =================== YOUR FULL SCRAPER CODE BELOW ===================
# Paste your entire script here (from playwright import ... to the end)
# But REMOVE the last line: if __name__ == "__main__": main()
# We'll define a clean function instead

from playwright.sync_api import sync_playwright
import os
import glob
import time
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import pytz as pytz_lib  # avoid conflict with pytz import above

load_dotenv()

# ================= CONFIGURATION =================
USERNAME = "rusvin@xpresshealth.ie"
PASSWORD = "Rusvin@123"

LOGIN_URL = "https://phase-three.xpresshealthapp.ie/login"
TARGET_URL = "https://phase-three.xpresshealthapp.ie/recruitment/all?start_date=2025-05-14&end_date=2026-05-14&page=1&per_page=50"

DOWNLOAD_FOLDER = "recruitment_downloads"
EXPORT_BUTTON_CLASS = "btn btn-outlined md flex items-center rounded-md gap-x-md justify-center transition-all font-medium relative pointer-events-all"

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
IS_HEADLESS = not os.getenv("DISPLAY", None)

# Use app.db from Flask context instead of creating new client
def get_db():
    return current_app.db

# =================== ALL YOUR FUNCTIONS (unchanged) ===================
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
                time.sleep(4)

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

    db = get_db()
    users_collection = db['users']

    print(f"\nProcessing file: {os.path.basename(file_path)}")
    df = pd.read_excel(file_path)

    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    print(f"Columns found: {list(df.columns)}")

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

    if not mapped['email']:
        print("No email column found. Cannot proceed.")
        return

    df = df.head(10).copy()
    print(f"Taking only the latest 10 records from Excel.")

    inserted = skipped_no_phone = skipped_not_irish = skipped_exists = skipped_blocked_role = 0
    now_utc = datetime.utcnow().replace(tzinfo=pytz_lib.UTC)
    BLOCKED_DESIGNATIONS = {"admin assistant", "support worker"}

    for idx, row in df.iterrows():
        raw_email = row.get(mapped['email'], '')
        email = str(raw_email).strip().lower() if pd.notna(raw_email) else ""
        if not email or '@' not in email:
            skipped_no_phone += 1
            continue

        if users_collection.find_one({"email": email}):
            skipped_exists += 1
            continue

        raw_phone = row.get(mapped['phone'], '') if mapped['phone'] else ''
        phone_str = str(raw_phone).strip().replace(" ", "").replace("-", "")
        if not phone_str.startswith('+353'):
            skipped_not_irish += 1
            continue

        raw_designation = row.get(mapped['designation'], "") if mapped['designation'] else ""
        designation_clean = str(raw_designation).strip().lower()
        if any(blocked in designation_clean for blocked in BLOCKED_DESIGNATIONS):
            skipped_blocked_role += 1
            continue

        user_doc = {
            "email": email,
            "first_name": str(row.get(mapped['first_name'], "") or "").strip() or "Unknown",
            "last_name": str(row.get(mapped['last_name'], "") or "").strip() or " ",
            "phone": phone_str,
            "designation": str(raw_designation).strip() or "unknown",
            "country": str(row.get(mapped['country'], "") or "Ireland").strip() or "Ireland",
            "call_sent": 1,
            "created_at": now_utc.isoformat(),
            "updated_at": now_utc.isoformat(),
        }

        try:
            users_collection.insert_one(user_doc)
            print(f"Inserted → {email} | {phone_str}")
            inserted += 1
        except Exception as e:
            print(f"Failed to insert {email}: {e}")
            skipped_exists += 1

    print("\n" + "="*60)
    print(f"SUMMARY: Inserted={inserted} | Exists={skipped_exists} | Not Irish={skipped_not_irish} | Blocked Role={skipped_blocked_role}")
    print("="*60)

# =================== MAIN JOB FUNCTION ===================
def run_recruitment_job():
    latest_file = download_excel()
    if not latest_file:
        print("Download failed.")
        return {"error": "Download failed"}

    print("\nCleaning up old export files...")
    delete_old_exports(latest_file)

    clean_and_insert_only_new_irish_users(latest_file)
    return {"success": True, "file": latest_file}

# =================== FLASK ROUTE REGISTRATION ===================
def register_scrap_users_route(app):
    @app.route('/api/scrape-recruitment', methods=['GET', 'POST'])
    def scrape_recruitment():
        api_key = request.args.get('key') or request.headers.get('X-API-Key')
        expected_key = os.getenv('SCRAPER_API_KEY')  # Set in .env

        if not expected_key:
            return jsonify({"error": "Server not configured"}), 500
        if api_key != expected_key:
            return jsonify({"error": "Unauthorized"}), 401

        def job():
            with app.app_context():
                print(f"[{datetime.now(pytz.timezone('Europe/Dublin'))}] Recruitment scraper started via API")

                # Full path to your Python script
                script_path = "/home/dev_xpresshealth/recruitassist_project/scrap_users_v1.py"

                # Optional: Use specific Python interpreter (recommended)
                python_interpreter = "/home/dev_xpresshealth/recruitassist_project/venv/bin/python"  # or "/home/dev_xpresshealth/venv/bin/python"

                try:
                    # Run the script in background
                    result = subprocess.Popen([
                        python_interpreter, script_path
                    ])
                    print(f"Started scrap_users_v1.py with PID: {result.pid}")

                except Exception as e:
                    print(f"Failed to start scrap_users_v1.py: {e}")

        # Start the job in a background thread
        threading.Thread(target=job, daemon=True).start()

        return jsonify({
            "success": True,
            "message": "scrap_users_v1.py started in background",
            "timestamp": datetime.now(pytz.UTC).isoformat()
        })

        #https://yourdomain.com/api/scrape-recruitment?key=your_very_strong_secret_key_here_2025