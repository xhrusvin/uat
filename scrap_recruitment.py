# scrap_recruitment.py
import threading
import os
from datetime import datetime
import pytz
from your_main_script_file import main as run_recruitment_scraper  # <-- Change this!
# OR paste the entire script content here and rename main() → run_recruitment_job()

def trigger_recruitment_scrape(background=True):
    def job():
        print(f"[{datetime.now(pytz.timezone('Europe/Dublin'))}] Starting recruitment scrape...")
        try:
            run_recruitment_scraper()  # This is your full original main() function
            print("Recruitment scrape completed successfully!")
        except Exception as e:
            print(f"Recruitment scrape FAILED: {e}")

    if background:
        thread = threading.Thread(target=job, daemon=True)
        thread.start()
        return {"status": "started", "message": "Recruitment scraper started in background"}
    else:
        job()
        return {"status": "completed", "message": "Recruitment scraper finished"}