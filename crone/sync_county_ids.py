#!/usr/bin/env python3
"""
sync_county_ids.py
Finds all users missing county_id and syncs them via /xnapi/recruitments/detail
Usage: python3 sync_county_ids.py
Cron:  0 3 * * * python3 /home/dev_xpresshealth/uat/sync_county_ids.py >> /home/dev_xpresshealth/uat/sync_county_ids.log 2>&1
SYNC_FROM=0 SYNC_TO=200 python3 sync_county_ids.py
SYNC_FROM=200 SYNC_TO=400 python3 sync_county_ids.py
SYNC_FROM=400 SYNC_TO=600 python3 sync_county_ids.py
"""
import os
import time
import logging
from datetime import datetime
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Config
MONGO_URI    = os.getenv("MONGODB_URI", "mongodb://127.0.0.1:27017")
MONGO_DB     = os.getenv("MONGODB_DB", "xpress_health")
API_BASE_URL = os.getenv("APP_BASE_URL", "https://uat.expresshealth.ie")
API_KEY      = os.getenv("API_KEY", "xh-uat-9f4a2c8b1d6e3f7a0b5c9d2e4f8a1b3c")
BATCH_DELAY  = float(os.getenv("SYNC_DELAY", "0.3"))  # seconds between calls
BATCH_FROM   = int(os.getenv("SYNC_FROM", "200"))      # start offset
BATCH_TO     = int(os.getenv("SYNC_TO", "400"))        # end offset

def run():
    client = MongoClient(MONGO_URI)
    db     = client[MONGO_DB]

    # Find users missing county_id
    query = {
        "county_id": {"$exists": False},
        "xn_user_id": {"$exists": True, "$ne": None, "$ne": ""},
        "status": "Enabled",
    }
    total = db["users"].count_documents(query)
    log.info(f"Found {total} users missing county_id — processing range {BATCH_FROM}–{BATCH_TO}")

    batch_size = BATCH_TO - BATCH_FROM
    log.info(f"Processing range {BATCH_FROM}–{BATCH_TO} ({batch_size} users)")
    users = list(db["users"].find(query, {"_id": 1, "xn_user_id": 1, "first_name": 1, "last_name": 1}).skip(BATCH_FROM).limit(batch_size))

    import requests as _req

    synced    = 0
    failed    = 0
    skipped   = 0

    for u in users:
        xn_user_id = str(u.get("xn_user_id", ""))
        name       = f"{u.get('first_name','')} {u.get('last_name','')}".strip()

        if not xn_user_id:
            skipped += 1
            continue

        try:
            resp = _req.post(
                f"{API_BASE_URL}/xnapi/recruitments/detail",
                json={"xn_user_id": xn_user_id},
                headers={
                    "Authorization": f"Bearer {API_KEY}",
                    "Content-Type":  "application/json",
                },
                timeout=15,
            )

            if resp.status_code == 200:
                data = resp.json()
                if data.get("success"):
                    synced += 1
                    log.info(f"✓ Synced {name} ({xn_user_id})")
                else:
                    failed += 1
                    log.warning(f"✗ Failed {name} ({xn_user_id}): {data.get('message','')}")
            else:
                failed += 1
                log.warning(f"✗ HTTP {resp.status_code} for {name} ({xn_user_id})")

        except Exception as e:
            failed += 1
            log.error(f"✗ Exception for {name} ({xn_user_id}): {e}")

        time.sleep(BATCH_DELAY)

    client.close()
    log.info(f"Done — synced={synced} failed={failed} skipped={skipped} / total_missing={total}")

if __name__ == "__main__":
    log.info(f"=== sync_county_ids.py started at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')} ===")
    run()
