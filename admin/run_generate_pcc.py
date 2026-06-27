"""
run_generate_pcc.py
────────────────────
Calls the PCC generate cron endpoint — one staff per call.
Each PCC is generated, uploaded to GCS, and pushed to the
outreach API as hse_document_type=others_2.

To regenerate ALL PCCs, run in MongoDB first:
    db.live_staffs.updateMany({}, {$set: {pcc_generated: false}})
    db.live_staff_ai_pcc.deleteMany({})

Usage:
    python run_generate_pcc.py
"""

import time
import requests
from datetime import datetime

URL        = "https://uat.expresshealth.ie/admin/live-staffs/cron/generate-pcc"
CRON_KEY   = "abcdefgqwert"
INTERVAL   = 15    # seconds between calls
ERROR_WAIT = 60    # seconds to wait after server errors


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def call_cron():
    try:
        resp = requests.get(URL, params={"cron_key": CRON_KEY}, timeout=120)
        if resp.status_code == 500:
            log(f"   WARNING: Server 500 — waiting {ERROR_WAIT}s")
            return "server_error"
        if resp.status_code != 200:
            log(f"   HTTP {resp.status_code}: {resp.text[:300]}")
            return None
        text = resp.text.strip()
        if not text:
            log("   Empty response")
            return None
        try:
            return resp.json()
        except Exception:
            log(f"   Non-JSON: {text[:300]}")
            return None
    except requests.exceptions.Timeout:
        log("⚠️  Timed out — retrying")
        return None
    except Exception as e:
        log(f"❌  {e}")
        return None


def main():
    log("🚀  PCC Generation runner started")
    log(f"    URL        : {URL}")
    log(f"    Interval   : every {INTERVAL} seconds")
    log(f"    Outreach   : hse_document_type = others_2")
    log("─" * 60)
    log("ℹ️   To regenerate ALL PCCs run in MongoDB first:")
    log("      db.live_staffs.updateMany({}, {$set: {pcc_generated: false}})")
    log("      db.live_staff_ai_pcc.deleteMany({})")
    log("─" * 60)

    run = processed = 0
    while True:
        run += 1
        log(f"▶  Run #{run}")

        result = call_cron()

        if result == "server_error":
            time.sleep(ERROR_WAIT)
            continue

        if result is None:
            log(f"   Waiting {INTERVAL}s...")
            time.sleep(INTERVAL)
            continue

        success   = result.get("success")
        remaining = result.get("remaining_count", "?")
        name      = result.get("staff_name", "")
        email     = result.get("email", "")
        reviewer  = result.get("reviewer", "")
        filename  = result.get("filename", "")
        error     = result.get("error", "")
        message   = result.get("message", "")

        if not success:
            log(f"   ❌  {error or message or 'unknown error'}")
            if email:
                log(f"   📧  {email}")
            log(f"   Waiting {INTERVAL}s...")
            time.sleep(INTERVAL)
            continue

        if remaining == 0 and not name:
            log("✅  All PCCs generated. Exiting.")
            break

        processed += 1
        log(f"   👤  {name} ({email})")
        if filename:
            log(f"   📄  {filename}")
        if reviewer:
            log(f"   👩  Reviewer: {reviewer}")
        log(f"   📤  Pushed to outreach API as others_2")
        log(f"   📊  Remaining: {remaining}")

        if remaining == 0:
            log(f"✅  Done — {processed} PCCs generated and uploaded.")
            break

        log(f"   Waiting {INTERVAL}s...")
        time.sleep(INTERVAL)

    log("─" * 60)
    log("🏁  Done.")


if __name__ == "__main__":
    main()
