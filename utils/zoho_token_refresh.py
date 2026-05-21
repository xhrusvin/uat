"""
utils/zoho_token_refresh.py

Keeps ZOHO_ACCESS_TOKEN fresh in Flask app.config.
Access tokens expire after ~1 hour; this refreshes every 50 minutes.

Usage (pick ONE approach):

  A) APScheduler (recommended – runs inside your Flask process)
     ─────────────────────────────────────────────────────────
     In your create_app() / app factory:

         from utils.zoho_token_refresh import start_token_scheduler
         start_token_scheduler(app)

  B) System cron (runs outside Flask)
     ──────────────────────────────────
     crontab -e
     Add:   */50 * * * * /path/to/venv/bin/python /path/to/project/utils/zoho_token_refresh.py

     The script writes the new token to a file that Flask reads on each request.
     Set TOKEN_FILE_PATH below.
"""

import os
import json
import logging
import requests
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────
ZOHO_TOKEN_URL  = "https://accounts.zoho.eu/oauth/v2/token"  # EU DC
TOKEN_FILE_PATH = Path("/tmp/zoho_access_token.json")         # used by cron mode
REFRESH_INTERVAL_MINUTES = 50                                  # tokens live ~60 min
# ───────────────────────────────────────────────────────────────────────────


def fetch_new_token() -> str:
    """
    Exchange the stored refresh_token for a fresh access_token.
    Raises on any HTTP or JSON error.
    """
    resp = requests.post(
        ZOHO_TOKEN_URL,
        data={
            "grant_type":    "refresh_token",
            "client_id":     os.environ["ZOHO_CLIENT_ID"],
            "client_secret": os.environ["ZOHO_CLIENT_SECRET"],
            "refresh_token": os.environ["ZOHO_REFRESH_TOKEN"],
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()

    if "access_token" not in data:
        raise ValueError(f"Zoho token refresh failed: {data}")

    log.info("Zoho access_token refreshed at %s", datetime.utcnow().isoformat())
    return data["access_token"]


# ── Approach A: APScheduler (in-process) ───────────────────────────────────

def start_token_scheduler(app):
    """
    Call this once inside create_app().
    Requires:  pip install apscheduler
    """
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except ImportError:
        log.warning(
            "APScheduler not installed. Run: pip install apscheduler\n"
            "Falling back to file-based token refresh."
        )
        return

    def _refresh_and_store():
        try:
            token = fetch_new_token()
            with app.app_context():
                app.config["ZOHO_ACCESS_TOKEN"] = token
            # Also persist to file so a process restart doesn't start with a stale token
            TOKEN_FILE_PATH.write_text(json.dumps({
                "access_token": token,
                "refreshed_at": datetime.utcnow().isoformat(),
            }))
        except Exception as exc:
            log.error("Zoho token refresh error: %s", exc)

    # Do an immediate refresh on startup so the token is never empty
    _refresh_and_store()

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        _refresh_and_store,
        trigger="interval",
        minutes=REFRESH_INTERVAL_MINUTES,
        id="zoho_token_refresh",
        replace_existing=True,
    )
    scheduler.start()
    log.info("Zoho token scheduler started (every %d min)", REFRESH_INTERVAL_MINUTES)


# ── Approach B: read token from file (used when running via system cron) ───

def load_token_from_file() -> str | None:
    """
    Call this in your _mail_client() helper if you're using cron mode:

        token = load_token_from_file() or current_app.config.get("ZOHO_ACCESS_TOKEN", "")
    """
    try:
        data = json.loads(TOKEN_FILE_PATH.read_text())
        return data.get("access_token")
    except Exception:
        return None


# ── Approach B: standalone script entry point (called by cron directly) ────

if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # Load .env if python-dotenv is available
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    try:
        token = fetch_new_token()
        TOKEN_FILE_PATH.write_text(json.dumps({
            "access_token": token,
            "refreshed_at": datetime.utcnow().isoformat(),
        }))
        print(f"[OK] Token saved to {TOKEN_FILE_PATH}")
        sys.exit(0)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)