"""
Preferred Contact Method campaign over Email.

Flow
────
1. Admin (or a cron job) hits POST /admin/email/preferred_contact/send_batch
   with {"limit": 25}.  N users are *atomically claimed* from db.users and
   sent an HTML email with three clickable buttons:

       ✅ Call        ✅ WhatsApp       ✅ Email

2. The user clicks a button in the email, which hits:
   GET /admin/email/preferred_contact/respond/<token>?answer=<1|2|3|all>

3. The handler parses the answer, writes preferred_contact: [<codes>] to the
   user document, and returns a confirmation HTML page.

   Multiple preferences are supported via a comma-separated `answer` param:
       ?answer=1,3   →  preferred_contact: [1, 3]

Bookkeeping lives on the user doc under `preferred_contact_email_prompt`:

    preferred_contact_email_prompt: {
        status:         "sending" | "sent" | "failed" | "answered",
        attempts:       1,
        token:          "<uuid4 hex>",       # one-time URL token
        claimed_at:     ISODate,
        last_sent_at:   ISODate,
        last_error:     "…",
        answered_at:    ISODate,
        raw_reply:      "1,3",
    }

Env vars
────────
    # SMTP — reuse the same vars as shift_booking_email.py where possible
    SHIFT_SMTP_HOST                      default "smtp.gmail.com"
    SHIFT_SMTP_PORT                      default 587
    SHIFT_SMTP_USER
    SHIFT_SMTP_PASSWORD
    SHIFT_FROM_EMAIL
    SHIFT_SMTP_FROM_NAME                 default "XpressHealth"

    APP_BASE_URL                         default "https://uat.expresshealth.ie"

    # Campaign knobs
    EMAIL_PC_BATCH_SIZE                  default 25
    EMAIL_PC_SEND_DELAY_MS               default 400
    EMAIL_PC_MAX_ATTEMPTS                default 2
    EMAIL_PC_RESEND_HOURS                default 72
    EMAIL_PC_BATCH_CAP                   default 500

    # Automation
    EMAIL_PC_AUTOMATION_ENABLED          default false  ← master switch, off
    EMAIL_PC_HOURLY_CAP                  default 25
    EMAIL_PC_TICK_MINUTES                default 10
    EMAIL_PC_HOURLY_CAP_CEILING          default 500
    EMAIL_PC_LOCK_TTL                    default 600

    # Test / safety
    EMAIL_PC_TEST_MODE                   default false
    EMAIL_PC_TEST_EMAILS                 comma-separated whitelist
    EMAIL_PC_TEST_REDIRECT               redirect all test sends to this address

Recommended indexes
───────────────────
    db.users.create_index([("preferred_contact_email_prompt.token", 1)],
                          unique=True, sparse=True)
    db.users.create_index([("preferred_contact", 1),
                           ("preferred_contact_email_prompt.status", 1)])
"""

import os
import re
import uuid
import time
import smtplib
import threading
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from flask import jsonify, request
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from database import db
from . import admin_bp
from admin.views import admin_required


# ── Configuration ─────────────────────────────────────────────────────────────

BATCH_SIZE      = int(os.environ.get("EMAIL_PC_BATCH_SIZE", 25))
SEND_DELAY_MS   = int(os.environ.get("EMAIL_PC_SEND_DELAY_MS", 400))
MAX_ATTEMPTS    = int(os.environ.get("EMAIL_PC_MAX_ATTEMPTS", 2))
RESEND_HOURS    = int(os.environ.get("EMAIL_PC_RESEND_HOURS", 72))
BATCH_HARD_CAP  = int(os.environ.get("EMAIL_PC_BATCH_CAP", 500))

BASE_URL        = os.environ.get("APP_BASE_URL", "https://uat.expresshealth.ie")

SMTP_HOST       = os.environ.get("SHIFT_SMTP_HOST", "smtp.gmail.com")
SMTP_PORT       = int(os.environ.get("SHIFT_SMTP_PORT", 587))
SMTP_USER       = os.environ.get("SHIFT_SMTP_USER", "")
SMTP_PASS       = os.environ.get("SHIFT_SMTP_PASSWORD", "")
FROM_EMAIL      = os.environ.get("SHIFT_FROM_EMAIL", "")
FROM_NAME       = os.environ.get("SHIFT_SMTP_FROM_NAME", "XpressHealth")

CONTACT_METHODS = {1: "Email", 2: "Call", 3: "WhatsApp", 4: "SMS"}

# ── Test mode ─────────────────────────────────────────────────────────────────

TEST_MODE      = os.environ.get("EMAIL_PC_TEST_MODE", "").lower() in ("1", "true", "yes")
TEST_EMAILS    = [e.strip().lower() for e in
                  os.environ.get("EMAIL_PC_TEST_EMAILS", "").split(",") if e.strip()]
TEST_REDIRECT  = os.environ.get("EMAIL_PC_TEST_REDIRECT", "").strip()

# ── Automation ────────────────────────────────────────────────────────────────

def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


AUTOMATION_ENABLED    = _env_bool("EMAIL_PC_AUTOMATION_ENABLED", False)
AUTOMATION_SETTINGS_ID = "email_pc_automation"
TICK_MINUTES          = int(os.environ.get("EMAIL_PC_TICK_MINUTES", 10))
DEFAULT_HOURLY_CAP    = int(os.environ.get("EMAIL_PC_HOURLY_CAP", BATCH_SIZE))
HOURLY_CAP_CEILING    = int(os.environ.get("EMAIL_PC_HOURLY_CAP_CEILING", 500))
LOCK_TTL_SECONDS      = int(os.environ.get("EMAIL_PC_LOCK_TTL", 600))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now():
    return datetime.now(timezone.utc)


def _iso(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


def _users_col():
    return db.users


def _settings_col():
    return db.settings


def _sends_col():
    """
    One document per outbound email prompt — audit trail + hourly cap counter.
    TTL index suggestion:
        db.email_pc_sends.create_index([("sent_at", 1)], expireAfterSeconds=604800)
    """
    return db.email_pc_sends


def _locks_col():
    return db.locks


# ── User lookup ───────────────────────────────────────────────────────────────

def find_user_by_email(email: str):
    email = (email or "").strip()
    if not email:
        return None
    return _users_col().find_one(
        {"email": re.compile(rf"^{re.escape(email)}$", re.IGNORECASE)}
    )


def find_user_by_token(token: str):
    """Resolve the one-time response token back to a user."""
    if not token:
        return None
    return _users_col().find_one({"preferred_contact_email_prompt.token": token})


def reset_prompt_state(user_id):
    """Clear answer + bookkeeping. Test-only — never call from the batch path."""
    _users_col().update_one(
        {"_id": user_id},
        {"$unset": {"preferred_contact": "", "preferred_contact_email_prompt": ""}},
    )


# ── Preference parsing ────────────────────────────────────────────────────────

_KEYWORDS = {
    "1": 1, "one": 1, "email": 1, "e mail": 1, "mail": 1, "gmail": 1,
    "2": 2, "two": 2, "call": 2, "phone": 2, "phone call": 2, "voice": 2,
    "telephone": 2, "ring": 2,
    "3": 3, "three": 3, "whatsapp": 3, "whats app": 3, "wa": 3, "wtsp": 3,
    "chat": 3,
    "4": 4, "four": 4, "sms": 4, "text": 4, "text message": 4,
}


def _clean(text: str) -> str:
    text = (text or "").lower().strip()
    return re.sub(r"[^a-z0-9,]+", " ", text).strip()


def parse_preference(text: str) -> list[int]:
    """
    Turn a URL param or free-text reply into a sorted list of contact codes.

        "2"           -> [2]
        "1,3"         -> [1, 3]
        "whatsapp"    -> [2]
        "all"         -> [1, 2, 3]
        "garbage"     -> []
    """
    cleaned = _clean(text)
    if not cleaned:
        return []

    if cleaned in ("all", "any", "anything", "all of them", "any of them"):
        return [1, 2, 3]

    codes: set[int] = set()

    # Bare digits (handles "1,3", "option 2", etc.)
    for token in re.findall(r"\d", cleaned):
        if token in ("1", "2", "3"):
            codes.add(int(token))

    # Keyword matches — longest phrases first
    for phrase in sorted(_KEYWORDS, key=len, reverse=True):
        if phrase.isdigit():
            continue
        if re.search(rf"\b{re.escape(phrase)}\b", cleaned):
            codes.add(_KEYWORDS[phrase])

    return sorted(codes)


# ── Automation state ──────────────────────────────────────────────────────────

def get_automation_settings() -> dict:
    doc = _settings_col().find_one({"_id": AUTOMATION_SETTINGS_ID}) or {}
    runtime_enabled = bool(doc.get("enabled", True))
    return {
        "enabled":         AUTOMATION_ENABLED and runtime_enabled,
        "env_enabled":     AUTOMATION_ENABLED,
        "runtime_enabled": runtime_enabled,
        "hourly_cap":      int(doc.get("hourly_cap") or DEFAULT_HOURLY_CAP),
        "updated_at":      doc.get("updated_at"),
        "updated_by":      doc.get("updated_by"),
        "last_tick":       doc.get("last_tick"),
    }


def set_automation_settings(enabled=None, hourly_cap=None, actor=None) -> dict:
    update = {"updated_at": _now()}
    if enabled is not None:
        update["enabled"] = bool(enabled)
    if hourly_cap is not None:
        update["hourly_cap"] = max(1, min(int(hourly_cap), HOURLY_CAP_CEILING))
    if actor:
        update["updated_by"] = actor
    _settings_col().update_one({"_id": AUTOMATION_SETTINGS_ID},
                                {"$set": update}, upsert=True)
    return get_automation_settings()


def _log_send(user_id, email, source):
    try:
        _sends_col().insert_one({
            "user_id": user_id,
            "email":   email,
            "source":  source,
            "sent_at": _now(),
        })
    except Exception:
        pass


def sent_last_hour(source=None) -> int:
    query = {"sent_at": {"$gte": _now() - timedelta(hours=1)}}
    if source:
        query["source"] = source
    return _sends_col().count_documents(query)


def _acquire_tick_lock() -> bool:
    now = _now()
    try:
        _locks_col().update_one(
            {"_id": "email_pc_tick",
             "$or": [{"expires_at": {"$lt": now}}, {"expires_at": None}]},
            {"$set": {"expires_at": now + timedelta(seconds=LOCK_TTL_SECONDS),
                      "acquired_at": now}},
            upsert=True,
        )
        return True
    except DuplicateKeyError:
        return False


def _release_tick_lock():
    try:
        _locks_col().update_one({"_id": "email_pc_tick"},
                                {"$set": {"expires_at": _now()}})
    except Exception:
        pass


def automation_tick(source="scheduler") -> dict:
    if not AUTOMATION_ENABLED:
        return {"ran": False, "sent_count": 0,
                "reason": "automation disabled in .env (EMAIL_PC_AUTOMATION_ENABLED)"}

    settings  = get_automation_settings()
    if not settings["enabled"]:
        return {"ran": False, "sent_count": 0, "reason": "automation paused from admin"}

    used      = sent_last_hour()
    remaining = settings["hourly_cap"] - used
    if remaining <= 0:
        return {"ran": False, "reason": "hourly cap reached", "sent_count": 0,
                "sent_last_hour": used, "hourly_cap": settings["hourly_cap"]}

    if not _acquire_tick_lock():
        return {"ran": False, "reason": "another tick in progress", "sent_count": 0}

    try:
        result = run_batch(remaining, source="automation")
    finally:
        _release_tick_lock()

    _settings_col().update_one(
        {"_id": AUTOMATION_SETTINGS_ID},
        {"$set": {"last_tick": {"at": _now(), "trigger": source,
                                "sent": result["sent_count"],
                                "failed": result["failed_count"]}}},
        upsert=True,
    )

    return {"ran": True, "trigger": source, "allowance_this_tick": remaining,
            "sent_last_hour_before": used, "hourly_cap": settings["hourly_cap"],
            **result}


# ── Email building ────────────────────────────────────────────────────────────

def _build_email_html(first_name: str, token: str) -> tuple[str, str]:
    """
    Returns (subject, html_body).

    Response URLs embed the one-time token so no login is required:
        GET /admin/email/preferred_contact/respond/<token>?answer=1
        GET /admin/email/preferred_contact/respond/<token>?answer=2
        GET /admin/email/preferred_contact/respond/<token>?answer=3
        GET /admin/email/preferred_contact/respond/<token>?answer=1,3  (multi)
    """
    base        = BASE_URL.rstrip("/")
    respond_url = f"{base}/admin/email/preferred_contact/respond/{token}"

    email_url     = f"{respond_url}?answer=1"
    call_url      = f"{respond_url}?answer=2"
    whatsapp_url  = f"{respond_url}?answer=3"
    sms_url       = f"{respond_url}?answer=4"

    logo_url = f"{base}/static/image/logo.png"
    subject  = "How would you like us to contact you?"

    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Inter,Arial,sans-serif;">
<div style="max-width:520px;margin:0 auto;">

  <!-- Top bar -->
  <div style="height:5px;background:linear-gradient(90deg,#016ab2 0%,#009540 100%);"></div>

  <!-- Card -->
  <div style="background:#ffffff;padding:32px 28px;">

    <!-- Logo -->
    <img src="{logo_url}" alt="Xpress Health" width="130"
         style="display:block;margin-bottom:20px;">
    <hr style="border:none;border-top:1px solid #e5e7eb;margin:0 0 24px;">

    <!-- Greeting -->
    <p style="font-size:15px;color:#111827;margin:0 0 12px;">
      Hi <strong>{first_name}</strong>,
    </p>
    <p style="font-size:14px;color:#374151;margin:0 0 8px;line-height:1.6;">
      We'd like to know how you prefer to receive shift availability requests
      from Xpress Health.
    </p>
    <p style="font-size:14px;color:#374151;margin:0 0 20px;line-height:1.6;">
      Please select your preferred option below:
    </p>

    <!-- Option buttons -->
    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:20px;">
      <tr>
        <td align="center" style="padding:6px;">
          <a href="{email_url}"
             style="display:inline-block;width:100%;max-width:360px;
                    background:#f59e0b;color:#ffffff;
                    padding:14px 0;border-radius:8px;
                    text-decoration:none;font-size:15px;font-weight:700;
                    text-align:center;">
            📧&nbsp;&nbsp;Email
          </a>
        </td>
      </tr>
      <tr>
        <td align="center" style="padding:6px;">
          <a href="{call_url}"
             style="display:inline-block;width:100%;max-width:360px;
                    background:#016ab2;color:#ffffff;
                    padding:14px 0;border-radius:8px;
                    text-decoration:none;font-size:15px;font-weight:700;
                    text-align:center;">
            📞&nbsp;&nbsp;Phone Call
          </a>
        </td>
      </tr>
      <tr>
        <td align="center" style="padding:6px;">
          <a href="{whatsapp_url}"
             style="display:inline-block;width:100%;max-width:360px;
                    background:#25d366;color:#ffffff;
                    padding:14px 0;border-radius:8px;
                    text-decoration:none;font-size:15px;font-weight:700;
                    text-align:center;">
            💬&nbsp;&nbsp;WhatsApp
          </a>
        </td>
      </tr>
      <tr>
        <td align="center" style="padding:6px;">
          <a href="{sms_url}"
             style="display:inline-block;width:100%;max-width:360px;
                    background:#6b7280;color:#ffffff;
                    padding:14px 0;border-radius:8px;
                    text-decoration:none;font-size:15px;font-weight:700;
                    text-align:center;">
            📱&nbsp;&nbsp;SMS
          </a>
        </td>
      </tr>
    </table>

    <p style="font-size:14px;color:#374151;text-align:center;margin:0 0 16px;">
      This will help us contact you about available shifts in the way that
      suits you best.
    </p>
    <p style="font-size:14px;color:#374151;text-align:center;margin:0 0 4px;">
      Thank you! 😊
    </p>
    <p style="font-size:13px;color:#6b7280;text-align:center;margin:0;">
      Xpress Health Team
    </p>
  </div>

  <!-- Bottom bar -->
  <div style="height:5px;background:linear-gradient(90deg,#016ab2 0%,#009540 100%);"></div>
  <div style="background:#f9fafb;text-align:center;padding:16px;
              font-size:11px;color:#9ca3af;">
    © {_now().year} Xpress Health
  </div>

</div>
</body>
</html>"""

    return subject, html


# ── SMTP send ─────────────────────────────────────────────────────────────────

def _smtp_send(to_email: str, subject: str, html: str) -> None:
    """Low-level SMTP send — raises on failure."""
    msg             = MIMEMultipart("alternative")
    msg["Subject"]  = subject
    msg["From"]     = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"]       = to_email
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(FROM_EMAIL, [to_email], msg.as_string())


# ── Sending ───────────────────────────────────────────────────────────────────

def _claim_next_user(cutoff, stale_cutoff):
    """
    Atomically reserve one un-answered user. Sets status='sending' and bumps
    attempts so parallel runs can't double-send.
    """
    no_preference = {
        "$or": [
            {"preferred_contact": {"$exists": False}},
            {"preferred_contact": None},
            {"preferred_contact": []},
        ]
    }
    needs_prompt = {
        "$or": [
            {"preferred_contact_email_prompt": {"$exists": False}},
            {"preferred_contact_email_prompt.status": {"$exists": False}},
            {
                "preferred_contact_email_prompt.status": "failed",
                "preferred_contact_email_prompt.attempts": {"$lt": MAX_ATTEMPTS},
            },
            {
                "preferred_contact_email_prompt.status": "sent",
                "preferred_contact_email_prompt.attempts": {"$lt": MAX_ATTEMPTS},
                "preferred_contact_email_prompt.last_sent_at": {"$lt": cutoff},
            },
            # Reclaim stale 'sending' records from crashed workers.
            {
                "preferred_contact_email_prompt.status": "sending",
                "preferred_contact_email_prompt.claimed_at": {"$lt": stale_cutoff},
            },
        ]
    }

    # Exclude users who already set their preference through another channel
    # (WhatsApp, call, SMS, manual back-office). Only target genuinely unanswered users.
    already_answered_elsewhere = {
        "preferred_contact_channel": {"$in": ["whatsapp", "call", "sms", "manual"]},
        "preferred_contact":         {"$exists": True, "$nin": [[], None]},
    }

    query = {
        "is_active": True,
        "email":     {"$nin": [None, ""]},
        "$and":      [no_preference, needs_prompt],
        "$nor":      [already_answered_elsewhere],
    }

    if TEST_MODE:
        query["email"] = {
            "$in": [re.compile(rf"^{re.escape(e)}$", re.IGNORECASE)
                    for e in TEST_EMAILS]
        }

    return _users_col().find_one_and_update(
        query,
        {
            "$set": {
                "preferred_contact_email_prompt.status":     "sending",
                "preferred_contact_email_prompt.claimed_at": _now(),
            },
            "$inc": {"preferred_contact_email_prompt.attempts": 1},
        },
        projection={"_id": 1, "first_name": 1, "last_name": 1, "email": 1,
                    "preferred_contact_email_prompt": 1},
        sort=[("created_at", 1)],
        return_document=ReturnDocument.AFTER,
    )


def _release_user(user_id, status, error=None):
    update = {
        "preferred_contact_email_prompt.status":     status,
        "preferred_contact_email_prompt.last_error": error,
    }
    if status == "sent":
        update["preferred_contact_email_prompt.last_sent_at"] = _now()
    _users_col().update_one({"_id": user_id}, {"$set": update})


def send_prompt(user: dict, override_email: str = None,
                source: str = "manual") -> dict:
    """
    Send the preferred-contact prompt email to one user.

    A fresh one-time token is generated (or reused if the user was already
    claimed and a token exists) so the respond link always works.
    """
    to_email   = (override_email or user.get("email") or "").strip()
    first_name = (user.get("first_name") or "there").strip()

    if not to_email or "@" not in to_email:
        _release_user(user["_id"], "failed", "invalid email")
        return {"ok": False, "error": "invalid email", "email": to_email}

    # Re-use existing token so repeat sends don't break old links.
    existing = (user.get("preferred_contact_email_prompt") or {}).get("token")
    token    = existing or uuid.uuid4().hex

    # Persist token before we send so the respond endpoint can always look it up.
    _users_col().update_one(
        {"_id": user["_id"]},
        {"$set": {"preferred_contact_email_prompt.token": token}},
    )

    try:
        subject, html = _build_email_html(first_name, token)
        _smtp_send(to_email, subject, html)
    except Exception as exc:
        error = str(exc)
        _release_user(user["_id"], "failed", error)
        return {"ok": False, "error": error, "email": to_email}

    _release_user(user["_id"], "sent")
    _log_send(user["_id"], to_email, source)
    return {"ok": True, "email": to_email, "token": token}


# ── Batch running ─────────────────────────────────────────────────────────────

def run_batch(limit: int, dry_run: bool = False, source: str = "batch") -> dict:
    """Claim and email up to `limit` users. Safe to call from cron."""
    limit        = max(1, min(int(limit), BATCH_HARD_CAP))
    cutoff       = _now() - timedelta(hours=RESEND_HOURS)
    stale_cutoff = _now() - timedelta(minutes=15)

    sent, failed = [], []

    while len(sent) + len(failed) < limit:
        user = _claim_next_user(cutoff, stale_cutoff)
        if not user:
            break

        if dry_run:
            _users_col().update_one(
                {"_id": user["_id"]},
                {"$set": {"preferred_contact_email_prompt.status":
                          (user.get("preferred_contact_email_prompt") or {}).get("status")},
                 "$inc": {"preferred_contact_email_prompt.attempts": -1}},
            )
            sent.append({"user_id": str(user["_id"]),
                         "email": user.get("email"), "dry_run": True})
            continue

        result = send_prompt(
            user,
            override_email=TEST_REDIRECT if TEST_MODE else None,
            source=source,
        )
        entry = {
            "user_id": str(user["_id"]),
            "name":    f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
            "email":   user.get("email"),
        }
        if result["ok"]:
            sent.append(entry)
        else:
            entry["error"] = result["error"]
            failed.append(entry)

        if SEND_DELAY_MS:
            time.sleep(SEND_DELAY_MS / 1000.0)

    return {
        "requested":    limit,
        "sent_count":   len(sent),
        "failed_count": len(failed),
        "sent":         sent,
        "failed":       failed,
        "remaining":    pending_count(),
    }


def pending_count() -> int:
    """Users who still need an email prompt (no preference, and not already answered elsewhere)."""
    return _users_col().count_documents({
        "is_active": True,
        "email":     {"$nin": [None, ""]},
        "$or": [
            {"preferred_contact": {"$exists": False}},
            {"preferred_contact": None},
            {"preferred_contact": []},
        ],
        "$nor": [{
            "preferred_contact_channel": {"$in": ["whatsapp", "call", "sms", "manual"]},
            "preferred_contact":         {"$exists": True, "$nin": [[], None]},
        }],
    })


# ── Answer recording ──────────────────────────────────────────────────────────

def _record_answer(user, codes: list[int], raw_text: str, channel: str = "email"):
    """
    Write the user's preference. `channel` records *how* they responded:
        "email"     — clicked a button in the preferred-contact email
        "whatsapp"  — replied via WhatsApp (set by whatsapp_preferred_contact.py)
        "call"      — confirmed verbally / set by an agent
        "sms"       — replied by SMS
        "manual"    — set directly by admin (simulate_reply / back-office)
    """
    _users_col().update_one(
        {"_id": user["_id"]},
        {"$set": {
            "preferred_contact":                                    codes,
            "preferred_contact_channel":                            channel,
            "preferred_contact_email_prompt.status":               "answered",
            "preferred_contact_email_prompt.answered_at":          _now(),
            "preferred_contact_email_prompt.raw_reply":            raw_text,
            "preferred_contact_email_prompt.answered_via_channel": channel,
            "updated_at":                                          _now(),
        }},
    )


# ── Targeted dispatch ─────────────────────────────────────────────────────────

def dispatch_prompt(user, override_email=None, reset=False,
                    force=False, mark_test=False) -> dict:
    """Send the prompt to one specific user, bypassing the batch queue."""
    entry = {
        "user_id":      str(user["_id"]),
        "email":        user.get("email"),
        "name":         f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
        "email_on_file": user.get("email"),
    }

    if user.get("preferred_contact") and not (force or reset):
        entry.update({
            "ok":                False,
            "skipped":           "already answered",
            "preferred_contact": user.get("preferred_contact"),
            "hint":              'pass "force": true to re-ask anyway',
        })
        return entry

    target = override_email or user.get("email") or ""
    if not target or "@" not in target:
        entry.update({"ok": False, "error": 'no valid email address'})
        return entry

    if reset:
        reset_prompt_state(user["_id"])

    set_fields = {
        "preferred_contact_email_prompt.claimed_at": _now(),
        "preferred_contact_email_prompt.status":     "sending",
    }
    if mark_test:
        set_fields["preferred_contact_email_prompt.is_test"] = True

    _users_col().update_one(
        {"_id": user["_id"]},
        {"$inc": {"preferred_contact_email_prompt.attempts": 1},
         "$set": set_fields},
    )

    result = send_prompt(user,
                         override_email=override_email or None,
                         source="test" if mark_test else "manual")
    entry.update({
        "ok":        result["ok"],
        "sent_to":   result.get("email"),
        "redirected": bool(override_email),
        "was_reset": reset,
    })
    if not result["ok"]:
        entry["error"] = result.get("error")
    return entry


def resolve_targets(body) -> tuple[list, list]:
    """Accept email / emails[] / user_id / user_ids[]. Returns (users, missing)."""
    from bson import ObjectId
    from bson.errors import InvalidId

    identifiers = []
    for key in ("email", "emails", "user_id", "user_ids"):
        value = body.get(key)
        if isinstance(value, str) and value.strip():
            identifiers += [(key, v.strip()) for v in value.split(",") if v.strip()]
        elif isinstance(value, list):
            identifiers += [(key, str(v).strip()) for v in value if str(v).strip()]

    users, missing, seen = [], [], set()
    for kind, value in identifiers:
        user = None
        if kind.startswith("email"):
            user = find_user_by_email(value)
        else:
            try:
                user = _users_col().find_one({"_id": ObjectId(value)})
            except InvalidId:
                user = None

        if not user:
            missing.append(value)
        elif user["_id"] not in seen:
            seen.add(user["_id"])
            users.append(user)

    return users, missing


# ── Routes ────────────────────────────────────────────────────────────────────

@admin_bp.route("/email/preferred_contact/send_batch", methods=["GET", "POST"])
def email_pc_send_batch():
    """
    GET  /admin/email/preferred_contact/send_batch?limit=25&dry_run=0
    POST /admin/email/preferred_contact/send_batch
         Body (all optional): { "limit": 25, "dry_run": false }

    Public — no login required (designed to be hit by a cron job or scheduler).
    """
    body    = request.get_json(silent=True) or {}
    limit   = body.get("limit") or request.args.get("limit") or BATCH_SIZE
    dry_run = bool(body.get("dry_run") or request.args.get("dry_run"))

    try:
        limit = int(limit)
    except (TypeError, ValueError):
        return jsonify({"success": False, "error": "limit must be an integer"}), 400

    try:
        return jsonify({"success": True, **run_batch(limit, dry_run=dry_run)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route("/email/preferred_contact/send_to", methods=["POST"])
@admin_bp.route("/email/preferred_contact/send_one", methods=["POST"])
@admin_required
def email_pc_send_to():
    """
    POST /admin/email/preferred_contact/send_to

    Body — any one of:
        { "email":  "user@example.com" }
        { "emails": ["a@x.com", "b@y.com"] }
        { "user_id":  "…" }
        { "user_ids": […] }

    Optional:
        "email_override": "other@test.com"  — redirect send (single target only)
        "force": true                        — re-ask even if already answered
        "reset": true                        — wipe answer + history, then ask fresh
    """
    body     = request.get_json(silent=True) or {}
    override = (body.get("email_override") or "").strip()
    force    = bool(body.get("force"))
    reset    = bool(body.get("reset"))

    if not any(body.get(k) for k in ("email", "emails", "user_id", "user_ids")):
        if request.args.get("email"):
            body["email"] = request.args["email"]

    users, missing = resolve_targets(body)

    if not users and not missing:
        return jsonify({"success": False,
                        "error": "email, emails, user_id or user_ids is required"}), 400

    if override and len(users) > 1:
        return jsonify({"success": False,
                        "error": "email_override only works with a single target"}), 400

    results = []
    for index, user in enumerate(users):
        results.append(dispatch_prompt(
            user, override_email=override or None, reset=reset, force=force,
        ))
        if SEND_DELAY_MS and index < len(users) - 1:
            time.sleep(SEND_DELAY_MS / 1000.0)

    sent    = [r for r in results if r.get("ok")]
    skipped = [r for r in results if r.get("skipped")]
    failed  = [r for r in results if not r.get("ok") and not r.get("skipped")]

    payload = {
        "success":       bool(sent) or not (failed or missing),
        "sent_count":    len(sent),
        "skipped_count": len(skipped),
        "failed_count":  len(failed),
        "not_found":     missing,
        "results":       results,
    }
    status = 200 if sent or skipped else (404 if missing and not failed else 502)
    return jsonify(payload), status


@admin_bp.route("/email/preferred_contact/respond/<token>", methods=["GET"])
def email_pc_respond(token):
    """
    GET /admin/email/preferred_contact/respond/<token>?answer=<1|2|3|1,3|all>

    Public — no @admin_required. The token is the only gate.
    Renders a confirmation page and writes the preference to the DB.
    """
    answer_raw = request.args.get("answer", "").strip()
    codes      = parse_preference(answer_raw)

    user = find_user_by_token(token)
    if not user:
        return (
            "<html><body style='font-family:Arial;max-width:500px;margin:40px auto;text-align:center;'>"
            "<h2>&#x26A0;&#xFE0F; Link not found</h2>"
            "<p>This link may have expired or already been used. "
            "Please contact us if you need to update your preference.</p>"
            "</body></html>",
            404,
        )

    prompt = user.get("preferred_contact_email_prompt") or {}

    if not codes:
        # Unknown answer value — show the choice page again
        first_name = (user.get("first_name") or "there").strip()
        _, html = _build_email_html(first_name, token)
        return html, 200

    _record_answer(user, codes, answer_raw)

    labels = ", ".join(CONTACT_METHODS[c] for c in codes)
    first_name = (user.get("first_name") or "there").strip()

    logo_url = f"{BASE_URL.rstrip('/')}/static/image/logo.png"
    html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="margin:0;padding:0;background:#f3f4f6;font-family:Inter,Arial,sans-serif;">
<div style="max-width:500px;margin:0 auto;">
  <div style="height:5px;background:linear-gradient(90deg,#016ab2 0%,#009540 100%);"></div>
  <div style="background:#fff;padding:32px 28px;text-align:center;">
    <img src="{logo_url}" alt="Xpress Health" width="130"
         style="display:block;margin:0 auto 20px;">
    <div style="font-size:48px;margin-bottom:12px;">&#x2705;</div>
    <h2 style="color:#1e7a38;font-size:18px;margin:0 0 10px;">
      Preference saved, {first_name}!
    </h2>
    <p style="font-size:14px;color:#374151;margin:0 0 16px;">
      We'll send your shift availability requests via <strong>{labels}</strong>.
    </p>
    <p style="font-size:13px;color:#6b7280;margin:0;">
      You can change your preference at any time by emailing us at
      <a href="mailto:app@xpresshealth.ie"
         style="color:#016ab2;text-decoration:none;">app@xpresshealth.ie</a>.
    </p>
  </div>
  <div style="height:5px;background:linear-gradient(90deg,#016ab2 0%,#009540 100%);"></div>
  <div style="background:#f9fafb;text-align:center;padding:16px;
              font-size:11px;color:#9ca3af;">
    © {_now().year} Xpress Health
  </div>
</div>
</body>
</html>"""

    return html, 200


@admin_bp.route("/email/preferred_contact/test", methods=["GET"])
def email_pc_test():
    """
    GET /admin/email/preferred_contact/test?email=<user@example.com>

    Optional params:
        email_override=you@test.com   — redirect the send to a different inbox
        reset=1                       — wipe previous answer + history first
        inspect=1                     — only show stored state, don't send

    Without inspect=1 this always triggers a send (force=True).
    With inspect=1 it just returns whatever is currently stored for that user.

    Examples:
        /admin/email/preferred_contact/test?email=user@example.com
        /admin/email/preferred_contact/test?email=user@example.com&reset=1
        /admin/email/preferred_contact/test?email=user@example.com&email_override=you@test.com
        /admin/email/preferred_contact/test?email=user@example.com&inspect=1
    """
    email    = request.args.get("email", "").strip()
    override = (request.args.get("email_override") or TEST_REDIRECT or "").strip()
    do_reset = request.args.get("reset", "").lower() in ("1", "true", "yes")
    inspect  = request.args.get("inspect", "").lower() in ("1", "true", "yes")

    if not email:
        return jsonify({"success": False, "error": "email param is required"}), 400

    user = find_user_by_email(email)
    if not user:
        return jsonify({"success": False,
                        "error": f"no user with email {email}"}), 404

    # ── inspect-only mode ──────────────────────────────────────────────────────
    if inspect:
        prompt = dict(user.get("preferred_contact_email_prompt") or {})
        for key in ("claimed_at", "last_sent_at", "answered_at"):
            if key in prompt:
                prompt[key] = _iso(prompt[key])
        codes = user.get("preferred_contact") or []
        return jsonify({
            "success":                  True,
            "inspect":                  True,
            "email":                    user.get("email"),
            "user_id":                  str(user["_id"]),
            "preferred_contact":        codes,
            "preferred_contact_labels": [CONTACT_METHODS.get(c, c) for c in codes],
            "prompt":                   prompt,
            "test_mode":                TEST_MODE,
        })

    # ── send mode (default) ────────────────────────────────────────────────────
    result = dispatch_prompt(user, override_email=override or None,
                             reset=do_reset, force=True, mark_test=True)

    return jsonify({
        "success":  result["ok"],
        "test":     True,
        **result,
        "next_step": (
            f"Open your inbox ({override or email}) and click a preference button. "
            f"Then hit this endpoint with &inspect=1 to confirm the write-back landed."
        ),
    }), (200 if result["ok"] else 502)


@admin_bp.route("/email/preferred_contact/simulate_reply", methods=["POST"])
@admin_required
def email_pc_simulate_reply():
    """
    POST /admin/email/preferred_contact/simulate_reply
    Body: { "email": "…", "reply": "1,3", "commit": true }

    Dry-runs the parser and optionally writes the answer — no real email needed.
    """
    body   = request.get_json(silent=True) or {}
    email  = (body.get("email") or "").strip()
    reply  = body.get("reply") or ""
    commit = bool(body.get("commit"))

    if not email:
        return jsonify({"success": False, "error": "email is required"}), 400

    user = find_user_by_email(email)
    if not user:
        return jsonify({"success": False,
                        "error": f"no user with email {email}"}), 404

    codes = parse_preference(reply)
    if commit and codes:
        _record_answer(user, codes, reply, channel="manual")

    return jsonify({
        "success":   True,
        "simulated": True,
        "email":     user.get("email"),
        "user_id":   str(user["_id"]),
        "reply":     reply,
        "parsed":    codes,
        "labels":    [CONTACT_METHODS[c] for c in codes],
        "committed": bool(commit and codes),
        "note":      None if codes else "Reply not understood — no preference would be saved",
    })


@admin_bp.route("/email/preferred_contact/status", methods=["GET"])
def email_pc_status():
    """
    GET /admin/email/preferred_contact/status

    Public — no login required. Returns a full picture of campaign responses:

    {
      "total_responses":   45,          # everyone who has set a preference (any channel)
      "by_preference": {
        "Email":     12,
        "Call":       8,
        "WhatsApp":  20,
        "SMS":        5
      },
      "by_channel": {                   # how they told us their preference
        "email":     30,                # clicked a button in the email campaign
        "whatsapp":  10,                # replied via WhatsApp campaign
        "call":       3,                # confirmed verbally / set by agent
        "sms":        1,
        "manual":     1                 # set directly by admin
      },
      "email_campaign": {
        "emails_sent":       80,        # total outbound prompts sent (all time)
        "awaiting_reply":    35,        # sent but not yet answered
        "failed_to_send":     2,
        "pending_to_send":   15         # not yet contacted
      }
    }

    Note: by_preference counts are per-code, so a user who chose both Email + Call
    is counted once in each. Total may exceed total_responses for multi-choice users.
    """
    col = _users_col()

    # ── total responses (any channel) ─────────────────────────────────────────
    total_responses = col.count_documents({
        "preferred_contact": {"$exists": True, "$nin": [[], None]},
    })

    # ── breakdown by what they chose ──────────────────────────────────────────
    by_preference = {
        label: col.count_documents({"preferred_contact": code})
        for code, label in CONTACT_METHODS.items()
    }

    # ── breakdown by how they told us (channel) ───────────────────────────────
    channels = ["email", "whatsapp", "call", "sms", "manual"]
    by_channel = {
        ch: col.count_documents({
            "preferred_contact_channel": ch,
            "preferred_contact": {"$exists": True, "$nin": [[], None]},
        })
        for ch in channels
    }
    # Users with a preference but no channel field (legacy / imported records)
    by_channel["unknown"] = col.count_documents({
        "preferred_contact":         {"$exists": True, "$nin": [[], None]},
        "preferred_contact_channel": {"$exists": False},
    })
    if not by_channel["unknown"]:
        del by_channel["unknown"]

    # ── email campaign funnel ─────────────────────────────────────────────────
    emails_sent_total = _sends_col().count_documents({})

    return jsonify({
        "success":          True,
        "total_responses":  total_responses,
        "by_preference":    by_preference,
        "by_channel":       by_channel,
        "email_campaign": {
            "emails_sent":    emails_sent_total,
            "awaiting_reply": col.count_documents(
                {"preferred_contact_email_prompt.status": "sent"}),
            "failed_to_send": col.count_documents(
                {"preferred_contact_email_prompt.status": "failed"}),
            "pending_to_send": pending_count(),
        },
    })


@admin_bp.route("/email/preferred_contact/automation", methods=["GET"])
@admin_required
def email_pc_automation_get():
    """GET /admin/email/preferred_contact/automation"""
    settings = get_automation_settings()
    used     = sent_last_hour()
    return jsonify({
        "success":              True,
        "enabled":              settings["enabled"],
        "env_enabled":          settings["env_enabled"],
        "runtime_enabled":      settings["runtime_enabled"],
        "env_var":              "EMAIL_PC_AUTOMATION_ENABLED",
        "hourly_cap":           settings["hourly_cap"],
        "sent_last_hour":       used,
        "remaining_this_hour":  max(0, settings["hourly_cap"] - used),
        "tick_minutes":         TICK_MINUTES,
        "pending_users":        pending_count(),
        "updated_at":           _iso(settings["updated_at"]),
        "updated_by":           settings["updated_by"],
        "last_tick":            settings["last_tick"],
        "test_mode":            TEST_MODE,
    })


@admin_bp.route("/email/preferred_contact/automation", methods=["POST"])
@admin_required
def email_pc_automation_set():
    """POST /admin/email/preferred_contact/automation  Body: { "enabled": true, "hourly_cap": 30 }"""
    body = request.get_json(silent=True) or {}

    if "enabled" not in body and "hourly_cap" not in body:
        return jsonify({"success": False,
                        "error": "enabled and/or hourly_cap is required"}), 400

    enabled = body.get("enabled")
    if enabled is not None and not isinstance(enabled, bool):
        enabled = str(enabled).lower() in ("1", "true", "yes", "on")

    cap = body.get("hourly_cap")
    if cap is not None:
        try:
            cap = int(cap)
        except (TypeError, ValueError):
            return jsonify({"success": False,
                            "error": "hourly_cap must be an integer"}), 400
        if cap < 1:
            return jsonify({"success": False,
                            "error": "hourly_cap must be at least 1"}), 400

    actor = None
    try:
        from flask_login import current_user
        actor = getattr(current_user, "email", None) or str(
            getattr(current_user, "id", "") or "")
    except Exception:
        pass

    settings = set_automation_settings(enabled=enabled, hourly_cap=cap, actor=actor)
    used = sent_last_hour()

    if enabled and not AUTOMATION_ENABLED:
        note = ("Ignored — automation is off in .env. Set "
                "EMAIL_PC_AUTOMATION_ENABLED=true and restart to enable it.")
    elif settings["enabled"]:
        note = (f"Automation is ON — next tick fires within {TICK_MINUTES} minutes.")
    else:
        note = "Automation is OFF — no emails will be sent automatically."

    return jsonify({
        "success":              True,
        "enabled":              settings["enabled"],
        "env_enabled":          settings["env_enabled"],
        "runtime_enabled":      settings["runtime_enabled"],
        "hourly_cap":           settings["hourly_cap"],
        "capped_at_ceiling":    settings["hourly_cap"] == HOURLY_CAP_CEILING,
        "sent_last_hour":       used,
        "remaining_this_hour":  max(0, settings["hourly_cap"] - used),
        "note":                 note,
    })


@admin_bp.route("/email/preferred_contact/automation/run", methods=["POST"])
@admin_required
def email_pc_automation_run():
    """POST /admin/email/preferred_contact/automation/run — fire a tick now."""
    return jsonify({"success": True, **automation_tick(source="manual")})


# ── Scheduler wiring ──────────────────────────────────────────────────────────

def init_automation(app=None, scheduler=None):
    """
    Call once at startup, e.g.:

        from admin.email_preferred_contact import init_automation
        init_automation(app)

    No-ops unless EMAIL_PC_AUTOMATION_ENABLED=true in .env.
    """
    if not AUTOMATION_ENABLED:
        print("[email_pc] automation disabled in .env — scheduler not started")
        return None

    if os.environ.get("EMAIL_PC_SCHEDULER", "1").lower() in ("0", "false", "no"):
        return None

    if scheduler is None:
        from apscheduler.schedulers.background import BackgroundScheduler
        scheduler    = BackgroundScheduler(daemon=True)
        started_here = True
    else:
        started_here = False

    def _job():
        try:
            result = automation_tick(source="scheduler")
            if result.get("sent_count"):
                print(f"[email_pc] tick sent {result['sent_count']} "
                      f"(cap {result.get('hourly_cap')})")
        except Exception as e:
            print(f"[email_pc] tick failed: {e}")

    scheduler.add_job(
        _job,
        "interval",
        minutes=TICK_MINUTES,
        id="email_pc_tick",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    if started_here:
        scheduler.start()
    return scheduler