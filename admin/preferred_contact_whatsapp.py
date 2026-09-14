"""
Preferred Contact Method campaign over WhatsApp (WATI).

Flow
────
1. Admin (or a cron job) hits POST /admin/whatsapp/preferred_contact/send_batch
   with {"limit": 25}.  N users are *atomically claimed* from db.users and sent a
   template message with three quick-reply buttons:

       1 - Call        2 - WhatsApp       3 - Email

2. The user replies (button tap or free text: "2", "whatsapp", "1 and 3", ...).

3. WATI POSTs the inbound message to
   POST /admin/whatsapp/preferred_contact/webhook?token=<WATI_WEBHOOK_TOKEN>
   We parse it and write:  preferred_contact: [ 2 ]

Bookkeeping lives on the user doc under `preferred_contact_prompt`:

    preferred_contact_prompt: {
        status:         "sending" | "sent" | "failed" | "answered",
        attempts:       1,
        clarifications: 0,
        wa_id:          "353877955501",
        claimed_at:     ISODate,
        last_sent_at:   ISODate,
        last_error:     "…",
        answered_at:    ISODate,
        raw_reply:      "2",
        last_message_id:"ABGH…"        # webhook idempotency
    }

Env vars
────────
    WATI_API_ENDPOINT                 (already used by whatsapp_wati.py)
    WATI_ACCESS_TOKEN                 (already used by whatsapp_wati.py)
    WATI_PREFERRED_CONTACT_TEMPLATE   default "preferred_contact_method"
    PREFERRED_CONTACT_BATCH_SIZE      default 25   – messages per run
    PREFERRED_CONTACT_SEND_DELAY_MS   default 400  – pause between sends
    PREFERRED_CONTACT_MAX_ATTEMPTS    default 2    – re-prompts per user
    PREFERRED_CONTACT_RESEND_HOURS    default 72   – wait before re-prompting
    WATI_WEBHOOK_TOKEN                shared secret for the webhook URL

Recommended indexes
───────────────────
    db.users.create_index([("preferred_contact_prompt.wa_id", 1)])
    db.users.create_index([("preferred_contact", 1),
                           ("preferred_contact_prompt.status", 1)])
"""

import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests
from flask import jsonify, request
from pymongo import ReturnDocument

from database import db
from . import admin_bp
from admin.views import admin_required

from .whatsapp_wati import (
    _send_template_message,
    _send_session_message,
    _normalise_phone,
)


# ── Configuration ─────────────────────────────────────────────────────────────

TEMPLATE_NAME   = os.environ.get("WATI_PREFERRED_CONTACT_TEMPLATE",
                                 "preferred_contact_method")
BATCH_SIZE      = int(os.environ.get("PREFERRED_CONTACT_BATCH_SIZE", 25))
SEND_DELAY_MS   = int(os.environ.get("PREFERRED_CONTACT_SEND_DELAY_MS", 400))
MAX_ATTEMPTS    = int(os.environ.get("PREFERRED_CONTACT_MAX_ATTEMPTS", 2))
RESEND_HOURS    = int(os.environ.get("PREFERRED_CONTACT_RESEND_HOURS", 72))
MAX_CLARIFY     = int(os.environ.get("PREFERRED_CONTACT_MAX_CLARIFY", 1))
WEBHOOK_TOKEN   = os.environ.get("WATI_WEBHOOK_TOKEN", "")

# Hard ceiling so a bad `limit` in the request body can't blast the whole DB.
BATCH_HARD_CAP  = int(os.environ.get("PREFERRED_CONTACT_BATCH_CAP", 500))

CONTACT_METHODS = {1: "Call", 2: "WhatsApp", 3: "Email"}

# ── Test mode ─────────────────────────────────────────────────────────────────
# PREFERRED_CONTACT_TEST_MODE=1 makes run_batch() ignore everyone except the
# emails in PREFERRED_CONTACT_TEST_EMAILS (comma separated). Flip it on before
# the first real run so a stray cron tick can't message your whole user base.
#
# PREFERRED_CONTACT_TEST_PHONE, if set, redirects *every* send in test mode to
# that one number — handy when the test user's real phone isn't yours. The
# prompt record still stores that number as wa_id, so replies from it map back
# to the right user and the write-back path is exercised end to end.
TEST_MODE   = os.environ.get("PREFERRED_CONTACT_TEST_MODE", "").lower() in ("1", "true", "yes")
TEST_EMAILS = [e.strip().lower() for e in
               os.environ.get("PREFERRED_CONTACT_TEST_EMAILS", "").split(",") if e.strip()]
TEST_PHONE  = os.environ.get("PREFERRED_CONTACT_TEST_PHONE", "").strip()

# Free-text / button-label → code.  Keys are lowercase, punctuation stripped.
_KEYWORDS = {
    "1": 1, "one": 1, "call": 1, "phone": 1, "phone call": 1, "voice": 1,
    "telephone": 1, "ring": 1,
    "2": 2, "two": 2, "whatsapp": 2, "whats app": 2, "wa": 2, "wtsp": 2,
    "message": 2, "text": 2, "chat": 2,
    "3": 3, "three": 3, "email": 3, "e mail": 3, "mail": 3, "gmail": 3,
}

PROMPT_TEXT = (
    "Hi {name}, how would you prefer us to contact you about shifts and "
    "compliance updates?\n\n"
    "1 - Call\n"
    "2 - WhatsApp\n"
    "3 - Email\n\n"
    "Reply with the number(s) that suit you — e.g. *2* or *1,3*."
)

CLARIFY_TEXT = (
    "Sorry, I didn't catch that. Please reply with a number:\n\n"
    "1 - Call\n"
    "2 - WhatsApp\n"
    "3 - Email"
)


def _users_col():
    return db.users


def find_user_by_email(email: str):
    """Case-insensitive exact match on the email field."""
    email = (email or "").strip()
    if not email:
        return None
    return _users_col().find_one(
        {"email": re.compile(rf"^{re.escape(email)}$", re.IGNORECASE)}
    )


def reset_prompt_state(user_id):
    """
    Clear preferred_contact and the prompt bookkeeping so the same user can be
    tested repeatedly. Test-only — never call this from the batch path.
    """
    _users_col().update_one(
        {"_id": user_id},
        {"$unset": {"preferred_contact": "", "preferred_contact_prompt": ""}},
    )


def _now():
    return datetime.now(timezone.utc)


def _iso(value):
    return value.isoformat() if hasattr(value, "isoformat") else value


# ── Reply parsing ─────────────────────────────────────────────────────────────

def _clean(text: str) -> str:
    text = (text or "").lower().strip()
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def parse_preference(text: str) -> list[int]:
    """
    Turn a WhatsApp reply into a sorted list of contact-method codes.

        "2"                 -> [2]
        "1,3"               -> [1, 3]
        "WhatsApp"          -> [2]
        "call or email pls" -> [1, 3]
        "any"/"all"         -> [1, 2, 3]
        "asdf"              -> []
    """
    cleaned = _clean(text)
    if not cleaned:
        return []

    if cleaned in ("all", "any", "anything", "all of them", "any of them"):
        return [1, 2, 3]

    codes: set[int] = set()

    # Bare digits anywhere in the reply ("option 2", "1 & 3", "13").
    for token in re.findall(r"\d", cleaned):
        if token in ("1", "2", "3"):
            codes.add(int(token))

    # Word matches — longest phrases first so "phone call" wins over "phone".
    for phrase in sorted(_KEYWORDS, key=len, reverse=True):
        if phrase.isdigit():
            continue
        if re.search(rf"\b{re.escape(phrase)}\b", cleaned):
            codes.add(_KEYWORDS[phrase])

    return sorted(codes)


def _extract_reply_text(payload: dict) -> str:
    """
    WATI shapes vary by message type. Cover text, quick-reply buttons and
    interactive list/button replies.
    """
    for key in ("text", "finalText", "buttonReply", "interactiveButtonReply",
                "listReply", "buttonText"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            for inner in ("text", "title", "body", "id", "payload"):
                if isinstance(value.get(inner), str) and value[inner].strip():
                    return value[inner].strip()

    data = payload.get("data")
    if isinstance(data, dict):
        return _extract_reply_text(data)
    return ""


# ── Sending ───────────────────────────────────────────────────────────────────

def _claim_next_user(cutoff, stale_cutoff):
    """
    Atomically reserve one un-answered user so parallel runs / overlapping cron
    ticks can never double-send. Sets status='sending' and bumps attempts.
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
            {"preferred_contact_prompt": {"$exists": False}},
            {"preferred_contact_prompt.status": {"$exists": False}},
            {
                "preferred_contact_prompt.status": "failed",
                "preferred_contact_prompt.attempts": {"$lt": MAX_ATTEMPTS},
            },
            {
                "preferred_contact_prompt.status": "sent",
                "preferred_contact_prompt.attempts": {"$lt": MAX_ATTEMPTS},
                "preferred_contact_prompt.last_sent_at": {"$lt": cutoff},
            },
            # A crashed worker left someone stuck in 'sending' — reclaim it.
            {
                "preferred_contact_prompt.status": "sending",
                "preferred_contact_prompt.claimed_at": {"$lt": stale_cutoff},
            },
        ]
    }

    query = {
        "is_active": True,
        "phone": {"$nin": [None, ""]},
        "$and": [no_preference, needs_prompt],
    }

    # Test mode: never leave the whitelist. An empty whitelist matches nobody,
    # which is the safe failure — better a no-op run than an accidental blast.
    if TEST_MODE:
        query["email"] = {
            "$in": [re.compile(rf"^{re.escape(e)}$", re.IGNORECASE)
                    for e in TEST_EMAILS]
        }

    return _users_col().find_one_and_update(
        query,
        {
            "$set": {
                "preferred_contact_prompt.status": "sending",
                "preferred_contact_prompt.claimed_at": _now(),
            },
            "$inc": {"preferred_contact_prompt.attempts": 1},
        },
        projection={"_id": 1, "first_name": 1, "last_name": 1, "phone": 1,
                    "preferred_contact_prompt": 1},
        sort=[("created_at", 1)],
        return_document=ReturnDocument.AFTER,
    )


def _release_user(user_id, status, error=None, wa_id=None):
    update = {
        "preferred_contact_prompt.status": status,
        "preferred_contact_prompt.last_error": error,
    }
    if status == "sent":
        update["preferred_contact_prompt.last_sent_at"] = _now()
    if wa_id:
        update["preferred_contact_prompt.wa_id"] = wa_id
    _users_col().update_one({"_id": user_id}, {"$set": update})


def send_prompt(user: dict, override_phone: str = None) -> dict:
    """
    Send the prompt to one user. Template first (works outside the 24h window);
    if the template is unavailable, fall back to a session message.

    `override_phone` redirects the message to a test handset. The number used is
    what gets stored as wa_id, so an inbound reply from it still resolves to this
    user and the webhook write-back is genuinely exercised.
    """
    wa_id = _normalise_phone(override_phone or user.get("phone") or "")
    name  = (user.get("first_name") or "there").strip()

    if not wa_id or len(wa_id) < 8:
        _release_user(user["_id"], "failed", "invalid phone")
        return {"ok": False, "error": "invalid phone", "wa_id": wa_id}

    try:
        _send_template_message(
            wa_id,
            TEMPLATE_NAME,
            [{"name": "name", "value": name}],
        )
    except Exception as template_error:
        try:
            _send_session_message(wa_id, PROMPT_TEXT.format(name=name))
        except Exception as session_error:
            error = f"template: {template_error} | session: {session_error}"
            _release_user(user["_id"], "failed", error, wa_id=wa_id)
            return {"ok": False, "error": error, "wa_id": wa_id}

    _release_user(user["_id"], "sent", None, wa_id=wa_id)
    return {"ok": True, "wa_id": wa_id}


def run_batch(limit: int, dry_run: bool = False) -> dict:
    """Claim and message up to `limit` users. Safe to call from cron."""
    limit        = max(1, min(int(limit), BATCH_HARD_CAP))
    cutoff       = _now() - timedelta(hours=RESEND_HOURS)
    stale_cutoff = _now() - timedelta(minutes=15)

    sent, failed, skipped = [], [], 0

    while len(sent) + len(failed) < limit:
        user = _claim_next_user(cutoff, stale_cutoff)
        if not user:
            break

        if dry_run:
            # Undo the claim so a dry run leaves no trace.
            _users_col().update_one(
                {"_id": user["_id"]},
                {"$set": {"preferred_contact_prompt.status":
                          (user.get("preferred_contact_prompt") or {}).get("status")},
                 "$inc": {"preferred_contact_prompt.attempts": -1}},
            )
            sent.append({"user_id": str(user["_id"]),
                         "phone": user.get("phone"), "dry_run": True})
            continue

        result = send_prompt(user, override_phone=TEST_PHONE if TEST_MODE else None)
        entry  = {
            "user_id": str(user["_id"]),
            "name": f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
            "phone": user.get("phone"),
            "wa_id": result.get("wa_id"),
        }
        if result["ok"]:
            sent.append(entry)
        else:
            entry["error"] = result["error"]
            failed.append(entry)

        if SEND_DELAY_MS:
            time.sleep(SEND_DELAY_MS / 1000.0)

    return {
        "requested": limit,
        "sent_count": len(sent),
        "failed_count": len(failed),
        "skipped": skipped,
        "sent": sent,
        "failed": failed,
        "remaining": pending_count(),
    }


def pending_count() -> int:
    return _users_col().count_documents({
        "is_active": True,
        "phone": {"$nin": [None, ""]},
        "$or": [
            {"preferred_contact": {"$exists": False}},
            {"preferred_contact": None},
            {"preferred_contact": []},
        ],
    })


# ── Webhook matching ──────────────────────────────────────────────────────────

def _phone_tail_regex(wa_id: str):
    """
    Stored phones are formatted ('+353 877955501') while WATI sends digits only.
    Match on the last 9 digits, tolerating any separators in between.
    """
    tail = wa_id[-9:]
    return re.compile(r"\D*".join(re.escape(d) for d in tail) + r"\s*$")


def find_user_by_wa_id(wa_id: str):
    if not wa_id:
        return None
    user = _users_col().find_one({"preferred_contact_prompt.wa_id": wa_id})
    if user:
        return user
    if len(wa_id) >= 9:
        return _users_col().find_one({"phone": _phone_tail_regex(wa_id)})
    return None


def _record_answer(user, codes, raw_text, message_id):
    _users_col().update_one(
        {"_id": user["_id"]},
        {"$set": {
            "preferred_contact": codes,
            "preferred_contact_prompt.status": "answered",
            "preferred_contact_prompt.answered_at": _now(),
            "preferred_contact_prompt.raw_reply": raw_text,
            "preferred_contact_prompt.last_message_id": message_id,
            "updated_at": _now(),
        }},
    )


# ── Targeted dispatch ─────────────────────────────────────────────────────────

def dispatch_prompt(user, override_phone=None, reset=False,
                    force=False, mark_test=False) -> dict:
    """
    Send the prompt to one specific user, bypassing the batch claim entirely.

    force=False skips anyone who already has a preferred_contact on file, so a
    re-run doesn't pester people who already answered.
    reset=True clears their answer and prompt history first (re-ask from scratch).
    """
    entry = {
        "user_id": str(user["_id"]),
        "email": user.get("email"),
        "name": f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
        "phone_on_file": user.get("phone"),
    }

    if user.get("preferred_contact") and not (force or reset):
        entry.update({
            "ok": False,
            "skipped": "already answered",
            "preferred_contact": user.get("preferred_contact"),
            "hint": 'pass "force": true to re-ask anyway',
        })
        return entry

    target = override_phone or user.get("phone") or ""
    if not _normalise_phone(target):
        entry.update({"ok": False,
                      "error": 'no usable phone — pass "phone" to redirect'})
        return entry

    if reset:
        reset_prompt_state(user["_id"])

    set_fields = {
        "preferred_contact_prompt.claimed_at": _now(),
        "preferred_contact_prompt.status": "sending",
    }
    if mark_test:
        set_fields["preferred_contact_prompt.is_test"] = True

    _users_col().update_one(
        {"_id": user["_id"]},
        {"$inc": {"preferred_contact_prompt.attempts": 1}, "$set": set_fields},
    )

    result = send_prompt(user, override_phone=override_phone or None)
    entry.update({
        "ok": result["ok"],
        "sent_to": result.get("wa_id"),
        "redirected": bool(override_phone),
        "was_reset": reset,
    })
    if not result["ok"]:
        entry["error"] = result.get("error")
    return entry


def resolve_targets(body) -> tuple[list, list]:
    """
    Accepts any of: email, emails[], user_id, user_ids[], phone_lookup.
    Returns (users_found, not_found_identifiers).
    """
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

@admin_bp.route("/whatsapp/preferred_contact/send_batch", methods=["POST"])
@admin_required
def preferred_contact_send_batch():
    """
    POST /admin/whatsapp/preferred_contact/send_batch
    Body (all optional): { "limit": 25, "dry_run": false }

    `limit` is how many messages go out in this run — that's the knob you asked
    for. It defaults to PREFERRED_CONTACT_BATCH_SIZE and is capped at
    PREFERRED_CONTACT_BATCH_CAP.
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
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "error": "WATI request timed out"}), 504
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route("/whatsapp/preferred_contact/send_one", methods=["POST"])
@admin_bp.route("/whatsapp/preferred_contact/send_to", methods=["POST"])
@admin_required
def preferred_contact_send_to():
    """
    POST /admin/whatsapp/preferred_contact/send_to     (alias: /send_one)

    Selectively trigger the prompt for specific people, addressed by email —
    no batch, no queue, no claim logic. This is the everyday targeted send.

    Body — any one of:
        { "email":  "adnanalrawahneh@gmail.com" }
        { "emails": ["a@x.com", "b@y.com"] }            # or a comma-separated string
        { "user_id": "6a6c65db248f0a2b59a5e08b" }
        { "user_ids": [...] }

    Optional flags:
        "phone": "+353 85…"   redirect the send to this number (single target only)
        "force": true         re-ask even if they already answered
        "reset": true         wipe their answer + history, then ask fresh

    Users who already have a preferred_contact are skipped unless force/reset.
    Returns per-recipient results, so a bad email in a list of five doesn't
    hide the four that worked.
    """
    body     = request.get_json(silent=True) or {}
    override = (body.get("phone") or "").strip()
    force    = bool(body.get("force"))
    reset    = bool(body.get("reset"))

    # Allow a quick one-off from the query string too: ?email=…
    if not any(body.get(k) for k in ("email", "emails", "user_id", "user_ids")):
        if request.args.get("email"):
            body["email"] = request.args["email"]

    users, missing = resolve_targets(body)

    if not users and not missing:
        return jsonify({"success": False,
                        "error": "email, emails, user_id or user_ids is required"}), 400

    if override and len(users) > 1:
        return jsonify({"success": False,
                        "error": "phone redirect only works with a single target"}), 400

    results = []
    for index, user in enumerate(users):
        results.append(dispatch_prompt(
            user, override_phone=override or None, reset=reset, force=force,
        ))
        if SEND_DELAY_MS and index < len(users) - 1:
            time.sleep(SEND_DELAY_MS / 1000.0)

    sent    = [r for r in results if r.get("ok")]
    skipped = [r for r in results if r.get("skipped")]
    failed  = [r for r in results if not r.get("ok") and not r.get("skipped")]

    payload = {
        "success": bool(sent) or not (failed or missing),
        "sent_count": len(sent),
        "skipped_count": len(skipped),
        "failed_count": len(failed),
        "not_found": missing,
        "results": results,
    }
    status = 200 if sent or skipped else (404 if missing and not failed else 502)
    return jsonify(payload), status


@admin_bp.route("/whatsapp/preferred_contact/test", methods=["POST"])
@admin_required
def preferred_contact_test():
    """
    POST /admin/whatsapp/preferred_contact/test

    Body:
        {
          "email":  "adnanalrawahneh@gmail.com",   # required — user to test on
          "phone":  "+353 851234567",              # optional — send here instead
          "reset":  true                           # optional — clear previous answer
        }

    Sends the real prompt to exactly one user, resolved by email. Nobody else is
    touched, regardless of TEST_MODE. With "reset": true the user's
    preferred_contact and prompt history are wiped first so you can re-run the
    same test as often as you like.

    Then reply on WhatsApp and check GET .../test?email=… to confirm the
    write-back landed.
    """
    body     = request.get_json(silent=True) or {}
    email    = (body.get("email") or request.args.get("email") or "").strip()
    override = (body.get("phone") or TEST_PHONE or "").strip()
    do_reset = bool(body.get("reset"))

    if not email:
        return jsonify({"success": False, "error": "email is required"}), 400

    user = find_user_by_email(email)
    if not user:
        return jsonify({"success": False,
                        "error": f"no user with email {email}"}), 404

    result = dispatch_prompt(user, override_phone=override or None,
                             reset=do_reset, force=True, mark_test=True)

    return jsonify({
        "success": result["ok"],
        "test": True,
        "template": TEMPLATE_NAME,
        **result,
        "next_step": ("Reply on WhatsApp, then GET "
                      f"/admin/whatsapp/preferred_contact/test?email={user.get('email')}"),
    }), (200 if result["ok"] else 502)


@admin_bp.route("/whatsapp/preferred_contact/test")
@admin_required
def preferred_contact_test_inspect():
    """
    GET /admin/whatsapp/preferred_contact/test?email=…&history=1

    Shows exactly what's stored for one user — the current preferred_contact
    array, the prompt bookkeeping, and (with history=1) the last few WATI
    messages so you can see whether the reply actually arrived.
    """
    email = request.args.get("email", "").strip()
    if not email:
        return jsonify({"success": False, "error": "email is required"}), 400

    user = find_user_by_email(email)
    if not user:
        return jsonify({"success": False,
                        "error": f"no user with email {email}"}), 404

    prompt = dict(user.get("preferred_contact_prompt") or {})
    for key in ("claimed_at", "last_sent_at", "answered_at"):
        if key in prompt:
            prompt[key] = _iso(prompt[key])

    codes = user.get("preferred_contact") or []
    out = {
        "success": True,
        "email": user.get("email"),
        "user_id": str(user["_id"]),
        "phone": user.get("phone"),
        "preferred_contact": codes,
        "preferred_contact_labels": [CONTACT_METHODS.get(c, c) for c in codes],
        "prompt": prompt,
        "test_mode": TEST_MODE,
    }

    if request.args.get("history"):
        from .whatsapp_wati import _get_messages
        wa_id = prompt.get("wa_id") or _normalise_phone(user.get("phone") or "")
        try:
            raw = _get_messages(wa_id, page_size=10, page=1)
            items = (raw.get("messages") or {}).get("items") or []
            out["wati_history"] = [{
                "text": m.get("text") or m.get("finalText") or "",
                "direction": "inbound" if m.get("owner") is False else "outbound",
                "status": m.get("statusString"),
                "created_at": m.get("created"),
            } for m in items]
        except Exception as e:
            out["wati_history_error"] = str(e)

    return jsonify(out)


@admin_bp.route("/whatsapp/preferred_contact/test/simulate_reply", methods=["POST"])
@admin_required
def preferred_contact_simulate_reply():
    """
    POST /admin/whatsapp/preferred_contact/test/simulate_reply

    Body: { "email": "…", "reply": "1,3", "commit": true }

    Runs the reply through the same parser and write-back the webhook uses,
    without WATI or a handset in the loop. With "commit": false (the default)
    it only reports what *would* be written — useful for checking odd replies
    like "call me pls" before trusting them in production.
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
        _record_answer(user, codes, reply, f"simulated-{int(time.time())}")

    return jsonify({
        "success": True,
        "simulated": True,
        "email": user.get("email"),
        "user_id": str(user["_id"]),
        "reply": reply,
        "parsed": codes,
        "labels": [CONTACT_METHODS[c] for c in codes],
        "committed": bool(commit and codes),
        "note": None if codes else "Reply not understood — webhook would send a clarification",
    })


@admin_bp.route("/whatsapp/preferred_contact/status")
@admin_required
def preferred_contact_status():
    """GET /admin/whatsapp/preferred_contact/status — campaign counters."""
    col = _users_col()
    answered = col.count_documents({"preferred_contact": {"$exists": True,
                                                          "$nin": [[], None]}})
    breakdown = {
        CONTACT_METHODS[code]: col.count_documents({"preferred_contact": code})
        for code in CONTACT_METHODS
    }
    return jsonify({
        "success": True,
        "batch_size_default": BATCH_SIZE,
        "template": TEMPLATE_NAME,
        "test_mode": TEST_MODE,
        "test_emails": TEST_EMAILS if TEST_MODE else [],
        "test_phone_redirect": TEST_PHONE if TEST_MODE else "",
        "pending": pending_count(),
        "prompt_sent_awaiting_reply": col.count_documents(
            {"preferred_contact_prompt.status": "sent"}),
        "failed": col.count_documents(
            {"preferred_contact_prompt.status": "failed"}),
        "answered": answered,
        "breakdown": breakdown,
    })


@admin_bp.route("/whatsapp/preferred_contact/webhook", methods=["POST"])
def preferred_contact_webhook():
    """
    POST /admin/whatsapp/preferred_contact/webhook?token=<WATI_WEBHOOK_TOKEN>

    Configure this URL in WATI ➝ Settings ➝ Webhooks for the
    "Message Received" event. No @admin_required — WATI can't log in — so the
    shared token is the only gate. Keep it long and random.

    Always returns 200 for anything we simply don't act on, otherwise WATI will
    retry the same payload forever.
    """
    token = request.args.get("token") or request.headers.get("X-Webhook-Token", "")
    if not WEBHOOK_TOKEN or token != WEBHOOK_TOKEN:
        return jsonify({"success": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}

    # Ignore our own outbound traffic and non-message events.
    if payload.get("owner") is True:
        return jsonify({"success": True, "ignored": "outbound"}), 200
    event = payload.get("eventType") or payload.get("type") or ""
    if event and event not in ("message", "text", "button", "interactive",
                              "interactiveButtonReply", "listReply"):
        return jsonify({"success": True, "ignored": event}), 200

    wa_id = _normalise_phone(
        payload.get("waId") or payload.get("wAid") or payload.get("phone") or ""
    )
    message_id = payload.get("id") or payload.get("whatsappMessageId") or ""
    raw_text   = _extract_reply_text(payload)

    user = find_user_by_wa_id(wa_id)
    if not user:
        return jsonify({"success": True, "ignored": "no matching user",
                        "wa_id": wa_id}), 200

    prompt = user.get("preferred_contact_prompt") or {}

    # Idempotency — WATI retries on non-2xx and occasionally duplicates.
    if message_id and prompt.get("last_message_id") == message_id:
        return jsonify({"success": True, "ignored": "duplicate"}), 200

    # Only interpret replies from someone we actually asked.
    if prompt.get("status") not in ("sent", "answered", "clarifying"):
        return jsonify({"success": True, "ignored": "not awaiting reply"}), 200

    codes = parse_preference(raw_text)

    if not codes:
        if prompt.get("clarifications", 0) < MAX_CLARIFY:
            try:
                _send_session_message(wa_id, CLARIFY_TEXT)
            except Exception:
                pass  # session may have closed; nothing to do
            _users_col().update_one(
                {"_id": user["_id"]},
                {"$inc": {"preferred_contact_prompt.clarifications": 1},
                 "$set": {"preferred_contact_prompt.status": "clarifying",
                          "preferred_contact_prompt.last_message_id": message_id}},
            )
        return jsonify({"success": True, "parsed": [], "action": "clarified"}), 200

    _record_answer(user, codes, raw_text, message_id)

    labels = ", ".join(CONTACT_METHODS[c] for c in codes)
    try:
        _send_session_message(
            wa_id,
            f"Thanks! We'll contact you by {labels} from now on. "
            "Reply here any time to change it.",
        )
    except Exception:
        pass

    return jsonify({
        "success": True,
        "user_id": str(user["_id"]),
        "preferred_contact": codes,
        "labels": labels,
    }), 200


@admin_bp.route("/whatsapp/preferred_contact/backfill", methods=["POST"])
@admin_required
def preferred_contact_backfill():
    """
    POST /admin/whatsapp/preferred_contact/backfill
    Body: { "limit": 50 }

    Recovery path for missed webhooks: re-reads recent WATI history for users
    stuck in 'sent'/'clarifying' and applies any parsable reply.
    """
    from .whatsapp_wati import _get_messages

    body  = request.get_json(silent=True) or {}
    limit = int(body.get("limit") or 50)

    stuck = list(_users_col().find(
        {"preferred_contact_prompt.status": {"$in": ["sent", "clarifying"]}},
        {"_id": 1, "phone": 1, "preferred_contact_prompt": 1},
    ).limit(limit))

    updated, checked = [], 0
    for user in stuck:
        checked += 1
        wa_id = (user.get("preferred_contact_prompt") or {}).get("wa_id") \
            or _normalise_phone(user.get("phone") or "")
        try:
            raw  = _get_messages(wa_id, page_size=20, page=1)
            msgs = (raw.get("messages") or {}).get("items") or []
        except Exception:
            continue

        for m in sorted(msgs, key=lambda x: x.get("created") or "", reverse=True):
            if m.get("owner") is not False:
                continue
            codes = parse_preference(m.get("text") or m.get("finalText") or "")
            if codes:
                _record_answer(user, codes, m.get("text") or "", m.get("id") or "")
                updated.append({"user_id": str(user["_id"]),
                                "preferred_contact": codes})
                break

        if SEND_DELAY_MS:
            time.sleep(SEND_DELAY_MS / 1000.0)

    return jsonify({"success": True, "checked": checked,
                    "updated_count": len(updated), "updated": updated})