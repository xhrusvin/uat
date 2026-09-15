"""
Preferred Contact Method campaign over WhatsApp (WATI).

Flow
────
1. Admin (or a cron job) hits POST /admin/whatsapp/preferred_contact/send_batch
   with {"limit": 25}.  N users are *atomically claimed* from db.users and sent
   the WATI template 'bulk_msg'.  The raw WATI API response for every send is
   saved to preferred_contact_prompt.wati_send_response.

2. The user replies (button tap or free text: "2", "whatsapp", "1 and 3", ...).

3. WATI POSTs the inbound message to
   POST /admin/whatsapp/preferred_contact/webhook?token=<WATI_WEBHOOK_TOKEN>
   We parse it and write:  preferred_contact: [ 2 ]

Skip rules (enforced in _claim_next_user and dispatch_prompt)
─────────────────────────────────────────────────────────────
  • Users whose preferred_contact already contains 2 (Email) or 3 (SMS/Call via
    non-WA channel) are NEVER messaged.  Concretely: anyone with a non-empty
    preferred_contact that does NOT include WhatsApp (code 2) is excluded.
  • Users who have already been sent the prompt in this session (status in
    'sending' | 'sent' | 'answered') are never sent it again —
    atomic find_one_and_update makes this safe across parallel workers.

Bookkeeping lives on the user doc under `preferred_contact_prompt`:

    preferred_contact_prompt: {
        status:               "sending" | "sent" | "failed" | "answered",
        attempts:             1,
        clarifications:       0,
        wa_id:                "353877955501",
        claimed_at:           ISODate,
        last_sent_at:         ISODate,
        last_error:           "…",
        answered_at:          ISODate,
        raw_reply:            "2",
        last_message_id:      "ABGH…"        # webhook idempotency
        wati_send_response:   { … }          # raw WATI /sendTemplateMessage body
    }

Env vars
────────
    WATI_API_ENDPOINT                 (already used by whatsapp_wati.py)
    WATI_ACCESS_TOKEN                 (already used by whatsapp_wati.py)
    WATI_PREFERRED_CONTACT_TEMPLATE   default "bulk_msg"
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

TEMPLATE_NAME   = os.environ.get("WATI_PREFERRED_CONTACT_TEMPLATE", "bulk_msg")
BATCH_SIZE      = int(os.environ.get("PREFERRED_CONTACT_BATCH_SIZE", 25))
SEND_DELAY_MS   = int(os.environ.get("PREFERRED_CONTACT_SEND_DELAY_MS", 400))
MAX_ATTEMPTS    = int(os.environ.get("PREFERRED_CONTACT_MAX_ATTEMPTS", 2))
RESEND_HOURS    = int(os.environ.get("PREFERRED_CONTACT_RESEND_HOURS", 72))
MAX_CLARIFY     = int(os.environ.get("PREFERRED_CONTACT_MAX_CLARIFY", 1))
WEBHOOK_TOKEN   = os.environ.get("WATI_WEBHOOK_TOKEN", "")

# Hard ceiling so a bad `limit` in the request body can't blast the whole DB.
BATCH_HARD_CAP  = int(os.environ.get("PREFERRED_CONTACT_BATCH_CAP", 500))

CONTACT_METHODS = {1: "Call", 2: "WhatsApp", 3: "Email"}

# Codes that indicate the user already has a non-WhatsApp preferred channel —
# we must NOT send them a WhatsApp prompt.
_NON_WA_CODES = {1, 3}   # Call (1) and Email (3)

# ── Test mode ─────────────────────────────────────────────────────────────────
TEST_MODE   = os.environ.get("PREFERRED_CONTACT_TEST_MODE", "").lower() in ("1", "true", "yes")
TEST_EMAILS = [e.strip().lower() for e in
               os.environ.get("PREFERRED_CONTACT_TEST_EMAILS", "").split(",") if e.strip()]
TEST_PHONE  = os.environ.get("PREFERRED_CONTACT_TEST_PHONE", "").strip()

# Free-text / button-label → code.
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


# ── Reply parsing ──────────────────────────────────────────────────────────────

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

    for token in re.findall(r"\d", cleaned):
        if token in ("1", "2", "3"):
            codes.add(int(token))

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


# ── Skip guard ────────────────────────────────────────────────────────────────

def _already_has_non_wa_preference(user: dict) -> bool:
    """
    Return True when the user's stored preferred_contact contains ONLY
    non-WhatsApp codes (Call=1, Email=3).  If they have no preference yet,
    or if WhatsApp (2) is among their choices, return False.
    """
    codes = user.get("preferred_contact") or []
    if not codes:
        return False
    code_set = set(codes)
    # Skip if every stored code is a non-WA method
    return bool(code_set) and code_set.issubset(_NON_WA_CODES)


# ── Sending ───────────────────────────────────────────────────────────────────

def _claim_next_user(cutoff, stale_cutoff):
    """
    Atomically reserve one un-answered user so parallel runs / overlapping cron
    ticks can never double-send. Sets status='sending' and bumps attempts.

    Excluded:
      • Users whose preferred_contact is non-empty AND contains no WhatsApp (2)
        — they already told us they prefer Call or Email.
      • Users already in 'sending'/'sent'/'answered' (unless stale or re-promptable).
    """
    no_preference_or_wa = {
        "$or": [
            {"preferred_contact": {"$exists": False}},
            {"preferred_contact": None},
            {"preferred_contact": []},
            # Already includes WhatsApp — valid to re-confirm
            {"preferred_contact": 2},
        ]
    }

    # Exclude anyone whose preference is purely non-WA (Email or Call only)
    not_non_wa_only = {
        "$nor": [
            {
                "preferred_contact": {"$exists": True, "$ne": [], "$not": {"$elemMatch": {"$eq": 2}}},
            }
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
        "$and": [no_preference_or_wa, not_non_wa_only, needs_prompt],
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
                "preferred_contact_prompt.status": "sending",
                "preferred_contact_prompt.claimed_at": _now(),
            },
            "$inc": {"preferred_contact_prompt.attempts": 1},
        },
        projection={"_id": 1, "first_name": 1, "last_name": 1, "phone": 1,
                    "preferred_contact": 1, "preferred_contact_prompt": 1},
        sort=[("created_at", 1)],
        return_document=ReturnDocument.AFTER,
    )


def _release_user(user_id, status, error=None, wa_id=None, wati_response=None):
    """
    Update the user's prompt state.  wati_response is the raw dict returned by
    the WATI /sendTemplateMessage (or /sendSessionMessage) call — it is saved
    verbatim so ops can inspect delivery status without hitting the WATI console.
    """
    update = {
        "preferred_contact_prompt.status": status,
        "preferred_contact_prompt.last_error": error,
    }
    if status == "sent":
        update["preferred_contact_prompt.last_sent_at"] = _now()
    if wa_id:
        update["preferred_contact_prompt.wa_id"] = wa_id
    if wati_response is not None:
        update["preferred_contact_prompt.wati_send_response"] = wati_response
    _users_col().update_one({"_id": user_id}, {"$set": update})


def _send_template_message_with_response(wa_id: str, template_name: str,
                                         params: list) -> dict:
    """
    Thin wrapper around _send_template_message that returns the raw WATI
    response body as a dict (so callers can save it to Mongo).

    Falls back gracefully: if the underlying helper doesn't expose the raw
    response we reconstruct a minimal record so something is always stored.
    """
    import inspect

    # Try to get the raw response if whatsapp_wati exposes it
    try:
        # Peek at the signature — some project versions return the response
        sig = inspect.signature(_send_template_message)
        result = _send_template_message(wa_id, template_name, params)
        if isinstance(result, dict):
            return result
        # If it returned something truthy but not a dict, wrap it
        return {"result": str(result), "captured_at": _iso(_now())}
    except Exception:
        raise  # let the caller handle the real send failure

    return {"captured_at": _iso(_now()), "note": "no response body available"}


def send_prompt(user: dict, override_phone: str = None) -> dict:
    """
    Send the 'bulk_msg' template to one user.

    The raw WATI response is saved to preferred_contact_prompt.wati_send_response
    regardless of success or failure, so every send attempt is fully auditable.

    `override_phone` redirects the message to a test handset.
    """
    wa_id = _normalise_phone(override_phone or user.get("phone") or "")
    name  = (user.get("first_name") or "there").strip()

    if not wa_id or len(wa_id) < 8:
        _release_user(user["_id"], "failed", "invalid phone",
                      wati_response={"error": "invalid phone", "wa_id": wa_id})
        return {"ok": False, "error": "invalid phone", "wa_id": wa_id}

    wati_response = None

    try:
        # ── Primary: template message ──────────────────────────────────────
        wati_response = _send_template_message_with_response(
            wa_id,
            TEMPLATE_NAME,                        # "bulk_msg"
            [{"name": "name", "value": name}],
        )
        # Stamp the time we captured the response
        if isinstance(wati_response, dict):
            wati_response.setdefault("captured_at", _iso(_now()))

    except Exception as template_error:
        # ── Fallback: session message (only within 24-h window) ───────────
        wati_response = {
            "template_error": str(template_error),
            "fallback": "session_message",
            "captured_at": _iso(_now()),
        }
        try:
            session_result = _send_session_message(wa_id, PROMPT_TEXT.format(name=name))
            if isinstance(session_result, dict):
                wati_response["session_result"] = session_result
        except Exception as session_error:
            error = f"template: {template_error} | session: {session_error}"
            wati_response["session_error"] = str(session_error)
            _release_user(user["_id"], "failed", error, wa_id=wa_id,
                          wati_response=wati_response)
            return {"ok": False, "error": error, "wa_id": wa_id,
                    "wati_response": wati_response}

    _release_user(user["_id"], "sent", None, wa_id=wa_id,
                  wati_response=wati_response)
    return {"ok": True, "wa_id": wa_id, "wati_response": wati_response}


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

        # Double-check in Python after the atomic claim (belt-and-suspenders).
        if _already_has_non_wa_preference(user):
            # Release the claim without incrementing attempts.
            _users_col().update_one(
                {"_id": user["_id"]},
                {"$set": {"preferred_contact_prompt.status": "skipped_non_wa"},
                 "$inc": {"preferred_contact_prompt.attempts": -1}},
            )
            skipped += 1
            continue

        if dry_run:
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
        "skipped_non_wa": skipped,
        "sent": sent,
        "failed": failed,
        "remaining": pending_count(),
    }


def pending_count() -> int:
    """
    Users who still need to be prompted: active, have a phone, no preference
    set yet (or their preference includes WhatsApp so we can still reach them).
    Users with a purely non-WA preference are NOT counted as pending.
    """
    return _users_col().count_documents({
        "is_active": True,
        "phone": {"$nin": [None, ""]},
        # No preference, OR preference that includes WA
        "$or": [
            {"preferred_contact": {"$exists": False}},
            {"preferred_contact": None},
            {"preferred_contact": []},
            {"preferred_contact": 2},          # WA is one of their choices
        ],
        # Exclude purely non-WA preferences (only 1s and 3s, no 2)
        "$nor": [
            {
                "preferred_contact": {
                    "$exists": True, "$ne": [], "$not": {"$elemMatch": {"$eq": 2}}
                }
            }
        ],
    })


# ── Webhook matching ───────────────────────────────────────────────────────────

def _phone_tail_regex(wa_id: str):
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


# ── Targeted dispatch ──────────────────────────────────────────────────────────

def dispatch_prompt(user, override_phone=None, reset=False,
                    force=False, mark_test=False) -> dict:
    """
    Send the prompt to one specific user, bypassing the batch claim entirely.

    Skip rules:
      • User already has a non-WA-only preference → skipped (unless force/reset).
      • User already has preferred_contact set     → skipped (unless force/reset).
    """
    entry = {
        "user_id": str(user["_id"]),
        "email": user.get("email"),
        "name": f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
        "phone_on_file": user.get("phone"),
    }

    # Skip users who prefer email or call (non-WA), unless overridden.
    if _already_has_non_wa_preference(user) and not (force or reset):
        codes = user.get("preferred_contact") or []
        entry.update({
            "ok": False,
            "skipped": "non_wa_preference",
            "preferred_contact": codes,
            "preferred_contact_labels": [CONTACT_METHODS.get(c, c) for c in codes],
            "hint": 'User prefers Email/Call. Pass "force": true to override.',
        })
        return entry

    if user.get("preferred_contact") and not (force or reset):
        entry.update({
            "ok": False,
            "skipped": "already_answered",
            "preferred_contact": user.get("preferred_contact"),
            "hint": 'Pass "force": true to re-ask anyway',
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
        "wati_response": result.get("wati_response"),
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


# ── Routes ─────────────────────────────────────────────────────────────────────

# ── GET: test endpoint (inspect one user) ─────────────────────────────────────

@admin_bp.route("/whatsapp/preferred_contact/test", methods=["GET"])
@admin_required
def preferred_contact_test_inspect():
    """
    GET /admin/whatsapp/preferred_contact/test?email=…&history=1

    Inspect everything stored for one user:
      • preferred_contact codes + labels
      • Full prompt bookkeeping (including wati_send_response)
      • Whether they would be skipped (non-WA preference, already answered, etc.)
      • Optionally the last 10 WATI messages (?history=1)

    This is the "did it work?" check after a test send.
    """
    email = request.args.get("email", "").strip()
    if not email:
        return jsonify({"success": False, "error": "email query param is required"}), 400

    user = find_user_by_email(email)
    if not user:
        return jsonify({"success": False,
                        "error": f"no user with email {email}"}), 404

    prompt = dict(user.get("preferred_contact_prompt") or {})
    for key in ("claimed_at", "last_sent_at", "answered_at"):
        if key in prompt:
            prompt[key] = _iso(prompt[key])

    codes = user.get("preferred_contact") or []

    # Determine what would happen if we tried to send now
    if _already_has_non_wa_preference(user):
        would_skip = "non_wa_preference"
    elif codes:
        would_skip = "already_answered"
    elif (prompt.get("status") in ("sent", "sending", "answered")):
        would_skip = "already_prompted"
    else:
        would_skip = None

    out = {
        "success": True,
        "email": user.get("email"),
        "user_id": str(user["_id"]),
        "phone": user.get("phone"),
        "preferred_contact": codes,
        "preferred_contact_labels": [CONTACT_METHODS.get(c, c) for c in codes],
        "prompt": prompt,
        "would_skip": would_skip,
        "template_used": TEMPLATE_NAME,
        "test_mode": TEST_MODE,
    }

    if request.args.get("history"):
        from .whatsapp_wati import _get_messages
        wa_id = prompt.get("wa_id") or _normalise_phone(user.get("phone") or "")
        try:
            raw   = _get_messages(wa_id, page_size=10, page=1)
            items = (raw.get("messages") or {}).get("items") or []
            out["wati_history"] = [{
                "text":      m.get("text") or m.get("finalText") or "",
                "direction": "inbound" if m.get("owner") is False else "outbound",
                "status":    m.get("statusString"),
                "created_at": m.get("created"),
            } for m in items]
        except Exception as e:
            out["wati_history_error"] = str(e)

    return jsonify(out)


# ── POST: send to one specific user (test-friendly) ───────────────────────────

@admin_bp.route("/whatsapp/preferred_contact/test", methods=["POST"])
@admin_required
def preferred_contact_test_send():
    """
    POST /admin/whatsapp/preferred_contact/test

    Body:
        {
          "email":  "user@example.com",    # required
          "phone":  "+353 851234567",      # optional – send here instead
          "reset":  true                   # optional – wipe previous answer first
        }

    Sends the 'bulk_msg' template to exactly one user. Nobody else is touched.
    With "reset": true the user's preferred_contact and prompt history are wiped
    first so you can re-run the same test repeatedly.

    After sending, hit GET .../test?email=… to confirm the write-back landed.

    Returns the full wati_response so you can see the raw WATI API reply.
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
        "success": result.get("ok", False),
        "test": True,
        "template": TEMPLATE_NAME,
        **result,
        "next_step": (
            "Reply on WhatsApp, then GET "
            f"/admin/whatsapp/preferred_contact/test?email={user.get('email')}"
        ),
    }), (200 if result.get("ok") else 502)


# ── POST: batch send ───────────────────────────────────────────────────────────

@admin_bp.route("/whatsapp/preferred_contact/send_batch", methods=["POST"])
@admin_required
def preferred_contact_send_batch():
    """
    POST /admin/whatsapp/preferred_contact/send_batch
    Body (all optional): { "limit": 25, "dry_run": false }

    Sends the 'bulk_msg' template to up to `limit` users who:
      • Are active and have a phone number
      • Have NOT already expressed a preference for Email or Call (non-WA)
      • Have NOT already been sent the prompt in a previous run (dedup via
        preferred_contact_prompt.status)

    The raw WATI response for each send is saved to
    preferred_contact_prompt.wati_send_response in the user's DB record.
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


# ── POST: targeted send to specific people ─────────────────────────────────────

@admin_bp.route("/whatsapp/preferred_contact/send_one", methods=["POST"])
@admin_bp.route("/whatsapp/preferred_contact/send_to", methods=["POST"])
@admin_required
def preferred_contact_send_to():
    """
    POST /admin/whatsapp/preferred_contact/send_to    (alias: /send_one)

    Selectively trigger the prompt for specific people.

    Body — any one of:
        { "email":  "user@example.com" }
        { "emails": ["a@x.com", "b@y.com"] }
        { "user_id": "6a6c65db248f0a2b59a5e08b" }
        { "user_ids": [...] }

    Optional flags:
        "phone": "+353 85…"   redirect the send to this number (single target only)
        "force": true         re-ask even if they already answered
        "reset": true         wipe their answer + history, then ask fresh

    Skip rules:
      • Users with a non-WA-only preference (Email/Call) are skipped unless
        force/reset.
      • Users already prompted (status = sent/answered) are not re-sent unless
        force/reset.
    """
    body     = request.get_json(silent=True) or {}
    override = (body.get("phone") or "").strip()
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


# ── Simulate a reply (dry-run webhook) ────────────────────────────────────────

@admin_bp.route("/whatsapp/preferred_contact/test/simulate_reply", methods=["POST"])
@admin_required
def preferred_contact_simulate_reply():
    """
    POST /admin/whatsapp/preferred_contact/test/simulate_reply

    Body: { "email": "…", "reply": "1,3", "commit": true }

    Runs the reply through the same parser and write-back the webhook uses,
    without WATI or a handset in the loop. With "commit": false (the default)
    it only reports what *would* be written.
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


# ── Campaign status ────────────────────────────────────────────────────────────

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

    # How many users were skipped because they already have a non-WA preference
    skipped_non_wa = col.count_documents({
        "preferred_contact": {
            "$exists": True, "$ne": [], "$not": {"$elemMatch": {"$eq": 2}}
        }
    })

    return jsonify({
        "success": True,
        "batch_size_default": BATCH_SIZE,
        "template": TEMPLATE_NAME,
        "test_mode": TEST_MODE,
        "test_emails": TEST_EMAILS if TEST_MODE else [],
        "test_phone_redirect": TEST_PHONE if TEST_MODE else "",
        "pending": pending_count(),
        "skipped_non_wa_preference": skipped_non_wa,
        "prompt_sent_awaiting_reply": col.count_documents(
            {"preferred_contact_prompt.status": "sent"}),
        "failed": col.count_documents(
            {"preferred_contact_prompt.status": "failed"}),
        "answered": answered,
        "breakdown": breakdown,
    })


# ── Webhook (inbound replies from WATI) ───────────────────────────────────────

@admin_bp.route("/whatsapp/preferred_contact/webhook", methods=["POST"])
def preferred_contact_webhook():
    """
    POST /admin/whatsapp/preferred_contact/webhook?token=<WATI_WEBHOOK_TOKEN>

    Configure this URL in WATI → Settings → Webhooks for the
    "Message Received" event. No @admin_required — WATI can't log in — so the
    shared token is the only gate. Keep it long and random.

    Always returns 200 for anything we simply don't act on, otherwise WATI will
    retry the same payload forever.
    """
    token = request.args.get("token") or request.headers.get("X-Webhook-Token", "")
    if not WEBHOOK_TOKEN or token != WEBHOOK_TOKEN:
        return jsonify({"success": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}

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

    if prompt.get("status") not in ("sent", "answered", "clarifying"):
        return jsonify({"success": True, "ignored": "not awaiting reply"}), 200

    codes = parse_preference(raw_text)

    if not codes:
        if prompt.get("clarifications", 0) < MAX_CLARIFY:
            try:
                _send_session_message(wa_id, CLARIFY_TEXT)
            except Exception:
                pass
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


# ── Backfill missed webhooks ───────────────────────────────────────────────────

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