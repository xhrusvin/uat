# admin/care_leads.py
#
# Self-contained Care Leads (from Facebook Lead Ads) → WhatsApp (WATI) module.
# Does NOT touch: admin/leads.py, admin/whatsapp_wati.py, db.leads,
# templates/admin/leads.html — everything lives in its own collections
# (db.care_leads, db.care_lead_replies) and its own page (/admin/care_leads).
#
# FLOW:
#   FB Lead Ad submitted → Meta webhook → save to db.care_leads
#     → send WATI template → lead replies on WhatsApp
#     → WATI webhook → captured into db.care_lead_replies → shown in new UI
#
# Required env vars:
#   FB_VERIFY_TOKEN        – secret string; same value in Meta App Dashboard
#                            → Webhooks → Verify Token
#   FB_PAGE_ACCESS_TOKEN   – Page token with leads_retrieval permission
#   WATI_WEBHOOK_SECRET    – secret string; append to the WATI webhook URL
#   WATI_LEAD_TEMPLATE     – template to send new leads (default: new_chat_v1)
#
# Register in your app factory:
#   from admin import care_leads                    # registers admin routes
#   app.register_blueprint(care_leads.fb_webhooks_bp) # public webhook routes
#
# Webhook URLs to configure:
#   Meta  → https://yourdomain.com/webhooks/facebook/leads   (field: leadgen)
#   WATI  → https://yourdomain.com/webhooks/wati/fb?secret=<WATI_WEBHOOK_SECRET>
#           (event: Message Received)

import json
import os
import re
from datetime import datetime

import requests
from bson import ObjectId
from flask import (Blueprint, current_app, jsonify, render_template, request)

from . import admin_bp
from .views import admin_required
from .whatsapp_wati import _send_template_message

fb_webhooks_bp = Blueprint("fb_webhooks", __name__, url_prefix="/webhooks")

FB_VERIFY_TOKEN      = os.environ.get("CARE_VERIFY_TOKEN", "")
FB_PAGE_ACCESS_TOKEN = os.environ.get("CARE_PAGE_ACCESS_TOKEN", "")
WATI_WEBHOOK_SECRET  = os.environ.get("WATI_WEBHOOK_SECRET", "")
WATI_LEAD_TEMPLATE   = os.environ.get("WATI_LEAD_TEMPLATE", "new_chat_v1")

FB_GRAPH_URL = "https://graph.facebook.com/v21.0"

# Every webhook hit (FB verify, FB leadgen, WATI reply) is appended here.
# Watch it live with:  tail -f /tmp/care_webhook_debug.log
WEBHOOK_DEBUG_LOG = os.environ.get(
    "CARE_WEBHOOK_DEBUG_LOG", "/tmp/care_webhook_debug.log"
)


def _webhook_debug(tag, payload=None, note=None):
    """Append one webhook event to the debug log file. Never raises."""
    try:
        with open(WEBHOOK_DEBUG_LOG, "a") as f:
            f.write("=" * 80 + "\n")
            f.write(f"[{datetime.utcnow().isoformat()}Z] {tag}\n")
            if request:  # only when inside an HTTP request context
                f.write(f"  remote_addr : {request.remote_addr}\n")
                f.write(f"  method/path : {request.method} {request.full_path}\n")
                ua = request.headers.get("User-Agent", "-")
                f.write(f"  user-agent  : {ua}\n")
            if note:
                f.write(f"  note        : {note}\n")
            if payload is not None:
                f.write(json.dumps(payload, indent=2, default=str) + "\n")
    except Exception:
        current_app.logger.exception("webhook debug log write failed")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalize_phone(phone_str):
    if not phone_str:
        return None
    cleaned = re.sub(r"\D", "", str(phone_str))
    return cleaned if len(cleaned) >= 10 else None


def _first(fields, *names):
    for n in names:
        v = fields.get(n)
        if v:
            return v
    return None


def _fetch_fb_lead(leadgen_id):
    """GET /{leadgen_id} from Graph API; flatten field_data → dict."""
    resp = requests.get(
        f"{FB_GRAPH_URL}/{leadgen_id}",
        params={
            "access_token": FB_PAGE_ACCESS_TOKEN,
            "fields": "id,created_time,ad_id,form_id,field_data",
        },
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    fields = {}
    for item in data.get("field_data", []):
        values = item.get("values") or []
        fields[(item.get("name") or "").lower()] = values[0] if values else None
    return data, fields


def _send_whatsapp_to_care_lead(db, lead, template_name=None):
    """Send WATI template to one care lead and record the outcome. Never raises."""
    template_name = template_name or WATI_LEAD_TEMPLATE
    phone = lead.get("phone_number")
    if not phone:
        return False
    try:
        result = _send_template_message(
            phone=phone, template_name=template_name, parameters=[]
        )
        db.care_leads.update_one(
            {"_id": lead["_id"]},
            {"$set": {
                "whatsapp_sent": True,
                "whatsapp_sent_at": datetime.utcnow(),
                "whatsapp_status": "sent",
                "whatsapp_template": template_name,
                "whatsapp_message_id":
                    result.get("id") or result.get("messageId"),
            }},
        )
        return True
    except Exception as e:
        current_app.logger.exception(f"WATI send failed for care_lead {lead['_id']}")
        db.care_leads.update_one(
            {"_id": lead["_id"]},
            {"$set": {"whatsapp_sent": False, "whatsapp_status": str(e)[:500]}},
        )
        return False


def process_fb_lead(db, leadgen_id, page_id=None, ad_id=None, form_id=None):
    """
    Fetch one FB lead, store in db.care_leads, trigger WhatsApp template.
    Idempotent: safe against Meta webhook retries and duplicate phones.
    Also callable from a polling fetcher if you use one.
    """
    if db.care_leads.find_one({"fb_leadgen_id": str(leadgen_id)}):
        _webhook_debug(f"LEAD {leadgen_id}: SKIPPED",
                       note="duplicate fb_leadgen_id — already in db.care_leads")
        return None  # already processed (Meta retries deliveries)

    try:
        raw, fields = _fetch_fb_lead(leadgen_id)
    except Exception as e:
        body = getattr(getattr(e, "response", None), "text", "")
        _webhook_debug(f"LEAD {leadgen_id}: GRAPH FETCH FAILED",
                       note=f"{e} | body={body[:500]}")
        raise
    _webhook_debug(f"LEAD {leadgen_id}: GRAPH FETCH OK", payload=fields)

    raw_phone = _first(
        fields, "phone_number", "phone", "mobile_number", "whatsapp_number"
    )
    phone = _normalize_phone(raw_phone)

    if phone and db.care_leads.find_one({"phone_number": phone}):
        # Record linkage so retries don't refetch, but don't message twice
        db.care_leads.update_one(
            {"phone_number": phone},
            {"$addToSet": {"other_leadgen_ids": str(leadgen_id)}},
        )
        _webhook_debug(f"LEAD {leadgen_id}: SKIPPED",
                       note=f"phone {phone} already exists in db.care_leads")
        return None

    email = (_first(fields, "email", "email_address") or "").strip().lower()
    created_time = raw.get("created_time")
    name = _first(fields, "full_name", "name")

    # Every lead is stored; problems are flagged, not dropped.
    issues = []
    if not raw_phone:
        issues.append("missing_phone")
    elif not phone:
        issues.append("invalid_phone")
    if not name:
        issues.append("missing_name")
    if "@" not in email:
        issues.append("missing_email" if not email else "invalid_email")

    lead = {
        "name": name,
        "email_id": email if "@" in email else None,
        "phone_number": phone,
        "raw_phone": raw_phone if (raw_phone and not phone) else None,
        "data_issues": issues,
        "location": (_first(fields, "location", "city") or "").title() or None,
        "fb_leadgen_id": str(leadgen_id),
        "fb_page_id": str(page_id) if page_id else None,
        "fb_ad_id": str(ad_id) if ad_id else None,
        "fb_form_id": str(form_id) if form_id else None,
        "fb_created_time": created_time,
        "form_fields": fields,                    # full raw form answers
        "received_at": datetime.utcnow(),
        "whatsapp_sent": False,
        "whatsapp_sent_at": None,
        "whatsapp_status": None,
        "whatsapp_template": None,
        "whatsapp_message_id": None,
        "reply_received": False,
        "reply_count": 0,
        "last_reply": None,
        "last_reply_at": None,
    }
    res = db.care_leads.insert_one(lead)
    lead["_id"] = res.inserted_id
    _webhook_debug(f"LEAD {leadgen_id}: SAVED",
                   note=f"_id={res.inserted_id} phone={phone} "
                        f"issues={issues or 'none'}")
    current_app.logger.info(f"FB lead {leadgen_id} saved as {res.inserted_id}")

    if phone:
        ok = _send_whatsapp_to_care_lead(db, lead)
        _webhook_debug(f"LEAD {leadgen_id}: WATI SEND {'OK' if ok else 'FAILED'}",
                       note=None if ok else "see whatsapp_status on the lead")
    else:
        _webhook_debug(f"LEAD {leadgen_id}: WATI SEND SKIPPED", note="no phone")
    return lead


# ── Public webhooks ───────────────────────────────────────────────────────────

@fb_webhooks_bp.route("/facebook/leads", methods=["GET"])
def fb_verify():
    """Meta's one-time subscription verification handshake."""
    if (request.args.get("hub.mode") == "subscribe"
            and request.args.get("hub.verify_token")
            and request.args.get("hub.verify_token") == FB_VERIFY_TOKEN):
        _webhook_debug("FB VERIFY OK", payload=dict(request.args))
        return request.args.get("hub.challenge", ""), 200
    _webhook_debug("FB VERIFY FAILED (token mismatch?)",
                   payload=dict(request.args))
    return "Verification failed", 403


@fb_webhooks_bp.route("/facebook/leads", methods=["POST"])
def fb_leadgen_webhook():
    """Receives Meta `leadgen` events and processes each new lead."""
    payload = request.get_json(silent=True) or {}
    _webhook_debug("FB LEADGEN WEBHOOK RECEIVED", payload=payload)
    if payload.get("object") != "page":
        _webhook_debug("FB LEADGEN IGNORED", note="object != 'page'")
        return jsonify({"status": "ignored"}), 200

    db = current_app.db
    processed = 0
    for entry in payload.get("entry", []):
        for change in entry.get("changes", []):
            if change.get("field") != "leadgen":
                continue
            value = change.get("value") or {}
            leadgen_id = value.get("leadgen_id")
            if not leadgen_id:
                continue
            try:
                if process_fb_lead(
                    db, leadgen_id,
                    page_id=value.get("page_id") or entry.get("id"),
                    ad_id=value.get("ad_id"),
                    form_id=value.get("form_id"),
                ):
                    processed += 1
            except Exception as e:
                _webhook_debug(f"LEAD {leadgen_id}: PROCESSING FAILED",
                               note=repr(e))
                current_app.logger.exception(
                    f"Failed to process FB lead {leadgen_id}"
                )
    # Always ACK with 200 — Meta retries aggressively otherwise
    _webhook_debug("FB LEADGEN DONE", note=f"processed={processed}")
    return jsonify({"status": "ok", "processed": processed}), 200


@fb_webhooks_bp.route("/wati/fb", methods=["POST"])
def wati_reply_webhook():
    """
    WATI "Message Received" webhook — captures a lead's WhatsApp input.
    Only stores messages whose number matches a Facebook lead.
    """
    if WATI_WEBHOOK_SECRET and request.args.get("secret") != WATI_WEBHOOK_SECRET:
        _webhook_debug("WATI WEBHOOK UNAUTHORIZED (bad/missing secret)")
        return jsonify({"error": "unauthorized"}), 403

    payload = request.get_json(silent=True) or {}
    _webhook_debug("WATI WEBHOOK RECEIVED", payload=payload)

    # Inbound customer message only (owner == False)
    if payload.get("eventType") != "message" or payload.get("owner") is not False:
        return jsonify({"status": "ignored"}), 200

    phone = _normalize_phone(payload.get("waId"))
    if not phone:
        return jsonify({"status": "ignored"}), 200

    db = current_app.db
    lead = db.care_leads.find_one({"phone_number": phone})
    if not lead:
        return jsonify({"status": "no_matching_fb_lead"}), 200

    wmid = payload.get("whatsappMessageId") or payload.get("id")
    if wmid and db.care_lead_replies.find_one({"wati_message_id": wmid}):
        return jsonify({"status": "duplicate"}), 200  # WATI redelivery

    text = (payload.get("text") or "").strip()
    db.care_lead_replies.insert_one({
        "lead_id": lead["_id"],
        "phone": phone,
        "sender_name": payload.get("senderName"),
        "text": text,
        "type": payload.get("type", "text"),
        "wati_message_id": wmid,
        "received_at": datetime.utcnow(),
        "raw": payload,
    })
    db.care_leads.update_one(
        {"_id": lead["_id"]},
        {"$set": {
            "reply_received": True,
            "last_reply": text[:1000],
            "last_reply_at": datetime.utcnow(),
        },
         "$inc": {"reply_count": 1}},
    )
    return jsonify({"status": "ok"}), 200


# ── Admin UI + JSON APIs (new page, separate from Leads Management) ──────────

def _serialize_lead(doc):
    return {
        "id": str(doc["_id"]),
        "name": doc.get("name"),
        "email_id": doc.get("email_id"),
        "phone_number": doc.get("phone_number"),
        "raw_phone": doc.get("raw_phone"),
        "data_issues": doc.get("data_issues") or [],
        "location": doc.get("location"),
        "fb_ad_id": doc.get("fb_ad_id"),
        "fb_form_id": doc.get("fb_form_id"),
        "form_fields": doc.get("form_fields") or {},
        "received_at": doc["received_at"].isoformat() if doc.get("received_at") else None,
        "whatsapp_sent": bool(doc.get("whatsapp_sent")),
        "whatsapp_status": doc.get("whatsapp_status"),
        "whatsapp_template": doc.get("whatsapp_template"),
        "whatsapp_sent_at": doc["whatsapp_sent_at"].isoformat() if doc.get("whatsapp_sent_at") else None,
        "reply_received": bool(doc.get("reply_received")),
        "reply_count": doc.get("reply_count", 0),
        "last_reply": doc.get("last_reply"),
        "last_reply_at": doc["last_reply_at"].isoformat() if doc.get("last_reply_at") else None,
    }


@admin_bp.route("/care_leads")
@admin_required
def care_leads_page():
    return render_template("admin/care_leads.html")


@admin_bp.route("/care_leads/data")
@admin_required
def care_leads_data():
    """
    GET /admin/care_leads/data?search=&status=&page=1&per_page=25
    status: '', 'pending', 'sent', 'failed', 'replied'
    """
    db = current_app.db
    page     = max(int(request.args.get("page", 1)), 1)
    per_page = min(int(request.args.get("per_page", 25)), 100)
    search   = request.args.get("search", "").strip()
    status   = request.args.get("status", "").strip()

    query = {}
    if search:
        rx = {"$regex": re.escape(search), "$options": "i"}
        query["$or"] = [
            {"name": rx}, {"phone_number": rx},
            {"email_id": rx}, {"location": rx},
        ]
    if status == "pending":
        query["whatsapp_sent"] = False
        query["whatsapp_status"] = None
        query["phone_number"] = {"$ne": None}
    elif status == "sent":
        query["whatsapp_sent"] = True
    elif status == "failed":
        query["whatsapp_sent"] = False
        query["whatsapp_status"] = {"$nin": [None, "sent"]}
    elif status == "replied":
        query["reply_received"] = True
    elif status == "incomplete":
        query["data_issues"] = {"$exists": True, "$ne": []}

    total = db.care_leads.count_documents(query)
    docs = (db.care_leads.find(query)
            .sort("received_at", -1)
            .skip((page - 1) * per_page)
            .limit(per_page))

    stats = {
        "total":   db.care_leads.count_documents({}),
        "sent":    db.care_leads.count_documents({"whatsapp_sent": True}),
        "failed":  db.care_leads.count_documents(
            {"whatsapp_sent": False, "whatsapp_status": {"$nin": [None, "sent"]}}),
        "replied": db.care_leads.count_documents({"reply_received": True}),
        "incomplete": db.care_leads.count_documents(
            {"data_issues": {"$exists": True, "$ne": []}}),
    }

    return jsonify({
        "success": True,
        "leads": [_serialize_lead(d) for d in docs],
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": max((total + per_page - 1) // per_page, 1),
        "stats": stats,
    })


@admin_bp.route("/care_leads/<lead_id>/replies")
@admin_required
def care_lead_replies(lead_id):
    """Captured WhatsApp input for one lead (chat thread for the modal)."""
    if not ObjectId.is_valid(lead_id):
        return jsonify({"success": False, "error": "Invalid lead ID"}), 400

    db = current_app.db
    lead = db.care_leads.find_one({"_id": ObjectId(lead_id)})
    if not lead:
        return jsonify({"success": False, "error": "Lead not found"}), 404

    replies = list(
        db.care_lead_replies
        .find({"lead_id": lead["_id"]}, {"raw": 0})
        .sort("received_at", 1)
    )
    out = [{
        "id": str(r["_id"]),
        "text": r.get("text"),
        "type": r.get("type", "text"),
        "sender_name": r.get("sender_name"),
        "received_at": r["received_at"].isoformat() if r.get("received_at") else None,
    } for r in replies]

    return jsonify({"success": True, "lead": _serialize_lead(lead), "replies": out})


@admin_bp.route("/care_leads/<lead_id>/resend", methods=["POST"])
@admin_required
def care_lead_resend(lead_id):
    """Manually (re)send the WATI template to one FB lead."""
    if not ObjectId.is_valid(lead_id):
        return jsonify({"success": False, "error": "Invalid lead ID"}), 400

    db = current_app.db
    lead = db.care_leads.find_one({"_id": ObjectId(lead_id)})
    if not lead:
        return jsonify({"success": False, "error": "Lead not found"}), 404
    if not lead.get("phone_number"):
        return jsonify({"success": False,
                        "error": "Lead has no valid phone number",
                        "lead": _serialize_lead(lead)}), 400

    ok = _send_whatsapp_to_care_lead(db, lead)
    fresh = db.care_leads.find_one({"_id": lead["_id"]})
    return jsonify({"success": ok,
                    "error": None if ok else fresh.get("whatsapp_status"),
                    "lead": _serialize_lead(fresh)})
