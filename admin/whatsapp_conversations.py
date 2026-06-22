from flask import jsonify, request, render_template
import os, requests as http_requests

from . import admin_bp
from admin.views import admin_required

from .whatsapp_wati import (
    _get_messages,
    _normalise_phone
)

WATI_API_ENDPOINT = os.environ.get("WATI_API_ENDPOINT", "").rstrip("/")
WATI_ACCESS_TOKEN = os.environ.get("WATI_ACCESS_TOKEN", "")


def _wati_headers():
    return {
        "Authorization": f"Bearer {WATI_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }


@admin_bp.route("/whatsapp/conversations")
@admin_required
def whatsapp_conversations():
    return render_template("admin/whatsapp_conversations.html")


@admin_bp.route("/api/whatsapp/recent_conversations")
@admin_required
def whatsapp_recent_conversations():
    """
    Returns the last 10 contacts sorted by most-recent message activity,
    using WATI's GET /api/ext/v3/contacts endpoint.
    """
    try:
        url = f"{WATI_API_ENDPOINT}/api/ext/v3/contacts"
        resp = http_requests.get(
            url,
            headers=_wati_headers(),
            params={"page_number": 1, "page_size": 10},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        # WATI returns { contact_list: [...] } or { contacts: [...] }
        raw = (
            data.get("contact_list")
            or data.get("contacts")
            or data.get("items")
            or []
        )

        contacts = []
        for c in raw:
            phone = (
                c.get("waBid")
                or c.get("phone")
                or c.get("whatsappNumber")
                or ""
            )
            first = c.get("firstName") or c.get("first_name") or ""
            last  = c.get("lastName")  or c.get("last_name")  or ""
            name  = f"{first} {last}".strip() or phone

            contacts.append({
                "phone":      phone,
                "name":       name,
                "first_name": first,
                "last_name":  last,
                "email":      c.get("email") or "",
                "last_seen":  c.get("lastUpdated") or c.get("updatedAt") or "",
                "last_message": c.get("lastMessage") or c.get("last_message") or "",
            })

        return jsonify({"success": True, "contacts": contacts})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route("/api/whatsapp/conversation")
@admin_required
def whatsapp_conversation_api():

    phone = request.args.get("phone", "").strip()

    if not phone:
        return jsonify({
            "success": False,
            "error": "phone required"
        }), 400

    try:
        raw = _get_messages(
            phone,
            page_size=100,
            page=1
        )

        raw_msgs = (
            raw.get("messages") or {}
        ).get("items") or []

        messages = []

        for m in raw_msgs:

            event_type = m.get("eventType")

            if event_type not in [
                "message",
                "broadcastMessage"
            ]:
                continue

            messages.append({
                "id": m.get("id"),
                "text": (
                    m.get("text")
                    or m.get("finalText")
                    or ""
                ),
                "direction": (
                    "inbound"
                    if m.get("owner") is False
                    else "outbound"
                ),
                "status": m.get("statusString"),
                "created_at": m.get("created"),
                "type": event_type
            })

        messages.sort(
            key=lambda x: x["created_at"]
        )

        return jsonify({
            "success": True,
            "phone": _normalise_phone(phone),
            "messages": messages
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
