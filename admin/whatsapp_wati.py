import os
import re
import requests

from flask import render_template, request, jsonify

from database import db
from . import admin_bp
from admin.views import admin_required


# ── Configuration ─────────────────────────────────────────────────────────────
#
# WATI WhatsApp Business API
# Docs: https://docs.wati.io
#
# Required env vars:
#   WATI_API_ENDPOINT   – your tenant endpoint, e.g. https://live-mt-server.wati.io/<tenant_id>
#   WATI_ACCESS_TOKEN   – Bearer token from WATI dashboard
#
# Endpoints used:
#   POST /api/v1/sendSessionMessage/{whatsappNumber}  – free-form session message
#   POST /api/v1/sendTemplateMessage/{whatsappNumber} – approved template message
#   GET  /api/v1/getContacts                          – search contacts in WATI
#   GET  /api/v1/getMessages/{whatsappNumber}         – message history for a contact
# ─────────────────────────────────────────────────────────────────────────────

WATI_API_ENDPOINT  = os.environ.get("WATI_API_ENDPOINT", "").rstrip("/")
WATI_ACCESS_TOKEN  = os.environ.get("WATI_ACCESS_TOKEN", "")


def _headers() -> dict:
    if not WATI_ACCESS_TOKEN:
        raise RuntimeError("WATI_ACCESS_TOKEN is not configured")
    return {
        "Authorization": f"Bearer {WATI_ACCESS_TOKEN}",
        "Content-Type":  "application/json",
    }


def _base() -> str:
    if not WATI_API_ENDPOINT:
        raise RuntimeError("WATI_API_ENDPOINT is not configured")
    return WATI_API_ENDPOINT


def _users_col():
    return db.users


# ── Helpers ───────────────────────────────────────────────────────────────────

def _normalise_phone(phone: str) -> str:
    """
    Strip everything except digits and a leading '+'.
    WATI expects E.164 without the '+', e.g. '353851234567'.
    """
    digits = re.sub(r"[^\d]", "", phone)
    return digits


def _send_session_message(phone: str, message: str) -> dict:
    url = f"{_base()}/api/v1/sendSessionMessage/{_normalise_phone(phone)}"
    resp = requests.post(
        url,
        headers=_headers(),
        params={"messageText": message},   # ← query param, not json=
        timeout=10,
    )
    return resp.text
    resp.raise_for_status()
    data = resp.json()

    # WATI returns 200 even on logical failures — check the result flag
    if data.get("result") is False:
        msg = data.get("message", "Unknown WATI error")
        status = data.get("ticketStatus", "")
        if "expired" in msg.lower() or status == "BROADCAST":
            raise ValueError(
                "Session window expired (>24 h since last inbound message). "
                "Use a Template Message instead."
            )
        raise ValueError(msg)

    return data


def _send_template_message(phone: str, template_name: str, parameters: list[dict]) -> dict:
    """
    POST /api/v1/sendTemplateMessage
    Phone goes in the JSON body as 'whatsappNumber', NOT as a query param.
    """
    url = f"{_base()}/api/v1/sendTemplateMessage?whatsappNumber={_normalise_phone(phone)}"
    payload = {
        "template_name":  template_name,
        "broadcast_name": template_name,
        "parameters":     parameters,
    }
    resp = requests.post(url, headers=_headers(), json=payload, timeout=10)

    #return resp.text
    resp.raise_for_status()
    data = resp.json()


    # WATI returns HTTP 200 even on logical failures
    if data.get("result") is False:
        raise ValueError(data.get("message") or "WATI template send failed")

    return data

def _get_contacts(search: str = "", page_size: int = 20, page: int = 1) -> dict:
    """
    GET /api/v1/getContacts?pageSize=&page=&name=
    """
    url = f"{_base()}/api/v1/getContacts"
    params = {"pageSize": page_size, "page": page}
    if search:
        params["name"] = search
    resp = requests.get(url, headers=_headers(), params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _get_messages(phone: str, page_size: int = 20, page: int = 1) -> dict:
    """
    GET /api/v1/getMessages/{whatsappNumber}
    """
    url = f"{_base()}/api/v1/getMessages/{_normalise_phone(phone)}"
    resp = requests.get(
        url,
        headers=_headers(),
        params={"pageSize": page_size, "page": page},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


# ── Routes ────────────────────────────────────────────────────────────────────

@admin_bp.route("/whatsapp_wati")
@admin_required
def whatsapp_wati():
    return render_template("admin/whatsapp_wati.html")


@admin_bp.route("/whatsapp_wati/send_session", methods=["POST"])
@admin_required
def whatsapp_wati_send_session():
    """
    POST /admin/whatsapp_wati/send_session
    Body: { "phone": "353851234567", "message": "Hello!" }

    Sends a free-form session message via WATI.

    Response:
        { "success": true, "result": { ...wati response... } }
    """
    data    = request.get_json(force=True) or {}
    phone   = (data.get("phone") or "").strip()
    message = (data.get("message") or "").strip()

    if not phone:
        return jsonify({"success": False, "error": "phone is required"}), 400
    if not message:
        return jsonify({"success": False, "error": "message is required"}), 400

    try:
        result = _send_session_message(phone, message)
        return jsonify({"success": True, "result": result})
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "error": "WATI request timed out"}), 504
    except requests.exceptions.HTTPError as e:
        return jsonify({"success": False, "error": f"WATI error {e.response.status_code}: {e.response.text}"}), 502
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "error": f"WATI request failed: {e}"}), 502
    except RuntimeError as e:
        return jsonify({"success": False, "error": str(e)}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route("/whatsapp_wati/send_template", methods=["POST"])
@admin_required
def whatsapp_wati_send_template():
    data          = request.get_json(force=True) or {}
    phone         = (data.get("phone")         or "").strip()
    template_name = (data.get("template_name") or "").strip()
    parameters    = data.get("parameters") or []

    if not phone:
        return jsonify({"success": False, "error": "phone is required"}), 400
    if not template_name:
        return jsonify({"success": False, "error": "template_name is required"}), 400

    try:
        result = _send_template_message(phone, template_name, parameters)
        return jsonify({"success": True, "result": result})
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 422
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "error": "WATI request timed out"}), 504
    except requests.exceptions.HTTPError as e:
        return jsonify({"success": False, "error": f"WATI error {e.response.status_code}: {e.response.text}"}), 502
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "error": f"WATI request failed: {e}"}), 502
    except RuntimeError as e:
        return jsonify({"success": False, "error": str(e)}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route("/whatsapp_wati/contacts")
@admin_required
def whatsapp_wati_contacts():
    """
    GET /admin/whatsapp_wati/contacts?search=<name>&page=1&page_size=20

    Proxies WATI contact search and returns a normalised list.

    Response:
        {
            "success":  true,
            "contacts": [
                {
                    "id":        "...",
                    "phone":     "353851234567",
                    "name":      "John Doe",
                    "opted_in":  true,
                    "tags":      ["vip"]
                },
                ...
            ],
            "total": 42
        }
    """
    search    = request.args.get("search", "").strip()
    page      = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 20))

    try:
        raw       = _get_contacts(search=search, page_size=page_size, page=page)
        raw_list  = raw.get("contact_list") or raw.get("contacts") or []
        contacts  = [
            {
                "id":       c.get("id", ""),
                "phone":    c.get("wAid") or c.get("phone", ""),
                "name":     c.get("fullName") or c.get("name", ""),
                "opted_in": c.get("optedIn", False),
                "tags":     c.get("tags") or [],
            }
            for c in raw_list
        ]
        return jsonify({"success": True, "contacts": contacts, "total": raw.get("total", len(contacts))})
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "error": "WATI request timed out"}), 504
    except requests.exceptions.HTTPError as e:
        return jsonify({"success": False, "error": f"WATI error {e.response.status_code}: {e.response.text}"}), 502
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "error": f"WATI request failed: {e}"}), 502
    except RuntimeError as e:
        return jsonify({"success": False, "error": str(e)}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route("/whatsapp_wati/messages")
@admin_required
def whatsapp_wati_messages():
    """
    GET /admin/whatsapp_wati/messages?phone=<number>&page=1&page_size=20

    Retrieves message history for a WhatsApp contact from WATI.

    Response:
        {
            "success":  true,
            "messages": [
                {
                    "id":         "...",
                    "text":       "Hello!",
                    "type":       "text",
                    "direction":  "inbound" | "outbound",
                    "status":     "read" | "delivered" | "sent" | "failed",
                    "created_at": "2024-01-01T12:00:00"
                },
                ...
            ]
        }
    """
    phone     = request.args.get("phone", "").strip()
    page      = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 20))

    if not phone:
        return jsonify({"success": False, "error": "phone parameter is required"}), 400

    try:
        raw      = _get_messages(phone, page_size=page_size, page=page)
        raw_msgs = raw.get("messages") or raw.get("data") or []
        messages = [
            {
                "id":         m.get("id", ""),
                "text":       m.get("text") or m.get("body") or "",
                "type":       m.get("type", "text"),
                "direction":  "inbound" if m.get("owner") is False or m.get("direction") == "inbound" else "outbound",
                "status":     m.get("statusString") or m.get("status", ""),
                "created_at": m.get("created") or m.get("created_at", ""),
            }
            for m in raw_msgs
        ]
        return jsonify({"success": True, "messages": messages})
    except requests.exceptions.Timeout:
        return jsonify({"success": False, "error": "WATI request timed out"}), 504
    except requests.exceptions.HTTPError as e:
        return jsonify({"success": False, "error": f"WATI error {e.response.status_code}: {e.response.text}"}), 502
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "error": f"WATI request failed: {e}"}), 502
    except RuntimeError as e:
        return jsonify({"success": False, "error": str(e)}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route("/whatsapp_wati/search_users")
@admin_required
def whatsapp_wati_search_users():
    """
    GET /admin/whatsapp_wati/search_users?q=<query>

    Searches local MongoDB users by phone / name / email (up to 50 results).
    Each result is enriched with a WATI contact lookup if a phone is present.

    Response:
        { "success": true, "users": [ {...user, "wati": {...} | null }, ... ] }
    """
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"success": True, "users": []})

    pattern     = re.compile(re.escape(q), re.IGNORECASE)
    mongo_query = {
        "$or": [
            {"phone":      pattern},
            {"first_name": pattern},
            {"last_name":  pattern},
            {"email":      pattern},
        ]
    }

    try:
        items = list(
            _users_col()
            .find(mongo_query, {
                "_id":        1,
                "xn_user_id": 1,
                "first_name": 1,
                "last_name":  1,
                "email":      1,
                "phone":      1,
                "country":    1,
                "city":       1,
                "created_at": 1,
            })
            .sort([("created_at", -1)])
            .limit(50)
        )

        for u in items:
            u["_id"] = str(u["_id"])
            if "created_at" in u and hasattr(u["created_at"], "isoformat"):
                u["created_at"] = u["created_at"].isoformat()

            # Enrich with WATI contact data if phone is present
            if WATI_ACCESS_TOKEN and WATI_API_ENDPOINT and u.get("phone"):
                try:
                    raw      = _get_contacts(search=u["phone"], page_size=1)
                    raw_list = raw.get("contact_list") or raw.get("contacts") or []
                    if raw_list:
                        c = raw_list[0]
                        u["wati"] = {
                            "id":       c.get("id", ""),
                            "opted_in": c.get("optedIn", False),
                            "tags":     c.get("tags") or [],
                        }
                    else:
                        u["wati"] = None
                except Exception:
                    u["wati"] = None

        return jsonify({"success": True, "users": items})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500