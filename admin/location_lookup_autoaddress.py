import os
import re
import requests

from flask import render_template, request, jsonify

from database import db
from . import admin_bp
from admin.views import admin_required


# ── Configuration ─────────────────────────────────────────────────────────────
#
# Autoaddress.com REST API v3.0
# Docs: https://api.autoaddress.com
#
# Required env vars:
#   AUTOADDRESS_API_KEY   – your Autoaddress licence key (pub_xxxx...)
#
# Endpoints used:
#   GET  /3.0/autocomplete  – Eircode / address search → returns options list
#   GET  /3.0/lookup        – resolve a verified address link → full address detail
# ─────────────────────────────────────────────────────────────────────────────

AUTOADDRESS_API_KEY  = os.environ.get("AUTOADDRESS_API_KEY", "")
AUTOADDRESS_BASE_URL = "https://api.autoaddress.com"
AUTOCOMPLETE_URL     = f"{AUTOADDRESS_BASE_URL}/3.0/autocomplete"


def _users_col():
    return db.users


# ── Helpers ───────────────────────────────────────────────────────────────────

def _autocomplete(postcode: str) -> list[dict]:
    """
    GET /3.0/autocomplete?address=<postcode>&key=<key>

    Returns the raw 'options' list from the API, e.g.:
    [
        {
            "value": "THE WESTBURY, Balfe Street, Dublin 2",
            "link": {
                "rel":   "lookup",
                "href":  "https://api.autoaddress.com/3.0/lookup?aa3Id=...&token=...&sig=...",
                "title": "THE WESTBURY, Balfe Street, Dublin 2"
            }
        },
        ...
    ]
    """
    if not AUTOADDRESS_API_KEY:
        raise RuntimeError("AUTOADDRESS_API_KEY is not configured")

    resp = requests.get(
        AUTOCOMPLETE_URL,
        params={"address": postcode, "key": AUTOADDRESS_API_KEY},
        timeout=5,
    )
    resp.raise_for_status()
    return resp.json().get("options") or []


def _lookup(href: str) -> dict:
    """
    Follow the signed lookup href that autocomplete returns.
    The href already contains token + sig — just GET it directly.
    """
    resp = requests.get(href, timeout=5)
    resp.raise_for_status()
    return resp.json()


def _resolve(postcode: str) -> dict | None:
    """
    Full two-step resolution:
      1. autocomplete  →  list of address options + signed lookup href
      2. lookup        →  full structured address + lat/lng for first option

    Returns:
        {
            "options":       [raw option dicts from autocomplete],
            "selected":      {full lookup result for first option},
            "display_value": "THE WESTBURY, Balfe Street, Dublin 2"
        }
    or None if autocomplete returns no options.
    """
    options = _autocomplete(postcode)
    if not options:
        return None

    first         = options[0]
    display_value = first.get("value", "")
    lookup_href   = (first.get("link") or {}).get("href", "")

    lookup_result = {}
    if lookup_href:
        try:
            lookup_result = _lookup(lookup_href)
        except Exception:
            pass   # degrade gracefully — display_value is still available

    return {
        "options":       options,
        "selected":      lookup_result,
        "display_value": display_value,
    }


def _extract_location(resolved: dict) -> dict:
    """
    Flatten the combined autocomplete + lookup result into the same clean dict
    shape produced by the Google Maps module, so the existing template and the
    rest of the app see an identical interface.

    Lookup v3.0 response (relevant fields):
    {
        "postcode":  "D02 CH66",
        "addressId": "IE...",
        "paf": {
            "addressLine1": "...",
            "town":         "Dublin 2",
            "county":       "Dublin",
            "country":      "Ireland",
            "eircode":      "D02 CH66",
        },
        "geoData": {
            "latitude":  53.3398,
            "longitude": -6.2593,
        }
    }
    """
    return "Hello"
    selected = resolved.get("selected", {})
    paf      = selected.get("paf", {})
    geo      = selected.get("geoData", {})

    formatted = resolved.get("display_value", "")
    if not formatted:
        parts = [
            paf.get("addressLine1"), paf.get("addressLine2"),
            paf.get("addressLine3"), paf.get("town"),
            paf.get("county"),       paf.get("country"),
        ]
        formatted = ", ".join(p for p in parts if p)

    # Human-readable list of all matching options (just the value strings).
    options_values = [
        opt.get("value", "")
        for opt in resolved.get("options", [])
        if opt.get("value")
    ]

    return {
        "formatted_address": formatted,
        "lat":          geo.get("latitude"),
        "lng":          geo.get("longitude"),
        "country":      paf.get("country", "Ireland"),
        "country_code": "IE",
        "county":       paf.get("county", ""),
        "city":         paf.get("town") or paf.get("addressLine2", ""),
        "postcode":     paf.get("eircode") or selected.get("postcode", ""),
        "place_id":     selected.get("addressId", ""),
        # Extra field — all autocomplete matches, displayed in the UI dropdown.
        "options":      options_values,
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@admin_bp.route("/location_lookup_autoaddress")
@admin_required
def location_lookup_autoaddress():
    return render_template("admin/location_lookup_autoaddress.html")


@admin_bp.route("/location_lookup_autoaddress/by_postcode")
@admin_required
def location_lookup_autoaddress_by_postcode():
    """
    GET /admin/location_lookup_autoaddress/by_postcode?postcode=<eircode>

    Full two-step resolution via Autoaddress v3.0.

    Response (identical shape to Google Maps version + extra 'options' list):
        {
            "success": true,
            "location": {
                "formatted_address": "THE WESTBURY, Balfe Street, Dublin 2",
                "lat":          53.3398,
                "lng":          -6.2593,
                "country":      "Ireland",
                "country_code": "IE",
                "county":       "Dublin",
                "city":         "Dublin 2",
                "postcode":     "D02 CH66",
                "place_id":     "IE...",
                "options": [
                    "THE WESTBURY, Balfe Street, Dublin 2"
                ]
            }
        }
    """
    postcode = request.args.get("postcode", "").strip()
    if not postcode:
        return jsonify({"success": False, "error": "postcode parameter is required"}), 400
    if not AUTOADDRESS_API_KEY:
        return jsonify({"success": False, "error": "AUTOADDRESS_API_KEY is not configured"}), 500

    try:
        resolved = _resolve(postcode)
        if resolved is None:
            return jsonify({"success": False, "error": "No results found for that postcode"}), 404
        return jsonify({"success": True, "location": _extract_location(resolved)})

    except requests.exceptions.Timeout:
        return jsonify({"success": False, "error": "Autoaddress request timed out"}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "error": f"Autoaddress request failed: {e}"}), 502
    except RuntimeError as e:
        return jsonify({"success": False, "error": str(e)}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route("/location_lookup_autoaddress/autocomplete")
@admin_required
def location_lookup_autoaddress_autocomplete():
    """
    GET /admin/location_lookup_autoaddress/autocomplete?postcode=<eircode>

    Lightweight endpoint — returns ONLY the autocomplete options list
    (no follow-up lookup call). Used by the frontend for live typeahead.

    Response:
        {
            "success": true,
            "options": [
                "THE WESTBURY, Balfe Street, Dublin 2",
                "..."
            ]
        }
    """
    postcode = request.args.get("postcode", "").strip()
    if not postcode:
        return jsonify({"success": False, "error": "postcode parameter is required"}), 400
    if not AUTOADDRESS_API_KEY:
        return jsonify({"success": False, "error": "AUTOADDRESS_API_KEY is not configured"}), 500

    try:
        options = _autocomplete(postcode)
        values  = [opt.get("value", "") for opt in options if opt.get("value")]
        return jsonify({"success": True, "options": values})

    except requests.exceptions.Timeout:
        return jsonify({"success": False, "error": "Autoaddress request timed out"}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "error": f"Autoaddress request failed: {e}"}), 502
    except RuntimeError as e:
        return jsonify({"success": False, "error": str(e)}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route("/location_lookup_autoaddress/search")
@admin_required
def location_lookup_autoaddress_search():
    """
    GET /admin/location_lookup_autoaddress/search?q=<query>

    Searches users by postcode / city / country (identical Mongo query to
    the Google Maps version), then enriches each result with live location
    data from Autoaddress v3.0.  Returns up to 50 results as JSON.
    """
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify({"success": True, "users": []})

    pattern     = re.compile(re.escape(q), re.IGNORECASE)
    mongo_query = {
        "$or": [
            {"postcode": pattern},
            {"country":  pattern},
            {"city":     pattern},
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
                "postcode":   1,
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

            if AUTOADDRESS_API_KEY and u.get("postcode"):
                try:
                    raw    = _resolve(u["postcode"])
                    u["geo"] = _extract_location(raw) if raw else None
                except Exception:
                    u["geo"] = None   # non-fatal

        return jsonify({"success": True, "users": items})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500