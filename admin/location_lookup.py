#location_lookup.py
import os
import re
import requests

from flask import render_template, request, jsonify

from database import db
from . import admin_bp
from admin.views import admin_required


GOOGLE_MAPS_API_KEY = os.environ.get("GOOGLE_MAPS_API_KEY", "")
GEOCODE_URL = "https://maps.googleapis.com/maps/api/geocode/json"


def _users_col():
    return db.users


# ── Helpers ──────────────────────────────────────────────────────────

def _geocode_postcode(postcode: str) -> dict | None:
    """
    Call the Google Maps Geocoding API for a postcode / EIR code.
    Returns the first result dict, or None on failure.
    """
    params = {
        "address": postcode,
        "key": GOOGLE_MAPS_API_KEY,
    }
    resp = requests.get(GEOCODE_URL, params=params, timeout=5)
    resp.raise_for_status()
    data = resp.json()

    if data.get("status") != "OK" or not data.get("results"):
        return None

    return data["results"][0]


def _extract_locationgoogle(geocode_result: dict) -> dict:
    """
    Flatten a single Geocoding API result into a clean dict.
    """
    geometry = geocode_result.get("geometry", {})
    location = geometry.get("location", {})

    components = {}
    for comp in geocode_result.get("address_components", []):
        for t in comp["types"]:
            components[t] = comp["long_name"]

    return {
        "formatted_address": geocode_result.get("formatted_address", ""),
        "lat": location.get("lat"),
        "lng": location.get("lng"),
        "country":       components.get("country", ""),
        "country_code":  components.get("country", ""),          # overwritten below
        "county":        components.get("administrative_area_level_1", ""),
        "city":          components.get("locality")
                         or components.get("postal_town", ""),
        "postcode":      components.get("postal_code", ""),
        "place_id":      geocode_result.get("place_id", ""),
    }


# ── Routes ───────────────────────────────────────────────────────────

@admin_bp.route("/location_lookup")
@admin_required
def location_lookup():
    return render_template("admin/location_lookup.html")


@admin_bp.route("/location_lookup/by_postcode")
@admin_required
def location_lookup_by_postcode():
    """
    GET /admin/location_lookup/by_postcode?postcode=<code>

    Resolves a postcode / EIR code to geographic coordinates and
    address components via the Google Maps Geocoding API.

    Returns:
        {
            "success": true,
            "location": {
                "formatted_address": "...",
                "lat": 53.3498,
                "lng": -6.2603,
                "country": "Ireland",
                "county": "County Dublin",
                "city": "Dublin",
                "postcode": "D01 F5P2",
                "place_id": "ChIJ..."
            }
        }
    """
    postcode = request.args.get("postcode", "").strip()

    if not postcode:
        return jsonify({"success": False, "error": "postcode parameter is required"}), 400

    if not GOOGLE_MAPS_API_KEY:
        return jsonify({"success": False, "error": "GOOGLE_MAPS_API_KEY is not configured"}), 500

    try:
        result = _geocode_postcode(postcode)
        if result is None:
            return jsonify({"success": False, "error": "No results found for that postcode"}), 404

        location = _extract_locationgoogle(result)
        return jsonify({"success": True, "location": location})

    except requests.exceptions.Timeout:
        return jsonify({"success": False, "error": "Geocoding request timed out"}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"success": False, "error": f"Geocoding request failed: {e}"}), 502
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route("/location_lookup/search")
@admin_required
def location_lookup_search():
    """
    GET /admin/location_lookup/search?q=<query>

    Searches users by postcode/country, then enriches each result with
    live location data from the Google Maps API.
    Returns up to 50 results as JSON.
    """
    q = request.args.get("q", "").strip()

    if not q:
        return jsonify({"success": True, "users": []})

    pattern = re.compile(re.escape(q), re.IGNORECASE)
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

        # Serialize ObjectId and dates
        for u in items:
            u["_id"] = str(u["_id"])
            if "created_at" in u and hasattr(u["created_at"], "isoformat"):
                u["created_at"] = u["created_at"].isoformat()

            # Optionally enrich with live geocode data if user has a postcode
            if GOOGLE_MAPS_API_KEY and u.get("postcode"):
                try:
                    geo = _geocode_postcode(u["postcode"])
                    if geo:
                        u["geo"] = _extract_location(geo)
                except Exception:
                    u["geo"] = None  # non-fatal; skip enrichment

        return jsonify({"success": True, "users": items})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500