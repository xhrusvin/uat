from flask import jsonify, request
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import math
from datetime import datetime
from bson import ObjectId
from bson.errors import InvalidId
from collections import Counter

from . import bp

load_dotenv()

# ==================== CONFIG ====================
MONGO_URI             = os.getenv('MONGO_URI')
DB_NAME               = os.getenv('DB_NAME')
USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_WEBHOOK_KEY')
APP_COUNTRY           = os.getenv('XN_APP_COUNTRY', 'ie')

DEFAULT_PAGE     = 1
DEFAULT_PAGE_SIZE = 10
MAX_PAGE_SIZE    = 100

# ==================== TEST SHIFT EXCLUSION LIST ====================
# Set EXCLUDE_TEST_SHIFTS = True to filter out the shifts listed in
# TEST_SHIFT_IDS from results. Set to False to include them.
EXCLUDE_TEST_SHIFTS = True

TEST_SHIFT_IDS = [
    # "TEST-001",
    # "SHIFT-XN-9999",
]

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db           = mongo_client[DB_NAME]
users_col    = db['users']
shifts_col   = db['shifts']


# ==================== HELPERS ====================
def _haversine_km(lat1, lng1, lat2, lng2):
    """Distance in km between two lat/lng points."""
    R = 6371
    try:
        from math import radians, sin, cos, sqrt, atan2
        dlat = radians(lat2 - lat1)
        dlng = radians(lng2 - lng1)
        a    = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng/2)**2
        return R * 2 * atan2(sqrt(a), sqrt(1 - a))
    except Exception:
        return None


def _serialize_shift(doc, score=None, score_reasons=None):
    def _dt(val):
        if isinstance(val, datetime):
            return val.isoformat()
        return val

    d = {
        "shift_id":           str(doc.get("_id")),
        "name":               doc.get("name"),
        "shift_xn_id":        doc.get("shift_xn_id"),
        "shift_code":         doc.get("shift_code"),
        "date":               _dt(doc.get("date")),
        "start_time":         doc.get("start_time"),
        "end_time":           doc.get("end_time"),
        "shift_timing":       doc.get("shift_timing"),
        "status":             doc.get("status"),
        "rate":               doc.get("rate"),
        "user_type":          doc.get("user_type"),
        "unit":               doc.get("unit"),
        "location":           doc.get("location"),
        "client_id":          doc.get("client_id"),
        "client_name":        doc.get("client_name"),
        "client_county":      doc.get("client_county"),
        "slots":              doc.get("slots", []),
        "created_at":         _dt(doc.get("created_at")),
        "updated_at":         _dt(doc.get("updated_at")),
    }
    if score is not None:
        d["match_score"]   = round(score, 2)
        d["match_reasons"] = score_reasons or []
    return d


def _get_user_history(user_doc):
    """
    Analyse user's shift history from shifts_users (availability=1 = worked).
    Returns:
        - top_counties: most worked counties
        - top_clients: most worked client names
        - top_shift_timings: preferred shift timings (morning/night/day)
        - worked_shift_ids: set of shift _ids user worked
    """
    user_oid = user_doc.get("_id")
    worked_records = list(db["shifts_users"].find(
        {"user_id": user_oid, "availability": 1},
        {"shift_id": 1}
    ))
    worked_shift_oids = [r["shift_id"] for r in worked_records if r.get("shift_id")]

    top_counties      = Counter()
    top_clients       = Counter()
    top_shift_timings = Counter()

    if worked_shift_oids:
        for s in shifts_col.find(
            {"_id": {"$in": worked_shift_oids}},
            {"client_county": 1, "client_name": 1, "shift_timing": 1}
        ):
            if s.get("client_county"):
                top_counties[s["client_county"]] += 1
            if s.get("client_name"):
                top_clients[s["client_name"]] += 1
            if s.get("shift_timing"):
                top_shift_timings[s["shift_timing"]] += 1

    return {
        "worked_shift_oids":  set(str(o) for o in worked_shift_oids),
        "top_counties":       top_counties,
        "top_clients":        top_clients,
        "top_shift_timings":  top_shift_timings,
    }


def _score_shift(shift, user_doc, history, user_lat, user_lng):
    """
    Score a shift for a user. Higher = better match.

    Scoring weights:
        County match (worked before)   : +30
        Client match (worked before)   : +20
        Shift timing preference        : +20
        Distance (closer = higher)     : 0–25
        County = user's home county    : +15
        Already worked this shift      : -50 (deprioritise repeat)
    """
    score   = 0
    reasons = []

    shift_id_str   = str(shift.get("_id", ""))
    shift_county   = (shift.get("client_county") or "").strip()
    shift_client   = (shift.get("client_name") or "").strip()
    shift_timing   = (shift.get("shift_timing") or "").strip()

    # Priority 1: Distance <5km (+50)
    if user_lat and user_lng:
        shift_lat = shift.get("latitude") or shift.get("client_lat")
        shift_lng = shift.get("longitude") or shift.get("client_lng")
        if not (shift_lat and shift_lng):
            client = db["clients"].find_one(
                {"xn_client_id": str(shift.get("client_id", ""))},
                {"latitude": 1, "longitude": 1}
            )
            if client:
                shift_lat = client.get("latitude")
                shift_lng = client.get("longitude")

        if shift_lat and shift_lng:
            try:
                dist_km = _haversine_km(float(user_lat), float(user_lng), float(shift_lat), float(shift_lng))
                if dist_km is not None:
                    if dist_km <= 5:       # Priority 1
                        pts = 50
                    elif dist_km <= 20:    # Priority 5
                        pts = int(30 - (dist_km - 5) * 1.5)
                    elif dist_km <= 50:    # Priority 7
                        pts = int(10 - (dist_km - 20) * 0.3)
                    elif dist_km <= 100:
                        pts = max(0, int(2 - (dist_km - 50) * 0.04))
                    else:
                        pts = 0
                    score += pts
                    reasons.append(f"Distance: {dist_km:.1f}km (+{pts})")
            except Exception:
                pass

    # Priority 2: Preferred shift timing (+40)
    if shift_timing and history["top_shift_timings"].get(shift_timing, 0) > 0:
        times = history["top_shift_timings"][shift_timing]
        pts   = min(40, 25 + times * 3)
        score += pts
        reasons.append(f"Preferred timing: {shift_timing} {times}x (+{pts})")

    # Priority 3: Home county match (+35)
    user_county = (user_doc.get("county") or "").strip()
    if shift_county and user_county and shift_county.lower() == user_county.lower():
        score += 35
        reasons.append(f"Home county match: {shift_county} (+35)")

    # Priority 4: Worked at this client before (+30)
    if shift_client and history["top_clients"].get(shift_client, 0) > 0:
        times = history["top_clients"][shift_client]
        pts   = min(30, 18 + times * 2)
        score += pts
        reasons.append(f"Worked at {shift_client} {times}x (+{pts})")

    # Priority 6: Worked in this county before (+20)
    if shift_county and history["top_counties"].get(shift_county, 0) > 0:
        times = history["top_counties"][shift_county]
        pts   = min(20, 10 + times * 2)
        score += pts
        reasons.append(f"Worked in {shift_county} {times}x (+{pts})")

    # Deprioritise if already worked this exact shift
    if shift_id_str in history["worked_shift_oids"]:
        score -= 50
        reasons.append("Already worked this shift (-50)")

    return score, reasons


# ==================== ROUTE ====================
@bp.route("/most-matching-shifts", methods=["POST"])
def most_matching_shifts():
    """
    Returns shifts ranked by match score for a staff member.

    Scoring factors:
      - County match (previously worked)
      - Client match (previously worked)
      - Shift timing preference
      - Distance from user's location
      - Home county match
    """
    try:
        # 1. Auth
        api_key = request.headers.get("Api-Key")
        if api_key != USER_EXTERNAL_API_KEY:
            return jsonify({"status": "error", "message": "Invalid or missing Api-Key"}), 401

        # 2. Payload
        data    = request.get_json(silent=True) or {}
        user_id = str(data.get("user_id", "")).strip()
        if not user_id:
            return jsonify({"status": "error", "message": "Missing required field: user_id"}), 400

        page      = max(int(data.get("page", DEFAULT_PAGE)), 1)
        page_size = min(int(data.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE)

        # 3. Find user
        or_conds = [{"xn_user_id": user_id}, {"user_id": user_id}]
        try:
            or_conds.append({"_id": ObjectId(user_id)})
        except (InvalidId, TypeError):
            pass
        user_doc = users_col.find_one({"$or": or_conds})
        if not user_doc:
            return jsonify({"status": "error", "message": f"User not found: {user_id}"}), 404

        # 4. User details
        user_details = {
            "user_id":           str(user_doc.get("_id")),
            "xn_user_id":        user_doc.get("xn_user_id"),
            "first_name":        user_doc.get("first_name"),
            "last_name":         user_doc.get("last_name"),
            "email":             user_doc.get("email"),
            "designation":       user_doc.get("designation"),
            "rating":            user_doc.get("rating"),
            "status":            user_doc.get("status"),
            "user_type_id":      str(user_doc.get("user_type_id")) if user_doc.get("user_type_id") else None,
        }

        # 5. User location
        user_lat = user_lng = None
        _loc = user_doc.get("location") or {}
        if isinstance(_loc, dict):
            user_lat = _loc.get("latitude") or _loc.get("lat")
            user_lng = _loc.get("longitude") or _loc.get("lng")

        # 6. Analyse history
        history = _get_user_history(user_doc)

        # 7. Candidate shifts — To Be Filled, matching user_type_id
        user_type_id = user_doc.get("user_type_id")
        shift_filter = {"upstream_status": "To Be Filled"}
        if user_type_id:
            variants = [user_type_id]
            try:
                variants.append(ObjectId(str(user_type_id)) if not isinstance(user_type_id, ObjectId) else str(user_type_id))
            except Exception:
                pass
            shift_filter["user_type_id"] = {"$in": variants}

        # Fetch up to 500 candidates then score + sort in Python
        candidates = list(shifts_col.find(shift_filter).sort("date", 1).limit(500))

        # 8. Score all candidates, excluding test shifts if flag is enabled
        test_ids_set = set(TEST_SHIFT_IDS)
        scored = []
        for shift in candidates:
            if EXCLUDE_TEST_SHIFTS and (
                shift.get("shift_xn_id") in test_ids_set or
                shift.get("shift_code") in test_ids_set
            ):
                continue
            score, reasons = _score_shift(shift, user_doc, history, user_lat, user_lng)
            scored.append((shift, score, reasons))

        # Sort by score descending, then date ascending
        scored.sort(key=lambda x: (-x[1], x[0].get("date") or datetime.max))

        # 9. Paginate
        total        = len(scored)
        total_pages  = math.ceil(total / page_size) if page_size else 0
        skip         = (page - 1) * page_size
        paged        = scored[skip: skip + page_size]

        shifts_out = [_serialize_shift(s, sc, r) for s, sc, r in paged]

        return jsonify({
            "status":  "success",
            "message": "Most matching shifts retrieved successfully",
            "user":    user_details,
            "scoring_factors": {
                "top_counties":       dict(history["top_counties"].most_common(5)),
                "top_clients":        dict(history["top_clients"].most_common(5)),
                "top_shift_timings":  dict(history["top_shift_timings"].most_common(3)),
                "total_shifts_worked": len(history["worked_shift_oids"]),
            },
            "shifts": shifts_out,
            "pagination": {
                "page":        page,
                "page_size":   page_size,
                "total_items": total,
                "total_pages": total_pages,
            },
            "timestamp": datetime.utcnow().isoformat(),
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500