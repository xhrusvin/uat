from flask import jsonify, request
from pymongo import MongoClient
from dotenv import load_dotenv
import os
import math
from datetime import datetime, timedelta
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

DEFAULT_PAGE      = 1
DEFAULT_PAGE_SIZE = 6
MAX_PAGE_SIZE     = 100

# Unpaid break deducted from each shift when the shift doc has no break value
DEFAULT_BREAK_HOURS = 0.5

# Fallback when neither the shift nor the client defines a check-in radius
DEFAULT_CHECK_IN_DISTANCE = 10000000000000

# Set True to append match_score / match_reasons to each shift (debugging)
INCLUDE_MATCH_DEBUG = False

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
clients_col  = db['clients']


# ==================== RESPONSE ENVELOPE ====================
def _ok(data, message="Success", status_code=200):
    return jsonify({
        "success":     True,
        "data":        data,
        "message":     message,
        "status_code": status_code,
        "errors":      [],
    }), status_code


def _fail(message, status_code=400, errors=None):
    return jsonify({
        "success":     False,
        "data":        None,
        "message":     message,
        "status_code": status_code,
        "errors":      errors or [message],
    }), status_code


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


def _get_client_doc(shift, cache):
    """Fetch (and memoise) the client doc for a shift."""
    cid = str(shift.get("client_id", "") or "")
    if not cid:
        return {}
    if cid not in cache:
        cache[cid] = clients_col.find_one(
            {"xn_client_id": cid},
            {"latitude": 1, "longitude": 1, "check_in_distance": 1, "county": 1, "name": 1}
        ) or {}
    return cache[cid]


def _shift_distance_km(shift, user_lat, user_lng, client_cache):
    """Distance in km from the user to the shift's client, or None."""
    if not (user_lat and user_lng):
        return None

    shift_lat = shift.get("latitude") or shift.get("client_lat")
    shift_lng = shift.get("longitude") or shift.get("client_lng")
    if not (shift_lat and shift_lng):
        client   = _get_client_doc(shift, client_cache)
        shift_lat = client.get("latitude")
        shift_lng = client.get("longitude")

    if not (shift_lat and shift_lng):
        return None

    try:
        return _haversine_km(
            float(user_lat), float(user_lng),
            float(shift_lat), float(shift_lng)
        )
    except (TypeError, ValueError):
        return None


def _as_date(val):
    """Coerce a shift's date field into a datetime.date."""
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%d/%m/%Y"):
            try:
                return datetime.strptime(val[:len(fmt) + 2].strip(), fmt).date()
            except ValueError:
                continue
    return None


def _fmt_date(val):
    """'2026-09-14' -> '14 Sep Mon'."""
    d = _as_date(val)
    return d.strftime("%d %b %a") if d else None


def _parse_time(val):
    """'08:00:00' / '08:00' -> timedelta from midnight, or None."""
    if not val:
        return None
    parts = str(val).strip().split(":")
    try:
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        s = int(float(parts[2])) if len(parts) > 2 else 0
        return timedelta(hours=h, minutes=m, seconds=s)
    except (ValueError, IndexError):
        return None


def _fmt_time(val):
    """Normalise a time string to 'HH:MM:SS'."""
    td = _parse_time(val)
    if td is None:
        return val
    total = int(td.total_seconds())
    return f"{total // 3600:02d}:{(total % 3600) // 60:02d}:{total % 60:02d}"


def _shift_datetimes(shift):
    """
    Build (start_dt, end_dt) as UTC datetimes.
    An end time earlier than the start time rolls over to the next day.
    """
    d     = _as_date(shift.get("date"))
    start = _parse_time(shift.get("start_time"))
    end   = _parse_time(shift.get("end_time"))
    if not d or start is None or end is None:
        return None, None

    midnight = datetime(d.year, d.month, d.day)
    start_dt = midnight + start
    end_dt   = midnight + end
    if end_dt <= start_dt:
        end_dt += timedelta(days=1)
    return start_dt, end_dt


def _iso_z(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ") if dt else None


def _break_hours(shift):
    for key in ("break_hours", "break_duration", "unpaid_break"):
        val = shift.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return DEFAULT_BREAK_HOURS


def _duration_hours(shift):
    """Paid hours = clock hours minus the unpaid break."""
    start_dt, end_dt = _shift_datetimes(shift)
    if not (start_dt and end_dt):
        return None
    hours = (end_dt - start_dt).total_seconds() / 3600.0 - _break_hours(shift)
    return max(0.0, round(hours, 2))


def _hourly_rate(shift):
    for key in ("hourly_rate", "rate", "pay_rate"):
        val = shift.get(key)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                continue
    return None


def _check_in_distance(shift, client_cache):
    val = shift.get("check_in_distance")
    if val is None:
        val = _get_client_doc(shift, client_cache).get("check_in_distance")
    if val is None:
        return DEFAULT_CHECK_IN_DISTANCE
    try:
        return float(val) if isinstance(val, float) else int(val)
    except (TypeError, ValueError):
        return DEFAULT_CHECK_IN_DISTANCE


def _serialize_shift(shift, distance_km, client_cache, score=None, score_reasons=None):
    """Build one entry of the `data.data` array."""
    start_dt, end_dt = _shift_datetimes(shift)
    duration         = _duration_hours(shift)
    hourly_rate      = _hourly_rate(shift)
    county           = shift.get("client_county") or _get_client_doc(shift, client_cache).get("county")

    total_pay_rate = None
    if duration is not None and hourly_rate is not None:
        total_pay_rate = round(duration * hourly_rate, 2)

    d = {
        "id":                 str(shift.get("_id")),
        "client":             shift.get("client_name"),
        "check_in_distance":  _check_in_distance(shift, client_cache),
        "date":               _fmt_date(shift.get("date")),
        "start_time":         _fmt_time(shift.get("start_time")),
        "end_time":           _fmt_time(shift.get("end_time")),
        "start_datetime_utc": _iso_z(start_dt),
        "end_datetime_utc":   _iso_z(end_dt),
        "distance":           round(distance_km, 1) if distance_km is not None else None,
        "shift_timing":       shift.get("shift_timing"),
        "type":               shift.get("shift_type") or shift.get("type") or "Regular",
        "county":             county,
        "duration":           f"{duration:.2f}" if duration is not None else None,
        "hourly_rate":        hourly_rate,
        "total_pay_rate":     total_pay_rate,
    }

    # Only present on shifts that carry an explicit pay_rate
    if shift.get("pay_rate") is not None:
        try:
            d["pay_rate"] = float(shift["pay_rate"])
        except (TypeError, ValueError):
            d["pay_rate"] = shift["pay_rate"]

    if INCLUDE_MATCH_DEBUG and score is not None:
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


def _score_shift(shift, user_doc, history, dist_km):
    """
    Score a shift for a user. Higher = better match.

    Scoring weights:
        Distance <5km                  : +50
        Shift timing preference        : up to +40
        County = user's home county    : +35
        Client match (worked before)   : up to +30
        County match (worked before)   : up to +20
        Already worked this shift      : -50 (deprioritise repeat)
    """
    score   = 0
    reasons = []

    shift_id_str   = str(shift.get("_id", ""))
    shift_county   = (shift.get("client_county") or "").strip()
    shift_client   = (shift.get("client_name") or "").strip()
    shift_timing   = (shift.get("shift_timing") or "").strip()

    # Priority 1: Distance
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
      - Distance from user's location
      - Shift timing preference
      - Home county match
      - Client match (previously worked)
      - County match (previously worked)
    """
    try:
        # 1. Auth
        api_key = request.headers.get("Api-Key")
        if api_key != USER_EXTERNAL_API_KEY:
            return _fail("Invalid or missing Api-Key", 401)

        # 2. Payload
        data    = request.get_json(silent=True) or {}
        user_id = str(data.get("user_id", "")).strip()
        if not user_id:
            return _fail("Missing required field: user_id", 400)

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
            return _fail(f"User not found: {user_id}", 404)

        # 4. User location
        user_lat = user_lng = None
        _loc = user_doc.get("location") or {}
        if isinstance(_loc, dict):
            user_lat = _loc.get("latitude") or _loc.get("lat")
            user_lng = _loc.get("longitude") or _loc.get("lng")

        # 5. Analyse history
        history = _get_user_history(user_doc)

        # 6. Candidate shifts — To Be Filled, matching user_type_id
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

        # 7. Score all candidates, excluding test shifts if flag is enabled
        test_ids_set = set(TEST_SHIFT_IDS)
        client_cache = {}
        scored       = []
        for shift in candidates:
            if EXCLUDE_TEST_SHIFTS and (
                shift.get("shift_xn_id") in test_ids_set or
                shift.get("shift_code") in test_ids_set
            ):
                continue
            dist_km        = _shift_distance_km(shift, user_lat, user_lng, client_cache)
            score, reasons = _score_shift(shift, user_doc, history, dist_km)
            scored.append((shift, score, reasons, dist_km))

        # Sort by score descending, then date ascending
        scored.sort(key=lambda x: (-x[1], _as_date(x[0].get("date")) or datetime.max.date()))

        # 8. Paginate
        total = len(scored)
        skip  = (page - 1) * page_size
        paged = scored[skip: skip + page_size]

        shifts_out = [
            _serialize_shift(s, dist, client_cache, sc, r)
            for s, sc, r, dist in paged
        ]

        return _ok({
            "data":         shifts_out,
            "total_count":  total,
            "per_page":     page_size,
            "current_page": page,
        }, "Shifts near me")

    except Exception as e:
        return _fail(f"Server error: {str(e)}", 500)