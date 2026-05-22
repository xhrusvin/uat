import os
import requests
from flask import request, jsonify

from . import bp
from database import db

API_BASE  = "https://admin.care-learning.com/api/mcp/v1"
API_TOKEN = os.environ.get("CARE_LEARNING_TOKEN", "")


def _auth_headers():
    return {
        "Accept":        "application/json",
        "Authorization": f"Bearer {API_TOKEN}",
    }


# ─── Jobs ────────────────────────────────────────────────────────────────────

@bp.route('/jobs-mcp/list', methods=['GET'])
def jobs_mcp_list():
    upstream_params = {}
    for key in ("keyword", "location", "designation", "sector",
                "date", "page", "limit", "status", "job_type"):
        value = request.args.get(key, "").strip()
        if value:
            upstream_params[key] = value

    try:
        resp = requests.get(
            f"{API_BASE}/jobs",
            headers=_auth_headers(),
            params=upstream_params,
            timeout=15,
        )
        resp.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        return jsonify({
            "success": False,
            "error":  f"Upstream API error: {exc.response.status_code}",
            "detail":  exc.response.text,
        }), exc.response.status_code
    except requests.exceptions.RequestException as exc:
        return jsonify({"success": False, "error": str(exc)}), 502

    return jsonify(resp.json())


@bp.route('/jobs-mcp/detail/<job_id>', methods=['GET'])
def jobs_mcp_detail(job_id):
    try:
        resp = requests.get(
            f"{API_BASE}/jobs/{job_id}",
            headers=_auth_headers(),
            timeout=15,
        )
        resp.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code
        return jsonify({
            "success": False,
            "error":  f"Upstream API error: {status}",
            "detail":  exc.response.text,
        }), status
    except requests.exceptions.RequestException as exc:
        return jsonify({"success": False, "error": str(exc)}), 502

    return jsonify(resp.json())


# ─── Courses ─────────────────────────────────────────────────────────────────

@bp.route('/courses-mcp/list', methods=['GET'])
def courses_mcp_list():
    upstream_params = {}
    for key in ("keyword", "category", "page", "limit", "status"):
        value = request.args.get(key, "").strip()
        if value:
            upstream_params[key] = value

    try:
        resp = requests.get(
            f"{API_BASE}/courses",
            headers=_auth_headers(),
            params=upstream_params,
            timeout=15,
        )
        resp.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        return jsonify({
            "success": False,
            "error":  f"Upstream API error: {exc.response.status_code}",
            "detail":  exc.response.text,
        }), exc.response.status_code
    except requests.exceptions.RequestException as exc:
        return jsonify({"success": False, "error": str(exc)}), 502

    return jsonify(resp.json())


@bp.route('/courses-mcp/detail/<course_id>', methods=['GET'])
def courses_mcp_detail(course_id):
    try:
        resp = requests.get(
            f"{API_BASE}/courses/{course_id}",
            headers=_auth_headers(),
            timeout=15,
        )
        resp.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code
        return jsonify({
            "success": False,
            "error":  f"Upstream API error: {status}",
            "detail":  exc.response.text,
        }), status
    except requests.exceptions.RequestException as exc:
        return jsonify({"success": False, "error": str(exc)}), 502

    return jsonify(resp.json())


# ─── Register ────────────────────────────────────────────────────────────────

@bp.route('/register-mcp', methods=['POST'])
def register_mcp():
    payload = request.get_json(silent=True) or {}

    name       = payload.get("name",  "").strip()
    last_name  = payload.get("last_name", "").strip()
    email      = payload.get("email", "").strip()

    if not name or not email:
        return jsonify({
            "success": False,
            "error":   "Both 'name' and 'email' are required.",
        }), 400

    collection = db["care-learning"]

    # Prevent duplicate registrations by email
    if collection.find_one({"email": email}):
        return jsonify({
            "success": False,
            "error":   "This email is already registered.",
        }), 409

    # ── Call upstream Care Learning registration API ──────────────────────
    temp_password = os.urandom(8).hex()   # 16-char hex password if not supplied
    upstream_payload = {
        "name":                  name,
        "last_name":             last_name,
        "email":                 email,
        "password":              temp_password,
        "password_confirmation": temp_password,
    }

    try:
        upstream_resp = requests.post(
            "https://admin.care-learning.com/api/register",
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            json=upstream_payload,
            timeout=15,
        )
        upstream_resp.raise_for_status()
        upstream_data = upstream_resp.json()
    except requests.exceptions.HTTPError as exc:
        return jsonify({
            "success": False,
            "error":  f"Upstream registration failed: {exc.response.status_code}",
            "detail":  exc.response.text,
        }), exc.response.status_code
    except requests.exceptions.RequestException as exc:
        return jsonify({"success": False, "error": str(exc)}), 502

    # ── Save to local MongoDB after successful upstream registration ───────
    result = collection.insert_one({
        "name":          name,
        "last_name":     last_name,
        "email":         email,
        "upstream_data": upstream_data,
    })

    return jsonify({
        "success": True,
        "message": f"Thank you {name}, you have been registered successfully!",
        "id":      str(result.inserted_id),
    }), 201