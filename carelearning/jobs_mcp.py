import os
import requests
from flask import request, jsonify

from . import bp

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