import os
import requests
from flask import request, jsonify

from . import bp

# Read once at module load; set CARE_LEARNING_TOKEN in your environment / .env file
API_BASE   = "https://admin.care-learning.com/api/mcp/v1"
API_TOKEN  = os.environ.get("CARE_LEARNING_TOKEN", "")


def _auth_headers():
    return {
        "Accept":        "application/json",
        "Authorization": f"Bearer {API_TOKEN}",
    }


@bp.route('/jobs-mcp/list', methods=['GET'])
def jobs_mcp_list():
    """
    Proxies the upstream jobs list API.
    Accepted query params (all optional):
      keyword, location, designation, sector, date, page, limit
      (legacy: status, job_type – passed through as-is if the upstream accepts them)
    """
    # Forward every recognised query param the upstream API supports
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
            "error":   f"Upstream API error: {exc.response.status_code}",
            "detail":  exc.response.text,
        }), exc.response.status_code
    except requests.exceptions.RequestException as exc:
        return jsonify({"success": False, "error": str(exc)}), 502

    data = resp.json()

    # Normalise to the shape your callers already expect
    raw_jobs = data.get("data") or data.get("jobs") or []

    return jsonify({
        "success": True,
        "jobs": [_normalise_job(j) for j in raw_jobs],
    })


@bp.route('/jobs-mcp/detail/<job_id>', methods=['GET'])
def jobs_mcp_detail(job_id):
    """
    Proxies the upstream single-job API.
    Example: /jobs-mcp/detail/019d77dc-d199-7148-8c31-1b5260361fd7
    """
    try:
        resp = requests.get(
            f"{API_BASE}/jobs/{job_id}",
            headers=_auth_headers(),
            timeout=15,
        )
        resp.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        status = exc.response.status_code
        if status == 404:
            return jsonify({"success": False, "error": "Job not found"}), 404
        return jsonify({
            "success": False,
            "error":   f"Upstream API error: {status}",
            "detail":  exc.response.text,
        }), status
    except requests.exceptions.RequestException as exc:
        return jsonify({"success": False, "error": str(exc)}), 502

    data = resp.json()
    raw_job = data.get("data") or data.get("job") or data

    return jsonify({"success": True, "job": _normalise_job(raw_job)})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalise_job(j: dict) -> dict:
    """Map upstream field names to the shape your API consumers expect."""
    return {
        "_id":            j.get("id") or j.get("_id", ""),
        "title":          j.get("title", ""),
        "client_name":    j.get("client_name") or j.get("company", ""),
        "job_type":       j.get("job_type") or j.get("type", ""),
        "status":         j.get("status", ""),
        "location":       j.get("location", ""),
        "scheduled_date": j.get("scheduled_date") or j.get("date"),
        "description":    j.get("description", ""),
        "notes":          j.get("notes", ""),
        "is_active":      j.get("is_active", True),
        "created_at":     j.get("created_at"),
        "updated_at":     j.get("updated_at"),
    }