import os
import logging
import requests
from flask import request, jsonify

from . import bp

logger = logging.getLogger(__name__)

API_BASE  = "https://admin.care-learning.com/api/mcp/v1"
API_TOKEN = os.environ.get("CARE_LEARNING_TOKEN", "")


def _auth_headers():
    return {
        "Accept":        "application/json",
        "Authorization": f"Bearer {API_TOKEN}",
    }


def _extract_jobs(data: any) -> list:
    """
    Robustly extract a list of job dicts from whatever shape the API returns.
    Logs the raw structure so you can see exactly what came back.
    """
    logger.warning("RAW API RESPONSE TYPE: %s", type(data))
    logger.warning("RAW API RESPONSE: %s", str(data)[:1000])  # first 1000 chars

    # Already a plain list
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        # Try common wrapper keys in order
        for key in ("data", "jobs", "results", "items", "records"):
            candidate = data.get(key)
            if isinstance(candidate, list):
                return candidate
            # Some APIs wrap further: {"data": {"data": [...], "total": N}}
            if isinstance(candidate, dict):
                for inner_key in ("data", "jobs", "results", "items"):
                    inner = candidate.get(inner_key)
                    if isinstance(inner, list):
                        return inner

    logger.error("Could not extract a job list from: %s", str(data)[:500])
    return []


def _normalise_job(j: any) -> dict:
    """Safely map one job item to the expected shape."""
    if not isinstance(j, dict):
        logger.error("Expected a dict for job, got %s: %r", type(j).__name__, j)
        return {"_id": str(j), "error": f"Unexpected job format: {type(j).__name__}"}

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
            "error":   f"Upstream API error: {exc.response.status_code}",
            "detail":  exc.response.text,
        }), exc.response.status_code
    except requests.exceptions.RequestException as exc:
        return jsonify({"success": False, "error": str(exc)}), 502

    data = resp.json()
    raw_jobs = _extract_jobs(data)

    return jsonify({
        "success": True,
        "jobs":    [_normalise_job(j) for j in raw_jobs],
    })


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
    logger.warning("DETAIL RAW API RESPONSE: %s", str(data)[:1000])

    # Detail endpoint may return the object directly or wrapped
    if isinstance(data, dict):
        raw_job = data.get("data") or data.get("job") or data
    else:
        raw_job = data

    return jsonify({"success": True, "job": _normalise_job(raw_job)})