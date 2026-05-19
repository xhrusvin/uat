from datetime import datetime
from flask import request, jsonify
from bson import ObjectId

from database import db
from . import bp


@bp.route('/jobs-mcp/list', methods=['GET'])
def jobs_mcp_list():
    """
    Returns all active jobs.
    Optional query filters:
      ?status=On Hold
      ?job_type=Nursing Assessment
      ?status=Pending&job_type=Nursing Assessment
    """
    status   = request.args.get('status', '').strip()
    job_type = request.args.get('job_type', '').strip()

    query = {"is_active": True}
    if status:
        query["status"] = status
    if job_type:
        query["job_type"] = job_type

    items = list(db.jobs.find(query).sort("scheduled_date", 1))

    return jsonify({
        "success": True,
        "jobs": [
            {
                "_id":            str(j["_id"]),
                "title":          j.get("title", ""),
                "client_name":    j.get("client_name", ""),
                "job_type":       j.get("job_type", ""),
                "status":         j.get("status", ""),
                "location":       j.get("location", ""),
                "scheduled_date": j["scheduled_date"].isoformat() if j.get("scheduled_date") else None,
                "description":    j.get("description", ""),
                "notes":          j.get("notes", ""),
                "is_active":      j.get("is_active", True),
                "created_at":     j["created_at"].isoformat() if j.get("created_at") else None,
                "updated_at":     j["updated_at"].isoformat() if j.get("updated_at") else None,
            }
            for j in items
        ]
    })


@bp.route('/jobs-mcp/detail/<job_id>', methods=['GET'])
def jobs_mcp_detail(job_id):
    """
    Returns a single job by ID.
    Example: /chatgpt/detail/6a01d8534ae67f246f9dc2ad
    """
    try:
        j = db.jobs.find_one({"_id": ObjectId(job_id)})
    except Exception:
        return jsonify({"success": False, "error": "Invalid job ID format"}), 400

    if not j:
        return jsonify({"success": False, "error": "Job not found"}), 404

    return jsonify({
        "success": True,
        "job": {
            "_id":            str(j["_id"]),
            "title":          j.get("title", ""),
            "client_name":    j.get("client_name", ""),
            "job_type":       j.get("job_type", ""),
            "status":         j.get("status", ""),
            "location":       j.get("location", ""),
            "scheduled_date": j["scheduled_date"].isoformat() if j.get("scheduled_date") else None,
            "description":    j.get("description", ""),
            "notes":          j.get("notes", ""),
            "is_active":      j.get("is_active", True),
            "created_at":     j["created_at"].isoformat() if j.get("created_at") else None,
            "updated_at":     j["updated_at"].isoformat() if j.get("updated_at") else None,
        }
    })