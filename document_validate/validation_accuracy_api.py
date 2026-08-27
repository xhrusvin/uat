"""
validation_accuracy_api.py
==========================
Adds the two things the dashboard and XN Portal need on top of
validation_accuracy.py:

  * a compact percentage-only summary XN Portal can poll for its pointer
  * a document-level listing (matches AND mismatches) for the drill-down

Plus the route that serves the dashboard page itself.

Routes
------
GET /document-validate/accuracy/summary     -> compact percentages (XN Portal)
GET /document-validate/accuracy/documents   -> every reconciled document, paginated
GET /document-validate/accuracy-dashboard   -> the HTML dashboard
"""

from flask import jsonify, request, render_template, session, redirect, url_for
from datetime import datetime

from . import bp
from .validation_accuracy import (
    audit_coll, build_filter, derive_metrics, pct, DECIDED
)

# Accuracy bands used by both the portal pointer and the dashboard colours.
BAND_STRONG = 95.0
BAND_WATCH  = 85.0

# Below this many human reviews a percentage is not trustworthy.
DEFAULT_MIN_CONFIDENT_SAMPLES = 20


def band_for(accuracy, reviewed, min_samples):
    if reviewed < min_samples:
        return "insufficient"
    if accuracy >= BAND_STRONG:
        return "strong"
    if accuracy >= BAND_WATCH:
        return "watch"
    return "poor"


# ==================== ROUTE: compact summary for XN Portal ====================
@bp.route("/accuracy/summary", methods=["GET"])
def accuracy_summary():
    """
    Small, cacheable payload for the XN Portal pointer.

    Params: from, to, level, xn_user_id, document_type,
            min_samples (confidence threshold, default 20),
            flat=1 (returns {"NMBI Certificate": 94.2, ...} only)

    Example:
        GET /document-validate/accuracy/summary
        GET /document-validate/accuracy/summary?flat=1
        GET /document-validate/accuracy/summary?document_type=NMBI
    """
    try:
        query = build_filter()
        min_samples = int(request.args.get("min_samples", DEFAULT_MIN_CONFIDENT_SAMPLES))

        rows = list(audit_coll.aggregate([
            {"$match": query},
            {"$group": {
                "_id": "$document_type_name",
                "level": {"$first": "$level"},
                "agree_approve":   {"$sum": {"$cond": [{"$eq": ["$outcome", "agree_approve"]}, 1, 0]}},
                "agree_reject":    {"$sum": {"$cond": [{"$eq": ["$outcome", "agree_reject"]}, 1, 0]}},
                "ai_false_accept": {"$sum": {"$cond": [{"$eq": ["$outcome", "ai_false_accept"]}, 1, 0]}},
                "ai_false_reject": {"$sum": {"$cond": [{"$eq": ["$outcome", "ai_false_reject"]}, 1, 0]}},
                "last_reviewed_at": {"$max": "$human_validated_at"},
            }},
        ]))

        documents, overall = [], {
            "agree_approve": 0, "agree_reject": 0,
            "ai_false_accept": 0, "ai_false_reject": 0,
        }

        for row in rows:
            for key in overall:
                overall[key] += row.get(key, 0)

            m = derive_metrics(row)
            documents.append({
                "document_type":       row["_id"] or "Unknown",
                "level":               row.get("level"),
                "accuracy_percentage": m["accuracy_percentage"],
                "reviewed":            m["total_human_reviewed"],
                "matched":             m["matched"],
                "mismatched":          m["mismatched"],
                "ai_false_accept":     m["breakdown"]["ai_false_accept"],
                "ai_false_reject":     m["breakdown"]["ai_false_reject"],
                "false_accept_rate":   m["false_accept_rate"],
                "false_reject_rate":   m["false_reject_rate"],
                "band":                band_for(m["accuracy_percentage"],
                                                m["total_human_reviewed"], min_samples),
                "last_reviewed_at":    row["last_reviewed_at"].isoformat()
                                       if row.get("last_reviewed_at") else None,
            })

        documents.sort(key=lambda d: (-d["reviewed"], d["document_type"]))

        if request.args.get("flat") == "1":
            return jsonify({
                d["document_type"]: d["accuracy_percentage"] for d in documents
            }), 200

        overall_metrics = derive_metrics(overall)
        return jsonify({
            "status": "success",
            "as_of": datetime.utcnow().isoformat(),
            "overall": {
                "accuracy_percentage": overall_metrics["accuracy_percentage"],
                "reviewed":            overall_metrics["total_human_reviewed"],
                "matched":             overall_metrics["matched"],
                "mismatched":          overall_metrics["mismatched"],
                "ai_false_accept":     overall_metrics["breakdown"]["ai_false_accept"],
                "ai_false_reject":     overall_metrics["breakdown"]["ai_false_reject"],
                "false_accept_rate":   overall_metrics["false_accept_rate"],
                "false_reject_rate":   overall_metrics["false_reject_rate"],
                "band":                band_for(overall_metrics["accuracy_percentage"],
                                                overall_metrics["total_human_reviewed"],
                                                min_samples),
            },
            "pending_ai_rows": audit_coll.count_documents({"outcome": "pending_ai"}),
            "min_confident_samples": min_samples,
            "bands": {"strong": f">= {BAND_STRONG}", "watch": f">= {BAND_WATCH}", "poor": f"< {BAND_WATCH}"},
            "documents": documents,
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


# ==================== ROUTE: document-level detail ====================
@bp.route("/accuracy/documents", methods=["GET"])
def accuracy_documents():
    """
    Every reconciled document — agreements and disagreements — for the
    drill-down table.

    Params: document_type, outcome, match=true|false, search (document_id or
            xn_user_id), validated_by, from, to, level, page, limit (default 25),
            include_pending=1
    """
    try:
        query = build_filter()

        if request.args.get("include_pending") == "1":
            query["outcome"] = {"$in": DECIDED + ["pending_ai"]}

        outcome = request.args.get("outcome", "").strip()
        if outcome:
            query["outcome"] = outcome

        match_param = request.args.get("match", "").strip().lower()
        if match_param in ("true", "1", "yes"):
            query["is_match"] = True
        elif match_param in ("false", "0", "no"):
            query["is_match"] = False

        validated_by = request.args.get("validated_by", "").strip()
        if validated_by:
            query["human_validated_by"] = {"$regex": validated_by, "$options": "i"}

        search = request.args.get("search", "").strip()
        if search:
            query["$or"] = [
                {"document_id": {"$regex": search, "$options": "i"}},
                {"xn_user_id":  {"$regex": search, "$options": "i"}},
            ]

        page  = max(int(request.args.get("page", 1)), 1)
        limit = min(int(request.args.get("limit", 25)), 200)

        total = audit_coll.count_documents(query)
        cursor = (audit_coll.find(query)
                  .sort("human_validated_at", -1)
                  .skip((page - 1) * limit)
                  .limit(limit))

        def iso(value):
            return value.isoformat() if isinstance(value, datetime) else None

        data = [{
            "document_id":        row.get("document_id"),
            "xn_user_id":         row.get("xn_user_id"),
            "document_type":      row.get("document_type_name"),
            "level":              row.get("level"),
            "ai_status":          row.get("ai_status"),
            "ai_reason":          row.get("ai_reason"),
            "ai_checked_at":      iso(row.get("ai_checked_at")),
            "human_status":       row.get("human_status"),
            "human_reason":       row.get("human_reason"),
            "human_validated_by": row.get("human_validated_by"),
            "human_validated_at": iso(row.get("human_validated_at")),
            "outcome":            row.get("outcome"),
            "is_match":           row.get("is_match"),
            "score":              row.get("score"),
            "mismatch_reason":    row.get("mismatch_reason"),
            "mismatch_tags":      row.get("mismatch_tags"),
            "mismatch_analysis":  row.get("mismatch_analysis"),
            "revision":           row.get("revision", 0),
        } for row in cursor]

        return jsonify({
            "status": "success",
            "page": page,
            "limit": limit,
            "total": total,
            "total_pages": (total + limit - 1) // limit if limit else 0,
            "data": data,
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


# ==================== ROUTE: dashboard page ====================
@bp.route("/accuracy-dashboard", methods=["GET"])
def accuracy_dashboard():
    """
    Serves templates/validation_accuracy.html. Reuses the admin session set
    by the admin blueprint; drop the guard if this sits behind another gate.
    """
    if 'user_id' not in session or not session.get('is_admin'):
        try:
            return redirect(url_for('admin.admin_login'))
        except Exception:
            return jsonify({"status": "error", "message": "Admin login required"}), 401

    return render_template("validation_accuracy.html")