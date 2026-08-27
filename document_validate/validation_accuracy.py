"""
validation_accuracy.py
======================
Reporting layer over the `validation_audit` collection written by
human_validation.py. Answers "how accurate is the AI, per document type?"

Routes
------
GET /document-validate/accuracy              -> per-document-type accuracy %
GET /document-validate/accuracy/mismatches   -> the disagreements + reasons
GET /document-validate/accuracy/reasons      -> mismatch reasons grouped by tag
GET /document-validate/accuracy/trend        -> daily/weekly accuracy trend

Common query params (all optional)
---------------------------------
from=2026-01-01            human_validated_at >= this date
to=2026-08-25              human_validated_at <= this date
document_type=NMBI         exact document type filter
level=2                    prompt level filter
xn_user_id=...             single user
min_samples=5              hide document types with too few reviews (accuracy only)
"""

from flask import jsonify, request
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os

from . import bp

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI')
DB_NAME   = os.getenv('DB_NAME')

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db           = mongo_client[DB_NAME]
audit_coll   = db['validation_audit']

DECIDED = ["agree_approve", "agree_reject", "ai_false_accept", "ai_false_reject"]


# ==================== HELPERS ====================
def parse_date(value, end_of_day=False):
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(value.strip(), fmt)
            if end_of_day and fmt == "%Y-%m-%d":
                dt += timedelta(days=1, seconds=-1)
            return dt
        except ValueError:
            continue
    return None


def build_filter():
    """Shared filter built from query params. Only decided rows count."""
    query = {"outcome": {"$in": DECIDED}}

    date_from = parse_date(request.args.get("from"))
    date_to   = parse_date(request.args.get("to"), end_of_day=True)
    if date_from or date_to:
        window = {}
        if date_from:
            window["$gte"] = date_from
        if date_to:
            window["$lte"] = date_to
        query["human_validated_at"] = window

    doc_type = request.args.get("document_type", "").strip()
    if doc_type:
        query["document_type_name"] = {"$regex": doc_type, "$options": "i"}

    level = request.args.get("level", "").strip()
    if level:
        try:
            query["level"] = int(level)
        except ValueError:
            query["level"] = level

    xn_user_id = request.args.get("xn_user_id", "").strip()
    if xn_user_id:
        query["xn_user_id"] = xn_user_id

    return query


def pct(numerator, denominator):
    if not denominator:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def derive_metrics(row):
    """row has counts: agree_approve, agree_reject, ai_false_accept, ai_false_reject."""
    tp = row.get("agree_approve", 0)     # AI valid,   human approved
    tn = row.get("agree_reject", 0)      # AI invalid, human rejected
    fp = row.get("ai_false_accept", 0)   # AI valid,   human rejected
    fn = row.get("ai_false_reject", 0)   # AI invalid, human approved

    total = tp + tn + fp + fn
    matched = tp + tn

    return {
        "total_human_reviewed":  total,
        "matched":               matched,
        "mismatched":            fp + fn,
        "accuracy_percentage":   pct(matched, total),
        "breakdown": {
            "agree_approve":     tp,
            "agree_reject":      tn,
            "ai_false_accept":   fp,   # AI passed something a human rejected
            "ai_false_reject":   fn,   # AI blocked something a human approved
        },
        "false_accept_rate":     pct(fp, total),
        "false_reject_rate":     pct(fn, total),
        # of everything the AI approved, how much did the human keep?
        "approval_precision":    pct(tp, tp + fp),
        # of everything the human approved, how much did the AI catch?
        "approval_recall":       pct(tp, tp + fn),
        "rejection_precision":   pct(tn, tn + fn),
    }


# ==================== ROUTE: accuracy per document type ====================
@bp.route("/accuracy", methods=["GET"])
def accuracy_report():
    """
    Per-document-type AI accuracy, plus an overall roll-up.

    Example:
        GET /document-validate/accuracy?from=2026-08-01&min_samples=5
    """
    try:
        query = build_filter()
        min_samples = int(request.args.get("min_samples", 0))

        pipeline = [
            {"$match": query},
            {"$group": {
                "_id": "$document_type_name",
                "level": {"$first": "$level"},
                "total": {"$sum": 1},
                "agree_approve":   {"$sum": {"$cond": [{"$eq": ["$outcome", "agree_approve"]}, 1, 0]}},
                "agree_reject":    {"$sum": {"$cond": [{"$eq": ["$outcome", "agree_reject"]}, 1, 0]}},
                "ai_false_accept": {"$sum": {"$cond": [{"$eq": ["$outcome", "ai_false_accept"]}, 1, 0]}},
                "ai_false_reject": {"$sum": {"$cond": [{"$eq": ["$outcome", "ai_false_reject"]}, 1, 0]}},
                "last_reviewed_at": {"$max": "$human_validated_at"},
            }},
            {"$sort": {"total": -1}},
        ]

        rows = list(audit_coll.aggregate(pipeline))

        per_document, overall = [], {
            "agree_approve": 0, "agree_reject": 0,
            "ai_false_accept": 0, "ai_false_reject": 0,
        }
        skipped_low_sample = 0

        for row in rows:
            for key in overall:
                overall[key] += row.get(key, 0)

            if row["total"] < min_samples:
                skipped_low_sample += 1
                continue

            metrics = derive_metrics(row)
            per_document.append({
                "document_type_name": row["_id"] or "Unknown",
                "level":              row.get("level"),
                "last_reviewed_at":   row["last_reviewed_at"].isoformat()
                                      if row.get("last_reviewed_at") else None,
                **metrics,
            })

        # worst performers first for the pointer/dashboard view
        per_document_sorted = sorted(per_document, key=lambda d: d["accuracy_percentage"])

        pending = audit_coll.count_documents({"outcome": "pending_ai"})

        return jsonify({
            "status": "success",
            "filters": {
                "from": request.args.get("from"),
                "to": request.args.get("to"),
                "document_type": request.args.get("document_type"),
                "level": request.args.get("level"),
                "xn_user_id": request.args.get("xn_user_id"),
                "min_samples": min_samples,
            },
            "overall": derive_metrics(overall),
            "document_types_count": len(per_document),
            "skipped_low_sample": skipped_low_sample,
            "pending_ai_rows": pending,
            "per_document_type": per_document,
            "weakest_document_types": per_document_sorted[:5],
            "generated_at": datetime.utcnow().isoformat(),
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


# ==================== ROUTE: the disagreements ====================
@bp.route("/accuracy/mismatches", methods=["GET"])
def mismatch_list():
    """
    Paginated list of documents where AI and human disagreed, with reasons.

    Extra params: page, limit (default 25), outcome=ai_false_accept|ai_false_reject
    """
    try:
        query = build_filter()
        query["is_match"] = False

        outcome = request.args.get("outcome", "").strip()
        if outcome in ("ai_false_accept", "ai_false_reject"):
            query["outcome"] = outcome

        page  = max(int(request.args.get("page", 1)), 1)
        limit = min(int(request.args.get("limit", 25)), 200)

        total = audit_coll.count_documents(query)
        cursor = (audit_coll.find(query)
                  .sort("human_validated_at", -1)
                  .skip((page - 1) * limit)
                  .limit(limit))

        items = []
        for row in cursor:
            items.append({
                "document_id":        row.get("document_id"),
                "xn_user_id":         row.get("xn_user_id"),
                "document_type_name": row.get("document_type_name"),
                "level":              row.get("level"),
                "ai_status":          row.get("ai_status"),
                "ai_reason":          row.get("ai_reason"),
                "human_status":       row.get("human_status"),
                "human_reason":       row.get("human_reason"),
                "human_validated_by": row.get("human_validated_by"),
                "human_validated_at": row["human_validated_at"].isoformat()
                                      if row.get("human_validated_at") else None,
                "outcome":            row.get("outcome"),
                "score":              row.get("score"),
                "mismatch_reason":    row.get("mismatch_reason"),
                "mismatch_tags":      row.get("mismatch_tags"),
                "mismatch_analysis":  row.get("mismatch_analysis"),
                "revision":           row.get("revision", 0),
            })

        return jsonify({
            "status": "success",
            "page": page,
            "limit": limit,
            "total_mismatches": total,
            "total_pages": (total + limit - 1) // limit,
            "data": items,
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


# ==================== ROUTE: mismatch reasons grouped ====================
@bp.route("/accuracy/reasons", methods=["GET"])
def mismatch_reasons():
    """
    Why do AI and humans disagree? Groups mismatches by tag and by
    document type so you know which prompt to fix first.
    """
    try:
        query = build_filter()
        query["is_match"] = False

        by_tag = list(audit_coll.aggregate([
            {"$match": query},
            {"$unwind": {"path": "$mismatch_tags", "preserveNullAndEmptyArrays": True}},
            {"$group": {
                "_id": {"tag": "$mismatch_tags", "document_type": "$document_type_name"},
                "count": {"$sum": 1},
                "false_accept": {"$sum": {"$cond": [{"$eq": ["$outcome", "ai_false_accept"]}, 1, 0]}},
                "false_reject": {"$sum": {"$cond": [{"$eq": ["$outcome", "ai_false_reject"]}, 1, 0]}},
                "sample_ai_reason":    {"$first": "$ai_reason"},
                "sample_human_reason": {"$first": "$human_reason"},
            }},
            {"$sort": {"count": -1}},
            {"$limit": 100},
        ]))

        results = [{
            "tag":                 (row["_id"].get("tag") or "unclassified"),
            "document_type_name":  row["_id"].get("document_type") or "Unknown",
            "count":               row["count"],
            "ai_false_accept":     row["false_accept"],
            "ai_false_reject":     row["false_reject"],
            "sample_ai_reason":    row.get("sample_ai_reason"),
            "sample_human_reason": row.get("sample_human_reason"),
        } for row in by_tag]

        return jsonify({
            "status": "success",
            "count": len(results),
            "data": results,
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


# ==================== ROUTE: accuracy trend ====================
@bp.route("/accuracy/trend", methods=["GET"])
def accuracy_trend():
    """
    Accuracy over time so prompt changes can be measured.

    Extra param: bucket=day|week|month (default day)
    """
    try:
        query = build_filter()
        bucket = request.args.get("bucket", "day").lower()
        fmt = {"day": "%Y-%m-%d", "week": "%Y-%V", "month": "%Y-%m"}.get(bucket, "%Y-%m-%d")

        rows = list(audit_coll.aggregate([
            {"$match": query},
            {"$group": {
                "_id": {
                    "period": {"$dateToString": {"format": fmt, "date": "$human_validated_at"}},
                    "document_type": "$document_type_name",
                },
                "agree_approve":   {"$sum": {"$cond": [{"$eq": ["$outcome", "agree_approve"]}, 1, 0]}},
                "agree_reject":    {"$sum": {"$cond": [{"$eq": ["$outcome", "agree_reject"]}, 1, 0]}},
                "ai_false_accept": {"$sum": {"$cond": [{"$eq": ["$outcome", "ai_false_accept"]}, 1, 0]}},
                "ai_false_reject": {"$sum": {"$cond": [{"$eq": ["$outcome", "ai_false_reject"]}, 1, 0]}},
            }},
            {"$sort": {"_id.period": 1}},
        ]))

        data = []
        for row in rows:
            metrics = derive_metrics(row)
            data.append({
                "period":              row["_id"]["period"],
                "document_type_name":  row["_id"]["document_type"] or "Unknown",
                "total_human_reviewed": metrics["total_human_reviewed"],
                "accuracy_percentage": metrics["accuracy_percentage"],
                "false_accept_rate":   metrics["false_accept_rate"],
                "false_reject_rate":   metrics["false_reject_rate"],
            })

        return jsonify({
            "status": "success",
            "bucket": bucket,
            "count": len(data),
            "data": data,
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500