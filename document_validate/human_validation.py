"""
human_validation.py
===================
Captures the HUMAN validation decision from XN Portal and reconciles it
against the AI decision already stored in `documents_new`.

Every reconciled pair is written to the `validation_audit` collection, which
becomes the single source of truth for AI accuracy reporting.

Outcome classes stored per document:
    agree_approve   -> AI valid      + Human approved   (true positive)
    agree_reject    -> AI invalid    + Human rejected   (true negative)
    ai_false_accept -> AI valid      + Human rejected   (AI was too lenient)
    ai_false_reject -> AI invalid    + Human approved   (AI was too strict)
    pending_ai      -> human decided but AI never ran on this document

Routes
------
POST /document-validate/human-validation          <- webhook from XN Portal
POST /document-validate/human-validation/reconcile <- retry pending_ai rows
GET  /document-validate/human-validation/<document_id>  <- inspect one audit row
"""

from flask import jsonify, request, current_app
from pymongo import MongoClient, ASCENDING, DESCENDING
from bson import ObjectId
from bson.json_util import dumps
from dotenv import load_dotenv
from datetime import datetime
import os
import json
import re

from . import bp

load_dotenv()

# ==================== CONFIG ====================
MONGO_URI             = os.getenv('MONGO_URI')
DB_NAME               = os.getenv('DB_NAME')
USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_WEBHOOK_KEY')
APP_COUNTRY           = os.getenv('XN_APP_COUNTRY', 'ie')

# Set ENABLE_MISMATCH_AI_REASONING=1 to have Gemini summarise *why* the two
# decisions differ. Falls back to rule-based tagging when disabled/unavailable.
ENABLE_AI_REASONING = os.getenv('ENABLE_MISMATCH_AI_REASONING', '0') == '1'

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db           = mongo_client[DB_NAME]

documents_coll = db['documents_new']       # written by admin/validate_document.py
audit_coll     = db['validation_audit']    # NEW: AI vs Human reconciliation
users_coll     = db['users']


# ==================== INDEXES ====================
def ensure_indexes():
    try:
        audit_coll.create_index(
            [("xn_user_id", ASCENDING), ("document_id", ASCENDING)],
            unique=True, name="uniq_user_document"
        )
        audit_coll.create_index([("outcome", ASCENDING)], name="idx_outcome")
        audit_coll.create_index(
            [("document_type_name", ASCENDING), ("outcome", ASCENDING)],
            name="idx_doctype_outcome"
        )
        audit_coll.create_index([("human_validated_at", DESCENDING)], name="idx_human_at")
    except Exception as e:  # never block app start-up on index creation
        print(f"[validation_audit] index setup skipped: {e}")


ensure_indexes()


# ==================== HELPERS ====================
TRUE_WORDS  = {"1", "true", "yes", "y", "approved", "approve", "accept", "accepted",
               "valid", "verified", "verify", "pass", "passed", "ok"}
FALSE_WORDS = {"0", "false", "no", "n", "rejected", "reject", "decline", "declined",
               "invalid", "unverified", "fail", "failed"}


def normalize_status(value):
    """Coerce whatever the portal sends into True / False / None."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(int(value))
    text = str(value).strip().lower()
    if text in TRUE_WORDS:
        return True
    if text in FALSE_WORDS:
        return False
    return None


def id_variants(value):
    """document_id may be stored as str or int — match both."""
    variants = [value]
    text = str(value).strip()
    if text not in variants:
        variants.append(text)
    try:
        variants.append(int(text))
    except (TypeError, ValueError):
        pass
    return variants


def find_ai_record(xn_user_id, document_id):
    """Locate the AI verdict written by validate_document()."""
    doc_ids = id_variants(document_id)
    record = documents_coll.find_one({
        "document_id": {"$in": doc_ids},
        "xn_user_id": str(xn_user_id).strip()
    })
    if record:
        return record
    # fall back: local user _id lookup (older rows may lack xn_user_id)
    user = users_coll.find_one({"xn_user_id": str(xn_user_id).strip()}, {"_id": 1})
    if user:
        record = documents_coll.find_one({
            "document_id": {"$in": doc_ids},
            "user_id": user["_id"]
        })
    return record


def classify(ai_status, human_status):
    if ai_status is None:
        return "pending_ai", None
    if ai_status == human_status:
        return ("agree_approve" if human_status else "agree_reject"), True
    if ai_status is True and human_status is False:
        return "ai_false_accept", False
    return "ai_false_reject", False


RULE_TAGS = [
    ("expiry",        r"expir|valid\s*until|out of date|date"),
    ("name_mismatch", r"name"),
    ("legibility",    r"blur|legib|unclear|unreadab|crop|cut off|quality|dark"),
    ("wrong_type",    r"wrong document|incorrect document|not a |document type|mismatch type"),
    ("missing_field", r"missing|absent|not present|no signature|not visible"),
    ("signature",     r"signature|signed|stamp|seal"),
    ("id_number",     r"number|reg no|registration|pin\b|nmbi"),
]


def rule_based_tag(ai_reason, human_reason):
    """Cheap deterministic bucket for the disagreement."""
    blob = f"{ai_reason or ''} {human_reason or ''}".lower()
    tags = [tag for tag, pattern in RULE_TAGS if re.search(pattern, blob)]
    return tags or ["unclassified"]


def ai_diff_reason(doc_type, ai_status, ai_reason, human_status, human_reason):
    """Optional Gemini pass that explains the disagreement in one line."""
    if not ENABLE_AI_REASONING:
        return None
    try:
        import google.generativeai as genai
        genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
        model = genai.GenerativeModel('gemini-2.5-flash')
        prompt = (
            "An AI document checker and a human reviewer disagreed on the same document.\n"
            f"Document type: {doc_type}\n"
            f"AI verdict: {'VALID' if ai_status else 'INVALID'} | AI reason: {ai_reason}\n"
            f"Human verdict: {'APPROVED' if human_status else 'REJECTED'} | "
            f"Human reason: {human_reason}\n\n"
            "Reply with JSON only, no markdown:\n"
            '{"category": "<one of: expiry, name_mismatch, legibility, wrong_type, '
            'missing_field, signature, id_number, prompt_gap, human_override, other>", '
            '"explanation": "<max 30 words on why they differ>", '
            '"likely_correct": "<ai|human|unclear>"}'
        )
        raw = model.generate_content(prompt).text
        clean = raw.replace('```json', '').replace('```', '').strip()
        return json.loads(clean)
    except Exception as e:
        return {"category": "other", "explanation": f"reasoning failed: {e}",
                "likely_correct": "unclear"}


def build_audit_doc(ai_record, xn_user_id, document_id, human_status,
                    human_reason, validated_by, document_type, app_country):
    ai_status = ai_record.get('ai_status') if ai_record else None
    ai_status = normalize_status(ai_status) if ai_record else None
    ai_reason = (ai_record or {}).get('ai_reason')
    outcome, is_match = classify(ai_status, human_status)

    doc_type = (
        document_type
        or (ai_record or {}).get('document_type_name')
        or 'Unknown'
    )

    audit = {
        "xn_user_id":         str(xn_user_id).strip(),
        "document_id":        str(document_id).strip(),
        "document_type_name": doc_type,
        "level":              (ai_record or {}).get('level'),
        "country":            app_country or APP_COUNTRY,

        # AI side
        "ai_status":          ai_status,
        "ai_reason":          ai_reason,
        "ai_checked_at":      (ai_record or {}).get('synced_at'),

        # Human side
        "human_status":       human_status,
        "human_reason":       human_reason,
        "human_validated_by": validated_by,
        "human_validated_at": datetime.utcnow(),

        # Verdict
        "outcome":            outcome,
        "is_match":           is_match,
        "score":              1 if is_match else (0 if is_match is False else None),
        "updated_at":         datetime.utcnow(),
    }

    if ai_record:
        audit["user_id"] = ai_record.get("user_id")

    if is_match is False:
        audit["mismatch_tags"] = rule_based_tag(ai_reason, human_reason)
        audit["mismatch_reason"] = (
            f"AI said {'VALID' if ai_status else 'INVALID'} "
            f"({ai_reason or 'no reason'}); "
            f"human {'APPROVED' if human_status else 'REJECTED'} "
            f"({human_reason or 'no reason'})"
        )
        diff = ai_diff_reason(doc_type, ai_status, ai_reason, human_status, human_reason)
        if diff:
            audit["mismatch_analysis"] = diff

    return audit


def save_audit(audit):
    """Upsert on (xn_user_id, document_id); keep prior decisions in history."""
    key = {"xn_user_id": audit["xn_user_id"], "document_id": audit["document_id"]}
    existing = audit_coll.find_one(key)

    update = {"$set": audit, "$setOnInsert": {"created_at": datetime.utcnow()}}

    if existing:
        update["$push"] = {"history": {
            "human_status": existing.get("human_status"),
            "human_reason": existing.get("human_reason"),
            "human_validated_by": existing.get("human_validated_by"),
            "human_validated_at": existing.get("human_validated_at"),
            "ai_status": existing.get("ai_status"),
            "outcome": existing.get("outcome"),
        }}
        update["$inc"] = {"revision": 1}
    else:
        update["$setOnInsert"]["revision"] = 0

    audit_coll.update_one(key, update, upsert=True)
    return audit_coll.find_one(key)


# ==================== ROUTE: human decision webhook ====================
@bp.route("/human-validation", methods=["POST"])
def human_validation_webhook():
    """
    Called by XN Portal the moment a human accepts/rejects a document.

    Headers:
        Api-Key: <XN_PORTAL_WEBHOOK_KEY>
        X-App-Country: ie

    Body (single):
        {
          "user_id": "695541458810dcdf8b0d4c51",   # xn_user_id
          "document_id": "12345",
          "status": true,                          # or "approved"/"rejected"/1/0
          "reject_reason": "Certificate expired",
          "validated_by": "admin@expresshealth.ie",
          "document_type": "NMBI Certificate"      # optional
        }

    Body (batch):
        { "documents": [ {..}, {..} ] }
    """
    try:
        api_key     = request.headers.get("Api-Key")
        app_country = request.headers.get("X-App-Country")

        if api_key != USER_EXTERNAL_API_KEY:
            return jsonify({"status": "error", "message": "Invalid or missing Api-Key"}), 401

        data = request.get_json(silent=True) or {}
        items = data.get("documents") if isinstance(data.get("documents"), list) else [data]

        results, errors = [], []

        for item in items:
            xn_user_id  = item.get("user_id") or item.get("xn_user_id")
            document_id = item.get("document_id")
            human_status = normalize_status(
                item.get("status", item.get("human_status", item.get("verified")))
            )

            if not xn_user_id or not document_id:
                errors.append({"item": item,
                               "message": "Missing required fields: user_id, document_id"})
                continue
            if human_status is None:
                errors.append({"item": item,
                               "message": "Missing/unrecognised 'status' (expected approved/rejected)"})
                continue

            human_reason = (
                item.get("reject_reason")
                or item.get("reason")
                or item.get("remarks")
                or ("" if human_status else "No reason supplied")
            )
            validated_by = item.get("validated_by") or item.get("validator") or "unknown"

            ai_record = find_ai_record(xn_user_id, document_id)
            audit = build_audit_doc(
                ai_record, xn_user_id, document_id, human_status,
                human_reason, validated_by, item.get("document_type"), app_country
            )
            saved = save_audit(audit)

            results.append({
                "document_id":        audit["document_id"],
                "user_id":            audit["xn_user_id"],
                "document_type_name": audit["document_type_name"],
                "ai_status":          audit["ai_status"],
                "ai_reason":          audit["ai_reason"],
                "human_status":       audit["human_status"],
                "human_reason":       audit["human_reason"],
                "outcome":            audit["outcome"],
                "is_match":           audit["is_match"],
                "score":              audit["score"],
                "mismatch_reason":    audit.get("mismatch_reason"),
                "mismatch_tags":      audit.get("mismatch_tags"),
                "mismatch_analysis":  audit.get("mismatch_analysis"),
                "revision":           (saved or {}).get("revision", 0),
            })

        status_code = 201 if results else 400
        return jsonify({
            "status":    "success" if results else "error",
            "message":   f"{len(results)} decision(s) reconciled, {len(errors)} skipped",
            "processed": results,
            "errors":    errors,
            "timestamp": datetime.utcnow().isoformat(),
        }), status_code

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


# ==================== ROUTE: reconcile pending rows ====================
@bp.route("/human-validation/reconcile", methods=["POST", "GET"])
def reconcile_pending():
    """
    Human decision can arrive before the AI has processed the document.
    Those rows are parked as outcome='pending_ai'; this re-checks them
    against documents_new and promotes any that now have an AI verdict.

    Optional query param: ?limit=100
    """
    try:
        limit = int(request.args.get("limit", 100))
        pending = list(audit_coll.find({"outcome": "pending_ai"}).limit(limit))

        promoted, still_pending = [], 0

        for row in pending:
            ai_record = find_ai_record(row["xn_user_id"], row["document_id"])
            if not ai_record or ai_record.get("ai_status") is None:
                still_pending += 1
                continue

            audit = build_audit_doc(
                ai_record, row["xn_user_id"], row["document_id"],
                row.get("human_status"), row.get("human_reason"),
                row.get("human_validated_by"), row.get("document_type_name"),
                row.get("country")
            )
            # keep the original human timestamp
            audit["human_validated_at"] = row.get("human_validated_at") or audit["human_validated_at"]
            audit_coll.update_one(
                {"_id": row["_id"]},
                {"$set": audit}
            )
            promoted.append({
                "document_id": row["document_id"],
                "outcome": audit["outcome"],
                "is_match": audit["is_match"],
            })

        return jsonify({
            "status": "success",
            "scanned": len(pending),
            "promoted": len(promoted),
            "still_pending": still_pending,
            "details": promoted,
        }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


# ==================== ROUTE: inspect one audit row ====================
@bp.route("/human-validation/<document_id>", methods=["GET"])
def get_audit_row(document_id):
    try:
        query = {"document_id": str(document_id).strip()}
        xn_user_id = request.args.get("xn_user_id")
        if xn_user_id:
            query["xn_user_id"] = xn_user_id

        rows = list(audit_coll.find(query))
        if not rows:
            return jsonify({"status": "not_found",
                            "message": "No reconciliation record for this document_id"}), 404

        return current_app.response_class(
            dumps({"status": "success", "count": len(rows), "data": rows}),
            mimetype="application/json"
        )
    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500