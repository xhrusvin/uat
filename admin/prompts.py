from flask import render_template, request, jsonify
from bson import ObjectId
from datetime import datetime

from database import db
from . import admin_bp
from admin.views import admin_required


# ── Helpers ──────────────────────────────────────────────────────────

def _prompts_col():
    return db.prompts


def _get_all(search, page, per_page):
    query = {}
    if search:
        import re
        pattern = re.compile(re.escape(search), re.IGNORECASE)
        query = {"$or": [
            {"document_type_code": pattern},
            {"prompt_text": pattern},
        ]}
    col = _prompts_col()
    total = col.count_documents(query)
    items = list(
        col.find(query)
           .sort([("document_type_code", 1), ("version", -1)])
           .skip((page - 1) * per_page)
           .limit(per_page)
    )
    return items, total


def _is_code_unique(code, exclude_id=None):
    query = {"document_type_code": code.upper().strip()}
    if exclude_id:
        query["_id"] = {"$ne": ObjectId(exclude_id)}
    return _prompts_col().count_documents(query) == 0


# ── Routes ───────────────────────────────────────────────────────────

@admin_bp.route('/prompts')
@admin_required
def prompts():
    page     = int(request.args.get('page', 1))
    search   = request.args.get('search', '').strip()
    per_page = 20

    items, total = _get_all(search, page, per_page)

    return render_template(
        'admin/prompts.html',
        prompts=items,
        page=page,
        total=total,
        per_page=per_page,
        search=search,
    )


@admin_bp.route('/prompts/add', methods=['POST'])
@admin_required
def prompt_add():
    data = request.get_json()
    code = (data.get('document_type_code') or '').strip().upper()
    text = (data.get('prompt_text') or '').strip()

    if not code:
        return jsonify({"success": False, "error": "Document type code is required"}), 400
    if not text:
        return jsonify({"success": False, "error": "Prompt text is required"}), 400
    if not _is_code_unique(code):
        return jsonify({"success": False, "error": f'Code "{code}" already exists'}), 400

    doc = {
        "document_type_code": code,
        "prompt_text":        text,
        "is_active":          data.get('is_active') is True,
        "version":            1,
        "created_at":         datetime.utcnow(),
    }
    try:
        _prompts_col().insert_one(doc)
        return jsonify({"success": True, "message": "Prompt created"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/prompts/edit', methods=['POST'])
@admin_required
def prompt_edit():
    data      = request.get_json()
    prompt_id = (data.get('prompt_id') or '').strip()
    code      = (data.get('document_type_code') or '').strip().upper()
    text      = (data.get('prompt_text') or '').strip()

    if not prompt_id:
        return jsonify({"success": False, "error": "Missing prompt_id"}), 400
    if not code:
        return jsonify({"success": False, "error": "Document type code is required"}), 400
    if not text:
        return jsonify({"success": False, "error": "Prompt text is required"}), 400
    if not _is_code_unique(code, exclude_id=prompt_id):
        return jsonify({"success": False, "error": f'Code "{code}" already exists'}), 400

    try:
        col     = _prompts_col()
        current = col.find_one({"_id": ObjectId(prompt_id)})
        if not current:
            return jsonify({"success": False, "error": "Prompt not found"}), 404

        # bump version only when prompt text changed
        new_version = current.get("version", 1)
        if text != current.get("prompt_text", ""):
            new_version += 1

        col.update_one(
            {"_id": ObjectId(prompt_id)},
            {"$set": {
                "document_type_code": code,
                "prompt_text":        text,
                "is_active":          data.get('is_active') is True,
                "version":            new_version,
            }}
        )
        return jsonify({"success": True, "message": "Prompt updated"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/prompts/delete', methods=['POST'])
@admin_required
def prompt_delete():
    data      = request.get_json()
    prompt_id = (data.get('prompt_id') or '').strip()

    if not prompt_id:
        return jsonify({"success": False, "error": "Missing prompt_id"}), 400
    try:
        _prompts_col().delete_one({"_id": ObjectId(prompt_id)})
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@admin_bp.route('/prompts/active', methods=['GET'])
def prompts_active_api():
    """Public JSON — returns all active prompts keyed by document_type_code."""
    items = list(_prompts_col().find({"is_active": True}))
    return jsonify({
        "success": True,
        "prompts": [
            {
                "_id":                str(p["_id"]),
                "document_type_code": p["document_type_code"],
                "prompt_text":        p["prompt_text"],
                "version":            p.get("version", 1),
            }
            for p in items
        ]
    })