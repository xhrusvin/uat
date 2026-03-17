from datetime import datetime
from flask import render_template, request, jsonify
from bson import ObjectId

from database import db
from booking.models.designation import Designation

designation_model = Designation(db.designations)

from . import bp
from admin.views import admin_required


@bp.route('/designations')
@admin_required
def designations():
    page   = int(request.args.get('page', 1))
    search = request.args.get('search', '').strip()
    per_page = 20

    items, total = designation_model.get_all(search, page, per_page)
    pages = (total + per_page - 1) // per_page if per_page else 1

    return render_template(
        'booking/designations.html',
        designations=items,
        page=page,
        total=total,
        per_page=per_page,
        pages=pages,
        search=search,
    )


@bp.route('/designations/add', methods=['POST'])
@admin_required
def designation_add():
    data = request.get_json()
    name = (data.get('name') or '').strip()

    if not name:
        return jsonify({"success": False, "error": "Designation name is required"}), 400

    if not designation_model.is_name_unique(name):
        return jsonify({"success": False, "error": f'"{name}" already exists'}), 400

    doc = {
        "name":        name,
        "description": (data.get('description') or '').strip(),
        "is_active":   data.get('is_active') is True,
    }
    try:
        designation_model.create(doc)
        return jsonify({"success": True, "message": "Designation created"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/designations/edit', methods=['POST'])
@admin_required
def designation_edit():
    data           = request.get_json()
    designation_id = data.get('designation_id')
    name           = (data.get('name') or '').strip()

    if not designation_id:
        return jsonify({"success": False, "error": "Missing designation_id"}), 400
    if not name:
        return jsonify({"success": False, "error": "Name is required"}), 400
    if not designation_model.is_name_unique(name, exclude_id=designation_id):
        return jsonify({"success": False, "error": f'"{name}" already exists'}), 400

    update = {
        "name":        name,
        "description": (data.get('description') or '').strip(),
        "is_active":   data.get('is_active') is True,
    }
    try:
        designation_model.update(designation_id, update)
        return jsonify({"success": True, "message": "Designation updated"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/designations/delete', methods=['POST'])
@admin_required
def designation_delete():
    data           = request.get_json()
    designation_id = data.get('designation_id')

    if not designation_id:
        return jsonify({"success": False, "error": "Missing designation_id"}), 400

    try:
        designation_model.delete(designation_id)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/designations/list', methods=['GET'])
def designations_list_api():
    """
    Public JSON endpoint — returns all active designations.
    Used by shift modal to populate copy-staff filter checkboxes.
    """
    items = designation_model.get_all_active()
    return jsonify({
        "success": True,
        "designations": [
            {"_id": str(d["_id"]), "name": d["name"]}
            for d in items
        ]
    })