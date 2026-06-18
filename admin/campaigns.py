from flask import render_template, request, jsonify
from bson import ObjectId

from database import db
from . import admin_bp
from admin.views import admin_required


def _campaigns_col():
    return db.campaign_configs


@admin_bp.route('/campaigns')
@admin_required
def campaigns():

    page = int(request.args.get('page', 1))
    search = request.args.get('search', '').strip()
    per_page = 20

    query = {}

    if search:
        import re
        pattern = re.compile(search, re.IGNORECASE)

        query = {
            "$or": [
                {"campaign_id": pattern},
                {"name": pattern}
            ]
        }

    total = _campaigns_col().count_documents(query)

    items = list(
        _campaigns_col()
        .find(query)
        .sort("name", 1)
        .skip((page - 1) * per_page)
        .limit(per_page)
    )

    return render_template(
        'admin/campaigns.html',
        campaigns=items,
        page=page,
        total=total,
        per_page=per_page,
        search=search
    )


@admin_bp.route('/campaigns/add', methods=['POST'])
@admin_required
def campaign_add():

    data = request.get_json()

    _campaigns_col().insert_one({
        "campaign_id": data["campaign_id"].strip(),
        "name": data["name"].strip(),
        "active": data.get("active", True)
    })

    return jsonify({
        "success": True,
        "message": "Campaign created"
    })


@admin_bp.route('/campaigns/edit', methods=['POST'])
@admin_required
def campaign_edit():

    data = request.get_json()

    _campaigns_col().update_one(
        {"_id": ObjectId(data["id"])},
        {
            "$set": {
                "campaign_id": data["campaign_id"].strip(),
                "name": data["name"].strip(),
                "active": data["active"]
            }
        }
    )

    return jsonify({
        "success": True,
        "message": "Campaign updated"
    })


@admin_bp.route('/campaigns/delete', methods=['POST'])
@admin_required
def campaign_delete():

    data = request.get_json()

    _campaigns_col().delete_one({
        "_id": ObjectId(data["id"])
    })

    return jsonify({"success": True})