# admin/garda_vetting.py
from flask import (
    render_template, request, redirect, url_for, flash, jsonify, current_app
)
from functools import wraps
from bson import ObjectId
from datetime import datetime

from .views import admin_required

from . import admin_bp  # we will register a sub-blueprint or just add routes


# ------------------------------------------------------------------
# GARDA VETTING ROUTES
# ------------------------------------------------------------------

@admin_bp.route('/garda-vetting')
@admin_required
def garda_vetting():
    page = int(request.args.get('page', 1))
    per_page = 12
    search = request.args.get('search', '').strip()

    query = {"is_admin": {"$ne": True}}

    if search:
        # Search across multiple fields
        regex = {"$regex": search, "$options": "i"}
        query["$or"] = [
            {"email": regex},
            {"phone": regex},
            {"first_name": regex},
            {"last_name": regex},
            {"country": regex},
            {"designation": regex},
        ]

    total = current_app.db.users.count_documents(query)

    users_list = list(
        current_app.db.users.find(query)
        .sort("created_at", -1)
        .skip((page - 1) * per_page)
        .limit(per_page)
    )

    # Format dates and ensure fields exist
    for u in users_list:
        u['id_str'] = str(u['_id'])

        # GARDA vetting info
        u['garda_vetted'] = u.get('garda_vetted', False)
        vetted_at = u.get('garda_vetted_at')
        if vetted_at and isinstance(vetted_at, datetime):
            u['garda_vetted_at_formatted'] = vetted_at.astimezone(pytz.UTC).strftime('%d %b %Y at %H:%M')
        else:
            u['garda_vetted_at_formatted'] = '—'

        # Name fallback
        name = f"{u.get('first_name', '')} {u.get('last_name', '')}".strip()
        u['display_name'] = name or u.get('email', 'Unknown')

        # Created at formatting (reuse your existing logic)
        created = u.get('created_at')
        if isinstance(created, datetime):
            u['created_at_formatted'] = created.strftime('%d %b %Y')
        elif isinstance(created, str):
            try:
                dt = datetime.fromisoformat(created.replace('Z', '+00:00'))
                u['created_at_formatted'] = dt.strftime('%d %b %Y')
            except:
                u['created_at_formatted'] = '—'
        else:
            u['created_at_formatted'] = '—'

    return render_template(
        'admin/garda_vetting.html',
        users=users_list,
        page=page,
        total=total,
        per_page=per_page,
        search=search
    )


@admin_bp.route('/api/garda-vetting/toggle', methods=['POST'])
@admin_required
def toggle_garda_vetting():
    data = request.get_json()
    user_id = data.get('user_id')
    action = data.get('action')  # 'vet' or 'revoke'

    if not user_id or not ObjectId.is_valid(user_id):
        return jsonify({"error": "Invalid user ID"}), 400

    if action not in ('vet', 'revoke'):
        return jsonify({"error": "Invalid action"}), 400

    user = current_app.db.users.find_one({"_id": ObjectId(user_id), "is_admin": {"$ne": True}})
    if not user:
        return jsonify({"error": "User not found or is admin"}), 404

    update_data = {}
    if action == 'vet':
        update_data = {
            "garda_vetted": True,
            "garda_vetted_at": datetime.utcnow()
        }
    else:  # revoke
        update_data = {
            "garda_vetted": False,
            "garda_vetted_at": None
        }

    result = current_app.db.users.update_one(
        {"_id": ObjectId(user_id)},
        {"$set": update_data}
    )

    if result.modified_count == 1:
        return jsonify({
            "success": True,
            "new_status": action == 'vet'
        })
    else:
        return jsonify({"error": "Failed to update"}), 500