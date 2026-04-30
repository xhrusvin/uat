from flask import render_template, request, jsonify
import re

from database import db
from . import admin_bp
from admin.views import admin_required


def _users_col():
    return db.users


# ── Routes ───────────────────────────────────────────────────────────

@admin_bp.route('/trigger_ai_call')
@admin_required
def trigger_ai_call():
    return render_template('admin/trigger_ai_call.html')


@admin_bp.route('/trigger_ai_call/search')
@admin_required
def trigger_ai_call_search():
    q = request.args.get('q', '').strip()

    if not q:
        return jsonify({"success": True, "users": []})

    pattern = re.compile(re.escape(q), re.IGNORECASE)

    or_conditions = [
        {"first_name": pattern},
        {"last_name":  pattern},
        {"email":      pattern},
        {"phone":      pattern},
    ]

    # If query contains a space, also try matching first + last name split
    parts = q.split(None, 1)  # split on whitespace, max 2 parts
    if len(parts) == 2:
        first_pat = re.compile(re.escape(parts[0]), re.IGNORECASE)
        last_pat  = re.compile(re.escape(parts[1]), re.IGNORECASE)
        or_conditions.append({
            "first_name": first_pat,
            "last_name":  last_pat,
        })

    query = {"$or": or_conditions}

    try:
        items = list(
            _users_col()
            .find(query, {
                "_id":         1,
                "xn_user_id":  1,
                "first_name":  1,
                "last_name":   1,
                "email":       1,
                "phone":       1,
                "designation": 1,
                "country":     1,
                "call_sent":   1,
                "created_at":  1,
            })
            .sort([("created_at", -1)])
            .limit(50)
        )

        # Serialize ObjectId and dates
        for u in items:
            u["_id"] = str(u["_id"])
            if "created_at" in u and hasattr(u["created_at"], "isoformat"):
                u["created_at"] = u["created_at"].isoformat()

        return jsonify({"success": True, "users": items})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500