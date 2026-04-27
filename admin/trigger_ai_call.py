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
    """
    GET /admin/trigger_ai_call/search?q=<query>

    Searches across first_name, last_name, email, phone.
    Returns up to 50 results as JSON.
    """
    q = request.args.get('q', '').strip()

    if not q:
        return jsonify({"success": True, "users": []})

    pattern = re.compile(re.escape(q), re.IGNORECASE)
    query = {
        "$or": [
            {"first_name": pattern},
            {"last_name":  pattern},
            {"email":      pattern},
            {"phone":      pattern},
        ]
    }

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