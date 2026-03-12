# call_missed.py
import threading
from flask import render_template, request, redirect, url_for, flash, current_app
from bson import ObjectId
from registration import make_ai_call
from datetime import datetime


def register_missed_call_routes(app):
    """Register server-rendered missed calls page."""

    # --------------------------------------------------------------
    # 1. PAGE – /call_missed
    # --------------------------------------------------------------
    @app.route('/call_missed', methods=['GET'])
    def call_missed_page():
        page = int(request.args.get('page', 1))
        per_page = 10

        # Query: call_sent == 0 OR missing + EXCLUDE ADMINS
        query = {
            "is_admin": {"$ne": True},                     # <-- NEW
            "$or": [
                {"call_sent": 0},
                {"call_sent": {"$exists": False}}
            ]
        }

        total = app.db.users.count_documents(query)
        users = list(
            app.db.users.find(query)
            .sort("created_at", -1)
            .skip((page - 1) * per_page)
            .limit(per_page)
        )

        for u in users:
            u['call_sent'] = u.get('call_sent', 0)

        return render_template(
            'admin/missed_calls.html',
            users=users,
            page=page,
            per_page=per_page,
            total=total
        )

    # --------------------------------------------------------------
    # 2. Trigger call – **FIXED ENDPOINT NAME + ADMIN CHECK**
    # --------------------------------------------------------------
    @app.route('/call_missed/trigger/<user_id>', methods=['POST'])
    def call_missed_trigger(user_id):   # ← ENDPOINT NAME MATCHES url_for
        try:
            obj_id = ObjectId(user_id)
        except Exception:
            flash("Invalid user ID", "danger")
            return redirect(url_for('call_missed_page'))

        user = app.db.users.find_one(
        {"_id": obj_id},
        {
            "first_name": 1,
            "last_name": 1,
            "phone": 1,
            "country": 1,
            "designation": 1,
            "call_sent": 1,
            "is_admin": 1
        }
        )
        if not user:
            flash("User not found", "danger")
            return redirect(url_for('call_missed_page'))

        # Prevent calling admins
        if user.get('is_admin'):
            flash("Cannot call admin users", "danger")
            return redirect(url_for('call_missed_page'))

        if user.get('call_sent') == 1:
            flash("Call already sent", "info")
            return redirect(url_for('call_missed_page'))

        # Mark as sent
        app.db.users.update_one(
            {"_id": obj_id},
            {"$set": {"call_sent": 1, "updated_at": datetime.utcnow()}}
        )

        # Fire call in background
        threading.Thread(
            target=make_ai_call,
            args=(current_app._get_current_object(), user['phone'], user, obj_id),
            daemon=True
        ).start()

        flash(f"Calling {user['phone']} now...", "success")
        return redirect(url_for('call_missed_page'))

    # --------------------------------------------------------------
    # DEBUG
    # --------------------------------------------------------------
    @app.route('/debug-call-missed')
    def debug():
        return "call_missed.py is loaded!"