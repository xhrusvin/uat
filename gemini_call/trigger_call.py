# gemini_call/trigger_call.py

from flask import render_template, request, flash, redirect, url_for
from . import bp
from .call_service import make_reminder_call
from functools import wraps
from flask import session

def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session or not session.get('is_admin'):
            return redirect(url_for('admin.admin_login'))  # adjust if admin is separate bp
        return f(*args, **kwargs)
    return decorated


@bp.route('/trigger-call', methods=['GET', 'POST'])
@admin_required
def trigger_call():
    if request.method == 'POST':
        phone = request.form.get('phone', '').strip()

        if not phone:
            flash("Phone number is required.", "error")
            return redirect(request.url)

        if not phone.startswith('+') or len(phone) < 10:
            flash("Use international format (+91...)", "error")
            return redirect(request.url)

        try:
            sid = make_reminder_call(phone)   # ← now starts Gemini conversation
            flash(f"Call started! SID: {sid}", "success")
        except Exception as e:
            flash(f"Call failed: {str(e)}", "error")

        return redirect(url_for('gemini_call.trigger_call'))

    return render_template('admin/gemini_call/trigger_call.html')