# xpressgp/ai_assistant.py
from flask import render_template, request
from . import bp

@bp.route('/ai-assistant')
def ai_assistant():
    return render_template('admin/ai_assistant.html')