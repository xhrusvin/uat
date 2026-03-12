# xpressgp/xh_availability_bot.py
from flask import render_template
from . import bp

@bp.route('/availability_bot')
def availability_bot():
    return render_template('admin/availability_bot.html')