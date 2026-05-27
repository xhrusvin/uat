from flask import render_template

# ─── Registration Page ────────────────────────────────────────────────────────

@bp.route('/register', methods=['GET'])
def register_page():
    return render_template('carelearning/registration.html')