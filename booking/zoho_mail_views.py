# booking/zoho_mail_views.py
"""
Routes for the Zoho Mail inbox viewer.
Mount on the same 'booking' blueprint.

Environment / app config keys expected:
    ZOHO_ACCESS_TOKEN   – OAuth2 bearer token (refresh via cron / background task)
    ZOHO_MAIL_ADDRESS   – defaults to rusvin@xpresshealth.ie
"""
from flask import render_template, request, jsonify, current_app

from . import bp
from admin.views import admin_required
from booking.models.zoho_mail import ZohoMail


def _mail_client() -> ZohoMail:
    token = current_app.config.get("ZOHO_ACCESS_TOKEN", "")
    email = current_app.config.get("ZOHO_MAIL_ADDRESS", "rusvin@xpresshealth.ie")
    return ZohoMail(access_token=token, account_email=email)


# ------------------------------------------------------------------ #
#  Pages
# ------------------------------------------------------------------ #

@bp.route("/mail")
@admin_required
def mail_inbox():
    """Main inbox listing page."""
    page      = int(request.args.get("page", 1))
    search    = request.args.get("search", "").strip()
    folder_id = request.args.get("folder", None)
    per_page  = 20

    try:
        client = _mail_client()
        messages, total = client.get_messages(
            folder_id=folder_id,
            search=search,
            page=page,
            per_page=per_page,
        )
        folders = client.get_folders()
        error = None
    except Exception as exc:
        messages, total, folders = [], 0, []
        error = str(exc)

    pages = max(1, (total + per_page - 1) // per_page)

    return render_template(
        "booking/mail_inbox.html",
        messages=messages,
        folders=folders,
        total=total,
        page=page,
        pages=pages,
        per_page=per_page,
        search=search,
        folder_id=folder_id,
        error=error,
        account_email=current_app.config.get("ZOHO_MAIL_ADDRESS", "rusvin@xpresshealth.ie"),
    )


# ------------------------------------------------------------------ #
#  AJAX endpoints
# ------------------------------------------------------------------ #

@bp.route("/mail/message/<message_id>")
@admin_required
def mail_message_detail(message_id: str):
    """Return full email HTML as JSON for the preview modal."""
    try:
        client  = _mail_client()
        message = client.get_message_detail(message_id)
        return jsonify({"success": True, "message": message})
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500


@bp.route("/mail/folders")
@admin_required
def mail_folders():
    """Return folder list as JSON (used by sidebar refresh)."""
    try:
        client  = _mail_client()
        folders = client.get_folders()
        return jsonify({"success": True, "folders": folders})
    except Exception as exc:
        return jsonify({"success": False, "error": str(exc)}), 500