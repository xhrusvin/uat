# admin.py
from flask import (
    redirect, url_for, flash, current_app, jsonify,
    request, session, render_template, Response
)
from functools import wraps
from . import admin_bp
from datetime import datetime, timedelta
import bcrypt
from pytz import utc
import pytz
from bson import ObjectId
from bson.binary import Binary
import os
import asyncio
import aiohttp
import threading
import re
from pymongo.errors import OperationFailure
import requests
import pandas as pd
from io import BytesIO


now_utc = datetime.now(pytz.UTC)


XN_PORTAL_BASE_URL = os.getenv('XN_PORTAL_BASE_URL')
NEXT_FOLLOW_UP_MINUTES = 2
NEXT_FOLLOW_UP_HOURS = 24

current_time = datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M")

def to_str(value):
    """
    Convert any value to string.
    - None → "null" or "" (your choice)
    - int/float → str without scientific notation
    - bool → "true"/"false" or "True"/"False"
    - Already string → return as-is
    """
    if value is None:
        return ""  # or return "null" if you prefer
    return str(value).strip()

def run_async(coro):
    """
    Safely runs an async coroutine in a new event loop (thread-safe).
    This avoids the "asyncio.run() cannot be called from a running event loop" error.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

# ==================================================================
# HELPER: Normalize password hash from any MongoDB storage type
# ==================================================================
def normalize_stored_hash(stored_hash):
    """
    Safely convert stored password hash to bytes, regardless of storage format:
    - str → encode to UTF-8 bytes
    - bson.Binary → convert to bytes
    - bytearray → convert to bytes
    - bytes → return as-is
    - anything else → return None (invalid)
    """
    if isinstance(stored_hash, str):
        try:
            return stored_hash.encode('utf-8')
        except (UnicodeEncodeError, AttributeError):
            return None
    elif isinstance(stored_hash, (bytes, bytearray, Binary)):
        return bytes(stored_hash)
    else:
        return None



# ------------------------------------------------------------------
# PROTECTED ROUTES DECORATOR
# ------------------------------------------------------------------
def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user_id' not in session or not session.get('is_admin'):
            return redirect(url_for('admin.admin_login'))
        return f(*args, **kwargs)
    return decorated

def get_preferred_contact_label(value):
    """Convert numeric preferred_contact value to readable string"""
    mapping = {
        1: "Call",
        2: "WhatsApp",
        3: "Email",
        4: "SMS"
    }
    return mapping.get(int(value), "Email")  # default to Email

@admin_bp.route('/staff')
@admin_required
def staff():
    page = int(request.args.get('page', 1))
    per_page = 10
    search = request.args.get('search', '').strip()

    query = {"is_admin": {"$ne": True}}
    if search:
        query["email"] = {"$regex": search, "$options": "i"}

    total = current_app.db.users.count_documents(query)
    users_list = list(
        current_app.db.users.find(query)
        .sort("created_at", -1)
        .skip((page - 1) * per_page)
        .limit(per_page)
    )

    for u in users_list:
        # Existing fields
        u['call_sent'] = u.get('call_sent', 1)
        u['garda_email_sent_status'] = "Sent" if u.get('garda_email_sent') == 1 else "No"
        u['missed_call_email_sent_status'] = "Sent" if u.get('missed_call_email_sent') == 1 else "No"

        # New field – make sure it's passed to template
        u['preferred_contact'] = u.get('preferred_contact', []) # default = Email

        # Date formatting (unchanged)
        created = u.get('created_at')
        if isinstance(created, datetime):
            u['created_at_formatted'] = created.strftime('%d %b %Y')
            u['created_at_time'] = created.strftime('%H:%M')
        elif isinstance(created, str):
            try:
                dt = datetime.fromisoformat(created.replace('Z', '+00:00'))
                u['created_at_formatted'] = dt.strftime('%d %b %Y')
                u['created_at_time'] = dt.strftime('%H:%M')
            except:
                u['created_at_formatted'] = '—'
                u['created_at_time'] = ''
        else:
            u['created_at_formatted'] = '—'
            u['created_at_time'] = ''

    return render_template('admin/staff.html',
                           users=users_list,
                           page=page,
                           total=total,
                           per_page=per_page,
                           search=search)

@admin_bp.route('/api/update-preferred-contact', methods=['POST'])
@admin_required
def update_preferred_contact():
    try:
        data = request.get_json()
        user_id_str = data.get('user_id')
        preferred_list = data.get('preferred_contact')  # now expecting list

        if not user_id_str or not isinstance(preferred_list, list):
            return jsonify({"success": False, "message": "Invalid data"}), 400

        try:
            user_id = ObjectId(user_id_str)
        except:
            return jsonify({"success": False, "message": "Invalid user ID"}), 400

        # Validate values
        valid_values = {1, 2, 3, 4}
        cleaned = [int(v) for v in preferred_list if str(v).isdigit() and int(v) in valid_values]

        if not cleaned:
            return jsonify({"success": False, "message": "No valid contact methods selected"}), 400

        # Save as array in MongoDB
        result = current_app.db.users.update_one(
            {"_id": user_id},
            {"$set": {
                "preferred_contact": cleaned,           # store as [1, 3] etc.
                "updated_at": datetime.utcnow()
            }}
        )

        if result.modified_count == 1:
            return jsonify({
                "success": True,
                "message": "Updated",
                "preferred_contact": cleaned
            })
        else:
            return jsonify({"success": False, "message": "User not found"}), 404

    except Exception as e:
        current_app.logger.error(f"Error: {str(e)}")
        return jsonify({"success": False, "message": "Server error"}), 500