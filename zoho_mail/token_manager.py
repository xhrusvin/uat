# zoho_mail/token_manager.py
# FULLY STANDALONE – No app.py required

from flask import Blueprint, jsonify
import requests
from datetime import datetime, timedelta
import pytz
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

# ==================== BLUEPRINT ====================
bp = Blueprint("zoho_token_manager", __name__, url_prefix="/zoho/token")

# ==================== ENV ====================
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_DC = os.getenv("ZOHO_DC", "eu")

if not ZOHO_CLIENT_ID or not ZOHO_CLIENT_SECRET:
    raise ValueError("Zoho client credentials missing")

# ==================== MONGO ====================
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
tokens_col = db["zoho_mail_tokens"]

# ==================== CONSTANTS ====================
TOKEN_URL = f"https://accounts.zoho.{ZOHO_DC}/oauth/v2/token"
ACCESS_TOKEN_TTL_BUFFER = 300  # seconds (refresh 5 min early)

# ==================== HELPERS ====================
def token_expired(token_doc):
    """
    Returns True if token is expired or about to expire
    """
    if not token_doc:
        return True

    updated_at = token_doc.get("updated_at")
    expires_in = token_doc.get("expires_in")

    if not updated_at or not expires_in:
        return True

    updated_at = datetime.fromisoformat(updated_at)
    expiry_time = updated_at + timedelta(seconds=int(expires_in))

    return datetime.utcnow().replace(tzinfo=pytz.UTC) >= (
        expiry_time - timedelta(seconds=ACCESS_TOKEN_TTL_BUFFER)
    )


def refresh_access_token(refresh_token):
    payload = {
        "grant_type": "refresh_token",
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "refresh_token": refresh_token,
    }

    resp = requests.post(TOKEN_URL, data=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()

# ==================== MAIN API ====================
@bp.route("/get", methods=["GET"])
def get_valid_access_token():
    """
    Returns a valid access token.
    Auto-refreshes if expired.
    """

    token_doc = tokens_col.find_one({"provider": "zoho_mail"})

    if not token_doc:
        return jsonify({
            "success": False,
            "message": "Zoho not connected. OAuth required."
        }), 400

    if not token_expired(token_doc):
        return jsonify({
            "success": True,
            "access_token": token_doc["access_token"],
            "source": "cache"
        })

    # ==================== REFRESH ====================
    refresh_token = token_doc.get("refresh_token")
    if not refresh_token:
        return jsonify({
            "success": False,
            "message": "Refresh token missing. Re-auth required."
        }), 401

    try:
        new_token = refresh_access_token(refresh_token)

        now = datetime.utcnow().replace(tzinfo=pytz.UTC)

        tokens_col.update_one(
            {"_id": token_doc["_id"]},
            {
                "$set": {
                    "access_token": new_token["access_token"],
                    "expires_in": new_token.get("expires_in", 3600),
                    "token_type": new_token.get("token_type", "Bearer"),
                    "updated_at": now.isoformat()
                }
            }
        )

        return jsonify({
            "success": True,
            "access_token": new_token["access_token"],
            "source": "refreshed"
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
