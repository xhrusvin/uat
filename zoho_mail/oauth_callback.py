# zoho_mail/oauth_callback.py
# FULLY STANDALONE – No app.py needed

from flask import Blueprint, request, jsonify
import requests
from datetime import datetime
import pytz
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

# ==================== BLUEPRINT ====================
bp = Blueprint("zoho_oauth", __name__, url_prefix="/zoho/oauth")

# ==================== ENV CONFIG ====================
ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REDIRECT_URI = os.getenv("ZOHO_REDIRECT_URI")  # must match exactly
ZOHO_DC = os.getenv("ZOHO_DC", "eu")  # eu / com / in

if not all([ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, ZOHO_REDIRECT_URI]):
    raise ValueError("Zoho OAuth env vars missing")

# ==================== MONGO DB ====================
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")


if not MONGO_URI or not DB_NAME:
    raise ValueError("MONGO_URI and DB_NAME must be set")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
zoho_tokens = db["zoho_mail_tokens"]

# ==================== CONSTANTS ====================
TOKEN_URL = f"https://accounts.zoho.{ZOHO_DC}/oauth/v2/token"

# ==================== CALLBACK ROUTE ====================
@bp.route("/callback")
def zoho_oauth_callback():
    """
    Zoho redirects here with ?code=XXXX
    """

    error = request.args.get("error")
    if error:
        return jsonify({
            "success": False,
            "error": error,
            "description": request.args.get("error_description")
        }), 400

    code = request.args.get("code")
    if not code:
        return jsonify({"success": False, "message": "Authorization code missing"}), 400

    try:
        # ==================== EXCHANGE CODE FOR TOKEN ====================
        payload = {
            "grant_type": "authorization_code",
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "redirect_uri": ZOHO_REDIRECT_URI,
            "code": code
        }

        resp = requests.post(TOKEN_URL, data=payload, timeout=30)
        resp.raise_for_status()
        token_data = resp.json()

        if "access_token" not in token_data:
            return jsonify({
                "success": False,
                "message": "Token exchange failed",
                "response": token_data
            }), 400

        now = datetime.utcnow().replace(tzinfo=pytz.UTC)

        # ==================== SAVE / UPDATE TOKEN ====================
        zoho_tokens.update_one(
            {"provider": "zoho_mail"},
            {
                "$set": {
                    "provider": "zoho_mail",
                    "access_token": token_data["access_token"],
                    "refresh_token": token_data.get("refresh_token"),  # only if offline
                    "expires_in": token_data.get("expires_in"),
                    "api_domain": token_data.get("api_domain"),
                    "token_type": token_data.get("token_type"),
                    "updated_at": now.isoformat()
                }
            },
            upsert=True
        )

        return jsonify({
            "success": True,
            "message": "Zoho OAuth connected successfully",
            "stored": True
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500
