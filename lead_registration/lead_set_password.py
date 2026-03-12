# lead_registration/lead_set_password.py

from flask import render_template, request, Blueprint, abort
import os
import re
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId

# Assuming your main blueprint is bp in the parent package
from . import bp

load_dotenv()

# ==================== MONGO DB CONNECTION ====================
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')

if not MONGO_URI or not DB_NAME:
    raise ValueError("MONGO_URI and DB_NAME must be set in .env")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
website_leads_collection = db['website_leads']
fb_leads_collection      = db['leads']

# ==================== SET PASSWORD ROUTE ====================
@bp.route("/set_password", methods=["GET"])
def set_password():
    """
    Renders the Set Password page.
    URL: /lead-registration/set_password?id=<mongodb_object_id>
    Fetches real lead data from fb_leads collection.
    """
    lead_id_str = request.args.get("id")

    if not lead_id_str:
        abort(400, description="Missing 'id' parameter")

    try:
        # Validate and convert to ObjectId
        lead_object_id = ObjectId(lead_id_str)
    except Exception:
        abort(400, description="Invalid lead ID format")

    # Fetch the lead
    lead = website_leads_collection.find_one({"_id": lead_object_id})

    if not lead:
        abort(404, description="Lead not found")

    # Extract and clean data
    email = str(lead.get("email") or "").strip().lower()
    name_str = str(lead.get("name") or "").strip()
    phone_raw = str(lead.get("phone") or "").strip()

    if not email or not name_str or not phone_raw:
        abort(400, description="Lead missing required fields (email, name, or phone)")

    # Parse name
    name_parts = name_str.split()
    first_name = name_parts[0] if name_parts else "User"
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

    # Parse phone number intelligently
    # Remove all non-digits
    digits_only = re.sub(r'\D', '', phone_raw)

    if phone_raw.startswith("+"):
        # International format
        if digits_only.startswith("353"):        # Ireland
            dial_code = "+353"
            phone_number = digits_only[3:]
        elif digits_only.startswith("44"):        # UK
            dial_code = "+44"
            phone_number = digits_only[2:]
        elif digits_only.startswith("1"):         # US/Canada
            dial_code = "+1"
            phone_number = digits_only[1:]
        else:
            # Fallback: take first 3-4 digits after +
            dial_code = "+" + digits_only[:4] if len(digits_only) >= 4 else "+353"
            phone_number = digits_only[len(dial_code)-1:]
    else:
        # No + prefix — assume UK or Ireland
        if digits_only.startswith("07") and len(digits_only) == 11:  # UK mobile
            dial_code = "+44"
            phone_number = "0" + digits_only[1:]  # keep leading 0 for display if needed
        elif len(digits_only) == 10 or len(digits_only) == 9:
            dial_code = "+353"
            phone_number = digits_only.lstrip("0")
        else:
            dial_code = "+44"  # fallback
            phone_number = digits_only

    # Final clean phone number (no spaces/dashes)
    phone_number = re.sub(r'\D', '', phone_number)

    # Prepare data for template and API call
    user_data = {
        "first_name": first_name,
        "last_name": last_name,
        "dial_code": dial_code,
        "phone_number": phone_number,
        "email": email,
        "XN_PORTAL_BASE_URL": os.getenv("XN_PORTAL_BASE_URL", "https://uat.user-xpresshealth.webc.in/api"),
        "XN_PORTAL_API_KEY": os.getenv("XN_PORTAL_API_KEY", ""),
        "XN_APP_COUNTRY": os.getenv("XN_APP_COUNTRY", "IE")
    }

    return render_template(
        "lead_registration/set_password.html",
        **user_data
    )

@bp.route("/set_password_fb", methods=["GET"])
def set_password_fb():
    """
    Renders the Set Password page.
    URL: /lead-registration/set_password_fb?id=<mongodb_object_id>
    Fetches real lead data from website_leads collection.
    """
    lead_id_str = request.args.get("id")

    if not lead_id_str:
        abort(400, description="Missing 'id' parameter")

    try:
        # Validate and convert to ObjectId
        lead_object_id = ObjectId(lead_id_str)
    except Exception:
        abort(400, description="Invalid lead ID format")

    # Fetch the lead
    lead = fb_leads_collection.find_one({"_id": lead_object_id})

    if not lead:
        abort(404, description="Lead not found")

    # Extract and clean data
    email = str(lead.get("email_id") or "").strip().lower()
    name_str = str(lead.get("name") or "").strip()
    phone_raw = str(lead.get("phone_number") or "").strip()

    if not email or not name_str or not phone_raw:
        abort(400, description="Lead missing required fields (email, name, or phone)")

    # Parse name
    name_parts = name_str.split()
    first_name = name_parts[0] if name_parts else "User"
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

    # Parse phone number intelligently
    # Remove all non-digits
    digits_only = re.sub(r'\D', '', phone_raw)

    if phone_raw.startswith("+"):
        # International format
        if digits_only.startswith("353"):        # Ireland
            dial_code = "+353"
            phone_number = digits_only[3:]
        elif digits_only.startswith("44"):        # UK
            dial_code = "+44"
            phone_number = digits_only[2:]
        elif digits_only.startswith("1"):         # US/Canada
            dial_code = "+1"
            phone_number = digits_only[1:]
        else:
            # Fallback: take first 3-4 digits after +
            dial_code = "+" + digits_only[:4] if len(digits_only) >= 4 else "+353"
            phone_number = digits_only[len(dial_code)-1:]
    else:
        # No + prefix — assume UK or Ireland
        if digits_only.startswith("07") and len(digits_only) == 11:  # UK mobile
            dial_code = "+44"
            phone_number = "0" + digits_only[1:]  # keep leading 0 for display if needed
        elif len(digits_only) == 10 or len(digits_only) == 9:
            dial_code = "+353"
            phone_number = digits_only.lstrip("0")
        else:
            dial_code = "+44"  # fallback
            phone_number = digits_only

    # Final clean phone number (no spaces/dashes)
    phone_number = re.sub(r'\D', '', phone_number)

    # Prepare data for template and API call
    user_data = {
        "first_name": first_name,
        "last_name": last_name,
        "dial_code": dial_code,
        "phone_number": phone_number,
        "email": email,
        "XN_PORTAL_BASE_URL": os.getenv("XN_PORTAL_BASE_URL", "https://uat.user-xpresshealth.webc.in/api"),
        "XN_PORTAL_API_KEY": os.getenv("XN_PORTAL_API_KEY", ""),
        "XN_APP_COUNTRY": os.getenv("XN_APP_COUNTRY", "IE")
    }

    return render_template(
        "lead_registration/set_password.html",
        **user_data
    )