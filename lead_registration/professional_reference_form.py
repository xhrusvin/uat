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
website_leads_collection = db['users']

# ==================== PROFESSIONAL REFERENCE ROUTE ====================
@bp.route("/prof_reference", methods=["GET"])
def professional_reference():
    """
    Renders the Professional Reference page.
    URL: /lead-registration/prof_reference?id=<mongodb_object_id>&references_id=<reference_id>
    Fetches lead data and, if xn_user_id exists, calls the portal API to get recruitment details.
    If references_id is provided, only the matching pending reference is sent to the template.
    """
    lead_id_str = request.args.get("id")
    references_id = request.args.get("references_id")  # Optional: specific reference to load

    if not lead_id_str:
        abort(400, description="Missing 'id' parameter")

    try:
        lead_object_id = ObjectId(lead_id_str)
    except Exception:
        abort(400, description="Invalid lead ID format")

    lead = website_leads_collection.find_one({"_id": lead_object_id})

    if not lead:
        abort(404, description="Lead not found")

    # Extract and clean basic lead data
    email = str(lead.get("email") or "").strip().lower()
    name_str = str(lead.get("name") or "").strip()
    phone_raw = str(lead.get("phone") or "").strip()

    if not email or not name_str or not phone_raw:
        abort(400, description="Lead missing required fields (email, name, or phone)")

    # Parse name
    name_parts = name_str.split()
    first_name = name_parts[0] if name_parts else "User"
    last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

    # Parse phone number (unchanged logic)
    digits_only = re.sub(r'\D', '', phone_raw)

    if phone_raw.startswith("+"):
        if digits_only.startswith("353"):
            dial_code = "+353"
            phone_number = digits_only[3:]
        elif digits_only.startswith("44"):
            dial_code = "+44"
            phone_number = digits_only[2:]
        elif digits_only.startswith("1"):
            dial_code = "+1"
            phone_number = digits_only[1:]
        else:
            dial_code = "+" + digits_only[:4] if len(digits_only) >= 4 else "+353"
            phone_number = digits_only[len(dial_code)-1:]
    else:
        if digits_only.startswith("07") and len(digits_only) == 11:
            dial_code = "+44"
            phone_number = "0" + digits_only[1:]
        elif len(digits_only) in (9, 10):
            dial_code = "+353"
            phone_number = digits_only.lstrip("0")
        else:
            dial_code = "+44"
            phone_number = digits_only

    phone_number = re.sub(r'\D', '', phone_number)

    # Base user data
    user_data = {
        "first_name": first_name,
        "last_name": last_name,
        "dial_code": dial_code,
        "phone_number": phone_number,
        "email": email,
        "XN_PORTAL_BASE_URL": os.getenv("XN_PORTAL_BASE_URL", "https://uat.user-xpresshealth.webc.in/api"),
        "XN_PORTAL_API_KEY": os.getenv("XN_PORTAL_API_KEY", ""),
        "XN_APP_COUNTRY": os.getenv("XN_APP_COUNTRY", "IE"),
        "existing_reference": None  # Will hold single reference if references_id is provided and matched
    }

    # Only fetch from API if we have xn_user_id (required for recruitment details)
    xn_user_id = lead.get("xn_user_id")
    if xn_user_id:
        import requests

        api_url = f"{user_data['XN_PORTAL_BASE_URL']}/recruitments/detail"
        headers = {
            "Api-Key": user_data["XN_PORTAL_API_KEY"],
            "X-App-Country": user_data["XN_APP_COUNTRY"],
            "Content-Type": "application/json"
        }
        payload = {"_id": str(xn_user_id)}

        try:
            response = requests.post(api_url, json=payload, headers=headers, timeout=10)
            if response.status_code == 200:
                api_data = response.json()
                if api_data.get("success"):
                    references = api_data.get("data", {}).get("references", [])

                    if references_id:
                        # Find the specific reference with matching id and pending status
                        matching_ref = next(
                            (ref for ref in references
                             if str(ref.get("id")) == references_id and ref.get("status") == "pending"),
                            None
                        )

                        if matching_ref:
                            user_data["existing_reference"] = {
                                "id": matching_ref.get("id"),
                                "name": matching_ref.get("name", ""),
                                "email": matching_ref.get("email", ""),
                                "dial_code": matching_ref.get("dial_code", ""),
                                "phone": matching_ref.get("phone", ""),
                                "job_role": matching_ref.get("job_role", ""),
                                "organization": matching_ref.get("organization", "")
                            }
                    else:
                        # Optional: if no references_id, you could send all pending (up to 2), but per your request we keep it empty
                        pass

        except Exception as e:
            current_app.logger.error(f"Failed to fetch recruitment details for user {xn_user_id}: {str(e)}")
            # Continue rendering without reference data

    return render_template(
        "lead_registration/professional_reference_form.html",
        **user_data
    )

