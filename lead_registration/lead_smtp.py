import smtplib
import os
import re
from flask import jsonify, render_template
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime
import pytz
from flask import request, redirect
from bson import ObjectId



from . import bp

load_dotenv()

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)
TO_EMAIL = "rusvin@xpresshealth.ie"  # Or make this configurable
BCC_EMAIL = os.getenv('BCC_EMAIL')

# ==================== MONGO DB CONFIG ====================
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')

if not MONGO_URI or not DB_NAME:
    raise ValueError("MONGO_URI and DB_NAME must be set in .env")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
website_leads_collection = db['website_leads']


@bp.route("/send-test-email", methods=["GET"])
def send_test_email():
    """
    Test endpoint: Sends a sample lead registration email using the template
    with dummy data. Accessible via simple GET request.
    """
    # Dummy data for testing
    lead_data = {
        "first_name": "John",
        "last_name": "Doe",
        "dial_code": "+353",
        "phone_number": "851234567",
        "email": "john.doe@example.com"
    }

    return send_lead_email(**lead_data)

# ==================== PROCESS WEBSITE LEADS ROUTE ====================
@bp.route("/process-website-leads", methods=["GET"])
def process_website_leads():
    """
    Processes ONLY ONE eligible lead.
    Returns JSON only with status and lead_id.
    """
    try:
        # Strict query: all fields present, non-empty, and not already sent
        query = {
    "email": {"$exists": True, "$nin": [None, ""]},
    "name":  {"$exists": True, "$nin": [None, ""]},
    "phone": {"$exists": True, "$nin": [None, ""]},
    "$or": [
        {"email_sent": {"$ne": 1}},
        {"email_sent": {"$exists": False}}
    ]
}

        # Get the oldest unsent eligible lead
        lead = website_leads_collection.find_one(
            query,
            sort=[("created_at", 1)]
        )

        if not lead:
            return jsonify({
                "status": "no_lead",
                "message": "No eligible lead found (missing fields or already processed)"
            }), 200

        lead_id = str(lead["_id"])
        name_str = str(lead.get("name", "")).strip()
        email = str(lead.get("email", "")).strip().lower()
        phone_raw = str(lead.get("phone", "")).strip()

        # Parse name
        name_parts = name_str.split()
        first_name = name_parts[0] if name_parts else "Valued"
        last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""

        # Parse phone
        if phone_raw.startswith("+"):
            digits = re.sub(r'\D', '', phone_raw)
            if digits.startswith("353"):
                dial_code = "+353"
                phone_number = digits[3:]
            else:
                dial_code = "+" + digits[:3] if len(digits) >= 3 else "+353"
                phone_number = digits[3:]
        else:
            dial_code = "+353"
            phone_number = re.sub(r'\D', '', phone_raw)

        # Send email
        success, message = send_lead_email(
            first_name=first_name,
            last_name=last_name,
            dial_code=dial_code,
            phone_number=phone_number,
            email=email,
            lead_id=lead_id
        )

        if success:
            # Mark as sent
            website_leads_collection.update_one(
                {"_id": lead["_id"]},
                {"$set": {
                    "email_sent": 1,
                    "email_sent_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat()
                }}
            )
            return jsonify({
                "status": "success",
                "lead_id": lead_id,
                "message": "Email sent and lead marked as processed"
            }), 200
        else:
            return jsonify({
                "status": "error",
                "lead_id": lead_id,
                "message": f"Email failed: {message}"
            }), 500

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Server error: {str(e)}"
        }), 500


def send_lead_email(first_name, last_name, dial_code, phone_number, email, lead_id):
    """
    Reusable function to send lead registration email using the Jinja template.
    """
    current_year = datetime.now().year
    msg = MIMEMultipart("alternative")
    msg["From"] = FROM_EMAIL
    msg["To"] = email
    msg["Bcc"] = BCC_EMAIL
    msg["Subject"] = "New Lead Registration – Xpress Health"

    # Render the HTML template with variables
    html_content = render_template(
        "email/register_mail.html",
        first_name=first_name,
        last_name=last_name,
        dial_code=dial_code,
        phone_number=phone_number,
        email=email,
        lead_id=lead_id,
        current_year=current_year
    )

    # Attach HTML part
    msg.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        return jsonify({
            "status": "success",
            "message": f"Lead registration email sent successfully to {TO_EMAIL}"
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "error": str(e),
            "message": "Failed to send email. Check SMTP settings and server logs."
        }), 500
    


@bp.route("/track-click", methods=["GET"])
def track_click():
    lead_id = request.args.get("lead_id")

    if not lead_id:
        return "Invalid request: missing lead_id", 400

    try:
        result = website_leads_collection.update_one(
            {
                "_id": ObjectId(lead_id),
                "link_clicked": {"$ne": 1}  # prevent duplicate updates
            },
            {
                "$set": {
                    "link_clicked": 1,
                    "link_clicked_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat()
                }
            }
        )

        print(f"[TRACK CLICK] lead_id={lead_id}, updated={result.modified_count}")

    except Exception as e:
        print(f"[TRACK CLICK ERROR] {str(e)}")
        return "Internal server error", 500

    # Redirect user to actual page
    return redirect(
        f"https://uat.expresshealth.ie/lead-registration/set_password?id={lead_id}"
    )


@bp.route("/hello")
def hello_world():
    return "Hello World!", 200