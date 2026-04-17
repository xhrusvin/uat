import smtplib
import os
import re
from flask import jsonify, render_template, request
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime
import pytz
from email.mime.application import MIMEApplication
from bson import ObjectId
from flask import current_app

from . import bp


load_dotenv()

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)
TO_EMAIL = "rusvin@xpresshealth.ie"  # Or make this configurable

# ==================== MONGO DB CONFIG ====================
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')
BCC_EMAIL = os.getenv('BCC_EMAIL')
CC_EMAIL = os.getenv('CC_EMAIL')

if not MONGO_URI or not DB_NAME:
    raise ValueError("MONGO_URI and DB_NAME must be set in .env")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
website_leads_collection = db['users']




# ==================== PROCESS WEBSITE LEADS ROUTE ====================
@bp.route("/practical-training-institutes-email", methods=["GET"])
def practical_training_institutes_email():
    """
    Processes ONLY ONE eligible lead.
    If ?id=... is provided, processes that specific lead (if valid and has required fields).
    Otherwise, processes the oldest unsent eligible lead.
    Returns JSON only with status and lead_id.
    """
   
    lead_id_param = request.args.get("id")  # e.g. ?id=2232sxzzxzxzx

    try:
        # Base required field check (used in both paths)
        required_fields_query = {
            "email": {"$exists": True, "$nin": [None, ""]},
            "name":  {"$exists": True, "$nin": [None, ""]},
            "phone": {"$exists": True, "$nin": [None, ""]},
        }

        lead = None

        # -----------------------------
        # 1. Specific lead by ?id=...
        # -----------------------------
        if lead_id_param:
            if not ObjectId.is_valid(lead_id_param):
                return jsonify({
                    "status": "error",
                    "message": "Invalid ID format"
                }), 400

            lead = website_leads_collection.find_one({
                "_id": ObjectId(lead_id_param),
                **required_fields_query
            })

            if not lead:
                return jsonify({
                    "status": "no_lead",
                    "message": "Specific lead not found or missing required fields"
                }), 200

        # -----------------------------
        # 2. Fallback: oldest unsent eligible lead
        # -----------------------------
        else:
            query = {
                **required_fields_query,
                "$or": [
                    {"garda_email_sent": {"$ne": 1}},
                    {"garda_email_sent": {"$exists": False}}
                ]
            }

            lead = website_leads_collection.find_one(
                query,
                sort=[("created_at", 1)]
            )

            if not lead:
                return jsonify({
                    "status": "no_lead",
                    "message": "No eligible lead found (missing fields or already processed)"
                }), 200

        # -----------------------------
        # Process the found lead
        # -----------------------------
        lead_id = str(lead["_id"])
        name_str = str(lead.get("name", "")).strip()
        email = str(lead.get("email", "")).strip().lower()
        phone_raw = str(lead.get("phone", "")).strip()
        designation = str(lead.get("designation", "")).strip()

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
            lead_id=lead_id,
            designation=designation
        )

        if success:
            # Mark as sent (only if it was a regular lookup we avoid marking a manually requested lead twice if already sent)
            website_leads_collection.update_one(
                {"_id": lead["_id"]},
                {"$set": {
                    "garda_email_sent": 1,
                    "garda_email_sent_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat()
                }}
            )
            return jsonify({
                "status": "success",
                "lead_id": lead_id,
                "email": email,
                "designation": designation,
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


def send_lead_email(first_name, last_name, dial_code, phone_number, email, lead_id, designation):
    """
    Reusable function to send lead registration email using the Jinja template.
    """
    current_year = datetime.now().year
    msg = MIMEMultipart("alternative")
    msg["From"] = FROM_EMAIL
    msg["To"] = email
    msg["Bcc"] = BCC_EMAIL
    msg["Cc"] = CC_EMAIL

    msg["Subject"] = "Garda Vetting Request – Next Steps for Your Application"

    # Render the HTML template with variables
    html_content = render_template(
        "email/garda_vetting_mail.html",
        first_name=first_name,
        last_name=last_name,
        dial_code=dial_code,
        phone_number=phone_number,
        email=email,
        lead_id=lead_id,
        designation=designation,
        current_year=current_year
    )
    # Attach HTML part
    msg.attach(MIMEText(html_content, "html"))

    pdf_path = os.path.join(
    current_app.root_path,          # almost always the folder that contains app.py / your package
    "static",
    "documents",
    "Garda_Vetting_Form.pdf"
    )

    try:
        with open(pdf_path, "rb") as pdf_file:
            attach = MIMEApplication(pdf_file.read(), _subtype="pdf")
            attach.add_header(
                'Content-Disposition',
                'attachment',
                filename="Garda_Vetting_Form.pdf"  # Name shown in email
            )
            msg.attach(attach)
    except FileNotFoundError:
        return False, f"PDF file not found at {pdf_path}"
    except Exception as e:
        return False, f"Error attaching PDF: {str(e)}"

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


