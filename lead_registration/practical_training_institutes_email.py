import smtplib
import os
import re
from flask import jsonify, render_template, request, current_app
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId

from . import bp

load_dotenv()

# ==================== SMTP CONFIG ====================
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)

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
    Processes ONE eligible lead.
    Supports ?id=... and ?county=...
    """
    lead_id_param = request.args.get("id")
    county = request.args.get("county")  # e.g. ?county=Wexford

    try:
        required_fields_query = {
            "email": {"$exists": True, "$nin": [None, ""]},
            "name": {"$exists": True, "$nin": [None, ""]},
            "phone": {"$exists": True, "$nin": [None, ""]},
        }

        lead = None

        # 1. Specific lead by ?id=...
        if lead_id_param:
            if not ObjectId.is_valid(lead_id_param):
                return jsonify({"status": "error", "message": "Invalid ID format"}), 400

            lead = website_leads_collection.find_one({
                "_id": ObjectId(lead_id_param),
                **required_fields_query
            })

            if not lead:
                return jsonify({
                    "status": "no_lead",
                    "message": "Specific lead not found or missing required fields"
                }), 200

        # 2. Oldest unsent eligible lead
        else:
            query = {
                **required_fields_query,
                "$or": [
                    {"garda_email_sent": {"$ne": 1}},
                    {"garda_email_sent": {"$exists": False}}
                ]
            }

            lead = website_leads_collection.find_one(query, sort=[("created_at", 1)])

            if not lead:
                return jsonify({
                    "status": "no_lead",
                    "message": "No eligible lead found"
                }), 200

        # Extract lead data
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

        # Send email (pass county for template selection)
        success, message = send_lead_email(
            first_name=first_name,
            last_name=last_name,
            dial_code=dial_code,
            phone_number=phone_number,
            email=email,
            lead_id=lead_id,
            designation=designation,
            county=county
        )

        if success:
            # Mark as sent
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


def send_lead_email(first_name, last_name, dial_code, phone_number, email, lead_id, designation, county=None):
    """
    Send lead email with optional county-specific template and PDF attachment.
    """
    current_year = datetime.now().year

    # Choose template based on county
    
    template_name = "email/practical_training_institutes.html"
    subject = "List of Practical Training Institutes"

    msg = MIMEMultipart("alternative")
    msg["From"] = FROM_EMAIL
    msg["To"] = email
    if BCC_EMAIL:
        msg["Bcc"] = BCC_EMAIL
    if CC_EMAIL:
        msg["Cc"] = CC_EMAIL

    msg["Subject"] = subject

    # Render HTML template
    html_content = render_template(
        template_name,
        first_name=first_name,
        last_name=last_name,
        dial_code=dial_code,
        phone_number=phone_number,
        email=email,
        lead_id=lead_id,
        designation=designation,
        current_year=current_year,
        county=county
    )

    msg.attach(MIMEText(html_content, "html"))

    # ====================== ATTACH PDF ======================
    pdf_path = os.path.join(
        current_app.root_path,
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
                filename="Garda_Vetting_Form.pdf"
            )
            #msg.attach(attach)          # ← Now actually attaching!
    except FileNotFoundError:
        return False, f"PDF file not found at {pdf_path}"
    except Exception as e:
        return False, f"Error attaching PDF: {str(e)}"

    # ====================== SEND EMAIL ======================
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        return True, f"Email sent successfully to {email}"

    except Exception as e:
        return False, f"SMTP error: {str(e)}"