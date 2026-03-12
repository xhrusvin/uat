import smtplib
import os
import re
from flask import jsonify, render_template, request
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId

from . import bp

load_dotenv()

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)
BCC_EMAIL = os.getenv("BCC_EMAIL")

# ==================== MONGO DB CONFIG ====================
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

if not MONGO_URI or not DB_NAME:
    raise ValueError("MONGO_URI and DB_NAME must be set in .env")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
references_collection = db["users"]

# ==================== TEST ROUTE ====================
@bp.route("/send-reference-test-email", methods=["GET"])
def send_reference_test_email():
    dummy_data = {
        "referee_name": "John Smith",
        "candidate_name": "Steffy John",
        "email": "john.smith@example.com"
    }

    return send_reference_email(**dummy_data)

# ==================== MAIN ROUTE ====================
@bp.route("/professional-reference-email", methods=["GET"])
def professional_reference_email():
    """
    Sends ONE professional reference request email.
    If ?id=... is provided, sends for that specific record.
    Otherwise, sends the oldest unsent eligible reference.
    """

    reference_id_param = request.args.get("id")

    try:
        required_fields_query = {
            "name": {"$exists": True, "$nin": [None, ""]},
            "phone": {"$exists": True, "$nin": [None, ""]},
            "xn_user_id": {"$exists": True, "$nin": [None, ""]},
        }

        reference = None

        # -----------------------------
        # 1. Specific reference by ID
        # -----------------------------
        if reference_id_param:
            if not ObjectId.is_valid(reference_id_param):
                return jsonify({
                    "status": "error",
                    "message": "Invalid ID format"
                }), 400

            reference = references_collection.find_one({
                "_id": ObjectId(reference_id_param),
                **required_fields_query
            })

            if not reference:
                return jsonify({
                    "status": "no_record",
                    "message": "Reference not found or missing required fields"
                }), 200

        # -----------------------------
        # 2. Oldest unsent reference
        # -----------------------------
        else:
            query = {
                **required_fields_query,
                "$or": [
                    {"reference_email_sent": {"$ne": 1}},
                    {"reference_email_sent": {"$exists": False}}
                ]
            }

            reference = references_collection.find_one(
                query,
                sort=[("created_at", 1)]
            )

            if not reference:
                return jsonify({
                    "status": "no_record",
                    "message": "No eligible reference found"
                }), 200

        # -----------------------------
        # Process reference
        # -----------------------------
        lead_id = str(reference["_id"])
        first_name = reference["first_name"].strip()
        last_name = reference["last_name"].strip()
        email = reference["email"].strip().lower()
        designation = reference["designation"].strip()

        success, message = send_reference_email(
            first_name=first_name,
            last_name=last_name,
            email=email,
            lead_id=lead_id,
            designation=designation
        )

        if success:
            references_collection.update_one(
                {"_id": reference["_id"]},
                {"$set": {
                    "reference_email_sent": 1,
                    "reference_email_sent_at": datetime.utcnow().isoformat(),
                    "updated_at": datetime.utcnow().isoformat()
                }}
            )

            return jsonify({
                "status": "success",
                "reference_id": lead_id,
                "email": email,
                "message": "Professional reference email sent"
            }), 200

        return jsonify({
            "status": "error",
            "reference_id": lead_id,
            "message": message
        }), 500

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Server error: {str(e)}"
        }), 500


# ==================== EMAIL SENDER ====================
def send_reference_email(first_name, last_name, email, lead_id, designation):
    """
    Sends the professional reference request email using Jinja template.
    """
    current_year = datetime.now().year
    msg = MIMEMultipart("alternative")
    msg["From"] = FROM_EMAIL
    msg["To"] = "rusvin@xpresshealth.ie"
    msg["Bcc"] = BCC_EMAIL
    msg["Subject"] = f"Professional Reference Request – {first_name} {last_name}"

    html_content = render_template(
        "email/professional_reference_mail.html",
        first_name=first_name,
        last_name=last_name,
        email=email,
        lead_id=lead_id,
        designation=designation,
        current_year=current_year
    )

    msg.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)

        return True, "Email sent successfully"

    except Exception as e:
        return False, f"Email send failed: {str(e)}"
