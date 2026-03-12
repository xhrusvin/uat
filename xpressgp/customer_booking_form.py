# xpressgp/customer_booking_form.py

import smtplib
import os
import random
from flask import render_template, request, flash, redirect, url_for
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime

from . import bp

load_dotenv()

# SMTP Settings
SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
FROM_EMAIL = os.getenv("XP_GP_FROM_EMAIL", SMTP_USER)
ADMIN_EMAIL = "info@xpressgp.ie"
BCC_EMAIL = os.getenv('XP_GP_BCC_EMAIL')
CC_EMAIL = os.getenv('XP_GP_CC_EMAIL')

# MongoDB
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')

if not MONGO_URI or not DB_NAME:
    raise ValueError("MONGO_URI and DB_NAME must be set in .env")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]
gp_registrations_collection = db['xpress_gp_registrations']


@bp.route('/xpress-gp-registration', methods=['GET', 'POST'])
def xpress_gp_registration():
    # Extract slot from URL query parameters (for GET and on redirect)
    slot_date = request.args.get('slot_date')
    slot_time = request.args.get('slot_time')  # Flask auto-decodes %3A to :

    if request.method == 'POST':
        # Emergency care check
        emergency_care = request.form.get('emergency_care')
        if emergency_care == 'yes':
            flash('This service is not suitable for emergency care. Please call 999 or 112 immediately.', 'error')
            return redirect(url_for('.xpress_gp_registration',
                                    slot_date=request.form.get('slot_date'),
                                    slot_time=request.form.get('slot_time')))

        # Collect form data
        registration_id_input = request.form.get('registration_id', '').strip() or None

        # Generate unique 5-digit reference ID
        reference_id = generate_reference_id()
        full_name = request.form.get('full_name', '').strip()
        dob_day = request.form.get('dob_day')
        dob_month = request.form.get('dob_month')
        dob_year = request.form.get('dob_year')
        phone = request.form.get('phone', '').strip()
        email = request.form.get('email', '').strip().lower()
        pps_number = request.form.get('pps_number', '').strip().upper()
        followup = request.form.get('followup')
        health_concern = request.form.get('health_concern', '').strip()

        # Hidden slot fields from form (preserved on submit)
        form_slot_date = request.form.get('slot_date')
        form_slot_time = request.form.get('slot_time')

        # Validation: required fields
        required_fields = [
            full_name, dob_day, dob_month, dob_year,
            phone, email, pps_number, health_concern
        ]
        if not all(required_fields):
            flash('Please fill in all required fields.', 'error')
            return redirect(url_for('.xpress_gp_registration',
                                    slot_date=form_slot_date,
                                    slot_time=form_slot_time))

        # Validate and construct Date of Birth
        try:
            dob = f"{dob_year}-{dob_month.zfill(2)}-{dob_day.zfill(2)}"
            datetime.strptime(dob, '%Y-%m-%d')  # Basic validity check
        except ValueError:
            flash('Please enter a valid date of birth.', 'error')
            return redirect(url_for('.xpress_gp_registration',
                                    slot_date=form_slot_date,
                                    slot_time=form_slot_time))

        # Format requested appointment string (for display and email)
        appointment_info = None
        if form_slot_date and form_slot_time:
            try:
                # Optional: format time nicely (e.g. 09:00 → 9:00 AM)
                time_obj = datetime.strptime(form_slot_time, '%H:%M')
                formatted_time = time_obj.strftime('%I:%M %p').lstrip('0')
                appointment_info = f"{form_slot_date} at {formatted_time}"
            except ValueError:
                appointment_info = f"{form_slot_date} at {form_slot_time}"

        # Save to MongoDB
        registration_data = {
            "xpress_gp_registration_id": registration_id_input,
            "reference_id": reference_id,
            "full_name": full_name,
            "date_of_birth": dob,
            "phone": phone,
            "email": email,
            "pps_number": pps_number,
            "is_followup": followup == 'yes',
            "health_concern": health_concern,
            "requested_slot_date": form_slot_date,
            "requested_slot_time": form_slot_time,
            "requested_appointment": appointment_info,
            "emergency_care": False,
            "created_at": datetime.utcnow(),
            "status": "pending"
        }

        result = gp_registrations_collection.insert_one(registration_data)
        mongo_id = str(result.inserted_id)

        # Send emails
        customer_success = send_customer_confirmation(
            full_name=full_name,
            email=email,
            mongo_id=mongo_id,
            registration_id_input=registration_id_input,
            health_concern=health_concern,
            appointment_info=appointment_info or "No specific time requested",
            reference_id=reference_id
        )

        admin_success = send_admin_notification(
            full_name=full_name,
            dob=dob,
            phone=phone,
            email=email,
            pps_number=pps_number,
            followup="Yes" if followup == 'yes' else "No",
            health_concern=health_concern,
            registration_id_input=registration_id_input or "Not provided",
            appointment_info=appointment_info or "No specific time requested",
            mongo_id=mongo_id,
            reference_id=reference_id,
            bcc=BCC_EMAIL
        )

        if customer_success and admin_success:
            flash('Thank you! Your Xpress GP appointment booking has been submitted successfully. '
                  'We will contact you shortly to arrange an appointment.', 'success')
        else:
            flash('Your request was recorded, but there was an issue sending confirmation emails. '
                  'We will still process your appointment booking soon.', 'warning')

        # Clear slot params on successful submission (optional)
        return redirect(url_for('.xpress_gp_registration'))

    # GET request: render form with slot info if present
    return render_template(
        'xpress_gp_registration_form.html',
        slot_date=slot_date,
        slot_time=slot_time
    )


def send_email(to_email, subject, html_content, bcc=None):
    """Reusable email sender"""
    msg = MIMEMultipart("alternative")
    msg["From"] = FROM_EMAIL
    msg["To"] = to_email
    #msg["To"] = "rusvin@xpresshealth.ie"
    if bcc:
        msg["Bcc"] = "pomin@xpresshealth.ie"
    msg["Cc"] = CC_EMAIL
    msg["Subject"] = subject
    msg.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"Email sending failed: {e}")
        return False


def send_customer_confirmation(full_name, email, mongo_id, registration_id_input, health_concern, appointment_info, reference_id=None):
    subject = "Xpress GP Online Appointment Booking Received – Thank You"
    html_content = render_template(
        'email/xpress_gp_customer_confirmation.html',
        full_name=full_name,
        mongo_id=mongo_id,
        registration_id=registration_id_input or "Not provided",
        health_concern=health_concern[:200] + "..." if len(health_concern) > 200 else health_concern,
        appointment_info=appointment_info,
        current_year=datetime.now().year,
        reference_id=reference_id
    )
    return send_email(email, subject, html_content)

def generate_reference_id():
    """Generate a random 5-digit numeric reference ID"""
    return ''.join(random.choices('0123456789', k=5))

def send_admin_notification(full_name, dob, phone, email, pps_number, followup,
                            health_concern, registration_id_input, appointment_info, mongo_id, reference_id=None, bcc=None):
    subject = f"New Xpress GP Online Appointment Booking – {full_name}"
    html_content = render_template(
        'email/xpress_gp_admin_notification.html',
        full_name=full_name,
        dob=dob,
        phone=phone,
        email=email,
        pps_number=pps_number,
        followup=followup,
        health_concern=health_concern,
        registration_id_input=registration_id_input,
        appointment_info=appointment_info,
        mongo_id=mongo_id,
        current_year=datetime.now().year,
        reference_id=reference_id,
        current_time=datetime.now()
    )
    return send_email(ADMIN_EMAIL, subject, html_content, bcc)