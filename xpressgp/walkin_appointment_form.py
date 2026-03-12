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


@bp.route('/walkin-appointment-form', methods=['GET', 'POST'])
def walkin_appointment_form():
    slot_date = request.args.get('slot_date')
    slot_time = request.args.get('slot_time')

    if request.method == 'POST':
        reference_id = generate_reference_id()
        # ── Collect ALL main fields ───────────────────────────────────────
        full_name          = request.form.get('full_name', '').strip()
        dob_day            = request.form.get('dob_day')
        dob_month          = request.form.get('dob_month')
        dob_year           = request.form.get('dob_year')
        gender             = request.form.get('gender')
        pps_number         = request.form.get('pps_number', '').strip().upper()
        birth_place        = request.form.get('birth_place', '').strip()
        preferred_language = request.form.get('preferred_language')
        interpreter        = request.form.get('interpreter')
        address            = request.form.get('address', '').strip()
        phone              = request.form.get('phone', '').strip()
        email              = request.form.get('email', '').strip().lower()
        
        # Contact methods (list)
        contact_methods    = request.form.getlist('contact_method[]')
        
        # Previous GP
        prev_gp_name       = request.form.get('previous_gp_name', '').strip()
        prev_gp_phone      = request.form.get('previous_gp_phone', '').strip()
        prev_gp_address    = request.form.get('previous_gp_address', '').strip()
        
        # Medical history
        allergies          = request.form.get('allergies')
        allergies_details  = request.form.get('allergies_details', '').strip() if allergies == 'yes' else ''
        medical_history    = request.form.get('medical_history', '').strip()
        ongoing_conditions = request.form.get('ongoing_conditions', '').strip()
        current_medications= request.form.get('current_medications', '').strip()
        
        # Lifestyle
        smoke_drink        = request.form.get('smoke_drink')
        smoke_drink_details= request.form.get('smoke_drink_details', '').strip() if smoke_drink == 'yes' else ''
        alcohol            = request.form.get('alcohol')
        alcohol_units      = request.form.get('alcohol_units', '').strip() if alcohol == 'yes' else ''
        
        # Confirmation fields
        consent            = request.form.get('consent')           # should be "1"
        terms_accept       = request.form.get('terms_accept')      # should be "1"
        sign_day           = request.form.get('sign_day')
        sign_month         = request.form.get('sign_month')
        sign_year          = request.form.get('sign_year')

        # Hidden slot fields
        form_slot_date     = request.form.get('slot_date') or slot_date
        form_slot_time     = request.form.get('slot_time') or slot_time

        # ── Basic required fields validation ───────────────────────────────
        required = [
            full_name, dob_day, dob_month, dob_year, gender,
            pps_number, birth_place, preferred_language, interpreter,
            address, phone, email, contact_methods,
            allergies, smoke_drink, alcohol, consent, terms_accept,
            sign_day, sign_month, sign_year
        ]
        if not all(required):
            flash('Please fill in all required fields.', 'error')
            return redirect(url_for('.walkin_appointment_form',
                                    slot_date=form_slot_date,
                                    slot_time=form_slot_time))

        # Validate DOB
        try:
            dob = f"{dob_year}-{dob_month.zfill(2)}-{dob_day.zfill(2)}"
            datetime.strptime(dob, '%Y-%m-%d')
        except ValueError:
            flash('Invalid date of birth.', 'error')
            return redirect(url_for('.walkin_appointment_form',
                                    slot_date=form_slot_date,
                                    slot_time=form_slot_time))

        # Format appointment info nicely
        appointment_info = "No specific slot requested"
        if form_slot_date and form_slot_time:
            try:
                t = datetime.strptime(form_slot_time, '%H:%M')
                fmt_time = t.strftime('%-I:%M %p').lstrip('0')
                appointment_info = f"{form_slot_date} at {fmt_time}"
            except:
                appointment_info = f"{form_slot_date} at {form_slot_time}"

        # Build data document for MongoDB
        registration_data = {
            "reference_id": reference_id,
            "full_name": full_name,
            "date_of_birth": dob,
            "gender": gender,
            "pps_number": pps_number,
            "birth_place": birth_place,
            "preferred_language": preferred_language,
            "interpreter_needed": interpreter == 'yes',
            "address": address,
            "phone": phone,
            "email": email,
            "preferred_contact_methods": contact_methods,
            "previous_gp": {
                "name": prev_gp_name,
                "phone": prev_gp_phone,
                "address": prev_gp_address
            },
            "allergies": {
                "has_allergies": allergies == 'yes',
                "details": allergies_details
            },
            "medical_history": medical_history,
            "ongoing_conditions": ongoing_conditions,
            "current_medications": current_medications,
            "lifestyle": {
                "smoke_drink": smoke_drink == 'yes',
                "smoke_drink_details": smoke_drink_details,
                "alcohol": alcohol == 'yes',
                "alcohol_units_per_week": alcohol_units
            },
            "requested_slot_date": form_slot_date,
            "requested_slot_time": form_slot_time,
            "requested_appointment": appointment_info,
            "signed_on": f"{sign_year}-{sign_month.zfill(2)}-{sign_day.zfill(2)}",
            "created_at": datetime.utcnow(),
            "status": "pending"
        }

        result = gp_registrations_collection.insert_one(registration_data)
        mongo_id = str(result.inserted_id)

        # Send emails
        customer_ok = send_customer_confirmation(
            full_name=full_name,
            email=email,
            mongo_id=mongo_id,
            appointment_info=appointment_info,
            reference_id=reference_id
        )

        admin_ok = send_admin_notification(
            full_name=full_name,
            dob=dob,
            gender=gender,
            pps_number=pps_number,
            phone=phone,
            email=email,
            address=address,
            appointment_info=appointment_info,
            mongo_id=mongo_id,
            registration_data=registration_data,  # optional: pass more if needed
            reference_id=reference_id
        )

        if customer_ok and admin_ok:
            flash('Thank you! Your registration & appointment request has been successfully submitted. '
                  'We will contact you soon.', 'success')
        else:
            flash('Registration saved, but there was a problem sending email notifications. '
                  'We will process your request anyway.', 'warning')

        return redirect(url_for('.walkin_appointment_form'))

    # GET
    return render_template(
        'walkin_appointment_form.html',   # ← your new big template
        slot_date=slot_date,
        slot_time=slot_time
    )


def send_email(to_email, subject, html_content, bcc=None):
    msg = MIMEMultipart("alternative")
    msg["From"] = FROM_EMAIL
    msg["To"] = to_email
    if bcc:
        msg["Bcc"] = bcc
    if CC_EMAIL:
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
        print(f"Email failed: {e}")
        return False

def generate_reference_id():
    """Generate a random 5-digit numeric reference ID"""
    return ''.join(random.choices('0123456789', k=5))
    
def send_customer_confirmation(full_name, email, mongo_id, appointment_info, reference_id):
    subject = "Xpress GP Online Walk-in Appointment Request Received"
    html_content = render_template(
        'email/walkin_appointment_form_customer.html',
        full_name=full_name,
        appointment_info=appointment_info,
        reference_id=reference_id,  # short friendly id
        current_year=datetime.now().year
    )
    return send_email(email, subject, html_content)


def send_admin_notification(full_name, dob, gender, pps_number, phone, email, address,
                           appointment_info, mongo_id, registration_data, reference_id):
    subject = f"New Xpress GP Online Walk-in Appointment Request – {full_name}"
    html_content = render_template(
        'email/walkin_appointment_form_admin.html',
        full_name=full_name,
        dob=dob,
        gender=gender,
        pps_number=pps_number,
        phone=phone,
        email=email,
        address=address,
        appointment_info=appointment_info,
        mongo_id=mongo_id,
        data=registration_data,
        reference_id=reference_id,
        current_year=datetime.now().year,
        current_time=datetime.now()
    )
    return send_email(ADMIN_EMAIL, subject, html_content, bcc=BCC_EMAIL)