import os
from datetime import datetime
from flask import render_template, request, jsonify, send_from_directory, current_app
from werkzeug.utils import secure_filename

from database import db
from booking.models.nmbi_qualification import NmbiQualification

from . import bp
from admin.views import admin_required


nmbi_model = NmbiQualification(db.nmbi_qualifications)

UPLOAD_FOLDER = "uploads/nmbi_qualifications"
ALLOWED_EXTENSIONS = {"pdf"}


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def save_pdf(file):
    if not file or file.filename == "":
        return None

    if not allowed_file(file.filename):
        raise ValueError("Only PDF files are allowed")

    upload_path = os.path.join(current_app.root_path, UPLOAD_FOLDER)
    os.makedirs(upload_path, exist_ok=True)

    filename = secure_filename(file.filename)
    unique_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{filename}"

    file.save(os.path.join(upload_path, unique_name))
    return unique_name


@bp.route('/nmbi-qualifications')
@admin_required
def nmbi_qualifications():
    page = int(request.args.get('page', 1))
    search = request.args.get('search', '').strip()
    per_page = 20

    items, total = nmbi_model.get_all(search, page, per_page)
    pages = (total + per_page - 1) // per_page if per_page else 1

    return render_template(
        'booking/nmbi_qualifications.html',
        qualifications=items,
        page=page,
        total=total,
        per_page=per_page,
        pages=pages,
        search=search,
    )


@bp.route('/nmbi-qualifications/add', methods=['POST'])
@admin_required
def nmbi_qualification_add():
    data = request.form

    staff_name = (data.get('staff_name') or '').strip()
    registration_number = (data.get('registration_number') or '').strip()

    if not staff_name:
        return jsonify({"success": False, "error": "Staff name is required"}), 400

    if not registration_number:
        return jsonify({"success": False, "error": "Registration number is required"}), 400

    if not nmbi_model.is_registration_unique(registration_number):
        return jsonify({"success": False, "error": f'"{registration_number}" already exists'}), 400

    try:
        pdf_filename = save_pdf(request.files.get('attachment'))

        doc = {
            "staff_name": staff_name,
            "registration_number": registration_number,
            "division": (data.get('division') or '').strip(),
            "initial_registration_date": (data.get('initial_registration_date') or '').strip(),
            "renewed_until": (data.get('renewed_until') or '').strip(),
            "description": (data.get('description') or '').strip(),
            "is_active": data.get('is_active') == "true",
            "attachment": pdf_filename,
        }

        nmbi_model.create(doc)
        return jsonify({"success": True, "message": "NMBI qualification created"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/nmbi-qualifications/edit', methods=['POST'])
@admin_required
def nmbi_qualification_edit():
    data = request.form

    qualification_id = data.get('qualification_id')
    staff_name = (data.get('staff_name') or '').strip()
    registration_number = (data.get('registration_number') or '').strip()

    if not qualification_id:
        return jsonify({"success": False, "error": "Missing qualification_id"}), 400

    if not staff_name:
        return jsonify({"success": False, "error": "Staff name is required"}), 400

    if not registration_number:
        return jsonify({"success": False, "error": "Registration number is required"}), 400

    if not nmbi_model.is_registration_unique(registration_number, exclude_id=qualification_id):
        return jsonify({"success": False, "error": f'"{registration_number}" already exists'}), 400

    try:
        update = {
            "staff_name": staff_name,
            "registration_number": registration_number,
            "division": (data.get('division') or '').strip(),
            "initial_registration_date": (data.get('initial_registration_date') or '').strip(),
            "renewed_until": (data.get('renewed_until') or '').strip(),
            "description": (data.get('description') or '').strip(),
            "is_active": data.get('is_active') == "true",
        }

        pdf_filename = save_pdf(request.files.get('attachment'))
        if pdf_filename:
            update["attachment"] = pdf_filename

        nmbi_model.update(qualification_id, update)
        return jsonify({"success": True, "message": "NMBI qualification updated"})

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/nmbi-qualifications/delete', methods=['POST'])
@admin_required
def nmbi_qualification_delete():
    data = request.get_json()
    qualification_id = data.get('qualification_id')

    if not qualification_id:
        return jsonify({"success": False, "error": "Missing qualification_id"}), 400

    try:
        nmbi_model.delete(qualification_id)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/nmbi-qualifications/download/<filename>')
@admin_required
def nmbi_qualification_download(filename):
    upload_path = os.path.join(current_app.root_path, UPLOAD_FOLDER)
    return send_from_directory(upload_path, filename, as_attachment=True)