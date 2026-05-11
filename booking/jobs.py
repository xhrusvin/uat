# booking/job.py
from datetime import datetime
from flask import render_template, request, jsonify
from bson import ObjectId
from io import BytesIO
import pandas as pd

from database import db
from booking.models.job import Job

job_model = Job(db.jobs)

from . import bp
from admin.views import admin_required


@bp.route('/jobs')
@admin_required
def jobs():
    page = int(request.args.get('page', 1))
    search = request.args.get('search', '').strip()
    per_page = 10

    jobs_list, total = job_model.get_all(search, page, per_page)

    pages = (total + per_page - 1) // per_page if per_page else 1

    return render_template(
        'booking/jobs.html',
        jobs=jobs_list,
        page=page,
        total=total,
        per_page=per_page,
        pages=pages,
        search=search
    )


@bp.route('/jobs/add', methods=['POST'])
@admin_required
def job_add():
    data = request.get_json()

    title = data.get('title', '').strip()
    if not title:
        return jsonify({"success": False, "error": "Job title is required"}), 400

    client_id = data.get('client_id', '').strip()
    if not client_id:
        return jsonify({"success": False, "error": "Client is required"}), 400

    job_data = {
        "title": title,
        "client_id": ObjectId(client_id),
        "client_name": data.get('client_name', '').strip(),
        "job_type": data.get('job_type', '').strip(),
        "status": data.get('status', 'Pending').strip(),
        "location": data.get('location', '').strip(),
        "scheduled_date": _parse_date(data.get('scheduled_date')),
        "description": data.get('description', '').strip(),
        "notes": data.get('notes', '').strip(),
        "is_active": data.get('is_active') is True,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }

    try:
        job_model.create(job_data)
        return jsonify({"success": True, "message": "Job created successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/jobs/edit', methods=['POST'])
@admin_required
def job_edit():
    data = request.get_json()
    job_id = data.get('job_id')

    if not job_id:
        return jsonify({"success": False, "error": "No job ID provided"}), 400

    title = data.get('title', '').strip()
    if not title:
        return jsonify({"success": False, "error": "Job title is required"}), 400

    client_id = data.get('client_id', '').strip()
    if not client_id:
        return jsonify({"success": False, "error": "Client is required"}), 400

    update_data = {
        "title": title,
        "client_id": ObjectId(client_id),
        "client_name": data.get('client_name', '').strip(),
        "job_type": data.get('job_type', '').strip(),
        "status": data.get('status', 'Pending').strip(),
        "location": data.get('location', '').strip(),
        "scheduled_date": _parse_date(data.get('scheduled_date')),
        "description": data.get('description', '').strip(),
        "notes": data.get('notes', '').strip(),
        "is_active": data.get('is_active') is True,
        "updated_at": datetime.utcnow(),
    }

    try:
        job_model.update(job_id, update_data)
        return jsonify({"success": True, "message": "Job updated successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/jobs/delete', methods=['POST'])
@admin_required
def job_delete():
    data = request.get_json()
    job_id = data.get('job_id')

    if not job_id:
        return jsonify({"success": False, "error": "Missing job_id"}), 400

    try:
        job_model.delete(job_id)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/jobs/import', methods=['POST'])
@admin_required
def job_import():
    if 'file' not in request.files:
        return jsonify({"success": False, "error": "No file uploaded"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"success": False, "error": "No file selected"}), 400

    if not file.filename.lower().endswith(('.xlsx', '.xls')):
        return jsonify({"success": False, "error": "Only .xlsx or .xls files allowed"}), 400

    try:
        excel_data = BytesIO(file.read())
        df = pd.read_excel(excel_data, engine='openpyxl')

        # Normalize column names
        df.columns = df.columns.str.strip().str.lower().str.replace(r'\s+', '_', regex=True)

        created = 0
        skipped = 0
        errors = []

        for _, row in df.iterrows():
            try:
                title = str(row.get('title', '')).strip()
                if not title:
                    skipped += 1
                    errors.append(f"Skipped row (no title): {row.get('title', 'unknown')}")
                    continue

                job_data = {
                    "title": title,
                    "client_id": None,  # Can be resolved by name lookup if needed
                    "client_name": str(row.get('client_name', '')).strip() or None,
                    "job_type": str(row.get('job_type', '')).strip() or None,
                    "status": str(row.get('status', 'Pending')).strip() or 'Pending',
                    "location": str(row.get('location', '')).strip() or None,
                    "scheduled_date": _parse_date(str(row.get('scheduled_date', '')).strip()),
                    "description": str(row.get('description', '')).strip() or None,
                    "notes": str(row.get('notes', '')).strip() or None,
                    "is_active": True,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }

                job_model.create(job_data)
                created += 1

            except Exception as row_err:
                skipped += 1
                errors.append(f"Error in row: {row.get('title', 'unknown')} - {str(row_err)}")

        message = f"Import complete: {created} created, {skipped} skipped."
        if errors:
            message += f" Errors: {len(errors)}"

        return jsonify({
            "success": True,
            "message": message,
            "details": {"created": created, "skipped": skipped, "errors": errors[:10]}
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"Import failed: {str(e)}"}), 500


@bp.route('/jobs/search', methods=['GET'])
def job_search():
    q = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    per_page = 20

    query = {"is_active": True}
    if q:
        query["title"] = {"$regex": q, "$options": "i"}

    jobs = list(db.jobs.find(query)
                .sort("created_at", -1)
                .skip((page - 1) * per_page)
                .limit(per_page))

    total = db.jobs.count_documents(query)

    return jsonify({
        "items": [
            {
                "_id": str(j["_id"]),
                "title": j["title"],
                "client_name": j.get("client_name", ""),
                "job_type": j.get("job_type", ""),
                "status": j.get("status", ""),
                "location": j.get("location", ""),
            }
            for j in jobs
        ],
        "more": (page * per_page) < total
    })


# ── helpers ──────────────────────────────────────────────────────────────────

def _parse_date(value):
    """Safely parse a date string to datetime, or return None."""
    if not value or str(value).strip() in ('', 'None', 'nan', 'NaT'):
        return None
    for fmt in ('%Y-%m-%d', '%d/%m/%Y', '%d-%m-%Y', '%m/%d/%Y'):
        try:
            return datetime.strptime(str(value).strip(), fmt)
        except ValueError:
            continue
    return None