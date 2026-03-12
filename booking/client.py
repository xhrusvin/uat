# booking/client.py
from datetime import datetime
from flask import render_template, request, jsonify
from bson import ObjectId
from io import BytesIO
import pandas as pd

from database import db
from booking.models.client import Client

client_model = Client(db.clients)

from . import bp   # assuming same blueprint as shifts


@bp.route('/clients')
def clients():
    page = int(request.args.get('page', 1))
    search = request.args.get('search', '').strip()
    per_page = 10

    clients_list, total = client_model.get_all(search, page, per_page)

    # Optional: add any formatting if needed in the future
    # (e.g. phone formatting, join date, etc.)
    for c in clients_list:
        # Example: you could add formatted fields here later
        pass

    pages = (total + per_page - 1) // per_page if per_page else 1

    return render_template(
        'booking/clients.html',
        clients=clients_list,
        page=page,
        total=total,
        per_page=per_page,
        pages=pages,
        search=search
    )


@bp.route('/clients/add', methods=['POST'])
def client_add():
    data = request.get_json()

    name = data.get('name', '').strip()
    if not name:
        return jsonify({"success": False, "error": "Client name is required"}), 400

    email = data.get('email', '').strip()
    phone = data.get('phone', '').strip()

    # Minimal validation — extend as needed
    if email and '@' not in email:
        return jsonify({"success": False, "error": "Invalid email format"}), 400

    client_data = {
        "name": name,
        "email": email.lower() if email else None,
        "phone": phone if phone else None,
        "address": data.get('address', '').strip(),
        "county": data.get('county', '').strip(),
        "notes": data.get('notes', '').strip(),
        "client_type": data.get('client_type', '').strip(),
        "is_active": data.get('is_active') is True,
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }

    try:
        client_model.create(client_data)
        return jsonify({"success": True, "message": "Client created successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/clients/edit', methods=['POST'])
def client_edit():
    data = request.get_json()
    client_id = data.get('client_id')

    if not client_id:
        return jsonify({"success": False, "error": "No client ID provided"}), 400

    name = data.get('name', '').strip()
    if not name:
        return jsonify({"success": False, "error": "Client name is required"}), 400

    email = data.get('email', '').strip()
    phone = data.get('phone', '').strip()

    update_data = {
        "name": name,
        "email": email.lower() if email else None,
        "phone": phone if phone else None,
        "address": data.get('address', '').strip(),
        "notes": data.get('notes', '').strip(),
        "client_type": data.get('client_type', '').strip(),
        "is_active": data.get('is_active') is True,
        "updated_at": datetime.utcnow(),
    }

    try:
        client_model.update(client_id, update_data)
        return jsonify({"success": True, "message": "Client updated successfully"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/clients/delete', methods=['POST'])
def client_delete():
    data = request.get_json()
    client_id = data.get('client_id')

    if not client_id:
        return jsonify({"success": False, "error": "Missing client_id"}), 400

    try:
        client_model.delete(client_id)
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/clients/import', methods=['POST'])
def client_import():
    if 'file' not in request.files:
        return jsonify({"success": False, "error": "No file uploaded"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"success": False, "error": "No file selected"}), 400

    if not file.filename.lower().endswith(('.xlsx', '.xls')):
        return jsonify({"success": False, "error": "Only .xlsx or .xls files allowed"}), 400

    try:
        # Read Excel file from memory
        excel_data = BytesIO(file.read())
        df = pd.read_excel(excel_data, engine='openpyxl')

        # Normalize column names (strip, lower, replace space → _)
        df.columns = df.columns.str.strip().str.lower().str.replace(r'\s+', '_', regex=True)

        created = 0
        skipped = 0
        errors = []

        for _, row in df.iterrows():
            try:
                # Map columns (adjust keys based on your Excel headers)
                client_data = {
                    "client_type": str(row.get('client_type', '')).strip() or None,
                    "name": str(row.get('name', '')).strip(),
                    "email": str(row.get('email', '')).strip().lower() or None,
                    "phone": str(row.get('phone_number', '')).strip() or None,
                    "address": str(row.get('address', '')).strip() or None,
                    "county": str(row.get('county', '')).strip() or None,
                    "notes": "",  # or map if you have a notes column
                    "is_active": True,  # default; can map from 'status' == 'Enabled'
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                }

                # Basic validation
                if not client_data["name"]:
                    skipped += 1
                    errors.append(f"Skipped row (no name): {row.get('name', 'unknown')}")
                    continue

                # Optional: upsert by email or phone if exists
                query = {}
            

                if query:
                    existing = client_model.collection.find_one(query)
                    if existing:
                        # Update existing
                        client_model.update(str(existing["_id"]), {
                            "name": client_data["name"],
                            "client_type": client_data["client_type"],
                            "address": client_data["address"],
                            "county": client_data["county"],
                            "notes": client_data["notes"],
                            "is_active": client_data["is_active"],
                            "updated_at": datetime.utcnow(),
                        })
                        continue  # count as updated, not created

                # Create new
                client_model.create(client_data)
                created += 1

            except Exception as row_err:
                skipped += 1
                errors.append(f"Error in row: {row.get('name', 'unknown')} - {str(row_err)}")

        message = f"Import complete: {created} created/updated, {skipped} skipped."
        if errors:
            message += f" Errors: {len(errors)}"

        return jsonify({
            "success": True,
            "message": message,
            "details": {"created": created, "skipped": skipped, "errors": errors[:10]}  # limit errors
        })

    except Exception as e:
        return jsonify({
            "success": False,
            "error": f"Import failed: {str(e)}"
        }), 500

@bp.route('/clients/search', methods=['GET'])
def client_search():
    q = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    per_page = 20

    query = {"is_active": True}
    if q:
        query["name"] = {"$regex": q, "$options": "i"}

    clients = list(db.clients.find(query)
                   .sort("name", 1)
                   .skip((page-1)*per_page)
                   .limit(per_page))

    total = db.clients.count_documents(query)

    return jsonify({
        "items": [
            {
                "_id": str(c["_id"]),
                "name": c["name"],
                "type": c.get("client_type", ""),           # ← NEW
                "default_location": c.get("county", "")  # ← NEW (or use "location")
            }
            for c in clients
        ],
        "more": (page * per_page) < total
    })