# admin/leads.py
from flask import render_template, request, flash, redirect, url_for, current_app, Response, send_file, make_response
from werkzeug.utils import secure_filename
from datetime import datetime
from flask import jsonify
import os
import io
import csv
import xlsxwriter
from bson import ObjectId
import re
import asyncio
import aiohttp

from .views import admin_bp
from .views import admin_required

ALLOWED_EXTENSIONS = {'csv'}
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads", "leads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def run_async(coro):
    """
    Run an async coroutine from a synchronous Flask view.
    Creates a new event loop safely – works even when called from a running loop.
    """
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # We're inside an existing event loop → use new_event_loop + thread
            import threading
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            return future.result(timeout=30)
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        # No event loop exists → create a fresh one
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def build_pagination_url(endpoint, page, request_args):
    """Build URL for pagination, excluding current 'page' to avoid duplicates."""
    # Copy args and remove existing 'page' if present
    args_copy = dict(request_args)
    args_copy.pop('page', None)
    # Add new page
    args_copy['page'] = page
    return url_for(endpoint, **args_copy)

def normalize_phone(phone_str):
    """Clean and normalize phone number: remove everything except digits, handle ="" prefix"""
    if not phone_str:
        return None
    # Remove Excel's ="447..." wrapper
    phone_str = re.sub(r'^="?\+?(\d+)"?$', r'\1', phone_str.strip())
    # Remove all non-digit characters
    cleaned = re.sub(r'\D', '', phone_str)
    # Optional: enforce minimum length (e.g. 10 digits)
    return cleaned if len(cleaned) >= 10 else None

@admin_bp.route('/lead_conversation/<conv_id>')
@admin_required
def get_conversation(conv_id):
    """
    API endpoint to fetch full conversation by _id (from lead_conversations collection)
    Used by the new Leads List modal
    """
    if not ObjectId.is_valid(conv_id):
        return jsonify({"error": "Invalid conversation ID"}), 400

    conv = current_app.db.lead_conversations.find_one({"_id": ObjectId(conv_id)})
    
    if not conv:
        return jsonify({"error": "Conversation not found"}), 404

    # Format turns nicely for frontend
    turns = []
    for turn in conv.get("turns", []):
        turns.append({
            "role": turn.get("role"),
            "text": turn.get("text", ""),
            "ts": turn.get("ts", conv.get("started_at"))  # fallback to start time
        })

    result = {
        "_id": str(conv["_id"]),
        "lead_id": str(conv.get("lead_id")),
        "phone": conv.get("phone"),
        "started_at": conv.get("started_at").isoformat() if conv.get("started_at") else None,
        "ended_at": conv.get("ended_at").isoformat() if conv.get("ended_at") else None,
        "elevenlabs_conversation_id": conv.get("elevenlabs_conversation_id"),
        "turns": turns
    }

    return jsonify(result)
# ── SAMPLE CSV ──
@admin_bp.route('/leads/download-sample')
@admin_required
def download_sample_csv():
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Name', 'Email ID', 'Phone Number', 'User Type', 'Location'])
    writer.writerow(['Christine McStravick', 'christine@example.com', '+447123456789', 'Nurses', 'Northern Ireland'])
    output.seek(0)
    return Response(output, mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=leads_sample.csv"})


# ── MAIN IMPORT WITH PHONE CLEANING + DUPLICATE CHECK ──
@admin_bp.route('/leads', methods=['GET', 'POST'])
@admin_required
def leads_management():
    if request.method == 'POST':
        file = request.files.get('file')
        if not file or file.filename == '' or not allowed_file(file.filename):
            flash('Please select a valid CSV file', 'danger')
            return redirect(request.url)

        filename = secure_filename(file.filename)
        filepath = os.path.join(UPLOAD_FOLDER, filename)

        try:
            file.save(filepath)
            records = []
            seen_phones = set()
            duplicates_found = 0
            imported_count = 0

            with open(filepath, 'r', encoding='utf-8-sig', newline='') as f:  # utf-8-sig handles BOM
                reader = csv.DictReader(f)

                # Normalize column names
                reader.fieldnames = [name.strip() for name in reader.fieldnames if name]

                for row_num, row in enumerate(reader, start=2):  # start=2 for actual data row
                    raw_name = (row.get('Name') or '').strip()
                    raw_email = (row.get('Email ID') or row.get('Email') or '').strip().lower()
                    raw_phone = (row.get('Phone Number') or '').strip()

                    # Normalize phone
                    phone = normalize_phone(raw_phone)

                    if not phone:
                        current_app.logger.warning(f"Row {row_num}: Invalid or missing phone number: {raw_phone}")
                        continue  # Skip if no valid phone

                    # Check for duplicate phone in this import batch
                    if phone in seen_phones:
                        duplicates_found += 1
                        current_app.logger.info(f"Row {row_num}: Duplicate phone in file: {phone}")
                        continue
                    seen_phones.add(phone)

                    # Check if phone already exists in DB
                    if current_app.db.leads.find_one({"phone_number": phone}):
                        duplicates_found += 1
                        current_app.logger.info(f"Row {row_num}: Phone already exists in DB: {phone}")
                        continue

                    # Build clean lead
                    lead = {
                        "name": raw_name or None,
                        "email_id": raw_email if raw_email and '@' in raw_email else None,
                        "phone_number": phone,
                        "user_type": (row.get('User Type') or '').strip().title() or None,
                        "location": (row.get('Location') or '').strip().title() or None,
                        "call_initiated": False,
                        "call_answered": None,
                        "feedback": None,
                        "uploaded_at": datetime.utcnow(),
                        "source_file": filename
                    }

                    # Remove None values except booleans
                    lead = {k: v for k, v in lead.items() if v is not None or isinstance(v, bool)}

                    records.append(lead)
                    imported_count += 1

            # Insert all valid unique leads
            if records:
                result = current_app.db.leads.insert_many(records, ordered=False)
                flash(f'Success! Imported {len(result.inserted_ids)} new leads.', 'success')
                if duplicates_found:
                    flash(f'{duplicates_found} duplicate phone number(s) were skipped.', 'warning')
            else:
                flash('No new leads to import (all duplicates or invalid).', 'info')

        except Exception as e:
            current_app.logger.error(f"CSV import failed: {e}")
            flash('Error processing file. Check format and try again.', 'danger')
        finally:
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except:
                    pass

        return redirect(url_for('admin.leads_management'))

    # GET: Dashboard
    total_leads = current_app.db.leads.count_documents({})
    recent_leads = list(current_app.db.leads.find().sort("uploaded_at", -1).limit(10))

    return render_template('admin/leads.html',
                           total_leads=total_leads,
                           recent_leads=recent_leads)


# ── LIST ALL LEADS (with search + status + COUNTRY filter) ──
@admin_bp.route('/leads/list')
@admin_required
def leads_list():
    page = int(request.args.get('page', 1))
    per_page = 25
    search = request.args.get('search', '').strip()
    status = request.args.get('status', '')
    country = request.args.get('country', '')  # ← NEW: Country filter

    # Build base query
    query = {}

    # Search filter
    if search:
        query["$or"] = [
            {"phone_number": {"$regex": search, "$options": "i"}},
            {"name": {"$regex": search, "$options": "i"}},
            {"email_id": {"$regex": search, "$options": "i"}},
        ]

    # Status filter (your existing logic – unchanged)
    if status == "not_called":
        query["call_initiated"] = {"$ne": True}
    elif status == "called":
        query["call_initiated"] = True
        query["call_answered"] = False
    elif status == "started":
        query["call_initiated"] = True
        query["call_answered"] = True
        query["conversation"] = {"$exists": False}
    elif status == "in_progress":
        query["conversation.ended_at"] = {"$exists": False}
        query["conversation.started_at"] = {"$exists": True}
    elif status == "completed":
        query["conversation.ended_at"] = {"$exists": True}

    # NEW: Country filter – matches your fb_lead_fetcher.py logic
    if country:
        try:
            country_id = int(country)
            query["country"] = country_id
        except:
            pass  # ignore invalid

    # Final pipeline
    pipeline = [
        {"$match": query},
        {
            "$lookup": {
                "from": "lead_conversations",
                "localField": "_id",
                "foreignField": "lead_id",
                "as": "conversation"
            }
        },
        {
            "$unwind": {
                "path": "$conversation",
                "preserveNullAndEmptyArrays": True
            }
        },
        {"$sort": {"uploaded_at": -1}}
    ]

    # Total count
    count_result = list(current_app.db.leads.aggregate(pipeline + [{"$count": "total"}]))
    total = count_result[0]["total"] if count_result else 0

    # Paginated results
    raw_leads = list(current_app.db.leads.aggregate(
        pipeline + [
            {"$skip": (page - 1) * per_page},
            {"$limit": per_page}
        ]
    ))

    leads = []
    for doc in raw_leads:
        conv = doc.get("conversation")
        if isinstance(conv, list):
            doc["conversation"] = conv[0] if conv else None
        else:
            doc["conversation"] = conv
        leads.append(doc)

    total_pages = (total + per_page - 1) // per_page

    return render_template("admin/leads_list.html",
                           leads=leads,
                           total=total,
                           page=page,
                           per_page=per_page,
                           total_pages=total_pages,
                           search=search,
                           status=status,
                           country=country)  # ← pass country back to template

@admin_bp.route('/leads/bulk-delete', methods=['POST'])
@admin_required
def bulk_delete_leads():
    lead_ids = request.form.getlist('lead_ids')
    if not lead_ids:
        flash('No leads selected for deletion.', 'warning')
        return redirect(url_for('admin.leads_list'))

    # Convert to ObjectId
    from bson import ObjectId
    object_ids = [ObjectId(oid) for oid in lead_ids]

    # Delete leads + their conversations
    deleted = current_app.db.leads.delete_many({"_id": {"$in": object_ids}})
    current_app.db.lead_conversations.delete_many({"lead_id": {"$in": object_ids}})

    flash(f'Successfully deleted {deleted.deleted_count} lead(s).', 'success')
    return redirect(url_for('admin.leads_list'))

# ── CLEAR ALL LEADS ──

@admin_bp.route('/leads/bulk-action', methods=['POST'])
@admin_required
def bulk_action():
    action = request.form.get('action')
    lead_ids = request.form.getlist('lead_ids')

    if not lead_ids and action != 'export_all':
        flash('No leads selected.', 'warning')
        return redirect(url_for('admin.leads_list'))

    # Convert IDs
    if lead_ids:
        object_ids = [ObjectId(oid) for oid in lead_ids]
        leads = list(current_app.db.leads.find({"_id": {"$in": object_ids}}))
    else:
        # Export all (filtered)
        leads = list(current_app.db.leads.find())

    if action == 'delete':
        if lead_ids:
            current_app.db.leads.delete_many({"_id": {"$in": object_ids}})
            current_app.db.lead_conversations.delete_many({"lead_id": {"$in": object_ids}})
            flash(f'Deleted {len(lead_ids)} lead(s).', 'success')
        return redirect(url_for('admin.leads_list'))

    elif action in ['export_selected', 'export_all']:
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet()

        headers = ['Name', 'Email', 'Phone', 'User Type', 'Location', 'Status', 'Uploaded At']
        worksheet.write_row(0, 0, headers)

        for i, lead in enumerate(leads, 1):
            status = "Not Called"
            conv = current_app.db.lead_conversations.find_one({"lead_id": lead['_id']})
            if conv:
                if conv.get('ended_at'):
                    status = "Completed"
                elif not conv.get('ended_at'):
                    status = "In Progress"
            elif lead.get('call_initiated'):
                status = "No Answer" if not lead.get('call_answered') else "Call Started"

            row = [
                lead.get('name', ''),
                lead.get('email_id', ''),
                lead.get('phone_number', ''),
                lead.get('user_type', ''),
                lead.get('location', ''),
                status,
                lead.get('uploaded_at', '').strftime('%Y-%m-%d %H:%M') if lead.get('uploaded_at') else ''
            ]
            worksheet.write_row(i, 0, row)

        workbook.close()
        output.seek(0)

        filename = f"leads_export_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )

    return redirect(url_for('admin.leads_list'))
# === NUEVA RUTA PARA AUDIO DE LEADS ===
@admin_bp.route('/lead_conversation/<conv_id>/audio')
@admin_required
def get_lead_conversation_audio(conv_id):
    """Serve ElevenLabs-generated audio for lead conversations"""
    if not ObjectId.is_valid(conv_id):
        return "Invalid conversation ID", 400

    conv = current_app.db.lead_conversations.find_one({"_id": ObjectId(conv_id)})
    if not conv:
        return "Conversation not found", 404

    el_id = conv.get("elevenlabs_conversation_id")
    if not el_id:
        return "Audio not generated for this call", 404

    url = f"https://api.elevenlabs.io/v1/convai/conversations/{el_id}/audio"
    api_key = os.getenv("ELEVENLABS_API_KEY")
    if not api_key:
        return "Server configuration error (missing API key)", 500

    audio_bytes = run_async(fetch_audio(url, api_key))

    if not audio_bytes:
        return "Audio not available yet (still processing)", 404

    return Response(
        audio_bytes,
        mimetype="audio/mpeg",
        headers={"Content-Disposition": f'attachment; filename="lead_call_{conv_id}.mp3"'}
    )

# Helper (reuse your existing run_async from admin.py)
async def fetch_audio(url, api_key):
    async with aiohttp.ClientSession() as session:
        headers = {"xi-api-key": api_key}
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                return None
            return await resp.read()


# ────────────────────────────────
# ElevenLabs API Proxy for Lead Conversations
# ────────────────────────────────
@admin_bp.route('/lead_elevenlabs/api/conversation/<conversation_id>')
@admin_required
def lead_elevenlabs_api_proxy(conversation_id):
    """Proxy to ElevenLabs API for lead conversation data"""
    api_key = os.getenv("ELEVENLABS_API_KEY")
    if not api_key:
        return jsonify({"error": "API key missing"}), 500

    url = f"https://api.elevenlabs.io/v1/convai/conversations/{conversation_id}"
    headers = {"xi-api-key": api_key}

    try:
        import requests
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return jsonify({
                "error": "ElevenLabs API error", 
                "details": resp.text
            }), resp.status_code
        
        data = resp.json()
        return jsonify(data)
        
    except Exception as e:
        current_app.logger.error(f"ElevenLabs proxy error: {e}")
        return jsonify({"error": str(e)}), 500

@admin_bp.route('/leads/clear-all')
@admin_required
def clear_all_leads():
    result = current_app.db.leads.delete_many({})
    # Also clean conversations?
    current_app.db.lead_conversations.delete_many({})
    flash(f"Cleared {result.deleted_count} leads and related conversations.", "success")
    return redirect(url_for('admin.leads_management'))