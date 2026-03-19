# booking/shift.py
from datetime import datetime
from flask import render_template, request, jsonify
from bson import ObjectId
import json

from database import db
from booking.models.shift import Shift
from booking.models.client import Client
from booking.models.shift_user import ShiftUser
from booking.models.shift_user_assigned import ShiftUserAssigned
shift_user_assigned_model = ShiftUserAssigned(db.shifts_users_assigned)


shift_model = Shift(db.shifts)
client_model = Client(db.clients)
shift_user_model = ShiftUser(db.shifts_users)

from . import bp
from admin.views import admin_required

def serialize_doc(doc):
    """Convert MongoDB document to JSON-serializable dict"""
    if isinstance(doc, dict):
        return {k: serialize_doc(v) for k, v in doc.items()}
    elif isinstance(doc, list):
        return [serialize_doc(item) for item in doc]
    elif isinstance(doc, ObjectId):
        return str(doc)
    elif isinstance(doc, datetime):
        return doc.isoformat()   # or .strftime('%Y-%m-%d %H:%M:%S')
    else:
        return doc

@bp.route('/shifts')
@admin_required
def shifts():
    page = int(request.args.get('page', 1))
    search = request.args.get('search', '').strip()
    date_filter_str = request.args.get('date_filter', '').strip()
    per_page = 10

    # ── Build base match query ────────────────────────────
    base_match = {}

    if date_filter_str:
        try:
            target_date = datetime.strptime(date_filter_str, '%Y-%m-%d')
            base_match["date"] = target_date
        except ValueError:
            pass

    # ── Aggregation pipeline ──────────────────────────────
    pipeline = [
        {"$match": base_match},
        {
            # Convert string client_id → ObjectId so $lookup can match
            "$addFields": {
                "client_id_oid": {
                    "$cond": {
                        "if": {"$and": [
                            {"$ne": ["$client_id", None]},
                            {"$ne": ["$client_id", ""]},
                        ]},
                        "then": {"$toObjectId": "$client_id"},
                        "else": None
                    }
                }
            }
        },
        {
            "$lookup": {
                "from": "clients",
                "localField": "client_id_oid",
                "foreignField": "_id",
                "as": "client"
            }
        },
        {
            "$addFields": {
                "client_name": {"$arrayElemAt": ["$client.name", 0]}
            }
        },
        {"$unset": ["client", "client_id_oid"]},
    ]

    # ── Text + client_name search (applied AFTER $lookup) ──
    if search:
        regex = {"$regex": search, "$options": "i"}
        pipeline.append({
            "$match": {
                "$or": [
                    {"name": regex},
                    {"location": regex},
                    {"shift_xn_id": regex},
                    {"description": regex},
                    {"client_name": regex},
                ]
            }
        })

    # ── Explicitly keep all needed fields + slots ────────────────────
    pipeline.append({
        "$project": {
            "_id": 1,
            "name": 1,
            "date": 1,
            "start_time": 1,
            "end_time": 1,
            "shift_xn_id": 1,
            "description": 1,
            "client_id": 1,
            "client_type": 1,
            "location": 1,
            "postal_code": 1,
            "is_active": 1,
            "is_premium": 1,
            "status": 1,
            "rate": 1,
            "created_at": 1,
            "updated_at": 1,
            "slots": 1,                # ← critical line
            "client_name": 1
        }
    })

    # ── Count before pagination ───────────────────────────
    count_pipeline = pipeline + [{"$count": "total"}]
    count_result = list(db.shifts.aggregate(count_pipeline))
    total = count_result[0]["total"] if count_result else 0

    # ── Sort + paginate ───────────────────────────────────
    pipeline.extend([
        {"$sort": {"created_at": -1}},
        {"$skip": (page - 1) * per_page},
        {"$limit": per_page},
    ]) 

    shifts_list = list(db.shifts.aggregate(pipeline))



   
    # ── Format times ──────────────────────────────────────
        # ── Format times + serialize slots for edit button ────────────────────────────────
    for s in shifts_list:
        # Format times
        if isinstance(s.get('start_time'), datetime):
            s['start_time_formatted'] = s['start_time'].strftime('%H:%M')
        if isinstance(s.get('end_time'), datetime):
            s['end_time_formatted'] = s['end_time'].strftime('%H:%M')

        if not s.get('client_name'):
            s['client_name'] = '—'

        # Serialize slots — MUST BE INSIDE THE LOOP
        slots_for_js = []
        raw_slots = s.get('slots', [])
        if isinstance(raw_slots, list):
            for slot in raw_slots:
                slot_date = slot.get('date')
                
                # Handle different date formats safely
                if isinstance(slot_date, datetime):
                    date_str = slot_date.strftime('%Y-%m-%d')
                elif isinstance(slot_date, dict) and '$date' in slot_date:
                    try:
                        dt = datetime.fromisoformat(slot_date['$date'].replace('Z', '+00:00'))
                        date_str = dt.strftime('%Y-%m-%d')
                    except Exception as e:
                        print(f"Date parse error: {e}")
                        date_str = ''
                elif isinstance(slot_date, str):
                    date_str = slot_date.split('T')[0] if 'T' in slot_date else slot_date
                else:
                    date_str = ''

                slots_for_js.append({
                    'date': date_str,
                    'start_time': slot.get('start_time', '') or '',
                    'end_time': slot.get('end_time', '') or '',
                    'shift_xn_id': slot.get('shift_xn_id', '') or '',
                    'shift_type': slot.get('shift_type', '') or '',
                })

        s['slots_json'] = slots_for_js  # ← assign per shift

    pages = (total + per_page - 1) // per_page if per_page else 1

    return render_template(
        'booking/shifts.html',
        shifts=shifts_list,
        page=page,
        total=total,
        per_page=per_page,
        pages=pages,
        search=search,
        date_filter=date_filter_str,
    )

@bp.route('/shifts/add', methods=['POST'])
def shift_add():
    data = request.get_json()

    slots_raw = data.get('shifts', [])
    if not slots_raw:
        return jsonify({"success": False, "error": "At least one date/time slot is required"}), 400

    # Validate and build slots array
    slots = []
    errors = []
    for i, slot in enumerate(slots_raw, 1):
        try:
            date = datetime.strptime(slot['date'], '%Y-%m-%d')
        except (KeyError, ValueError):
            errors.append(f"Row {i}: Invalid or missing date")
            continue

        shift_xn = slot.get('shift_xn_id', '').strip() or None
        if shift_xn and not shift_model.is_shift_xn_id_unique(shift_xn):
            errors.append(f"Shift ID '{shift_xn}' already exists")
            #errors.append(f"Row {i}: Shift ID '{shift_xn}' already exists")
            continue

        slots.append({
            "date": date,
            "start_time": slot.get('start_time', ''),
            "end_time": slot.get('end_time', ''),
            "shift_xn_id": shift_xn,
            "shift_type": slot.get('shift_type', '').strip() or None,
        })

    if errors:
        return jsonify({"success": False, "error": errors[0], "details": errors}), 400

    name = data.get('name', '').strip()
    shift_data = {
        "name": name or None,
        "slots": slots,
        # Keep top-level date/start/end from first slot for backwards compatibility
        "date": slots[0]["date"],
        "start_time": slots[0]["start_time"],
        "end_time": slots[0]["end_time"],
        "shift_xn_id": slots[0]["shift_xn_id"],
        "description": data.get('description', ''),
        "client_id": data.get('client_id') or None,
        "client_type": data.get('client_type', '').strip(),
        "location": data.get('location', '').strip(),
        "postal_code": data.get('postal_code', '').strip() or None,
        "is_active": data.get('is_active') is True,
        "is_premium": data.get('is_premium') in (True, 'true', 'on', '1', 1),
        "status": data.get('status', 'To be assigned').strip() or 'To be assigned',
        "rate": float(data['rate']) if data.get('rate') not in (None, '', 'null') else None,
    }

    try:
        shift_model.create(shift_data)
        return jsonify({"success": True, "message": "Shift created with {} slot(s)".format(len(slots))})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/shifts/edit', methods=['POST'])
def shift_edit():
    data = request.get_json()
    shift_id = data.get('shift_id')
    if not shift_id:
        return jsonify({"success": False, "error": "No shift ID"}), 400

    slots_raw = data.get('shifts', [])
    slots = []
    for slot in slots_raw:
        try:
            d = datetime.strptime(slot['date'], '%Y-%m-%d')
            slots.append({
                "date": d,
                "start_time": slot.get('start_time', ''),
                "end_time": slot.get('end_time', ''),
                "shift_xn_id": slot.get('shift_xn_id', '').strip() or None,
                "shift_type": slot.get('shift_type', '').strip() or None,
            })
        except:
            continue

    update_data = {
        "name": data.get('name', '').strip() or None,
        "client_id": data.get('client_id') or None,
        "client_type": data.get('client_type', '').strip(),
        "location": data.get('location', '').strip(),
        "postal_code": data.get('postal_code', '').strip() or None,
        "description": data.get('description', ''),
        "is_active": data.get('is_active') is True,
        "is_premium": data.get('is_premium') in (True, 1, 'true', '1'),
        "rate": float(data['rate']) if data.get('rate') else None,
        "status": data.get('status', 'To be assigned'),
        "slots": slots if slots else None,
        # Keep top-level fields from first slot (for search/compatibility)
        "date": slots[0]["date"] if slots else None,
        "start_time": slots[0]["start_time"] if slots else None,
        "end_time": slots[0]["end_time"] if slots else None,
        "shift_xn_id": slots[0]["shift_xn_id"] if slots else None,
        "updated_at": datetime.utcnow()
    }

    try:
        db.shifts.update_one({"_id": ObjectId(shift_id)}, {"$set": update_data})
        return jsonify({"success": True, "message": f"Shift updated ({len(slots)} slots)"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/shifts/delete', methods=['POST'])
def shift_delete():
    data = request.get_json()
    shift_id = data.get('shift_id')

    if not shift_id:
        return jsonify({"success": False, "error": "Missing shift_id"}), 400

    # Log what we received (very useful for debugging)
    print(f"Delete request received for shift_id: {shift_id} (type: {type(shift_id)})")

    try:
        success = shift_model.delete(shift_id)
        success = True
        if success:
            return jsonify({"success": True, "message": "Shift and assignments deleted"})
        else:
            # Try to give better hint
            shift = shift_model.get_by_id(shift_id)
            if shift is None:
                msg = "Shift not found (possibly already deleted or invalid ID)"
            else:
                msg = "Delete failed for unknown reason (shift still exists)"
            return jsonify({"success": False, "error": msg}), 404

    except Exception as e:
        print(f"Delete exception for {shift_id}: {str(e)}")
        return jsonify({"success": False, "error": f"Server error: {str(e)}"}), 500

@bp.route('/shifts/<shift_id>/users', methods=['GET'])
def get_shift_users(shift_id):
    try:
        raw_users = shift_user_model.get_users_for_shift(shift_id)
        serialized_users = serialize_doc(raw_users)

        # ── Enrich each user with sms_sent fields from shifts_users ──
        try:
            shift_oid = ObjectId(shift_id)
            for user in serialized_users:
                uid = user.get('_id')
                if not uid:
                    continue
                assignment = db.shifts_users.find_one(
                    {
                        "shift_id": shift_oid,
                        "user_id":  ObjectId(uid)
                    },
                    {"sms_sent": 1, "sms_sent_at": 1, "_id": 0}
                )
                if assignment:
                    user['sms_sent']    = assignment.get('sms_sent', 0)
                    user['sms_sent_at'] = assignment.get('sms_sent_at')
        except Exception as enrich_err:
            print(f"[get_shift_users] SMS enrich error: {enrich_err}")
            # Non-fatal — users still returned without sms fields

        return jsonify({
            "success": True,
            "users": serialized_users
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@bp.route('/shifts/assign_user', methods=['POST'])
def assign_user_to_shift():
    data = request.get_json()
    shift_id = data.get('shift_id')
    user_id  = data.get('user_id')

    if not shift_id or not user_id:
        return jsonify({"success": False, "error": "Missing shift_id or user_id"}), 400

    success = shift_user_model.assign(shift_id, user_id)  # add assigned_by later if needed
    if success:
        return jsonify({"success": True, "message": "User assigned to shift"})
    else:
        return jsonify({"success": False, "error": "Failed to assign (possibly already assigned)"}), 400

@bp.route('/shifts/unassign_user', methods=['POST'])
def unassign_user_from_shift():
    data = request.get_json()
    shift_id = data.get('shift_id')
    user_id  = data.get('user_id')

    if not shift_id or not user_id:
        return jsonify({"success": False, "error": "Missing ids"}), 400

    if shift_user_model.unassign(shift_id, user_id):
        return jsonify({"success": True, "message": "User removed from shift"})
    else:
        return jsonify({"success": False, "error": "Assignment not found"}), 404


    
@bp.route('/users/search', methods=['GET'])
def user_search():
    q = request.args.get('q', '').strip()
    page = int(request.args.get('page', 1))
    per_page = 20

    query = {}
    if q:
        regex = {'$regex': q, '$options': 'i'}
        query = {
            '$or': [
                {'name': regex},
                {'first_name': regex},
                {'last_name': regex},
                {'email': regex},
            ]
        }

    users = list(db.users.find(query)
                 .sort('name', 1)
                 .skip((page-1)*per_page)
                 .limit(per_page))

    total = db.users.count_documents(query)
    more = (page * per_page) < total

    items = []
    for u in users:
        items.append({
            '_id': str(u['_id']),
            'name': u.get('name'),
            'first_name': u.get('first_name'),
            'last_name': u.get('last_name'),
            'email': u.get('email'),
            'designation': u.get('designation'),
        })

    return jsonify({
        'items': items,
        'more': more,
        'total': total
    })

@bp.route('/shifts/<shift_id>/copy-last-client-staff', methods=['POST'])
def copy_staff_from_last_shift(shift_id):
    try:
        data = request.get_json() or {}
        designations = data.get('designations', [])  # ← new: list of designation strings

        current_shift = shift_model.get_by_id(shift_id)
        if not current_shift:
            return jsonify({"success": False, "error": "Shift not found"}), 404

        client_id = current_shift.get('client_id')
        if not client_id:
            return jsonify({"success": False, "error": "This shift has no client assigned"}), 400

        current_date = current_shift.get('date')
        if not current_date:
            return jsonify({"success": False, "error": "Current shift has no date"}), 400

        previous_shift = db.shifts.find_one(
            {"client_id": client_id, "date": {"$lt": current_date}},
            sort=[("date", -1)],
            projection={"_id": 1}
        )

        if not previous_shift:
            return jsonify({"success": False, "error": "No previous shift found for this client"}), 200

        prev_shift_id = previous_shift['_id']
        prev_users = shift_user_model.get_users_for_shift(prev_shift_id)
        if not prev_users:
            return jsonify({"success": True, "message": "Previous shift had no assigned users", "copied": 0})

        # ── Filter by designation if provided ──
        if designations:
            desig_lower = [d.strip().lower() for d in designations]
            prev_users = [
                u for u in prev_users
                if (u.get('designation') or '').strip().lower() in desig_lower
            ]

        if not prev_users:
            return jsonify({
                "success": False,
                "error": "No staff found matching the selected designation(s) in the previous shift"
            }), 200

        copied_count = 0
        for user in prev_users:
            if shift_user_model.assign(shift_id, user['_id']):
                copied_count += 1

        return jsonify({
            "success": True,
            "message": f"Copied {copied_count} staff from previous shift"
            + (f" (filtered by: {', '.join(designations)})" if designations else ""),
            "copied": copied_count
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/shifts/<shift_id>/staff-conflicts', methods=['POST'])
def get_staff_conflicts(shift_id):
    try:
        data = request.get_json()
        user_ids = data.get('user_ids', [])

        if not user_ids:
            return jsonify({"success": True, "conflicts": {}})

        # Get current shift date
        current_shift = db.shifts.find_one(
            {"_id": ObjectId(shift_id)},
            {"date": 1}
        )
        if not current_shift or 'date' not in current_shift:
            return jsonify({"success": False, "error": "Shift date not found"}), 400

        target_date = current_shift['date']

        # Get all shift IDs on the same date (including current one)
        same_date_shift_ids = db.shifts.find(
            {"date": target_date},
            {"_id": 1}
        ).distinct("_id")

        conflicts = {}

        for uid_str in user_ids:
            uid = ObjectId(uid_str)

            # Find if user is assigned to ANY OTHER shift on the same date
            conflict_exists = db.shifts_users.find_one({
                "user_id": uid,
                "shift_id": {
                    "$in": same_date_shift_ids,
                    "$ne": ObjectId(shift_id)           # exclude current shift
                }
            })

            if conflict_exists:
                # Get info about the conflicting shift
                other_shift = db.shifts.find_one(
                    {"_id": conflict_exists["shift_id"]},
                    {"name": 1, "start_time": 1, "end_time": 1}
                )
                conflicts[uid_str] = {
                    "has_conflict": True,
                    "other_shift_name": other_shift.get("name", "another shift") if other_shift else "another shift"
                }
            else:
                conflicts[uid_str] = {"has_conflict": False}

        return jsonify({
            "success": True,
            "conflicts": conflicts
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/shifts/trigger-call', methods=['POST'])
def trigger_call_for_shift():
    try:
        data = request.get_json()
        shift_id = data.get('shift_id')

        if not shift_id:
            return jsonify({"success": False, "error": "Missing shift_id"}), 400

        # Update all assignments for this shift: set call_enabled = 1
        result = db.shifts_users.update_many(
            {"shift_id": ObjectId(shift_id)},
            {"$set": {"call_enabled": 1}}
        )

        return jsonify({
            "success": True,
            "message": "Call triggered successfully",
            "updated_count": result.modified_count
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@bp.route('/shifts/update-status', methods=['POST'])
def update_shift_status():
    try:
        data = request.get_json()
        shift_id = data.get('shift_id')
        new_status = data.get('status', '').strip()

        if not shift_id or not new_status:
            return jsonify({"success": False, "error": "Missing shift_id or status"}), 400

        valid_statuses = ['To be assigned', 'Assigned', 'Completed', 'Cancelled']
        if new_status not in valid_statuses:
            return jsonify({"success": False, "error": "Invalid status value"}), 400

        # ───────────────────────────────────────────────────────────────
        # Get current status BEFORE update (to know if we are moving AWAY from Assigned)
        # ───────────────────────────────────────────────────────────────
        current_shift = shift_model.get_by_id(shift_id)
        if not current_shift:
            return jsonify({"success": False, "error": "Shift not found"}), 404

        current_status = current_shift.get('status', 'To be assigned')

        # ───────────────────────────────────────────────────────────────
        # If we are changing AWAY FROM "Assigned" → remove all assigned staff
        # ───────────────────────────────────────────────────────────────
        if current_status == 'Assigned' and new_status != 'Assigned':
            deleted_count = shift_user_assigned_model.unassign_all(shift_id)
            print(f"[STATUS CHANGE] Removed {deleted_count} assigned staff from shift {shift_id} "
                  f"because status changed from '{current_status}' to '{new_status}'")

        # ───────────────────────────────────────────────────────────────
        # Perform the actual status update
        # ───────────────────────────────────────────────────────────────
        shift_model.update_status(shift_id, new_status)

        return jsonify({
            "success": True,
            "message": f"Status updated to '{new_status}'",
            # Optional: return how many staff were removed (useful for frontend log/debug)
            "staff_removed": deleted_count if 'deleted_count' in locals() else 0
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@bp.route('/shifts/<shift_id>/assigned-users', methods=['GET'])
def get_shift_assigned_users(shift_id):
    try:
        from booking.models.shift_user_assigned import ShiftUserAssigned
        model = ShiftUserAssigned(db.shifts_users_assigned)
        raw_users = model.get_users_for_shift(shift_id)
        serialized = serialize_doc(raw_users)
        return jsonify({"success": True, "users": serialized})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/shifts/assigned-users/assign', methods=['POST'])
def assign_user_to_shift_assigned():
    data = request.get_json()
    shift_id = data.get('shift_id')
    user_id  = data.get('user_id')

    if not shift_id or not user_id:
        return jsonify({"success": False, "error": "Missing shift_id or user_id"}), 400

    success = shift_user_assigned_model.assign(shift_id, user_id)
    if success:
        return jsonify({"success": True, "message": "User assigned"})
    else:
        return jsonify({"success": False, "error": "Already assigned or failed"}), 400


@bp.route('/shifts/assigned-users/unassign', methods=['POST'])
def unassign_user_from_shift_assigned():
    data = request.get_json()
    shift_id = data.get('shift_id')
    user_id  = data.get('user_id')

    if not shift_id or not user_id:
        return jsonify({"success": False, "error": "Missing ids"}), 400

    success = shift_user_assigned_model.unassign(shift_id, user_id)
    if success:
        return jsonify({"success": True})
    else:
        return jsonify({"success": False, "error": "Not found"}), 404

@bp.route('/shifts/assigned-users/unassign-all', methods=['POST'])
def unassign_all_assigned_staff():
    try:
        data = request.get_json()
        shift_id = data.get('shift_id')
        if not shift_id:
            return jsonify({"success": False, "error": "Missing shift_id"}), 400

        # Call your model method
        deleted_count = shift_user_assigned_model.unassign_all(shift_id)

        return jsonify({
            "success": True,
            "deleted_count": deleted_count,
            "message": f"Removed {deleted_count} assigned staff"
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@bp.route('/shifts/<shift_id>', methods=['GET'])
def get_shift(shift_id):
    try:
        shift = db.shifts.find_one({"_id": ObjectId(shift_id)})
        if not shift:
            return jsonify({"success": False, "error": "Shift not found"}), 404
        
        # Convert datetime to nice string format
        if 'slots' in shift and isinstance(shift['slots'], list):
            for slot in shift['slots']:
                date_obj = slot.get('date')
                if isinstance(date_obj, datetime):
                    slot['date'] = date_obj.strftime('%d %b %Y')   # ← 09 Mar 2026

        return jsonify({
            "success": True,
            "shift": serialize_doc(shift)
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@bp.route('/shifts/recall-call', methods=['POST'])
def recall_call_for_shift():
    try:
        data = request.get_json()
        shift_id = data.get('shift_id')
        user_ids = data.get('user_ids', [])

        if not shift_id:
            return jsonify({"success": False, "error": "Missing shift_id"}), 400
        if not user_ids:
            return jsonify({"success": False, "error": "No users selected"}), 400

        shift_oid = ObjectId(shift_id)
        user_oids = [ObjectId(uid) for uid in user_ids]

        # ── Step 1: Delete transcripts from shift_booking_conv ──
        # Match directly by shift_id + user_id (no need to look up conversation_id first)
        del_result = db.shift_booking_conv.delete_many(
            {
                "shift_id": shift_id,           # stored as string in shift_booking_conv
                "user_id": {"$in": user_ids}    # stored as string in shift_booking_conv
            }
        )
        transcript_deleted = del_result.deleted_count
        print(
            f"[RECALL] Deleted {transcript_deleted} transcript(s) "
            f"for shift_id={shift_id}, user_ids={user_ids}"
        )

        # ── Step 2: Reset call state on shifts_users ──
        update_result = db.shifts_users.update_many(
            {
                "shift_id": shift_oid,
                "user_id": {"$in": user_oids}
            },
            {
                "$set": {
                    "call_enabled": 1,
                    "call_processed": 0,
                    "availability": None,
                    "conversation_id": None,
                    "call_processed_at": None
                }
            }
        )

        return jsonify({
            "success": True,
            "message": (
                f"Recall triggered for {update_result.modified_count} "
                f"staff member(s). "
                f"{transcript_deleted} transcript(s) cleared."
            ),
            "updated_count": update_result.modified_count,
            "transcripts_deleted": transcript_deleted
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@bp.route('/shifts/send-sms', methods=['POST'])
def send_sms_to_staff():
    """
    Send a Twilio SMS to selected assigned staff.
    Expects: { shift_id, user_ids: [...], message: "..." }
    """
    import os
    from twilio.rest import Client as TwilioClient
    from twilio.base.exceptions import TwilioRestException

    try:
        data = request.get_json()
        shift_id   = data.get('shift_id')
        user_ids   = data.get('user_ids', [])
        message    = (data.get('message') or '').strip()

        # ── Validation ────────────────────────────────────────────────
        if not shift_id:
            return jsonify({"success": False, "error": "Missing shift_id"}), 400
        if not user_ids:
            return jsonify({"success": False, "error": "No users selected"}), 400
        if not message:
            return jsonify({"success": False, "error": "Message cannot be empty"}), 400
        if len(message) > 1600:
            return jsonify({"success": False, "error": "Message too long (max 1600 chars)"}), 400

        # ── Twilio credentials from environment ───────────────────────
        account_sid = os.environ.get('TWILIO_ACCOUNT_SID')
        auth_token  = os.environ.get('TWILIO_AUTH_TOKEN')
        from_number = os.environ.get('TWILIO_FROM_NUMBER')   # e.g. "+14155552671"

        if not all([account_sid, auth_token, from_number]):
            return jsonify({
                "success": False,
                "error": "Twilio credentials not configured (check TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER)"
            }), 500

        twilio_client = TwilioClient(account_sid, auth_token)

        # ── Fetch user phone numbers from DB ──────────────────────────
        user_oids = [ObjectId(uid) for uid in user_ids]
        users = list(db.users.find(
            {"_id": {"$in": user_oids}},
            {"_id": 1, "name": 1, "first_name": 1, "last_name": 1, "phone": 1, "mobile": 1}
        ))

        sent   = 0
        failed = 0
        errors = []

        for user in users:
            uid_str   = str(user['_id'])
            full_name = (
                user.get('name') or
                f"{user.get('first_name', '')} {user.get('last_name', '')}".strip() or
                uid_str
            )
            # Try 'phone' first, then 'mobile'
            to_number = (user.get('phone') or user.get('mobile') or '').strip()

            if not to_number:
                failed += 1
                errors.append({
                    "user_id": uid_str,
                    "name": full_name,
                    "error": "No phone number on file"
                })
                continue

            # Normalise: ensure E.164 format (basic check — adjust for your region)
            if not to_number.startswith('+'):
                # Default to India (+91) — change as needed
                to_number = '+91' + to_number.lstrip('0')

            try:
                msg = twilio_client.messages.create(
                    body=message,
                    from_=from_number,
                    to=to_number
                )
                sent += 1

                # Optional: log the SMS in a collection for audit
                db.sms_log.insert_one({
                   "shift_id":    shift_id,
                   "user_id":     uid_str,
                   "to_number":   to_number,
                   "message":     message,
                   "message_sid": msg.sid,   # ← ADD THIS
                   "status":      "sent",
                   "sent_at":     datetime.utcnow()
                })

                # Flag the assignment so we know SMS was sent to this user
                db.shifts_users.update_one(
                {
                 "shift_id": ObjectId(shift_id),
                 "user_id":  ObjectId(uid_str)
                },
                {
                 "$set": {
                    "sms_sent":    1,
                    "sms_sent_at": datetime.utcnow()
                }
                })

            except TwilioRestException as te:
                failed += 1
                errors.append({
                    "user_id": uid_str,
                    "name": full_name,
                    "error": str(te.msg)
                })

        return jsonify({
            "success": True,
            "sent":    sent,
            "failed":  failed,
            "errors":  errors,
            "message": f"SMS dispatched: {sent} sent, {failed} failed"
        })

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"success": False, "error": str(e)}), 500

@bp.route('/sms/reply', methods=['POST'])
def sms_reply_webhook():
    try:
        from_number    = request.form.get('From', '').strip()
        to_number      = request.form.get('To', '').strip()
        body           = request.form.get('Body', '').strip()
        message_sid    = request.form.get('MessageSid', '')
        in_response_to = request.form.get('InResponseTo', '')

        print(f"[SMS REPLY] From: {from_number} | Body: {body} | InResponseTo: {in_response_to}")

        # ── 1. Find user FIRST ─────────────────────────────────────
        user = db.users.find_one({
            "$or": [
                {"phone":  from_number},
                {"phone":  from_number.lstrip('+91')},
                {"mobile": from_number},
                {"mobile": from_number.lstrip('+91')},
            ]
        })

        # ── 2. Find original SMS (now user exists for fallback) ────
        original_sms = None
        if in_response_to:
            original_sms = db.sms_log.find_one({"message_sid": in_response_to})

        if not original_sms and user:
            # Fallback: most recent SMS sent to this number
            original_sms = db.sms_log.find_one(
                {"to_number": from_number},
                sort=[("sent_at", -1)]
            )

        # ── 3. Build reply doc ─────────────────────────────────────
        reply_doc = {
            "from_number": from_number,
            "to_number":   to_number,
            "body":        body,
            "message_sid": message_sid,
            "user_id":     str(user['_id']) if user else None,
            "user_name":   (
                user.get('name') or
                f"{user.get('first_name', '')} {user.get('last_name', '')}".strip()
            ) if user else None,
            "received_at": datetime.utcnow(),
            'in_response_to': in_response_to,
            "processed":   False,
        }

        # ── 4. Parse availability ──────────────────────────────────
        body_upper = body.upper().strip()
        availability = None

        if body_upper in ('YES', 'Y', 'AVAILABLE', 'OK', 'CONFIRM'):
            availability = 1
            reply_doc['parsed_response'] = 'available'
        elif body_upper in ('NO', 'N', 'UNAVAILABLE', 'BUSY', 'CANT', "CAN'T"):
            availability = 0
            reply_doc['parsed_response'] = 'unavailable'
        else:
            reply_doc['parsed_response'] = 'unknown'

        reply_doc['availability'] = availability

        # ── 5. Save reply ──────────────────────────────────────────
        db.sms_replies.insert_one(reply_doc)

        # ── 6. Update shift assignment if possible ─────────────────
        if user and availability is not None and original_sms:
            shift_id = original_sms.get('shift_id')
            if shift_id:
                db.shifts_users.update_one(
                    {
                        "shift_id": ObjectId(shift_id),
                        "user_id":  user['_id']
                    },
                    {
                        "$set": {
                            "availability": availability,
                            "sms_reply":    body,
                            "replied_at":   datetime.utcnow()
                        }
                    }
                )
                # Single update for both shift_id + processed flag
                db.sms_replies.update_one(
                    {"message_sid": message_sid},
                    {"$set": {"shift_id": shift_id, "processed": True}}
                )

        # ── 7. Auto-reply ──────────────────────────────────────────
        from twilio.twiml.messaging_response import MessagingResponse
        resp = MessagingResponse()
        if availability == 1:
            resp.message("Thank you! Your availability has been recorded as: Available ✅")
        elif availability == 0:
            resp.message("Thank you! Your availability has been recorded as: Unavailable ❌")
        else:
            resp.message("Thank you for your reply. Our team will review it shortly.")

        return str(resp), 200, {'Content-Type': 'text/xml'}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return '<Response></Response>', 200, {'Content-Type': 'text/xml'}


@bp.route('/sms/replies', methods=['GET'])
@admin_required
def sms_replies():
    q        = request.args.get('q', '').strip()
    response = request.args.get('response', '').strip()

    query = {}
    if q:
        regex = {'$regex': q, '$options': 'i'}
        query['$or'] = [
            {'from_number': regex},
            {'user_name': regex},
            {'body': regex},
        ]
    if response:
        query['parsed_response'] = response

    replies = list(db.sms_replies.find(
        query,
        sort=[("received_at", -1)],
        limit=200
    ))

    return render_template(
        'booking/sms_replies.html',
        replies=serialize_doc(replies)
    )
