# booking/shift.py
from datetime import datetime
from flask import render_template, request, jsonify
from bson import ObjectId

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

@bp.route('/shifts_multiple')
def shifts_multiple():
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
    for s in shifts_list:
        if isinstance(s.get('start_time'), datetime):
            s['start_time_formatted'] = s['start_time'].strftime('%H:%M')
        if isinstance(s.get('end_time'), datetime):
            s['end_time_formatted'] = s['end_time'].strftime('%H:%M')

        if not s.get('client_name'):
            s['client_name'] = '—'

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

@bp.route('/shifts_multiple/add', methods=['POST'])
def shift_multiple_add():
    data = request.get_json()

    if 'shifts' in data and isinstance(data['shifts'], list) and data['shifts']:

        common = {
            "name": data.get('name', '').strip() or None,
            "client_id": data.get('client_id') or None,
            "client_type": data.get('client_type', '').strip(),
            "location": data.get('location', '').strip(),
            "description": data.get('description', ''),
            "is_active": bool(data.get('is_active')),
            "is_premium": bool(data.get('is_premium')),
            "status": data.get('status', 'To be assigned').strip() or 'To be assigned',
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }

        inserted = []
        errors = []

        for i, slot in enumerate(data['shifts'], 1):
            doc = common.copy()
            try:
                doc["date"] = datetime.strptime(slot['date'], '%Y-%m-%d')
            except:
                errors.append(f"Row {i}: Invalid date format")
                continue

            doc["start_time"] = slot.get('start_time')
            doc["end_time"]   = slot.get('end_time')

            # ── Only use what user entered ──────────────────────────────
            shift_xn = slot.get('shift_xn_id', '').strip()
            if shift_xn:
                if not shift_model.is_shift_xn_id_unique(shift_xn):
                    errors.append(f"Row {i}: Shift ID '{shift_xn}' already exists")
                    continue
                doc["shift_xn_id"] = shift_xn
            else:
                # Important: do NOT generate anything
                doc["shift_xn_id"] = None   # or just omit the key

            try:
                result = shift_model.create(doc)
                inserted.append(str(result))
            except Exception as e:
                errors.append(f"Row {i}: {str(e)}")

        if errors:
            return jsonify({
                "success": False,
                "error": "Some shifts could not be created",
                "details": errors,
                "created_count": len(inserted)
            }), 400

        return jsonify({
            "success": True,
            "message": f"Created {len(inserted)} shift(s)",
            "ids": inserted
        })

    else:
      data = request.get_json()

    name = data.get('name', '').strip()


    shift_data = {
        "name": name if name else None,
        "date": datetime.strptime(data.get('date'), '%Y-%m-%d') if data.get('date') else None,
        "start_time": data.get('start_time'),     # "HH:MM"
        "end_time": data.get('end_time'),         # "HH:MM"
        "description": data.get('description', ''),
        "client_id": data.get('client_id') or None,   # ← NEW
        "client_type": data.get('client_type', '').strip(),  # ← NEW
        "location": data.get('location', '').strip(),  # ← NEW
        "shift_xn_id": data.get('shift_xn_id', '').strip() or None,
        "is_active": data.get('is_active') is True,
        "is_premium": data.get('is_premium') in (True, 'true', 'on', '1', 1),
        "status": data.get('status', 'To be assigned').strip() or 'To be assigned',
        "created_at": datetime.utcnow(),
        "updated_at": datetime.utcnow(),
    }

    try:
        shift_model.create(shift_data)
        return jsonify({"success": True, "message": "Shift created"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/shifts_multiple/edit', methods=['POST'])
def shifts_multiple_edit():
    data = request.get_json()
    shift_id = data.get('shift_id')
    if not shift_id:
        return jsonify({"success": False, "error": "No shift ID"}), 400

    name = data.get('name', '').strip()
   
    update_data = {
        "name": name if name else None,
        "date": datetime.strptime(data.get('date'), '%Y-%m-%d') if data.get('date') else None,
        "start_time": data.get('start_time'),
        "end_time": data.get('end_time'),
        "client_type": data.get('client_type', '').strip(),  # ← NEW
        "location": data.get('location', '').strip(),  # ← NEW
        "description": data.get('description', ''),
        "shift_xn_id": data.get('shift_xn_id', '').strip() or None,
        "client_id": data.get('client_id') or None,   # ← NEW
        "is_active": data.get('is_active') is True,
        "status": data.get('status', 'To be assigned').strip() or 'To be assigned',
        "is_premium": data.get('is_premium') in (True, 'true', 'on', '1', 1),
        "updated_at": datetime.utcnow(),
    }

    try:
        shift_model.update(shift_id, update_data)
        return jsonify({"success": True, "message": "Shift updated"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/shifts_multiple/delete', methods=['POST'])
def shifts_multiple_delete():
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

@bp.route('/shifts_multiple/<shift_id>/users', methods=['GET'])
def get_shifts_multiple_users(shift_id):
    try:
        raw_users = shift_user_model.get_users_for_shift(shift_id)
        
        # Convert everything to serializable format
        serialized_users = serialize_doc(raw_users)
        
        return jsonify({
            "success": True,
            "users": serialized_users
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

@bp.route('/shifts_multiple/assign_user', methods=['POST'])
def assign_user_to_shifts_multiple():
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

@bp.route('/shifts_multiple/unassign_user', methods=['POST'])
def unassign_user_from_shifts_multiple():
    data = request.get_json()
    shift_id = data.get('shift_id')
    user_id  = data.get('user_id')

    if not shift_id or not user_id:
        return jsonify({"success": False, "error": "Missing ids"}), 400

    if shift_user_model.unassign(shift_id, user_id):
        return jsonify({"success": True, "message": "User removed from shift"})
    else:
        return jsonify({"success": False, "error": "Assignment not found"}), 404


 
@bp.route('/shifts_multiple/<shift_id>/copy-last-client-staff', methods=['POST'])
def copy_staff_from_last_shifts_multiple(shift_id):
    try:
        current_shift = shift_model.get_by_id(shift_id)
        if not current_shift:
            return jsonify({"success": False, "error": "Shift not found"}), 404

        client_id = current_shift.get('client_id')
        if not client_id:
            return jsonify({"success": False, "error": "This shift has no client assigned"}), 400

        current_date = current_shift.get('date')
        if not current_date:
            return jsonify({"success": False, "error": "Current shift has no date"}), 400

        # Find the most recent previous shift of the same client
        previous_shift = db.shifts.find_one(
            {
                "client_id": client_id,
                "date": {"$lt": current_date},          # strictly before
                # Optional: "is_active": True
            },
            sort=[("date", -1)],                        # latest first
            projection={"_id": 1}
        )

        if not previous_shift:
            return jsonify({
                "success": False,
                "error": "No previous shift found for this client"
            }), 200

        prev_shift_id = previous_shift['_id']

        # Get users from previous shift
        prev_users = shift_user_model.get_users_for_shift(prev_shift_id)
        if not prev_users:
            return jsonify({
                "success": True,
                "message": "Previous shift had no assigned users",
                "copied": 0
            })

        copied_count = 0
        for user in prev_users:
            user_id = user['_id']
            # assign returns True if newly inserted
            if shift_user_model.assign(shift_id, user_id):
                copied_count += 1

        return jsonify({
            "success": True,
            "message": f"Copied {copied_count} users from previous shift",
            "copied": copied_count
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/shifts_multiple/<shift_id>/staff-conflicts', methods=['POST'])
def get_shifts_multiple_conflicts(shift_id):
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


@bp.route('/shifts_multiple/trigger-call', methods=['POST'])
def trigger_call_for_shifts_multiple():
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

@bp.route('/shifts_multiple/update-status', methods=['POST'])
def update_shifts_multiple_status():
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

@bp.route('/shifts_multiple/<shift_id>/assigned-users', methods=['GET'])
def get_shifts_multiple_assigned_users(shift_id):
    try:
        from booking.models.shift_user_assigned import ShiftUserAssigned
        model = ShiftUserAssigned(db.shifts_users_assigned)
        raw_users = model.get_users_for_shift(shift_id)
        serialized = serialize_doc(raw_users)
        return jsonify({"success": True, "users": serialized})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@bp.route('/shifts_multiple/assigned-users/assign', methods=['POST'])
def assign_user_to_shifts_multiple_assigned():
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


@bp.route('/shifts_multiple/assigned-users/unassign', methods=['POST'])
def unassign_user_from_shifts_multiple_assigned():
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

@bp.route('/shifts_multiple/assigned-users/unassign-all', methods=['POST'])
def unassign_all_assigned_shifts_multiple():
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
