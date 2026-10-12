# ═══════════════════════════════════════════════════════════════════════
# ADD THIS SECTION to the crons blueprint file (the same file that
# contains live_staff_cron_generate_pcc, live_staff_cron_sync_garda_vetting,
# etc. — e.g. admin/live_staffs_crons.py).
#
# Placement: add the wrapper near the other lazy-import wrappers at the
# top of the file (next to _build_pcc_docx, _build_ai_cv_docx, etc.),
# and add the route itself right after live_staff_cron_generate_pcc.
# ═══════════════════════════════════════════════════════════════════════


# ── Add this wrapper alongside the other lazy-import wrappers ─────────
# (near _build_pcc_docx, _build_ai_cv_docx, _build_ai_interview_docx, etc.)

def _build_screening_docx(first_shift_date=None, candidate_name='',
                           location='', candidate_id='', extracted_cv=''):
    from admin.live_staffs import _build_screening_docx as _f
    return _f(
        first_shift_date=first_shift_date,
        candidate_name=candidate_name,
        location=location,
        candidate_id=candidate_id,
        extracted_cv=extracted_cv,
    )


def _screening_col():
    return db.live_staff_screening


# ── Cron: Generate Screening Interview Record one staff at a time ─────

@admin_bp.route('/live-staffs/cron/generate-screening', methods=['GET', 'POST'])
def live_staff_cron_generate_screening():
    """
    Cron job — generates Screening Interview Record for ONE staff member per call.

    Finds staff where screening_generated is not True.
    Generates DOCX (template + dates from work_start_date + Gemini-extracted
    education/employment from extracted_cv), uploads to GCS, saves to
    live_staff_screening collection.

    To regenerate ALL Screening Interview Records, run in MongoDB first:
        db.live_staffs.updateMany({}, {$set: {screening_generated: false}})
        db.live_staff_screening.deleteMany({})

    Protect with ?cron_key=<CRON_SECRET> env var.
    """
    cron_secret = os.environ.get('CRON_SECRET', '')
    if cron_secret:
        provided = (request.args.get('cron_key') or
                    request.headers.get('X-Cron-Key', ''))
        if provided != cron_secret:
            return jsonify({"success": False, "error": "Unauthorised"}), 401

    col           = _staffs_col()
    screening_col = _screening_col()

    pending_query = {
        "$or": [
            {"screening_generated": {"$exists": False}},
            {"screening_generated": False},
            {"screening_generated": None},
        ]
    }
    remaining_total = col.count_documents(pending_query)
    staff           = col.find_one(pending_query)

    if not staff:
        return jsonify({
            "success":         True,
            "message":         "All staff Screening Interview Records already generated.",
            "remaining_count": 0,
        })

    staff_id = str(staff['_id'])

    # Same multi-schema field lookup used by the on-demand route —
    # live_staffs documents exist in more than one shape (old nested
    # section_1_personal_details.full_name vs new flat 'Name',
    # 'Employee Code' with a space/capitals vs lowercase employee_code).
    def _first(*keys, default=''):
        for k in keys:
            val = staff.get(k)
            if val not in (None, ''):
                return _v(val)
        return default

    s1           = staff.get('section_1_personal_details') or {}
    full_name    = _first('Name') or _v(s1.get('full_name') or '') or 'staff'
    emp_code     = _first('Employee Code', 'employee_code')
    county       = _first('County', 'county', 'Location', 'location')
    email        = _v(staff.get('email') or s1.get('email_address') or '')
    extracted_cv = _v(staff.get('extracted_cv') or '')

    def _mark_done(fields):
        fields["screening_generated"]    = True
        fields["screening_generated_at"] = datetime.utcnow()
        col.update_one({"_id": staff['_id']}, {"$set": fields})

    def _parse_any_date(value):
        """Accepts a native datetime (pymongo BSON date) or a string in
        any of several common formats. Returns a datetime, or None."""
        if value in (None, ''):
            return None
        if isinstance(value, datetime):
            return value
        s = str(value).strip()
        for fmt in (
            '%d-%m-%Y', '%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d',
            '%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S',
        ):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        return None

    try:
        first_shift_date = _parse_any_date(staff.get('work_start_date'))

        docx_bytes = _build_screening_docx(
            first_shift_date=first_shift_date,
            candidate_name=full_name,
            location=county,
            candidate_id=emp_code,
            extracted_cv=extracted_cv,
        )

        safe_name = full_name.replace(' ', '_').replace('/', '_')
        filename  = f"Screening_{safe_name}.docx"
        gcs_blob  = f"screening/{filename}"
        _gcs_upload(gcs_blob, docx_bytes,
                    content_type='application/vnd.openxmlformats-officedocument.wordprocessingml.document')

        existing = screening_col.find_one({"staff_id": staff_id})
        rec = {
            "staff_id":         staff_id,
            "staff_name":       full_name,
            "employee_code":    emp_code,
            "filename":         filename,
            "gcs_blob":         gcs_blob,
            "first_shift_date": staff.get('work_start_date'),
            "generated_at":     datetime.utcnow(),
        }
        if existing:
            screening_col.update_one({"_id": existing["_id"]}, {"$set": rec})
            rec_id = str(existing["_id"])
        else:
            rec_id = str(screening_col.insert_one(rec).inserted_id)

        download_url = _gcs_signed_url(gcs_blob) or ''
        _mark_done({
            "screening_gcs_blob":     gcs_blob,
            "screening_filename":     filename,
            "screening_download_url": download_url,
        })

        # ── Push to outreach API (optional) ──────────────────────────
        # NOTE: HSE_DOC_TYPES in admin/live_staffs.py currently has no
        # mapping for 'screening' — it will fall back to using the key
        # literally ('screening') as the hse_document_type value sent
        # to the external API. Add a 'screening': '<correct_value>'
        # entry to HSE_DOC_TYPES once you know what the outreach API
        # expects, or remove this block if the Screening Interview
        # Record should not be pushed there at all.
        try:
            _push_hse_document_background(
                staff_id_str=staff_id,
                doc_type_key='screening',
                docx_bytes=docx_bytes,
                staff_name=full_name,
                mongo_id=staff_id,
                email=email,
            )
        except Exception:
            pass

        return jsonify({
            "success":         True,
            "staff_name":      full_name,
            "email":           email,
            "screening_id":    rec_id,
            "filename":        filename,
            "gcs_blob":        gcs_blob,
            "download_url":    download_url,
            "remaining_count": max(0, remaining_total - 1),
            "message": (
                f"Screening Interview Record generated for {full_name} — "
                f"{max(0, remaining_total - 1)} remaining."
            ),
        })

    except Exception as e:
        _mark_done({"screening_note": f"error: {e}"})
        return jsonify({
            "success":         False,
            "email":           email,
            "staff_name":      full_name,
            "error":           str(e),
            "remaining_count": max(0, remaining_total - 1),
        })
