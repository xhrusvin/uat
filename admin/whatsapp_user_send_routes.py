"""
Send WhatsApp template messages to users picked from the database.

Two ways to build a recipient list:

  1. Search `db.users`, tick the ones you want (or select every match).
  2. Upload an Excel sheet, exactly like the existing bulk page.

Both paths write into the SAME collections the bulk campaign flow uses
(`whatsapp_bulk_campaigns` / `whatsapp_bulk_messages`), so the existing
worker and the existing status endpoints keep working unchanged:

    GET /admin/whatsapp_wati/bulk/process/<campaign_id>
    GET /admin/whatsapp_wati/bulk/status/<campaign_id>
    GET /admin/whatsapp_wati/bulk/messages/<campaign_id>

Every recipient gets a `row_data` dict. Template parameters of type
"user" or "excel" are looked up in `row_data`, so one worker code path
covers both sources.
"""

from datetime import datetime
import re

import pandas as pd
from bson import ObjectId
from flask import request, jsonify, render_template

from database import db
from . import admin_bp
from admin.views import admin_required
from .whatsapp_wati import _normalise_phone


# Hard ceiling on a "select all matching" send, so a stray empty search
# can never queue the entire user table by accident.
MAX_SELECT_ALL = 5000

PHONE_COLUMN_NAMES = (
    "phone", "mobile", "mobile number", "phone number",
    "whatsapp", "whatsapp number", "contact", "contact number",
)

NAME_COLUMN_NAMES = ("name", "full name", "first name", "customer name")

# Fields copied out of a user document into row_data (usable as parameters).
USER_FIELDS = (
    "first_name", "last_name", "email", "phone", "designation",
    "company", "city", "state", "country", "xn_user_id",
)


def _users_col():
    return db.users


def _bulk_campaigns_col():
    return db.whatsapp_bulk_campaigns


def _bulk_messages_col():
    return db.whatsapp_bulk_messages


# ── Helpers ───────────────────────────────────────────────────────────────────

def _full_phone(raw, country_code=""):
    """
    Normalise to the digits-only E.164 form WATI expects ('919876543210').

    `country_code` is prepended only when the number looks local, so a
    sheet that already carries country codes is left alone.
    Returns "" when the number is unusable.
    """
    digits = _normalise_phone(str(raw or ""))
    if not digits:
        return ""

    cc = re.sub(r"\D", "", str(country_code or ""))

    if cc:
        if digits.startswith("00" + cc):
            digits = digits[2:]
        elif len(digits) <= 10:
            digits = cc + digits

    # Shortest valid international number is ~8 digits; anything less is junk.
    return digits if len(digits) >= 10 else ""


def _display_name(user):
    parts = [
        str(user.get("first_name") or "").strip(),
        str(user.get("last_name") or "").strip(),
    ]
    return " ".join(p for p in parts if p).strip()


def _user_row_data(user):
    """Flatten a user document into string values usable as template params."""
    row = {}

    for field in USER_FIELDS:
        value = user.get(field)
        row[field] = "" if value is None else str(value).strip()

    row["name"] = _display_name(user)
    row["full_name"] = row["name"]

    return row


def _build_user_query(q="", country="", city="", phone_only=True):
    """Shared by search and 'select all matching', so both see the same set."""
    conditions = []

    if q:
        pattern = re.compile(re.escape(q), re.IGNORECASE)

        or_conditions = [
            {"first_name": pattern},
            {"last_name":  pattern},
            {"email":      pattern},
            {"phone":      pattern},
            {"xn_user_id": pattern},
        ]

        # "john doe" → first_name john AND last_name doe
        parts = q.split(None, 1)
        if len(parts) == 2:
            or_conditions.append({
                "first_name": re.compile(re.escape(parts[0]), re.IGNORECASE),
                "last_name":  re.compile(re.escape(parts[1]), re.IGNORECASE),
            })

        conditions.append({"$or": or_conditions})

    if country:
        conditions.append({"country": re.compile(re.escape(country), re.IGNORECASE)})

    if city:
        conditions.append({"city": re.compile(re.escape(city), re.IGNORECASE)})

    if phone_only:
        conditions.append({"phone": {"$exists": True, "$nin": ["", None]}})

    return {"$and": conditions} if conditions else {}


def _filters_from_args(args):
    return {
        "q":       (args.get("q") or "").strip(),
        "country": (args.get("country") or "").strip(),
        "city":    (args.get("city") or "").strip(),
    }


def _create_campaign(template_name, parameter_config, source, total, extra=None):
    doc = {
        "template_name":    template_name,
        "parameter_config": parameter_config,
        "source":           source,
        "status":           "queued",
        "total":            total,
        "sent":             0,
        "failed":           0,
        "created_at":       datetime.utcnow(),
    }
    if extra:
        doc.update(extra)

    return _bulk_campaigns_col().insert_one(doc).inserted_id


def _detect_column(columns, candidates):
    for col in columns:
        if str(col).lower().strip() in candidates:
            return col
    return None


# ── Page ──────────────────────────────────────────────────────────────────────

@admin_bp.route("/whatsapp_wati/users")
@admin_required
def whatsapp_user_send():
    return render_template("admin/whatsapp_user_send.html")


# ── Search users ──────────────────────────────────────────────────────────────

@admin_bp.route("/whatsapp_wati/users/search")
@admin_required
def whatsapp_user_search():
    """
    GET /admin/whatsapp_wati/users/search
        ?q=&country=&city=&page=1&per_page=25&country_code=91

    Returns a page of matching users plus the total match count, so the UI
    can offer "select all N matching".
    """
    filters      = _filters_from_args(request.args)
    page         = max(int(request.args.get("page", 1)), 1)
    per_page     = min(max(int(request.args.get("per_page", 25)), 1), 100)
    country_code = (request.args.get("country_code") or "").strip()

    query = _build_user_query(**filters)

    try:
        total = _users_col().count_documents(query)

        items = list(
            _users_col()
            .find(query, {f: 1 for f in USER_FIELDS})
            .sort([("created_at", -1)])
            .skip((page - 1) * per_page)
            .limit(per_page)
        )

        users = []
        for u in items:
            phone = _full_phone(u.get("phone"), country_code)
            users.append({
                "id":         str(u["_id"]),
                "name":       _display_name(u) or "—",
                "email":      u.get("email") or "",
                "phone":      u.get("phone") or "",
                "send_phone": phone,
                "valid":      bool(phone),
                "city":       u.get("city") or "",
                "country":    u.get("country") or "",
            })

        return jsonify({
            "success":  True,
            "users":    users,
            "total":    total,
            "page":     page,
            "per_page": per_page,
            "pages":    max((total + per_page - 1) // per_page, 1),
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── Queue selected users ──────────────────────────────────────────────────────

@admin_bp.route("/whatsapp_wati/users/queue", methods=["POST"])
@admin_required
def whatsapp_user_queue():
    """
    POST /admin/whatsapp_wati/users/queue

    {
      "template_name": "new_chat_v1",
      "country_code":  "91",
      "mode":          "ids" | "query",
      "user_ids":      ["665f...", ...],          # mode = ids
      "filters":       {"q": "", "country": "", "city": ""},   # mode = query
      "parameter_config": [
        {"name": "name",   "type": "user",   "value": "first_name"},
        {"name": "course", "type": "static", "value": "Safety Level 2"}
      ]
    }
    """
    data = request.get_json(force=True) or {}

    template_name    = (data.get("template_name") or "").strip()
    country_code     = (data.get("country_code") or "").strip()
    mode             = (data.get("mode") or "ids").strip()
    parameter_config = data.get("parameter_config") or []

    if not template_name:
        return jsonify({"success": False, "error": "Enter a template name"}), 400

    try:
        if mode == "query":
            filters = data.get("filters") or {}
            query = _build_user_query(
                q=(filters.get("q") or "").strip(),
                country=(filters.get("country") or "").strip(),
                city=(filters.get("city") or "").strip(),
            )

            match_count = _users_col().count_documents(query)
            if match_count == 0:
                return jsonify({"success": False, "error": "No users match this search"}), 400
            if match_count > MAX_SELECT_ALL:
                return jsonify({
                    "success": False,
                    "error": (
                        f"{match_count} users match, which is over the "
                        f"{MAX_SELECT_ALL} limit. Narrow the search first."
                    )
                }), 400

            cursor = _users_col().find(query, {f: 1 for f in USER_FIELDS}).limit(MAX_SELECT_ALL)

        else:
            user_ids = data.get("user_ids") or []
            if not user_ids:
                return jsonify({"success": False, "error": "Select at least one user"}), 400

            object_ids = [ObjectId(uid) for uid in user_ids]
            cursor = _users_col().find(
                {"_id": {"$in": object_ids}},
                {f: 1 for f in USER_FIELDS}
            )

        docs = []
        skipped = []
        seen_phones = set()

        for user in cursor:
            name  = _display_name(user)
            phone = _full_phone(user.get("phone"), country_code)

            if not phone:
                skipped.append({"name": name or str(user["_id"]), "reason": "No usable phone number"})
                continue

            if phone in seen_phones:
                skipped.append({"name": name or str(user["_id"]), "reason": "Duplicate phone number"})
                continue

            seen_phones.add(phone)

            docs.append({
                "campaign_name": template_name,
                "phone":         phone,
                "name":          name,
                "user_id":       str(user["_id"]),
                "row_data":      _user_row_data(user),
                "source":        "users",
                "status":        "pending",
                "created_at":    datetime.utcnow(),
            })

        if not docs:
            return jsonify({
                "success": False,
                "error": "None of the selected users have a usable phone number",
                "skipped": skipped[:50],
            }), 400

        campaign_id = _create_campaign(
            template_name,
            parameter_config,
            source="users",
            total=len(docs),
            extra={"country_code": country_code, "skipped": len(skipped)},
        )

        for doc in docs:
            doc["campaign_id"] = campaign_id

        _bulk_messages_col().insert_many(docs)

        return jsonify({
            "success":     True,
            "campaign_id": str(campaign_id),
            "queued":      len(docs),
            "skipped":     len(skipped),
            "skipped_rows": skipped[:50],
        })

    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ── Excel path ────────────────────────────────────────────────────────────────

@admin_bp.route("/whatsapp_wati/users/excel_preview", methods=["POST"])
@admin_required
def whatsapp_user_excel_preview():
    """Reads the sheet and reports its columns, so parameters can be mapped."""
    if "file" not in request.files:
        return jsonify({"success": False, "error": "Choose an Excel file first"}), 400

    try:
        df = pd.read_excel(request.files["file"], dtype=str).fillna("")
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not read the file: {e}"}), 400

    phone_column = _detect_column(df.columns, PHONE_COLUMN_NAMES)

    if not phone_column:
        return jsonify({
            "success": False,
            "error": "No phone column found. Name one column Phone, Mobile or WhatsApp.",
            "columns": [str(c) for c in df.columns],
        }), 400

    return jsonify({
        "success":      True,
        "columns":      [str(c) for c in df.columns],
        "phone_column": str(phone_column),
        "total":        int(len(df)),
        "preview":      df.head(5).to_dict(orient="records"),
    })


@admin_bp.route("/whatsapp_wati/users/queue_excel", methods=["POST"])
@admin_required
def whatsapp_user_queue_excel():
    """
    Multipart POST: file, template_name, country_code, parameter_config (JSON).

    Every column lands in row_data, so a parameter of type "excel" can point
    at any column by its exact header.
    """
    import json

    if "file" not in request.files:
        return jsonify({"success": False, "error": "Choose an Excel file first"}), 400

    template_name = (request.form.get("template_name") or "").strip()
    country_code  = (request.form.get("country_code") or "").strip()

    if not template_name:
        return jsonify({"success": False, "error": "Enter a template name"}), 400

    try:
        parameter_config = json.loads(request.form.get("parameter_config", "[]"))
    except ValueError:
        return jsonify({"success": False, "error": "Parameter list is not valid JSON"}), 400

    try:
        df = pd.read_excel(request.files["file"], dtype=str).fillna("")
    except Exception as e:
        return jsonify({"success": False, "error": f"Could not read the file: {e}"}), 400

    phone_column = _detect_column(df.columns, PHONE_COLUMN_NAMES)
    name_column  = _detect_column(df.columns, NAME_COLUMN_NAMES)

    if not phone_column:
        return jsonify({
            "success": False,
            "error": "No phone column found. Name one column Phone, Mobile or WhatsApp."
        }), 400

    docs = []
    skipped = []
    seen_phones = set()

    for idx, row in df.iterrows():
        row_data = {
            str(col): ("" if pd.isna(row.get(col)) else str(row.get(col)).strip())
            for col in df.columns
        }

        phone = _full_phone(row_data.get(str(phone_column), ""), country_code)
        name  = row_data.get(str(name_column), "") if name_column else ""

        if not phone:
            skipped.append({"name": f"Row {idx + 2}", "reason": "Missing or invalid phone"})
            continue

        if phone in seen_phones:
            skipped.append({"name": f"Row {idx + 2}", "reason": "Duplicate phone number"})
            continue

        seen_phones.add(phone)

        # Give Excel rows the same 'name' key user rows have, so a template
        # parameter mapped to "name" works whichever source was used.
        row_data.setdefault("name", name)

        docs.append({
            "campaign_name": template_name,
            "phone":         phone,
            "name":          name,
            "row_data":      row_data,
            "source":        "excel",
            "status":        "pending",
            "created_at":    datetime.utcnow(),
        })

    if not docs:
        return jsonify({
            "success": False,
            "error": "No rows in the sheet have a usable phone number",
            "skipped_rows": skipped[:50],
        }), 400

    campaign_id = _create_campaign(
        template_name,
        parameter_config,
        source="excel",
        total=len(docs),
        extra={"country_code": country_code, "skipped": len(skipped)},
    )

    for doc in docs:
        doc["campaign_id"] = campaign_id

    _bulk_messages_col().insert_many(docs)

    return jsonify({
        "success":      True,
        "campaign_id":  str(campaign_id),
        "queued":       len(docs),
        "skipped":      len(skipped),
        "skipped_rows": skipped[:50],
    })
