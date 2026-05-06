# admin/get_doc_users.py

from flask import render_template, request, current_app
from .views import admin_bp, admin_required


@admin_bp.route("/get_doc_users")
@admin_required
def get_doc_users():

    page = int(request.args.get("page", 1))
    per_page = 20
    search = request.args.get("search", "").strip()

    query = {
        "document_fetched": {"$ne": 1},
        "xn_user_id": {"$exists": True, "$ne": ""},
        "designation": "Nurse"
    }

    # Search
    if search:
        query["$or"] = [
            {"first_name": {"$regex": search, "$options": "i"}},
            {"last_name": {"$regex": search, "$options": "i"}},
            {"email": {"$regex": search, "$options": "i"}},
            {"phone": {"$regex": search, "$options": "i"}},
            {"xn_user_id": {"$regex": search, "$options": "i"}}
        ]

    total = current_app.db.users.count_documents(query)

    users = list(
        current_app.db.users.find(
            query,
            {
                "first_name": 1,
                "last_name": 1,
                "email": 1,
                "phone": 1,
                "designation": 1,
                "xn_user_id": 1,
                "country": 1,
                "created_at": 1,
                "document_fetched": 1
            }
        )
        .sort("_id", -1)
        .skip((page - 1) * per_page)
        .limit(per_page)
    )

    return render_template(
        "admin/get_doc_users.html",
        users=users,
        total=total,
        page=page,
        per_page=per_page,
        search=search
    )