# booking/models/doc_status.py
from bson import ObjectId
from datetime import datetime


class DocStatus:
    def __init__(self, collection):
        self.collection = collection

    # ── Read ──────────────────────────────────────────────────────────────
    def get_all(self, search: str = '', page: int = 1, per_page: int = 10):
        query = {}
        if search:
            query["$or"] = [
                {"title":       {"$regex": search, "$options": "i"}},
                {"description": {"$regex": search, "$options": "i"}},
            ]

        total  = self.collection.count_documents(query)
        cursor = (
            self.collection
            .find(query)
            .sort("created_at", -1)
            .skip((page - 1) * per_page)
            .limit(per_page)
        )
        doc_statuses = list(cursor)
        for d in doc_statuses:
            d["_id"] = str(d["_id"])
        return doc_statuses, total

    def get_by_id(self, doc_status_id: str):
        doc_status = self.collection.find_one({"_id": ObjectId(doc_status_id)})
        if doc_status:
            doc_status["_id"] = str(doc_status["_id"])
        return doc_status

    # ── Create ────────────────────────────────────────────────────────────
    def create(self, data: dict):
        data.setdefault("created_at", datetime.utcnow())
        data.setdefault("updated_at", datetime.utcnow())
        result = self.collection.insert_one(data)
        return str(result.inserted_id)

    # ── Update ────────────────────────────────────────────────────────────
    def update(self, doc_status_id: str, data: dict):
        data["updated_at"] = datetime.utcnow()
        self.collection.update_one(
            {"_id": ObjectId(doc_status_id)},
            {"$set": data}
        )

    # ── Delete ────────────────────────────────────────────────────────────
    def delete(self, doc_status_id: str):
        self.collection.delete_one({"_id": ObjectId(doc_status_id)})