# booking/models/job.py
from bson import ObjectId
from datetime import datetime


class Job:
    def __init__(self, collection):
        self.collection = collection

    # ── Read ──────────────────────────────────────────────────────────────
    def get_all(self, search: str = '', page: int = 1, per_page: int = 10):
        query = {}
        if search:
            query["$or"] = [
                {"title":       {"$regex": search, "$options": "i"}},
                {"client_name": {"$regex": search, "$options": "i"}},
                {"location":    {"$regex": search, "$options": "i"}},
            ]

        total  = self.collection.count_documents(query)
        cursor = (
            self.collection
            .find(query)
            .sort("created_at", -1)
            .skip((page - 1) * per_page)
            .limit(per_page)
        )
        jobs = list(cursor)
        for j in jobs:
            j["_id"] = str(j["_id"])
        return jobs, total

    def get_by_id(self, job_id: str):
        job = self.collection.find_one({"_id": ObjectId(job_id)})
        if job:
            job["_id"] = str(job["_id"])
        return job

    # ── Create ────────────────────────────────────────────────────────────
    def create(self, data: dict):
        data.setdefault("created_at", datetime.utcnow())
        data.setdefault("updated_at", datetime.utcnow())
        result = self.collection.insert_one(data)
        return str(result.inserted_id)

    # ── Update ────────────────────────────────────────────────────────────
    def update(self, job_id: str, data: dict):
        data["updated_at"] = datetime.utcnow()
        self.collection.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": data}
        )

    # ── Delete ────────────────────────────────────────────────────────────
    def delete(self, job_id: str):
        self.collection.delete_one({"_id": ObjectId(job_id)})