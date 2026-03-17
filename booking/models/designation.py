from datetime import datetime
from bson import ObjectId
import re

class Designation:
    def __init__(self, collection):
        self.collection = collection

    def get_all(self, search=None, page=1, per_page=20):
        query = {}
        if search:
            query["name"] = {"$regex": re.escape(search.strip()), "$options": "i"}
        skips = (page - 1) * per_page
        items = list(
            self.collection.find(query)
            .sort("name", 1)
            .skip(skips)
            .limit(per_page)
        )
        total = self.collection.count_documents(query)
        return items, total

    def create(self, data):
        data["created_at"] = datetime.utcnow()
        data["updated_at"] = datetime.utcnow()
        result = self.collection.insert_one(data)
        return result.inserted_id

    def update(self, designation_id, data):
        data["updated_at"] = datetime.utcnow()
        result = self.collection.update_one(
            {"_id": ObjectId(designation_id)},
            {"$set": data}
        )
        return result.modified_count > 0

    def delete(self, designation_id):
        result = self.collection.delete_one({"_id": ObjectId(designation_id)})
        return result.deleted_count > 0

    def get_by_id(self, designation_id):
        return self.collection.find_one({"_id": ObjectId(designation_id)})

    def get_all_active(self):
        """Return all active designations sorted by name — used in dropdowns/APIs."""
        return list(
            self.collection.find({"is_active": True})
            .sort("name", 1)
        )

    def is_name_unique(self, name, exclude_id=None):
        query = {"name": {"$regex": f"^{re.escape(name.strip())}$", "$options": "i"}}
        if exclude_id:
            query["_id"] = {"$ne": ObjectId(exclude_id)}
        return self.collection.count_documents(query) == 0