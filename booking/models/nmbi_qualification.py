from datetime import datetime
from bson import ObjectId
import re


class NmbiQualification:
    def __init__(self, collection):
        self.collection = collection

    def get_all(self, search=None, page=1, per_page=20):
        query = {}

        if search:
            regex = {"$regex": re.escape(search.strip()), "$options": "i"}
            query["$or"] = [
                {"staff_name": regex},
                {"registration_number": regex},
                {"division": regex},
            ]

        skips = (page - 1) * per_page

        items = list(
            self.collection.find(query)
            .sort("created_at", -1)
            .skip(skips)
            .limit(per_page)
        )

        total = self.collection.count_documents(query)
        return items, total

    def create(self, data):
        now = datetime.utcnow()
        data["created_at"] = now
        data["updated_at"] = now
        result = self.collection.insert_one(data)
        return result.inserted_id

    def update(self, qualification_id, data):
        data["updated_at"] = datetime.utcnow()
        result = self.collection.update_one(
            {"_id": ObjectId(qualification_id)},
            {"$set": data}
        )
        return result.modified_count > 0

    def delete(self, qualification_id):
        result = self.collection.delete_one({"_id": ObjectId(qualification_id)})
        return result.deleted_count > 0

    def get_by_id(self, qualification_id):
        return self.collection.find_one({"_id": ObjectId(qualification_id)})

    def is_registration_unique(self, registration_number, exclude_id=None):
        query = {
            "registration_number": {
                "$regex": f"^{re.escape(registration_number.strip())}$",
                "$options": "i"
            }
        }

        if exclude_id:
            query["_id"] = {"$ne": ObjectId(exclude_id)}

        return self.collection.count_documents(query) == 0