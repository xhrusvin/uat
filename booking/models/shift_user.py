from datetime import datetime
from bson import ObjectId

class ShiftUser:
    def __init__(self, collection):
        self.collection = collection

    def assign(self, shift_id, user_id, assigned_by=None):
        """Assign user to shift. Returns True if successful (wasn't already assigned)"""
        data = {
            "shift_id": ObjectId(shift_id),
            "user_id": ObjectId(user_id),
            "assigned_at": datetime.utcnow(),
        }
        if assigned_by:
            data["assigned_by"] = ObjectId(assigned_by)

        try:
            result = self.collection.update_one(
                {"shift_id": data["shift_id"], "user_id": data["user_id"]},
                {"$setOnInsert": data},
                upsert=True
            )
            return result.upserted_id is not None or result.modified_count > 0
        except Exception:
            return False

    def unassign(self, shift_id, user_id):
        return self.collection.delete_one({
            "shift_id": ObjectId(shift_id),
            "user_id": ObjectId(user_id)
        }).deleted_count > 0

    def get_users_for_shift(self, shift_id):
        pipeline = [
            {"$match": {"shift_id": ObjectId(shift_id)}},
            {"$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user"
            }},
            {"$unwind": "$user"},
            {"$project": {
                "_id": "$user._id",
                "name": "$user.name",
                "first_name": "$user.first_name",
                "last_name": "$user.last_name",
                "email": "$user.email",
                "phone": "$user.phone",
                "designation": "$user.designation",
                "assigned_at": "$assigned_at",
                "call_enabled": "$call_enabled",
                "call_processed": "$call_processed",
                "availability": "$availability"
            }},
            {"$sort": {"name": 1}}
        ]
        return list(self.collection.aggregate(pipeline))

    def get_shifts_for_user(self, user_id): 
        # similar pipeline – can implement later if needed
        pass

    