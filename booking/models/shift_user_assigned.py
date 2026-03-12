from datetime import datetime
from bson import ObjectId

class ShiftUserAssigned:
    def __init__(self, collection):
        self.collection = collection

    def assign(self, shift_id, user_id):
        try:
            existing = self.collection.find_one({
                "shift_id": ObjectId(shift_id),
                "user_id": ObjectId(user_id)
            })
            if existing:
                return False  # already assigned

            self.collection.insert_one({
                "shift_id": ObjectId(shift_id),
                "user_id": ObjectId(user_id),
                "assigned_at": datetime.utcnow()
            })
            return True
        except Exception as e:
            print(f"Assign error: {e}")
            return False

    def unassign(self, shift_id, user_id):
        result = self.collection.delete_one({
            "shift_id": ObjectId(shift_id),
            "user_id": ObjectId(user_id)
        })
        return result.deleted_count > 0

    def get_users_for_shift(self, shift_id):
        from database import db
        pipeline = [
            {"$match": {"shift_id": ObjectId(shift_id)}},
            {"$lookup": {
                "from": "users",
                "localField": "user_id",
                "foreignField": "_id",
                "as": "user"
            }},
            {"$unwind": "$user"},
            {"$replaceRoot": {
                "newRoot": {
                    "$mergeObjects": ["$user", {"assigned_at": "$assigned_at"}]
                }
            }}
        ]
        return list(db.shifts_users_assigned.aggregate(pipeline))

    def unassign_all(self, shift_id):
      """Remove all assigned staff for a shift"""
      result = self.collection.delete_many({
        "shift_id": ObjectId(shift_id)
      })
      return result.deleted_count