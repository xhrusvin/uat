# booking/models/shift.py
from datetime import datetime
from bson import ObjectId

class Shift:
    def __init__(self, collection):
        self.collection = collection

    def get_all(self, search=None, page=1, per_page=10):
        query = {}
        if search:
            query["name"] = {"$regex": search, "$options": "i"}

        skips = (page - 1) * per_page
        shifts = list(self.collection.find(query)
                      .sort("created_at", -1)
                      .skip(skips)
                      .limit(per_page))

        total = self.collection.count_documents(query)
        return shifts, total

    def is_shift_xn_id_unique(self, value, exclude_id=None):
        """
        Check if shift_xn_id is already used by another shift.
        exclude_id: current shift's _id when updating (to allow keeping same value)
        """
        query = {"shift_xn_id": value}
        if exclude_id:
            query["_id"] = {"$ne": ObjectId(exclude_id)}
        
        return self.collection.count_documents(query) == 0

    def create(self, data):
        data["created_at"] = datetime.utcnow()     # ← uses datetime
        data["updated_at"] = datetime.utcnow()
        data["is_premium"] = bool(data.get("is_premium", False))
        if "rate" in data and data["rate"] is not None:
            data["rate"] = float(data["rate"])
        if "status" not in data or not data["status"]:
            data["status"] = "To be assigned"
        result = self.collection.insert_one(data)
        return result.inserted_id

    def update(self, shift_id, data):
        data["updated_at"] = datetime.utcnow()     # ← uses datetime
        if "is_active" in data:
            data["is_active"] = bool(data["is_active"])
        if "is_premium" in data:                           # ← new
            data["is_premium"] = bool(data["is_premium"])
        if "rate" in data and data["rate"] is not None:
            data["rate"] = float(data["rate"])  
        self.collection.update_one(
            {"_id": ObjectId(shift_id)},
            {"$set": data}
        )

    def delete(self, shift_id):
        try:
          # Accept string or ObjectId
          if isinstance(shift_id, str):
            oid = ObjectId(shift_id)
          else:
            oid = shift_id

          self.collection.delete_one({"_id": oid})
          
          db.shifts_users.delete_many({"shift_id": oid})
          
          return True

        except Exception as e:
            print(f"Error deleting shift {shift_id}: {e}")
            return False

    def get_by_id(self, shift_id):
        return self.collection.find_one({"_id": ObjectId(shift_id)})

    def update_status(self, shift_id, status):
        self.collection.update_one(
        {"_id": ObjectId(shift_id)},
        {"$set": {
            "status": status,
            "updated_at": datetime.utcnow()
        }}
        )
        return True