# booking/models/client.py
from datetime import datetime
from bson import ObjectId
import re
import pandas as pd
from flask import request, jsonify
from werkzeug.utils import secure_filename
import os
from io import BytesIO

class Client:
    def __init__(self, collection):
        self.collection = collection

    def get_all(self, search=None, page=1, per_page=10):
        """
        Fetch paginated list of clients with optional name/email search
        """
        query = {}
        if search:
            escaped_search = re.escape(search.strip())
            
            query["$or"] = [
                {"name": {"$regex": escaped_search, "$options": "i"}},
                {"email": {"$regex": escaped_search, "$options": "i"}},
                {"phone": {"$regex": escaped_search, "$options": "i"}},
                {"county": {"$regex": escaped_search, "$options": "i"}}
            ]

        skips = (page - 1) * per_page
        clients = list(self.collection.find(query)
                       .sort("created_at", -1)
                       .skip(skips)
                       .limit(per_page))

        total = self.collection.count_documents(query)
        return clients, total

    def create(self, data):
        """
        Create a new client document
        """
        data["created_at"] = datetime.utcnow()
        data["updated_at"] = datetime.utcnow()
        result = self.collection.insert_one(data)
        return result.inserted_id

    def update(self, client_id, data):
        """
        Update existing client
        """
        data["updated_at"] = datetime.utcnow()
        result = self.collection.update_one(
            {"_id": ObjectId(client_id)},
            {"$set": data}
        )
        return result.modified_count > 0

    def delete(self, client_id):
        """
        Delete a client by ID
        """
        result = self.collection.delete_one({"_id": ObjectId(client_id)})
        return result.deleted_count > 0

    def get_by_id(self, client_id):
        """
        Get single client by ID
        """
        return self.collection.find_one({"_id": ObjectId(client_id)})

    # Optional: common extra helper methods
    def get_by_email(self, email):
        return self.collection.find_one({"email": email.lower().strip()})

    def get_by_phone(self, phone):
        return self.collection.find_one({"phone": phone.strip()})