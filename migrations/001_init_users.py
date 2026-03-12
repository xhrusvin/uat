from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# MongoDB connection settings
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')

def init_users_collection():
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        
        # Check if users collection exists, create if not
        if 'users' not in db.list_collection_names():
            print("Creating users collection...")
            db.create_collection('users')
        
        # Define indexes
        users = db['users']
        users.create_index('email', unique=True)
        users.create_index('phone', unique=True)
        users.create_index([('country', 1), ('designation', 1)])

        print("Users collection initialized with indexes.")
        
        # Optional: Insert a sample user for testing
        sample_user = {
            "first_name": "John",
            "last_name": "Doe",
            "email": "john.doe@example.com",
            "password": "hashed_password",  # In production, use proper password hashing
            "phone": "+353 123456789",
            "country": "Ireland",
            "designation": "Nurse",
            "created_at": "2025-10-27T19:06:00Z"
        }
        
        # Check if sample user exists to avoid duplicate
        if not users.find_one({"email": sample_user["email"]}):
            users.insert_one(sample_user)
            print("Sample user inserted.")
        
        client.close()
    except Exception as e:
        print(f"Error initializing users collection: {e}")

if __name__ == '__main__':
    init_users_collection()