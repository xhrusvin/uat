from flask import jsonify, request
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
import os
import requests
from datetime import datetime

from . import bp

load_dotenv()

# ==================== CONFIG ====================
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('DB_NAME')

USER_EXTERNAL_API_KEY = os.getenv('XN_PORTAL_WEBHOOK_KEY')
APP_COUNTRY = os.getenv('XN_APP_COUNTRY', 'ie')

VALIDATE_DOCUMENT_URL = "https://uat.expresshealth.ie/admin/validate_document"

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]

uploaded_documents_coll = db['uploaded_documents']
leads_collection = db['users']


# ==================== ROUTE ====================
@bp.route("/document-upload", methods=["POST", "GET"])
def document_upload_webhook():
    """
    GET: Calls the external validate_document API using xn_user_id and document_id
         passed as query parameters.

         Example:
            GET /document-upload?xn_user_id=69e8d03d7e63d8e7bf05a4e8&document_id=69e8d03d7e63d8e7bf05a4cd

    POST: Webhook endpoint triggered when a user uploads a document.

        Expected Headers:
            Api-Key: <your-api-key>
            X-App-Country: ie

        Expected JSON Body:
            {
                "user_id": "695541458810dcdf8b0d4c51",
                "document_id": "696742358815dcdf8b0g4c06"
            }
    """

    # ==================== GET ====================
    if request.method == "POST":
        try:
            xn_user_id = request.json.get("user_id")
            document_id = request.json.get("document_id")

            if not xn_user_id or not document_id:
                return jsonify({
                    "status": "error",
                    "message": "Missing required query params: xn_user_id and document_id"
                }), 400

            # Build the external URL with query params
            params = {
                "limit": 1,
                "xn_user_id": xn_user_id,
                "document_id": document_id
            }

            # Call the external validate_document endpoint (equivalent to curl GET)
            external_response = requests.get(
                VALIDATE_DOCUMENT_URL,
                params=params,
                timeout=10
            )

            # Return the external API's response back to the caller
            return jsonify({
                "status": "success",
                "external_status_code": external_response.status_code,
                "external_response": external_response.json() if external_response.headers.get("Content-Type", "").startswith("application/json") else external_response.text
            }), 200

        except requests.exceptions.Timeout:
            return jsonify({
                "status": "error",
                "message": "External API request timed out"
            }), 504

        except requests.exceptions.RequestException as e:
            return jsonify({
                "status": "error",
                "message": f"Failed to reach external API: {str(e)}"
            }), 502

        except Exception as e:
            return jsonify({
                "status": "error",
                "message": f"Server error: {str(e)}"
            }), 500

    # ==================== POST ====================
    try:
        api_key = request.headers.get("Api-Key")
        app_country = request.headers.get("X-App-Country")

        if api_key != USER_EXTERNAL_API_KEY:
            return jsonify({
                "status": "error",
                "message": "Invalid or missing Api-Key"
            }), 401

        data = request.get_json(silent=True) or {}
        

        user_id = data.get("user_id")
        document_id = data.get("document_id")

        if not user_id or not document_id:
            return jsonify({
                "status": "error",
                "message": "Missing required fields: user_id and document_id"
            }), 400

        record = {
            "user_id": str(user_id).strip(),
            "document_id": str(document_id).strip(),
            "uploaded_at": datetime.utcnow(),
            "country": app_country,          # Fixed typo: "cuntry" → "country"
            "status": "uploaded"
        }

        result = uploaded_documents_coll.insert_one(record)

        return jsonify({
            "status": "success",
            "message": "Document upload recorded successfully1",
            "uploaded_id": str(result.inserted_id),
            "user_id": user_id,
            "document_id": document_id,
            "timestamp": record["uploaded_at"].isoformat()
        }), 201

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Server error: {str(e)}"
        }), 500