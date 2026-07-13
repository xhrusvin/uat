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

WEB_URL = os.getenv('WEB_URL')
VALIDATE_DOCUMENT_URL = f"{WEB_URL}/admin/validate_document"

if not all([MONGO_URI, DB_NAME]):
    raise ValueError("Required env vars missing (MONGO_URI, DB_NAME)")

mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DB_NAME]

uploaded_documents_coll = db['uploaded_documents']
leads_collection = db['users']


# ==================== ROUTE ====================
# ==================== ROUTE ====================
@bp.route("/document-upload", methods=["POST", "GET"])
def document_upload_webhook():
    """
    GET: Calls the external validate_document API using xn_user_id and document_id
         passed as query parameters.

         Example:
            GET /document-upload?xn_user_id=69e8d03d...&document_id=69e8d03d...

    POST: Webhook endpoint triggered when a user uploads a document.

        Expected Headers:
            Api-Key: <your-api-key>
            X-App-Country: ie

        Expected JSON Body:
            {
                "user_id": "...",
                "document_id": "..."
            }
    """

    # ==================== GET ====================
    if request.method == "GET":
        xn_user_id = request.args.get("xn_user_id")
        document_id = request.args.get("document_id")

        if not xn_user_id or not document_id:
            return jsonify({
                "status": "error",
                "message": "Missing required query params: xn_user_id and document_id"
            }), 400

        try:
            params = {
                "limit": 1,
                "xn_user_id": xn_user_id,
                "document_id": document_id
            }

            external_response = requests.get(
                VALIDATE_DOCUMENT_URL,
                params=params,
                timeout=10
            )

            is_json = external_response.headers.get("Content-Type", "").startswith("application/json")
            return jsonify({
                "status": "success",
                "external_status_code": external_response.status_code,
                "external_response": external_response.json() if is_json else external_response.text
            }), 200

        except requests.exceptions.Timeout:
            return jsonify({"status": "error", "message": "External API request timed out"}), 504
        except requests.exceptions.RequestException as e:
            return jsonify({"status": "error", "message": f"Failed to reach external API: {str(e)}"}), 502
        except Exception as e:
            return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500

    # ==================== POST ====================
    api_key = request.headers.get("Api-Key")
    app_country = request.headers.get("X-App-Country")

    if api_key != USER_EXTERNAL_API_KEY:
        return jsonify({
            "status": "error",
            "message": "Invalid or missing Api-Key"
        }), 401

    try:
        body = request.get_json(silent=True) or {}
        xn_user_id = body.get("user_id")
        document_id = body.get("document_id")

        if not xn_user_id or not document_id:
            return jsonify({
                "status": "error",
                "message": "Missing required fields: user_id and document_id"
            }), 400

        params = {
            "limit": 1,
            "xn_user_id": xn_user_id,
            "document_id": document_id
        }

        external_response = requests.get(
            VALIDATE_DOCUMENT_URL,
            params=params,
            timeout=10
        )

        uploaded_documents_coll.insert_one({
            "user_id": xn_user_id,
            "document_id": document_id,
            "uploaded_at": datetime.utcnow(),
            "country": app_country,
            "status": "uploaded"
        })

        is_json = external_response.headers.get("Content-Type", "").startswith("application/json")
        return jsonify({
            "status": "success",
            "external_status_code": external_response.status_code,
            "external_response": external_response.json() if is_json else external_response.text
        }), 200

    except requests.exceptions.Timeout:
        return jsonify({"status": "error", "message": "External API request timed out"}), 504
    except requests.exceptions.RequestException as e:
        return jsonify({"status": "error", "message": f"Failed to reach external API: {str(e)}"}), 502
    except Exception as e:
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500

    # ==================== NEW WEBHOOK ====================
@bp.route("/reference-added", methods=["POST"])
def reference_added_webhook():
    """
    POST: Webhook triggered when a reference is added to a document.

        Expected Headers:
            Api-Key: <your-api-key>
            X-App-Country: ie

        Expected JSON Body:
            {
                "user_id": "695541458810dcdf8b0d4c51"
            }
    """
    api_key = request.headers.get("Api-Key")
    app_country = request.headers.get("X-App-Country")

    if api_key != USER_EXTERNAL_API_KEY:
        return jsonify({
            "status": "error",
            "message": "Invalid or missing Api-Key"
        }), 401

    try:
        xn_user_id = request.json.get("user_id")

        if not xn_user_id:
            return jsonify({
                "status": "error",
                "message": "Missing required fields: user_id"
            }), 400

        db["reference_record"].insert_one({
            "user_id": xn_user_id,
            "added_at": datetime.utcnow(),
            "country": app_country,
            "status": "reference_added"
        })

        return jsonify({
            "status": "success",
            "message": "Reference recorded successfully"
        }), 200

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Server error: {str(e)}"
        }), 500

