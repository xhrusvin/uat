from flask import Blueprint, jsonify, request
import threading
from datetime import datetime
import uuid

import logging

from document_worker import process_documents

bp = Blueprint("document_worker_runner", __name__)

worker_running = False

# @bp.route("/document_validate", methods=["GET"])
# def run_worker():
#     try:
#         if request.method == "GET":
#             limit = int(request.args.get("limit", 10))
#         else:
#             data = request.get_json(silent=True) or {}
#             limit = data.get("limit", 10)

#         thread = threading.Thread(target=process_documents, args=(limit,))
#         thread.start()

#         return jsonify({
#             "status": "started",
#             "limit": limit
#         }), 202

#     except Exception as e:
#         return jsonify({
#             "status": "error",
#             "message": str(e)
#         }), 500



@bp.route("/document_validate", methods=["GET"])
def run_worker():
    global worker_running

    try:
        # ── VALIDATE INPUT ─────────────────────
        try:
            limit = int(request.args.get("limit", 10))
            if limit <= 0:
                return jsonify({
                    "status": "error",
                    "message": "limit must be greater than 0"
                }), 400
        except ValueError:
            return jsonify({
                "status": "error",
                "message": "limit must be an integer"
            }), 400

        
        if worker_running:
            return jsonify({
                "status": "conflict",
                "message": "Worker already running"
            }), 409

        job_id = str(uuid.uuid4())

        
        def run():
            global worker_running
            worker_running = True
            try:
                logging.info(f"[WORKER STARTED] job_id={job_id} limit={limit}")
                process_documents(limit)
                logging.info(f"[WORKER COMPLETED] job_id={job_id}")
            except Exception as e:
                logging.error(f"[WORKER FAILED] job_id={job_id} error={str(e)}")
            finally:
                worker_running = False

        thread = threading.Thread(target=run)
        thread.start()

        # ── RESPONSE ───────────────────────────
        return jsonify({
            "status": "accepted",
            "message": "Document processing started",
            "job_id": job_id,
            "limit": limit
        }), 202

    except Exception as e:
        logging.exception("[API ERROR]")
        return jsonify({
            "status": "error",
            "message": "Internal server error"
        }), 500