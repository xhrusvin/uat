# certificate_reminder.py
"""
Certificate Reminder Call Dispatcher
─────────────────────────────────────
Separate from call_missed.py — dispatches certificate reminder calls
(PCC, Garda Vetting, Occupational Certificate) with a configurable limit per run.

Routes:
  GET  /certificate-reminder                    Process up to `limit` pending reminders
  POST /certificate-reminder/trigger/<id>       Manually trigger one reminder by _id
  GET  /certificate-reminder/status             Show counts + recent records
  POST /certificate-reminder/reset/<id>         Reset a reminder back to pending
  POST /certificate-reminder/call-outcome/<id>  Record ElevenLabs call outcome

Usage:
  GET /certificate-reminder?limit=10             → process up to 10
  GET /certificate-reminder?limit=5&dry_run=1    → preview without triggering

Batch behaviour (scheduler):
  The background scheduler fires calls in batches of 10, with a 3-minute
  gap between batches.  See certificatereminder.py for BATCH_SIZE /
  BATCH_INTERVAL_S constants.

Retry behaviour:
  If ElevenLabs returns any outcome other than "Call Success" the record is
  marked with that outcome as its status and scheduled for a retry in 1 hour.
  This repeats up to MAX_RETRIES (10) times, after which the record is marked
  "max_retries_reached" and no further calls are made.
"""

import threading
import logging
from flask import jsonify, request, current_app
from bson import ObjectId
from certificatereminder import (
    make_certificate_reminder_call  as make_ai_call,
    handle_certificate_reminder_outcome,
    MAX_RETRIES,
    SUCCESS_OUTCOME,
    RETRY_DELAY_HOURS,
    BATCH_SIZE,
    BATCH_INTERVAL_S,
)
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Default call window (UTC)
ALLOWED_START_HOUR = 0   # 00:00 UTC
ALLOWED_END_HOUR   = 24  # full day

# Default limit for the GET batch-dispatch route
DEFAULT_LIMIT = 2
MAX_LIMIT     = 20


def _col(app):
    return app.db.certificate_reminder_calls


def _is_allowed():
    now  = datetime.utcnow()
    hour = now.hour
    ok   = ALLOWED_START_HOUR <= hour < ALLOWED_END_HOUR
    return ok, now


def register_certificate_reminder_routes(app):

    # ─────────────────────────────────────────────────────────────────
    # 1. BATCH DISPATCH  GET /certificate-reminder
    #    ?limit=N        how many to process this run  (default 2)
    #    ?dry_run=1      preview only — no calls fired, no DB updates
    # ─────────────────────────────────────────────────────────────────
    @app.route('/certificate-reminder', methods=['GET'])
    def certificate_reminder():
        allowed, now = _is_allowed()

        response_base = {
            "server_time":      now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window":   f"{ALLOWED_START_HOUR}:00 – {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":     allowed,
            "batch_size":       BATCH_SIZE,
            "batch_interval_s": BATCH_INTERVAL_S,
        }

        if not allowed:
            return jsonify({
                **response_base,
                "status":  "outside_hours",
                "message": "Calls only allowed within the configured time window.",
            }), 200

        try:
            limit = int(request.args.get('limit', DEFAULT_LIMIT))
        except (ValueError, TypeError):
            limit = DEFAULT_LIMIT

        limit   = max(1, min(limit, MAX_LIMIT))
        dry_run = request.args.get('dry_run', '0').strip() in ('1', 'true', 'yes')

        col     = _col(app)
        pending = list(
            col.find({"call_status": "pending"})
               .sort("created_at", 1)
               .limit(limit)
        )

        total_pending = col.count_documents({"call_status": "pending"})

        if not pending:
            return jsonify({
                **response_base,
                "status":          "no_pending",
                "message":         "No pending certificate reminders.",
                "limit_requested": limit,
                "remaining_count": 0,
            }), 200

        if dry_run:
            preview = [
                {
                    "reminder_id":         str(r["_id"]),
                    "name":                r.get("name", ""),
                    "phone":               r.get("phone", ""),
                    "certificates_needed": r.get("certificates_needed", []),
                    "xn_user_id":          r.get("xn_user_id", ""),
                }
                for r in pending
            ]
            return jsonify({
                **response_base,
                "status":          "dry_run",
                "message":         f"Dry run — {len(preview)} records would be triggered.",
                "limit_requested": limit,
                "would_trigger":   len(preview),
                "remaining_after": total_pending - len(preview),
                "records":         preview,
            }), 200

        triggered = []
        failed    = []

        for reminder in pending:
            try:
                result = col.update_one(
                    {"_id": reminder["_id"], "call_status": "pending"},
                    {"$set": {
                        "call_status":       "triggered",
                        "last_triggered_at": datetime.utcnow(),
                        "updated_at":        datetime.utcnow(),
                    }}
                )

                if result.modified_count == 0:
                    failed.append({
                        "reminder_id": str(reminder["_id"]),
                        "reason":      "already triggered or update failed",
                    })
                    continue

                threading.Thread(
                    target=make_ai_call,
                    args=(
                        current_app._get_current_object(),
                        reminder.get("phone"),
                        reminder,
                        reminder["_id"],
                    ),
                    daemon=True,
                ).start()

                triggered.append({
                    "reminder_id":         str(reminder["_id"]),
                    "name":                reminder.get("name", ""),
                    "phone":               reminder.get("phone", ""),
                    "certificates_needed": reminder.get("certificates_needed", []),
                    "xn_user_id":          reminder.get("xn_user_id", ""),
                })

                log.info(f"[CERT REMINDER] Triggered → {reminder.get('name')} "
                         f"({reminder.get('phone')}) certs={reminder.get('certificates_needed')}")

            except Exception as e:
                failed.append({"reminder_id": str(reminder["_id"]), "reason": str(e)})
                log.error(f"[CERT REMINDER] Failed for {reminder['_id']}: {e}")

        remaining_after = col.count_documents({"call_status": "pending"})

        return jsonify({
            **response_base,
            "status":          "dispatched",
            "limit_requested": limit,
            "triggered":       len(triggered),
            "failed":          len(failed),
            "remaining_count": remaining_after,
            "details":         triggered,
            "errors":          failed,
        }), 200

    # ─────────────────────────────────────────────────────────────────
    # 2. MANUAL TRIGGER  POST /certificate-reminder/trigger/<reminder_id>
    # ─────────────────────────────────────────────────────────────────
    @app.route('/certificate-reminder/trigger/<reminder_id>', methods=['POST'])
    def certificate_reminder_trigger(reminder_id):
        allowed, now = _is_allowed()

        response_base = {
            "server_time":    now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "allowed_window": f"{ALLOWED_START_HOUR}:00 – {ALLOWED_END_HOUR}:00 UTC",
            "call_allowed":   allowed,
        }

        if not allowed:
            return jsonify({
                **response_base,
                "status":  "outside_hours",
                "message": "Manual trigger blocked outside allowed hours.",
            }), 403

        try:
            obj_id = ObjectId(reminder_id)
        except Exception:
            return jsonify({"status": "error", "message": "Invalid reminder ID"}), 400

        col      = _col(app)
        reminder = col.find_one({"_id": obj_id})

        if not reminder:
            return jsonify({"status": "error", "message": "Reminder not found"}), 404

        # Block only if currently in-flight (triggered); allow retry of
        # non-success statuses and completed ones via manual override.
        if reminder.get("call_status") == "triggered":
            return jsonify({
                **response_base,
                "status":  "already_triggered",
                "message": "This reminder is currently in-flight.",
                "name":    reminder.get("name", ""),
                "phone":   reminder.get("phone", ""),
            }), 200

        col.update_one(
            {"_id": obj_id},
            {"$set": {
                "call_status":       "triggered",
                "last_triggered_at": datetime.utcnow(),
                "updated_at":        datetime.utcnow(),
            }}
        )

        threading.Thread(
            target=make_ai_call,
            args=(
                current_app._get_current_object(),
                reminder["phone"],
                reminder,
                obj_id,
            ),
            daemon=True,
        ).start()

        return jsonify({
            **response_base,
            "status":              "triggered",
            "reminder_id":         str(obj_id),
            "name":                reminder.get("name", ""),
            "phone":               reminder.get("phone", ""),
            "xn_user_id":          reminder.get("xn_user_id", ""),
            "certificates_needed": reminder.get("certificates_needed", []),
            "retry_count":         reminder.get("retry_count", 0),
            "triggered_at":        datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
        }), 200

    # ─────────────────────────────────────────────────────────────────
    # 3. CALL OUTCOME  POST /certificate-reminder/call-outcome/<reminder_id>
    #    Called by your ElevenLabs / Telnyx webhook handler to record what
    #    happened on the call.
    #
    #    Body (JSON):  { "outcome": "Voicemail Detected" }
    #                  { "outcome": "Call Success" }
    #
    #    You can also wire this from your existing /call/completed route:
    #      from certificatereminder import handle_certificate_reminder_outcome
    #      handle_certificate_reminder_outcome(app, reminder_id, outcome)
    # ─────────────────────────────────────────────────────────────────
    @app.route('/certificate-reminder/call-outcome/<reminder_id>', methods=['POST'])
    def certificate_reminder_call_outcome(reminder_id):
        data    = request.get_json(silent=True) or {}
        outcome = str(data.get("outcome", "")).strip()

        if not outcome:
            return jsonify({"status": "error", "message": "outcome field required"}), 400

        try:
            obj_id = ObjectId(reminder_id)
        except Exception:
            return jsonify({"status": "error", "message": "Invalid reminder ID"}), 400

        handle_certificate_reminder_outcome(app, obj_id, outcome)

        col      = _col(app)
        reminder = col.find_one({"_id": obj_id}, {"call_status": 1, "retry_count": 1,
                                                   "retry_after": 1, "name": 1})
        if not reminder:
            return jsonify({"status": "error", "message": "Reminder not found"}), 404

        retry_after_str = None
        if reminder.get("retry_after"):
            retry_after_str = reminder["retry_after"].strftime("%Y-%m-%d %H:%M:%S UTC")

        return jsonify({
            "status":       "recorded",
            "outcome":      outcome,
            "call_status":  reminder.get("call_status"),
            "retry_count":  reminder.get("retry_count", 0),
            "retry_after":  retry_after_str,
            "max_retries":  MAX_RETRIES,
        }), 200

    # ─────────────────────────────────────────────────────────────────
    # 4. STATUS  GET /certificate-reminder/status
    # ─────────────────────────────────────────────────────────────────
    @app.route('/certificate-reminder/status', methods=['GET'])
    def certificate_reminder_status():
        allowed, now = _is_allowed()
        col          = _col(app)

        pending_count         = col.count_documents({"call_status": "pending"})
        triggered_count       = col.count_documents({"call_status": "triggered"})
        completed_count       = col.count_documents({"call_status": "completed"})
        failed_count          = col.count_documents({"call_status": "failed"})
        max_retries_count     = col.count_documents({"call_status": "max_retries_reached"})
        non_success_count     = col.count_documents({
            "call_status": {"$nin": ["pending", "triggered", "completed",
                                     "failed", "max_retries_reached"]},
            "retry_after":  {"$exists": True},
        })
        total = col.count_documents({})

        recent = list(
            col.find({"call_status": "triggered"})
               .sort("last_triggered_at", -1)
               .limit(10)
        )
        recent_list = [
            {
                "reminder_id":         str(r["_id"]),
                "name":                r.get("name", ""),
                "phone":               r.get("phone", ""),
                "certificates_needed": r.get("certificates_needed", []),
                "xn_user_id":          r.get("xn_user_id", ""),
                "triggered_at": (
                    r["last_triggered_at"].strftime("%Y-%m-%d %H:%M:%S UTC")
                    if isinstance(r.get("last_triggered_at"), datetime) else "—"
                ),
            }
            for r in recent
        ]

        next_pending = list(
            col.find({"call_status": "pending"})
               .sort("created_at", 1)
               .limit(5)
        )
        next_list = [
            {
                "reminder_id":         str(r["_id"]),
                "name":                r.get("name", ""),
                "phone":               r.get("phone", ""),
                "certificates_needed": r.get("certificates_needed", []),
            }
            for r in next_pending
        ]

        # Non-success records pending retry
        retry_soon = list(
            col.find({
                "call_status": {"$nin": ["pending", "triggered", "completed",
                                         "failed", "max_retries_reached"]},
                "retry_after":  {"$exists": True},
            })
            .sort("retry_after", 1)
            .limit(5)
        )
        retry_list = [
            {
                "reminder_id":  str(r["_id"]),
                "name":         r.get("name", ""),
                "call_status":  r.get("call_status"),
                "retry_count":  r.get("retry_count", 0),
                "retry_after":  (
                    r["retry_after"].strftime("%Y-%m-%d %H:%M:%S UTC")
                    if isinstance(r.get("retry_after"), datetime) else "—"
                ),
            }
            for r in retry_soon
        ]

        return jsonify({
            "server_time":        now.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "call_allowed":       allowed,
            "allowed_window":     f"{ALLOWED_START_HOUR}:00 – {ALLOWED_END_HOUR}:00 UTC",
            "batch_size":         BATCH_SIZE,
            "batch_interval_s":   BATCH_INTERVAL_S,
            "retry_delay_hours":  RETRY_DELAY_HOURS,
            "max_retries":        MAX_RETRIES,
            "total":              total,
            "pending":            pending_count,
            "triggered":          triggered_count,
            "completed":          completed_count,
            "failed":             failed_count,
            "max_retries_reached": max_retries_count,
            "awaiting_retry":     non_success_count,
            "default_limit":      DEFAULT_LIMIT,
            "max_limit":          MAX_LIMIT,
            "recent_triggered":   recent_list,
            "next_pending":       next_list,
            "retry_soon":         retry_list,
        }), 200

    # ─────────────────────────────────────────────────────────────────
    # 5. RESET  POST /certificate-reminder/reset/<reminder_id>
    # ─────────────────────────────────────────────────────────────────
    @app.route('/certificate-reminder/reset/<reminder_id>', methods=['POST'])
    def certificate_reminder_reset(reminder_id):
        try:
            obj_id = ObjectId(reminder_id)
        except Exception:
            return jsonify({"status": "error", "message": "Invalid reminder ID"}), 400

        col      = _col(app)
        reminder = col.find_one({"_id": obj_id})

        if not reminder:
            return jsonify({"status": "error", "message": "Reminder not found"}), 404

        col.update_one(
            {"_id": obj_id},
            {
                "$set":   {"call_status": "pending", "updated_at": datetime.utcnow()},
                "$unset": {"last_triggered_at": "", "retry_after": "",
                           "call_outcome": "", "retry_count": ""},
            }
        )

        return jsonify({
            "status":      "reset",
            "reminder_id": str(obj_id),
            "name":        reminder.get("name", ""),
            "message":     "Reminder fully reset to pending — retry counter cleared.",
        }), 200
