# admin.py
from flask import (
    redirect, url_for, flash, current_app, jsonify,
    request, session, render_template, Response
)
from functools import wraps
from . import admin_bp
from datetime import datetime, timedelta
import bcrypt
from pytz import utc
import pytz
from bson import ObjectId
from bson.binary import Binary
import os
import asyncio
import aiohttp
import threading
import re
from pymongo.errors import OperationFailure
import requests
import pandas as pd
from io import BytesIO


now_utc = datetime.now(pytz.UTC)


XN_PORTAL_BASE_URL = os.getenv('XN_PORTAL_BASE_URL')
NEXT_FOLLOW_UP_MINUTES = 2
NEXT_FOLLOW_UP_HOURS = 24
WEB_URL = os.getenv('WEB_URL')


current_time = datetime.now(pytz.UTC).strftime("%Y-%m-%d %H:%M")

def to_str(value):
    """
    Convert any value to string.
    - None → "null" or "" (your choice)
    - int/float → str without scientific notation
    - bool → "true"/"false" or "True"/"False"
    - Already string → return as-is
    """
    if value is None:
        return ""  # or return "null" if you prefer
    return str(value).strip()


@admin_bp.route('/api/brief-summary-conv-new')
def api_brief_summary_cov_new():
    conv_id = request.args.get('conv_id')
    collection = request.args.get('collection')
    if not conv_id:
        return jsonify({"error": "Missing conv_id parameter"}), 400
    
    try:
        # Step 1: Find the user by last_elevenlabs_conversation_id
        user = current_app.db.users.find_one({
            "last_elevenlabs_conversation_id": conv_id
        })

        if not user:
            return jsonify({
                "message": "No user found with this conversation ID",
                "collected_data": {}
            }), 404

        xn_user_id = user.get("xn_user_id")
        if not xn_user_id:
            return jsonify({
                "message": "User found but missing xn_user_id",
                "collected_data": {}
            }), 404

        first_name = user.get("first_name", "").strip()
        last_name = user.get("last_name", "").strip()
        caller_name = " ".join(filter(None, [first_name, last_name])) or "Unknown Caller"
        phone = user.get("phone")

        # --- FIX: Initialize conversation as None upfront ---
        conversation = None

        # Step 2: Determine which collection to query
        if collection in (None, "1", 1):  # Accept None, "1", or 1
            conversation = current_app.db.conversations.find_one({
                "elevenlabs_conversation_id": conv_id,
                "ended_at": {"$ne": "Ongoing", "$exists": True, "$ne": None}
            })
        elif collection in ("2", 2):
            conversation = current_app.db.follow_up_conv.find_one({
                "elevenlabs_conversation_id": conv_id,
                "ended_at": {"$ne": "Ongoing", "$exists": True, "$ne": None}
            })

        # --- Critical: If conversation not found or not ended, return early ---
        if not conversation:
            return jsonify({
                "message": "Conversation not found or not completed",
                "collected_data": {}
            }), 404

        # === NEW BLOCK START ===
        email_message = ""
        if collection in (None, "1", 1):
            garda_url = f"{WEB_URL}/lead-registration/garda-vetting-email?id={user['_id']}"
            try:
                import requests
                garda_resp = requests.get(garda_url, timeout=100)
                if garda_resp.status_code in (200, 201):
                    current_app.logger.info(f"Garda vetting email request successful for user {user['_id']}")
                    email_message = garda_resp.text
                else:
                    current_app.logger.warning(
                        f"Garda vetting email request failed {garda_resp.status_code} for user {user['_id']}: {garda_resp.text}"
                    )
                    email_message = garda_resp.text
            except Exception as garda_err:
                current_app.logger.error(f"Garda vetting email request exception for user {user['_id']}: {garda_err}")
                email_message = str(garda_err)
        # === NEW BLOCK END ===


        

        el_id = conv_id

        # Fetch from ElevenLabs
        api_key = os.getenv("ELEVENLABS_API_KEY")
        if not api_key:
            return jsonify({"error": "Missing ELEVENLABS_API_KEY"}), 500

        import requests
        url = f"https://api.elevenlabs.io/v1/convai/conversations/{el_id}"
        resp = requests.get(url, headers={"xi-api-key": api_key}, timeout=25)



        if resp.status_code != 200:
            current_app.logger.warning(f"ElevenLabs {resp.status_code} for {el_id}: {resp.text}")
            return jsonify({
                "message": "Analysis not ready yet or error fetching from ElevenLabs",
                "elevenlabs_id": el_id,
                "collected_data": {}
            }), 202

        data = resp.json()
        analysis = data.get("analysis") or {}
        dcr = analysis.get("data_collection_results") or {}


         #fetch current details
        if xn_user_id:
            details_url = f"{XN_PORTAL_BASE_URL}ai/recruitments/detail"
            details_payload = {
                "_id": str(xn_user_id)
            }

            details_headers = {
                "Api-Key": os.getenv("XN_PORTAL_API_KEY"),         # Set this in env
                "X-App-Country": os.getenv("XN_APP_COUNTRY"),       # Set this in env
                "Content-Type": "application/json"
            }

            details_resp = requests.get(details_url, json=details_payload, headers=details_headers, timeout=25)

            details_resp.raise_for_status()   

            try:
              api_data = details_resp.json()
            except ValueError:
                # Response was not JSON → handle gracefully
                return jsonify({
                    "success": False,
                    "message": "Invalid JSON response from server",
                    "xn_user_id": xn_user_id,
                    "collected_data": {},
                    "errors": ["Response is not valid JSON"]
                }), 500   
            user_data = api_data.get("data")

            if user_data is not None:
                # Store to variable (you can use it later if needed)
                collected_user_info = user_data       # ← here is your variable

                response_body = {
                 "success": True,
                 "message": api_data.get("message", "Recruitment detail fetched"),
                 "data": collected_user_info,      # ← full user object here (most useful)
                 "xn_user_id": xn_user_id,
                 "collected_data": {},             # or put collected_user_info here if needed
                 "status_code": api_data.get("status_code", details_resp.status_code),
                 "errors": []
                  }
                status_code = 200
            else:
                # "data" was missing or null
                response_body = {
                    "success": False,
                    "message": "User data not found in response",
                    "xn_user_id": xn_user_id,
                    "collected_data": {},
                    "errors": ["'data' field is missing or null"]
                }
                status_code = 404   # or 200 — depends on your API contract
                return jsonify(response_body), status_code

            

        # Get current user data to check what's already present
        current_user_data = current_app.db.users.find_one({"_id": user["_id"]})
        
        # Build collected data
        collected_data = {}
        schedule_call_value = ""
        full_address = ""
        dob = ""
        eir_code = ""
        years_experience_ireland = ""
        location_in_ireland = ""
        company_name = ""
        company_phone = ""
        job_title = ""
        travel_mode = ""
        last_company_experience_year = ""
        experience_month = ""
        masters = ""
        visa_type = ""
        previous_work_county = ""
        pps_number = ""
        uniform_size = ""
        tuberculosis_vaccine = ""
        hepatitis_antibody = ""
        mmr_vaccine = ""
        covid_19_vaccine = ""
        gender = ""
        right_to_work_ireland = ""
        location_in_ireland_name=""


        for field_id, item in dcr.items():
          raw_value = item.get("value")
          value = to_str(raw_value)  # Convert EVERYTHING to clean string early

          collected_data[field_id] = value # Store as string or None

          if field_id == "schedule_call":
            schedule_call_value = value

          if field_id == "gender":
            gender = value

          if field_id == "full_address" and value:
            full_address = value

          if field_id == "dob" and value:
            dob = value

          if field_id == "eir_code" and value:
            eir_code = value

          if field_id == "years_experience_ireland" and value:
            years_experience_ireland = int(float(value))
          
          if field_id == "county" and value:
            location_in_ireland = value

            return location_in_ireland
            try:
                 county_doc = current_app.db.county.find_one({
                   "_id": ObjectId(value)
                  })

                 if county_doc:
                   location_in_ireland_name = county_doc.get("name", "")
                 else:
                   location_in_ireland_name = ""

            except Exception:
                location_in_ireland_name = ""

          if location_in_ireland_name:
           # === PRACTICAL TRAINING INSTITUTES EMAIL BLOCK START ===
           email_message = ""
           if collection in (None, "1", 1):
            garda_url = f"{WEB_URL}/lead-registration/practical-training-institutes-email?id={user['_id']}&county={location_in_ireland_name}"
            try:
                import requests
                garda_resp = requests.get(garda_url, timeout=100)
                if garda_resp.status_code in (200, 201):
                    current_app.logger.info(f"Practical training institutes email request successful for user {user['_id']}")
                    email_message = garda_resp.text
                else:
                    current_app.logger.warning(
                        f"Practical training institutes email request failed {garda_resp.status_code} for user {user['_id']}: {garda_resp.text}"
                    )
                    email_message = garda_resp.text
            except Exception as garda_err:
                current_app.logger.error(f"Garda vetting email request exception for user {user['_id']}: {garda_err}")
                email_message = str(garda_err)
           # === PRACTICAL TRAINING INSTITUTES EMAIL BLOCK END ===

          if field_id == "last_employer_name" and value:
            company_name = value 

          if field_id == "employer_phone_number" and value:
            company_phone = value 

          if field_id == "last_job_title" and value:
            job_title = value

          if field_id == "commute_plan" and value:
            travel_mode = value  

          if field_id == "employment_duration_years" and value:
            last_company_experience_year = value

          if field_id == "masters" and value:
            masters = value

          if field_id == "visa_type" and value:
            visa_type = value

          if field_id == "previous_work_county" and value:
            previous_work_county = value

          if field_id == "pps_number" and value:
            pps_number = value

          if field_id == "uniform_size" and value:
            uniform_size = value

          if field_id == "tuberculosis_vaccine" and value:
            tuberculosis_vaccine = value

          if field_id == "hepatitis_b_antibodies" and value:
            hepatitis_antibody = value

          if field_id == "mmr_varicella_vaccination" and value:
            mmr_vaccine = value

          if field_id == "covid_vaccination" and value:
            covid_19_vaccine = value

          if field_id == "right_to_work_ireland" and value:
            right_to_work_ireland = value

          if field_id == "employment_duration_months" and value:
            try:
             # Convert to float first (handles "2.0", "6.5", etc.)
             num = float(str(value).strip())
             # Convert to int (drops decimal part)
             experience_month = int(num)
             # Optional: prevent negative numbers
             if experience_month < 0:
                experience_month = 0
            except (ValueError, TypeError):
             experience_month = 0

        now_utc = datetime.now(pytz.UTC)
        next_follow_up_at = now_utc + timedelta(hours=24)
        next_compliance_document_at = now_utc + timedelta(hours=56)
        next_professional_reference_at = now_utc + timedelta(hours=240)
        

        update_fields = {
          "next_follow_up_at": next_follow_up_at,
          "next_compliance_document_at": next_compliance_document_at,
          "next_professional_reference_at": next_professional_reference_at,
          "follow_up_sent": 1 if collection in ("2", 2) else 0,
          "updated_at": now_utc
        }

    

        update_result = current_app.db.users.update_one(
        {"_id": user["_id"]},
        {"$set": update_fields}
        )

        if update_result.matched_count == 0:
           current_app.logger.warning("No user found for follow-up update")

        if update_result.modified_count > 0:
           current_app.logger.info(f"User data updated with selective fields: ")

        # Final result
        result = {
            "elevenlabs_conversation_id": el_id,
            "internal_conv_id": str(conversation["_id"]),
            "xn_user_id": xn_user_id,
            "phone": phone,
            "caller_name": caller_name,
            "ended_at": conversation.get("ended_at"),
            "summary_title": analysis.get("call_summary_title"),
            "collected_data": collected_data,
            "schedule_call": schedule_call_value,
            "total_fields": len(collected_data),
            "from_cache": False,
            "address_update_status": "not_triggered",
            "email_status": email_message,
            "practical_training_institutes_email_status": email_message
        }

        # === UPDATE EXTERNAL API IF ADDRESS FOUND ===
        if xn_user_id and user_data is not None:
            update_url = f"{XN_PORTAL_BASE_URL}ai/recruitments/update"
    
            update_payload = {
                "_id": str(xn_user_id),
            }
    
             # Helper: only add field if we have a real value AND current value is missing/empty
            def should_update(current_val, new_val):
             if not new_val:  # new_val is "", None, False, 0, etc.
               return False
             # Treat various "empty" representations as missing
             if current_val in (None, "", "null", "None", [], {}):
               return True
             return False
    
           # ────────────────────────────────────────────────
           # Add fields conditionally
           # ────────────────────────────────────────────────
            # ────────────────────────────────────────────────
            # Update ONLY if the field is currently None in user_data
            # ────────────────────────────────────────────────
            if user_data.get("gender") is None:
                update_payload["gender"] = gender
    
            if user_data.get("dob") is None:
                update_payload["dob"] = dob
    
            if user_data.get("county_id") is None:
                update_payload["county_id"] = location_in_ireland
    
            if user_data.get("eir_code") is None:
                update_payload["eir_code"] = eir_code
    
            if user_data.get("address") is None:
                update_payload["address"] = full_address
    
            if user_data.get("experience_year") is None:
                update_payload["experience_year"] = years_experience_ireland

            if user_data.get("experience_month") is None:
                update_payload["experience_month"] = experience_month
    
            if user_data.get("masters") is None:
                update_payload["masters"] = masters
    
            if user_data.get("travel_mode") is None:
                update_payload["travel_mode"] = travel_mode
    
            if user_data.get("company_name") is None:
                update_payload["company_name"] = company_name
    
            if user_data.get("job_title") is None:
                update_payload["job_title"] = job_title
    
            if user_data.get("company_phone") is None:
                update_payload["company_phone"] = company_phone
    
            if user_data.get("last_company_experience_year") is None:
                update_payload["last_company_experience_year"] = last_company_experience_year
    
            if user_data.get("company_county_id") is None:
                update_payload["company_county_id"] = previous_work_county
    
            if user_data.get("pps_number") is None:
                update_payload["pps_number"] = pps_number
    
            if user_data.get("visa_type_id") is None:
                update_payload["visa_type_id"] = visa_type
    
            if user_data.get("uniform_size") is None:
                update_payload["uniform_size"] = uniform_size
    
            if user_data.get("tuberculosis_vaccine") is None:
                update_payload["tuberculosis_vaccine"] = tuberculosis_vaccine
    
            if user_data.get("hepatitis_antibody") is None:
                update_payload["hepatitis_antibody"] = hepatitis_antibody
    
            if user_data.get("mmr_vaccine") is None:
                update_payload["mmr_vaccine"] = mmr_vaccine
    
            if user_data.get("covid_19_vaccine") is None:
                update_payload["covid_19_vaccine"] = covid_19_vaccine

            if user_data.get("permission_to_work") is None:
                update_payload["permission_to_work"] = right_to_work_ireland
             # ────────────────────────────────────────────────
             # Only send request if we actually have something to update
             # (besides _id)
             # ────────────────────────────────────────────────
            
            update_headers = {
                     "Api-Key": os.getenv("XN_PORTAL_API_KEY"),
                     "X-App-Country": os.getenv("XN_APP_COUNTRY"),
                     "Content-Type": "application/json"
                 }

        try:
           
            update_resp = requests.get(           # ← most update endpoints use POST, not GET
                update_url,
                json=update_payload,
                headers=update_headers,
                timeout=10
                )
            
            if update_resp.status_code in (200, 201, 204):
                current_app.logger.info(
                    f"Selective update successful for xn_user_id {xn_user_id} "
                    f"({len(update_payload)-1} fields)"
                )
                result["address_update_status"] = {
                    "status": "success",
                    "updated_fields": list(update_payload.keys())[1:],  # exclude _id
                    "response": update_resp.json() if update_resp.text else {}
                }
            else:
                current_app.logger.warning(
                    f"Update failed {update_resp.status_code} for {xn_user_id}: {update_resp.text}"
                )
                result["address_update_status"] = {
                    "status": "failed",
                    "code": update_resp.status_code,
                    "message": update_resp.text[:200]
                }

        except Exception as exc:
            current_app.logger.error(f"Update request exception: {exc}", exc_info=True)
            result["address_update_status"] = {
                "status": "request_error",
                "error": str(exc)
            }
        else:
            result["address_update_status"] = update_resp.json() if update_resp.text else {}

        if not xn_user_id or not user_data:
            result["address_update_status"] = "skipped_missing_xn_user_id_or_user_data"

        current_app.logger.info(f"No address found in collected_data for conv {el_id}")

        current_app.logger.info(f"Brief summary fetched for EL conv {el_id} (xn_user_id: {xn_user_id}) | Schedule call: {schedule_call_value}")

        return jsonify(result), 200

    except Exception as e:
        current_app.logger.error(f"api_brief_summary_cov CRASH: {e}", exc_info=True)
        return jsonify({
            "error": "Server error",
            "details": str(e),
            "collected_data": {}
        }), 500
