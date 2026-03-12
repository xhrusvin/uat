# routes.py
from flask import request, jsonify, render_template, session, redirect, url_for
import json
import os
from werkzeug.utils import secure_filename
import openai
import pdfplumber
from docx import Document
from functools import wraps
import bcrypt
import re
from flask import current_app

# List of allowed designations
ALLOWED_DESIGNATIONS = [
    "Nurse", "Healthcare Assistant", "RPN", "Pharmacist", "Pharmacy Technician",
    "Multi-Task Assistant", "Cleaner", "Radiographer", "Cardiac Physiologist",
    "Chef", "Housekeeping", "Occupational Therapists", "Physiotherapist",
    "Speech And Language Therapist", "Podiatrists", "Support Worker",
    "Admin Assistant", "Test", "Social Care Worker", "Anaesthetic Rn",
    "Midwives", "Psychologists", "Kitchen Assistant"
]

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated_function

def register_routes(app):
    # Attach MongoDB to app (once)
    if not hasattr(app, 'db'):
        from pymongo import MongoClient
        client = MongoClient(os.getenv('MONGO_URI', 'mongodb://localhost:27017/xpress_health'))
        app.db = client[os.getenv('DB_NAME', 'xpress_health')]


    # ------------------------------------------------------------------
    # Helper: Load knowledge base
    # ------------------------------------------------------------------
    def load_knowledge_base():
        try:
            with open('static/knowledge_base.json', 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            return {
                "What is Xpress Health?": "Xpress Health is an all-in-one shift dashboard for managing healthcare schedules.",
                "How to register?": "To register, go to the registration page, provide your name, email, password, and optionally upload a CV.",
                "What are the supported file types for CV upload?": "Supported file types are PDF, DOC, and DOCX."
            }

    # ------------------------------------------------------------------
    # Helper: File validation
    # ------------------------------------------------------------------
    def allowed_file(filename):
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'pdf', 'doc', 'docx'}

    # ------------------------------------------------------------------
    # Helper: Extract text from CV
    # ------------------------------------------------------------------
    def extract_cv_text(file_path):
        file_extension = file_path.rsplit('.', 1)[1].lower()
        if file_extension == 'pdf':
            with pdfplumber.open(file_path) as pdf:
                return '\n'.join(page.extract_text() or '' for page in pdf.pages)
        elif file_extension in ['doc', 'docx']:
            doc = Document(file_path)
            return '\n'.join(paragraph.text for paragraph in doc.paragraphs)
        return ""

    # ------------------------------------------------------------------
    # Helper: Parse CV with OpenAI
    # ------------------------------------------------------------------
    def parse_cv_with_openai(file_path):
        try:
            cv_text = extract_cv_text(file_path)
            if not cv_text:
                raise ValueError("No text extracted from CV")

            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a CV parsing assistant. Extract the first name, last name, phone number, email, country of residence, and designation from the provided CV text. "
                            "The phone number should include a country code (e.g., +353, +44) followed by the number (e.g., 123456789). "
                            "If no phone number is found, return an empty string for 'phone'. "
                            "If no email is found, return an empty string for 'email'. "
                            "If no country is found, return an empty string for 'country'. "
                            "The country should be one of: Ireland, UK, Northern Ireland, Australia. "
                            f"The designation should be the most matching role from the following list: {', '.join(ALLOWED_DESIGNATIONS)}. "
                            "If no designation is found or no match is identified, return an empty string for 'designation'. "
                            "Return ONLY a valid JSON string with keys 'first_name', 'last_name', 'phone', 'email', 'country', and 'designation'. "
                            "Do not include any additional text, Markdown, or formatting."
                        )
                    },
                    {"role": "user", "content": cv_text}
                ],
                max_tokens=250,
                temperature=0.2
            )

            result = response.choices[0].message.content.strip()
            cleaned_result = re.sub(r'```json\s*|\s*```', '', result).strip()

            if cleaned_result.startswith('{') and cleaned_result.endswith('}'):
                try:
                    data = json.loads(cleaned_result)
                    # Normalize phone
                    if 'phone' in data and data['phone']:
                        phone = data['phone'].strip()
                        if not phone.startswith('+'):
                            phone = f"+353 {phone}"
                        data['phone'] = phone
                    else:
                        data['phone'] = ""
                    # Validate fields
                    data['email'] = data.get('email', '') if data.get('email') else ''
                    data['country'] = data.get('country', '') if data.get('country') in ['Ireland', 'UK', 'Northern Ireland', 'Australia'] else ''
                    data['designation'] = data.get('designation', '') if data.get('designation') in ALLOWED_DESIGNATIONS else ''
                    return data
                except json.JSONDecodeError:
                    pass

            # Fallback parsing
            parts = cleaned_result.split()
            data = {
                "first_name": parts[0] if parts else "",
                "last_name": parts[1] if len(parts) > 1 else "",
                "phone": next((p for p in parts if p.startswith('+')), "") or "",
                "email": next((p for p in parts if '@' in p and '.' in p), "") or "",
                "country": next((p for p in parts if p in ['Ireland', 'UK', 'Northern Ireland', 'Australia']), "") or "",
                "designation": next((p for p in parts if p in ALLOWED_DESIGNATIONS), "") or ""
            }
            if not data['phone']:
                for part in parts:
                    if any(c.isdigit() for c in part):
                        data['phone'] = f"+353 {part}"
                        break
            data['_empty'] = False
            return data

        except Exception as e:
            print(f"Error parsing CV with OpenAI: {e}")
            return {
                "first_name": "", "last_name": "",
                "phone": "", "email": "", "country": "", "designation": ""
            }

    # ------------------------------------------------------------------
    # ROUTES START HERE
    # ------------------------------------------------------------------

    @app.route('/')
    def login():
     # Check if Support Agent is enabled
     settings = current_app.db.settings.find_one({"_id": "global"})
     support_enabled = settings.get("enable_support_agent", True) if settings else True

     return render_template('login.html', support_enabled=support_enabled)

    @app.route('/register')
    def register():
        return render_template('register.html')

    @app.route('/registration_success')
    @login_required
    def registration_success():
     return render_template('registration_success.html')

    @app.route('/register/personal')
    def personal_details():
        user_id = request.args.get('user_id')
        if not user_id:
         user_id = session.get('user_id')
        if not user_id:
            return redirect(url_for('registration_success'))
        return render_template('personal_details.html', user_id=user_id)

    @app.route('/api/welcome_message', methods=['POST'])
    def welcome_message():
        return jsonify({"message": "Welcome to Xpress Health! Please log in or ask me anything."})

    @app.route('/api/ask_question', methods=['POST'])
    def ask_question():
        question = "What is Xpress Health?"  # Placeholder
        knowledge_base = load_knowledge_base()
        answer = knowledge_base.get(question, "Sorry, I don't have an answer for that.")
        return jsonify({"answer": answer})

    @app.route('/dashboard')
    @login_required
    def dashboard():
      return render_template('dashboard.html', name=session['name'])

   

    @app.route('/api/login', methods=['POST'])
    def api_login():
    #   try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid JSON"}), 400

        email = data.get('email')
        password = data.get('password')

        if not email or not password:
            return jsonify({"error": "Email and password required"}), 400

        # Find user in DB
        user = app.db.users.find_one({"email": email})
        if not user:
            return jsonify({"error": "Invalid credentials"}), 401

        # Check password
        if not bcrypt.checkpw(password.encode('utf-8'), user['password'].encode('utf-8')):
            return jsonify({"error": "Invalid credentials"}), 401

        # SET SESSION
        session['user_id'] = str(user['_id'])
        session['email'] = user['email']
        session['name'] = f"{user.get('first_name', '')} {user.get('last_name', '')}".strip() or email.split('@')[0]

        return jsonify({
            "message": "Login successful",
            "name": session['name']
        }), 200

    #   except Exception as e:
    #     print(f"Login error: {e}")
    #     return jsonify({"error": "Server error"}), 500

    @app.route('/api/upload_cv', methods=['POST'])
    def upload_cv():
        if 'cv' not in request.files:
            return jsonify({"error": "No file uploaded"}), 400

        file = request.files['cv']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            cv_data = parse_cv_with_openai(file_path)
            return jsonify({"message": "CV uploaded and parsed successfully", "cv_data": cv_data, "empty": cv_data.pop('user_name', False)})
        return jsonify({"error": "Invalid file type"}), 400

    @app.route('/test-openai', methods=['GET'])
    def test_openai():
        try:
            response = openai.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": "Say hello"}]
            )
            return jsonify({"success": True, "response": response.choices[0].message.content})
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.route('/api/voice_command', methods=['POST'])
    def voice_command():
        data = request.get_json()
        command = (data.get('audio_data') or '').lower()

        if "fill login form" in command:
            return jsonify({"action": "fill_login", "data": {"email": "user@example.com", "password": "password123"}})
        elif "fill registration form" in command:
            return jsonify({"action": "fill_register", "data": {
                "first_name": "John", "last_name": "Doe", "email": "john@example.com",
                "password": "password123", "confirm_password": "password123",
                "phone": "+353 123456789", "country": "Ireland", "designation": "Nurse"
            }})
        elif "go to registration" in command or "sign up" in command:
            return jsonify({"action": "redirect", "url": "/register"})
        elif "upload cv" in command:
            return jsonify({"action": "upload_cv"})
        else:
            knowledge_base = load_knowledge_base()
            answer = knowledge_base.get(command, "Sorry, I don't understand that command.")
            return jsonify({"answer": answer})

    @app.route('/api/registration_progress', methods=['GET', 'POST'])
    def registration_progress():
        user_id = request.args.get('user_id') or (request.json or {}).get('user_id')
        if not user_id:
            return jsonify({"error": "missing user_id"}), 400

        progress_coll = app.db['registration_progress']

        if request.method == 'GET':
            doc = progress_coll.find_one({"user_id": user_id})
            return jsonify(doc or {"current_step": None})

        data = request.get_json()
        step_id = data.get('step_id')
        progress_coll.update_one(
            {"user_id": user_id},
            {"$set": {"user_id": user_id, "current_step": step_id}},
            upsert=True
        )
        return jsonify({"saved": True})

    @app.route('/logout')
    def logout():
       session.clear()
       return redirect(url_for('login'))

    @app.route('/debug-session')
    def debug_session():
       return jsonify({
        "session_exists": bool(session),
        "session_keys": list(session.keys()),
        "session_data": dict(session),
        "cookies": request.cookies,
        "secret_key_set": bool(app.secret_key),
        "is_secure": request.is_secure,
    })

    # ------------------------------------------------------------------
# Inside register_routes(app) – add these two routes
# ------------------------------------------------------------------

    @app.route('/contact')
    def contact_page():
      return render_template('contact.html')


    @app.route('/api/submit-contact', methods=['POST'])
    def submit_contact():
      data = request.get_json() or {}
      phone = data.get('phone')
      first_name = data.get('first_name', 'Friend').strip()

      if not phone:
        return jsonify({"error": "Phone number is required"}), 400

       # Validate format: +44 7123456789
      if not re.match(r'^\+\d{1,3}\s\d{7,15}$', phone):
        return jsonify({"error": "Invalid phone format. Use: +44 7123456789"}), 400

    # --------------------------------------------------------------
    # Call the **pure** function from lead_call.py
    # --------------------------------------------------------------
      try:
        from lead_call import make_lead_call
        result = make_lead_call(phone, first_name)

        if result["success"]:
            return jsonify({
                "message": "Calling you now to discuss healthcare opportunities!",
                "call_sid": result["call_sid"]
            }), 200
        else:
            return jsonify({"error": result["error"]}), 500

      except Exception as exc:               # any unexpected import / runtime error
        print(f"Lead call error: {exc}")
        return jsonify({"error": "Failed to initiate call"}), 500

