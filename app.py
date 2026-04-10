# app.py
from flask import Flask, session
from flask_cors import CORS
from dotenv import load_dotenv
import os
from routes import register_routes
from call_missed import register_missed_call_routes
from lead_call import register_lead_call_routes
from follow_up_call import register_follow_up_call_routes
from shift_booking_call import register_shift_booking_call_routes
from compliance_document_call import register_compliance_doc_call_routes
from professional_reference_call import register_professional_reference_call_routes
from registration import register_registration_routes, schedule_calls
from scrap_users import register_scrap_users_route
from lead_webhook import register_lead_webhook_routes
from lead_webhook_duplicate import register_lead_webhook_duplicate_routes
from fb_form_sync import register_fb_form_sync_routes
from fb_lead_fetcher import register_fb_lead_routes
from xn_portal_call.new_registration import bp as scrape_bp
from zoho_mail.oauth_callback import bp as zoho_oauth_bp
from zoho_mail.token_manager import bp as zoho_token_bp
from elevenlabs1.agent_conversations import bp as elevenlabs_bp
from elevenlabs1.agent_conversations_ivr import bp as elevenlabs_bp_ivr
from elevenlabs1.agent_conversations_xpgp import bp as elevenlabs_bp_xpgp
from elevenlabs1.agent_conversations_shift_booking import bp as elevenlabs_bp_shift_booking
from elevenlabs1.agent_conversations_intro_call import bp as elevenlabs_bp_intro_call
from elevenlabs1.agent_conversations_followup_call import bp as elevenlabs_bp_followup_call
from elevenlabs1.agent_conversations_levelfour_call import bp as elevenlabs_bp_levelfour_call
from elevenlabs1.agent_conversations_levelfive_call import bp as elevenlabs_bp_levelfive_call



from lead_registration import bp as lead_registration_bp
from document_validate import bp as document_validate_bp
from xpressgp import bp as xpressgp_bp
from gemini_call import bp as gemini_call_bp
from booking import bp as booking_bp

from webhook import webhook_bp
from flask_talisman import Talisman
from admin import admin_bp
from pymongo import MongoClient
from flask_socketio import SocketIO
import base64  # For audio handling if needed
import eventlet  # For non-blocking WS
import pytz
from datetime import datetime
from migrations import run_migrations


load_dotenv()


app = Flask(__name__)


@app.template_filter('format_datetime_12h')
def format_datetime_12h(value):
    if not value or not isinstance(value, str):
        return "—"
    try:
        # Parse your string format: "2025-12-02 14:30:15"
        dt = datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return dt.strftime('%b %d, %Y %I:%M %p').replace(' 0', ' ').lstrip('0')
    except:
        return value[:16]  # fallback

@app.template_filter('strptime')
def _jinja2_filter_strptime(date_string, fmt='%Y-%m-%d %H:%M:%S'):
    if not date_string or date_string in ['Ongoing', '—', '-', '']:
        return None
    try:
        return datetime.strptime(date_string.strip(), fmt)
    except ValueError:
        return None  #

# =========================
# 1. SESSION CONFIG (CRITICAL)
# =========================
app.secret_key = os.getenv('SECRET_KEY')  # Must be strong!
if not app.secret_key or app.secret_key == 'dev':
    raise ValueError("SECRET_KEY must be set in .env")
    
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

client = MongoClient(os.getenv('MONGODB_URI'))
app.db = client[os.getenv('DB_NAME')]  # Attach to app

def ensure_mongodb():
    """Ensure MongoDB is running"""
    try:
        app.db.client.server_info()
        print("MongoDB connected.")
    except Exception:
        print("MongoDB not running. Starting...")
        os.system("sudo systemctl start mongod")
        time.sleep(3)
        try:
            app.db.client.server_info()
            print("MongoDB started.")
        except Exception as e:
            print(f"Failed to start MongoDB: {e}")
            exit(1)

with app.app_context():
        # Run migrations automatically
        ensure_mongodb()
        run_migrations(app.db, direction='up')

app.register_blueprint(admin_bp, url_prefix='/admin')

app.config['SESSION_TYPE'] = 'filesystem'      # or 'redis' in prod
app.config['SESSION_FILE_DIR'] = '/tmp/flask_session'  # Linux/macOS
app.config['SESSION_PERMANENT'] = False
app.config['SESSION_USE_SIGNER'] = True        # Signs cookies
app.config['SESSION_COOKIE_SECURE'] = False    # HTTPS only
app.config['SESSION_COOKIE_HTTPONLY'] = True   # No JS access
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'  # CSRF protection

# Optional: Use Redis in production
# from flask_session import Session
# app.config['SESSION_TYPE'] = 'redis'
# app.config['SESSION_REDIS'] = 'redis://localhost:6379/0'
# Session(app)
UPLOAD_FOLDER = os.getenv('UPLOAD_FOLDER', 'uploads')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
os.makedirs(UPLOAD_FOLDER, exist_ok=True)  # Create folder if not exists


CORS(app, supports_credentials=True)  # Important for cookies
Talisman(app, content_security_policy=None)

# =========================
# 2. Create session dir
# =========================
os.makedirs(app.config.get('SESSION_FILE_DIR', ''), exist_ok=True)

# =========================
# 3. Register routes
# =========================

register_routes(app)
register_missed_call_routes(app)
register_lead_call_routes(app)
register_scrap_users_route(app)
register_follow_up_call_routes(app)
register_shift_booking_call_routes(app)
register_compliance_doc_call_routes(app)
register_professional_reference_call_routes(app)
app.register_blueprint(webhook_bp)
register_lead_webhook_routes(app)
register_fb_form_sync_routes(app)
register_fb_lead_routes(app)
app.register_blueprint(scrape_bp)
app.register_blueprint(zoho_oauth_bp)
app.register_blueprint(zoho_token_bp)
register_lead_webhook_duplicate_routes(app)
app.register_blueprint(elevenlabs_bp)
app.register_blueprint(elevenlabs_bp_ivr)
app.register_blueprint(elevenlabs_bp_xpgp)
app.register_blueprint(elevenlabs_bp_shift_booking)
app.register_blueprint(elevenlabs_bp_intro_call)
app.register_blueprint(elevenlabs_bp_followup_call)
app.register_blueprint(elevenlabs_bp_levelfour_call)
app.register_blueprint(elevenlabs_bp_levelfive_call)
app.register_blueprint(lead_registration_bp)
app.register_blueprint(xpressgp_bp)
app.register_blueprint(gemini_call_bp)
app.register_blueprint(booking_bp)
app.register_blueprint(document_validate_bp)



from registration import register_registration_routes
register_registration_routes(app)

with app.app_context():
    schedule_calls(app)
@app.template_filter('strftime')
def _jinja2_filter_strftime(date, fmt='%Y-%m-%d %H:%M'):
    if isinstance(date, str):
        try:
            date = datetime.fromisoformat(date.replace('Z', '+00:00'))
        except:
            return 'N/A'
    if date.tzinfo is None:
        date = pytz.UTC.localize(date)
    return date.astimezone(pytz.timezone('Europe/Dublin')).strftime(fmt)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=1000, debug=True)
