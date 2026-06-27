# admin/__init__.py
from flask import Blueprint

# Tell Flask to look for templates in templates/admin/
admin_bp = Blueprint(
    'admin',
    __name__,
    template_folder='../templates/admin'  # ← CRITICAL
)

from . import views
from . import leads_copy_June2026
from . import garda_vetting
from . import website_leads
from . import ivr_calls
from . import transcriptions_followup
from . import transcriptions_levelfour
from . import transcriptions_levelfive
from . import transcriptions_onboarding
from . import user_documents
from . import staff
from . import postcall
from . import validate_document
from . import prompts
from . import trigger_ai_call
from . import location_lookup
from . import location_lookup_autoaddress
from . import get_doc_users
from . import whatsapp_wati
from . import campaigns
from . import live_staffs
from . import whatsapp_bulk_routes
from . import whatsapp_conversations
from . import cert_reminders
from . import transcriptions_reminder
from . import live_staff_stage1
from . import live_staff_sub
from . import live_staffs_crons



