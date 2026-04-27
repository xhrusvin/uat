# admin/__init__.py
from flask import Blueprint

# Tell Flask to look for templates in templates/admin/
admin_bp = Blueprint(
    'admin',
    __name__,
    template_folder='../templates/admin'  # ← CRITICAL
)

from . import views
from . import leads
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

