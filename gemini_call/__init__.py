# gemini_call/__init__.py

from flask import Blueprint

# Create the blueprint for xpressgp module
bp = Blueprint(
    'gemini_call',
    __name__,
    template_folder='templates',          # Looks for templates in xpressgp/templates first
    static_folder='static',
    url_prefix='/gemini_call'                # Optional: change or remove if you don't want prefix
)

# Import routes at the bottom to avoid circular imports
from . import trigger_call
from . import voice_webhook