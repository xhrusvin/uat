# carelearning/__init__.py

from flask import Blueprint

# Create the blueprint for xpressgp module
bp = Blueprint(
    'carelearning',
    __name__,
    template_folder='templates',          # Looks for templates in xpressgp/templates first
    static_folder='static',
    url_prefix='/carelearning'                # Optional: change or remove if you don't want prefix
)

# Import routes at the bottom to avoid circular imports
from . import chatgpt
from . import jobs_mcp
