# booking/__init__.py

from flask import Blueprint

# Create the blueprint for xpressgp module
bp = Blueprint(
    'booking',
    __name__,
    template_folder='templates',          # Looks for templates in xpressgp/templates first
    static_folder='static',
    url_prefix='/booking'                # Optional: change or remove if you don't want prefix
)

# Import routes at the bottom to avoid circular imports
from . import booking
from . import client 
from . import shift_users
from . import shift_multiple
from . import designation
