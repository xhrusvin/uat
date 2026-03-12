# xpressgp/__init__.py

from flask import Blueprint

# Create the blueprint for xpressgp module
bp = Blueprint(
    'xpressgp',
    __name__,
    template_folder='templates',          # Looks for templates in xpressgp/templates first
    static_folder='static',
    url_prefix='/xpressgp'                # Optional: change or remove if you don't want prefix
)

# Import routes at the bottom to avoid circular imports
from .customer_booking_form import *  # Imports the customer_booking route

