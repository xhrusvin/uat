from flask import Blueprint

bp = Blueprint(
    "lead_registration",
    __name__,
    url_prefix="/lead-registration"
)


from . import lead_smtp  # noqa
from . import lead_set_password  # noqa
from . import garda_vetting_email  # noqa
from . import lead_fb_smtp  # noqa
from . import missed_call_smtp  # noqa
from . import professional_reference_form  # noqa
from . import professional_reference_smtp  # noqa
from . import staff_details  # noqa
from . import practical_training_institutes_email  # noqa




