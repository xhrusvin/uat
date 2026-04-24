from flask import Blueprint

bp = Blueprint(
    "document_validate",
    __name__,
    url_prefix="/document-validate"
)



from . import validate_nmbi  # noqa
from . import document_upload
from . import changed_to_staff