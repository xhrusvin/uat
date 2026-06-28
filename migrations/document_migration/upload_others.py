from pathlib import Path
import mimetypes
import os

import requests
from dotenv import load_dotenv

# ----------------------------------------------------
# Load .env
# ----------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")

API_KEY = os.getenv("XN_PORTAL_API_KEY")
COUNTRY = os.getenv("XN_APP_COUNTRY", "ie")

UPLOAD_URL = "https://admin.xpresshealthapp.com/api/admin/staff/hse-document-upload"

# ----------------------------------------------------
# Test values
# ----------------------------------------------------

STAFF_ID = "685aa791129a31f06a04ef90"

DOCUMENT_URL = "https://storage.googleapis.com/xpresshealthcdn/consent_form/sandrakjoshy%40gmail.com_consent_form.docx?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=659491062075-compute%40developer.gserviceaccount.com%2F20260627%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20260627T171831Z&X-Goog-Expires=3600&X-Goog-SignedHeaders=host&X-Goog-Signature=726c83888eab47dca5c3328899ca4afe9095ac4cf62c9ecf19aff7e28eb26b39382ffd6f4e0629fd5b5b7ae92f50ef892aa03a17351c86b99a92e9d930d7e63e110291a2a63fbcf83883ff685cc4982dc09a8c5f674213949f3c42aa4c15316eb95f28f221171ff07aa010ed1e05ab8205efa2d035549a4d1fbd9ff6dd53c38f591bd3ce18f7f420f8e9618d5ac48c44a39a5dc4794a4f522f267cdc2432f73e9541d44a6c649fabcb3102fae9c5121b2c4fd95aa3bcd9d16e072dd178c77bb336ed1ded1c55479fce561c123d22b8fff1a1c605c3f0de91a0bfc197574180b70d1cf3e1f9d0ba915a0a5058967e76334f166b6e850d2f75836d0af8771a9e35"

DOCUMENT_NAME = "sandrakjoshy@gmail.com_consent_form.docx"

HSE_DOCUMENT_TYPE = "others_1"


def upload_document(
    document_url: str,
    filename: str,
    staff_id: str,
    document_type: str,
):
    print(f"Downloading: {document_url}")

    download = requests.get(document_url, timeout=120)
    download.raise_for_status()

    content_type = (
        download.headers.get("Content-Type")
        or mimetypes.guess_type(filename)[0]
        or "application/octet-stream"
    )

    files = {
        "file": (
            filename,
            download.content,
            content_type,
        )
    }

    data = {
        "staff_id": staff_id,
        "hse_document_type": document_type,
    }

    headers = {
        "Api-Key": API_KEY,
        "X-App-Country": COUNTRY,
    }

    print("Uploading to HSE...")

    response = requests.post(
        UPLOAD_URL,
        headers=headers,
        data=data,
        files=files,
        timeout=120,
    )

    print(f"Status : {response.status_code}")

    try:
        print(response.json())
    except Exception:
        print(response.text)

    response.raise_for_status()

    return response.json()


if __name__ == "__main__":
    upload_document(
        document_url=DOCUMENT_URL,
        filename=DOCUMENT_NAME,
        staff_id=STAFF_ID,
        document_type=HSE_DOCUMENT_TYPE,
    )