from pathlib import Path
import mimetypes
import os

import requests
from dotenv import load_dotenv
from google.cloud import storage

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

BUCKET_NAME = "xpresshealthcdn"

STAFF_ID = "685aa791129a31f06a04ef90"

BLOB_NAME = "consent_form/sandrakjoshy@gmail.com_consent_form.docx"

DOCUMENT_NAME = "sandrakjoshy@gmail.com_consent_form.docx"

HSE_DOCUMENT_TYPE = "others_1"


def upload_gcs_file(
    bucket_name: str,
    blob_name: str,
    filename: str,
    staff_id: str,
    document_type: str,
):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    print(f"Downloading from GCS: {blob_name}")

    file_bytes = blob.download_as_bytes()

    content_type = (
        blob.content_type
        or mimetypes.guess_type(filename)[0]
        or "application/octet-stream"
    )

    files = {
        "file": (
            filename,
            file_bytes,
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
    upload_gcs_file(
        bucket_name=BUCKET_NAME,
        blob_name=BLOB_NAME,
        filename=DOCUMENT_NAME,
        staff_id=STAFF_ID,
        document_type=HSE_DOCUMENT_TYPE,
    )