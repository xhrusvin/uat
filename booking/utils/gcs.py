# booking/utils/gcs.py
import os
from google.cloud import storage
from dotenv import load_dotenv
import json

load_dotenv()

GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
GCS_CREDENTIALS_JSON = os.getenv('GCS_CREDENTIALS_JSON')

def get_gcs_client():
    if not GCS_CREDENTIALS_JSON:
        raise ValueError("GCS_CREDENTIALS_JSON not found in .env")
    
    credentials_dict = json.loads(GCS_CREDENTIALS_JSON)
    return storage.Client.from_service_account_info(credentials_dict)

def upload_to_gcs(file_obj, destination_blob_name):
    """Upload file-like object to GCS"""
    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    
    if hasattr(file_obj, 'read'):
        file_obj.seek(0)
        blob.upload_from_file(file_obj)
    else:
        blob.upload_from_filename(file_obj)
    
    return destination_blob_name

def get_gcs_public_url(blob_name):
    """Returns public URL (make sure bucket is public or use signed URL)"""
    return f"https://storage.googleapis.com/{GCS_BUCKET_NAME}/{blob_name}"

def delete_from_gcs(blob_name):
    client = get_gcs_client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.delete()