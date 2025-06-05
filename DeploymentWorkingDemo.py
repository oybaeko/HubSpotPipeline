# src/main.py

import os
import logging
import requests
from flask import Request
from google.cloud import secretmanager

# Configure lightweight logging for early debug
logging.basicConfig(
    level=logging.INFO,
    format="%Y-%m-%d %H:%M:%S %(levelname)s [cloud_entry]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def get_project_id():
    try:
        metadata_url = "http://metadata.google.internal/computeMetadata/v1/project/project-id"
        response = requests.get(metadata_url, headers={"Metadata-Flavor": "Google"})
        response.raise_for_status()
        return response.text
    except Exception as e:
        logging.error(f"❌ Failed to get project ID from metadata: {e}")
        raise RuntimeError("Failed to determine project ID from metadata server")

def init_env():
    logging.info("🔧 init_env(): start")
    if os.getenv("HUBSPOT_API_KEY"):
        logging.info("✅ HUBSPOT_API_KEY already set in environment")
        return

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or get_project_id()
    logging.info(f"📡 Using project ID: {project_id}")

    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/HUBSPOT_API_KEY/versions/latest"
        response = client.access_secret_version(request={"name": name})
        secret_value = response.payload.data.decode("UTF-8").strip()
        os.environ["HUBSPOT_API_KEY"] = secret_value
        logging.info(f"✅ HUBSPOT_API_KEY loaded from Secret Manager (first 6 chars): {secret_value[:6]}...")
    except Exception as e:
        logging.error(f"❌ Failed to load HUBSPOT_API_KEY from Secret Manager: {e}")
        raise

def main(request: Request):
    logging.info("🌐 Cloud Function trigger received")
    init_env()
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or get_project_id()
    logging.info("✅ Environment initialized, HUBSPOT_API_KEY attempted from SM")
    return (f"Ready\nProject ID: {project_id}", 200)

"""
requirements.txt:
flask>=2.0.0
google-cloud-secret-manager>=2.0.0
google-cloud-logging>=3.0.0
requests>=2.25.0
"""


"""gcloud functions deploy hubspotIngestStaging \  --gen2 \                         
  --runtime=python311 \
  --region=europe-west1 \
  --source=src \
  --entry-point=main \
  --trigger-http \
  --allow-unauthenticated"""