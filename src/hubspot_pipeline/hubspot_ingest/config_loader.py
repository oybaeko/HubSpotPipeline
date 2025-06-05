# Empty init to allow package import

# --- config_loader.py ---
# Loads environment and local schema

import os
import yaml

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'schema.yaml')

def load_schema():
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

# load .env if needed
def init_env():
    import os
    import logging
    from google.cloud import secretmanager
    from google.api_core.exceptions import NotFound

    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    if project_id:
        logging.info(f"Detected project: {project_id}")
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/HUBSPOT_API_KEY/versions/latest"
            response = client.access_secret_version(request={"name": name})
            secret_value = response.payload.data.decode("UTF-8").strip()
            logging.info(f"🔍 HUBSPOT_API_KEY (first 6 chars): {secret_value[:6]}...")
            os.environ["HUBSPOT_API_KEY"] = secret_value
            logging.info("✅ HUBSPOT_API_KEY loaded from Secret Manager")
        except NotFound:
            logging.warning("HUBSPOT_API_KEY not found in Secret Manager; falling back to .env")
    else:
        logging.warning("GOOGLE_CLOUD_PROJECT not set; falling back to .env")

    from dotenv import load_dotenv
    load_dotenv()