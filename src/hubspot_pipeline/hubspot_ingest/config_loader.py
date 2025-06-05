# src/hubspot_pipeline/hubspot_ingest/config_loader.py

import os
import yaml
import logging
import requests
from dotenv import load_dotenv
from google.cloud import secretmanager
from google.api_core.exceptions import NotFound

CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'schema.yaml')

def load_schema():
    """Load the schema configuration from YAML file"""
    with open(CONFIG_PATH, 'r') as f:
        return yaml.safe_load(f)

def is_running_in_gcp():
    """Detect if running in Google Cloud"""
    try:
        response = requests.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/zone",
            headers={"Metadata-Flavor": "Google"},
            timeout=1
        )
        return response.status_code == 200
    except:
        return False

def get_project_id():
    """Get project ID from GCP metadata or environment"""
    if is_running_in_gcp():
        try:
            response = requests.get(
                "http://metadata.google.internal/computeMetadata/v1/project/project-id",
                headers={"Metadata-Flavor": "Google"}
            )
            response.raise_for_status()
            return response.text
        except Exception as e:
            logging.error(f"Failed to get project ID from metadata: {e}")
            raise RuntimeError("Failed to determine project ID from metadata server")
    else:
        # Fall back to environment variable for local dev
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("BIGQUERY_PROJECT_ID")
        if not project_id:
            raise RuntimeError("GOOGLE_CLOUD_PROJECT or BIGQUERY_PROJECT_ID must be set for local development")
        return project_id

def init_env():
    """Initialize environment variables from appropriate source"""
    # Always load .env first (no-op in GCP, helpful for local dev)
    load_dotenv()
    
    if is_running_in_gcp():
        # Use Secret Manager in GCP
        project_id = get_project_id()
        logging.info(f"🌐 Running in GCP, project: {project_id}")
        
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/HUBSPOT_API_KEY/versions/latest"
            response = client.access_secret_version(request={"name": name})
            secret_value = response.payload.data.decode("UTF-8").strip()
            os.environ["HUBSPOT_API_KEY"] = secret_value
            logging.info(f"✅ HUBSPOT_API_KEY loaded from Secret Manager (first 6 chars): {secret_value[:6]}...")
            
            # Set project ID in environment for consistency
            os.environ["BIGQUERY_PROJECT_ID"] = project_id
            
        except NotFound:
            logging.error("❌ HUBSPOT_API_KEY not found in Secret Manager")
            raise RuntimeError("HUBSPOT_API_KEY not found in Secret Manager")
        except Exception as e:
            logging.error(f"❌ Failed to load secrets from Secret Manager: {e}")
            raise
    else:
        # Local development - validate required env vars
        logging.info("🏠 Running locally")
        required_vars = ["HUBSPOT_API_KEY", "BIGQUERY_PROJECT_ID", "BIGQUERY_DATASET_ID"]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            logging.error(f"❌ Missing required environment variables: {missing}")
            raise RuntimeError(f"Missing required environment variables: {missing}. Check your .env file.")
        
        logging.info("✅ All required environment variables loaded from .env")
    
    # Validate that essential config is now available
    if not os.getenv("HUBSPOT_API_KEY"):
        raise RuntimeError("HUBSPOT_API_KEY not available after environment initialization")
    
    # Set default dataset if not specified
    if not os.getenv("BIGQUERY_DATASET_ID"):
        default_dataset = "hubspot_dev" if not is_running_in_gcp() else "hubspot_prod"
        os.environ["BIGQUERY_DATASET_ID"] = default_dataset
        logging.info(f"📊 Using default dataset: {default_dataset}")

def get_config():
    """Get configuration dictionary after init_env() has been called"""
    return {
        'HUBSPOT_API_KEY': os.getenv('HUBSPOT_API_KEY'),
        'BIGQUERY_PROJECT_ID': os.getenv('BIGQUERY_PROJECT_ID'),
        'BIGQUERY_DATASET_ID': os.getenv('BIGQUERY_DATASET_ID'),
        'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'IS_GCP': is_running_in_gcp(),
    }

def validate_config():
    """Validate that all required configuration is available"""
    config = get_config()
    required = ['HUBSPOT_API_KEY', 'BIGQUERY_PROJECT_ID', 'BIGQUERY_DATASET_ID']
    missing = [key for key in required if not config.get(key)]
    
    if missing:
        raise RuntimeError(f"Configuration validation failed. Missing: {missing}")
    
    logging.info("✅ Configuration validation passed")
    return config