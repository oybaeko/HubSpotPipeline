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

def get_environment():
    """Detect environment from Cloud Function name or env var"""
    function_name = os.getenv('K_SERVICE', '')  # Cloud Run service name
    if 'prod' in function_name:
        return 'production'
    elif 'staging' in function_name:
        return 'staging'
    else:
        return 'development'

def setup_logging(log_level=None):
    """
    Configure structured logging based on environment and optional override
    
    Args:
        log_level (str, optional): Override log level ('DEBUG', 'INFO', 'WARN', 'ERROR')
    """
    # Environment-based defaults
    env = get_environment()
    default_levels = {
        'development': 'DEBUG',
        'staging': 'INFO', 
        'production': 'WARN'
    }
    
    # Determine final log level
    final_level = (
        log_level or                           # Request override
        os.getenv('LOG_LEVEL') or             # Environment variable
        default_levels.get(env, 'INFO')       # Environment default
    ).upper()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, final_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True  # Override any existing configuration
    )
    
    # Create loggers for different components
    loggers = {
        'config': logging.getLogger('hubspot.config'),
        'fetch': logging.getLogger('hubspot.fetch'),
        'store': logging.getLogger('hubspot.store'),
        'process': logging.getLogger('hubspot.process')
    }
    
    # Log the logging configuration
    config_logger = loggers['config']
    config_logger.info(f"Logging configured - Environment: {env}, Level: {final_level}")
    
    if final_level == 'DEBUG':
        config_logger.debug("Debug logging enabled - detailed information will be logged")
        config_logger.debug(f"Available loggers: {list(loggers.keys())}")
    
    return loggers

def get_project_id():
    """Get project ID from GCP metadata or environment"""
    logger = logging.getLogger('hubspot.config')
    
    if is_running_in_gcp():
        try:
            response = requests.get(
                "http://metadata.google.internal/computeMetadata/v1/project/project-id",
                headers={"Metadata-Flavor": "Google"}
            )
            response.raise_for_status()
            project_id = response.text
            logger.debug(f"Retrieved project ID from metadata: {project_id}")
            return project_id
        except Exception as e:
            logger.error(f"Failed to get project ID from metadata: {e}")
            raise RuntimeError("Failed to determine project ID from metadata server")
    else:
        # Fall back to environment variable for local dev
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("BIGQUERY_PROJECT_ID")
        if not project_id:
            logger.error("No project ID found in environment variables")
            raise RuntimeError("GOOGLE_CLOUD_PROJECT or BIGQUERY_PROJECT_ID must be set for local development")
        
        logger.debug(f"Using project ID from environment: {project_id}")
        return project_id

def init_env(log_level=None):
    """
    Initialize environment variables from appropriate source
    
    Args:
        log_level (str, optional): Override log level for this session
    """
    # Setup logging first
    loggers = setup_logging(log_level)
    logger = loggers['config']
    
    # Always load .env first (no-op in GCP, helpful for local dev)
    load_dotenv()
    logger.debug("Loaded .env file (if present)")
    
    env = get_environment()
    is_gcp = is_running_in_gcp()
    
    logger.info(f"Initializing environment - Running in: {'GCP' if is_gcp else 'Local'}, Environment: {env}")
    
    if is_gcp:
        # Use Secret Manager in GCP
        project_id = get_project_id()
        logger.info(f"Running in GCP, project: {project_id}")
        
        try:
            logger.debug("Connecting to Secret Manager")
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/HUBSPOT_API_KEY/versions/latest"
            
            logger.debug(f"Retrieving secret: {name}")
            response = client.access_secret_version(request={"name": name})
            secret_value = response.payload.data.decode("UTF-8").strip()
            os.environ["HUBSPOT_API_KEY"] = secret_value
            
            logger.info("HUBSPOT_API_KEY loaded from Secret Manager")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"HUBSPOT_API_KEY (first 6 chars): {secret_value[:6]}...")
            
            # Set project ID in environment for consistency
            os.environ["BIGQUERY_PROJECT_ID"] = project_id
            logger.debug(f"Set BIGQUERY_PROJECT_ID to {project_id}")
            
        except NotFound:
            logger.error("HUBSPOT_API_KEY not found in Secret Manager")
            raise RuntimeError("HUBSPOT_API_KEY not found in Secret Manager")
        except Exception as e:
            logger.error(f"Failed to load secrets from Secret Manager: {e}")
            raise
    else:
        # Local development - validate required env vars
        logger.info("Running locally")
        required_vars = ["HUBSPOT_API_KEY", "BIGQUERY_PROJECT_ID", "BIGQUERY_DATASET_ID"]
        missing = [var for var in required_vars if not os.getenv(var)]
        
        if missing:
            logger.error(f"Missing required environment variables: {missing}")
            raise RuntimeError(f"Missing required environment variables: {missing}. Check your .env file.")
        
        logger.info("All required environment variables loaded from .env")
        if logger.isEnabledFor(logging.DEBUG):
            for var in required_vars:
                value = os.getenv(var)
                if var == "HUBSPOT_API_KEY":
                    logger.debug(f"{var}: {value[:6] if value else 'None'}...")
                else:
                    logger.debug(f"{var}: {value}")
    
    # Validate that essential config is now available
    if not os.getenv("HUBSPOT_API_KEY"):
        logger.error("HUBSPOT_API_KEY not available after environment initialization")
        raise RuntimeError("HUBSPOT_API_KEY not available after environment initialization")
    
    # Set default dataset if not specified
    if not os.getenv("BIGQUERY_DATASET_ID"):
        default_dataset = get_default_dataset(env)
        os.environ["BIGQUERY_DATASET_ID"] = default_dataset
        logger.info(f"Using default dataset: {default_dataset}")
    else:
        logger.debug(f"Using configured dataset: {os.getenv('BIGQUERY_DATASET_ID')}")
    
    logger.info("Environment initialization completed successfully")
    return loggers

def get_default_dataset(env):
    """Get default dataset name based on environment"""
    datasets = {
        'development': 'Hubspot_dev_ob',
        'staging': 'Hubspot_staging',
        'production': 'Hubspot_prod'
    }
    return datasets.get(env, 'Hubspot_dev_ob')

def get_config():
    """Get configuration dictionary after init_env() has been called"""
    return {
        'HUBSPOT_API_KEY': os.getenv('HUBSPOT_API_KEY'),
        'BIGQUERY_PROJECT_ID': os.getenv('BIGQUERY_PROJECT_ID'),
        'BIGQUERY_DATASET_ID': os.getenv('BIGQUERY_DATASET_ID'),
        'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'IS_GCP': is_running_in_gcp(),
        'ENVIRONMENT': get_environment(),
    }

def validate_config():
    """Validate that all required configuration is available"""
    logger = logging.getLogger('hubspot.config')
    config = get_config()
    required = ['HUBSPOT_API_KEY', 'BIGQUERY_PROJECT_ID', 'BIGQUERY_DATASET_ID']
    missing = [key for key in required if not config.get(key)]
    
    if missing:
        logger.error(f"Configuration validation failed. Missing: {missing}")
        raise RuntimeError(f"Configuration validation failed. Missing: {missing}")
    
    logger.info("Configuration validation passed")
    if logger.isEnabledFor(logging.DEBUG):
        safe_config = {k: v for k, v in config.items() if k != 'HUBSPOT_API_KEY'}
        safe_config['HUBSPOT_API_KEY'] = f"{config['HUBSPOT_API_KEY'][:6]}..." if config.get('HUBSPOT_API_KEY') else 'None'
        logger.debug(f"Validated configuration: {safe_config}")
    
    return config