# src/hubspot_pipeline/scoring/config.py

import os
import logging
import requests
from dotenv import load_dotenv
from google.cloud import secretmanager
from google.api_core.exceptions import NotFound

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

def get_project_id():
    """Get project ID from GCP metadata or environment"""
    logger = logging.getLogger('hubspot.scoring.config')
    
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

def get_default_dataset(env):
    """Get default dataset name based on environment"""
    datasets = {
        'development': 'Hubspot_dev_ob',
        'staging': 'Hubspot_staging',
        'production': 'Hubspot_prod'
    }
    return datasets.get(env, 'Hubspot_dev_ob')

def setup_logging(log_level=None):
    """
    Configure structured logging for scoring function
    
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
    
    # Create logger for scoring
    logger = logging.getLogger('hubspot.scoring')
    logger.info(f"Scoring logging configured - Environment: {env}, Level: {final_level}")
    
    if final_level == 'DEBUG':
        logger.debug("Debug logging enabled for scoring function")
    
    return logger

def init_env(log_level=None):
    """
    Initialize environment variables and configuration for scoring function
    
    Args:
        log_level (str, optional): Override log level for this session
    """
    # Setup logging first
    logger = setup_logging(log_level)
    
    # Always load .env first (no-op in GCP, helpful for local dev)
    load_dotenv()
    logger.debug("Loaded .env file (if present)")
    
    env = get_environment()
    is_gcp = is_running_in_gcp()
    
    logger.info(f"Initializing scoring environment - Running in: {'GCP' if is_gcp else 'Local'}, Environment: {env}")
    
    if is_gcp:
        # Use Secret Manager in GCP (scoring doesn't need HubSpot API key)
        project_id = get_project_id()
        logger.info(f"Running in GCP, project: {project_id}")
        
        # Set project ID in environment for consistency
        os.environ["BIGQUERY_PROJECT_ID"] = project_id
        logger.debug(f"Set BIGQUERY_PROJECT_ID to {project_id}")
    else:
        # Local development - validate required env vars
        logger.info("Running locally")
        required_vars = ["BIGQUERY_PROJECT_ID", "BIGQUERY_DATASET_ID"]
        missing = [var for var in required_vars if not os.getenv(var)]
        
        if missing:
            logger.error(f"Missing required environment variables: {missing}")
            raise RuntimeError(f"Missing required environment variables: {missing}. Check your .env file.")
        
        logger.info("Required environment variables loaded from .env")
        if logger.isEnabledFor(logging.DEBUG):
            for var in required_vars:
                value = os.getenv(var)
                logger.debug(f"{var}: {value}")
    
    # Set default dataset if not specified
    if not os.getenv("BIGQUERY_DATASET_ID"):
        default_dataset = get_default_dataset(env)
        os.environ["BIGQUERY_DATASET_ID"] = default_dataset
        logger.info(f"Using default dataset: {default_dataset}")
    else:
        logger.debug(f"Using configured dataset: {os.getenv('BIGQUERY_DATASET_ID')}")
    
    logger.info("Scoring environment initialization completed successfully")
    return logger

def get_config():
    """Get configuration dictionary after init_env() has been called"""
    return {
        'BIGQUERY_PROJECT_ID': os.getenv('BIGQUERY_PROJECT_ID'),
        'BIGQUERY_DATASET_ID': os.getenv('BIGQUERY_DATASET_ID'),
        'GOOGLE_APPLICATION_CREDENTIALS': os.getenv('GOOGLE_APPLICATION_CREDENTIALS'),
        'IS_GCP': is_running_in_gcp(),
        'ENVIRONMENT': get_environment(),
    }

def validate_config():
    """Validate that all required configuration is available for scoring"""
    logger = logging.getLogger('hubspot.scoring.config')
    config = get_config()
    required = ['BIGQUERY_PROJECT_ID', 'BIGQUERY_DATASET_ID']
    missing = [key for key in required if not config.get(key)]
    
    if missing:
        logger.error(f"Scoring configuration validation failed. Missing: {missing}")
        raise RuntimeError(f"Scoring configuration validation failed. Missing: {missing}")
    
    logger.info("Scoring configuration validation passed")
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Validated scoring configuration: {config}")
    
    return config