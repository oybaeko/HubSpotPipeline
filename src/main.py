# src/main.py

import logging
from flask import Request
from hubspot_pipeline.hubspot_ingest.config_loader import init_env
from hubspot_pipeline.hubspot_ingest.main import main as ingest_main

# Configure production-ready logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(module)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

def main(request: Request):
    """
    Cloud Function entry point for HTTP triggers
    """
    logging.info("🌐 Cloud Function HTTP trigger received")
    
    # Initialize environment (handles both local and GCP automatically)
    try:
        init_env()
    except Exception as e:
        logging.error(f"❌ Failed to initialize environment: {e}")
        return f"Configuration error: {e}", 500
    
    # Parse request data
    try:
        data = request.get_json(silent=True) or {}
        logging.info(f"📦 Parsed request data: {data}")
    except Exception as e:
        logging.warning(f"⚠️ Failed to parse JSON body: {e}")
        data = {}
    
    # Call the ingest main function
    try:
        return ingest_main(event=data)
    except Exception as e:
        logging.error(f"❌ Ingest failed: {e}", exc_info=True)
        return f"Ingest error: {e}", 500