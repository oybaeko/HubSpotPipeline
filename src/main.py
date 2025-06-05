# src/main.py

import logging
from flask import Request
from hubspot_pipeline.hubspot_ingest.config_loader import init_env
from hubspot_pipeline.hubspot_ingest.main import main as ingest_main

def main(request: Request):
    """
    Cloud Function entry point for HTTP triggers
    """
    # Basic logging setup (will be reconfigured by init_env)
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('hubspot.cloudfunction')
    
    logger.info("🌐 Cloud Function HTTP trigger received")
    
    # Parse request data first to get any log level override
    try:
        data = request.get_json(silent=True) or {}
        logger.info(f"📦 Parsed request data keys: {list(data.keys())}")
        
        # Log request details at debug level (will be visible if debug enabled)
        if logger.isEnabledFor(logging.DEBUG):
            safe_data = {k: v for k, v in data.items() if k not in ['api_key', 'token']}
            logger.debug(f"Full request data: {safe_data}")
    except Exception as e:
        logger.warning(f"Failed to parse JSON body: {e}")
        data = {}
    
    # Call the ingest main function (it will reconfigure logging)
    try:
        result = ingest_main(event=data)
        logger.info(f"✅ Ingest completed successfully")
        return result
    except Exception as e:
        logger.error(f"❌ Ingest failed: {e}", exc_info=True)
        return f"Ingest error: {e}", 500