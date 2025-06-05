import sys
import argparse
import logging
from flask import Request
from werkzeug.test import EnvironBuilder
from werkzeug.wrappers import Request as WerkzeugRequest

# Configure logging FIRST, before any other imports
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True  # Override any existing configuration
)

from src.main import main as cloud_main

def run_as_flask():
    """Simulate a Flask/Cloud Function request locally"""
    builder = EnvironBuilder(method='POST', json={"local": True})
    env = builder.get_environ()
    request = Request(WerkzeugRequest(env))
    response = cloud_main(request)
    print("📤 Response:")
    print(response[0])

def run_as_cli():
    """Run the ingest directly as CLI (bypassing HTTP layer)"""
    from src.hubspot_pipeline.hubspot_ingest.config_loader import init_env
    from src.hubspot_pipeline.hubspot_ingest.main import main as ingest_main
    
    logging.info("🚀 Running in CLI mode")
    try:
        init_env()
        logging.info("✅ Environment initialized")
        
        # Run ingest with default event
        result = ingest_main(event={"cli_mode": True})
        logging.info(f"📤 Result: {result}")
        
    except Exception as e:
        logging.error(f"❌ CLI execution failed: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["flask", "cli"], default="cli", help="Execution mode: flask or cli (default)")
    args = parser.parse_args()
    
    if args.mode == "flask":
        run_as_flask()
    else:
        run_as_cli()