# --- main.py ---
# Entry point for Cloud Function
import logging

# Configure production-ready logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(module)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

import uuid
from datetime import datetime
from hubspot_pipeline.hubspot_ingest.config_loader import init_env, load_schema
from hubspot_pipeline.hubspot_ingest.fetcher import fetch_object
from hubspot_pipeline.hubspot_ingest.store import store_to_bigquery

def main(event=None, context=None):
    logging.info("🚀 Cloud Function started.")
    init_env()
    fetch_limit = None if isinstance(event, dict) and event.get("no_limit") is True else 100

    schema = load_schema()
    snapshot_id = datetime.utcnow().isoformat(timespec="seconds")

    for object_type, config in schema.items():
        logging.info(f"Fetching {object_type}...")
        rows = fetch_object(object_type, config, snapshot_id, limit=fetch_limit)
        logging.info(f"Fetched {len(rows)} rows")
        store_to_bigquery(rows, config["object_name"])

    return "Ingestion complete", 200

if __name__ == "__main__":
    main()