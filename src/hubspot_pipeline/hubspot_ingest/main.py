# src/hubspot_pipeline/hubspot_ingest/main.py

import logging
import uuid
from datetime import datetime
from hubspot_pipeline.hubspot_ingest.config_loader import init_env, load_schema, validate_config
from hubspot_pipeline.hubspot_ingest.fetcher import fetch_object
from hubspot_pipeline.hubspot_ingest.store import store_to_bigquery

# Get logger for this module (don't reconfigure)
logger = logging.getLogger(__name__)

def main(event=None, context=None):
    """
    Main entry point for HubSpot data ingestion
    
    Args:
        event: Event data (dict) - can contain:
            - no_limit: bool - if True, fetch all records (no limit)
            - limit: int - specific limit override
            - dry_run: bool - if True, don't write to BigQuery
        context: Cloud Function context (unused)
    
    Returns:
        tuple: (message, status_code)
    """
    logging.info("🚀 HubSpot ingest started")
    
    # Parse event data
    if not isinstance(event, dict):
        event = {}
    
    # Determine fetch limit
    if event.get("no_limit") is True:
        fetch_limit = None
        logging.info("📊 Fetching ALL records (no limit)")
    elif "limit" in event:
        fetch_limit = int(event["limit"])
        logging.info(f"📊 Using custom limit: {fetch_limit}")
    else:
        fetch_limit = 100
        logging.info(f"📊 Using default limit: {fetch_limit}")
    
    dry_run = event.get("dry_run", False)
    if dry_run:
        logging.info("🛑 DRY RUN MODE - no data will be written to BigQuery")
    
    # Validate configuration
    try:
        config = validate_config()
        logging.info(f"✅ Configuration validated for environment: {'GCP' if config['IS_GCP'] else 'Local'}")
    except Exception as e:
        logging.error(f"❌ Configuration validation failed: {e}")
        return f"Configuration error: {e}", 500
    
    # Load schema and create snapshot ID
    try:
        schema = load_schema()
        snapshot_id = datetime.utcnow().isoformat(timespec="seconds")
        logging.info(f"📸 Created snapshot ID: {snapshot_id}")
    except Exception as e:
        logging.error(f"❌ Failed to load schema: {e}")
        return f"Schema error: {e}", 500
    
    # Process each object type from schema
    results = {}
    total_rows = 0
    
    for object_type, config_obj in schema.items():
        try:
            logging.info(f"🔄 Processing {object_type}...")
            
            # Fetch data
            rows = fetch_object(object_type, config_obj, snapshot_id, limit=fetch_limit)
            row_count = len(rows)
            total_rows += row_count
            results[object_type] = row_count
            
            logging.info(f"✅ Fetched {row_count} {object_type} records")
            
            # Store to BigQuery (unless dry run)
            if not dry_run and rows:
                table_name = config_obj["object_name"]
                store_to_bigquery(rows, table_name)
                logging.info(f"✅ Stored {row_count} records to {table_name}")
            elif dry_run:
                logging.info(f"🛑 DRY RUN: Would have stored {row_count} records to {config_obj['object_name']}")
            else:
                logging.info(f"ℹ️ No records to store for {object_type}")
                
        except Exception as e:
            logging.error(f"❌ Failed to process {object_type}: {e}", exc_info=True)
            return f"Processing error for {object_type}: {e}", 500
    
    # Summary
    summary = f"Ingestion complete. Snapshot: {snapshot_id}, Total records: {total_rows}"
    if dry_run:
        summary += " (DRY RUN)"
    
    logging.info(f"🎉 {summary}")
    logging.info(f"📊 Results by object: {results}")
    
    return summary, 200

if __name__ == "__main__":
    # For direct execution
    logging.info("🧪 Running ingest directly")
    init_env()
    result = main({"limit": 10, "dry_run": True})
    print(f"Result: {result}")