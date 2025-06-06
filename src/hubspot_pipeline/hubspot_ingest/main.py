# src/hubspot_pipeline/hubspot_ingest/main.py

import logging
import uuid
from datetime import datetime
from hubspot_pipeline.hubspot_ingest.config_loader import init_env, load_schema, validate_config
from hubspot_pipeline.hubspot_ingest.fetcher import fetch_object
from hubspot_pipeline.hubspot_ingest.store import store_to_bigquery

def main(event=None, context=None):
    """
    Main entry point for HubSpot data ingestion
    
    Args:
        event: Event data (dict) - can contain:
            - no_limit: bool - if True, fetch all records (no limit)
            - limit: int - specific limit override
            - dry_run: bool - if True, don't write to BigQuery
            - log_level: str - override log level ('DEBUG', 'INFO', 'WARN', 'ERROR')
        context: Cloud Function context (unused)
    
    Returns:
        tuple: (message, status_code)
    """
    # Parse event data first to get log level
    if not isinstance(event, dict):
        event = {}
    
    # Initialize environment and logging
    try:
        loggers = init_env(log_level=event.get('log_level'))
        logger = loggers['process']
    except Exception as e:
        # Fallback logging if init_env fails
        logging.basicConfig(level=logging.INFO)
        logging.error(f"Failed to initialize environment: {e}")
        return f"Configuration error: {e}", 500
    
    logger.info("🚀 HubSpot ingest started")
    
    # Log trigger source for tracking
    trigger_source = event.get("trigger_source", "unknown")
    logger.info(f"🎯 Triggered by: {trigger_source}")
    
    # Log request details
    if logger.isEnabledFor(logging.DEBUG):
        safe_event = {k: v for k, v in event.items() if k != 'api_key'}  # Don't log sensitive data
        logger.debug(f"Request event: {safe_event}")
    
    # Determine fetch limit
    if event.get("no_limit") is True:
        fetch_limit = None
        logger.info("📊 Fetching ALL records (no limit)")
    elif "limit" in event:
        fetch_limit = int(event["limit"])
        logger.info(f"📊 Using custom limit: {fetch_limit}")
    else:
        fetch_limit = 10  # Safer default for testing
        logger.info(f"📊 Using default limit: {fetch_limit}")
    
    dry_run = event.get("dry_run", True)  # Default to dry run for safety
    if dry_run:
        logger.info("🛑 DRY RUN MODE - no data will be written to BigQuery")
    else:
        logger.info("💾 LIVE MODE - data will be written to BigQuery")
    
    # Validate configuration
    try:
        config = validate_config()
        logger.info(f"✅ Configuration validated for environment: {config['ENVIRONMENT']}")
        logger.debug(f"Target dataset: {config['BIGQUERY_DATASET_ID']}")
    except Exception as e:
        logger.error(f"Configuration validation failed: {e}")
        return f"Configuration error: {e}", 500
    
    # Load schema and create snapshot ID
    try:
        schema = load_schema()
        snapshot_id = datetime.utcnow().isoformat(timespec="seconds")
        logger.info(f"📸 Created snapshot ID: {snapshot_id}")
        logger.debug(f"Schema contains {len(schema)} object types: {list(schema.keys())}")
    except Exception as e:
        logger.error(f"Failed to load schema: {e}")
        return f"Schema error: {e}", 500
    
    # Process each object type from schema
    results = {}
    total_rows = 0
    fetch_logger = loggers['fetch']
    store_logger = loggers['store']
    
    start_time = datetime.utcnow()
    logger.info(f"Starting processing of {len(schema)} object types")
    
    for object_type, config_obj in schema.items():
        obj_start_time = datetime.utcnow()
        logger.info(f"🔄 Processing {object_type}...")
        
        try:
            # Fetch data
            fetch_logger.debug(f"Fetching {object_type} with limit {fetch_limit}")
            rows = fetch_object(object_type, config_obj, snapshot_id, limit=fetch_limit)
            row_count = len(rows)
            total_rows += row_count
            results[object_type] = row_count
            
            obj_fetch_time = (datetime.utcnow() - obj_start_time).total_seconds()
            logger.info(f"✅ Fetched {row_count} {object_type} records in {obj_fetch_time:.2f}s")
            
            if fetch_logger.isEnabledFor(logging.DEBUG) and rows:
                sample_record = {k: v for k, v in rows[0].items() if k not in ['api_key', 'token']}
                fetch_logger.debug(f"Sample {object_type} record structure: {list(sample_record.keys())}")
            
            # Store to BigQuery (unless dry run)
            if not dry_run and rows:
                table_name = config_obj["object_name"]
                store_logger.info(f"💾 Storing {row_count} records to {table_name}")
                
                store_start_time = datetime.utcnow()
                store_to_bigquery(rows, table_name)
                store_time = (datetime.utcnow() - store_start_time).total_seconds()
                
                store_logger.info(f"✅ Stored {row_count} records to {table_name} in {store_time:.2f}s")
                if store_logger.isEnabledFor(logging.DEBUG):
                    store_logger.debug(f"BigQuery insert rate: {row_count/store_time:.1f} records/second")
                    
            elif dry_run:
                logger.info(f"🛑 DRY RUN: Would have stored {row_count} records to {config_obj['object_name']}")
            else:
                logger.info(f"ℹ️ No records to store for {object_type}")
            
            obj_total_time = (datetime.utcnow() - obj_start_time).total_seconds()
            logger.debug(f"Completed {object_type} processing in {obj_total_time:.2f}s")
                
        except Exception as e:
            logger.error(f"Failed to process {object_type}: {e}", exc_info=True)
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Error details for {object_type}: {type(e).__name__}: {str(e)}")
            return f"Processing error for {object_type}: {e}", 500
    
    # Summary
    total_time = (datetime.utcnow() - start_time).total_seconds()
    summary = f"Ingestion complete. Snapshot: {snapshot_id}, Total records: {total_rows}"
    if dry_run:
        summary += " (DRY RUN)"
    
    logger.info(f"🎉 {summary}")
    logger.info(f"📊 Results by object: {results}")
    logger.info(f"⏱️ Total processing time: {total_time:.2f}s")
    
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Average processing rate: {total_rows/total_time:.1f} records/second")
        logger.debug(f"Memory and performance stats would go here in production")
    
    return summary, 200

if __name__ == "__main__":
    # For direct execution
    print("🧪 Running ingest directly")
    result = main({"limit": 5, "dry_run": True, "log_level": "DEBUG"})
    print(f"Result: {result}")