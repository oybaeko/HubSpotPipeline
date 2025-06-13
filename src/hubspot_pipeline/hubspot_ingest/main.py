# src/hubspot_pipeline/hubspot_ingest/main.py

import logging
from datetime import datetime
from .config_loader import init_env, load_schema, validate_config
from .fetcher import fetch_object
from .store import store_to_bigquery
from .reference import update_reference_data
from .registry import (
    register_snapshot_start, 
    register_snapshot_ingest_complete, 
    register_snapshot_failure
)
from .events import publish_snapshot_completed_event, publish_snapshot_failed_event

def main(event=None, context=None):
    """
    Main entry point for HubSpot data ingestion with reference data and registry tracking.
    
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
    
    logger.info("🚀 HubSpot ingest started (with reference data and registry)")
    
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
        snapshot_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        logger.info(f"📸 Created snapshot ID: {snapshot_id}")
        logger.debug(f"Schema contains {len(schema)} object types: {list(schema.keys())}")
    except Exception as e:
        logger.error(f"Failed to load schema: {e}")
        return f"Schema error: {e}", 500
    
    # Pre-flight check: Ensure all required tables are ready
    logger.info("🔍 Running pre-flight table check...")
    try:
        from .table_checker import ensure_all_tables_ready
        
        if not dry_run:
            if not ensure_all_tables_ready(schema):
                logger.error("❌ Pre-flight check failed - not all tables are ready")
                return "Pre-flight check failed: tables not ready", 500
            logger.info("✅ Pre-flight check passed - all tables ready")
        else:
            logger.info("🛑 DRY RUN: Skipping pre-flight table check")
    except Exception as e:
        logger.error(f"❌ Pre-flight check error: {e}")
        if not dry_run:
            return f"Pre-flight check error: {e}", 500
    
    # Register snapshot start (only in live mode)
    if not dry_run:
        logger.info("📝 Registering snapshot start...")
        register_success = register_snapshot_start(snapshot_id, trigger_source)
        if not register_success:
            logger.warning("⚠️ Failed to register snapshot start, but continuing...")
    else:
        logger.info("🛑 DRY RUN: Skipping snapshot registration")
    
    # Main processing variables
    results = {}
    total_rows = 0
    fetch_logger = loggers['fetch']
    store_logger = loggers['store']
    start_time = datetime.utcnow()
    
    try:
        logger.info(f"Starting processing of {len(schema)} object types")
        
        # Process each object type from schema (companies and deals)
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
                    store_to_bigquery(rows, table_name)
                    
                    store_time = (datetime.utcnow() - obj_start_time).total_seconds() - obj_fetch_time
                    store_logger.info(f"✅ Stored {row_count} records to {table_name} in {store_time:.2f}s")
                        
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
                raise  # Re-raise to trigger failure handling
        
        # Process reference data (owners and deal stages)
        reference_counts = {}
        if not dry_run:
            logger.info("🔄 Processing reference data...")
            try:
                reference_counts = update_reference_data()
                logger.info(f"✅ Reference data processed: {reference_counts}")
            except Exception as e:
                logger.error(f"❌ Reference data processing failed: {e}")
                # Don't fail the whole ingest for reference data issues
                reference_counts = {'hs_owners': 0, 'hs_deal_stage_reference': 0}
        else:
            logger.info("🛑 DRY RUN: Skipping reference data processing")
            reference_counts = {'hs_owners': 0, 'hs_deal_stage_reference': 0}
        
        # Register snapshot ingest completion
        if not dry_run:
            logger.info("📝 Registering snapshot ingest completion...")
            try:
                register_success = register_snapshot_ingest_complete(
                    snapshot_id=snapshot_id,
                    data_counts=results,
                    reference_counts=reference_counts
                )
                if register_success:
                    logger.info(f"✅ Registered ingest completion in snapshot registry")
                else:
                    logger.warning("⚠️ Registry registration failed")
            except Exception as e:
                logger.warning(f"⚠️ Registry registration failed: {e}")
                # Don't fail the ingest for registry issues
        else:
            logger.info("🛑 DRY RUN: Skipping registry registration")
        
        # Publish completion event
        if not dry_run:
            logger.info("📤 Publishing snapshot completion event...")
            try:
                message_id = publish_snapshot_completed_event(
                    snapshot_id=snapshot_id,
                    data_counts=results,
                    reference_counts=reference_counts
                )
                if message_id:
                    if message_id.startswith("local_mode"):
                        logger.info(f"ℹ️ Event publishing: {message_id}")
                    else:
                        logger.info(f"✅ Published completion event: {message_id}")
                else:
                    logger.warning("⚠️ Failed to publish event (but ingest succeeded)")
            except Exception as e:
                logger.warning(f"⚠️ Event publishing failed: {e}")
                # Don't fail the ingest for event issues
        else:
            logger.info("🛑 DRY RUN: Skipping event publishing")
        
        # Success summary
        total_time = (datetime.utcnow() - start_time).total_seconds()
        summary = f"Ingestion complete. Snapshot: {snapshot_id}, Total records: {total_rows}"
        if dry_run:
            summary += " (DRY RUN)"
        
        logger.info(f"🎉 {summary}")
        logger.info(f"📊 Results by object: {results}")
        logger.info(f"📊 Reference data: {reference_counts}")
        logger.info(f"⏱️ Total processing time: {total_time:.2f}s")
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Average processing rate: {total_rows/total_time:.1f} records/second")
        
        return {
            "status": "success",
            "snapshot_id": snapshot_id,
            "total_records": total_rows,
            "results": results,
            "reference_counts": reference_counts,
            "processing_time_seconds": total_time,
            "dry_run": dry_run
        }, 200
        
    except Exception as e:
        # Handle any processing failures
        total_time = (datetime.utcnow() - start_time).total_seconds()
        error_msg = str(e)
        
        logger.error(f"❌ Ingestion failed after {total_time:.2f}s: {error_msg}", exc_info=True)
        
        # Register failure (only in live mode)
        if not dry_run:
            try:
                logger.info("📝 Registering snapshot failure...")
                register_snapshot_failure(snapshot_id, error_msg)
                logger.info("📝 Failure registered in snapshot registry")
                
                # Publish failure event
                logger.info("📤 Publishing snapshot failure event...")
                publish_snapshot_failed_event(snapshot_id, error_msg)
                
            except Exception as registry_error:
                logger.error(f"❌ Failed to register failure: {registry_error}")
        
        return {
            "status": "error",
            "snapshot_id": snapshot_id,
            "error": error_msg,
            "total_records": total_rows,
            "results": results,
            "processing_time_seconds": total_time,
            "dry_run": dry_run
        }, 500


if __name__ == "__main__":
    # For direct execution
    print("🧪 Running ingest directly")
    result = main({"limit": 5, "dry_run": True, "log_level": "DEBUG"})
    print(f"Result: {result}")