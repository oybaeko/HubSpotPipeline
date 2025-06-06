# src/hubspot_pipeline/hubspot_ingest/main.py

import logging
import uuid
from datetime import datetime
from hubspot_pipeline.hubspot_ingest.config_loader import init_env, load_schema, validate_config
from hubspot_pipeline.hubspot_ingest.fetcher import fetch_object
from hubspot_pipeline.hubspot_ingest.store import store_to_bigquery, upsert_to_bigquery, publish_snapshot_completed_event, register_snapshot_ingest

def fetch_and_process_reference_data(snapshot_id):
    """
    Fetch and upsert reference data (owners and deal stages)
    Uses existing logic from your working functions
    
    Returns:
        dict: Reference data counts
    """
    logger = logging.getLogger('hubspot.reference')
    
    logger.info("🔄 Fetching reference data (owners and deal stages)")
    
    reference_counts = {}
    
    # ═══ FETCH OWNERS (using your existing working logic) ═══
    try:
        logger.info("📊 Fetching owners from HubSpot...")
        
        # Import your existing working fetch_owners function
        from hubspot_pipeline.fetch_hubspot_data import fetch_owners
        
        raw_owners = fetch_owners()
        
        if raw_owners:
            # Transform to BigQuery schema format
            owners_rows = []
            for owner in raw_owners:
                row = {
                    "owner_id": owner.get("id"),
                    "email": owner.get("email"),
                    "first_name": owner.get("firstName"),
                    "last_name": owner.get("lastName"),
                    "user_id": owner.get("userId"),
                    "active": owner.get("active"),
                    "timestamp": owner.get("updatedAt") or owner.get("createdAt"),
                }
                owners_rows.append(row)
            
            # Upsert to BigQuery
            owners_count = upsert_to_bigquery(owners_rows, "hs_owners", "owner_id")
            reference_counts['hs_owners'] = owners_count
            logger.info(f"✅ Upserted {owners_count} owner records")
        else:
            logger.warning("⚠️ No owners data received from HubSpot")
            reference_counts['hs_owners'] = 0
            
    except Exception as e:
        logger.error(f"❌ Failed to fetch/upsert owners: {e}")
        reference_counts['hs_owners'] = 0
    
    # ═══ FETCH DEAL STAGES (using your existing working logic) ═══
    try:
        logger.info("📊 Fetching deal stages from HubSpot...")
        
        # Import here to avoid circular imports
        import requests
        import os
        
        # Fetch deal pipelines (using your existing logic)
        api_key = os.getenv('HUBSPOT_API_KEY')
        url = "https://api.hubapi.com/crm/v3/pipelines/deals"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.error(f"❌ Failed to fetch pipelines: {response.status_code} - {response.text}")
            reference_counts['hs_deal_stage_reference'] = 0
        else:
            pipelines = response.json().get("results", [])
            
            # Transform to stage records (your existing logic with type fixes)
            records = []
            for pipeline in pipelines:
                pipeline_id = pipeline.get("id")
                pipeline_label = pipeline.get("label")
                for stage in pipeline.get("stages", []):
                    # Fix boolean conversion for is_closed
                    is_closed_raw = stage.get("metadata", {}).get("isClosed", False)
                    if isinstance(is_closed_raw, str):
                        is_closed = is_closed_raw.lower() == "true"
                    else:
                        is_closed = bool(is_closed_raw)
                    
                    records.append({
                        "pipeline_id": pipeline_id,
                        "pipeline_label": pipeline_label,
                        "stage_id": stage.get("id"),
                        "stage_label": stage.get("label"),
                        "is_closed": is_closed,  # Now properly boolean
                        "probability": float(stage.get("metadata", {}).get("probability", 0)),
                        "display_order": stage.get("displayOrder", 0)
                    })
            
            logger.info(f"📊 Fetched {len(records)} deal stages from {len(pipelines)} pipelines")
            
            # Upsert to BigQuery
            if records:
                stages_count = upsert_to_bigquery(records, "hs_deal_stage_reference", "stage_id")
                reference_counts['hs_deal_stage_reference'] = stages_count
                logger.info(f"✅ Upserted {stages_count} deal stage records")
            else:
                logger.warning("⚠️ No deal stage records to upsert")
                reference_counts['hs_deal_stage_reference'] = 0
                
    except Exception as e:
        logger.error(f"❌ Failed to fetch/upsert deal stages: {e}")
        reference_counts['hs_deal_stage_reference'] = 0
    
    logger.info(f"✅ Reference data processing complete: {reference_counts}")
    return reference_counts


def main(event=None, context=None):
    """
    Main entry point for HubSpot data ingestion with reference data
    
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
    
    logger.info("🚀 HubSpot ingest started (with reference data)")
    
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
    
    # Process each object type from schema (companies and deals)
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
            return f"Processing error for {object_type}: {e}", 500
    
    # Process reference data (owners and deal stages)
    reference_counts = {}
    if not dry_run:
        logger.info("🔄 Processing reference data...")
        try:
            reference_counts = fetch_and_process_reference_data(snapshot_id)
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
            register_snapshot_ingest(
                snapshot_id=snapshot_id,
                triggered_by=trigger_source,
                data_counts=results,
                reference_counts=reference_counts
            )
            logger.info(f"✅ Registered ingest completion in snapshot registry")
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
                logger.info(f"✅ Published completion event: {message_id}")
            else:
                logger.warning("⚠️ Failed to publish event (but ingest succeeded)")
        except Exception as e:
            logger.warning(f"⚠️ Event publishing failed: {e}")
            # Don't fail the ingest for event issues
    else:
        logger.info("🛑 DRY RUN: Skipping event publishing")
    
    # Summary
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

if __name__ == "__main__":
    # For direct execution
    print("🧪 Running ingest directly")
    result = main({"limit": 5, "dry_run": True, "log_level": "DEBUG"})
    print(f"Result: {result}")