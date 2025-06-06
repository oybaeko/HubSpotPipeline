# src/hubspot_pipeline/hubspot_ingest/store.py

import logging
import os
import time
import json
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Lazy import for Pub/Sub to avoid import errors in local testing
def _get_pubsub_client():
    """Lazy import of Pub/Sub client"""
    try:
        from google.cloud import pubsub_v1
        return pubsub_v1.PublisherClient()
    except ImportError as e:
        logging.error("❌ google-cloud-pubsub not installed. Run: pip install google-cloud-pubsub")
        raise ImportError("Missing dependency: google-cloud-pubsub") from e

def store_to_bigquery(rows, table_name, dataset=None):
    """
    Write rows to BigQuery with comprehensive logging
    
    Args:
        rows: List of dictionaries to insert
        table_name: Name of the BigQuery table
        dataset: Dataset name (uses env var if not provided)
    """
    logger = logging.getLogger('hubspot.store')
    
    if not rows:
        logger.info(f"📊 No data to store for {table_name}")
        return

    start_time = datetime.utcnow()
    
    # Use explicit project ID to avoid client project ID mismatch
    project_id = os.getenv("BIGQUERY_PROJECT_ID")
    if not project_id:
        logger.error("BIGQUERY_PROJECT_ID environment variable not set")
        raise RuntimeError("BIGQUERY_PROJECT_ID environment variable not set")
    
    # Create client with explicit project ID
    client = bigquery.Client(project=project_id)
    
    # Determine dataset
    dataset = dataset or os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
    full_table = f"{project_id}.{dataset}.{table_name}"
    
    logger.info(f"💾 Preparing to store {len(rows)} rows into '{full_table}'")
    
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"BigQuery project: {project_id}")
        logger.debug(f"Dataset: {dataset}")
        logger.debug(f"Table: {table_name}")
        logger.debug(f"Full table reference: {full_table}")

    # Analyze data structure
    sample = rows[0]
    schema_fields = []
    
    logger.debug("🔍 Analyzing data structure for schema generation")
    for key, value in sample.items():
        field_type = _bq_type(value)
        schema_fields.append(bigquery.SchemaField(key, field_type))
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Field mapping: {key} -> {field_type} (sample: {type(value).__name__})")
    
    schema = schema_fields
    logger.info(f"📋 Generated schema with {len(schema)} fields")

    # Check if table exists (should exist due to pre-flight check, but verify)
    try:
        existing_table = client.get_table(full_table)
        logger.debug(f"✅ Table {full_table} exists and ready")
        
        # Verify schema compatibility if in debug mode
        if logger.isEnabledFor(logging.DEBUG):
            existing_fields = {field.name: field.field_type for field in existing_table.schema}
            new_fields = {field.name: field.field_type for field in schema}
            
            schema_changes = []
            for field_name, field_type in new_fields.items():
                if field_name in existing_fields:
                    if existing_fields[field_name] != field_type:
                        schema_changes.append(f"{field_name}: {existing_fields[field_name]} -> {field_type}")
                else:
                    schema_changes.append(f"{field_name}: NEW ({field_type})")
            
            if schema_changes:
                logger.debug(f"Schema differences detected: {schema_changes}")
            else:
                logger.debug("Schema matches existing table")
                
    except NotFound:
        # This shouldn't happen due to pre-flight check, but handle gracefully
        logger.warning(f"⚠️ Table {full_table} not found despite pre-flight check")
        logger.info(f"📝 Creating table {full_table}")
        try:
            table = bigquery.Table(full_table, schema=schema)
            client.create_table(table)
            logger.info(f"✅ Created table {full_table}")
            
            # Verify table is ready
            from .table_checker import verify_table_readiness
            if not verify_table_readiness(table_name):
                raise RuntimeError(f"Table {table_name} not ready after creation")
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Created table with schema: {[f.name + ':' + f.field_type for f in schema]}")
        except Exception as e:
            logger.error(f"❌ Failed to create table {full_table}: {e}")
            raise RuntimeError(f"Failed to create table: {e}")
    
    # Prepare data for insertion
    prep_start = datetime.utcnow()
    processed_rows = []
    
    logger.debug(f"🔄 Processing {len(rows)} rows for BigQuery insertion")
    
    for i, row in enumerate(rows):
        try:
            # Clean and validate row data with consistent type conversion
            clean_row = {}
            for key, value in row.items():
                # Handle None values and type conversions
                if value is None:
                    clean_row[key] = None
                elif isinstance(value, (list, dict)):
                    # Convert complex types to strings for BigQuery
                    clean_row[key] = str(value)
                    if logger.isEnabledFor(logging.DEBUG) and i == 0:
                        logger.debug(f"Converted complex type {key}: {type(value).__name__} -> STRING")
                else:
                    # Ensure consistent string conversion for ID fields
                    if key.endswith('_id') and value is not None:
                        clean_row[key] = str(value)  # Force string conversion for ID fields
                    else:
                        clean_row[key] = value
            
            processed_rows.append(clean_row)
            
        except Exception as e:
            logger.warning(f"⚠️ Error processing row {i}: {e}")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Problematic row data: {row}")
            continue  # Skip problematic rows
    
    prep_time = (datetime.utcnow() - prep_start).total_seconds()
    logger.debug(f"📊 Data preparation completed in {prep_time:.2f}s")
    
    if not processed_rows:
        logger.warning(f"⚠️ No valid rows to insert after processing")
        return
    
    # Insert data
    insert_start = datetime.utcnow()
    logger.info(f"⬆️ Inserting {len(processed_rows)} rows into BigQuery")
    
    try:
        errors = client.insert_rows_json(full_table, processed_rows)
        insert_time = (datetime.utcnow() - insert_start).total_seconds()
        
        if errors:
            logger.error(f"❌ BigQuery insertion errors: {errors}")
            
            # Log detailed error information
            if logger.isEnabledFor(logging.DEBUG):
                for i, error in enumerate(errors[:5]):  # Show first 5 errors
                    logger.debug(f"Error {i+1}: {error}")
                if len(errors) > 5:
                    logger.debug(f"... and {len(errors)-5} more errors")
            
            raise RuntimeError(f"BigQuery insertion failed with {len(errors)} errors: {errors[:3]}")
        
        # Success logging
        total_time = (datetime.utcnow() - start_time).total_seconds()
        
        logger.info(f"✅ Successfully inserted {len(processed_rows)} rows into {full_table}")
        logger.info(f"⏱️ Total time: {total_time:.2f}s (prep: {prep_time:.2f}s, insert: {insert_time:.2f}s)")
        
        if insert_time > 0:
            logger.info(f"📈 Insert rate: {len(processed_rows)/insert_time:.1f} rows/second")
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"BigQuery job completed successfully")
            logger.debug(f"Effective rows processed: {len(processed_rows)}/{len(rows)}")
            if len(processed_rows) != len(rows):
                logger.debug(f"Skipped {len(rows) - len(processed_rows)} problematic rows")
        
    except Exception as e:
        logger.error(f"❌ Failed to insert rows into {full_table}: {e}")
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Insert operation details:")
            logger.debug(f"  Target table: {full_table}")
            logger.debug(f"  Rows to insert: {len(processed_rows)}")
            logger.debug(f"  Sample row keys: {list(processed_rows[0].keys()) if processed_rows else 'None'}")
        
        raise RuntimeError(f"BigQuery insertion failed: {e}")


def upsert_to_bigquery(rows, table_name, id_field, dataset=None):
    """
    Upsert rows to BigQuery - update existing, add new, keep deleted
    CREATES TABLE if it doesn't exist, then performs upsert
    
    Args:
        rows: List of dictionaries to upsert
        table_name: Name of the BigQuery table
        id_field: Field name to use as unique key for upserts
        dataset: Dataset name (uses env var if not provided)
    """
    logger = logging.getLogger('hubspot.store')
    
    if not rows:
        logger.info(f"📊 No data to upsert for {table_name}")
        return 0

    client = bigquery.Client()
    dataset = dataset or os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
    # Use explicit project ID to avoid client project ID mismatch
    project_id = os.getenv("BIGQUERY_PROJECT_ID") or client.project
    full_table = f"{project_id}.{dataset}.{table_name}"
    
    logger.info(f"🔄 Upserting {len(rows)} rows into {table_name} (key: {id_field})")
    
    try:
        # CRITICAL FIX: Ensure consistent data types for all rows
        processed_rows = []
        for row in rows:
            processed_row = {}
            for key, value in row.items():
                if value is None:
                    processed_row[key] = None
                elif key.endswith('_id') and value is not None:
                    # Force all ID fields to be strings for consistency
                    processed_row[key] = str(value)
                elif isinstance(value, bool):
                    # Ensure booleans stay as booleans
                    processed_row[key] = value
                elif isinstance(value, (int, float)) and not key.endswith('_id'):
                    # Keep numeric fields as numbers (except IDs)
                    processed_row[key] = value
                else:
                    # Convert everything else to string
                    processed_row[key] = str(value) if value is not None else None
            processed_rows.append(processed_row)
        
        # Get schema from sample row (after processing)
        sample = processed_rows[0]
        schema = []
        for key, value in sample.items():
            # Use consistent type determination
            if key.endswith('_id'):
                field_type = "STRING"  # Force all IDs to be STRING
            else:
                field_type = _bq_type(value)
            schema.append(bigquery.SchemaField(key, field_type))
        
        # Check if target table exists, create if needed
        try:
            existing_table = client.get_table(full_table)
            logger.debug(f"✅ Target table {full_table} exists")
        except NotFound:
            logger.info(f"📝 Target table {full_table} not found. Creating new table")
            try:
                table = bigquery.Table(full_table, schema=schema)
                client.create_table(table)
                logger.info(f"✅ Created target table {full_table}")
                
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Created table with schema: {[f.name + ':' + f.field_type for f in schema]}")
            except Exception as e:
                logger.error(f"❌ Failed to create target table {full_table}: {e}")
                raise RuntimeError(f"Failed to create target table: {e}")
        
        # Create temporary table
        temp_table_id = f"{project_id}.{dataset}.temp_{table_name}_{int(time.time())}"
        
        logger.debug(f"Creating temp table: {temp_table_id}")
        
        # Load data to temp table
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=schema
        )
        
        job = client.load_table_from_json(processed_rows, temp_table_id, job_config=job_config)
        job.result()
        
        logger.debug(f"Loaded {len(processed_rows)} rows to temp table")
        
        # Build field list for MERGE statement
        fields = [field.name for field in schema if field.name != id_field]
        update_assignments = [f"{field} = source.{field}" for field in fields]
        insert_fields = [field.name for field in schema]
        insert_values = [f"source.{field}" for field in insert_fields]
        
        # MERGE statement (upsert) with explicit CAST to ensure type consistency
        merge_query = f"""
        MERGE `{full_table}` AS target
        USING `{temp_table_id}` AS source
        ON CAST(target.{id_field} AS STRING) = CAST(source.{id_field} AS STRING)
        WHEN MATCHED THEN
          UPDATE SET {', '.join(update_assignments)}
        WHEN NOT MATCHED THEN
          INSERT ({', '.join(insert_fields)})
          VALUES ({', '.join(insert_values)})
        """
        
        logger.debug(f"Executing MERGE for {table_name} with type casting")
        merge_job = client.query(merge_query)
        merge_job.result()
        
        # Clean up temp table with retry logic
        try:
            client.delete_table(temp_table_id, not_found_ok=True)
            logger.debug(f"✅ Cleaned up temp table {temp_table_id}")
        except Exception as cleanup_error:
            logger.warning(f"⚠️ Failed to cleanup temp table {temp_table_id}: {cleanup_error}")
            # Don't fail the whole operation for cleanup issues
        
        logger.info(f"✅ Upserted {len(processed_rows)} rows into {table_name}")
        return len(processed_rows)
        
    except Exception as e:
        logger.error(f"❌ Failed to upsert {table_name}: {e}")
        
        # Attempt cleanup even on failure
        try:
            if 'temp_table_id' in locals():
                client.delete_table(temp_table_id, not_found_ok=True)
                logger.debug(f"🧹 Emergency cleanup of temp table {temp_table_id}")
        except Exception as cleanup_error:
            logger.warning(f"⚠️ Failed emergency cleanup of temp table: {cleanup_error}")
        
        if 'processed_rows' in locals() and processed_rows:
            logger.debug(f"Sample processed row: {processed_rows[0]}")
        raise


def publish_snapshot_completed_event(snapshot_id: str, data_counts: dict, reference_counts: dict):
    """
    Publish snapshot completion event to Pub/Sub
    
    Args:
        snapshot_id: The snapshot identifier
        data_counts: Dict of table_name -> record_count for snapshot data
        reference_counts: Dict of table_name -> record_count for reference data
    """
    logger = logging.getLogger('hubspot.events')
    
    try:
        client = bigquery.Client()
        project_id = client.project
        
        # Lazy import Pub/Sub
        publisher = _get_pubsub_client()
        topic_path = publisher.topic_path(project_id, "hubspot-events")
        
        event_data = {
            'snapshot_id': snapshot_id,
            'timestamp': datetime.utcnow().isoformat() + "Z",
            'data_tables': data_counts,
            'reference_tables': reference_counts,
            'metadata': {
                'triggered_by': 'ingest_function',
                'environment': os.getenv('ENVIRONMENT', 'production')
            }
        }
        
        event = {
            "type": "hubspot.snapshot.completed",
            "version": "1.0",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "data": event_data
        }
        
        message_json = json.dumps(event)
        future = publisher.publish(topic_path, message_json.encode('utf-8'))
        message_id = future.result()
        
        logger.info(f"📤 Published snapshot.completed event (message ID: {message_id})")
        return message_id
        
    except ImportError as e:
        logger.warning(f"⚠️ Pub/Sub not available for local testing: {e}")
        logger.info("📤 Would have published event (local mode)")
        return "local_mode_no_pubsub"
    except Exception as e:
        # Check if it's a permission error for local testing
        if "403" in str(e) or "not authorized" in str(e).lower():
            logger.warning(f"⚠️ Pub/Sub permission error (expected for local testing): {e}")
            logger.info("📤 Would have published event (no permissions for local testing)")
            return "local_mode_no_permissions"
        else:
            logger.error(f"❌ Failed to publish event: {e}")
            # Don't fail the whole ingest if event publishing fails
            return None


def register_snapshot_ingest(snapshot_id: str, triggered_by: str, data_counts: dict, reference_counts: dict):
    """
    Register snapshot ingest completion in hs_snapshot_registry
    
    Args:
        snapshot_id: The snapshot identifier
        triggered_by: Who/what triggered this ingest
        data_counts: Dict of data table counts
        reference_counts: Dict of reference table counts
    """
    logger = logging.getLogger('hubspot.registry')
    
    try:
        client = bigquery.Client()
        dataset = os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
        # Use explicit project ID to avoid client project ID mismatch
        project_id = os.getenv("BIGQUERY_PROJECT_ID") or client.project
        table_ref = f"{project_id}.{dataset}.hs_snapshot_registry"
        
        # Create comprehensive notes
        total_data = sum(data_counts.values())
        total_reference = sum(reference_counts.values())
        notes = f"Ingest: {total_data} data records, {total_reference} reference records. Tables: {list(data_counts.keys())}"
        
        row = {
            "snapshot_id": snapshot_id,
            "snapshot_timestamp": datetime.utcnow().isoformat(),
            "triggered_by": triggered_by,
            "status": "ingest_completed",  # Specific status for ingest
            "notes": notes,
        }
        
        errors = client.insert_rows_json(table_ref, [row])
        if errors:
            logger.error(f"❌ Failed to register snapshot ingest: {errors}")
        else:
            logger.info(f"✅ Registered ingest completion for snapshot {snapshot_id}")
            
    except Exception as e:
        logger.error(f"❌ Exception while registering snapshot ingest: {e}")
        # Don't fail the whole ingest for registry issues


def update_snapshot_registry_scoring(snapshot_id: str, status: str = "completed", notes: str = None):
    """
    Update snapshot registry when scoring completes
    This would be called from the scoring function
    
    Args:
        snapshot_id: The snapshot identifier
        status: "scoring_completed", "scoring_failed", etc.
        notes: Additional notes about scoring
    """
    logger = logging.getLogger('hubspot.registry')
    
    try:
        client = bigquery.Client()
        dataset = os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
        # Use explicit project ID to avoid client project ID mismatch
        project_id = os.getenv("BIGQUERY_PROJECT_ID") or client.project
        
        # Update the existing record
        update_query = f"""
        UPDATE `{project_id}.{dataset}.hs_snapshot_registry`
        SET 
            status = @new_status,
            notes = CONCAT(IFNULL(notes, ''), ' | Scoring: ', @new_notes)
        WHERE snapshot_id = @snapshot_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id),
                bigquery.ScalarQueryParameter("new_status", "STRING", f"ingest_and_{status}"),
                bigquery.ScalarQueryParameter("new_notes", "STRING", notes or "completed")
            ]
        )
        
        query_job = client.query(update_query, job_config=job_config)
        query_job.result()
        
        logger.info(f"✅ Updated registry for snapshot {snapshot_id} with scoring status")
        
    except Exception as e:
        logger.error(f"❌ Failed to update snapshot registry: {e}")


def _bq_type(value):
    """
    Determine BigQuery field type from Python value
    UPDATED: Improved type determination for consistency
    
    Args:
        value: Python value to analyze
        
    Returns:
        str: BigQuery field type
    """
    logger = logging.getLogger('hubspot.store')
    
    if value is None:
        return "STRING"  # Default for null values
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, (list, dict)):
        logger.debug(f"Complex type {type(value).__name__} will be stored as STRING")
        return "STRING"
    else:
        return "STRING"