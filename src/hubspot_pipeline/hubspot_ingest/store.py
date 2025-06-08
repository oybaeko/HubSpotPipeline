# src/hubspot_pipeline/hubspot_ingest/store.py - Updated to use smart retry logic

import logging
import os
import time
from datetime import datetime
from typing import List, Dict, Any
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Import our updated BigQuery utilities
from hubspot_pipeline.bigquery_utils import (
    get_bigquery_client,
    get_table_reference,
    insert_rows_with_smart_retry,  # Updated function name
    ensure_table_exists,
    build_schema_from_sample,
    infer_bigquery_type
)

def store_to_bigquery(rows: List[Dict[str, Any]], table_name: str, dataset: str = None) -> None:
    """
    Write rows to BigQuery with smart retry logic that expects first-attempt failures
    
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
    
    # Use BigQuery utilities for consistent client and table reference
    client = get_bigquery_client()
    full_table = get_table_reference(table_name, dataset)
    
    logger.info(f"💾 Preparing to store {len(rows)} rows into '{full_table}'")
    
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Full table reference: {full_table}")

    # Analyze data structure and build schema
    sample = rows[0]
    schema_fields = build_schema_from_sample(sample)
    
    logger.info(f"📋 Generated schema with {len(schema_fields)} fields")
    
    if logger.isEnabledFor(logging.DEBUG):
        for field in schema_fields:
            logger.debug(f"Field mapping: {field.name} -> {field.field_type}")

    # Check if table exists (simple check - let retry logic handle readiness)
    try:
        existing_table = client.get_table(full_table)
        logger.debug(f"✅ Table {full_table} exists")
        
        # Verify schema compatibility if in debug mode
        if logger.isEnabledFor(logging.DEBUG):
            existing_fields = {field.name: field.field_type for field in existing_table.schema}
            new_fields = {field.name: field.field_type for field in schema_fields}
            
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
        
        ensure_table_exists(client, full_table, schema_fields)
    
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
    
    # Insert data using smart retry logic
    insert_start = datetime.utcnow()
    logger.info(f"⬆️ Inserting {len(processed_rows)} rows into BigQuery")
    
    try:
        # Use the smart retry function that expects first-attempt failures
        insert_rows_with_smart_retry(
            client=client,
            table_ref=full_table,
            rows=processed_rows,
            operation_name=f"store {len(processed_rows)} rows to {table_name}"
        )
        
        # Success timing and metrics
        insert_time = (datetime.utcnow() - insert_start).total_seconds()
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


def upsert_to_bigquery(rows: List[Dict[str, Any]], table_name: str, id_field: str, 
                      dataset: str = None) -> int:
    """
    Upsert rows to BigQuery with smart retry logic
    CREATES TABLE if it doesn't exist, then performs upsert
    
    Args:
        rows: List of dictionaries to upsert
        table_name: Name of the BigQuery table
        id_field: Field name to use as unique key for upserts
        dataset: Dataset name (uses env var if not provided)
        
    Returns:
        Number of rows processed
    """
    logger = logging.getLogger('hubspot.store')
    
    if not rows:
        logger.info(f"📊 No data to upsert for {table_name}")
        return 0

    # Use BigQuery utilities for consistent client and table reference
    client = get_bigquery_client()
    full_table = get_table_reference(table_name, dataset)
    
    logger.info(f"🔄 Upserting {len(rows)} rows into {table_name} (key: {id_field})")
    
    try:
        # Ensure consistent data types for all rows
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
        schema_fields = []
        for key, value in sample.items():
            # Use consistent type determination
            if key.endswith('_id'):
                field_type = "STRING"  # Force all IDs to be STRING
            else:
                field_type = infer_bigquery_type(value)
            schema_fields.append(bigquery.SchemaField(key, field_type))
        
        # Check if target table exists, create if needed
        try:
            existing_table = client.get_table(full_table)
            logger.debug(f"✅ Target table {full_table} exists")
        except NotFound:
            logger.info(f"📝 Target table {full_table} not found. Creating new table")
            ensure_table_exists(client, full_table, schema_fields)
            logger.info(f"✅ Created target table {full_table}")
        
        # Create temporary table for merge operation
        import time as time_module
        project_id = os.getenv("BIGQUERY_PROJECT_ID")
        dataset_id = dataset or os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
        temp_table_id = f"{project_id}.{dataset_id}.temp_{table_name}_{int(time_module.time())}"
        
        logger.debug(f"Creating temp table: {temp_table_id}")
        
        # Load data to temp table
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=schema_fields
        )
        
        job = client.load_table_from_json(processed_rows, temp_table_id, job_config=job_config)
        job.result()
        
        logger.debug(f"Loaded {len(processed_rows)} rows to temp table")
        
        # Build field list for MERGE statement
        fields = [field.name for field in schema_fields if field.name != id_field]
        update_assignments = [f"{field} = source.{field}" for field in fields]
        insert_fields = [field.name for field in schema_fields]
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
        
        # Clean up temp table
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