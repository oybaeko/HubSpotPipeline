# src/hubspot_pipeline/hubspot_ingest/store.py

import logging
import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime

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
    client = bigquery.Client()
    
    # Determine dataset
    dataset = dataset or os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
    project_id = client.project
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

    # Check if table exists, create if needed
    table_exists = False
    try:
        existing_table = client.get_table(full_table)
        table_exists = True
        logger.debug(f"✅ Table {full_table} exists")
        
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
        logger.info(f"📝 Table {full_table} not found. Creating new table")
        try:
            table = bigquery.Table(full_table, schema=schema)
            client.create_table(table)
            logger.info(f"✅ Created table {full_table}")
            
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
            # Clean and validate row data
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


def _bq_type(value):
    """
    Determine BigQuery field type from Python value
    
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