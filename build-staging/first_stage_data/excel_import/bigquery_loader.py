# src/hubspot_pipeline/excel_import/bigquery_loader.py
import logging
from typing import Dict, List, Any
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from datetime import datetime

from .schema import TABLE_SCHEMAS, TABLE_NAMES

def load_to_bigquery(mapped_data: Dict[str, List[Dict]], dry_run: bool = True):
    """Load mapped Excel data to BigQuery tables (companies and deals only)"""
    logger = logging.getLogger('hubspot.excel_import')
    
    if dry_run:
        logger.info("🛑 DRY RUN MODE - No data will be written to BigQuery")
        _preview_data(mapped_data)
        return
    
    # Import here to avoid circular imports
    import os
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset_id = os.getenv('BIGQUERY_DATASET_ID')
    
    if not project_id or not dataset_id:
        raise ValueError("BIGQUERY_PROJECT_ID and BIGQUERY_DATASET_ID must be set")
    
    client = bigquery.Client(project=project_id)
    
    # Process only companies and deals
    for data_type in ['companies', 'deals']:
        records = mapped_data.get(data_type, [])
        
        if not records:
            logger.info(f"ℹ️ No {data_type} records to load")
            continue
            
        table_name = TABLE_NAMES[data_type]
        table_id = f"{project_id}.{dataset_id}.{table_name}"
        
        logger.info(f"📤 Loading {len(records)} {data_type} records to {table_id}")
        
        try:
            # Ensure table exists with correct schema
            _ensure_table_exists(client, table_id, data_type)
            
            # Insert data in batches for better performance
            batch_size = 1000
            total_records = len(records)
            
            for i in range(0, total_records, batch_size):
                batch = records[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                total_batches = (total_records + batch_size - 1) // batch_size
                
                logger.info(f"📦 Inserting batch {batch_num}/{total_batches} ({len(batch)} records)")
                
                errors = client.insert_rows_json(table_id, batch)
                if errors:
                    logger.error(f"❌ BigQuery errors for {data_type} batch {batch_num}: {errors}")
                    raise RuntimeError(f"BigQuery insertion failed: {errors}")
                
            logger.info(f"✅ Successfully loaded all {total_records} {data_type} records")
                
        except Exception as e:
            logger.error(f"❌ Failed to load {data_type} data: {e}")
            raise

def load_multiple_snapshots(snapshots_data: Dict[str, Dict[str, Any]], dry_run: bool = True):
    """
    Load multiple snapshots to BigQuery
    
    Args:
        snapshots_data: Dict of snapshot_date -> {companies: DataFrame, deals: DataFrame}
        dry_run: If True, preview only
    """
    logger = logging.getLogger('hubspot.excel_import')
    
    from .data_mapper import map_excel_to_schema
    
    total_loaded = {'companies': 0, 'deals': 0}
    
    for snapshot_date, sheet_data in snapshots_data.items():
        logger.info(f"📸 Processing snapshot: {snapshot_date}")
        
        # Use the date as snapshot_id (matches your existing pattern)
        snapshot_id = snapshot_date
        
        # Map data to schema
        mapped_data = map_excel_to_schema(sheet_data, snapshot_id)
        
        # Load to BigQuery
        load_to_bigquery(mapped_data, dry_run=dry_run)
        
        # Track totals
        for data_type in ['companies', 'deals']:
            count = len(mapped_data.get(data_type, []))
            total_loaded[data_type] += count
        
        logger.info(f"✅ Completed snapshot {snapshot_date}")
    
    # Summary
    total_records = sum(total_loaded.values())
    logger.info("=" * 60)
    logger.info("📊 MULTI-SNAPSHOT PROCESSING SUMMARY")
    logger.info("=" * 60)
    logger.info(f"📸 Snapshots processed: {len(snapshots_data)}")
    logger.info(f"🏢 Total companies: {total_loaded['companies']}")
    logger.info(f"🤝 Total deals: {total_loaded['deals']}")
    logger.info(f"📊 Total records: {total_records}")
    
    if dry_run:
        logger.info("🛑 DRY RUN - No data was written to BigQuery")
    else:
        logger.info("✅ All data successfully loaded to BigQuery")
    logger.info("=" * 60)

def _preview_data(mapped_data: Dict[str, List[Dict]]):
    """Preview data structure in dry run mode"""
    logger = logging.getLogger('hubspot.excel_import')
    
    for data_type in ['companies', 'deals']:
        records = mapped_data.get(data_type, [])
        
        if not records:
            logger.info(f"📊 {data_type.upper()}: No records")
            continue
            
        logger.info(f"📊 {data_type.upper()}: {len(records)} records")
        
        # Show sample record structure
        sample = records[0]
        logger.info(f"  Sample {data_type} record structure:")
        for key, value in sample.items():
            value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
            logger.info(f"    {key}: {type(value).__name__} = {value_preview}")
        
        # Show data quality stats
        if len(records) > 1:
            logger.info(f"  Data quality stats:")
            
            # Count non-null values for key fields
            if data_type == 'companies':
                key_fields = ['company_id', 'company_name', 'lifecycle_stage', 'hubspot_owner_id']
            elif data_type == 'deals':
                key_fields = ['deal_id', 'deal_name', 'deal_stage', 'associated_company_id']
            else:
                key_fields = list(sample.keys())[:4]
            
            for field in key_fields:
                non_null_count = sum(1 for r in records if r.get(field) is not None)
                percentage = (non_null_count / len(records)) * 100
                logger.info(f"    {field}: {non_null_count}/{len(records)} ({percentage:.1f}%) non-null")

def _ensure_table_exists(client: bigquery.Client, table_id: str, data_type: str):
    """Ensure BigQuery table exists with correct schema, create if needed"""
    logger = logging.getLogger('hubspot.excel_import')
    
    try:
        existing_table = client.get_table(table_id)
        logger.debug(f"✅ Table {table_id} exists")
        
        # Verify schema compatibility in debug mode
        if logger.isEnabledFor(logging.DEBUG):
            _verify_schema_compatibility(existing_table, data_type)
            
    except NotFound:
        logger.info(f"📝 Creating table {table_id}")
        
        # Get schema definition
        schema = _get_table_schema(data_type)
        if not schema:
            raise ValueError(f"No schema definition for data type: {data_type}")
        
        # Create table
        table = bigquery.Table(table_id, schema=schema)
        created_table = client.create_table(table)
        
        logger.info(f"✅ Created table {table_id} with {len(schema)} columns")
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Schema: {[(f.name, f.field_type) for f in schema]}")

def _get_table_schema(data_type: str) -> List[bigquery.SchemaField]:
    """Get BigQuery schema for data type from our separate schema file"""
    if data_type not in TABLE_SCHEMAS:
        return None
    
    schema_definition = TABLE_SCHEMAS[data_type]
    return [bigquery.SchemaField(name, field_type) for name, field_type in schema_definition]

def _verify_schema_compatibility(existing_table: bigquery.Table, data_type: str):
    """Verify that existing table schema is compatible with our data"""
    logger = logging.getLogger('hubspot.excel_import')
    
    expected_schema = _get_table_schema(data_type)
    if not expected_schema:
        return
    
    existing_fields = {field.name: field.field_type for field in existing_table.schema}
    expected_fields = {field.name: field.field_type for field in expected_schema}
    
    # Check for missing or incompatible fields
    issues = []
    
    for field_name, expected_type in expected_fields.items():
        if field_name not in existing_fields:
            issues.append(f"Missing field: {field_name} ({expected_type})")
        elif existing_fields[field_name] != expected_type:
            issues.append(f"Type mismatch: {field_name} is {existing_fields[field_name]}, expected {expected_type}")
    
    if issues:
        logger.warning(f"⚠️ Schema compatibility issues for {data_type}:")
        for issue in issues:
            logger.warning(f"  • {issue}")
        logger.warning("Consider updating your table schema or recreating the table")
    else:
        logger.debug(f"✅ Schema is compatible for {data_type}")