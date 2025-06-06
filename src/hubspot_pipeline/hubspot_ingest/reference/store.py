# src/hubspot_pipeline/hubspot_ingest/reference/store.py

import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Tuple
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def ensure_table_exists(table_name: str, schema: List[Tuple[str, str]], dataset: str = None) -> None:
    """
    Ensure BigQuery table exists with correct schema, create if needed.
    
    Args:
        table_name: Name of the BigQuery table
        schema: List of (column_name, type) tuples
        dataset: Dataset name (uses env var if not provided)
    """
    logger = logging.getLogger('hubspot.reference')
    
    client = bigquery.Client()
    dataset = dataset or os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
    project_id = client.project
    full_table = f"{project_id}.{dataset}.{table_name}"
    
    try:
        existing_table = client.get_table(full_table)
        logger.debug(f"✅ Table {full_table} exists")
    except NotFound:
        logger.info(f"📝 Table {full_table} not found. Creating with schema...")
        
        # Convert schema tuples to BigQuery schema fields
        bq_schema = []
        for col_name, col_type in schema:
            bq_schema.append(bigquery.SchemaField(col_name, col_type))
        
        try:
            table = bigquery.Table(full_table, schema=bq_schema)
            client.create_table(table)
            logger.info(f"✅ Created table {full_table} with {len(bq_schema)} columns")
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Schema: {[(f.name, f.field_type) for f in bq_schema]}")
        except Exception as e:
            logger.error(f"❌ Failed to create table {full_table}: {e}")
            raise RuntimeError(f"Failed to create table: {e}")


def replace_reference_table(rows: List[Dict[str, Any]], table_name: str, 
                          schema: List[Tuple[str, str]], dataset: str = None) -> int:
    """
    Replace all data in a reference table (truncate + insert).
    
    Args:
        rows: List of dictionaries to insert
        table_name: Name of the BigQuery table
        schema: List of (column_name, type) tuples for table schema
        dataset: Dataset name (uses env var if not provided)
        
    Returns:
        Number of rows inserted
    """
    logger = logging.getLogger('hubspot.reference')
    
    if not rows:
        logger.info(f"📊 No data to replace for {table_name}")
        return 0
    
    client = bigquery.Client()
    dataset = dataset or os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
    project_id = client.project
    full_table = f"{project_id}.{dataset}.{table_name}"
    
    logger.info(f"🔄 Replacing {len(rows)} rows in {table_name}")
    
    try:
        # Step 1: Ensure table exists
        ensure_table_exists(table_name, schema, dataset)
        
        # Step 2: Truncate existing data
        logger.debug(f"🗑️ Truncating table {full_table}")
        truncate_query = f"TRUNCATE TABLE `{full_table}`"
        client.query(truncate_query).result()
        logger.debug("✅ Table truncated")
        
        # Step 3: Insert new data
        logger.debug(f"⬆️ Inserting {len(rows)} rows")
        errors = client.insert_rows_json(full_table, rows)
        
        if errors:
            logger.error(f"❌ BigQuery insertion errors: {errors}")
            raise RuntimeError(f"BigQuery insertion failed: {errors}")
        
        logger.info(f"✅ Successfully replaced {len(rows)} rows in {table_name}")
        return len(rows)
        
    except Exception as e:
        logger.error(f"❌ Failed to replace data in {table_name}: {e}")
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Target table: {full_table}")
            logger.debug(f"Rows to insert: {len(rows)}")
            if rows:
                logger.debug(f"Sample row: {rows[0]}")
        
        raise RuntimeError(f"Reference table replacement failed: {e}")


def replace_owners(owners_data: List[Dict[str, Any]]) -> int:
    """
    Replace all owners data in hs_owners table.
    
    Args:
        owners_data: List of owner dictionaries
        
    Returns:
        Number of owners inserted
    """
    from .schemas import OWNERS_SCHEMA
    return replace_reference_table(owners_data, "hs_owners", OWNERS_SCHEMA)


def replace_deal_stages(stages_data: List[Dict[str, Any]]) -> int:
    """
    Replace all deal stages data in hs_deal_stage_reference table.
    
    Args:
        stages_data: List of deal stage dictionaries
        
    Returns:
        Number of stages inserted
    """
    from .schemas import DEAL_STAGES_SCHEMA
    return replace_reference_table(stages_data, "hs_deal_stage_reference", DEAL_STAGES_SCHEMA)