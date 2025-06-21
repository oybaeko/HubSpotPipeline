# src/hubspot_pipeline/hubspot_ingest/reference/store.py - Updated with smart retry

import logging
import os
from typing import List, Dict, Any, Tuple
from google.cloud import bigquery
from hubspot_pipeline.schema import SCHEMA_OWNERS, SCHEMA_DEAL_STAGE_REFERENCE
# Import our updated BigQuery utilities
from hubspot_pipeline.bigquery_utils import (
    get_bigquery_client,
    get_table_reference,
    truncate_and_insert_with_smart_retry,  # Updated function name
    ensure_table_exists
)

def ensure_table_exists_with_schema(table_name: str, schema: List[Tuple[str, str]], dataset: str = None) -> None:
    """
    Ensure BigQuery table exists with correct schema, create if needed.
    Simple existence check - let smart retry handle timing issues.
    
    Args:
        table_name: Name of the BigQuery table
        schema: List of (column_name, type) tuples
        dataset: Dataset name (uses env var if not provided)
    """
    logger = logging.getLogger('hubspot.reference')
    
    client = get_bigquery_client()
    full_table = get_table_reference(table_name, dataset)
    
    try:
        existing_table = client.get_table(full_table)
        logger.debug(f"✅ Table {full_table} exists")
    except Exception:  # NotFound or other errors
        logger.info(f"📝 Table {full_table} not found. Creating with schema...")
        
        # Convert schema tuples to BigQuery schema fields
        bq_schema = []
        for col_name, col_type in schema:
            bq_schema.append(bigquery.SchemaField(col_name, col_type))
        
        # Use utilities to ensure table exists (no complex readiness verification)
        ensure_table_exists(client, full_table, bq_schema)
        
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Schema: {[(f.name, f.field_type) for f in bq_schema]}")


def replace_reference_table(rows: List[Dict[str, Any]], table_name: str, 
                          schema: List[Tuple[str, str]], dataset: str = None) -> int:
    """
    Replace all data in a reference table (truncate + insert) with smart retry logic.
    
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
    
    # Use BigQuery utilities for consistent client and table reference
    client = get_bigquery_client()
    full_table = get_table_reference(table_name, dataset)
    
    logger.info(f"🔄 Replacing {len(rows)} rows in {table_name}")
    
    try:
        # Step 1: Ensure table exists (simple check)
        ensure_table_exists_with_schema(table_name, schema, dataset)
        
        # Step 2: Truncate and insert with smart retry using utilities
        rows_inserted = truncate_and_insert_with_smart_retry(
            client=client,
            table_ref=full_table,
            rows=rows,
            operation_name=f"replace {len(rows)} rows in {table_name}"
        )
        
        return rows_inserted
        
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
    return replace_reference_table(owners_data, "hs_owners", SCHEMA_OWNERS)


def replace_deal_stages(stages_data: List[Dict[str, Any]]) -> int:
    """
    Replace all deal stages data in hs_deal_stage_reference table.
    
    Args:
        stages_data: List of deal stage dictionaries
        
    Returns:
        Number of stages inserted
    """
    return replace_reference_table(stages_data, "hs_deal_stage_reference", SCHEMA_DEAL_STAGE_REFERENCE)