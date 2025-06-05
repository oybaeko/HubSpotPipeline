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

    #