# src/hubspot_pipeline/scoring/processor.py

import logging
import time
import os
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

def process_snapshot(snapshot_id: str):
    """
    Master function: processes a snapshot by running unit-score and score-history jobs in sequence.
    
    Args:
        snapshot_id: The identifier for the snapshot to process
        
    Returns:
        dict: Processing results with record counts and timing
    """
    logger = logging.getLogger('hubspot.scoring.processor')
    logger.info(f"🔄 Starting full processing for snapshot: {snapshot_id}")
    
    start_time = datetime.utcnow()
    
    try:
        # Step 1: Process unit scores
        unit_results = process_unit_score_for_snapshot(snapshot_id)
        
        # Step 2: Process score history
        history_results = process_score_history_for_snapshot(snapshot_id)
        
        total_time = (datetime.utcnow() - start_time).total_seconds()
        
        results = {
            'status': 'success',
            'snapshot_id': snapshot_id,
            'unit_records': unit_results.get('records', 0),
            'history_records': history_results.get('records', 0),
            'processing_time_seconds': total_time
        }
        
        logger.info(f"✅ Completed full processing for snapshot: {snapshot_id}")
        logger.info(f"📊 Results: {unit_results.get('records', 0)} unit records, {history_results.get('records', 0)} history records")
        logger.info(f"⏱️ Total time: {total_time:.2f}s")
        
        return results
        
    except Exception as e:
        total_time = (datetime.utcnow() - start_time).total_seconds()
        logger.error(f"❌ Error during process_snapshot({snapshot_id}): {e}", exc_info=True)
        
        return {
            'status': 'error',
            'snapshot_id': snapshot_id,
            'error': str(e),
            'processing_time_seconds': total_time
        }

def process_unit_score_for_snapshot(snapshot_id: str):
    """
    Processes a given snapshot by:
      1) Querying and transforming company + deal data from BigQuery,
      2) Mapping each record to its combined_stage and score,
      3) Appending results to the pipeline_units table.
    
    Args:
        snapshot_id (str): The identifier for the snapshot to process.
        
    Returns:
        dict: Results with record count and timing
    """
    logger = logging.getLogger('hubspot.scoring.processor')
    logger.info(f"🔹 Processing unit scores for snapshot: {snapshot_id}")

    start_time = datetime.utcnow()
    
    client = bigquery.Client()
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset_id = os.getenv('BIGQUERY_DATASET_ID')

    # Build the SQL query
    query = f"""
    -- Step 1: Filter companies for this snapshot
    WITH companies AS (
      SELECT 
        LOWER(lifecycle_stage) AS lifecycle_stage,
        LOWER(REPLACE(lead_status, " ", "_")) AS lead_status,
        company_id,
        hubspot_owner_id,
        snapshot_id
      FROM `{project_id}.{dataset_id}.hs_companies`
      WHERE snapshot_id = @snapshot_id
    ),
    -- Step 2: Filter open deals for this snapshot
    deals AS (
      SELECT 
        LOWER(deal_stage) AS deal_stage,
        deal_id,
        associated_company_id,
        snapshot_id
      FROM `{project_id}.{dataset_id}.hs_deals`
      WHERE snapshot_id = @snapshot_id
        AND deal_stage NOT IN (
            SELECT stage_id
            FROM `{project_id}.{dataset_id}.hs_deal_stage_reference`
            WHERE is_closed = TRUE
        )
    ),
    -- Step 3: Left-join companies with their deals (if any)
    joined AS (
      SELECT
        c.snapshot_id,
        CURRENT_TIMESTAMP() AS snapshot_timestamp,
        c.company_id,
        d.deal_id,
        c.hubspot_owner_id AS owner_id,
        c.lifecycle_stage,
        c.lead_status,
        d.deal_stage,
        CASE
          WHEN c.lifecycle_stage = 'lead' THEN CONCAT(c.lifecycle_stage, '/', IFNULL(c.lead_status, ''))
          WHEN c.lifecycle_stage = 'opportunity' THEN CONCAT(c.lifecycle_stage, '/', IFNULL(d.deal_stage, 'missing'))
          WHEN c.lifecycle_stage IN ('salesqualifiedlead', 'closed-won', 'disqualified') THEN c.lifecycle_stage
          ELSE 'unmapped'
        END AS combined_stage,
        CASE 
          WHEN d.deal_id IS NULL THEN 'company' 
          ELSE 'deal' 
        END AS stage_source
      FROM companies c
      LEFT JOIN deals d ON d.associated_company_id = c.company_id
    ),
    -- Step 4: Join the "scoring metadata" from stage_mapping
    scored AS (
      SELECT
        j.*,
        sm.stage_level,
        sm.adjusted_score
      FROM joined j
      LEFT JOIN `{project_id}.{dataset_id}.hs_stage_mapping` sm
        ON sm.combined_stage = j.combined_stage
    )
    SELECT * FROM scored
    """

    logger.info("🔹 Submitting BigQuery job for unit scores...")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
        ],
        destination=f"{project_id}.{dataset_id}.hs_pipeline_units_snapshot",
        write_disposition="WRITE_APPEND",
    )

    try:
        job = client.query(query, job_config=job_config)
        logger.info(f"   • BigQuery job ID (unit score): {job.job_id}")
        job.result()  # Wait for completion
        
        # Get number of rows processed
        rows_processed = job.num_dml_affected_rows or 0
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        logger.info(f"✅ Unit-score job completed: {rows_processed} records in {processing_time:.2f}s")
        
        return {
            'status': 'success',
            'records': rows_processed,
            'processing_time_seconds': processing_time
        }
        
    except GoogleAPIError as e:
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        logger.error(f"❌ BigQuery unit-score job failed: {e}", exc_info=True)
        raise RuntimeError(f"Unit score processing failed: {e}")

def process_score_history_for_snapshot(snapshot_id: str):
    """
    Processes and appends score history data for a given snapshot to the BigQuery score history table.

    Steps:
      1) Sleep briefly to allow any streaming buffer to settle,
      2) Aggregate pipeline_units rows by owner & combined_stage,
      3) Append results to the hs_pipeline_score_history table.

    Args:
        snapshot_id (str): The unique identifier for the snapshot to process.
        
    Returns:
        dict: Results with record count and timing
    """
    logger = logging.getLogger('hubspot.scoring.processor')
    logger.info(f"🔹 Processing score history for snapshot: {snapshot_id}")

    start_time = datetime.utcnow()
    
    # Brief wait to ensure data availability
    wait_secs = 5
    logger.info(f"   • Waiting {wait_secs}s to ensure pipeline_units data is available...")
    time.sleep(wait_secs)

    client = bigquery.Client()
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset_id = os.getenv('BIGQUERY_DATASET_ID')

    # Build the aggregation SQL
    query = f"""
    SELECT
      snapshot_id,
      owner_id,
      combined_stage,
      COUNT(DISTINCT company_id) AS num_companies,
      SUM(adjusted_score) AS total_score,
      MAX(snapshot_timestamp) AS snapshot_timestamp
    FROM `{project_id}.{dataset_id}.hs_pipeline_units_snapshot`
    WHERE snapshot_id = @snapshot_id
    GROUP BY snapshot_id, owner_id, combined_stage
    """

    logger.info("🔹 Submitting BigQuery job for score history...")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
        ],
        destination=f"{project_id}.{dataset_id}.hs_pipeline_score_history",
        write_disposition="WRITE_APPEND",
    )

    try:
        job = client.query(query, job_config=job_config)
        logger.info(f"   • BigQuery job ID (score history): {job.job_id}")
        job.result()  # Wait for completion
        
        # Get number of rows processed
        rows_processed = job.num_dml_affected_rows or 0
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        logger.info(f"✅ Score-history job completed: {rows_processed} records in {processing_time:.2f}s")
        
        return {
            'status': 'success',
            'records': rows_processed,
            'processing_time_seconds': processing_time
        }
        
    except GoogleAPIError as e:
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        logger.error(f"❌ BigQuery score-history job failed: {e}", exc_info=True)
        raise RuntimeError(f"Score history processing failed: {e}")