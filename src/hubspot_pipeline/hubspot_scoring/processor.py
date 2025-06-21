# src/hubspot_pipeline/hubspot_scoring/processor.py

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

    # FIXED: Updated SQL to match actual table structure and filter only open deals
    query = f"""
    -- Step 1: Filter companies for this snapshot
    WITH companies AS (
      SELECT 
        LOWER(lifecycle_stage) AS lifecycle_stage,
        LOWER(COALESCE(REPLACE(lead_status, " ", "_"), "")) AS lead_status,
        company_id,
        hubspot_owner_id,
        snapshot_id
      FROM `{project_id}.{dataset_id}.hs_companies`
      WHERE snapshot_id = @snapshot_id
    ),
    -- Step 2: Filter open deals for this snapshot and join with companies
    deals AS (
      SELECT 
        d.deal_id,
        d.associated_company_id,
        d.snapshot_id,
        LOWER(d.deal_stage) AS deal_stage,
        -- Check if deal is closed using reference table
        COALESCE(ref.is_closed, FALSE) as is_deal_closed
      FROM `{project_id}.{dataset_id}.hs_deals` d
      LEFT JOIN `{project_id}.{dataset_id}.hs_deal_stage_reference` ref 
        ON d.deal_stage = ref.stage_id
      WHERE d.snapshot_id = @snapshot_id
        AND COALESCE(ref.is_closed, FALSE) = FALSE  -- Only open deals
    ),
    -- Step 3: Left-join companies with their open deals (if any)
    joined AS (
      SELECT
        c.company_id,
        d.deal_id,
        c.hubspot_owner_id AS owner_id,
        c.lifecycle_stage,
        c.lead_status,
        d.deal_stage,
        -- Build combined_stage logic
        CASE
          WHEN c.lifecycle_stage = 'lead' THEN 
            CONCAT('lead/', COALESCE(NULLIF(c.lead_status, ''), 'unknown'))
          WHEN c.lifecycle_stage = 'opportunity' THEN 
            CONCAT('opportunity/', COALESCE(NULLIF(d.deal_stage, ''), 'missing'))
          WHEN c.lifecycle_stage IN ('salesqualifiedlead', 'sales qualified lead') THEN 'salesqualifiedlead'
          WHEN c.lifecycle_stage = 'closed-won' THEN 'closed-won'
          WHEN c.lifecycle_stage = 'disqualified' THEN 'disqualified'
          ELSE 'unmapped'
        END AS combined_stage,
        CASE 
          WHEN d.deal_id IS NULL THEN 'company' 
          ELSE 'deal' 
        END AS stage_source,
        c.snapshot_id,
        CURRENT_TIMESTAMP() AS record_timestamp
      FROM companies c
      LEFT JOIN deals d ON d.associated_company_id = c.company_id
    ),
    -- Step 4: Join with stage mapping to get scores
    scored AS (
      SELECT
        j.company_id,
        j.deal_id,
        j.owner_id,
        j.lifecycle_stage,
        j.lead_status,
        j.deal_stage,
        j.combined_stage,
        COALESCE(sm.stage_level, 0) as stage_level,
        COALESCE(sm.adjusted_score, 0.0) as adjusted_score,
        j.stage_source,
        j.snapshot_id,
        j.record_timestamp
      FROM joined j
      LEFT JOIN `{project_id}.{dataset_id}.hs_stage_mapping` sm
        ON sm.combined_stage = j.combined_stage
    )
    SELECT * FROM scored
    -- Remove the owner_id filter to include all companies, including those with owner_id=0
    """

    logger.info("🔹 Submitting BigQuery job for unit scores...")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)
        ],
        destination=f"{project_id}.{dataset_id}.hs_pipeline_units_snapshot",
        write_disposition="WRITE_APPEND",
    )

    try:
        # Step 1: Try to delete any existing records for this snapshot to avoid duplicates
        # Handle the case where table doesn't exist gracefully
        logger.info(f"🗑️ Cleaning existing records for snapshot: {snapshot_id}")
        
        delete_query = f"""
        DELETE FROM `{project_id}.{dataset_id}.hs_pipeline_units_snapshot`
        WHERE snapshot_id = @snapshot_id
        """
        
        delete_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)
            ]
        )
        
        # Try DELETE with broader exception handling
        delete_success = False
        try:
            delete_job = client.query(delete_query, job_config=delete_job_config)
            delete_job.result()
            logger.debug(f"✅ Cleaned existing records for snapshot: {snapshot_id}")
            delete_success = True
        except Exception as delete_error:
            # Table might not exist - check if it's a "not found" error
            if "not found" in str(delete_error).lower() or "404" in str(delete_error):
                logger.debug(f"ℹ️ Pipeline units table doesn't exist yet - will be created by INSERT")
            else:
                logger.warning(f"⚠️ Unexpected error during DELETE (continuing anyway): {delete_error}")
        
        # Step 2: Insert new records (will create table if needed)
        job = client.query(query, job_config=job_config)
        logger.info(f"   • BigQuery job ID (unit score): {job.job_id}")
        job.result()  # Wait for completion
        
        # Step 3: Get accurate row count of newly inserted records
        # Use num_dml_affected_rows from the INSERT job, with fallback to table count
        rows_processed = job.num_dml_affected_rows
        
        if rows_processed is None or rows_processed == 0:
            # Fallback: count records in table for this snapshot
            count_query = f"""
            SELECT COUNT(*) as row_count
            FROM `{project_id}.{dataset_id}.hs_pipeline_units_snapshot`
            WHERE snapshot_id = @snapshot_id
            """
            
            count_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)
                ]
            )
            
            count_job = client.query(count_query, job_config=count_job_config)
            count_result = next(count_job.result())
            rows_processed = count_result.row_count
            logger.debug(f"Used fallback count query: {rows_processed} records")
        
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        logger.info(f"✅ Unit-score job completed: {rows_processed} records in {processing_time:.2f}s")
        
        # Debug: Log sample data if no records found
        if rows_processed == 0:
            logger.warning("⚠️ No unit score records generated - checking source data...")
            debug_query = f"""
            SELECT 
              'companies' as source_table,
              COUNT(*) as record_count,
              STRING_AGG(DISTINCT lifecycle_stage ORDER BY lifecycle_stage) as lifecycle_stages
            FROM `{project_id}.{dataset_id}.hs_companies`
            WHERE snapshot_id = @snapshot_id
            
            UNION ALL
            
            SELECT 
              'deals' as source_table,
              COUNT(*) as record_count,
              STRING_AGG(DISTINCT deal_stage ORDER BY deal_stage LIMIT 10) as deal_stages
            FROM `{project_id}.{dataset_id}.hs_deals`
            WHERE snapshot_id = @snapshot_id
            
            UNION ALL
            
            SELECT 
              'stage_mapping' as source_table,
              COUNT(*) as record_count,
              STRING_AGG(DISTINCT combined_stage ORDER BY combined_stage LIMIT 10) as combined_stages
            FROM `{project_id}.{dataset_id}.hs_stage_mapping`
            """
            
            debug_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)
                ]
            )
            
            debug_job = client.query(debug_query, job_config=debug_job_config)
            debug_results = debug_job.result()
            
            logger.info("📊 Source data summary:")
            for row in debug_results:
                logger.info(f"   • {row.source_table}: {row.record_count} records")
                if row.source_table == 'companies' and hasattr(row, 'lifecycle_stages'):
                    logger.info(f"     Lifecycle stages: {row.lifecycle_stages}")
                elif row.source_table == 'deals' and hasattr(row, 'deal_stages'):
                    logger.info(f"     Deal stages: {row.deal_stages}")
                elif row.source_table == 'stage_mapping' and hasattr(row, 'combined_stages'):
                    logger.info(f"     Combined stages: {row.combined_stages}")
        
        return {
            'status': 'success',
            'records': rows_processed,
            'processing_time_seconds': processing_time
        }
        
    except GoogleAPIError as e:
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        logger.error(f"❌ BigQuery unit-score job failed: {e}", exc_info=True)
        raise RuntimeError(f"Unit score processing failed: {e}")

def ensure_score_history_table_exists():
    """Ensure the score history table exists with correct schema"""
    logger = logging.getLogger('hubspot.scoring.processor')
    
    client = bigquery.Client()
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset_id = os.getenv('BIGQUERY_DATASET_ID')
    table_name = "hs_pipeline_score_history"
    full_table = f"{project_id}.{dataset_id}.{table_name}"
    
    try:
        existing_table = client.get_table(full_table)
        logger.debug(f"✅ Score history table {full_table} exists")
    except Exception:
        logger.info(f"📝 Creating score history table {full_table}")
        
        # Import schema from schema.py
        from hubspot_pipeline.schema import SCHEMA_PIPELINE_SCORE_HISTORY
        
        # Convert schema to BigQuery schema fields
        bq_schema = []
        for col_name, col_type in SCHEMA_PIPELINE_SCORE_HISTORY:
            bq_schema.append(bigquery.SchemaField(col_name, col_type))
        
        try:
            table = bigquery.Table(full_table, schema=bq_schema)
            client.create_table(table)
            logger.info(f"✅ Created score history table {full_table}")
        except Exception as e:
            logger.error(f"❌ Failed to create score history table: {e}")
            raise RuntimeError(f"Failed to create score history table: {e}")

def process_score_history_for_snapshot(snapshot_id: str):
    """
    Processes and appends score history data for a given snapshot to the BigQuery score history table.

    Steps:
      1) Ensure the score history table exists,
      2) Sleep briefly to allow any streaming buffer to settle,
      3) Aggregate pipeline_units rows by owner & combined_stage,
      4) Append results to the hs_pipeline_score_history table.

    Args:
        snapshot_id (str): The unique identifier for the snapshot to process.
        
    Returns:
        dict: Results with record count and timing
    """
    logger = logging.getLogger('hubspot.scoring.processor')
    logger.info(f"🔹 Processing score history for snapshot: {snapshot_id}")

    start_time = datetime.utcnow()
    
    # Ensure the score history table exists before any operations
    ensure_score_history_table_exists()
    
    # Brief wait to ensure data availability
    wait_secs = 5
    logger.info(f"   • Waiting {wait_secs}s to ensure pipeline_units data is available...")
    time.sleep(wait_secs)

    client = bigquery.Client()
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset_id = os.getenv('BIGQUERY_DATASET_ID')

    # FIXED: Updated aggregation query with proper field names
    query = f"""
    SELECT
      snapshot_id,
      owner_id,
      combined_stage,
      COUNT(DISTINCT company_id) AS num_companies,
      SUM(adjusted_score) AS total_score,
      CURRENT_TIMESTAMP() AS record_timestamp
    FROM `{project_id}.{dataset_id}.hs_pipeline_units_snapshot`
    WHERE snapshot_id = @snapshot_id
      -- Remove owner_id IS NOT NULL filter to include owner_id=0
      AND adjusted_score IS NOT NULL
    GROUP BY snapshot_id, owner_id, combined_stage
    -- Remove the HAVING clause to include all groups, even with 0 companies
    """

    logger.info("🔹 Submitting BigQuery job for score history...")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)
        ],
        destination=f"{project_id}.{dataset_id}.hs_pipeline_score_history",
        write_disposition="WRITE_APPEND",
    )

    try:
        # Step 1: Delete any existing records for this snapshot to avoid duplicates
        # This ensures we only keep the latest run of each snapshot
        delete_query = f"""
        DELETE FROM `{project_id}.{dataset_id}.hs_pipeline_score_history`
        WHERE snapshot_id = @snapshot_id
        """
        
        logger.info(f"🗑️ Cleaning existing score history for snapshot: {snapshot_id}")
        delete_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)
            ]
        )
        
        delete_job = client.query(delete_query, job_config=delete_job_config)
        delete_job.result()
        
        # Step 2: Insert new aggregated records
        job = client.query(query, job_config=job_config)
        logger.info(f"   • BigQuery job ID (score history): {job.job_id}")
        job.result()  # Wait for completion
        
        # Step 3: Get accurate row count of newly inserted records
        # Use num_dml_affected_rows from the INSERT job, with fallback to table count
        rows_processed = job.num_dml_affected_rows
        
        if rows_processed is None or rows_processed == 0:
            # Fallback: count records in table for this specific snapshot only
            count_query = f"""
            SELECT COUNT(*) as row_count
            FROM `{project_id}.{dataset_id}.hs_pipeline_score_history`
            WHERE snapshot_id = @snapshot_id
            """
            
            count_job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)
                ]
            )
            
            count_job = client.query(count_query, job_config=count_job_config)
            count_result = next(count_job.result())
            rows_processed = count_result.row_count
            logger.debug(f"Used fallback count query: {rows_processed} records")
        
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
    
def debug_snapshot_data(snapshot_id: str) -> dict:
    """
    Debug function to check what data exists for a snapshot
    
    Args:
        snapshot_id: The snapshot to debug
        
    Returns:
        dict: Debug information about the snapshot data
    """
    logger = logging.getLogger('hubspot.scoring.processor')
    logger.info(f"🔍 Debugging data for snapshot: {snapshot_id}")
    
    client = bigquery.Client()
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset_id = os.getenv('BIGQUERY_DATASET_ID')
    
    debug_info = {}
    
    try:
        # Check companies
        companies_query = f"""
        SELECT 
          COUNT(*) as total_companies,
          COUNT(DISTINCT lifecycle_stage) as unique_lifecycle_stages,
          STRING_AGG(DISTINCT lifecycle_stage ORDER BY lifecycle_stage) as lifecycle_stages,
          COUNT(DISTINCT hubspot_owner_id) as unique_owners
        FROM `{project_id}.{dataset_id}.hs_companies`
        WHERE snapshot_id = @snapshot_id
        """
        
        companies_job = client.query(companies_query, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)]
        ))
        companies_result = next(companies_job.result())
        
        debug_info['companies'] = {
            'total': companies_result.total_companies,
            'unique_lifecycle_stages': companies_result.unique_lifecycle_stages,
            'lifecycle_stages': companies_result.lifecycle_stages,
            'unique_owners': companies_result.unique_owners
        }
        
        # Check deals
        deals_query = f"""
        SELECT 
          COUNT(*) as total_deals,
          COUNT(DISTINCT deal_stage) as unique_deal_stages,
          STRING_AGG(DISTINCT deal_stage ORDER BY deal_stage) as deal_stages,
          COUNT(DISTINCT associated_company_id) as unique_associated_companies
        FROM `{project_id}.{dataset_id}.hs_deals`
        WHERE snapshot_id = @snapshot_id
        """
        
        deals_job = client.query(deals_query, job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)]
        ))
        deals_result = next(deals_job.result())
        
        debug_info['deals'] = {
            'total': deals_result.total_deals,
            'unique_deal_stages': deals_result.unique_deal_stages,
            'deal_stages': deals_result.deal_stages,
            'unique_associated_companies': deals_result.unique_associated_companies
        }
        
        # Check stage mapping
        mapping_query = f"""
        SELECT 
          COUNT(*) as total_mappings,
          STRING_AGG(DISTINCT combined_stage ORDER BY combined_stage) as combined_stages
        FROM `{project_id}.{dataset_id}.hs_stage_mapping`
        """
        
        mapping_job = client.query(mapping_query)
        mapping_result = next(mapping_job.result())
        
        debug_info['stage_mapping'] = {
            'total': mapping_result.total_mappings,
            'combined_stages': mapping_result.combined_stages
        }
        
        logger.info(f"📊 Debug summary for {snapshot_id}:")
        logger.info(f"   • Companies: {debug_info['companies']['total']} ({debug_info['companies']['unique_owners']} owners)")
        logger.info(f"   • Deals: {debug_info['deals']['total']} ({debug_info['deals']['unique_associated_companies']} associated companies)")
        logger.info(f"   • Stage mappings: {debug_info['stage_mapping']['total']}")
        
        return debug_info
        
    except Exception as e:
        logger.error(f"❌ Debug query failed: {e}")
        return {'error': str(e)}