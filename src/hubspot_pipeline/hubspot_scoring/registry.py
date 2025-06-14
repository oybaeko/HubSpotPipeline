# src/hubspot_pipeline/scoring/registry.py

import logging
import os
from datetime import datetime
from typing import Optional
from google.cloud import bigquery

def register_scoring_completion(snapshot_id: str, processed_records: int, notes: Optional[str] = None) -> bool:
    """
    Register scoring completion in snapshot registry
    
    Args:
        snapshot_id: The snapshot identifier
        processed_records: Number of records processed
        notes: Additional notes about scoring process
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger('hubspot.scoring.registry')
    
    try:
        client = bigquery.Client()
        
        # Get config
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        if not project_id or not dataset_id:
            logger.error("Missing BigQuery configuration")
            return False
        
        table_ref = f"{project_id}.{dataset_id}.hs_snapshot_registry"
        
        # Create scoring completion notes
        completion_notes = f"Scoring: Processed {processed_records} records"
        if notes:
            completion_notes += f" - {notes}"
        
        # Insert new record for scoring completion (avoids streaming buffer conflicts)
        # Use BigQuery's server-side timestamp for consistency
        query = f"""
        INSERT INTO `{table_ref}` (
            snapshot_id,
            record_timestamp,
            triggered_by,
            status,
            notes
        ) VALUES (
            @snapshot_id,
            CURRENT_TIMESTAMP(),
            @triggered_by,
            @status,
            @notes
        )
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id),
                bigquery.ScalarQueryParameter("triggered_by", "STRING", "scoring_completion"),
                bigquery.ScalarQueryParameter("status", "STRING", "scoring_completed"),
                bigquery.ScalarQueryParameter("notes", "STRING", completion_notes),
            ]
        )
        
        job = client.query(query, job_config=job_config)
        job.result()  # Wait for completion
        
        logger.info(f"✅ Registered scoring completion for snapshot {snapshot_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to register scoring completion: {e}")
        return False

def register_scoring_failure(snapshot_id: str, error_message: str) -> bool:
    """
    Register scoring failure in snapshot registry
    
    Args:
        snapshot_id: The snapshot identifier
        error_message: Description of the error
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger('hubspot.scoring.registry')
    
    try:
        client = bigquery.Client()
        
        # Get config
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        if not project_id or not dataset_id:
            logger.error("Missing BigQuery configuration")
            return False
        
        table_ref = f"{project_id}.{dataset_id}.hs_snapshot_registry"
        
        # Insert new record for scoring failure using BigQuery server-side timestamp
        query = f"""
        INSERT INTO `{table_ref}` (
            snapshot_id,
            record_timestamp,
            triggered_by,
            status,
            notes
        ) VALUES (
            @snapshot_id,
            CURRENT_TIMESTAMP(),
            @triggered_by,
            @status,
            @notes
        )
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id),
                bigquery.ScalarQueryParameter("triggered_by", "STRING", "scoring_failure"),
                bigquery.ScalarQueryParameter("status", "STRING", "scoring_failed"),
                bigquery.ScalarQueryParameter("notes", "STRING", f"Scoring failed: {error_message}"),
            ]
        )
        
        job = client.query(query, job_config=job_config)
        job.result()  # Wait for completion
        
        logger.info(f"✅ Registered scoring failure for snapshot {snapshot_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to register scoring failure: {e}")
        return False

def register_scoring_start(snapshot_id: str) -> bool:
    """
    Register scoring start in snapshot registry
    
    Args:
        snapshot_id: The snapshot identifier
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger('hubspot.scoring.registry')
    
    try:
        client = bigquery.Client()
        
        # Get config
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        if not project_id or not dataset_id:
            logger.error("Missing BigQuery configuration")
            return False
        
        table_ref = f"{project_id}.{dataset_id}.hs_snapshot_registry"
        
        # Insert new record for scoring start using BigQuery server-side timestamp
        query = f"""
        INSERT INTO `{table_ref}` (
            snapshot_id,
            record_timestamp,
            triggered_by,
            status,
            notes
        ) VALUES (
            @snapshot_id,
            CURRENT_TIMESTAMP(),
            @triggered_by,
            @status,
            @notes
        )
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id),
                bigquery.ScalarQueryParameter("triggered_by", "STRING", "scoring_start"),
                bigquery.ScalarQueryParameter("status", "STRING", "scoring_started"),
                bigquery.ScalarQueryParameter("notes", "STRING", "Scoring process initiated"),
            ]
        )
        
        job = client.query(query, job_config=job_config)
        job.result()  # Wait for completion
        
        logger.info(f"✅ Registered scoring start for snapshot {snapshot_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to register scoring start: {e}")
        return False