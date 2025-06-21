# src/hubspot_pipeline/hubspot_scoring/registry.py

import logging
import os
from datetime import datetime
from typing import Optional
from google.cloud import bigquery

def register_scoring_start(snapshot_id: str) -> bool:
    """
    Register scoring start in snapshot registry
    Deletes any previous scoring events for this snapshot first
    """
    logger = logging.getLogger('hubspot.scoring.registry')
    
    try:
        client = bigquery.Client()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        table_ref = f"{project_id}.{dataset_id}.hs_snapshot_registry"
        
        # Clean previous scoring events
        delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE snapshot_id = @snapshot_id
          AND triggered_by IN ('scoring_start', 'scoring_completion')
        """
        
        delete_job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)]
        )
        
        try:
            delete_job = client.query(delete_query, job_config=delete_job_config)
            delete_job.result()
        except Exception as delete_error:
            logger.debug(f"ℹ️ No previous scoring events to clean: {delete_error}")
        
        # Insert new scoring start record
        insert_query = f"""
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
        
        insert_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id),
                bigquery.ScalarQueryParameter("triggered_by", "STRING", "scoring_start"),
                bigquery.ScalarQueryParameter("status", "STRING", "started"),
                bigquery.ScalarQueryParameter("notes", "STRING", "Scoring process initiated"),
            ]
        )
        
        insert_job = client.query(insert_query, job_config=insert_job_config)
        insert_job.result()
        
        logger.info(f"✅ Registered scoring start for snapshot {snapshot_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to register scoring start: {e}")
        return False


def register_scoring_completion(snapshot_id: str, processed_records: int, notes: Optional[str] = None) -> bool:
    """
    Register scoring completion in snapshot registry
    """
    logger = logging.getLogger('hubspot.scoring.registry')
    
    try:
        client = bigquery.Client()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        table_ref = f"{project_id}.{dataset_id}.hs_snapshot_registry"
        
        completion_notes = f"Scoring: Processed {processed_records} records"
        if notes:
            completion_notes += f" - {notes}"
        
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
                bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id),
                bigquery.ScalarQueryParameter("triggered_by", "STRING", "scoring_completion"),
                bigquery.ScalarQueryParameter("status", "STRING", "completed"),
                bigquery.ScalarQueryParameter("notes", "STRING", completion_notes),
            ]
        )
        
        job = client.query(query, job_config=job_config)
        job.result()
        
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
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        table_ref = f"{project_id}.{dataset_id}.hs_snapshot_registry"
        
        # Safety clean: Try to delete any previous scoring events for this snapshot
        # (in case failure happened before register_scoring_start)
        delete_query = f"""
        DELETE FROM `{table_ref}`
        WHERE snapshot_id = @snapshot_id
          AND triggered_by IN ('scoring_start', 'scoring_completion', 'scoring_failure')
        """
        
        logger.debug(f"🗑️ Safety clean of scoring events for failed snapshot: {snapshot_id}")
        delete_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)
            ]
        )
        
        try:
            delete_job = client.query(delete_query, job_config=delete_job_config)
            delete_job.result()
            logger.debug(f"✅ Safety cleaned scoring events for snapshot: {snapshot_id}")
        except Exception as delete_error:
            # Table might not exist yet - this is OK, INSERT will create it
            logger.debug(f"ℹ️ No scoring events to clean (table may not exist): {delete_error}")
        
        # Insert new record for scoring failure using BigQuery server-side timestamp
        insert_query = f"""
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
        
        insert_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id),
                bigquery.ScalarQueryParameter("triggered_by", "STRING", "scoring_failure"),
                bigquery.ScalarQueryParameter("status", "STRING", "failed"),
                bigquery.ScalarQueryParameter("notes", "STRING", f"Scoring failed: {error_message}"),
            ]
        )
        
        insert_job = client.query(insert_query, job_config=insert_job_config)
        insert_job.result()  # Wait for completion
        
        logger.info(f"✅ Registered scoring failure for snapshot {snapshot_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to register scoring failure: {e}")
        return False