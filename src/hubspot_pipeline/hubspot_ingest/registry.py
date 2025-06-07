# src/hubspot_pipeline/hubspot_ingest/registry.py

import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Import our new BigQuery utilities
from hubspot_pipeline.bigquery_utils import (
    get_bigquery_client,
    get_table_reference,
    insert_rows_with_retry,
    ensure_table_exists
)
from hubspot_pipeline.hubspot_ingest.reference.schemas import SNAPSHOT_REGISTRY_SCHEMA

def ensure_registry_table_exists() -> None:
    """
    Ensure the snapshot registry table exists with correct schema.
    """
    logger = logging.getLogger('hubspot.registry')
    
    client = get_bigquery_client()
    full_table = get_table_reference("hs_snapshot_registry")
    
    try:
        existing_table = client.get_table(full_table)
        logger.debug(f"✅ Registry table {full_table} exists")
    except NotFound:
        logger.info(f"📝 Creating registry table {full_table}")
        
        # Convert schema to BigQuery schema fields
        bq_schema = []
        for col_name, col_type in SNAPSHOT_REGISTRY_SCHEMA:
            bq_schema.append(bigquery.SchemaField(col_name, col_type))
        
        ensure_table_exists(client, full_table, bq_schema)
        logger.info(f"✅ Created registry table {full_table}")


def register_snapshot_start(snapshot_id: str, triggered_by: str = "manual") -> bool:
    """
    Register the start of a snapshot process.
    
    Args:
        snapshot_id: Unique identifier for this snapshot
        triggered_by: Who/what triggered this snapshot
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger('hubspot.registry')
    
    try:
        ensure_registry_table_exists()
        
        client = get_bigquery_client()
        table_ref = get_table_reference("hs_snapshot_registry")
        
        row = {
            "snapshot_id": snapshot_id,
            "snapshot_timestamp": datetime.utcnow().isoformat(),
            "triggered_by": triggered_by,
            "status": "started",
            "notes": "Snapshot process initiated",
        }
        
        # Use utilities function with built-in retry logic
        insert_rows_with_retry(
            client=client,
            table_ref=table_ref,
            rows=[row],
            operation_name=f"register snapshot start for {snapshot_id}"
        )
        
        logger.info(f"✅ Registered snapshot start: {snapshot_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Exception registering snapshot start: {e}")
        return False


def register_snapshot_ingest_complete(snapshot_id: str, data_counts: Dict[str, int], 
                                    reference_counts: Dict[str, int]) -> bool:
    """
    Register the completion of snapshot ingest phase.
    Uses INSERT instead of UPDATE to avoid streaming buffer conflicts.
    
    Args:
        snapshot_id: The snapshot identifier
        data_counts: Dict of data table counts
        reference_counts: Dict of reference table counts
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger('hubspot.registry')
    
    try:
        client = get_bigquery_client()
        table_ref = get_table_reference("hs_snapshot_registry")
        
        # Create comprehensive notes
        total_data = sum(data_counts.values())
        total_reference = sum(reference_counts.values())
        notes = f"Ingest completed: {total_data} data records, {total_reference} reference records. Tables: {list(data_counts.keys())}"
        
        # Instead of UPDATE, insert a new completion record with same snapshot_id
        # This avoids streaming buffer conflicts while keeping clean snapshot_id
        completion_row = {
            "snapshot_id": snapshot_id,  # Same ID, different status
            "snapshot_timestamp": datetime.utcnow().isoformat(),
            "triggered_by": "ingest_completion",
            "status": "ingest_completed",
            "notes": notes,
        }
        
        # Use utilities function with built-in retry logic
        insert_rows_with_retry(
            client=client,
            table_ref=table_ref,
            rows=[completion_row],
            operation_name=f"register ingest completion for {snapshot_id}"
        )
        
        logger.info(f"✅ Registered ingest completion for snapshot {snapshot_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Exception registering ingest completion: {e}")
        return False


def register_snapshot_failure(snapshot_id: str, error_message: str) -> bool:
    """
    Register a snapshot failure.
    
    Args:
        snapshot_id: The snapshot identifier
        error_message: Description of the error
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger('hubspot.registry')
    
    try:
        client = get_bigquery_client()
        dataset = os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
        project_id = os.getenv("BIGQUERY_PROJECT_ID")
        
        # Update the existing record (this doesn't use insertAll API, so no retry needed)
        update_query = f"""
        UPDATE `{project_id}.{dataset}.hs_snapshot_registry`
        SET 
            status = "failed",
            notes = CONCAT(IFNULL(notes, ''), ' | ERROR: ', @error_message)
        WHERE snapshot_id = @snapshot_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id),
                bigquery.ScalarQueryParameter("error_message", "STRING", str(error_message)[:500])  # Limit length
            ]
        )
        
        query_job = client.query(update_query, job_config=job_config)
        query_job.result()
        
        logger.info(f"✅ Registered failure for snapshot {snapshot_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Exception registering failure: {e}")
        return False


def update_snapshot_status(snapshot_id: str, status: str, notes: Optional[str] = None) -> bool:
    """
    Generic function to update snapshot status.
    
    Args:
        snapshot_id: The snapshot identifier
        status: New status value
        notes: Additional notes to append
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger('hubspot.registry')
    
    try:
        client = get_bigquery_client()
        dataset = os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
        project_id = os.getenv("BIGQUERY_PROJECT_ID")
        
        if notes:
            update_query = f"""
            UPDATE `{project_id}.{dataset}.hs_snapshot_registry`
            SET 
                status = @status,
                notes = CONCAT(IFNULL(notes, ''), ' | ', @notes)
            WHERE snapshot_id = @snapshot_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id),
                    bigquery.ScalarQueryParameter("status", "STRING", status),
                    bigquery.ScalarQueryParameter("notes", "STRING", notes)
                ]
            )
        else:
            update_query = f"""
            UPDATE `{project_id}.{dataset}.hs_snapshot_registry`
            SET status = @status
            WHERE snapshot_id = @snapshot_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id),
                    bigquery.ScalarQueryParameter("status", "STRING", status)
                ]
            )
        
        query_job = client.query(update_query, job_config=job_config)
        query_job.result()
        
        logger.info(f"✅ Updated snapshot {snapshot_id} status to: {status}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Exception updating snapshot status: {e}")
        return False


def get_latest_snapshot(status_filter: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Get the latest snapshot from the registry.
    
    Args:
        status_filter: Optional status to filter by (e.g., "ingest_completed")
        
    Returns:
        Dictionary with snapshot info or None if not found
    """
    logger = logging.getLogger('hubspot.registry')
    
    try:
        client = get_bigquery_client()
        dataset = os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
        project_id = os.getenv("BIGQUERY_PROJECT_ID")
        
        base_query = f"""
        SELECT 
            snapshot_id,
            snapshot_timestamp,
            triggered_by,
            status,
            notes
        FROM `{project_id}.{dataset}.hs_snapshot_registry`
        """
        
        if status_filter:
            query = f"{base_query} WHERE status = @status ORDER BY snapshot_timestamp DESC LIMIT 1"
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("status", "STRING", status_filter)
                ]
            )
        else:
            query = f"{base_query} ORDER BY snapshot_timestamp DESC LIMIT 1"
            job_config = bigquery.QueryJobConfig()
        
        result = client.query(query, job_config=job_config).result()
        latest = next(result, None)
        
        if latest:
            return {
                'snapshot_id': latest.snapshot_id,
                'snapshot_timestamp': latest.snapshot_timestamp,
                'triggered_by': latest.triggered_by,
                'status': latest.status,
                'notes': latest.notes
            }
        else:
            logger.info(f"No snapshots found with status filter: {status_filter}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Failed to get latest snapshot: {e}")
        return None