# src/hubspot_pipeline/hubspot_ingest/registry.py

import logging
import os
from datetime import datetime
from typing import Dict, Any, Optional
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Import our updated BigQuery utilities
from hubspot_pipeline.bigquery_utils import (
    get_bigquery_client,
    get_table_reference,
    ensure_table_exists
)
from hubspot_pipeline.schema import SCHEMA_SNAPSHOT_REGISTRY

def ensure_registry_table_exists() -> None:
    """
    Ensure the snapshot registry table exists with correct schema.
    Simple existence check - let smart retry handle timing issues.
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
        for col_name, col_type in SCHEMA_SNAPSHOT_REGISTRY:
            bq_schema.append(bigquery.SchemaField(col_name, col_type))
        
        ensure_table_exists(client, full_table, bq_schema)
        logger.info(f"✅ Created registry table {full_table}")


def register_snapshot_start(snapshot_id: str, triggered_by: str = "manual") -> bool:
    """
    Register the start of a snapshot process using parameterized query for consistency.
    
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
        
        # Use parameterized INSERT query with CURRENT_TIMESTAMP() for server-side consistency
        query = f"""
        INSERT INTO `{table_ref}` (
            triggered_by,
            status,
            notes,
            snapshot_id,
            record_timestamp
        ) VALUES (
            @triggered_by,
            @status,
            @notes,
            @snapshot_id,
            CURRENT_TIMESTAMP()
        )
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("triggered_by", "STRING", triggered_by),
                bigquery.ScalarQueryParameter("status", "STRING", "started"),
                bigquery.ScalarQueryParameter("notes", "STRING", "Snapshot process initiated"),
                bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id),
            ]
        )
        
        job = client.query(query, job_config=job_config)
        job.result()  # Wait for completion
        
        logger.info(f"✅ Registered snapshot start: {snapshot_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Exception registering snapshot start: {e}")
        return False


def register_snapshot_ingest_complete(snapshot_id: str, data_counts: Dict[str, int], 
                                    reference_counts: Dict[str, int]) -> bool:
    """
    Register the completion of snapshot ingest phase using simplified statuses
    """
    logger = logging.getLogger('hubspot.registry')
    
    try:
        client = get_bigquery_client()
        table_ref = get_table_reference("hs_snapshot_registry")
        
        # Create comprehensive notes
        total_data = sum(data_counts.values())
        total_reference = sum(reference_counts.values())
        notes = f"Ingest: {total_data} data records, {total_reference} reference records. Tables: {list(data_counts.keys())}"
        
        # Insert new record for completion using simplified statuses
        query = f"""
        INSERT INTO `{table_ref}` (
            triggered_by,
            status,
            notes,
            snapshot_id,
            record_timestamp
        ) VALUES (
            @triggered_by,
            @status,
            @notes,
            @snapshot_id,
            CURRENT_TIMESTAMP()
        )
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("triggered_by", "STRING", "ingest_completion"),
                bigquery.ScalarQueryParameter("status", "STRING", "completed"),
                bigquery.ScalarQueryParameter("notes", "STRING", notes),
                bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id),
            ]
        )
        
        job = client.query(query, job_config=job_config)
        job.result()
        
        logger.info(f"✅ Registered ingest completion for snapshot {snapshot_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Exception registering ingest completion: {e}")
        return False


def register_snapshot_failure(snapshot_id: str, error_message: str) -> bool:
    """
    Register snapshot failure in registry
    
    Args:
        snapshot_id: The snapshot identifier
        error_message: Description of the error
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger('hubspot.registry')
    
    try:
        client = get_bigquery_client()
        table_ref = get_table_reference("hs_snapshot_registry")
        
        # Insert new record for ingest failure
        query = f"""
        INSERT INTO `{table_ref}` (
            triggered_by,
            status,
            notes,
            snapshot_id,
            record_timestamp
        ) VALUES (
            @triggered_by,
            @status,
            @notes,
            @snapshot_id,
            CURRENT_TIMESTAMP()
        )
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("triggered_by", "STRING", "ingest_failure"),
                bigquery.ScalarQueryParameter("status", "STRING", "failed"),
                bigquery.ScalarQueryParameter("notes", "STRING", f"Ingest failed: {error_message}"),
                bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id),
            ]
        )
        
        job = client.query(query, job_config=job_config)
        job.result()
        
        logger.info(f"✅ Registered ingest failure for snapshot {snapshot_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to register ingest failure: {e}")
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
                    bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id),
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
                    bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id),
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
        status_filter: Optional status to filter by (e.g., "completed")
        
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
            record_timestamp,
            triggered_by,
            status,
            notes
        FROM `{project_id}.{dataset}.hs_snapshot_registry`
        """
        
        if status_filter:
            query = f"{base_query} WHERE status = @status ORDER BY record_timestamp DESC LIMIT 1"
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("status", "STRING", status_filter)
                ]
            )
        else:
            query = f"{base_query} ORDER BY record_timestamp DESC LIMIT 1"
            job_config = bigquery.QueryJobConfig()
        
        result = client.query(query, job_config=job_config).result()
        latest = next(result, None)
        
        if latest:
            # Convert snapshot_id back to string format for consistency
            snapshot_id_str = latest.snapshot_id.strftime("%Y-%m-%dT%H:%M:%SZ") if hasattr(latest.snapshot_id, 'strftime') else str(latest.snapshot_id)
            
            return {
                'snapshot_id': snapshot_id_str,
                'record_timestamp': latest.record_timestamp,
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