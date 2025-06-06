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
        scoring_row = {
            "snapshot_id": snapshot_id,
            "snapshot_timestamp": datetime.utcnow().isoformat(),
            "triggered_by": "scoring_completion",
            "status": "scoring_completed",
            "notes": completion_notes,
        }
        
        errors = client.insert_rows_json(table_ref, [scoring_row])
        if errors:
            logger.error(f"❌ Failed to register scoring completion: {errors}")
            return False
        
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
        
        # Insert new record for scoring failure
        failure_row = {
            "snapshot_id": snapshot_id,
            "snapshot_timestamp": datetime.utcnow().isoformat(),
            "triggered_by": "scoring_failure",
            "status": "scoring_failed",
            "notes": f"Scoring failed: {error_message}",
        }
        
        errors = client.insert_rows_json(table_ref, [failure_row])
        if errors:
            logger.error(f"❌ Failed to register scoring failure: {errors}")
            return False
        
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
        
        # Insert new record for scoring start
        start_row = {
            "snapshot_id": snapshot_id,
            "snapshot_timestamp": datetime.utcnow().isoformat(),
            "triggered_by": "scoring_start",
            "status": "scoring_started",
            "notes": "Scoring process initiated",
        }
        
        errors = client.insert_rows_json(table_ref, [start_row])
        if errors:
            logger.error(f"❌ Failed to register scoring start: {errors}")
            return False
        
        logger.info(f"✅ Registered scoring start for snapshot {snapshot_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to register scoring start: {e}")
        return False