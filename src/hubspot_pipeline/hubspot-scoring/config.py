# hubspot-scoring/config.py
# Configuration helpers for scoring function

import logging
import os
from google.cloud import bigquery

def setup_logging():
    """Setup logging for scoring function"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True
    )
    return logging.getLogger('hubspot.scoring')

def get_config():
    """Get BigQuery configuration"""
    return {
        'BIGQUERY_PROJECT_ID': os.getenv('BIGQUERY_PROJECT_ID'),
        'BIGQUERY_DATASET_ID': os.getenv('BIGQUERY_DATASET_ID', 'Hubspot_dev_ob')
    }

def update_snapshot_registry_scoring(snapshot_id: str, status: str = "completed", notes: str = None):
    """
    Update snapshot registry when scoring completes
    
    Args:
        snapshot_id: The snapshot identifier
        status: "scoring_completed", "scoring_failed", etc.
        notes: Additional notes about scoring
    """
    logger = logging.getLogger('hubspot.scoring.registry')
    
    try:
        client = bigquery.Client()
        config = get_config()
        
        # Update the existing record
        update_query = f"""
        UPDATE `{config['BIGQUERY_PROJECT_ID']}.{config['BIGQUERY_DATASET_ID']}.hs_snapshot_registry`
        SET 
            status = @new_status,
            notes = CONCAT(IFNULL(notes, ''), ' | Scoring: ', @new_notes)
        WHERE snapshot_id = @snapshot_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id),
                bigquery.ScalarQueryParameter("new_status", "STRING", f"ingest_and_{status}"),
                bigquery.ScalarQueryParameter("new_notes", "STRING", notes or "completed")
            ]
        )
        
        query_job = client.query(update_query, job_config=job_config)
        query_job.result()
        
        logger.info(f"✅ Updated registry for snapshot {snapshot_id} with scoring status")
        
    except Exception as e:
        logger.error(f"❌ Failed to update snapshot registry: {e}")