# src/hubspot_pipeline/hubspot_scoring/rescore_all.py

import logging
import os
from datetime import datetime
from typing import List, Dict, Any
from google.cloud import bigquery

def get_all_snapshots_from_registry() -> List[str]:
    """
    Get all completed ingest snapshots from registry, ordered by timestamp
    
    Returns:
        List[str]: All snapshot IDs that have completed ingest
    """
    logger = logging.getLogger('hubspot.scoring.rescore_all')
    
    try:
        client = bigquery.Client()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        query = f"""
        SELECT snapshot_id
        FROM `{project_id}.{dataset_id}.hs_snapshot_registry`
        WHERE status LIKE '%ingest%'
          AND triggered_by LIKE '%ingest%'
        GROUP BY snapshot_id
        ORDER BY MIN(record_timestamp) ASC
        """
        
        job = client.query(query)
        results = job.result()
        
        snapshots = [row.snapshot_id for row in results]
        logger.info(f"📊 Discovered {len(snapshots)} snapshots for rescoring")
        
        return snapshots
        
    except Exception as e:
        logger.error(f"❌ Failed to get snapshots from registry: {e}")
        raise RuntimeError(f"Failed to discover snapshots: {e}")

def handle_rescore_all_complete() -> Dict[str, Any]:
    """
    Process every single snapshot found in registry - no limits or filtering
    
    Returns:
        dict: Complete rescore results with timing and counts
    """
    logger = logging.getLogger('hubspot.scoring.rescore_all')
    
    logger.info("🔄 Starting COMPLETE rescore-all operation (no limits)")
    start_time = datetime.utcnow()
    
    try:
        # Import here to avoid circular imports
        from .main import process_snapshot_event
        
        # 1. Discover all snapshots (no filtering)
        snapshots = get_all_snapshots_from_registry()
        
        if not snapshots:
            logger.warning("⚠️ No snapshots found in registry")
            return {
                "status": "success",
                "rescore_type": "all_snapshots_no_limits",
                "timing": {
                    "total_duration_seconds": 0.0,
                    "started_at": start_time.isoformat() + "Z",
                    "completed_at": start_time.isoformat() + "Z",
                    "average_per_snapshot_seconds": 0.0
                },
                "snapshots": {
                    "discovered": 0,
                    "processed_successfully": 0,
                    "failed": 0,
                    "total_attempted": 0
                },
                "failed_snapshots": []
            }
        
        logger.info(f"🎯 Will process {len(snapshots)} snapshots - THIS WILL TAKE SIGNIFICANT TIME")
        
        # 2. Initialize tracking
        successful_snapshots = []
        failed_snapshots = []
        snapshot_times = {}
        
        # 3. Process every snapshot
        for i, snapshot_id in enumerate(snapshots, 1):
            snapshot_start = datetime.utcnow()
            
            try:
                logger.info(f"Processing snapshot {i}/{len(snapshots)}: {snapshot_id}")
                
                # Create event data for this snapshot
                event_data = {
                    "snapshot_id": snapshot_id,
                    "data_tables": {},  # Will be populated by scoring function
                    "reference_tables": {}
                }
                
                # Process the snapshot
                result = process_snapshot_event(event_data)
                
                if result.get('status') == 'success':
                    successful_snapshots.append(snapshot_id)
                    logger.debug(f"✅ Snapshot {i}/{len(snapshots)} completed successfully")
                else:
                    error_msg = result.get('error', 'Unknown error')
                    failed_snapshots.append({
                        "snapshot_id": snapshot_id,
                        "error": error_msg
                    })
                    logger.warning(f"⚠️ Snapshot {i}/{len(snapshots)} failed: {error_msg}")
                
            except Exception as e:
                error_msg = str(e)
                failed_snapshots.append({
                    "snapshot_id": snapshot_id,
                    "error": error_msg
                })
                logger.error(f"❌ Failed snapshot {i}/{len(snapshots)}: {snapshot_id} - {error_msg}")
            
            # Record timing for this snapshot
            snapshot_duration = (datetime.utcnow() - snapshot_start).total_seconds()
            snapshot_times[snapshot_id] = snapshot_duration
            
            # Log progress every 10 snapshots or at significant milestones
            if i % 10 == 0 or i == len(snapshots):
                elapsed = (datetime.utcnow() - start_time).total_seconds()
                logger.info(f"📈 Progress: {i}/{len(snapshots)} completed in {elapsed:.1f}s")
        
        # 4. Calculate final timing
        end_time = datetime.utcnow()
        total_duration = (end_time - start_time).total_seconds()
        avg_duration = total_duration / len(snapshots) if snapshots else 0.0
        
        # 5. Determine overall status
        successful_count = len(successful_snapshots)
        failed_count = len(failed_snapshots)
        
        if failed_count == 0:
            status = "success"
        elif successful_count > 0:
            status = "partial_success"
        else:
            status = "error"
        
        # 6. Build response
        response = {
            "status": status,
            "rescore_type": "all_snapshots_no_limits",
            "timing": {
                "total_duration_seconds": round(total_duration, 2),
                "started_at": start_time.isoformat() + "Z",
                "completed_at": end_time.isoformat() + "Z",
                "average_per_snapshot_seconds": round(avg_duration, 2)
            },
            "snapshots": {
                "discovered": len(snapshots),
                "processed_successfully": successful_count,
                "failed": failed_count,
                "total_attempted": len(snapshots)
            },
            "failed_snapshots": failed_snapshots
        }
        
        # 7. Log final summary
        logger.info(f"🎉 Rescore-all completed: {successful_count}/{len(snapshots)} successful in {total_duration:.1f}s")
        if failed_count > 0:
            logger.warning(f"⚠️ {failed_count} snapshots failed during processing")
        
        return response
        
    except Exception as e:
        # Handle catastrophic failure
        end_time = datetime.utcnow()
        total_duration = (end_time - start_time).total_seconds()
        
        logger.error(f"❌ Rescore-all operation failed catastrophically: {e}")
        
        return {
            "status": "error",
            "rescore_type": "all_snapshots_no_limits",
            "error": str(e),
            "timing": {
                "total_duration_seconds": round(total_duration, 2),
                "started_at": start_time.isoformat() + "Z",
                "completed_at": end_time.isoformat() + "Z",
                "average_per_snapshot_seconds": 0.0
            },
            "snapshots": {
                "discovered": 0,
                "processed_successfully": 0,
                "failed": 0,
                "total_attempted": 0
            },
            "failed_snapshots": []
        }