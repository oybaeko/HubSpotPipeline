# src/hubspot_pipeline/hubspot_scoring/main.py

import logging
from datetime import datetime
from typing import Dict, Any

from .config import init_env, validate_config
from .stage_mapping import populate_stage_mapping
from .processor import process_snapshot
from .registry import register_scoring_start, register_scoring_completion, register_scoring_failure

def process_snapshot_event(event_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process a snapshot completion event and run scoring pipeline with normalization validation
    
    Args:
        event_data: Parsed event data from Pub/Sub message
        
    Returns:
        dict: Processing results
    """
    logger = logging.getLogger('hubspot.scoring')
    
    # Extract snapshot information
    snapshot_id = event_data.get('snapshot_id')
    data_tables = event_data.get('data_tables', {})
    reference_tables = event_data.get('reference_tables', {})
    
    if not snapshot_id:
        raise ValueError("Missing snapshot_id in event data")
    
    logger.info(f"📊 Processing snapshot: {snapshot_id}")
    logger.info(f"🔧 Data normalization validation enabled")
    logger.info(f"📋 Data tables: {data_tables}")
    logger.info(f"📋 Reference tables: {reference_tables}")
    
    start_time = datetime.utcnow()
    
    try:
        # Validate configuration
        config = validate_config()
        logger.info(f"✅ Configuration validated for environment: {config['ENVIRONMENT']}")
        
        # Register scoring start
        register_scoring_start(snapshot_id)
        
        # Step 1: Populate stage mapping with normalized values
        logger.info("📋 Populating normalized stage mapping...")
        mapping_count = populate_stage_mapping()
        logger.info(f"✅ Stage mapping populated: {mapping_count} normalized records")
        
        # Step 2: Process the snapshot with normalization validation
        logger.info(f"⚙️ Processing snapshot scores for {snapshot_id} with normalization checks...")
        processing_results = process_snapshot(snapshot_id)
        
        if processing_results.get('status') != 'success':
            raise RuntimeError(f"Snapshot processing failed: {processing_results.get('error')}")
        
        logger.info(f"✅ Snapshot {snapshot_id} scoring completed")
        
        # Extract normalization validation results
        normalization_validation = processing_results.get('normalization_validation', {})
        if normalization_validation.get('status') == 'issues_found':
            issues_count = len(normalization_validation.get('issues', []))
            logger.warning(f"⚠️ Completed processing despite {issues_count} normalization issues")
            logger.warning("💡 Consider re-running ingest pipeline with updated normalization")
        elif normalization_validation.get('status') == 'validation_error':
            logger.warning(f"⚠️ Normalization validation had errors: {normalization_validation.get('error')}")
        else:
            logger.info("✅ All input data was properly normalized")
        
        # Calculate totals
        total_records = sum(data_tables.values()) if data_tables else 0
        unit_records = processing_results.get('unit_records', 0)
        history_records = processing_results.get('history_records', 0)
        
        # Register completion
        completion_notes = f"Units: {unit_records}, History: {history_records}, Mapping: {mapping_count}"
        if normalization_validation.get('status') == 'issues_found':
            completion_notes += f", Normalization issues: {len(normalization_validation.get('issues', []))}"
        
        register_scoring_completion(snapshot_id, total_records, completion_notes)
        
        # Build success result
        total_time = (datetime.utcnow() - start_time).total_seconds()
        
        result = {
            "status": "success",
            "snapshot_id": snapshot_id,
            "processed_records": total_records,
            "pipeline_units": unit_records,
            "score_history": history_records,
            "stage_mapping": mapping_count,
            "processing_time_seconds": total_time,
            "timestamp": datetime.utcnow().isoformat(),
            "normalization_validation": normalization_validation,
            "normalization_enabled": True
        }
        
        logger.info(f"🎉 Scoring completed successfully for {snapshot_id}")
        logger.info(f"📊 Final results: {result}")
        
        # Log normalization summary
        if normalization_validation.get('status') == 'issues_found':
            logger.info(f"🔧 Normalization summary: {len(normalization_validation.get('issues', []))} issues detected and handled")
        else:
            logger.info(f"🔧 Normalization summary: All data properly normalized")
        
        return result
        
    except Exception as e:
        total_time = (datetime.utcnow() - start_time).total_seconds()
        error_msg = str(e)
        
        logger.error(f"❌ Scoring failed for snapshot {snapshot_id}: {error_msg}", exc_info=True)
        
        # Register failure
        register_scoring_failure(snapshot_id, error_msg)
        
        # Return error result
        return {
            "status": "error",
            "snapshot_id": snapshot_id,
            "error": error_msg,
            "processing_time_seconds": total_time,
            "timestamp": datetime.utcnow().isoformat(),
            "normalization_enabled": True
        }


if __name__ == "__main__":
    # For standalone testing
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    # Initialize environment
    init_env()
    
    # Test with mock event data
    test_event_data = {
        "snapshot_id": "2025-06-07T10:00:00",
        "data_tables": {"hs_companies": 50, "hs_deals": 30},
        "reference_tables": {"hs_owners": 6, "hs_deal_stage_reference": 13}
    }
    
    result = process_snapshot_event(test_event_data)
    print(f"Test result: {result}")