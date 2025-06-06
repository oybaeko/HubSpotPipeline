# src/hubspot_pipeline/hubspot_ingest/reference/main.py

import logging
from typing import Dict

from .fetchers import fetch_owners, fetch_deal_stages
from .store import replace_owners, replace_deal_stages

def update_reference_data() -> Dict[str, int]:
    """
    Update all reference data (owners and deal stages).
    
    Returns:
        Dictionary with counts of updated records by table
    """
    logger = logging.getLogger('hubspot.reference')
    
    logger.info("🔄 Starting reference data update")
    
    reference_counts = {}
    
    # Update owners
    try:
        logger.info("👥 Updating owners...")
        owners_data = fetch_owners()
        owners_count = replace_owners(owners_data)
        reference_counts['hs_owners'] = owners_count
        logger.info(f"✅ Updated {owners_count} owners")
        
    except Exception as e:
        logger.error(f"❌ Failed to update owners: {e}")
        reference_counts['hs_owners'] = 0
        # Continue with other reference data
    
    # Update deal stages
    try:
        logger.info("📋 Updating deal stages...")
        stages_data = fetch_deal_stages()
        stages_count = replace_deal_stages(stages_data)
        reference_counts['hs_deal_stage_reference'] = stages_count
        logger.info(f"✅ Updated {stages_count} deal stages")
        
    except Exception as e:
        logger.error(f"❌ Failed to update deal stages: {e}")
        reference_counts['hs_deal_stage_reference'] = 0
    
    # Summary
    total_updated = sum(reference_counts.values())
    logger.info(f"🎉 Reference data update completed: {total_updated} total records")
    logger.info(f"📊 Breakdown: {reference_counts}")
    
    return reference_counts


if __name__ == "__main__":
    # For standalone testing
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    from ..config_loader import init_env
    
    # Initialize environment
    init_env()
    
    # Update reference data
    result = update_reference_data()
    print(f"Reference update result: {result}")