# src/scoring_main.py

import logging
import json
import base64
from datetime import datetime

def main(event, context):
    """
    Scoring Cloud Function entry point for Pub/Sub triggers
    
    Args:
        event: Pub/Sub event data containing base64 encoded message
        context: Cloud Function context (unused)
        
    Returns:
        dict: Response with status and processing details
    """
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True
    )
    logger = logging.getLogger('hubspot.scoring.cloudfunction')
    
    logger.info("📊 Scoring Cloud Function triggered via Pub/Sub")
    
    try:
        # Parse Pub/Sub event
        message = parse_pubsub_event(event)
        if not message:
            logger.error("❌ Could not parse Pub/Sub event data")
            return {"status": "error", "message": "Invalid event data"}
        
        logger.info(f"📤 Received event type: {message.get('type', 'unknown')}")
        
        # Check if this is the event we care about
        if message.get('type') != 'hubspot.snapshot.completed':
            logger.info(f"ℹ️ Ignoring event type: {message.get('type')}")
            return {"status": "ignored", "event_type": message.get('type')}
        
        # Extract and validate event data
        event_data = message.get('data', {})
        if not event_data.get('snapshot_id'):
            logger.error("❌ No snapshot_id in event data")
            return {"status": "error", "message": "Missing snapshot_id"}
        
        # Initialize scoring environment
        from hubspot_pipeline.hubspot_scoring.config import init_env
        init_env(log_level='INFO')
        
        # Process the scoring event
        from hubspot_pipeline.hubspot_scoring.main import process_snapshot_event
        result = process_snapshot_event(event_data)
        
        logger.info(f"🎉 Scoring function completed with status: {result.get('status')}")
        return result
        
    except Exception as e:
        logger.error(f"❌ Scoring function failed: {e}", exc_info=True)
        
        return {
            "status": "error",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


def parse_pubsub_event(event):
    """Parse Pub/Sub event data"""
    logger = logging.getLogger('hubspot.scoring.cloudfunction')
    
    try:
        if 'data' in event:
            message_data = base64.b64decode(event['data']).decode('utf-8')
            message = json.loads(message_data)
            logger.debug(f"Parsed Pub/Sub message: {message.get('type', 'unknown')}")
            return message
        else:
            logger.warning("No data field in event")
            return None
    except Exception as e:
        logger.error(f"Failed to parse Pub/Sub event: {e}")
        return None


if __name__ == "__main__":
    # For local testing
    print("🧪 Testing scoring function locally")
    
    # Create a test event
    test_event = {
        'data': base64.b64encode(json.dumps({
            "type": "hubspot.snapshot.completed",
            "data": {
                "snapshot_id": "2025-06-07T10:00:00",
                "data_tables": {"hs_companies": 50, "hs_deals": 30},
                "reference_tables": {"hs_owners": 6, "hs_deal_stage_reference": 13}
            }
        }).encode('utf-8'))
    }
    
    result = main(test_event, None)
    print(f"Test result: {result}")