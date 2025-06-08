# src/scoring_main.py

import logging
import json
import base64
from datetime import datetime
import functions_framework

# Cloud Functions 2nd gen uses this decorator for Pub/Sub
@functions_framework.cloud_event
def main(cloud_event):
    """
    Scoring Cloud Function entry point for Pub/Sub triggers (2nd gen)
    
    Args:
        cloud_event: CloudEvent object containing Pub/Sub message
        
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
    
    logger.info("📊 Scoring Cloud Function triggered via Pub/Sub (2nd gen)")
    
    try:
        # Parse CloudEvent (2nd gen format)
        message = parse_cloud_event(cloud_event)
        if not message:
            logger.error("❌ Could not parse CloudEvent data")
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


def parse_cloud_event(cloud_event):
    """Parse CloudEvent from 2nd gen Cloud Functions"""
    logger = logging.getLogger('hubspot.scoring.cloudfunction')
    
    try:
        # CloudEvent has data attribute with Pub/Sub message
        if hasattr(cloud_event, 'data') and cloud_event.data:
            # The data contains the Pub/Sub message
            pubsub_message = cloud_event.data
            
            # Message data is base64 encoded
            if 'message' in pubsub_message:
                message_data = pubsub_message['message'].get('data', '')
                if message_data:
                    # Decode base64 and parse JSON
                    decoded_data = base64.b64decode(message_data).decode('utf-8')
                    message = json.loads(decoded_data)
                    logger.debug(f"Parsed CloudEvent message: {message.get('type', 'unknown')}")
                    return message
            
            # Fallback: try direct JSON parsing
            logger.debug("Trying direct JSON parsing of CloudEvent data")
            return json.loads(str(pubsub_message))
            
        logger.warning("No data found in CloudEvent")
        return None
        
    except Exception as e:
        logger.error(f"Failed to parse CloudEvent: {e}")
        logger.debug(f"CloudEvent type: {type(cloud_event)}")
        logger.debug(f"CloudEvent attributes: {dir(cloud_event)}")
        if hasattr(cloud_event, 'data'):
            logger.debug(f"CloudEvent data: {cloud_event.data}")
        return None


# Legacy entry point for 1st gen (in case we need it)
def main_legacy(event, context):
    """
    Legacy entry point for 1st gen Cloud Functions
    """
    logger = logging.getLogger('hubspot.scoring.cloudfunction')
    logger.info("📊 Scoring Cloud Function triggered via Pub/Sub (1st gen)")
    
    try:
        # Parse 1st gen Pub/Sub event
        message = parse_pubsub_event_legacy(event)
        if not message:
            return {"status": "error", "message": "Invalid event data"}
        
        # Same processing logic
        from hubspot_pipeline.hubspot_scoring.config import init_env
        init_env(log_level='INFO')
        
        from hubspot_pipeline.hubspot_scoring.main import process_snapshot_event
        event_data = message.get('data', {})
        result = process_snapshot_event(event_data)
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Legacy scoring function failed: {e}", exc_info=True)
        return {"status": "error", "error": str(e)}


def parse_pubsub_event_legacy(event):
    """Parse Pub/Sub event data from 1st gen Cloud Functions"""
    logger = logging.getLogger('hubspot.scoring.cloudfunction')
    
    try:
        if 'data' in event:
            message_data = base64.b64decode(event['data']).decode('utf-8')
            message = json.loads(message_data)
            logger.debug(f"Parsed legacy Pub/Sub message: {message.get('type', 'unknown')}")
            return message
        else:
            logger.warning("No data field in legacy event")
            return None
    except Exception as e:
        logger.error(f"Failed to parse legacy Pub/Sub event: {e}")
        return None


if __name__ == "__main__":
    # For local testing
    print("🧪 Testing scoring function locally")
    
    # Create a test CloudEvent-like object
    class MockCloudEvent:
        def __init__(self, data):
            self.data = data
    
    test_data = {
        'message': {
            'data': base64.b64encode(json.dumps({
                "type": "hubspot.snapshot.completed",
                "data": {
                    "snapshot_id": "2025-06-07T10:00:00",
                    "data_tables": {"hs_companies": 50, "hs_deals": 30},
                    "reference_tables": {"hs_owners": 6, "hs_deal_stage_reference": 13}
                }
            }).encode('utf-8')).decode('utf-8')
        }
    }
    
    mock_event = MockCloudEvent(test_data)
    result = main(mock_event)
    print(f"Test result: {result}")