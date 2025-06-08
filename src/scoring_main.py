# ===============================================================================
# src/scoring_main.py - Updated with pytest testing integration
# ===============================================================================

import logging
import json
import base64
from datetime import datetime
import functions_framework

@functions_framework.cloud_event
def main(cloud_event):
    """
    Scoring Cloud Function entry point with integrated pytest testing
    
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
        # Parse CloudEvent
        message = parse_cloud_event(cloud_event)
        if not message:
            logger.error("❌ Could not parse CloudEvent data")
            return {"status": "error", "message": "Invalid event data"}
        
        event_type = message.get('type', 'unknown')
        logger.info(f"📤 Received event type: {event_type}")
        
        # Check for test request
        if event_type == 'hubspot.test.request':
            logger.info("🧪 Test mode detected - running pytest-based tests")
            return run_pytest_tests(message, logger)
        
        # Check if this is the event we care about
        if event_type != 'hubspot.snapshot.completed':
            logger.info(f"ℹ️ Ignoring event type: {event_type}")
            return {"status": "ignored", "event_type": event_type}
        
        # Extract and validate event data
        event_data = message.get('data', {})
        if not event_data.get('snapshot_id'):
            logger.error("❌ No snapshot_id in event data")
            return {"status": "error", "message": "Missing snapshot_id"}
        
        # Normal scoring logic
        from hubspot_pipeline.hubspot_scoring.config import init_env
        init_env(log_level='INFO')
        
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

def run_pytest_tests(message: dict, logger) -> dict:
    """
    Run pytest-based tests for scoring function
    
    Args:
        message: Parsed Pub/Sub message containing test parameters
        logger: Logger instance
        
    Returns:
        dict: Test results
    """
    test_data = message.get('data', {})
    test_type = test_data.get('test_type', 'infrastructure')
    
    logger.info(f"🧪 Running {test_type} tests via pytest framework")
    
    try:
        # Import and run tests
        from tests import run_production_tests
        
        test_results = run_production_tests(
            test_type=test_type,
            function_type='scoring',
            event_data=test_data
        )
        
        # Log results
        if test_results['status'] == 'success':
            logger.info(f"✅ Tests passed: {test_results['summary']['passed']}/{test_results['summary']['total']}")
        elif test_results['status'] == 'partial_success':
            logger.warning(f"⚠️ Partial success: {test_results['summary']['failed']} tests failed")
        else:
            logger.error(f"❌ Tests failed: {test_results.get('error', 'Unknown error')}")
        
        # Format response for Pub/Sub context
        formatted_response = {
            'test_mode': True,
            'function_type': 'scoring',
            'test_type': test_type,
            'status': test_results['status'],
            'summary': test_results['summary'],
            'environment': _detect_environment_scoring(),
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'trigger_type': 'pubsub',
            'details': {
                'passed_tests': [t['name'] for t in test_results.get('tests', []) if t['outcome'] == 'passed'],
                'failed_tests': [
                    {'name': t['name'], 'error': t['error']} 
                    for t in test_results.get('tests', []) 
                    if t['outcome'] == 'failed'
                ],
                'skipped_tests': [t['name'] for t in test_results.get('tests', []) if t['outcome'] == 'skipped']
            }
        }
        
        return formatted_response
        
    except ImportError as e:
        logger.error(f"❌ Testing framework not available: {e}")
        return {
            'test_mode': True,
            'status': 'error',
            'error': 'pytest testing framework not available in this deployment',
            'suggestion': 'This function was deployed without testing framework. Redeploy with tests included.',
            'import_error': str(e)
        }
        
    except Exception as e:
        logger.error(f"❌ Test execution failed: {e}", exc_info=True)
        return {
            'test_mode': True,
            'status': 'error',
            'error': str(e),
            'function_type': 'scoring',
            'test_type': test_type
        }

def _detect_environment_scoring() -> str:
    """Detect current environment from Cloud Function context"""
    import os
    function_name = os.getenv('K_SERVICE', '')
    if 'prod' in function_name:
        return 'production'
    elif 'staging' in function_name:
        return 'staging'
    else:
        return 'development'

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

# ===============================================================================
# Helper function to test the scoring function via Pub/Sub message
# ===============================================================================

def create_test_pubsub_message(test_type: str = 'infrastructure', **kwargs) -> dict:
    """
    Create a test Pub/Sub message for triggering scoring function tests
    
    Args:
        test_type: Type of test to run
        **kwargs: Additional test parameters
        
    Returns:
        dict: Formatted message for Pub/Sub publishing
    """
    import json
    import base64
    from datetime import datetime
    
    test_event = {
        "type": "hubspot.test.request",
        "version": "1.0",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "test-framework",
        "data": {
            "test_type": test_type,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            **kwargs
        }
    }
    
    # Encode for Pub/Sub
    message_json = json.dumps(test_event)
    message_data = base64.b64encode(message_json.encode('utf-8')).decode('utf-8')
    
    return {
        'data': message_data,
        'attributes': {
            'eventType': 'hubspot.test.request',
            'source': 'test-framework'
        }
    }