# ===============================================================================
# src/scoring_main.py - Updated with simplified two-tier testing
# ===============================================================================

import logging
import json
import base64
from datetime import datetime
import functions_framework

@functions_framework.cloud_event
def main(cloud_event):
    """
    Scoring Cloud Function entry point with two-tier testing framework
    
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
            logger.info("🧪 Test mode detected - running two-tier validation")
            return run_two_tier_tests_scoring(message, logger)
        
        # Check if this is the event we care about
        if event_type != 'hubspot.snapshot.completed':
            logger.info(f"ℹ️ Ignoring event type: {event_type}")
            return {"status": "ignored", "event_type": event_type}
        
        # Normal scoring logic
        from hubspot_pipeline.hubspot_scoring.config import init_env
        init_env(log_level='INFO')
        
        from hubspot_pipeline.hubspot_scoring.main import process_snapshot_event
        event_data = message.get('data', {})
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

def run_two_tier_tests_scoring(message: dict, logger) -> dict:
    """
    Run two-tier validation tests for scoring function
    
    Args:
        message: Parsed Pub/Sub message containing test parameters
        logger: Logger instance
        
    Returns:
        dict: Test results
    """
    test_data = message.get('data', {})
    test_type = test_data.get('test_type', 'deployment')
    
    # Map old test types to new two-tier system
    if test_type in ['infrastructure', 'database', 'events', 'logging', 'all_safe']:
        tier = 'deployment'  # Tier 1: Environment-specific validation
    elif test_type in ['runtime', 'mechanisms']:
        tier = 'runtime'     # Tier 2: Basic runtime validation
    else:
        tier = 'deployment'  # Default to deployment validation
    
    logger.info(f"🧪 Running {tier} validation (test_type: {test_type})")
    
    try:
        # Import and run tests
        from tests import run_production_tests
        
        test_results = run_production_tests(
            test_type=tier,
            function_type='scoring',
            event_data=test_data
        )
        
        # Log results
        if test_results['status'] == 'success':
            logger.info(f"✅ {tier.title()} validation passed: {test_results['summary']['passed']}/{test_results['summary']['total']}")
        elif test_results['status'] == 'partial_success':
            logger.warning(f"⚠️ {tier.title()} validation partial: {test_results['summary']['failed']} tests failed")
        else:
            logger.error(f"❌ {tier.title()} validation failed: {test_results.get('error', 'Unknown error')}")
        
        # Format response for Pub/Sub context
        formatted_response = {
            'test_mode': True,
            'validation_tier': tier,
            'function_type': 'scoring',
            'test_type': test_type,
            'status': test_results['status'],
            'summary': test_results['summary'],
            'environment': _detect_environment_scoring(),
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'trigger_type': 'pubsub',
            'framework_info': {
                'tier_1_deployment': 'Environment-specific validation',
                'tier_2_runtime': 'Basic mechanism validation',
                'current_tier': tier
            },
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
            'error': 'Two-tier testing framework not available in this deployment',
            'validation_tier': tier,
            'suggestion': 'Redeploy with updated deploy.sh script that includes tests directory',
            'import_error': str(e)
        }
        
    except Exception as e:
        logger.error(f"❌ Test execution failed: {e}", exc_info=True)
        return {
            'test_mode': True,
            'status': 'error',
            'error': str(e),
            'validation_tier': tier,
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
        return None