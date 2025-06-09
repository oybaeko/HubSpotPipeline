# ===============================================================================
# src/ingest_main.py - Updated with simplified two-tier testing (legacy removed)
# ===============================================================================

import logging
import sys
import os
from flask import Request

def main(request: Request):
    """
    Ingest Cloud Function entry point with two-tier testing framework
    
    Args:
        request: Flask Request object containing HTTP request data
        
    Returns:
        tuple: (response_data, status_code) or dict for testing
    """
    # Basic logging setup (will be reconfigured by init_env)
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('hubspot.ingest.cloudfunction')
    
    logger.info("🌐 Ingest Cloud Function HTTP trigger received")
    
    # Parse request data
    try:
        data = request.get_json(silent=True) or {}
        logger.info(f"📦 Parsed request data keys: {list(data.keys())}")
    except Exception as e:
        logger.warning(f"Failed to parse JSON body: {e}")
        data = {}
    
    # Check for test mode
    if data.get('mode') == 'test':
        logger.info("🧪 Test mode detected - running two-tier validation")
        return run_two_tier_tests(data, logger, 'ingest')
    
    # Normal production logic
    try:
        from hubspot_pipeline.hubspot_ingest.main import main as ingest_main
        result = ingest_main(event=data)
        logger.info(f"✅ Ingest completed successfully")
        return result
    except Exception as e:
        logger.error(f"❌ Ingest failed: {e}", exc_info=True)
        return f"Ingest error: {e}", 500

def run_two_tier_tests(data: dict, logger, function_type: str) -> tuple:
    """
    Run two-tier validation tests
    
    Args:
        data: Request data containing test parameters
        logger: Logger instance
        function_type: Type of function ('ingest' or 'scoring')
        
    Returns:
        tuple: (response_data, status_code)
    """
    test_type = data.get('test_type', 'deployment')
    
    # Validate test type - allow deployment, runtime, or integration
    if test_type not in ['deployment', 'runtime', 'integration']:
        logger.warning(f"Invalid test_type '{test_type}', defaulting to 'deployment'")
        tier = 'deployment'
    else:
        tier = test_type
    
    logger.info(f"🧪 Running {tier} validation (test_type: {test_type})")
    
    try:
        # Add current directory to Python path for imports
        current_dir = os.path.dirname(os.path.abspath(__file__))
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)
        
        # Import the two-tier testing framework
        try:
            from tests import run_production_tests
        except ImportError as import_error:
            logger.error(f"❌ Import error: {import_error}")
            
            return {
                'test_mode': True,
                'status': 'error',
                'error': 'Two-tier testing framework not available in this deployment',
                'error_type': 'ImportError',
                'suggestion': 'Redeploy with updated deploy.sh script that includes tests directory',
                'tier': tier,
                'function_type': function_type
            }, 501
        
        # Run appropriate tier
        test_results = run_production_tests(
            test_type=tier,
            function_type=function_type,
            request_data=data
        )
        
        # Determine HTTP status code
        if test_results['status'] == 'success':
            status_code = 200
            logger.info(f"✅ {tier.title()} validation passed: {test_results['summary']['passed']}/{test_results['summary']['total']}")
        elif test_results['status'] == 'partial_success':
            status_code = 206  # Partial Content
            logger.warning(f"⚠️ {tier.title()} validation partial: {test_results['summary']['failed']} tests failed")
        else:
            status_code = 500
            logger.error(f"❌ {tier.title()} validation failed: {test_results.get('error', 'Unknown error')}")
        
        # Format response
        formatted_response = {
            'test_mode': True,
            'validation_tier': tier,
            'function_type': function_type,
            'test_type': test_type,
            'status': test_results['status'],
            'summary': test_results['summary'],
            'environment': _detect_environment(),
            'timestamp': _get_timestamp(),
            'framework_info': {
                'tier_1_deployment': 'Environment-specific validation',
                'tier_2_runtime': 'Basic mechanism validation',
                'current_tier': tier,
                'test_discovery': f"Found {test_results['summary']['total']} tests"
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
        
        return formatted_response, status_code
        
    except Exception as e:
        logger.error(f"❌ Test execution failed: {e}", exc_info=True)
        
        error_response = {
            'test_mode': True,
            'status': 'error', 
            'error': str(e),
            'error_type': type(e).__name__,
            'validation_tier': tier,
            'function_type': function_type,
            'test_type': test_type
        }
        
        return error_response, 500

def _detect_environment() -> str:
    """Detect current environment from Cloud Function context"""
    import os
    function_name = os.getenv('K_SERVICE', '')
    if 'prod' in function_name:
        return 'production'
    elif 'staging' in function_name:
        return 'staging'
    else:
        return 'development'

def _get_timestamp() -> str:
    """Get current timestamp in ISO format"""
    from datetime import datetime
    return datetime.utcnow().isoformat() + 'Z'