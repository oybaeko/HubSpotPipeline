# ===============================================================================
# src/ingest_main.py - Updated with pytest testing integration
# ===============================================================================

import logging
from flask import Request

def main(request: Request):
    """
    Ingest Cloud Function entry point with integrated pytest testing
    
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
        
        # Log request details at debug level
        if logger.isEnabledFor(logging.DEBUG):
            safe_data = {k: v for k, v in data.items() if k not in ['api_key', 'token']}
            logger.debug(f"Full request data: {safe_data}")
            
    except Exception as e:
        logger.warning(f"Failed to parse JSON body: {e}")
        data = {}
    
    # Check for test mode
    if data.get('mode') == 'test':
        logger.info("🧪 Test mode detected - running pytest-based tests")
        return run_pytest_tests(data, logger)
    
    # Normal production logic
    try:
        from hubspot_pipeline.hubspot_ingest.main import main as ingest_main
        result = ingest_main(event=data)
        logger.info(f"✅ Ingest completed successfully")
        return result
    except Exception as e:
        logger.error(f"❌ Ingest failed: {e}", exc_info=True)
        return f"Ingest error: {e}", 500

def run_pytest_tests(data: dict, logger) -> tuple:
    """
    Run pytest-based tests and return formatted results
    
    Args:
        data: Request data containing test parameters
        logger: Logger instance
        
    Returns:
        tuple: (response_data, status_code)
    """
    test_type = data.get('test_type', 'infrastructure')
    
    logger.info(f"🧪 Running {test_type} tests via pytest framework")
    
    try:
        # Import and run tests
        from tests import run_production_tests
        
        test_results = run_production_tests(
            test_type=test_type,
            function_type='ingest',
            request_data=data
        )
        
        # Determine HTTP status code based on test results
        if test_results['status'] == 'success':
            status_code = 200
            logger.info(f"✅ Tests passed: {test_results['summary']['passed']}/{test_results['summary']['total']}")
        elif test_results['status'] == 'partial_success':
            status_code = 206  # Partial Content
            logger.warning(f"⚠️ Partial success: {test_results['summary']['failed']} tests failed")
        else:
            status_code = 500
            logger.error(f"❌ Tests failed: {test_results.get('error', 'Unknown error')}")
        
        # Format response for better readability
        formatted_response = {
            'test_mode': True,
            'function_type': 'ingest',
            'test_type': test_type,
            'status': test_results['status'],
            'summary': test_results['summary'],
            'environment': _detect_environment(),
            'timestamp': _get_timestamp(),
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
        
        # Add debug info if test failed
        if test_results['status'] != 'success' and test_results.get('stderr'):
            formatted_response['debug'] = {
                'stderr': test_results['stderr'][:1000],  # Limit length
                'exit_code': test_results.get('exit_code')
            }
        
        return formatted_response, status_code
        
    except ImportError as e:
        logger.error(f"❌ Testing framework not available: {e}")
        return {
            'test_mode': True,
            'status': 'error',
            'error': 'pytest testing framework not available in this deployment',
            'suggestion': 'This function was deployed without testing framework. Redeploy with tests included.',
            'import_error': str(e)
        }, 501  # Not Implemented
        
    except Exception as e:
        logger.error(f"❌ Test execution failed: {e}", exc_info=True)
        return {
            'test_mode': True,
            'status': 'error', 
            'error': str(e),
            'function_type': 'ingest',
            'test_type': test_type
        }, 500

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