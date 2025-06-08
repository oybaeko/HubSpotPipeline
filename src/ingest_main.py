# ===============================================================================
# src/ingest_main.py - Updated with pytest testing integration and better error handling
# ===============================================================================

import logging
import sys
import os
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
        # Add current directory to Python path for imports
        current_dir = os.path.dirname(os.path.abspath(__file__))
        if current_dir not in sys.path:
            sys.path.insert(0, current_dir)
        
        # Try to import the testing framework
        try:
            from tests import run_production_tests
        except ImportError as import_error:
            logger.error(f"❌ Import error: {import_error}")
            
            # Provide detailed debugging information
            debug_info = {
                'cwd': os.getcwd(),
                'file_location': __file__,
                'sys_path': sys.path[:5],  # First 5 entries
                'available_modules': [],
                'import_error': str(import_error)
            }
            
            # Check what's available in current directory
            try:
                files = os.listdir('.')
                debug_info['current_dir_contents'] = [f for f in files if not f.startswith('.')]
            except:
                debug_info['current_dir_contents'] = 'Unable to read'
            
            # Check if tests directory exists
            tests_exists = os.path.exists('tests')
            tests_init_exists = os.path.exists('tests/__init__.py')
            
            debug_info['tests_directory_exists'] = tests_exists
            debug_info['tests_init_exists'] = tests_init_exists
            
            if tests_exists:
                try:
                    tests_contents = os.listdir('tests')
                    debug_info['tests_contents'] = tests_contents
                except:
                    debug_info['tests_contents'] = 'Unable to read tests directory'
            
            return {
                'test_mode': True,
                'status': 'error',
                'error': 'pytest testing framework not available in this deployment',
                'error_type': 'ImportError',
                'suggestion': 'Tests directory may not be included in deployment or pytest not installed',
                'debug_info': debug_info,
                'solution': 'Redeploy with updated deploy.sh script that includes tests directory'
            }, 501  # Not Implemented
        
        # If import successful, run tests
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
            'framework_info': {
                'pytest_available': True,
                'test_discovery': f"Found {test_results['summary']['total']} tests",
                'execution_successful': test_results['status'] in ['success', 'partial_success']
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
        
        # Add debug info if test failed
        if test_results['status'] != 'success' and test_results.get('stderr'):
            formatted_response['debug'] = {
                'stderr': test_results['stderr'][:1000],  # Limit length
                'exit_code': test_results.get('exit_code')
            }
        
        return formatted_response, status_code
        
    except Exception as e:
        logger.error(f"❌ Test execution failed: {e}", exc_info=True)
        
        # Provide comprehensive error information
        error_response = {
            'test_mode': True,
            'status': 'error', 
            'error': str(e),
            'error_type': type(e).__name__,
            'function_type': 'ingest',
            'test_type': test_type,
            'troubleshooting': {
                'common_causes': [
                    'pytest not installed (missing from requirements.txt)',
                    'tests directory not included in deployment',
                    'Import path issues in Cloud Functions environment',
                    'Missing test dependencies'
                ],
                'solutions': [
                    'Ensure pytest is in requirements.txt',
                    'Verify .gcloudignore includes tests directory',
                    'Redeploy with updated deploy.sh script',
                    'Check Cloud Function logs for detailed error'
                ]
            }
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