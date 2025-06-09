# ===============================================================================
# src/tests/__init__.py
# Two-Tier Environment Validation Framework + Integration Tests - FIXED VERSION
# ===============================================================================

import logging
import json
import sys
import io
import os
import tempfile
import re
from contextlib import redirect_stdout, redirect_stderr
from typing import Dict, Any, Optional

def run_production_tests(test_type: str = 'deployment', 
                        function_type: str = 'unknown', 
                        **kwargs) -> Dict[str, Any]:
    """
    Two-tier environment validation + integration testing entry point.
    
    Tier 1: deployment_validation - "Will THIS deployment work in THIS environment?"
    Tier 2: runtime_validation - "Can the Python code execute at all?"
    Integration: integration_tests - "Does the end-to-end pipeline work?"
    
    Args:
        test_type: 'deployment' (environment-specific), 'runtime' (basic sanity), or 'integration' (e2e)
        function_type: 'ingest' or 'scoring' for function-specific validation
        **kwargs: Additional parameters including request_data
        
    Returns:
        Dictionary with test results and summary
    """
    # Set up logger for the test framework itself
    logger = logging.getLogger('hubspot.test.framework')
    logger.info(f"🧪 Test framework starting: test_type={test_type}, function_type={function_type}")
    
    # Set up logging for pytest subprocess
    logger.info("🔧 Configuring pytest logging...")
    _setup_pytest_logging()
    logger.info("✅ Pytest logging configuration complete")
    
    try:
        import pytest
        logger.info(f"📦 Pytest imported successfully, version available")
    except ImportError:
        logger.error("❌ Pytest import failed - not available in this environment")
        return {
            'status': 'error',
            'error': 'pytest not available - install with: pip install pytest',
            'test_type': test_type,
            'function_type': function_type
        }
    
    # Extract limit for integration tests (thread-safe parameter passing)
    limit = None
    if test_type == 'integration':
        logger.info("🔍 Extracting limit for integration tests...")
        limit = _extract_limit(kwargs)
        logger.info(f"📊 Limit set to: {limit}")
    
    # Build pytest arguments based on test tier
    logger.info(f"⚙️ Building pytest arguments for {test_type} tests...")
    pytest_args = _build_pytest_args(test_type, function_type, limit)
    logger.info(f"🔧 Pytest args: {' '.join(pytest_args)}")
    
    # Capture pytest output
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    
    # Disable JSON report for Cloud Functions compatibility
    json_report_available = False
    results_file = None
    logger.info("📝 Using output parser (JSON report disabled for Cloud Functions)")
    
    try:
        # Pass limit as pytest parameter (thread-safe)
        if limit is not None:
            logger.info(f"📊 Passing limit as parameter: {limit}")
            # Add limit to pytest args as a custom option
            pytest_args.extend(['--limit', str(limit)])
        
        # Log execution details
        logger.info(f"🚀 Executing pytest with {len(pytest_args)} arguments")
        logger.info(f"📁 Test directory: {os.path.dirname(__file__)}")
        
        # Capture output but DON'T redirect stderr (so logging works)
        with redirect_stdout(stdout_capture):
            # Only capture stderr for non-integration tests to preserve logging
            if test_type != 'integration':
                logger.info("📝 Capturing stderr for clean output")
                with redirect_stderr(stderr_capture):
                    logger.info("▶️ Starting pytest execution...")
                    exit_code = pytest.main(pytest_args)
            else:
                # For integration tests, let stderr through for logging
                logger.info("📝 Preserving stderr for integration test logging")
                logger.info("▶️ Starting pytest execution with logging enabled...")
                exit_code = pytest.main(pytest_args)
                # Manually capture what we can from pytest's own output
                stderr_capture.write("(stderr not captured for integration tests to preserve logging)")
        
        logger.info(f"🏁 Pytest execution completed with exit code: {exit_code}")
        
        # Parse results
        logger.info("📋 Parsing test results...")
        test_results = _parse_pytest_output_fallback(
            stdout_capture.getvalue(), 
            stderr_capture.getvalue(), 
            exit_code
        )
        
        # Log summary
        summary = test_results.get('summary', {})
        logger.info(f"📊 Test Summary: {summary.get('total', 0)} total, "
                   f"{summary.get('passed', 0)} passed, "
                   f"{summary.get('failed', 0)} failed, "
                   f"{summary.get('skipped', 0)} skipped")
        
        if test_results['status'] == 'success':
            logger.info("✅ All tests passed successfully")
        elif test_results['status'] == 'partial_success':
            logger.warning("⚠️ Some tests failed but execution completed")
        else:
            logger.error(f"❌ Test execution failed: {test_results.get('error', 'Unknown error')}")
        
        test_results.update({
            'test_type': test_type,
            'function_type': function_type,
            'validation_tier': test_type,  # For integration, tier == test_type
            'stdout': stdout_capture.getvalue(),
            'stderr': stderr_capture.getvalue() if test_type != 'integration' else None
        })
        
        return test_results
        
    except Exception as e:
        logger.error(f"💥 Test framework exception: {e}", exc_info=True)
        return {
            'status': 'error',
            'error': str(e),
            'test_type': test_type,
            'function_type': function_type,
            'stdout': stdout_capture.getvalue(),
            'stderr': stderr_capture.getvalue()
        }
    finally:
        # Cleanup temp file (no longer used)
        if results_file:
            try:
                os.unlink(results_file)
                logger.info(f"🧹 Cleaned up temp results file: {results_file}")
            except Exception as cleanup_error:
                logger.warning(f"⚠️ Failed to cleanup temp file: {cleanup_error}")

def _setup_pytest_logging():
    """Configure logging for pytest execution"""
    setup_logger = logging.getLogger('hubspot.test.setup')
    setup_logger.info("🔧 Setting up pytest logging configuration...")
    
    # Get the root logger used by Cloud Functions
    root_logger = logging.getLogger()
    setup_logger.info(f"📊 Root logger level: {root_logger.level}, handlers: {len(root_logger.handlers)}")
    
    # If no handlers exist, set up basic config
    if not root_logger.handlers:
        setup_logger.info("🔨 No handlers found, setting up basic logging config...")
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            force=True
        )
        setup_logger.info("✅ Basic logging configuration applied")
    else:
        setup_logger.info(f"✅ Found {len(root_logger.handlers)} existing handlers")
    
    # Set up specific loggers that tests might use
    test_logger = logging.getLogger('hubspot.ingest.test')
    test_logger.setLevel(logging.INFO)
    setup_logger.info(f"✅ Configured test logger: {test_logger.name}")
    
    # Ensure pytest's logging works
    logging.getLogger('pytest').setLevel(logging.INFO)
    setup_logger.info("✅ Configured pytest logger")
    
    # Set environment variable to ensure pytest shows logging output
    os.environ['PYTEST_DISABLE_PLUGIN_AUTOLOAD'] = '0'
    setup_logger.info("✅ Pytest environment configured for logging")

def _extract_limit(kwargs: Dict[str, Any]) -> Optional[int]:
    """Extract limit from request/event data (thread-safe)"""
    # Try different sources for limit
    limit = None
    
    # From request_data (HTTP requests) - primary source
    # Note: HTTP API uses 'record_limit' but pipeline expects 'limit'
    request_data = kwargs.get('request_data', {})
    if isinstance(request_data, dict):
        limit = request_data.get('record_limit')  # Extract from HTTP request
        if limit is None:
            # Also check for no_limit flag
            if request_data.get('no_limit'):
                limit = 0  # 0 means no limit
    
    # From event_data (Pub/Sub events) if not found in request_data
    if limit is None:
        event_data = kwargs.get('event_data', {})
        if isinstance(event_data, dict):
            limit = event_data.get('record_limit')
            if limit is None and event_data.get('no_limit'):
                limit = 0
    
    # Validate and convert to int
    if limit is not None:
        try:
            return int(limit)
        except (ValueError, TypeError):
            logging.warning(f"Invalid limit value: {limit}")
            return None
    
    return None

def _build_pytest_args(test_type: str, function_type: str, limit: Optional[int]) -> list:
    """Build pytest command line arguments for three-tier testing"""
    
    args = [
        '--tb=short',           # Short traceback format
        '--no-header',          # No pytest header  
        '-v',                   # Verbose test names
    ]
    
    # For integration tests, show logging output
    if test_type == 'integration':
        args.extend([
            '--log-cli-level=INFO',     # Show log messages at INFO level and above
            '--log-cli-format=%(asctime)s [%(levelname)8s] %(name)s: %(message)s',
            '--log-cli-date-format=%Y-%m-%d %H:%M:%S',
            '--capture=no',             # Don't capture output (shows logs immediately)
            '-s',                       # Don't capture stdout/stderr (alternative to --capture=no)
        ])
    else:
        # For deployment/runtime tests, keep output clean
        args.extend([
            '--quiet',              # Quiet output
            '--disable-warnings',   # Clean output
        ])
    
    # Add custom options
    args.extend(['--function-type', function_type])
    
    # Add environment detection
    env = _detect_environment()
    args.extend(['--environment', env])
    
    # Specify test directory relative to this file
    test_dir = os.path.dirname(__file__)
    
    # Select appropriate test file based on tier
    if test_type == 'deployment':
        # Tier 1: Environment-specific deployment validation
        test_file = os.path.join(test_dir, 'deployment_validation.py')
        args.append(test_file)
    elif test_type == 'runtime':
        # Tier 2: Basic runtime/mechanism validation
        test_file = os.path.join(test_dir, 'runtime_validation.py')
        args.append(test_file)
    elif test_type == 'integration':
        # Integration: End-to-end pipeline testing
        test_file = os.path.join(test_dir, 'integration_tests.py')
        args.append(test_file)
        # Only run the main integration test
        args.extend(['-k', 'test_end_to_end_pipeline_with_limit'])
    else:
        # Fallback: run both deployment and runtime tiers
        args.append(test_dir)
        args.extend(['-m', 'production_safe'])
    
    return args

def _detect_environment() -> str:
    """Detect current environment from Cloud Function context"""
    function_name = os.getenv('K_SERVICE', '')
    if 'prod' in function_name:
        return 'production'
    elif 'staging' in function_name:
        return 'staging'
    else:
        return 'development'

def _parse_pytest_output_fallback(stdout: str, stderr: str, exit_code: int) -> Dict[str, Any]:
    """Parse pytest output when JSON report is not available"""
    # Try to extract test results from stdout
    total_tests = 0
    passed = 0
    failed = 0
    skipped = 0
    
    # Look for pytest summary line like "3 passed, 1 failed, 2 skipped in 1.23s"
    summary_pattern = r'(\d+)\s+passed|(\d+)\s+failed|(\d+)\s+skipped'
    matches = re.findall(summary_pattern, stdout)
    
    for match in matches:
        if match[0]:  # passed
            passed = int(match[0])
        elif match[1]:  # failed  
            failed = int(match[1])
        elif match[2]:  # skipped
            skipped = int(match[2])
    
    total_tests = passed + failed + skipped
    
    # Extract test names from output
    test_pattern = r'([a-zA-Z_][a-zA-Z0-9_]*\.py)::[a-zA-Z_][a-zA-Z0-9_]* (PASSED|FAILED|SKIPPED)'
    test_matches = re.findall(test_pattern, stdout)
    
    tests = []
    for match in test_matches:
        test_name = match[0].replace('.py', '')
        outcome = match[1].lower()
        tests.append({
            'name': test_name,
            'outcome': outcome,
            'duration': 0,
            'error': None
        })
    
    # Determine overall status
    if exit_code == 0 and failed == 0:
        status = 'success'
    elif exit_code == 0 and failed > 0:
        status = 'partial_success'
    else:
        status = 'failed'
    
    return {
        'status': status,
        'exit_code': exit_code,
        'summary': {
            'total': total_tests,
            'passed': passed,
            'failed': failed,
            'skipped': skipped,
            'duration': 0
        },
        'tests': tests,
        'environment': {},
        'pytest_version': 'unknown',
        'fallback_parser': True
    }

def _parse_pytest_results(results_file: str, exit_code: int) -> Dict[str, Any]:
    """Parse pytest JSON results file"""
    try:
        with open(results_file, 'r') as f:
            pytest_data = json.load(f)
        
        # Handle both old and new JSON report formats
        if 'report' in pytest_data:
            report = pytest_data['report']
        else:
            report = pytest_data
        
        summary = report.get('summary', {})
        
        # Extract key metrics
        total_tests = summary.get('total', 0)
        passed = summary.get('passed', 0)
        failed = summary.get('failed', 0)
        skipped = summary.get('skipped', 0)
        
        # Determine overall status
        if exit_code == 0 and failed == 0:
            status = 'success'
        elif exit_code == 0 and failed > 0:
            status = 'partial_success'  # Some tests passed but some failed
        else:
            status = 'failed'
        
        # Extract test details
        tests = []
        for test in report.get('tests', []):
            tests.append({
                'name': test.get('nodeid', '').split('::')[-1],  # Just test name
                'outcome': test.get('outcome'),
                'duration': test.get('duration', 0),
                'error': test.get('call', {}).get('longrepr') if test.get('outcome') == 'failed' else None
            })
        
        return {
            'status': status,
            'exit_code': exit_code,
            'summary': {
                'total': total_tests,
                'passed': passed,
                'failed': failed,
                'skipped': skipped,
                'duration': report.get('duration', 0)
            },
            'tests': tests,
            'environment': pytest_data.get('environment', {}),
            'pytest_version': pytest_data.get('pytest_version')
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'exit_code': exit_code,
            'error': f'Failed to parse pytest results: {str(e)}',
            'summary': {'total': 0, 'passed': 0, 'failed': 0, 'skipped': 0}
        }