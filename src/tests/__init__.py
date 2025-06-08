# ===============================================================================
# src/tests/__init__.py
# Main entry point for pytest-based testing framework
# ===============================================================================

import logging
import json
import sys
import io
from contextlib import redirect_stdout, redirect_stderr
from typing import Dict, Any, Optional

def run_production_tests(test_type: str = 'infrastructure', 
                        function_type: str = 'unknown', 
                        **kwargs) -> Dict[str, Any]:
    """
    Main entry point for production testing using pytest.
    
    Args:
        test_type: Type of tests to run ('infrastructure', 'database', 'events', 'all_safe')
        function_type: Context of which function is running ('ingest', 'scoring')
        **kwargs: Additional parameters passed to tests
        
    Returns:
        Dictionary with test results and summary
    """
    try:
        import pytest
    except ImportError:
        return {
            'status': 'error',
            'error': 'pytest not available - install with: pip install pytest',
            'test_type': test_type,
            'function_type': function_type
        }
    
    # Build pytest arguments
    pytest_args = _build_pytest_args(test_type, function_type, kwargs)
    
    # Capture pytest output
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    
    # Check if pytest-json-report is available
    json_report_available = False
    try:
        import pytest_jsonreport
        json_report_available = True
    except ImportError:
        pass
    
    # Set up temporary results file if JSON report available
    results_file = None
    if json_report_available:
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            results_file = f.name
        
        # Add JSON report to pytest args
        pytest_args.extend([
            '--json-report',
            f'--json-report-file={results_file}'
        ])
    
    try:
        # Capture output and run pytest
        with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
            exit_code = pytest.main(pytest_args)
        
        # Parse results
        if json_report_available and results_file:
            test_results = _parse_pytest_results(results_file, exit_code)
        else:
            test_results = _parse_pytest_output_fallback(
                stdout_capture.getvalue(), 
                stderr_capture.getvalue(), 
                exit_code
            )
            
        test_results.update({
            'test_type': test_type,
            'function_type': function_type,
            'stdout': stdout_capture.getvalue(),
            'stderr': stderr_capture.getvalue() if exit_code != 0 else None
        })
        
        return test_results
        
    except Exception as e:
        return {
            'status': 'error',
            'error': str(e),
            'test_type': test_type,
            'function_type': function_type,
            'stdout': stdout_capture.getvalue(),
            'stderr': stderr_capture.getvalue()
        }
    finally:
        # Cleanup temp file
        if results_file:
            try:
                os.unlink(results_file)
            except:
                pass

def _build_pytest_args(test_type: str, function_type: str, kwargs: Dict[str, Any]) -> list:
    """Build pytest command line arguments based on test type"""
    
    args = [
        '--tb=short',           # Short traceback format
        '--no-header',          # No pytest header  
        '--quiet',              # Quiet output
        '-v',                   # Verbose test names
        '--disable-warnings',   # Clean output
    ]
    
    # Add custom options
    args.extend(['--function-type', function_type])
    
    # Add environment detection
    env = _detect_environment()
    args.extend(['--environment', env])
    
    # Filter tests by type using markers
    if test_type == 'infrastructure':
        args.extend(['-m', 'infrastructure and production_safe'])
    elif test_type == 'database':
        args.extend(['-m', 'database and production_safe'])
    elif test_type == 'events':
        args.extend(['-m', 'events and production_safe'])
    elif test_type == 'logging':
        args.extend(['-m', 'logging and production_safe'])
    elif test_type == 'all_safe':
        args.extend(['-m', 'production_safe'])
    else:
        # Default to infrastructure if unknown type
        args.extend(['-m', 'infrastructure and production_safe'])
    
    # Specify test directory relative to this file
    import os
    test_dir = os.path.dirname(__file__)
    args.append(test_dir)
    
    return args

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

def _parse_pytest_output_fallback(stdout: str, stderr: str, exit_code: int) -> Dict[str, Any]:
    """Parse pytest output when JSON report is not available"""
    import re
    
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