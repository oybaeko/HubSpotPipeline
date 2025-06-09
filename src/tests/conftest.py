# ===============================================================================
# src/tests/conftest.py  
# Updated pytest configuration with limit parameter
# ===============================================================================

import pytest
import os
import uuid
import logging
from datetime import datetime
from typing import Generator, TYPE_CHECKING

if TYPE_CHECKING:
    from .fixtures.test_session import TestSession

def pytest_addoption(parser):
    """Add custom command line options for Cloud Function testing"""
    parser.addoption(
        "--function-type", 
        action="store", 
        default="unknown",
        help="Type of Cloud Function: ingest, scoring, or unknown"
    )
    parser.addoption(
        "--environment",
        action="store", 
        default="development",
        help="Environment: development, staging, or production"
    )
    parser.addoption(
        "--limit",
        action="store",
        type=int,
        default=5,
        help="Record limit for integration tests (default: 5)"
    )

def pytest_configure(config):
    """Configure pytest with custom markers for two-tier validation"""
    # Tier 1: Deployment validation markers
    config.addinivalue_line(
        "markers", "deployment: Tier 1 - Environment-specific deployment validation"
    )
    
    # Tier 2: Runtime validation markers  
    config.addinivalue_line(
        "markers", "runtime: Tier 2 - Basic runtime mechanism validation"
    )
    
    # Safety markers
    config.addinivalue_line(
        "markers", "production_safe: Tests that are safe to run in production"
    )
    config.addinivalue_line(
        "markers", "production_only: Tests that should only run in production"
    )

@pytest.fixture(scope="session")
def function_type(request) -> str:
    """Fixture to get the function type context (ingest/scoring)"""
    return request.config.getoption("--function-type")

@pytest.fixture(scope="session") 
def environment(request) -> str:
    """Fixture to get the current environment (development/staging/production)"""
    return request.config.getoption("--environment")

@pytest.fixture(scope="session")
def limit(request) -> int:
    """Fixture to get the record limit for integration tests"""
    return request.config.getoption("--limit")

@pytest.fixture(scope="session")
def test_session(environment) -> Generator["TestSession", None, None]:
    """Test session fixture for cleanup tracking"""
    from .fixtures.test_session import TestSession
    
    session = TestSession(environment=environment)
    
    try:
        yield session
    finally:
        # Cleanup after all tests complete
        cleanup_results = session.cleanup_all()
        if cleanup_results['failed'] > 0:
            print(f"\n⚠️ Cleanup issues: {cleanup_results['failed']} failed, {cleanup_results['cleaned']} succeeded")
        elif cleanup_results['cleaned'] > 0:
            print(f"\n✅ Cleanup completed: {cleanup_results['cleaned']} resources cleaned")

@pytest.fixture(scope="function")
def safe_test_id() -> str:
    """Generate a safe, unique test identifier for temporary resources"""
    timestamp = int(datetime.utcnow().timestamp())
    random_suffix = uuid.uuid4().hex[:8]
    return f"test_{timestamp}_{random_suffix}"

@pytest.fixture(scope="function")
def test_logger() -> Generator[logging.Logger, None, None]:
    """Provide a test-specific logger"""
    logger = logging.getLogger('hubspot.test')
    logger.setLevel(logging.DEBUG)
    
    # Add handler if not already present
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    yield logger  # This was missing!
    
    # Optional cleanup - remove handlers after test
    # for handler in logger.handlers[:]:
    #     logger.removeHandler(handler)