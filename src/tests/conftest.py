# ===============================================================================
# src/tests/conftest.py  
# Pytest configuration and shared fixtures
# ===============================================================================

import pytest
import os
import uuid
from datetime import datetime
from typing import Generator, TYPE_CHECKING

if TYPE_CHECKING:
    from .fixtures.test_session import TestSession

def pytest_addoption(parser):
    """Add custom command line options for our Cloud Function context"""
    parser.addoption(
        "--function-type", 
        action="store", 
        default="unknown",
        help="Type of Cloud Function running tests: ingest or scoring"
    )
    parser.addoption(
        "--environment",
        action="store", 
        default="development",
        help="Environment: development, staging, or production"
    )

def pytest_configure(config):
    """Configure pytest with custom markers"""
    config.addinivalue_line(
        "markers", "infrastructure: Infrastructure connectivity and permissions tests"
    )
    config.addinivalue_line(
        "markers", "database: Database operation tests"
    )
    config.addinivalue_line(
        "markers", "events: Event system tests (Pub/Sub)"
    )
    config.addinivalue_line(
        "markers", "logging: Logging system tests"
    )
    config.addinivalue_line(
        "markers", "production_safe: Tests that are safe to run in production"
    )
    config.addinivalue_line(
        "markers", "production_only: Tests that should only run in production"
    )
    config.addinivalue_line(
        "markers", "slow: Tests that take longer than 30 seconds"
    )

@pytest.fixture(scope="session")
def function_type(request) -> str:
    """Fixture to get the function type context"""
    return request.config.getoption("--function-type")

@pytest.fixture(scope="session") 
def environment(request) -> str:
    """Fixture to get the current environment"""
    return request.config.getoption("--environment")

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
            for error in cleanup_results['errors']:
                print(f"  • {error}")
        elif cleanup_results['cleaned'] > 0:
            print(f"\n✅ Cleanup completed: {cleanup_results['cleaned']} resources cleaned")

@pytest.fixture(scope="function")
def safe_test_id() -> str:
    """Generate a safe, unique test identifier"""
    timestamp = int(datetime.utcnow().timestamp())
    random_suffix = uuid.uuid4().hex[:8]
    return f"test_{timestamp}_{random_suffix}"

@pytest.fixture(scope="function")
def test_logger() -> Generator[object, None, None]:
    """Provide a test-specific logger"""
    import logging
    
    logger = logging.getLogger('hubspot.test')
    logger.setLevel(logging.DEBUG)
    
    # Add handler if not already present
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    yield logger