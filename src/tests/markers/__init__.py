# ===============================================================================
# src/tests/markers/__init__.py  
# Make markers directory a Python package
# ===============================================================================

"""
Custom pytest markers for HubSpot pipeline testing.
Defines production-safe and environment-specific test markers.
"""

# Custom marker definitions - these are also defined in conftest.py
PRODUCTION_SAFE_MARKER = "production_safe"
PRODUCTION_ONLY_MARKER = "production_only" 
INFRASTRUCTURE_MARKER = "infrastructure"
DATABASE_MARKER = "database"
EVENTS_MARKER = "events"
LOGGING_MARKER = "logging"
SLOW_MARKER = "slow"

__all__ = [
    'PRODUCTION_SAFE_MARKER',
    'PRODUCTION_ONLY_MARKER', 
    'INFRASTRUCTURE_MARKER',
    'DATABASE_MARKER',
    'EVENTS_MARKER',
    'LOGGING_MARKER',
    'SLOW_MARKER'
]