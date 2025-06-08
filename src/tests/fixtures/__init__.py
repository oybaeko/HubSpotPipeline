# ===============================================================================
# src/tests/fixtures/__init__.py
# Make fixtures directory a Python package
# ===============================================================================

"""
Test fixtures for HubSpot pipeline testing framework.
Provides shared test utilities and session management.
"""

from .test_session import TestSession

__all__ = ['TestSession']