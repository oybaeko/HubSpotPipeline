# tests/test_fetch_hs_objects_real_api.py

import os
import pytest
from hubspot_pipeline.fetch_hubspot_data import fetch_hs_objects

@pytest.mark.external_api
def test_fetch_companies_real_api():
    """
    Real‐HubSpot API test for fetch_hs_objects (companies).
    Skips unless HUBSPOT_API_KEY is set in the environment.
    """
    api_key = os.getenv("HUBSPOT_API_KEY")
    if not api_key:
        pytest.skip("Skipping real API test: HUBSPOT_API_KEY not set")

    # fetch a small batch so the test stays fast
    results, calls = fetch_hs_objects("companies", limit=3)
    assert calls >= 1, "Expected at least one API call"
    assert isinstance(results, list), "Results should be a list"
    assert len(results) <= 3, "Should respect the limit"
    # each result must have an 'id' field
    assert all("id" in rec for rec in results), "Every record needs an 'id'"

@pytest.mark.external_api
def test_fetch_deals_real_api():
    """
    Real‐HubSpot API test for fetch_hs_objects (deals).
    Skips unless HUBSPOT_API_KEY is set in the environment.
    """
    api_key = os.getenv("HUBSPOT_API_KEY")
    if not api_key:
        pytest.skip("Skipping real API test: HUBSPOT_API_KEY not set")

    results, calls = fetch_hs_objects("deals", limit=2)
    assert calls >= 1
    assert isinstance(results, list)
    # deal records should contain 'id' and 'properties'
    assert all("id" in r and "properties" in r for r in results)
