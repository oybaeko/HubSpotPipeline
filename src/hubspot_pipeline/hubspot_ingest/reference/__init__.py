# src/hubspot_pipeline/hubspot_ingest/reference/__init__.py

"""
Reference data management for HubSpot pipeline.

This package handles reference data that changes infrequently:
- Owners (users/salespeople)
- Deal stages/pipelines

Reference data is replaced completely on each update (no history kept).
"""

from .main import update_reference_data
from .fetchers import fetch_owners, fetch_deal_stages
from .store import replace_owners, replace_deal_stages
from hubspot_pipeline.schema import SCHEMA_OWNERS, SCHEMA_DEAL_STAGE_REFERENCE, SCHEMA_SNAPSHOT_REGISTRY

# UPDATE __all__:
__all__ = [
    "update_reference_data",
    "fetch_owners", 
    "fetch_deal_stages",
    "replace_owners",
    "replace_deal_stages", 
    "SCHEMA_OWNERS",           # CHANGED
    "SCHEMA_DEAL_STAGE_REFERENCE",  # CHANGED
    "SCHEMA_SNAPSHOT_REGISTRY",     # CHANGED
]