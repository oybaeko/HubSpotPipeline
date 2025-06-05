# src/hubspot_pipeline/__init__.py
"""
HubSpotPipeline package exports.
"""

# Only import the new ingest modules, not the old fetch modules
# This avoids the HUBSPOT_API_KEY check that happens at import time

# ─── New Ingest Pipeline (hubspot_ingest) ─────────────────────────────────────
# These are the modules we actually use now
from .hubspot_ingest.config_loader import init_env, load_schema, get_config
from .hubspot_ingest.main import main as ingest_main

# ─── Schema Definitions & Field Maps (still needed) ───────────────────────────
from .schema import (
    SCHEMA_COMPANIES,
    HUBSPOT_COMPANY_FIELD_MAP,
    SCHEMA_OWNERS,
    HUBSPOT_OWNER_FIELD_MAP,
    SCHEMA_DEALS,
    HUBSPOT_DEAL_FIELD_MAP,
    SCHEMA_DEAL_STAGE_REFERENCE,
    SCHEMA_STAGE_MAPPING,
    SCHEMA_PIPELINE_UNITS_SNAPSHOT,
    SCHEMA_SNAPSHOT_REGISTRY,
    SCHEMA_PIPELINE_SCORE_HISTORY,
)

# Note: Removed imports of old fetch_hubspot_data and bigquery_utils modules
# to avoid the HUBSPOT_API_KEY import-time check that causes deployment to fail.
# These modules can still be imported directly if needed for the old pipeline.

__all__ = [
    # New ingest pipeline
    "init_env",
    "load_schema", 
    "get_config",
    "ingest_main",
    # schemas & maps
    "SCHEMA_COMPANIES",
    "HUBSPOT_COMPANY_FIELD_MAP",
    "SCHEMA_OWNERS",
    "HUBSPOT_OWNER_FIELD_MAP",
    "SCHEMA_DEALS",
    "HUBSPOT_DEAL_FIELD_MAP",
    "SCHEMA_DEAL_STAGE_REFERENCE",
    "SCHEMA_STAGE_MAPPING",
    "SCHEMA_PIPELINE_UNITS_SNAPSHOT",
    "SCHEMA_SNAPSHOT_REGISTRY",
    "SCHEMA_PIPELINE_SCORE_HISTORY",
]