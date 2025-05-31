# src/hubspot_pipeline/__init__.py
"""
HubSpotPipeline package exports.
"""

# ─── HubSpot API Fetchers ──────────────────────────────────────────────────────
from .fetch_hubspot_data import (
    fetch_companies,
    fetch_deals,
    fetch_all_deals_with_company,
    fetch_owners,
)

# ─── BigQuery Utilities ───────────────────────────────────────────────────────
from .bigquery_utils import (
    insert_companies_into_bigquery,
    overwrite_owners_into_bigquery,
    insert_deals_into_bigquery,
    recreate_table,
)

# ─── Snapshot & Processing Pipeline ───────────────────────────────────────────
from .process_snapshot import process_snapshot
from .populate_deal_stage_reference import populate_deal_stage_reference
from .populate_stage_mapping import populate_stage_mapping
from .recreate import recreate_all_snapshots
from .snapshot_runner import run_snapshot_and_process

# ─── Schema Definitions & Field Maps ──────────────────────────────────────────
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

__all__ = [
    # fetchers
    "fetch_companies",
    "fetch_deals",
    "fetch_all_deals_with_company",
    "fetch_owners",
    # BigQuery
    "insert_companies_into_bigquery",
    "overwrite_owners_into_bigquery",
    "insert_deals_into_bigquery",
    "recreate_table",
    # pipeline
    "process_snapshot",
    "populate_deal_stage_reference",
    "populate_stage_mapping",
    "recreate_all_snapshots",
    "run_snapshot_pipeline",
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
