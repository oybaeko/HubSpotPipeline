import os
from dotenv import load_dotenv
import logging

# Load local .env (for development only)
load_dotenv()

# ─── HubSpot ───────────────────────────────────────────────────────────────────────
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")

# ─── Google Cloud ───────────────────────────────────────────────────────────────────
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if GOOGLE_CREDENTIALS:
    # Ensure the BigQuery client picks up your service account key
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS

BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
DATASET_ID = os.getenv("BIGQUERY_DATASET_ID")

# ─── BigQuery Table Names (static constants) ────────────────────────────────────────
BQ_COMPANY_TABLE                 = "hs_companies"
BQ_OWNER_TABLE                   = "hs_owners"
BQ_DEALS_TABLE                   = "hs_deals"
BQ_STAGE_MAPPING_TABLE           = "hs_stage_mapping"
BQ_PIPELINE_UNITS_TABLE          = "hs_pipeline_units_snapshot"
BQ_SNAPSHOT_REGISTRY_TABLE       = "hs_snapshot_registry"
BQ_PIPELINE_SCORE_HISTORY_TABLE  = "hs_pipeline_score_history"

logging.info(f"📦 Loaded BQ owner table: {BQ_OWNER_TABLE}")
