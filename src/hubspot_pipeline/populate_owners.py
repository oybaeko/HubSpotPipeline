# src/hubspot_pipeline/populate_owners.py

import logging
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from .fetch_hubspot_data import fetch_owners
from .config.config import BIGQUERY_PROJECT_ID, DATASET_ID

def overwrite_owners_into_bigquery():
    """
    Fetches all owners from HubSpot, truncates the hs_owners table,
    and inserts the latest rows. Raises on any BigQuery error.
    """
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    table_id = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.hs_owners"

    logging.info(f"⚠️ Truncating table {table_id} …")
    client.query(f"TRUNCATE `{table_id}`").result()

    raw_owners = fetch_owners()
    if not isinstance(raw_owners, list) or not raw_owners:
        logging.info("ℹ️ fetch_owners() returned no data; skipping insert.")
        return

    rows_to_insert = []
    for owner in raw_owners:
        # Map HubSpot fields to your SCHEMA_OWNERS columns:
        row = {
            "owner_id": owner.get("id"),
            "email": owner.get("email"),
            "first_name": owner.get("firstName"),
            "last_name": owner.get("lastName"),
            "user_id": owner.get("userId"),
            "active": owner.get("active"),
            "timestamp": owner.get("updatedAt") or owner.get("createdAt"),
        }
        rows_to_insert.append(row)

    logging.info(f"⏳ Inserting {len(rows_to_insert)} owners into {table_id} …")
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        logging.error(f"❌ overwrite_owners_into_bigquery errors: {errors}")
        raise GoogleAPIError(f"Owner insert errors: {errors}")
    logging.info(f"✅ Successfully inserted {len(rows_to_insert)} owners into {table_id}")

