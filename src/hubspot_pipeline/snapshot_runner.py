# src/hubspot_pipeline/snapshot_runner.py

import logging
from datetime import datetime, timezone
from google.cloud import bigquery

from .fetch_hubspot_data import fetch_companies, fetch_all_deals_with_company
from .bigquery_utils import insert_companies_into_bigquery, insert_deals_into_bigquery
from .process_snapshot import process_snapshot
from .config.config import (
    BIGQUERY_PROJECT_ID,
    DATASET_ID,
    BQ_COMPANY_TABLE,
    BQ_DEALS_TABLE,
    BQ_SNAPSHOT_REGISTRY_TABLE,
)


def create_snapshot_id() -> str:
    """
    Generate a unique snapshot ID based on the current UTC timestamp.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def register_snapshot(
    snapshot_id: str,
    triggered_by: str = "manual",
    status: str = "completed",
    notes: str | None = None,
):
    """
    Write a single row into the hs_snapshot_registry table to record
    whether this snapshot succeeded or failed.
    """
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    table_ref = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{BQ_SNAPSHOT_REGISTRY_TABLE}"

    row = {
        "snapshot_id": snapshot_id,
        "snapshot_timestamp": datetime.now(timezone.utc).isoformat(),
        "triggered_by": triggered_by,
        "status": status,
        "notes": notes or "",
    }

    try:
        errors = client.insert_rows_json(table_ref, [row])
        if errors:
            logging.error(f"❌ Failed to register snapshot: {errors}")
        else:
            logging.info(f"✅ Registered snapshot {snapshot_id} ({status})")
    except Exception as e:
        logging.error(f"❌ Exception while registering snapshot: {e}", exc_info=True)


def run_snapshot_and_process(
    filters_company=None,
    filters_deal=None,
    triggered_by: str = "manual",
    limit: int | None = 100,
    dry_run: bool = False,
    test_mode: bool = False,
):
    """
    1) Fetch companies & deals (up to `limit`),
    2) Insert them into `hs_companies` and `hs_deals`,
    3) Call process_snapshot(snapshot_id) to build pipeline_units and score history,
    4) Register success/failure in hs_snapshot_registry.

    Args:
      filters_company: Optional list of filter dicts for company‐search.
      filters_deal: Optional list of filter dicts for deal‐search.
      triggered_by:   Who kicked off this snapshot (e.g. "manual", "scheduled").
      limit:          Max number of records to fetch from each endpoint; None→unlimited.
      dry_run:        If True, skip any BigQuery writes (just fetch & log).
      test_mode:      If True, populate test tables instead of production tables.

    Raises:
      Re‐raises any exception encountered during insertion or processing.
    """
    snapshot_id = create_snapshot_id()
    logging.info(
        f"▶️ Starting snapshot run {snapshot_id} "
        f"(triggered_by={triggered_by}, limit={limit}, dry_run={dry_run}, test_mode={test_mode})"
    )

    try:
        # ─── 1) FETCH COMPANIES ─────────────────────────────────────────────────────
        logging.info(f"⏳ Fetching companies (limit={limit}, filters={filters_company})…")
        raw_companies = fetch_companies(filters=filters_company, limit=limit)
        # unpack (results, call_count) if returned as a tuple
        if isinstance(raw_companies, tuple) and len(raw_companies) == 2 and isinstance(raw_companies[1], int):
            companies = raw_companies[0]
        else:
            companies = raw_companies

        # In case companies is a nested list (unlikely if unpacked correctly)
        if companies and all(isinstance(x, (list, tuple)) for x in companies):
            logging.warning("🔄 Detected nested list in `companies`; flattening one level.")
            companies = [item for sublist in companies for item in sublist]

        logging.info(f"✅ Retrieved {len(companies)} companies.")

        if not dry_run:
            logging.info(f"⏳ Inserting {len(companies)} companies into `{BQ_COMPANY_TABLE}`…")
            insert_companies_into_bigquery(companies, snapshot_id)
            logging.info(f"✅ Inserted {len(companies)} companies into `{BQ_COMPANY_TABLE}`")
        else:
            logging.info("🛑 Dry‐run: skipping insert_companies_into_bigquery()")

        # ─── 2) FETCH DEALS ───────────────────────────────────────────────────────
        logging.info(f"🔄 Fetching deals (limit={limit}, filters={filters_deal})…")
        raw_deals = fetch_all_deals_with_company(limit=limit)
        if isinstance(raw_deals, tuple) and len(raw_deals) == 2 and isinstance(raw_deals[1], int):
            deals = raw_deals[0]
        else:
            deals = raw_deals

        if deals and all(isinstance(x, (list, tuple)) for x in deals):
            logging.warning("🔄 Detected nested list in `deals`; flattening one level.")
            deals = [item for sublist in deals for item in sublist]

        logging.info(f"✅ Retrieved {len(deals)} deals.")

        if not dry_run:
            logging.info(f"⏳ Inserting {len(deals)} deals into `{BQ_DEALS_TABLE}`…")
            insert_deals_into_bigquery(deals, snapshot_id)
            logging.info(f"✅ Inserted {len(deals)} deals into `{BQ_DEALS_TABLE}`")
        else:
            logging.info("🛑 Dry‐run: skipping insert_deals_into_bigquery()")

        # ─── 3) PROCESS SNAPSHOT ───────────────────────────────────────────────────
        if not dry_run:
            logging.info(f"⏳ Processing snapshot {snapshot_id}…")
            process_snapshot(snapshot_id)
            logging.info(f"✅ Snapshot {snapshot_id} processed successfully.")
        else:
            logging.info("🛑 Dry‐run: skipping process_snapshot()")

        # ─── 4) REGISTER SUCCESS ───────────────────────────────────────────────────
        if not dry_run:
            register_snapshot(snapshot_id, triggered_by=triggered_by, status="completed")
        else:
            logging.info("🛑 Dry‐run: skipping register_snapshot()")

        logging.info(f"✅ Snapshot run {snapshot_id} completed successfully.")

    except Exception as e:
        logging.error(f"❌ Snapshot run {snapshot_id} failed: {e}", exc_info=True)
        if not dry_run:
            register_snapshot(snapshot_id, triggered_by=triggered_by, status="error", notes=str(e))
        raise  # re‐raise so that callers know it failed


# If someone runs this module directly, run a single snapshot with default settings
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    run_snapshot_and_process(limit=100)
