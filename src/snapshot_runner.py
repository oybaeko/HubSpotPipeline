from datetime import datetime, timezone
from src.fetch_hubspot_data import fetch_companies, fetch_all_deals_with_company
from src.bigquery_utils import insert_companies_into_bigquery, insert_deals_into_bigquery
from src.process_snapshot import process_snapshot, process_score_history_for_snapshot
from google.cloud import bigquery
from config.config import BIGQUERY_PROJECT_ID, DATASET_ID, BQ_SNAPSHOT_REGISTRY_TABLE


def create_snapshot_id():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

def register_snapshot(snapshot_id, triggered_by="manual", status="completed", notes=None):
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)

    table_ref = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{BQ_SNAPSHOT_REGISTRY_TABLE}"

    row = {
        "snapshot_id": snapshot_id,
        "snapshot_timestamp": datetime.now(timezone.utc).isoformat(),
        "triggered_by": triggered_by,
        "status": status,
        "notes": notes or ""
    }

    errors = client.insert_rows_json(table_ref, [row])
    if errors:
        print(f"❌ Failed to register snapshot: {errors}")
    else:
        print(f"✅ Registered snapshot {snapshot_id} ({status})")

def run_snapshot_and_process(filters_company=None, filters_deal=None, triggered_by="manual", limit=1000):
    snapshot_id = create_snapshot_id()

    try:
        companies = fetch_companies(filters=filters_company, total_limit=limit)
        insert_companies_into_bigquery(companies, snapshot_id)

        deals = fetch_all_deals_with_company(limit=limit)
        insert_deals_into_bigquery(deals, snapshot_id)

        process_snapshot(snapshot_id)

        register_snapshot(snapshot_id, triggered_by=triggered_by, status="completed")
        print(f"✅ Snapshot run {snapshot_id} completed.")
    except Exception as e:
        register_snapshot(snapshot_id, triggered_by=triggered_by, status="error", notes=str(e))
        print(f"❌ Snapshot run {snapshot_id} failed: {e}")


if __name__ == "__main__":
    run_snapshot_and_process()
    
