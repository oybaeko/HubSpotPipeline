# src/hubspot_pipeline/bigquery_utils.py

import logging
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from .config.config import BIGQUERY_PROJECT_ID, DATASET_ID
from .schema import HUBSPOT_COMPANY_FIELD_MAP, HUBSPOT_DEAL_FIELD_MAP

# ────────────────────────────────────────────────────────────────────────────────
#  Table Recreation & Deletion Helpers
# ────────────────────────────────────────────────────────────────────────────────

def recreate_table(table_name: str, schema: list[tuple[str, str]]):
    """
    Drop (if exists) and recreate a BigQuery table with the given name and schema.
    """
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    dataset_ref = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}"
    table_id = f"{dataset_ref}.{table_name}"

    # 1) Delete existing table if it exists
    try:
        client.delete_table(table_id, not_found_ok=True)
        logging.info(f"🗑️ Deleted table {table_id} (if it existed).")
    except GoogleAPIError as e:
        logging.error(f"❌ Failed to delete existing table {table_id}: {e}")
        raise

    # 2) Construct new table schema
    bq_schema = []
    for col_name, col_type in schema:
        bq_schema.append(bigquery.SchemaField(col_name, col_type))

    table = bigquery.Table(table_id, schema=bq_schema)
    try:
        client.create_table(table)
        logging.info(f"✅ Created table {table_id} with schema: {[c.name for c in bq_schema]}")
    except GoogleAPIError as e:
        logging.error(f"❌ Failed to create table {table_id}: {e}")
        raise


def delete_all_tables_in_dataset() -> bool:
    """
    Deletes every table in the dataset specified by (BIGQUERY_PROJECT_ID, DATASET_ID).
    """
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    dataset_ref = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}"

    try:
        tables = client.list_tables(dataset_ref)
    except GoogleAPIError as e:
        logging.error(f"❌ Failed to list tables in dataset {dataset_ref}: {e}")
        return False

    success = True
    for table in tables:
        table_id = f"{dataset_ref}.{table.table_id}"
        try:
            client.delete_table(table_id)
            logging.info(f"🗑️ Deleted table {table_id}.")
        except GoogleAPIError as e:
            logging.error(f"❌ Failed to delete table {table_id}: {e}")
            success = False

    return success


# ────────────────────────────────────────────────────────────────────────────────
#  Data‐Insertion Helpers
# ────────────────────────────────────────────────────────────────────────────────

def insert_companies_into_bigquery(companies: list[dict], snapshot_id: str):
    """
    Insert a list of HubSpot‐company dictionaries into the table `hs_companies`.
    Automatically includes:
      - company_id (from company["id"])
      - all other columns in HUBSPOT_COMPANY_FIELD_MAP (from company["properties"])
      - snapshot_id
      - timestamp (from properties['createdate'])
    """
    # 1) Flatten one level if needed
    if companies and isinstance(companies[0], list):
        logging.warning("🔄 Detected nested list in `companies`; flattening one level.")
        companies = [item for sublist in companies for item in sublist]

    rows_to_insert = []
    for company in companies:
        if not isinstance(company, dict):
            logging.warning(f"⚠️ Skipping non‐dict company: {type(company)}")
            continue

        props = company.get("properties", {})
        row: dict[str, any] = {}

        # 1.A) Special‐case the company_id (top‐level "id")
        row["company_id"] = company.get("id")

        # 1.B) Loop over every other mapped column
        for col_name, hs_prop_name in HUBSPOT_COMPANY_FIELD_MAP.items():
            if hs_prop_name == "id":
                # we've already handled "id" above
                continue
            row[col_name] = props.get(hs_prop_name)

        # 2) Add snapshot_id and timestamp
        row["snapshot_id"] = snapshot_id
        row["timestamp"]   = props.get("createdate")

        rows_to_insert.append(row)

    if not rows_to_insert:
        logging.info("ℹ️ No valid company rows to insert; skipping.")
        return

    table_id = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.hs_companies"
    logging.info(f"⏳ Inserting {len(rows_to_insert)} companies into `{table_id}`…")

    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    try:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            logging.error(f"❌ insert_companies_into_bigquery errors: {errors}")
            raise GoogleAPIError(f"BigQuery insert errors: {errors}")
        logging.info(f"✅ Successfully inserted {len(rows_to_insert)} companies into `{table_id}`")
    except GoogleAPIError as e:
        logging.error(f"❌ Failed to insert companies into `{table_id}`: {e}")
        raise


def insert_deals_into_bigquery(deals: list[dict], snapshot_id: str):
    """
    Insert a list of HubSpot‐deal dictionaries into the table `hs_deals`.
    Automatically includes:
      - deal_id (from deal["id"])
      - all other columns in HUBSPOT_DEAL_FIELD_MAP (from deal["properties"])
      - associated_company_id (first in associations, if any)
      - timestamp (from properties['createdate'])
      - snapshot_id
    """
    # 1) Flatten one level if needed
    if deals and isinstance(deals[0], list):
        logging.warning("🔄 Detected nested list in `deals`; flattening one level.")
        deals = [item for sublist in deals for item in sublist]

    rows_to_insert = []
    for deal in deals:
        if not isinstance(deal, dict):
            logging.warning(f"⚠️ Skipping non‐dict deal: {type(deal)}")
            continue

        props = deal.get("properties", {})
        associations = deal.get("associations", {}).get("companies", {}).get("results", [])
        associated_company_id = associations[0]["id"] if associations else None

        row: dict[str, any] = {}

        # 1.A) Special‐case the deal_id (top‐level "id")
        row["deal_id"] = deal.get("id")

        # 1.B) Loop over every other mapped column
        for col_name, hs_prop_name in HUBSPOT_DEAL_FIELD_MAP.items():
            if hs_prop_name == "id":
                continue
            row[col_name] = props.get(hs_prop_name)

        # 2) Add associated_company_id, timestamp, snapshot_id
        row["associated_company_id"] = associated_company_id
        row["timestamp"]             = props.get("createdate")
        row["snapshot_id"]           = snapshot_id

        rows_to_insert.append(row)

    if not rows_to_insert:
        logging.info("ℹ️ No valid deal rows to insert; skipping.")
        return

    table_id = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.hs_deals"
    logging.info(f"⏳ Inserting {len(rows_to_insert)} deals into `{table_id}`…")

    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    try:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            logging.error(f"❌ insert_deals_into_bigquery errors: {errors}")
            raise GoogleAPIError(f"BigQuery insert errors: {errors}")
        logging.info(f"✅ Successfully inserted {len(rows_to_insert)} deals into `{table_id}`")
    except GoogleAPIError as e:
        logging.error(f"❌ Failed to insert deals into `{table_id}`: {e}")
        raise
