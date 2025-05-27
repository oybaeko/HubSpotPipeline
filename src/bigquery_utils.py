from datetime import datetime, timezone
from google.cloud import bigquery
from config.config import BIGQUERY_PROJECT_ID, DATASET_ID, BQ_COMPANY_TABLE, BQ_OWNER_TABLE, BQ_DEALS_TABLE
from src.schema import SCHEMA_COMPANIES, HUBSPOT_COMPANY_FIELD_MAP, SCHEMA_OWNERS, HUBSPOT_OWNER_FIELD_MAP, SCHEMA_DEALS, HUBSPOT_DEAL_FIELD_MAP

DEBUG = True  # Set to False to disable debug output

def insert_companies_into_bigquery(companies, snapshot_id):
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    table_ref = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{BQ_COMPANY_TABLE}"
    if DEBUG:
        print(f"Inserting companies into table: {table_ref}")

    rows_to_insert = []
    now = datetime.now(timezone.utc).isoformat()

    for company in companies:
        props = company.get("properties", {})
        row = {}

        for field_name, _ in SCHEMA_COMPANIES:
            if field_name == "company_id":
                row[field_name] = str(company.get("id", ""))
            elif field_name == "timestamp":
                row[field_name] = now
            elif field_name == "snapshot_id":
                row[field_name] = snapshot_id
            else:
                hs_key = HUBSPOT_COMPANY_FIELD_MAP.get(field_name, field_name)
                row[field_name] = props.get(hs_key, "")

        rows_to_insert.append(row)

    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print(f"❌ Errors inserting companies: {errors}")
    else:
        print(f"✅ Inserted {len(rows_to_insert)} companies into {BQ_COMPANY_TABLE}")

def overwrite_owners_into_bigquery(owners):
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    table_ref = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{BQ_OWNER_TABLE}"

    rows_to_insert = []
    for owner in owners:
        row = {}
        for name, _ in SCHEMA_OWNERS:
            if name == "timestamp":
                row[name] = datetime.now(timezone.utc).isoformat()
            else:
                hs_key = HUBSPOT_OWNER_FIELD_MAP.get(name, name)
                row[name] = owner.get(hs_key, "UNKNOWN")
        rows_to_insert.append(row)

    if DEBUG:
        print(f"Overwriting table: {table_ref}")
        for row in rows_to_insert[:3]:
            print(row)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_json(rows_to_insert, table_ref, job_config=job_config)
    job.result()

    print(f"✅ Overwrote table {table_ref} with {len(rows_to_insert)} owner records.")



def recreate_table(table_name, schema_fields):
    """
    Recreates a BigQuery table with the specified schema.

    Args:
        table_name (str): The name of the table to recreate (without project/dataset).
        schema_fields (list): List of (name, field_type) tuples for the schema.

    This function deletes the existing table (if it exists) and creates a new one
    using the provided schema. It prints status messages for deletion and creation steps.
    """
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    table_ref = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{table_name}"

    schema = [bigquery.SchemaField(name, field_type) for name, field_type in schema_fields]

    try:
        client.delete_table(table_ref, not_found_ok=True)
        print(f"🗑️ Deleted existing table: {table_ref}")
    except Exception as e:
        print(f"⚠️ Error deleting table: {e}")

    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)
    print(f"✅ Created table: {table_ref}")


def insert_deals_into_bigquery(deals, snapshot_id):
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    table_ref = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{BQ_DEALS_TABLE}"
    now = datetime.now(timezone.utc).isoformat()

    rows_to_insert = []

    for deal in deals:
        props = deal.get("properties", {})
        associations = deal.get("associations", {})
        companies = associations.get("companies", {}).get("results", [])
        company_id = companies[0]["id"] if companies else None

        row = {}
        for field_name, _ in SCHEMA_DEALS:
            if field_name == "deal_id":
                row[field_name] = str(deal.get("id", ""))
            elif field_name == "associated_company_id":
                row[field_name] = str(company_id) if company_id else None
            elif field_name == "timestamp":
                row[field_name] = now
            elif field_name == "snapshot_id":
                row[field_name] = snapshot_id
            else:
                hs_key = HUBSPOT_DEAL_FIELD_MAP.get(field_name, field_name)
                if field_name == "amount":
                    row[field_name] = float(props.get(hs_key) or 0)
                else:
                    row[field_name] = props.get(hs_key, "")

        rows_to_insert.append(row)

    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print(f"❌ Errors inserting deals: {errors}")
    else:
        print(f"✅ Inserted {len(rows_to_insert)} deals into {BQ_DEALS_TABLE}")


def delete_all_tables_in_dataset():
    """
    Deletes all tables in the configured BigQuery dataset after user confirmation.
    Returns True if deletion was performed, False if aborted.
    """
    confirm = input(f"⚠️ This will permanently DELETE ALL TABLES in dataset {BIGQUERY_PROJECT_ID}.{DATASET_ID}. Type 'YES' to confirm: ")
    if confirm.strip().upper() != "YES":
        print("❌ Aborted. No tables were deleted.")
        return False

    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    dataset_ref = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}"
    tables = list(client.list_tables(dataset_ref))

    if not tables:
        print("✅ No tables to delete.")
        return True

    for table in tables:
        table_id = f"{dataset_ref}.{table.table_id}"
        client.delete_table(table_id, not_found_ok=True)
        print(f"🗑️ Deleted table: {table_id}")

    print("✅ All tables deleted.")
    return True