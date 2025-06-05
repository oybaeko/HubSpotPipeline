# --- store.py ---
# Write to BigQuery
import logging
import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def store_to_bigquery(rows, table_name, dataset=None):
    if not rows:
        logging.info(f"No data for {table_name}")
        return

    client = bigquery.Client()
    logging.info(f"Preparing to store {len(rows)} rows into '{dataset}.{table_name}'")
    dataset = dataset or os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
    full_table = f"{client.project}.{dataset}.{table_name}"

    # Schema from first row
    sample = rows[0]
    schema = [
        bigquery.SchemaField(k, _bq_type(v)) for k, v in sample.items()
    ]

    try:
        client.get_table(full_table)
    except NotFound:
        logging.info(f"Table {full_table} not found. Creating new table.")
        client.create_table(bigquery.Table(full_table, schema=schema))

    errors = client.insert_rows_json(full_table, rows)
    logging.info(f"Inserted {len(rows)} rows into {full_table}")
    if errors:
        logging.error(f"Failed to insert rows into {full_table}: {errors}")
        raise RuntimeError(errors)


def _bq_type(v):
    if isinstance(v, int): return "INTEGER"
    if isinstance(v, float): return "FLOAT"
    if isinstance(v, bool): return "BOOLEAN"
    return "STRING"