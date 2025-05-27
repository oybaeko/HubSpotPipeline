import requests
from google.cloud import bigquery
from config.config import HUBSPOT_API_KEY, BIGQUERY_PROJECT_ID, DATASET_ID
from src.schema import SCHEMA_DEAL_STAGE_REFERENCE
from src.bigquery_utils import recreate_table

def populate_deal_stage_reference():
    url = "https://api.hubapi.com/crm/v3/pipelines/deals"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"❌ Failed to fetch pipelines: {response.status_code} - {response.text}")
        return

    pipelines = response.json().get("results", [])
    records = []

    for pipeline in pipelines:
        pipeline_id = pipeline.get("id")
        pipeline_label = pipeline.get("label")
        for stage in pipeline.get("stages", []):
            records.append({
                "pipeline_id": pipeline_id,
                "pipeline_label": pipeline_label,
                "stage_id": stage.get("id"),
                "stage_label": stage.get("label"),
                "is_closed": stage.get("metadata", {}).get("isClosed", False),
                "probability": float(stage.get("metadata", {}).get("probability", 0)),
                "display_order": stage.get("displayOrder", 0)
            })

    table_name = "hs_deal_stage_reference"
    recreate_table(table_name, SCHEMA_DEAL_STAGE_REFERENCE)

    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    table_ref = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{table_name}"

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_json(records, table_ref, job_config=job_config)
    job.result()

    print(f"✅ Loaded {len(records)} deal stage records into {table_ref}")

if __name__ == "__main__":
    populate_deal_stage_reference()
