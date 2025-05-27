from google.cloud import bigquery
from config.config import BIGQUERY_PROJECT_ID, DATASET_ID, BQ_PIPELINE_UNITS_TABLE, BQ_PIPELINE_SCORE_HISTORY_TABLE, BQ_SNAPSHOT_REGISTRY_TABLE
import time

def process_snapshot(snapshot_id):
    """
    Processes a snapshot by running the `process_unit_score_for_snapshot` and `process_score_history_for_snapshot` functions.
    """
    print(f"🔄 Processing snapshot: {snapshot_id}")
    process_unit_score_for_snapshot(snapshot_id)
    process_score_history_for_snapshot(snapshot_id)
    print(f"✅ Snapshot {snapshot_id} processed successfully.")


def process_unit_score_for_snapshot(snapshot_id):
    """Processes a given snapshot by querying and transforming company and deal data from BigQuery,
    combining lifecycle and deal stages, mapping them to scoring metadata, and appending the results
    to the pipeline units table.

    Args:
        snapshot_id (str): The identifier for the snapshot to process.

    Side Effects:
        - Executes a BigQuery SQL query with the provided snapshot_id.
        - Appends the processed and scored data to the destination BigQuery table specified by
          BQ_PIPELINE_UNITS_TABLE.
        - Prints a confirmation message upon successful completion.

    Raises:
        google.api_core.exceptions.GoogleAPIError: If the BigQuery job fails."""
    
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)

    query = f"""
    WITH companies AS (
      SELECT 
        LOWER(lifecycle_stage) AS lifecycle_stage,
        LOWER(REPLACE(lead_status, " ", "_")) AS lead_status,
        company_id,
        hubspot_owner_id,
        snapshot_id
      FROM `{BIGQUERY_PROJECT_ID}.{DATASET_ID}.hs_companies`
      WHERE snapshot_id = @snapshot_id
    ),
    deals AS (
      SELECT 
        LOWER(deal_stage) AS deal_stage,
        deal_id,
        associated_company_id,
        snapshot_id
      FROM `{BIGQUERY_PROJECT_ID}.{DATASET_ID}.hs_deals`
      WHERE snapshot_id = @snapshot_id
        AND deal_stage NOT IN (
            SELECT stage_id
            FROM `{BIGQUERY_PROJECT_ID}.{DATASET_ID}.hs_deal_stage_reference`
            WHERE is_closed = TRUE
        )
    ),
    joined AS (
      SELECT
        c.snapshot_id,
        CURRENT_TIMESTAMP() AS snapshot_timestamp,
        c.company_id,
        d.deal_id,
        c.hubspot_owner_id AS owner_id,
        c.lifecycle_stage,
        c.lead_status,
        d.deal_stage,
        CASE
          WHEN c.lifecycle_stage = 'lead' THEN CONCAT(c.lifecycle_stage, '/', IFNULL(c.lead_status, ''))
          WHEN c.lifecycle_stage = 'opportunity' THEN CONCAT(c.lifecycle_stage, '/', IFNULL(d.deal_stage, 'missing'))
          WHEN c.lifecycle_stage IN ('salesqualifiedlead', 'closed-won', 'disqualified') THEN c.lifecycle_stage
          ELSE 'unmapped'
        END AS combined_stage,
        CASE WHEN d.deal_id IS NULL THEN 'company' ELSE 'deal' END AS stage_source
      FROM companies c
      LEFT JOIN deals d ON d.associated_company_id = c.company_id
    ),
    scored AS (
      SELECT
        j.*,
        sm.stage_level,
        sm.adjusted_score
      FROM joined j
      LEFT JOIN `{BIGQUERY_PROJECT_ID}.{DATASET_ID}.hs_stage_mapping` sm
      ON sm.combined_stage = j.combined_stage
    )
    SELECT * FROM scored
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
        ],
        destination=f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{BQ_PIPELINE_UNITS_TABLE}",
        write_disposition="WRITE_APPEND"
    )

    job = client.query(query, job_config=job_config)
    job.result()

    print(f"✅ Processed snapshot_id = {snapshot_id} → {BQ_PIPELINE_UNITS_TABLE}")

def process_score_history_for_snapshot(snapshot_id):
    """Processes and appends score history data for a given snapshot to the BigQuery score history table.

    This function executes a SQL query that aggregates company scores by snapshot, owner, and stage,
    then appends the results to the designated BigQuery table. It waits briefly before querying to
    ensure data consistency.

    Args:
        snapshot_id (str): The unique identifier for the snapshot to process.

    Side Effects:
        - Waits for 10 seconds before executing the query.
        - Appends aggregated score history data to the BigQuery score history table.
        - Prints a confirmation message upon successful completion."""
    
    time.sleep(10)  # Optional: wait for streaming buffer to flush
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)

    query = f"""
        SELECT
            snapshot_id,
            owner_id,
            combined_stage,
            COUNT(DISTINCT company_id) AS num_companies,
            SUM(adjusted_score) AS total_score,
            MAX(snapshot_timestamp) AS snapshot_timestamp
        FROM `{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{BQ_PIPELINE_UNITS_TABLE}`
        WHERE snapshot_id = @snapshot_id
        GROUP BY snapshot_id, owner_id, combined_stage
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
        ],
        destination=f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{BQ_PIPELINE_SCORE_HISTORY_TABLE}",
        write_disposition="WRITE_APPEND"
    )

    job = client.query(query, job_config=job_config)
    job.result()
    print(f"✅ Appended score history for snapshot: {snapshot_id}")



def reprocess_all_score_summaries():
    """Reprocesses all snapshots in the hs_snapshot_registry table to update their score summaries.
    This function queries all snapshot IDs from the BigQuery table, ordered by their timestamp,
    and iteratively calls `process_score_history_for_snapshot` for each snapshot. It provides
    console output indicating progress and completion.

    Raises:
        google.cloud.exceptions.GoogleCloudError: If there is an error querying BigQuery.
        Exception: If `process_score_history_for_snapshot` raises an exception. """
    
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    table_ref = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{BQ_SNAPSHOT_REGISTRY_TABLE}" 

    query = f"""
        SELECT snapshot_id
        FROM `{table_ref}`
        ORDER BY snapshot_timestamp
    """
    snapshot_ids = [row["snapshot_id"] for row in client.query(query).result()]
    
    print(f"🔁 Reprocessing {len(snapshot_ids)} snapshots for score summary...")
    for snapshot_id in snapshot_ids:
        print(f"🔄 Processing snapshot {snapshot_id}")
        process_score_history_for_snapshot(snapshot_id)
    
    print("✅ All snapshots reprocessed into hs_score_summary.")


if __name__ == "__main__":
    #reprocess_all_score_summaries()
    process_snapshot("2025-05-25T20:13:32")