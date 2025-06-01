import logging
import time

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

from .config.config import (
    BIGQUERY_PROJECT_ID,
    DATASET_ID,
    BQ_PIPELINE_UNITS_TABLE,
    BQ_PIPELINE_SCORE_HISTORY_TABLE,
    BQ_SNAPSHOT_REGISTRY_TABLE,
)


def process_snapshot(snapshot_id: str):
    """
    Master function: processes a snapshot by running unit‐score and score‐history jobs in sequence.
    """
    logging.info(f"🔄 Starting full processing for snapshot: {snapshot_id}")
    try:
        process_unit_score_for_snapshot(snapshot_id)
        process_score_history_for_snapshot(snapshot_id)
        logging.info(f"✅ Completed full processing for snapshot: {snapshot_id}")
    except Exception as e:
        logging.error(f"❌ Error during process_snapshot({snapshot_id}): {e}", exc_info=True)
        raise


def process_unit_score_for_snapshot(snapshot_id: str):
    """
    Processes a given snapshot by:
      1) Querying and transforming company + deal data from BigQuery,
      2) Mapping each record to its combined_stage and score,
      3) Appending results to the pipeline_units table.

    Args:
        snapshot_id (str): The identifier for the snapshot to process.

    Raises:
        GoogleAPIError: If the BigQuery job fails.
    """
    logging.info(f"🔹 Entering process_unit_score_for_snapshot({snapshot_id})")

    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)

    # Build the SQL string (we trim indentation to keep the code neat)
    query = f"""
    -- Step 1: Filter companies for this snapshot
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
    -- Step 2: Filter open deals for this snapshot
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
    -- Step 3: Left‐join companies with their deals (if any)
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
        CASE 
          WHEN d.deal_id IS NULL THEN 'company' 
          ELSE 'deal' 
        END AS stage_source
      FROM companies c
      LEFT JOIN deals d ON d.associated_company_id = c.company_id
    ),
    -- Step 4: Join the “scoring metadata” from stage_mapping
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

    logging.info("🔹 Submitting BigQuery job for unit scores...")
    logging.debug(f"SQL for unit score (truncated to 200 chars):\n{query[:200]}...")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
        ],
        destination=f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{BQ_PIPELINE_UNITS_TABLE}",
        write_disposition="WRITE_APPEND",
    )

    try:
        job = client.query(query, job_config=job_config)
        logging.info(f"   • BigQuery job ID (unit score): {job.job_id}")
        job.result()  # Wait for completion
        logging.info(f"✅ Unit‐score job completed and data appended to `{BQ_PIPELINE_UNITS_TABLE}`")
    except GoogleAPIError as e:
        logging.error(f"❌ BigQuery unit‐score job failed: {e}", exc_info=True)
        raise

    logging.info(f"🔹 Exiting process_unit_score_for_snapshot({snapshot_id})")


def process_score_history_for_snapshot(snapshot_id: str):
    """
    Processes and appends score history data for a given snapshot to the BigQuery score history table.

    Steps:
      1) Sleep 10 seconds to allow streaming buffer flush (if any),
      2) Aggregate pipeline_units rows by owner & combined_stage,
      3) Append results to the BQ_PIPELINE_SCORE_HISTORY_TABLE.

    Args:
        snapshot_id (str): The unique identifier for the snapshot to process.

    Raises:
        GoogleAPIError: If the BigQuery job fails.
    """
    logging.info(f"🔹 Entering process_score_history_for_snapshot({snapshot_id})")

    # 1) Optional wait
    wait_secs = 10
    logging.info(f"   • Waiting {wait_secs}s to ensure pipeline_units data is available …")
    time.sleep(wait_secs)

    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)

    # Build the SQL string
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

    logging.info("🔹 Submitting BigQuery job for score history …")
    logging.debug(f"SQL for score history (truncated to 200 chars):\n{query[:200]}...")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
        ],
        destination=f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{BQ_PIPELINE_SCORE_HISTORY_TABLE}",
        write_disposition="WRITE_APPEND",
    )

    try:
        job = client.query(query, job_config=job_config)
        logging.info(f"   • BigQuery job ID (score history): {job.job_id}")
        job.result()  # Wait for completion
        logging.info(f"✅ Score‐history job completed and data appended to `{BQ_PIPELINE_SCORE_HISTORY_TABLE}`")
    except GoogleAPIError as e:
        logging.error(f"❌ BigQuery score‐history job failed: {e}", exc_info=True)
        raise

    logging.info(f"🔹 Exiting process_score_history_for_snapshot({snapshot_id})")


def reprocess_all_score_summaries():
    """
    Reprocesses all snapshots in the hs_snapshot_registry table to update their score summaries.

    1) Reads every snapshot_id from hs_snapshot_registry,
    2) Iterates in chronological order, calling process_score_history_for_snapshot() for each.

    Raises:
        GoogleAPIError: If there is an error querying BigQuery.
    """
    logging.info("🔹 Entering reprocess_all_score_summaries()")

    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    table_ref = f"{BIGQUERY_PROJECT_ID}.{DATASET_ID}.{BQ_SNAPSHOT_REGISTRY_TABLE}"

    # 1) Fetch all snapshot_ids
    query = f"""
    SELECT snapshot_id
    FROM `{table_ref}`
    ORDER BY snapshot_timestamp
    """
    logging.info(f"   • Fetching snapshot IDs from `{BQ_SNAPSHOT_REGISTRY_TABLE}` …")
    try:
        rows = client.query(query).result()
        snapshot_ids = [row["snapshot_id"] for row in rows]
        logging.info(f"   • Found {len(snapshot_ids)} snapshots to reprocess.")
    except GoogleAPIError as e:
        logging.error(f"❌ Failed to query `{BQ_SNAPSHOT_REGISTRY_TABLE}`: {e}", exc_info=True)
        raise

    # 2) Loop over each snapshot and re‐run score history
    for idx, snapshot_id in enumerate(snapshot_ids, start=1):
        logging.info(f"   🔄 ({idx}/{len(snapshot_ids)}) Reprocessing snapshot `{snapshot_id}` …")
        try:
            process_score_history_for_snapshot(snapshot_id)
        except Exception as e:
            logging.error(f"❌ Error reprocessing snapshot `{snapshot_id}`: {e}", exc_info=True)
            # decide whether to continue or break; here we continue to attempt all
            continue

    logging.info("✅ Completed reprocess_all_score_summaries()")


if __name__ == "__main__":
    # Example hard‐coded test run (uncomment to test locally)
    # reprocess_all_score_summaries()
    test_snapshot = "2025-05-25T20:13:32"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s"
    )
    process_snapshot(test_snapshot)
