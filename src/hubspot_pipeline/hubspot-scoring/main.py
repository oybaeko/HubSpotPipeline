# hubspot-scoring/main.py
# Simple standalone scoring function

import json
import base64
import logging
import os
import time
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

def setup_logging():
    """Setup logging for scoring function"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True
    )
    return logging.getLogger('hubspot.scoring')

def get_config():
    """Get BigQuery configuration"""
    return {
        'BIGQUERY_PROJECT_ID': os.getenv('BIGQUERY_PROJECT_ID'),
        'BIGQUERY_DATASET_ID': os.getenv('BIGQUERY_DATASET_ID', 'Hubspot_dev_ob')
    }

def populate_stage_mapping():
    """Populate the hs_stage_mapping table with hardcoded stage mapping logic"""
    logger = logging.getLogger('hubspot.scoring.stage_mapping')
    logger.info("🔄 Populating stage mapping table")
    
    config = get_config()
    client = bigquery.Client(project=config['BIGQUERY_PROJECT_ID'])
    table_ref = f"{config['BIGQUERY_PROJECT_ID']}.{config['BIGQUERY_DATASET_ID']}.hs_stage_mapping"

    # Your existing stage mapping data
    stage_mapping = [
        # Lead lifecycle stages
        {"lifecycle_stage": "lead", "lead_status": "new", "deal_stage": None, "combined_stage": "lead/new", "stage_level": 1, "adjusted_score": 1.0},
        {"lifecycle_stage": "lead", "lead_status": "restart", "deal_stage": None, "combined_stage": "lead/restart", "stage_level": 1, "adjusted_score": 1.0},
        {"lifecycle_stage": "lead", "lead_status": "attempted to contact", "deal_stage": None, "combined_stage": "lead/attempted_to_contact", "stage_level": 2, "adjusted_score": 1.5},
        {"lifecycle_stage": "lead", "lead_status": "connected", "deal_stage": None, "combined_stage": "lead/connected", "stage_level": 3, "adjusted_score": 2.0},
        {"lifecycle_stage": "lead", "lead_status": "nurturing", "deal_stage": None, "combined_stage": "lead/nurturing", "stage_level": 0, "adjusted_score": 2.0},

        # Sales Qualified Lead
        {"lifecycle_stage": "sales qualified lead", "lead_status": None, "deal_stage": None, "combined_stage": "salesqualifiedlead", "stage_level": 4, "adjusted_score": 6.0},

        # Opportunity stages
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": None, "combined_stage": "opportunity/missing", "stage_level": 5, "adjusted_score": 7.0},
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": "appointmentscheduled", "combined_stage": "opportunity/appointmentscheduled", "stage_level": 5, "adjusted_score": 8.0},
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": "qualifiedtobuy", "combined_stage": "opportunity/qualifiedtobuy", "stage_level": 6, "adjusted_score": 10.0},
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": "presentationscheduled", "combined_stage": "opportunity/presentationscheduled", "stage_level": 7, "adjusted_score": 12.0},
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": "decisionmakerboughtin", "combined_stage": "opportunity/decisionmakerboughtin", "stage_level": 8, "adjusted_score": 14.0},

        # Closed-Won
        {"lifecycle_stage": "closed-won", "lead_status": None, "deal_stage": "contractsent", "combined_stage": "closed-won/contractsent", "stage_level": 9, "adjusted_score": 30.0},

        # Disqualified
        {"lifecycle_stage": "disqualified", "lead_status": None, "deal_stage": None, "combined_stage": "disqualified", "stage_level": -1, "adjusted_score": 0.0}
    ]

    try:
        # Truncate and reload
        logger.info(f"🗑️ Truncating table {table_ref}")
        client.query(f"TRUNCATE `{table_ref}`").result()

        logger.info(f"⏳ Loading {len(stage_mapping)} stage mapping records")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_json(stage_mapping, table_ref, job_config=job_config)
        job.result()

        logger.info(f"✅ Loaded {len(stage_mapping)} rows into {table_ref}")
        
    except Exception as e:
        logger.error(f"❌ Failed to populate stage mapping: {e}")
        raise

def process_snapshot(snapshot_id: str):
    """Process a snapshot by running unit-score and score-history jobs"""
    logger = logging.getLogger('hubspot.scoring.processor')
    logger.info(f"🔄 Starting processing for snapshot: {snapshot_id}")
    
    try:
        process_unit_score_for_snapshot(snapshot_id)
        process_score_history_for_snapshot(snapshot_id)
        logger.info(f"✅ Completed processing for snapshot: {snapshot_id}")
    except Exception as e:
        logger.error(f"❌ Error during process_snapshot({snapshot_id}): {e}", exc_info=True)
        raise

def process_unit_score_for_snapshot(snapshot_id: str):
    """Process unit scores for a snapshot"""
    logger = logging.getLogger('hubspot.scoring.processor')
    logger.info(f"🔹 Processing unit scores for snapshot: {snapshot_id}")

    config = get_config()
    client = bigquery.Client(project=config['BIGQUERY_PROJECT_ID'])

    # Build the SQL query
    query = f"""
    -- Step 1: Filter companies for this snapshot
    WITH companies AS (
      SELECT 
        LOWER(lifecycle_stage) AS lifecycle_stage,
        LOWER(REPLACE(lead_status, " ", "_")) AS lead_status,
        company_id,
        hubspot_owner_id,
        snapshot_id
      FROM `{config['BIGQUERY_PROJECT_ID']}.{config['BIGQUERY_DATASET_ID']}.hs_companies`
      WHERE snapshot_id = @snapshot_id
    ),
    -- Step 2: Filter open deals for this snapshot
    deals AS (
      SELECT 
        LOWER(deal_stage) AS deal_stage,
        deal_id,
        associated_company_id,
        snapshot_id
      FROM `{config['BIGQUERY_PROJECT_ID']}.{config['BIGQUERY_DATASET_ID']}.hs_deals`
      WHERE snapshot_id = @snapshot_id
        AND deal_stage NOT IN (
            SELECT stage_id
            FROM `{config['BIGQUERY_PROJECT_ID']}.{config['BIGQUERY_DATASET_ID']}.hs_deal_stage_reference`
            WHERE is_closed = TRUE
        )
    ),
    -- Step 3: Left-join companies with their deals
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
    -- Step 4: Join with stage mapping
    scored AS (
      SELECT
        j.*,
        sm.stage_level,
        sm.adjusted_score
      FROM joined j
      LEFT JOIN `{config['BIGQUERY_PROJECT_ID']}.{config['BIGQUERY_DATASET_ID']}.hs_stage_mapping` sm
        ON sm.combined_stage = j.combined_stage
    )
    SELECT * FROM scored
    """

    logger.info("🔹 Submitting BigQuery job for unit scores...")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
        ],
        destination=f"{config['BIGQUERY_PROJECT_ID']}.{config['BIGQUERY_DATASET_ID']}.hs_pipeline_units_snapshot",
        write_disposition="WRITE_APPEND",
    )

    try:
        job = client.query(query, job_config=job_config)
        logger.info(f"   • BigQuery job ID (unit score): {job.job_id}")
        job.result()
        logger.info(f"✅ Unit-score job completed")
    except GoogleAPIError as e:
        logger.error(f"❌ BigQuery unit-score job failed: {e}", exc_info=True)
        raise

def process_score_history_for_snapshot(snapshot_id: str):
    """Process score history for a snapshot"""
    logger = logging.getLogger('hubspot.scoring.processor')
    logger.info(f"🔹 Processing score history for snapshot: {snapshot_id}")

    # Wait for streaming buffer
    logger.info("   • Waiting 10s for pipeline_units data to be available")
    time.sleep(10)

    config = get_config()
    client = bigquery.Client(project=config['BIGQUERY_PROJECT_ID'])

    query = f"""
    SELECT
      snapshot_id,
      owner_id,
      combined_stage,
      COUNT(DISTINCT company_id) AS num_companies,
      SUM(adjusted_score) AS total_score,
      MAX(snapshot_timestamp) AS snapshot_timestamp
    FROM `{config['BIGQUERY_PROJECT_ID']}.{config['BIGQUERY_DATASET_ID']}.hs_pipeline_units_snapshot`
    WHERE snapshot_id = @snapshot_id
    GROUP BY snapshot_id, owner_id, combined_stage
    """

    logger.info("🔹 Submitting BigQuery job for score history")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
        ],
        destination=f"{config['BIGQUERY_PROJECT_ID']}.{config['BIGQUERY_DATASET_ID']}.hs_pipeline_score_history",
        write_disposition="WRITE_APPEND",
    )

    try:
        job = client.query(query, job_config=job_config)
        logger.info(f"   • BigQuery job ID (score history): {job.job_id}")
        job.result()
        logger.info(f"✅ Score-history job completed")
    except GoogleAPIError as e:
        logger.error(f"❌ BigQuery score-history job failed: {e}", exc_info=True)
        raise

def update_snapshot_registry_scoring(snapshot_id: str, status: str = "completed", notes: str = None):
    """Update snapshot registry when scoring completes"""
    logger = logging.getLogger('hubspot.scoring.registry')
    
    try:
        config = get_config()
        client = bigquery.Client()
        
        update_query = f"""
        UPDATE `{config['BIGQUERY_PROJECT_ID']}.{config['BIGQUERY_DATASET_ID']}.hs_snapshot_registry`
        SET 
            status = @new_status,
            notes = CONCAT(IFNULL(notes, ''), ' | Scoring: ', @new_notes)
        WHERE snapshot_id = @snapshot_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id),
                bigquery.ScalarQueryParameter("new_status", "STRING", f"ingest_and_{status}"),
                bigquery.ScalarQueryParameter("new_notes", "STRING", notes or "completed")
            ]
        )
        
        query_job = client.query(update_query, job_config=job_config)
        query_job.result()
        
        logger.info(f"✅ Updated registry for snapshot {snapshot_id} with scoring status")
        
    except Exception as e:
        logger.error(f"❌ Failed to update snapshot registry: {e}")

def parse_pubsub_event(event):
    """Parse Pub/Sub event data"""
    logger = logging.getLogger('hubspot.scoring')
    
    try:
        if 'data' in event:
            message_data = base64.b64decode(event['data']).decode('utf-8')
            message = json.loads(message_data)
            logger.debug(f"Parsed Pub/Sub message: {message.get('type', 'unknown')}")
            return message
        else:
            logger.warning("No data field in event")
            return None
    except Exception as e:
        logger.error(f"Failed to parse Pub/Sub event: {e}")
        return None

def main(event, context):
    """Main entry point for scoring Cloud Function"""
    logger = setup_logging()
    logger.info("🔄 HubSpot scoring function started")
    
    # Parse the event
    message = parse_pubsub_event(event)
    if not message:
        logger.error("❌ Could not parse event data")
        return {"status": "error", "message": "Invalid event data"}
    
    # Check if this is the event we care about
    if message.get('type') != 'hubspot.snapshot.completed':
        logger.info(f"ℹ️ Ignoring event type: {message.get('type')}")
        return {"status": "ignored", "event_type": message.get('type')}
    
    # Extract snapshot information
    event_data = message.get('data', {})
    snapshot_id = event_data.get('snapshot_id')
    
    if not snapshot_id:
        logger.error("❌ No snapshot_id in event data")
        return {"status": "error", "message": "Missing snapshot_id"}
    
    logger.info(f"📊 Processing snapshot: {snapshot_id}")
    logger.info(f"📋 Data tables: {event_data.get('data_tables', {})}")
    logger.info(f"📋 Reference tables: {event_data.get('reference_tables', {})}")
    
    try:
        # Step 1: Populate stage mapping
        logger.info("🔄 Populating stage mapping...")
        populate_stage_mapping()
        logger.info("✅ Stage mapping populated")
        
        # Step 2: Process the snapshot
        logger.info(f"🔄 Processing snapshot scores for {snapshot_id}...")
        process_snapshot(snapshot_id)
        logger.info(f"✅ Snapshot {snapshot_id} scoring completed")
        
        # Step 3: Update registry with success
        logger.info("📝 Updating snapshot registry...")
        data_counts = event_data.get('data_tables', {})
        total_records = sum(data_counts.values())
        
        update_snapshot_registry_scoring(
            snapshot_id=snapshot_id,
            status="scoring_completed",
            notes=f"Scored {total_records} records successfully"
        )
        logger.info("✅ Registry updated with scoring completion")
        
        # Return success
        result = {
            "status": "success",
            "snapshot_id": snapshot_id,
            "processed_records": total_records,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        logger.info(f"🎉 Scoring function completed successfully for {snapshot_id}")
        return result
        
    except Exception as e:
        logger.error(f"❌ Scoring failed for snapshot {snapshot_id}: {e}", exc_info=True)
        
        # Update registry with failure
        try:
            update_snapshot_registry_scoring(
                snapshot_id=snapshot_id,
                status="scoring_failed",
                notes=f"Error: {str(e)}"
            )
            logger.info("📝 Registry updated with scoring failure")
        except Exception as registry_error:
            logger.error(f"❌ Failed to update registry: {registry_error}")
        
        # Return error
        return {
            "status": "error",
            "snapshot_id": snapshot_id,
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

if __name__ == "__main__":
    # For local testing
    print("🧪 Testing scoring function locally")
    
    test_event = {
        'data': base64.b64encode(json.dumps({
            "type": "hubspot.snapshot.completed",
            "data": {
                "snapshot_id": "2025-06-06T10:00:00",
                "data_tables": {"hs_companies": 100, "hs_deals": 50},
                "reference_tables": {"hs_owners": 5, "hs_deal_stage_reference": 12}
            }
        }).encode('utf-8'))
    }
    
    result = main(test_event, None)
    print(f"Test result: {result}")