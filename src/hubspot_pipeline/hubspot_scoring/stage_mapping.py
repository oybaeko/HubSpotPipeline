# src/hubspot_pipeline/scoring/stage_mapping.py

import logging
import os
from typing import List, Tuple
from google.cloud import bigquery

# Stage mapping schema
STAGE_MAPPING_SCHEMA: List[Tuple[str, str]] = [
    ("lifecycle_stage", "STRING"),
    ("lead_status",     "STRING"),
    ("deal_stage",      "STRING"),
    ("combined_stage",  "STRING"),
    ("stage_level",     "INTEGER"),
    ("adjusted_score",  "FLOAT"),
]

def get_stage_mapping_data():
    """Get the hardcoded stage mapping configuration"""
    return [
        # Lead lifecycle stages
        {"lifecycle_stage": "lead", "lead_status": "new", "deal_stage": None, "combined_stage": "lead/new", "stage_level": 1, "adjusted_score": 1.0},
        {"lifecycle_stage": "lead", "lead_status": "restart", "deal_stage": None, "combined_stage": "lead/restart", "stage_level": 1, "adjusted_score": 1.0},
        {"lifecycle_stage": "lead", "lead_status": "attempted to contact", "deal_stage": None, "combined_stage": "lead/attempted_to_contact", "stage_level": 2, "adjusted_score": 1.5},
        {"lifecycle_stage": "lead", "lead_status": "connected", "deal_stage": None, "combined_stage": "lead/connected", "stage_level": 3, "adjusted_score": 2.0},
        {"lifecycle_stage": "lead", "lead_status": "nurturing", "deal_stage": None, "combined_stage": "lead/nurturing", "stage_level": 0, "adjusted_score": 2.0},

        # Sales Qualified Lead (no lead_status or deal_stage)
        {"lifecycle_stage": "sales qualified lead", "lead_status": None, "deal_stage": None, "combined_stage": "salesqualifiedlead", "stage_level": 4, "adjusted_score": 6.0},

        # Opportunity (deal-driven)
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

def ensure_stage_mapping_table_exists():
    """Ensure the stage mapping table exists with correct schema"""
    logger = logging.getLogger('hubspot.scoring.stage_mapping')
    
    client = bigquery.Client()
    project_id = os.getenv('BIGQUERY_PROJECT_ID')
    dataset_id = os.getenv('BIGQUERY_DATASET_ID')
    table_name = "hs_stage_mapping"
    full_table = f"{project_id}.{dataset_id}.{table_name}"
    
    try:
        existing_table = client.get_table(full_table)
        logger.debug(f"✅ Stage mapping table {full_table} exists")
    except Exception:
        logger.info(f"📝 Creating stage mapping table {full_table}")
        
        # Convert schema to BigQuery schema fields
        bq_schema = []
        for col_name, col_type in STAGE_MAPPING_SCHEMA:
            bq_schema.append(bigquery.SchemaField(col_name, col_type))
        
        try:
            table = bigquery.Table(full_table, schema=bq_schema)
            client.create_table(table)
            logger.info(f"✅ Created stage mapping table {full_table}")
        except Exception as e:
            logger.error(f"❌ Failed to create stage mapping table: {e}")
            raise RuntimeError(f"Failed to create stage mapping table: {e}")

def populate_stage_mapping():
    """
    Populate the hs_stage_mapping table with scoring configuration
    
    Returns:
        int: Number of stage mapping records loaded
    """
    logger = logging.getLogger('hubspot.scoring.stage_mapping')
    logger.info("🔄 Populating stage mapping table")
    
    try:
        client = bigquery.Client()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        table_ref = f"{project_id}.{dataset_id}.hs_stage_mapping"

        # Ensure table exists
        ensure_stage_mapping_table_exists()

        # Get stage mapping data
        stage_mapping = get_stage_mapping_data()

        # Recreate table (truncate and reload)
        logger.info(f"🗑️ Truncating table {table_ref}")
        client.query(f"TRUNCATE TABLE `{table_ref}`").result()

        # Load new data
        logger.info(f"⏳ Loading {len(stage_mapping)} stage mapping records")
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_json(stage_mapping, table_ref, job_config=job_config)
        job.result()

        logger.info(f"✅ Loaded {len(stage_mapping)} rows into {table_ref}")
        return len(stage_mapping)
        
    except Exception as e:
        logger.error(f"❌ Failed to populate stage mapping: {e}")
        raise