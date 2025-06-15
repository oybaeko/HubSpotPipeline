# src/hubspot_pipeline/hubspot_scoring/stage_mapping.py

import logging
import os
from typing import List, Tuple
from datetime import datetime
from google.cloud import bigquery

# Stage mapping schema with record_timestamp
STAGE_MAPPING_SCHEMA: List[Tuple[str, str]] = [
    ("lifecycle_stage", "STRING"),
    ("lead_status",     "STRING"),
    ("deal_stage",      "STRING"),
    ("combined_stage",  "STRING"),
    ("stage_level",     "INTEGER"),
    ("adjusted_score",  "FLOAT"),
    ("record_timestamp", "TIMESTAMP"),
]

def get_stage_mapping_data():
    """Get the hardcoded stage mapping configuration with consistent timestamps"""
    current_timestamp = datetime.utcnow().isoformat() + "Z"
    
    return [
        # Lead lifecycle stages
        {"lifecycle_stage": "lead", "lead_status": "new", "deal_stage": None, "combined_stage": "lead/new", "stage_level": 1, "adjusted_score": 1.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "lead", "lead_status": "restart", "deal_stage": None, "combined_stage": "lead/restart", "stage_level": 1, "adjusted_score": 1.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "lead", "lead_status": "attempted to contact", "deal_stage": None, "combined_stage": "lead/attempted_to_contact", "stage_level": 2, "adjusted_score": 1.5, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "lead", "lead_status": "connected", "deal_stage": None, "combined_stage": "lead/connected", "stage_level": 3, "adjusted_score": 2.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "lead", "lead_status": "nurturing", "deal_stage": None, "combined_stage": "lead/nurturing", "stage_level": 0, "adjusted_score": 2.0, "record_timestamp": current_timestamp},

        # Sales Qualified Lead (no lead_status or deal_stage)
        {"lifecycle_stage": "sales qualified lead", "lead_status": None, "deal_stage": None, "combined_stage": "salesqualifiedlead", "stage_level": 4, "adjusted_score": 6.0, "record_timestamp": current_timestamp},

        # Opportunity (deal-driven)
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": None, "combined_stage": "opportunity/missing", "stage_level": 5, "adjusted_score": 7.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": "appointmentscheduled", "combined_stage": "opportunity/appointmentscheduled", "stage_level": 5, "adjusted_score": 8.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": "qualifiedtobuy", "combined_stage": "opportunity/qualifiedtobuy", "stage_level": 6, "adjusted_score": 10.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": "presentationscheduled", "combined_stage": "opportunity/presentationscheduled", "stage_level": 7, "adjusted_score": 12.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": "decisionmakerboughtin", "combined_stage": "opportunity/decisionmakerboughtin", "stage_level": 8, "adjusted_score": 14.0, "record_timestamp": current_timestamp},

        # Closed-Won
        {"lifecycle_stage": "closed-won", "lead_status": None, "deal_stage": "contractsent", "combined_stage": "closed-won/contractsent", "stage_level": 9, "adjusted_score": 30.0, "record_timestamp": current_timestamp},

        # Disqualified
        {"lifecycle_stage": "disqualified", "lead_status": None, "deal_stage": None, "combined_stage": "disqualified", "stage_level": -1, "adjusted_score": 0.0, "record_timestamp": current_timestamp}
    ]

def ensure_stage_mapping_table_exists():
    """Ensure the stage mapping table exists with correct schema including record_timestamp"""
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
    Populate the hs_stage_mapping table with scoring configuration using SQL INSERT
    to maintain correct column order
    
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

        # Ensure table exists with correct schema
        ensure_stage_mapping_table_exists()

        # Get stage mapping data
        stage_mapping = get_stage_mapping_data()

        # Truncate existing data
        logger.info(f"🗑️ Truncating table {table_ref}")
        client.query(f"TRUNCATE TABLE `{table_ref}`").result()

        # Build INSERT statement with column order derived from schema
        column_names = [col_name for col_name, col_type in STAGE_MAPPING_SCHEMA]
        columns_clause = ", ".join(column_names)
        
        insert_sql = f"""
        INSERT INTO `{table_ref}` (
            {columns_clause}
        ) VALUES
        """
        
        # Build VALUES clauses using schema order
        values_clauses = []
        for record in stage_mapping:
            # Build values in schema order
            values = []
            for col_name, col_type in STAGE_MAPPING_SCHEMA:
                value = record[col_name]
                
                # Handle different data types properly for SQL
                if value is None:
                    values.append("NULL")
                elif col_type == "STRING":
                    values.append(f"'{value}'")
                elif col_type == "TIMESTAMP":
                    values.append(f"TIMESTAMP('{value}')")
                else:  # INTEGER, FLOAT
                    values.append(str(value))
            
            values_clause = f"({', '.join(values)})"
            values_clauses.append(values_clause)
        
        # Combine INSERT with VALUES
        full_insert_sql = insert_sql + ",\n".join(values_clauses)
        
        logger.info(f"⏳ Loading {len(stage_mapping)} stage mapping records using SQL INSERT")
        insert_job = client.query(full_insert_sql)
        insert_job.result()

        logger.info(f"✅ Loaded {len(stage_mapping)} rows into {table_ref}")
        return len(stage_mapping)
        
    except Exception as e:
        logger.error(f"❌ Failed to populate stage mapping: {e}")
        raise

def populate_stage_mapping_alternative():
    """
    Alternative method: Delete and recreate table entirely to ensure correct column order
    
    Returns:
        int: Number of stage mapping records loaded
    """
    logger = logging.getLogger('hubspot.scoring.stage_mapping')
    logger.info("🔄 Populating stage mapping table (recreate method)")
    
    try:
        client = bigquery.Client()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        table_ref = f"{project_id}.{dataset_id}.hs_stage_mapping"

        # Step 1: Delete existing table
        logger.info(f"🗑️ Deleting existing table {table_ref}")
        client.delete_table(table_ref, not_found_ok=True)

        # Step 2: Create table with correct schema order
        bq_schema = []
        for col_name, col_type in STAGE_MAPPING_SCHEMA:
            bq_schema.append(bigquery.SchemaField(col_name, col_type))
        
        logger.info(f"📝 Creating table {table_ref} with correct schema order")
        table = bigquery.Table(table_ref, schema=bq_schema)
        client.create_table(table)

        # Step 3: Load data using SQL to maintain schema column order
        stage_mapping = get_stage_mapping_data()
        
        # Build INSERT statement with column order derived from schema
        column_names = [col_name for col_name, col_type in STAGE_MAPPING_SCHEMA]
        columns_clause = ", ".join(column_names)
        
        insert_sql = f"""
        INSERT INTO `{table_ref}` (
            {columns_clause}
        ) VALUES
        """
        
        # Build VALUES clauses using schema order
        values_clauses = []
        for record in stage_mapping:
            # Build values in schema order
            values = []
            for col_name, col_type in STAGE_MAPPING_SCHEMA:
                value = record[col_name]
                
                # Handle different data types properly for SQL
                if value is None:
                    values.append("NULL")
                elif col_type == "STRING":
                    values.append(f"'{value}'")
                elif col_type == "TIMESTAMP":
                    values.append(f"TIMESTAMP('{value}')")
                else:  # INTEGER, FLOAT
                    values.append(str(value))
            
            values_clause = f"({', '.join(values)})"
            values_clauses.append(values_clause)
        
        # Execute INSERT
        full_insert_sql = insert_sql + ",\n".join(values_clauses)
        
        logger.info(f"⏳ Loading {len(stage_mapping)} stage mapping records")
        insert_job = client.query(full_insert_sql)
        insert_job.result()

        logger.info(f"✅ Loaded {len(stage_mapping)} rows into {table_ref}")
        return len(stage_mapping)
        
    except Exception as e:
        logger.error(f"❌ Failed to populate stage mapping (recreate method): {e}")
        raise