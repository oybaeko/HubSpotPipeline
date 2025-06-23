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
    """
    Get the hardcoded stage mapping configuration with consistent timestamps and normalized lowercase values.
    
    IMPORTANT: All string values are normalized to lowercase to match ingest pipeline normalization.
    """
    current_timestamp = datetime.utcnow().isoformat() + "Z"
    
    # All values normalized to lowercase for consistency with ingest pipeline
    return [
        # Lead lifecycle stages - all normalized to lowercase
        {"lifecycle_stage": "lead", "lead_status": "new", "deal_stage": None, "combined_stage": "lead/new", "stage_level": 1, "adjusted_score": 1.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "lead", "lead_status": "restart", "deal_stage": None, "combined_stage": "lead/restart", "stage_level": 1, "adjusted_score": 1.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "lead", "lead_status": "attempted_to_contact", "deal_stage": None, "combined_stage": "lead/attempted_to_contact", "stage_level": 2, "adjusted_score": 1.5, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "lead", "lead_status": "connected", "deal_stage": None, "combined_stage": "lead/connected", "stage_level": 3, "adjusted_score": 2.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "lead", "lead_status": "nurturing", "deal_stage": None, "combined_stage": "lead/nurturing", "stage_level": 0, "adjusted_score": 2.0, "record_timestamp": current_timestamp},

        # Sales Qualified Lead (normalized to lowercase)
        {"lifecycle_stage": "salesqualifiedlead", "lead_status": None, "deal_stage": None, "combined_stage": "salesqualifiedlead", "stage_level": 4, "adjusted_score": 6.0, "record_timestamp": current_timestamp},
        
        # Handle both normalized forms for backward compatibility
        {"lifecycle_stage": "sales qualified lead", "lead_status": None, "deal_stage": None, "combined_stage": "salesqualifiedlead", "stage_level": 4, "adjusted_score": 6.0, "record_timestamp": current_timestamp},

        # Opportunity (deal-driven) - all normalized to lowercase
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": None, "combined_stage": "opportunity/missing", "stage_level": 5, "adjusted_score": 7.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": "appointmentscheduled", "combined_stage": "opportunity/appointmentscheduled", "stage_level": 5, "adjusted_score": 8.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": "qualifiedtobuy", "combined_stage": "opportunity/qualifiedtobuy", "stage_level": 6, "adjusted_score": 10.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": "presentationscheduled", "combined_stage": "opportunity/presentationscheduled", "stage_level": 7, "adjusted_score": 12.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "opportunity", "lead_status": None, "deal_stage": "decisionmakerboughtin", "combined_stage": "opportunity/decisionmakerboughtin", "stage_level": 8, "adjusted_score": 14.0, "record_timestamp": current_timestamp},

        # Closed-Won (normalized to lowercase)
        {"lifecycle_stage": "closed-won", "lead_status": None, "deal_stage": "contractsent", "combined_stage": "closed-won/contractsent", "stage_level": 9, "adjusted_score": 30.0, "record_timestamp": current_timestamp},

        # Disqualified (normalized to lowercase)
        {"lifecycle_stage": "disqualified", "lead_status": None, "deal_stage": None, "combined_stage": "disqualified", "stage_level": -1, "adjusted_score": 0.0, "record_timestamp": current_timestamp},
        
        # Additional normalized mappings for common variations
        {"lifecycle_stage": "customer", "lead_status": None, "deal_stage": None, "combined_stage": "customer", "stage_level": 10, "adjusted_score": 50.0, "record_timestamp": current_timestamp},
        {"lifecycle_stage": "evangelist", "lead_status": None, "deal_stage": None, "combined_stage": "evangelist", "stage_level": 10, "adjusted_score": 60.0, "record_timestamp": current_timestamp},
    ]

def validate_stage_mapping_normalization():
    """
    Validate that all stage mapping data is properly normalized to lowercase.
    
    Returns:
        dict: Validation results
    """
    logger = logging.getLogger('hubspot.scoring.stage_mapping')
    
    stage_mapping = get_stage_mapping_data()
    validation_results = {
        'status': 'success',
        'issues': [],
        'total_records': len(stage_mapping)
    }
    
    for i, record in enumerate(stage_mapping):
        # Check string fields for proper normalization
        string_fields = ['lifecycle_stage', 'lead_status', 'deal_stage', 'combined_stage']
        
        for field in string_fields:
            value = record.get(field)
            if value is not None and isinstance(value, str):
                if value != value.lower():
                    validation_results['issues'].append({
                        'record_index': i,
                        'field': field,
                        'value': value,
                        'expected': value.lower(),
                        'issue': 'not_lowercase'
                    })
    
    if validation_results['issues']:
        validation_results['status'] = 'issues_found'
        logger.warning(f"🔧 Found {len(validation_results['issues'])} normalization issues in stage mapping")
        for issue in validation_results['issues']:
            logger.warning(f"   • Record {issue['record_index']}: {issue['field']} = '{issue['value']}' should be '{issue['expected']}'")
    else:
        logger.info("✅ Stage mapping data is properly normalized")
    
    return validation_results

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
    to maintain correct column order. All data is pre-normalized to lowercase.
    
    Returns:
        int: Number of stage mapping records loaded
    """
    logger = logging.getLogger('hubspot.scoring.stage_mapping')
    logger.info("🔄 Populating stage mapping table with normalized lowercase values")
    
    try:
        # Validate normalization before proceeding
        validation = validate_stage_mapping_normalization()
        if validation['status'] == 'issues_found':
            logger.error(f"❌ Stage mapping contains normalization issues: {len(validation['issues'])} problems")
            raise RuntimeError("Stage mapping data is not properly normalized")
        
        client = bigquery.Client()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        table_ref = f"{project_id}.{dataset_id}.hs_stage_mapping"

        # Ensure table exists with correct schema
        ensure_stage_mapping_table_exists()

        # Get stage mapping data (already normalized)
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
                    # Ensure string values are properly escaped and normalized
                    escaped_value = value.replace("'", "''") if value else ""
                    values.append(f"'{escaped_value}'")
                elif col_type == "TIMESTAMP":
                    values.append(f"TIMESTAMP('{value}')")
                else:  # INTEGER, FLOAT
                    values.append(str(value))
            
            values_clause = f"({', '.join(values)})"
            values_clauses.append(values_clause)
        
        # Combine INSERT with VALUES
        full_insert_sql = insert_sql + ",\n".join(values_clauses)
        
        logger.info(f"⏳ Loading {len(stage_mapping)} normalized stage mapping records using SQL INSERT")
        insert_job = client.query(full_insert_sql)
        insert_job.result()

        # Verify normalization after insert
        logger.info("🔧 Verifying post-insert normalization...")
        verification_query = f"""
        SELECT 
          COUNT(*) as total_records,
          SUM(CASE WHEN lifecycle_stage != LOWER(lifecycle_stage) THEN 1 ELSE 0 END) as lifecycle_issues,
          SUM(CASE WHEN lead_status != LOWER(lead_status) THEN 1 ELSE 0 END) as lead_status_issues,
          SUM(CASE WHEN deal_stage != LOWER(deal_stage) THEN 1 ELSE 0 END) as deal_stage_issues,
          SUM(CASE WHEN combined_stage != LOWER(combined_stage) THEN 1 ELSE 0 END) as combined_stage_issues
        FROM `{table_ref}`
        """
        
        verification_job = client.query(verification_query)
        verification_result = next(verification_job.result())
        
        total_issues = (
            verification_result.lifecycle_issues +
            verification_result.lead_status_issues +
            verification_result.deal_stage_issues +
            verification_result.combined_stage_issues
        )
        
        if total_issues > 0:
            logger.warning(f"⚠️ Found {total_issues} normalization issues after insert")
            logger.warning("💡 This indicates a problem with the stage mapping data")
        else:
            logger.info("✅ All stage mapping records are properly normalized")

        logger.info(f"✅ Loaded {len(stage_mapping)} normalized rows into {table_ref}")
        return len(stage_mapping)
        
    except Exception as e:
        logger.error(f"❌ Failed to populate stage mapping: {e}")
        raise

def populate_stage_mapping_alternative():
    """
    Alternative method: Delete and recreate table entirely to ensure correct column order
    with normalized data validation.
    
    Returns:
        int: Number of stage mapping records loaded
    """
    logger = logging.getLogger('hubspot.scoring.stage_mapping')
    logger.info("🔄 Populating stage mapping table (recreate method) with normalized data")
    
    try:
        # Validate normalization before proceeding
        validation = validate_stage_mapping_normalization()
        if validation['status'] == 'issues_found':
            logger.error(f"❌ Stage mapping contains normalization issues: {len(validation['issues'])} problems")
            raise RuntimeError("Stage mapping data is not properly normalized")
        
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
                    # Ensure string values are properly escaped and normalized
                    escaped_value = value.replace("'", "''") if value else ""
                    values.append(f"'{escaped_value}'")
                elif col_type == "TIMESTAMP":
                    values.append(f"TIMESTAMP('{value}')")
                else:  # INTEGER, FLOAT
                    values.append(str(value))
            
            values_clause = f"({', '.join(values)})"
            values_clauses.append(values_clause)
        
        # Execute INSERT
        full_insert_sql = insert_sql + ",\n".join(values_clauses)
        
        logger.info(f"⏳ Loading {len(stage_mapping)} normalized stage mapping records")
        insert_job = client.query(full_insert_sql)
        insert_job.result()

        # Verify normalization after insert
        logger.info("🔧 Verifying post-insert normalization...")
        verification_query = f"""
        SELECT 
          COUNT(*) as total_records,
          SUM(CASE WHEN lifecycle_stage != LOWER(lifecycle_stage) THEN 1 ELSE 0 END) as lifecycle_issues,
          SUM(CASE WHEN lead_status != LOWER(lead_status) THEN 1 ELSE 0 END) as lead_status_issues,
          SUM(CASE WHEN deal_stage != LOWER(deal_stage) THEN 1 ELSE 0 END) as deal_stage_issues,
          SUM(CASE WHEN combined_stage != LOWER(combined_stage) THEN 1 ELSE 0 END) as combined_stage_issues
        FROM `{table_ref}`
        """
        
        verification_job = client.query(verification_query)
        verification_result = next(verification_job.result())
        
        total_issues = (
            verification_result.lifecycle_issues +
            verification_result.lead_status_issues +
            verification_result.deal_stage_issues +
            verification_result.combined_stage_issues
        )
        
        if total_issues > 0:
            logger.warning(f"⚠️ Found {total_issues} normalization issues after recreate")
        else:
            logger.info("✅ All stage mapping records are properly normalized")

        logger.info(f"✅ Loaded {len(stage_mapping)} normalized rows into {table_ref}")
        return len(stage_mapping)
        
    except Exception as e:
        logger.error(f"❌ Failed to populate stage mapping (recreate method): {e}")
        raise