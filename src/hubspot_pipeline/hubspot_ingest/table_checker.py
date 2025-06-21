# src/hubspot_pipeline/hubspot_ingest/table_checker.py

import logging
import os
import time
from typing import Dict, List, Tuple, Optional
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

# Import all schemas from the shared schema module
from ..schema import (
    SCHEMA_COMPANIES,
    SCHEMA_DEALS,
    SCHEMA_OWNERS,
    SCHEMA_DEAL_STAGE_REFERENCE,
    SCHEMA_SNAPSHOT_REGISTRY
)

def get_required_tables_with_schemas(schema_config: Dict) -> Dict[str, List[Tuple[str, str]]]:
    """
    Get all required tables and their schemas based on the ingest configuration.
    Uses shared schemas from ../schema.py for consistency across both pipelines.
    
    Args:
        schema_config: The loaded schema configuration
        
    Returns:
        Dictionary of table_name -> schema definition
    """
    logger = logging.getLogger('hubspot.table_checker')
    required_tables = {}
    
    # Add data tables from schema config using shared schemas
    for object_type, config_obj in schema_config.items():
        table_name = config_obj["object_name"]
        
        if table_name == "hs_companies":
            required_tables[table_name] = SCHEMA_COMPANIES
            
        elif table_name == "hs_deals":
            required_tables[table_name] = SCHEMA_DEALS
        else:
            logger.warning(f"⚠️ Unknown data table: {table_name}")
    
    # Add reference tables using shared schemas
    required_tables["hs_owners"] = SCHEMA_OWNERS
    required_tables["hs_deal_stage_reference"] = SCHEMA_DEAL_STAGE_REFERENCE
    
    # Add registry table using shared schema
    required_tables["hs_snapshot_registry"] = SCHEMA_SNAPSHOT_REGISTRY
    
    logger.debug(f"📋 Required tables: {list(required_tables.keys())}")
    
    return required_tables


def ensure_table_exists_and_ready(client: bigquery.Client, full_table_name: str, 
                                 schema: List[Tuple[str, str]], max_attempts: int = 3) -> bool:
    """
    Ensure a table exists and is ready for operations
    
    Args:
        client: BigQuery client
        full_table_name: Full table name (project.dataset.table)
        schema: Table schema as list of (column_name, type) tuples
        max_attempts: Maximum attempts to verify table readiness
        
    Returns:
        True if table is ready, False otherwise
    """
    logger = logging.getLogger('hubspot.table_checker')
    
    try:
        # Try to get existing table
        existing_table = client.get_table(full_table_name)
        logger.debug(f"✅ Table {full_table_name} already exists")
        
        # Quick schema validation in debug mode
        if logger.isEnabledFor(logging.DEBUG):
            existing_fields = {field.name: field.field_type for field in existing_table.schema}
            expected_fields = {col_name: col_type for col_name, col_type in schema}
            
            schema_diffs = []
            for field_name, expected_type in expected_fields.items():
                if field_name in existing_fields:
                    if existing_fields[field_name] != expected_type:
                        schema_diffs.append(f"{field_name}: {existing_fields[field_name]} -> {expected_type}")
                else:
                    schema_diffs.append(f"{field_name}: MISSING -> {expected_type}")
            
            if schema_diffs:
                logger.debug(f"Schema differences in {full_table_name}: {schema_diffs}")
            else:
                logger.debug(f"Schema matches for {full_table_name}")
        
        return True
        
    except NotFound:
        logger.info(f"📝 Creating table {full_table_name}")
        
        # Convert schema to BigQuery schema fields
        bq_schema = []
        for col_name, col_type in schema:
            bq_schema.append(bigquery.SchemaField(col_name, col_type))
        
        try:
            # Create table
            table = bigquery.Table(full_table_name, schema=bq_schema)
            client.create_table(table)
            logger.info(f"✅ Created table {full_table_name}")
            
            # Verify table is ready for operations
            for attempt in range(max_attempts):
                try:
                    time.sleep(1)  # Brief wait between attempts
                    client.get_table(full_table_name)
                    logger.debug(f"✅ Table {full_table_name} verified ready (attempt {attempt + 1})")
                    return True
                except NotFound:
                    if attempt < max_attempts - 1:
                        logger.debug(f"⏳ Table not ready yet, retrying... (attempt {attempt + 1})")
                        continue
                    else:
                        logger.error(f"❌ Table {full_table_name} not ready after {max_attempts} attempts")
                        return False
            
        except Exception as e:
            logger.error(f"❌ Failed to create table {full_table_name}: {e}")
            return False
    
    except Exception as e:
        logger.error(f"❌ Error checking table {full_table_name}: {e}")
        return False


def ensure_all_tables_ready(schema_config: Dict) -> bool:
    """
    Ensure all required tables exist and are ready before starting ingest.
    This is for ingest pipeline - creates tables if they don't exist.
    
    Args:
        schema_config: The loaded schema configuration
        
    Returns:
        True if all tables are ready, False if any issues
    """
    logger = logging.getLogger('hubspot.table_checker')
    logger.info("🔍 Pre-flight check: Ensuring all required tables are ready")
    
    # Get project and dataset info
    project_id = os.getenv("BIGQUERY_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DATASET_ID")
    
    if not project_id or not dataset_id:
        logger.error("❌ Missing BigQuery configuration (project or dataset)")
        return False
    
    # Create BigQuery client with explicit project
    client = bigquery.Client(project=project_id)
    
    # Get all required tables with consistent schemas
    required_tables = get_required_tables_with_schemas(schema_config)
    
    logger.info(f"📋 Checking {len(required_tables)} required tables:")
    for table_name in required_tables.keys():
        logger.info(f"   • {table_name}")
    
    # Debug: Log the schemas being used (with TIMESTAMP fields)
    if logger.isEnabledFor(logging.DEBUG):
        for table_name, table_schema in required_tables.items():
            timestamp_fields = [col for col, col_type in table_schema if col_type == "TIMESTAMP"]
            logger.debug(f"Schema for {table_name}: {len(table_schema)} fields, TIMESTAMP fields: {timestamp_fields}")
    
    # Check each table
    all_ready = True
    start_time = time.time()
    
    for table_name, table_schema in required_tables.items():
        full_table_name = f"{project_id}.{dataset_id}.{table_name}"
        
        logger.debug(f"🔍 Checking table: {table_name}")
        
        if not ensure_table_exists_and_ready(client, full_table_name, table_schema):
            logger.error(f"❌ Table {table_name} is not ready")
            all_ready = False
        else:
            logger.debug(f"✅ Table {table_name} is ready")
    
    total_time = time.time() - start_time
    
    if all_ready:
        logger.info(f"✅ Pre-flight check completed: All {len(required_tables)} tables ready ({total_time:.2f}s)")
        return True
    else:
        logger.error(f"❌ Pre-flight check failed: Some tables not ready ({total_time:.2f}s)")
        return False


def validate_required_data_exists(snapshot_id: str) -> bool:
    """
    Validate that required data exists for scoring pipeline (fail-fast approach).
    This is for scoring pipeline - only checks existence, doesn't create tables.
    
    Args:
        snapshot_id: The snapshot to validate
        
    Returns:
        True if all required data exists, False otherwise
    """
    logger = logging.getLogger('hubspot.scoring.validation')
    
    # Get project and dataset info
    project_id = os.getenv("BIGQUERY_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DATASET_ID")
    
    if not project_id or not dataset_id:
        logger.error("❌ Missing BigQuery configuration (project or dataset)")
        logger.error("💡 Check BIGQUERY_PROJECT_ID and BIGQUERY_DATASET_ID environment variables")
        return False
    
    client = bigquery.Client(project=project_id)
    
    # Define required existence checks
    required_checks = {
        "snapshot_companies": f"""
            SELECT COUNT(*) as count 
            FROM `{project_id}.{dataset_id}.hs_companies` 
            WHERE snapshot_id = @snapshot_id
        """,
        "snapshot_deals": f"""
            SELECT COUNT(*) as count 
            FROM `{project_id}.{dataset_id}.hs_deals` 
            WHERE snapshot_id = @snapshot_id
        """,
        "owners_data": f"""
            SELECT COUNT(*) as count 
            FROM `{project_id}.{dataset_id}.hs_owners`
        """,
        "deal_stages_data": f"""
            SELECT COUNT(*) as count 
            FROM `{project_id}.{dataset_id}.hs_deal_stage_reference`
        """,
    }
    
    logger.info(f"🔍 Validating required data exists for snapshot: {snapshot_id}")
    
    validation_results = {}
    all_valid = True
    
    for check_name, query in required_checks.items():
        try:
            job_config = bigquery.QueryJobConfig()
            if "@snapshot_id" in query:
                job_config.query_parameters = [
                    bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)
                ]
            
            result = client.query(query, job_config=job_config).result()
            count = next(result).count
            
            validation_results[check_name] = count
            
            if count == 0:
                logger.error(f"❌ No data found for {check_name}")
                all_valid = False
            else:
                logger.debug(f"✅ {check_name}: {count} records")
                
        except Exception as e:
            logger.error(f"❌ Validation query failed for {check_name}: {e}")
            if "not found" in str(e).lower() or "404" in str(e):
                logger.error(f"💡 Table missing for {check_name} - run ingest pipeline first")
            else:
                logger.error(f"💡 Check BigQuery permissions and connectivity")
            validation_results[check_name] = 0
            all_valid = False
    
    # Log summary
    if all_valid:
        logger.info(f"✅ Data validation passed: {validation_results}")
        return True
    else:
        logger.error(f"❌ Data validation failed: {validation_results}")
        logger.error("💡 This indicates ingest pipeline failure or premature event publishing")
        logger.error("💡 Run ingest pipeline to ensure all required data is available")
        return False


def verify_table_readiness(table_name: str, max_wait_seconds: int = 10) -> bool:
    """
    Verify a specific table is ready for operations (useful after creation)
    
    Args:
        table_name: Name of the table to verify
        max_wait_seconds: Maximum time to wait for table readiness
        
    Returns:
        True if table is ready, False otherwise
    """
    logger = logging.getLogger('hubspot.table_checker')
    
    project_id = os.getenv("BIGQUERY_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DATASET_ID")
    
    if not project_id or not dataset_id:
        return False
    
    client = bigquery.Client(project=project_id)
    full_table_name = f"{project_id}.{dataset_id}.{table_name}"
    
    start_time = time.time()
    
    while time.time() - start_time < max_wait_seconds:
        try:
            client.get_table(full_table_name)
            logger.debug(f"✅ Table {table_name} is ready")
            return True
        except NotFound:
            time.sleep(0.5)
            continue
        except Exception as e:
            logger.error(f"❌ Error verifying table {table_name}: {e}")
            return False
    
    logger.error(f"❌ Table {table_name} not ready after {max_wait_seconds}s")
    return False


def get_table_counts_for_snapshot(snapshot_id: str) -> Optional[Dict[str, int]]:
    """
    Get record counts for all tables related to a specific snapshot.
    Useful for debugging and validation.
    
    Args:
        snapshot_id: The snapshot to check
        
    Returns:
        Dictionary of table_name -> record_count, or None if error
    """
    logger = logging.getLogger('hubspot.table_checker')
    
    project_id = os.getenv("BIGQUERY_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DATASET_ID")
    
    if not project_id or not dataset_id:
        return None
    
    client = bigquery.Client(project=project_id)
    
    count_queries = {
        "hs_companies": f"""
            SELECT COUNT(*) as count 
            FROM `{project_id}.{dataset_id}.hs_companies` 
            WHERE snapshot_id = @snapshot_id
        """,
        "hs_deals": f"""
            SELECT COUNT(*) as count 
            FROM `{project_id}.{dataset_id}.hs_deals` 
            WHERE snapshot_id = @snapshot_id
        """,
        "hs_owners": f"""
            SELECT COUNT(*) as count 
            FROM `{project_id}.{dataset_id}.hs_owners`
        """,
        "hs_deal_stage_reference": f"""
            SELECT COUNT(*) as count 
            FROM `{project_id}.{dataset_id}.hs_deal_stage_reference`
        """,
    }
    
    counts = {}
    
    for table_name, query in count_queries.items():
        try:
            job_config = bigquery.QueryJobConfig()
            if "@snapshot_id" in query:
                job_config.query_parameters = [
                    bigquery.ScalarQueryParameter("snapshot_id", "TIMESTAMP", snapshot_id)
                ]
            
            result = client.query(query, job_config=job_config).result()
            counts[table_name] = next(result).count
            
        except Exception as e:
            logger.warning(f"⚠️ Failed to get count for {table_name}: {e}")
            counts[table_name] = 0
    
    return counts