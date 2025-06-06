# src/hubspot_pipeline/hubspot_ingest/table_checker.py

import logging
import os
import time
from typing import Dict, List, Tuple
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from .reference.schemas import SNAPSHOT_REGISTRY_SCHEMA

def get_required_tables_with_schemas(schema_config: Dict) -> Dict[str, List[Tuple[str, str]]]:
    """
    Get all required tables and their schemas based on the ingest configuration
    
    Args:
        schema_config: The loaded schema configuration
        
    Returns:
        Dictionary of table_name -> schema definition
    """
    required_tables = {}
    
    # Add data tables from schema config
    for object_type, config_obj in schema_config.items():
        table_name = config_obj["object_name"]
        # We'll need to import the actual schemas - for now use basic schema
        if table_name == "hs_companies":
            from ..schema import SCHEMA_COMPANIES
            required_tables[table_name] = SCHEMA_COMPANIES
        elif table_name == "hs_deals":
            from ..schema import SCHEMA_DEALS
            required_tables[table_name] = SCHEMA_DEALS
    
    # Add reference tables
    required_tables["hs_owners"] = [
        ("owner_id", "STRING"),
        ("email", "STRING"),
        ("first_name", "STRING"),
        ("last_name", "STRING"),
        ("user_id", "STRING"),
        ("active", "BOOLEAN"),
        ("timestamp", "TIMESTAMP"),
    ]
    
    required_tables["hs_deal_stage_reference"] = [
        ("pipeline_id", "STRING"),
        ("pipeline_label", "STRING"),
        ("stage_id", "STRING"),
        ("stage_label", "STRING"),
        ("is_closed", "BOOLEAN"),
        ("probability", "FLOAT"),
        ("display_order", "INTEGER"),
    ]
    
    # Add registry table
    required_tables["hs_snapshot_registry"] = SNAPSHOT_REGISTRY_SCHEMA
    
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
    Ensure all required tables exist and are ready before starting ingest
    
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
    
    # Get all required tables
    required_tables = get_required_tables_with_schemas(schema_config)
    
    logger.info(f"📋 Checking {len(required_tables)} required tables:")
    for table_name in required_tables.keys():
        logger.info(f"   • {table_name}")
    
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