# src/hubspot_pipeline/bigquery_utils.py

import logging
import os
import time
import functools
from typing import List, Dict, Any, Callable, Optional, Type
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, GoogleAPIError

class BigQueryRetryConfig:
    """Configuration for BigQuery retry behavior"""
    
    def __init__(self, max_attempts: int = 3, base_delay: float = 2.0, 
                 exponential_backoff: bool = True, retry_exceptions: List[Type[Exception]] = None):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.exponential_backoff = exponential_backoff
        self.retry_exceptions = retry_exceptions or [NotFound]

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number (1-based)"""
        if self.exponential_backoff:
            return self.base_delay * (2 ** (attempt - 1))
        else:
            return self.base_delay

def bigquery_retry(config: BigQueryRetryConfig = None, operation_name: str = "BigQuery operation"):
    """
    Decorator for adding retry logic to BigQuery operations
    
    Args:
        config: Retry configuration (uses default if None)
        operation_name: Human-readable name for logging
        
    Returns:
        Decorated function with retry logic
    """
    if config is None:
        config = BigQueryRetryConfig()
    
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger('hubspot.bigquery')
            
            for attempt in range(1, config.max_attempts + 1):
                try:
                    if attempt > 1:
                        logger.info(f"🔄 Retry attempt {attempt}/{config.max_attempts} for {operation_name}")
                    
                    result = func(*args, **kwargs)
                    
                    # Success!
                    if attempt > 1:
                        logger.info(f"🎯 {operation_name} success on attempt {attempt}/{config.max_attempts}")
                    
                    return result
                    
                except tuple(config.retry_exceptions) as e:
                    # Retryable error
                    if attempt < config.max_attempts:
                        delay = config.get_delay(attempt)
                        logger.warning(f"⚠️ {operation_name} failed (attempt {attempt}/{config.max_attempts})")
                        logger.info(f"⏳ Waiting {delay}s before retry...")
                        time.sleep(delay)
                        continue
                    else:
                        # Final attempt failed
                        logger.error(f"❌ {operation_name} failed after {config.max_attempts} attempts")
                        logger.error(f"❌ Final error: {e}")
                        raise RuntimeError(f"{operation_name} timeout after {config.max_attempts} attempts: {e}")
                        
                except Exception as e:
                    # Non-retryable error
                    logger.error(f"❌ Non-retryable error in {operation_name} on attempt {attempt}: {e}")
                    raise
            
            # Should never reach here
            raise RuntimeError(f"Unexpected error in retry logic for {operation_name}")
        
        return wrapper
    return decorator

def get_bigquery_client(project_id: Optional[str] = None) -> bigquery.Client:
    """
    Create BigQuery client with consistent configuration
    
    Args:
        project_id: Optional project ID (uses env var if not provided)
        
    Returns:
        Configured BigQuery client
    """
    if project_id is None:
        project_id = os.getenv("BIGQUERY_PROJECT_ID")
        if not project_id:
            raise RuntimeError("BIGQUERY_PROJECT_ID environment variable not set")
    
    return bigquery.Client(project=project_id)

def get_table_reference(table_name: str, dataset: Optional[str] = None, 
                       project_id: Optional[str] = None) -> str:
    """
    Build full BigQuery table reference
    
    Args:
        table_name: Name of the table
        dataset: Dataset name (uses env var if not provided)
        project_id: Project ID (uses env var if not provided)
        
    Returns:
        Full table reference string (project.dataset.table)
    """
    if project_id is None:
        project_id = os.getenv("BIGQUERY_PROJECT_ID")
        if not project_id:
            raise RuntimeError("BIGQUERY_PROJECT_ID environment variable not set")
    
    if dataset is None:
        dataset = os.getenv("BIGQUERY_DATASET_ID", "hubspot_dev")
    
    return f"{project_id}.{dataset}.{table_name}"

def insert_rows_with_retry(client: bigquery.Client, table_ref: str, rows: List[Dict[str, Any]], 
                          operation_name: str = "data insertion") -> None:
    """
    Insert rows to BigQuery with automatic retry logic
    
    Args:
        client: BigQuery client
        table_ref: Full table reference (project.dataset.table)
        rows: List of dictionaries to insert
        operation_name: Description for logging
        
    Raises:
        RuntimeError: If insertion fails after all retries or non-retryable error
    """
    logger = logging.getLogger('hubspot.bigquery')
    
    # Create retry configuration for table readiness issues
    config = BigQueryRetryConfig(
        max_attempts=3,
        base_delay=2.0,
        retry_exceptions=[NotFound]
    )
    
    @bigquery_retry(config, f"{operation_name} to {table_ref}")
    def _insert_operation():
        errors = client.insert_rows_json(table_ref, rows)
        if errors:
            logger.error(f"❌ BigQuery insertion errors: {errors}")
            raise RuntimeError(f"BigQuery insertion failed: {errors}")
        return True
    
    return _insert_operation()

def truncate_and_insert_with_retry(client: bigquery.Client, table_ref: str, rows: List[Dict[str, Any]], 
                                  operation_name: str = "table replacement") -> int:
    """
    Truncate table and insert new data with retry logic
    
    Args:
        client: BigQuery client
        table_ref: Full table reference (project.dataset.table)
        rows: List of dictionaries to insert
        operation_name: Description for logging
        
    Returns:
        Number of rows inserted
        
    Raises:
        RuntimeError: If operation fails after all retries
    """
    logger = logging.getLogger('hubspot.bigquery')
    
    if not rows:
        logger.info(f"📊 No data to replace in {table_ref}")
        return 0
    
    # Step 1: Truncate table
    logger.debug(f"🗑️ Truncating table {table_ref}")
    truncate_query = f"TRUNCATE TABLE `{table_ref}`"
    client.query(truncate_query).result()
    logger.debug("✅ Table truncated")
    
    # Step 2: Insert with retry
    logger.debug(f"⬆️ Inserting {len(rows)} rows")
    insert_rows_with_retry(client, table_ref, rows, f"{operation_name} for {table_ref}")
    
    logger.info(f"✅ Successfully replaced {len(rows)} rows in {table_ref}")
    return len(rows)

def ensure_table_exists(client: bigquery.Client, table_ref: str, 
                       schema: List[bigquery.SchemaField]) -> None:
    """
    Ensure BigQuery table exists with correct schema, create if needed
    
    Args:
        client: BigQuery client
        table_ref: Full table reference (project.dataset.table)
        schema: List of BigQuery schema fields
        
    Raises:
        RuntimeError: If table creation fails
    """
    logger = logging.getLogger('hubspot.bigquery')
    
    try:
        existing_table = client.get_table(table_ref)
        logger.debug(f"✅ Table {table_ref} exists")
    except NotFound:
        logger.info(f"📝 Creating table {table_ref}")
        
        try:
            table = bigquery.Table(table_ref, schema=schema)
            client.create_table(table)
            logger.info(f"✅ Created table {table_ref} with {len(schema)} columns")
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Schema: {[(f.name, f.field_type) for f in schema]}")
        except Exception as e:
            logger.error(f"❌ Failed to create table {table_ref}: {e}")
            raise RuntimeError(f"Failed to create table: {e}")

def infer_bigquery_type(value: Any) -> str:
    """
    Infer BigQuery field type from Python value
    
    Args:
        value: Python value to analyze
        
    Returns:
        BigQuery field type string
    """
    if value is None:
        return "STRING"  # Default for null values
    elif isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, (list, dict)):
        return "STRING"  # Complex types stored as strings
    else:
        return "STRING"

def build_schema_from_sample(sample_row: Dict[str, Any]) -> List[bigquery.SchemaField]:
    """
    Build BigQuery schema from a sample row
    
    Args:
        sample_row: Dictionary representing a sample data row
        
    Returns:
        List of BigQuery schema fields
    """
    schema_fields = []
    for key, value in sample_row.items():
        field_type = infer_bigquery_type(value)
        schema_fields.append(bigquery.SchemaField(key, field_type))
    return schema_fields

# Convenience configurations for different use cases
INSERT_RETRY_CONFIG = BigQueryRetryConfig(
    max_attempts=3,
    base_delay=2.0,
    retry_exceptions=[NotFound]
)

QUERY_RETRY_CONFIG = BigQueryRetryConfig(
    max_attempts=2,
    base_delay=1.0,
    retry_exceptions=[GoogleAPIError]  # Could expand based on needs
)