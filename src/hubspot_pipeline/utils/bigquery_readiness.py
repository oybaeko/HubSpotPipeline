# src/hubspot_pipeline/utils/bigquery_readiness.py

import logging
import time
import os
from typing import Optional, Dict, Any
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def wait_for_data_availability(
    table_name: str, 
    expected_snapshot_id: str,
    min_expected_rows: int = 1,
    max_wait_seconds: int = 120,
    check_interval: int = 2,
    dataset: str = None
) -> bool:
    """
    Wait for data to become available in BigQuery table after streaming insert
    
    Args:
        table_name: Name of the BigQuery table
        expected_snapshot_id: The snapshot_id we're waiting for
        min_expected_rows: Minimum number of rows expected
        max_wait_seconds: Maximum time to wait
        check_interval: Seconds between checks
        dataset: Dataset name (uses env var if not provided)
        
    Returns:
        True if data is available, False if timeout
    """
    logger = logging.getLogger('hubspot.bigquery.readiness')
    
    client = bigquery.Client()
    project_id = os.getenv("BIGQUERY_PROJECT_ID") or client.project
    dataset = dataset or os.getenv("BIGQUERY_DATASET_ID")
    full_table = f"{project_id}.{dataset}.{table_name}"
    
    logger.info(f"⏳ Waiting for data availability in {table_name} (snapshot: {expected_snapshot_id})")
    
    start_time = time.time()
    attempt = 0
    
    while time.time() - start_time < max_wait_seconds:
        attempt += 1
        
        try:
            # Check if data is queryable (not just in streaming buffer)
            query = f"""
            SELECT COUNT(*) as row_count
            FROM `{full_table}`
            WHERE snapshot_id = @snapshot_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("snapshot_id", "STRING", expected_snapshot_id)
                ]
            )
            
            result = client.query(query, job_config=job_config).result()
            row_count = next(result).row_count
            
            # Also check streaming buffer status
            table = client.get_table(full_table)
            streaming_buffer = table.streaming_buffer
            
            if streaming_buffer:
                estimated_bytes = streaming_buffer.estimated_bytes
                estimated_rows = streaming_buffer.estimated_rows
                logger.debug(f"Attempt {attempt}: Streaming buffer active - "
                           f"estimated_rows: {estimated_rows}, estimated_bytes: {estimated_bytes}")
            else:
                logger.debug(f"Attempt {attempt}: No streaming buffer detected")
            
            if row_count >= min_expected_rows:
                elapsed = time.time() - start_time
                logger.info(f"✅ Data available: {row_count} rows in {elapsed:.2f}s")
                
                # Additional verification: ensure no active streaming buffer for this snapshot
                if streaming_buffer and streaming_buffer.estimated_rows > 0:
                    logger.debug(f"📊 Note: Streaming buffer still active, but data is queryable")
                
                return True
            else:
                logger.debug(f"Attempt {attempt}: Found {row_count} rows, need {min_expected_rows}")
                
        except Exception as e:
            logger.debug(f"Attempt {attempt}: Query failed: {e}")
            
        # Wait before next attempt
        time.sleep(check_interval)
    
    # Timeout reached
    elapsed = time.time() - start_time
    logger.warning(f"⏰ Timeout waiting for data in {table_name} after {elapsed:.2f}s")
    return False


def wait_for_table_ready(
    table_name: str,
    max_wait_seconds: int = 30,
    check_interval: int = 1,
    dataset: str = None
) -> bool:
    """
    Wait for table to be ready for operations after creation
    
    Args:
        table_name: Name of the BigQuery table
        max_wait_seconds: Maximum time to wait
        check_interval: Seconds between checks
        dataset: Dataset name (uses env var if not provided)
        
    Returns:
        True if table is ready, False if timeout
    """
    logger = logging.getLogger('hubspot.bigquery.readiness')
    
    client = bigquery.Client()
    project_id = os.getenv("BIGQUERY_PROJECT_ID") or client.project
    dataset = dataset or os.getenv("BIGQUERY_DATASET_ID")
    full_table = f"{project_id}.{dataset}.{table_name}"
    
    logger.debug(f"⏳ Waiting for table {table_name} to be ready")
    
    start_time = time.time()
    
    while time.time() - start_time < max_wait_seconds:
        try:
            table = client.get_table(full_table)
            
            # Try a simple query to ensure table is actually queryable
            query = f"SELECT 1 FROM `{full_table}` LIMIT 1"
            client.query(query).result()
            
            logger.debug(f"✅ Table {table_name} is ready")
            return True
            
        except NotFound:
            logger.debug(f"Table {table_name} not found yet, retrying...")
        except Exception as e:
            logger.debug(f"Table {table_name} not ready: {e}")
            
        time.sleep(check_interval)
    
    elapsed = time.time() - start_time
    logger.warning(f"⏰ Timeout waiting for table {table_name} after {elapsed:.2f}s")
    return False


def check_streaming_buffer_status(table_name: str, dataset: str = None) -> Dict[str, Any]:
    """
    Check the current streaming buffer status for a table
    
    Args:
        table_name: Name of the BigQuery table
        dataset: Dataset name (uses env var if not provided)
        
    Returns:
        Dict with streaming buffer information
    """
    logger = logging.getLogger('hubspot.bigquery.readiness')
    
    client = bigquery.Client()
    project_id = os.getenv("BIGQUERY_PROJECT_ID") or client.project
    dataset = dataset or os.getenv("BIGQUERY_DATASET_ID")
    full_table = f"{project_id}.{dataset}.{table_name}"
    
    try:
        table = client.get_table(full_table)
        streaming_buffer = table.streaming_buffer
        
        if streaming_buffer:
            status = {
                'has_streaming_buffer': True,
                'estimated_bytes': streaming_buffer.estimated_bytes,
                'estimated_rows': streaming_buffer.estimated_rows,
                'oldest_entry_time': streaming_buffer.oldest_entry_time.isoformat() if streaming_buffer.oldest_entry_time else None
            }
        else:
            status = {
                'has_streaming_buffer': False,
                'estimated_bytes': 0,
                'estimated_rows': 0,
                'oldest_entry_time': None
            }
        
        logger.debug(f"Streaming buffer status for {table_name}: {status}")
        return status
        
    except Exception as e:
        logger.error(f"Failed to check streaming buffer for {table_name}: {e}")
        return {'error': str(e)}


def force_streaming_buffer_flush(table_name: str, dataset: str = None) -> bool:
    """
    Attempt to force streaming buffer flush using query with CURRENT_TIMESTAMP()
    This is a workaround that sometimes helps
    
    Args:
        table_name: Name of the BigQuery table  
        dataset: Dataset name (uses env var if not provided)
        
    Returns:
        True if successful, False otherwise
    """
    logger = logging.getLogger('hubspot.bigquery.readiness')
    
    client = bigquery.Client()
    project_id = os.getenv("BIGQUERY_PROJECT_ID") or client.project
    dataset = dataset or os.getenv("BIGQUERY_DATASET_ID")
    full_table = f"{project_id}.{dataset}.{table_name}"
    
    try:
        # This query forces BigQuery to process the streaming buffer
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            CURRENT_TIMESTAMP() as query_time
        FROM `{full_table}`
        """
        
        logger.debug(f"🔄 Attempting to flush streaming buffer for {table_name}")
        result = client.query(query).result()
        row = next(result)
        
        logger.debug(f"✅ Buffer flush query completed: {row.total_rows} rows at {row.query_time}")
        return True
        
    except Exception as e:
        logger.warning(f"⚠️ Failed to flush streaming buffer for {table_name}: {e}")
        return False


def ensure_data_ready_for_scoring(snapshot_id: str, data_counts: Dict[str, int]) -> bool:
    """
    Ensure all required data is ready before starting scoring
    
    Args:
        snapshot_id: The snapshot identifier
        data_counts: Expected row counts by table
        
    Returns:
        True if all data is ready, False otherwise
    """
    logger = logging.getLogger('hubspot.bigquery.readiness')
    
    logger.info(f"🔍 Verifying data readiness for scoring (snapshot: {snapshot_id})")
    
    # Tables to check before scoring
    tables_to_check = {
        'hs_companies': data_counts.get('hs_companies', 0),
        'hs_deals': data_counts.get('hs_deals', 0)
    }
    
    all_ready = True
    
    for table_name, expected_count in tables_to_check.items():
        if expected_count > 0:  # Only check if we expect data
            logger.info(f"📊 Checking {table_name} (expecting {expected_count} rows)")
            
            ready = wait_for_data_availability(
                table_name=table_name,
                expected_snapshot_id=snapshot_id,
                min_expected_rows=expected_count,
                max_wait_seconds=90  # Allow up to 90 seconds
            )
            
            if not ready:
                logger.error(f"❌ Data not ready in {table_name}")
                all_ready = False
            else:
                logger.info(f"✅ {table_name} data ready")
    
    if all_ready:
        logger.info("🎉 All data ready for scoring")
    else:
        logger.error("❌ Some data not ready - scoring may fail")
    
    return all_ready


def wait_for_scoring_input_ready(snapshot_id: str, max_wait_seconds: int = 120) -> bool:
    """
    Wait specifically for scoring input data (companies + deals) to be ready
    
    Args:
        snapshot_id: The snapshot identifier
        max_wait_seconds: Maximum time to wait
        
    Returns:
        True if input data is ready, False otherwise
    """
    logger = logging.getLogger('hubspot.bigquery.readiness')
    
    logger.info(f"⏳ Waiting for scoring input data (snapshot: {snapshot_id})")
    
    # Check both tables simultaneously
    companies_ready = wait_for_data_availability(
        table_name='hs_companies',
        expected_snapshot_id=snapshot_id,
        min_expected_rows=1,
        max_wait_seconds=max_wait_seconds
    )
    
    deals_ready = wait_for_data_availability(
        table_name='hs_deals', 
        expected_snapshot_id=snapshot_id,
        min_expected_rows=1,
        max_wait_seconds=max_wait_seconds
    )
    
    if companies_ready and deals_ready:
        logger.info("✅ Both companies and deals data ready for scoring")
        return True
    else:
        logger.warning("⚠️ Not all input data ready for scoring")
        return False


def verify_registry_prerequisite(snapshot_id: str, required_status: str = "ingest_completed") -> Dict[str, Any]:
    """
    Check snapshot registry for prerequisite completion
    
    Args:
        snapshot_id: The snapshot identifier
        required_status: Status that must be present
        
    Returns:
        Dict with registry info and verification result
    """
    logger = logging.getLogger('hubspot.bigquery.readiness')
    
    client = bigquery.Client()
    project_id = os.getenv("BIGQUERY_PROJECT_ID") or client.project
    dataset = os.getenv("BIGQUERY_DATASET_ID")
    
    try:
        query = f"""
        SELECT 
            snapshot_id,
            status,
            companies_count,
            deals_count,
            owners_count,
            deal_stages_count,
            notes,
            processing_duration_seconds
        FROM `{project_id}.{dataset}.hs_snapshot_registry`
        WHERE snapshot_id = @snapshot_id
        ORDER BY snapshot_timestamp DESC
        LIMIT 1
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
            ]
        )
        
        result = client.query(query, job_config=job_config).result()
        latest_record = next(result, None)
        
        if not latest_record:
            logger.warning(f"⚠️ No registry entry found for snapshot {snapshot_id}")
            return {
                'found': False,
                'status': None,
                'prerequisite_met': False,
                'data_counts': {}
            }
        
        status = latest_record.status
        prerequisite_met = status == required_status
        
        data_counts = {
            'companies': latest_record.companies_count or 0,
            'deals': latest_record.deals_count or 0,
            'owners': latest_record.owners_count or 0,
            'deal_stages': latest_record.deal_stages_count or 0
        }
        
        logger.info(f"📋 Registry status for {snapshot_id}: {status} (required: {required_status})")
        
        if prerequisite_met:
            logger.info(f"✅ Prerequisite met: {required_status}")
            logger.info(f"📊 Data counts: {data_counts}")
        else:
            logger.warning(f"⚠️ Prerequisite not met: {status} != {required_status}")
        
        return {
            'found': True,
            'status': status,
            'prerequisite_met': prerequisite_met,
            'data_counts': data_counts,
            'notes': latest_record.notes,
            'processing_duration': latest_record.processing_duration_seconds
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to check registry for {snapshot_id}: {e}")
        return {
            'found': False,
            'status': 'error',
            'prerequisite_met': False,
            'data_counts': {},
            'error': str(e)
        }


def wait_with_progressive_backoff(
    check_function: callable,
    max_wait_seconds: int = 300,
    initial_interval: int = 2,
    max_interval: int = 30,
    backoff_multiplier: float = 1.5
) -> bool:
    """
    Generic wait function with progressive backoff
    
    Args:
        check_function: Function that returns True when condition is met
        max_wait_seconds: Maximum total time to wait
        initial_interval: Starting interval between checks
        max_interval: Maximum interval between checks
        backoff_multiplier: Multiplier for increasing interval
        
    Returns:
        True if condition met, False if timeout
    """
    logger = logging.getLogger('hubspot.bigquery.readiness')
    
    start_time = time.time()
    interval = initial_interval
    attempt = 0
    
    while time.time() - start_time < max_wait_seconds:
        attempt += 1
        
        try:
            if check_function():
                elapsed = time.time() - start_time
                logger.info(f"✅ Condition met after {elapsed:.2f}s (attempt {attempt})")
                return True
        except Exception as e:
            logger.debug(f"Attempt {attempt} failed: {e}")
        
        # Calculate next wait interval
        remaining_time = max_wait_seconds - (time.time() - start_time)
        actual_interval = min(interval, remaining_time)
        
        if actual_interval <= 0:
            break
        
        logger.debug(f"Waiting {actual_interval:.1f}s before next attempt...")
        time.sleep(actual_interval)
        
        # Increase interval for next time (progressive backoff)
        interval = min(interval * backoff_multiplier, max_interval)
    
    elapsed = time.time() - start_time
    logger.warning(f"⏰ Timeout after {elapsed:.2f}s ({attempt} attempts)")
    return False


def wait_for_multiple_tables_ready(
    tables: Dict[str, str],  # table_name -> expected_snapshot_id
    expected_counts: Dict[str, int] = None,  # table_name -> min_rows
    max_wait_seconds: int = 180
) -> Dict[str, bool]:
    """
    Wait for multiple tables to have data ready simultaneously
    
    Args:
        tables: Dict of table_name -> snapshot_id to check
        expected_counts: Optional dict of table_name -> minimum expected rows
        max_wait_seconds: Maximum time to wait
        
    Returns:
        Dict of table_name -> readiness status
    """
    logger = logging.getLogger('hubspot.bigquery.readiness')
    
    logger.info(f"⏳ Waiting for {len(tables)} tables to be ready")
    
    results = {}
    expected_counts = expected_counts or {}
    
    for table_name, snapshot_id in tables.items():
        min_rows = expected_counts.get(table_name, 1)
        
        logger.info(f"📊 Checking {table_name} for snapshot {snapshot_id}")
        
        ready = wait_for_data_availability(
            table_name=table_name,
            expected_snapshot_id=snapshot_id,
            min_expected_rows=min_rows,
            max_wait_seconds=max_wait_seconds
        )
        
        results[table_name] = ready
        
        if ready:
            logger.info(f"✅ {table_name} ready")
        else:
            logger.error(f"❌ {table_name} not ready")
    
    all_ready = all(results.values())
    
    if all_ready:
        logger.info("🎉 All tables ready")
    else:
        failed_tables = [name for name, ready in results.items() if not ready]
        logger.error(f"❌ Tables not ready: {failed_tables}")
    
    return results


def verify_data_consistency(
    snapshot_id: str,
    tables_to_check: Dict[str, int],  # table_name -> expected_count
    tolerance_percent: float = 5.0
) -> Dict[str, Any]:
    """
    Verify data consistency across tables for a snapshot
    
    Args:
        snapshot_id: The snapshot identifier
        tables_to_check: Dict of table_name -> expected_count
        tolerance_percent: Acceptable percentage difference
        
    Returns:
        Dict with consistency check results
    """
    logger = logging.getLogger('hubspot.bigquery.readiness')
    
    logger.info(f"🔍 Verifying data consistency for snapshot {snapshot_id}")
    
    client = bigquery.Client()
    project_id = os.getenv("BIGQUERY_PROJECT_ID") or client.project
    dataset = os.getenv("BIGQUERY_DATASET_ID")
    
    results = {
        'consistent': True,
        'tables': {},
        'issues': []
    }
    
    for table_name, expected_count in tables_to_check.items():
        try:
            query = f"""
            SELECT COUNT(*) as actual_count
            FROM `{project_id}.{dataset}.{table_name}`
            WHERE snapshot_id = @snapshot_id
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
                ]
            )
            
            result = client.query(query, job_config=job_config).result()
            actual_count = next(result).actual_count
            
            # Calculate percentage difference
            if expected_count > 0:
                diff_percent = abs(actual_count - expected_count) / expected_count * 100
            else:
                diff_percent = 0 if actual_count == 0 else 100
            
            table_result = {
                'expected': expected_count,
                'actual': actual_count,
                'difference_percent': diff_percent,
                'within_tolerance': diff_percent <= tolerance_percent
            }
            
            results['tables'][table_name] = table_result
            
            if not table_result['within_tolerance']:
                results['consistent'] = False
                issue = f"{table_name}: expected {expected_count}, got {actual_count} ({diff_percent:.1f}% difference)"
                results['issues'].append(issue)
                logger.warning(f"⚠️ Consistency issue: {issue}")
            else:
                logger.info(f"✅ {table_name}: {actual_count} rows (within tolerance)")
                
        except Exception as e:
            results['consistent'] = False
            error_msg = f"{table_name}: verification failed - {e}"
            results['issues'].append(error_msg)
            logger.error(f"❌ {error_msg}")
    
    if results['consistent']:
        logger.info("🎉 Data consistency verification passed")
    else:
        logger.error(f"❌ Data consistency issues found: {len(results['issues'])} problems")
    
    return results


def get_table_readiness_status(
    tables: list,
    snapshot_id: str = None,
    dataset: str = None
) -> Dict[str, Dict[str, Any]]:
    """
    Get comprehensive readiness status for multiple tables
    
    Args:
        tables: List of table names to check
        snapshot_id: Optional snapshot_id to check for specific data
        dataset: Dataset name (uses env var if not provided)
        
    Returns:
        Dict of table_name -> status info
    """
    logger = logging.getLogger('hubspot.bigquery.readiness')
    
    client = bigquery.Client()
    project_id = os.getenv("BIGQUERY_PROJECT_ID") or client.project
    dataset = dataset or os.getenv("BIGQUERY_DATASET_ID")
    
    status = {}
    
    for table_name in tables:
        full_table = f"{project_id}.{dataset}.{table_name}"
        
        try:
            # Check if table exists
            table = client.get_table(full_table)
            
            table_status = {
                'exists': True,
                'created': table.created.isoformat() if table.created else None,
                'num_rows': table.num_rows,
                'num_bytes': table.num_bytes,
                'streaming_buffer': check_streaming_buffer_status(table_name, dataset)
            }
            
            # If snapshot_id provided, check for specific data
            if snapshot_id:
                try:
                    query = f"""
                    SELECT 
                        COUNT(*) as snapshot_rows,
                        MIN(snapshot_id) as min_snapshot_id,
                        MAX(snapshot_id) as max_snapshot_id
                    FROM `{full_table}`
                    WHERE snapshot_id = @snapshot_id
                    """
                    
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
                        ]
                    )
                    
                    result = client.query(query, job_config=job_config).result()
                    row = next(result)
                    
                    table_status['snapshot_data'] = {
                        'snapshot_id': snapshot_id,
                        'rows': row.snapshot_rows,
                        'data_available': row.snapshot_rows > 0
                    }
                    
                except Exception as e:
                    table_status['snapshot_data'] = {
                        'snapshot_id': snapshot_id,
                        'error': str(e)
                    }
            
            # Overall readiness assessment
            table_status['ready'] = (
                table_status['exists'] and
                not table_status['streaming_buffer'].get('has_streaming_buffer', False) and
                (not snapshot_id or table_status.get('snapshot_data', {}).get('data_available', False))
            )
            
        except NotFound:
            table_status = {
                'exists': False,
                'ready': False,
                'error': 'Table not found'
            }
        except Exception as e:
            table_status = {
                'exists': None,
                'ready': False,
                'error': str(e)
            }
        
        status[table_name] = table_status
        
        # Log status
        if table_status.get('ready'):
            logger.info(f"✅ {table_name}: Ready")
        else:
            logger.warning(f"⚠️ {table_name}: Not ready - {table_status.get('error', 'Unknown issue')}")
    
    return status