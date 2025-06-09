# ===============================================================================
# src/tests/integration_tests.py
# End-to-End Pipeline Integration Tests
# ===============================================================================

import pytest
import os
import logging
from datetime import datetime
from typing import Dict, Any, Optional

# ===============================================================================
# End-to-End Pipeline Integration Tests
# ===============================================================================

@pytest.mark.deployment
@pytest.mark.production_safe
def test_end_to_end_pipeline_with_limit(test_logger, environment, function_type, safe_test_id):
    """
    End-to-end pipeline test with configurable record limit
    
    Tests the complete HubSpot → BigQuery → Pub/Sub → Scoring pipeline
    - Only runs in dev/staging environments (skips prod)
    - Uses actual production tables (no cleanup)
    - Configurable record limit for different test scenarios
    - Returns detailed results for inspection
    """
    test_logger.info("🔄 Starting end-to-end pipeline integration test")
    
    # Environment safety check - only run in dev/staging
    if environment == 'production':
        test_logger.info("⚠️ Skipping end-to-end test in production environment")
        pytest.skip("End-to-end integration tests not run in production for safety")
    
    # Function type check - only meaningful for ingest function
    if function_type != 'ingest':
        test_logger.info("ℹ️ End-to-end test only applicable to ingest function")
        pytest.skip("End-to-end pipeline test only runs for ingest function")
    
    # Get record limit from environment or use default
    record_limit = int(os.getenv('E2E_RECORD_LIMIT', '5'))
    test_logger.info(f"🎯 Testing with record limit: {record_limit}")
    
    try:
        # Initialize the pipeline environment
        test_logger.info("🔧 Initializing pipeline environment")
        from hubspot_pipeline.hubspot_ingest.config_loader import init_env
        init_env()
        test_logger.info("✅ Pipeline environment initialized successfully")
        
        # Import and run the main ingest pipeline
        test_logger.info("📥 Importing HubSpot ingest pipeline")
        from hubspot_pipeline.hubspot_ingest.main import main as ingest_main
        test_logger.info("✅ Pipeline import successful")
        
        # Prepare event data for the pipeline
        event_data = {
            'limit': record_limit,
            'dry_run': False,  # Real run - data will be written
            'log_level': 'INFO',
            'trigger_source': f'integration_test_{safe_test_id}',
            'test_mode': True  # Mark as test for identification
        }
        
        test_logger.info(f"🚀 Executing pipeline with event: {event_data}")
        
        # Execute the pipeline
        start_time = datetime.utcnow()
        result = ingest_main(event=event_data)
        end_time = datetime.utcnow()
        
        execution_time = (end_time - start_time).total_seconds()
        test_logger.info(f"⏱️ Pipeline execution completed in {execution_time:.2f} seconds")
        test_logger.info(f"📋 Pipeline result type: {type(result)}")
        test_logger.info(f"📋 Pipeline result: {result}")
        
        # Validate pipeline result - handle both dict and tuple formats
        if isinstance(result, tuple) and len(result) == 2:
            # Handle HTTP response format (response_data, status_code)
            result_data, status_code = result
            test_logger.info(f"📋 Received tuple result: status_code={status_code}, data_type={type(result_data)}")
            
            if status_code != 200:
                test_logger.error(f"❌ Pipeline failed with HTTP status {status_code}")
                test_logger.error(f"❌ Error response: {result_data}")
                pytest.fail(f"Pipeline failed with status {status_code}: {result_data}")
            
            # Use the response data as the result
            result = result_data
            test_logger.info("✅ Using response data from tuple result")
            
        elif not isinstance(result, dict):
            test_logger.error(f"❌ Pipeline returned unexpected format: {type(result)}")
            test_logger.error(f"❌ Result content: {result}")
            pytest.fail(f"Pipeline returned unexpected format: {type(result)} - {result}")
        
        # Now result should be a dict
        if not result:
            test_logger.error("❌ Pipeline returned empty result")
            pytest.fail("Pipeline returned empty result")
        
        test_logger.info(f"📊 Pipeline result keys: {list(result.keys())}")
        
        if result.get('status') != 'success':
            error_msg = result.get('error', 'Unknown error')
            test_logger.error(f"❌ Pipeline execution failed: {error_msg}")
            pytest.fail(f"Pipeline execution failed: {error_msg}")
        
        # Extract key metrics from result
        snapshot_id = result.get('snapshot_id')
        total_records = result.get('total_records', 0)
        results_breakdown = result.get('results', {})
        
        test_logger.info(f"📊 Pipeline Results:")
        test_logger.info(f"  Snapshot ID: {snapshot_id}")
        test_logger.info(f"  Total Records: {total_records}")
        test_logger.info(f"  Execution Time: {execution_time:.2f}s")
        
        # Log data breakdown
        if results_breakdown:
            test_logger.info("📋 Data Breakdown:")
            for table, count in results_breakdown.items():
                test_logger.info(f"  {table}: {count} records")
        
        # Validate minimum expectations
        if total_records == 0:
            test_logger.warning("⚠️ Pipeline completed but no records were processed")
            # Don't fail the test - this might be expected if no new data
        
        if total_records > record_limit:
            test_logger.warning(f"⚠️ More records processed ({total_records}) than limit ({record_limit})")
        
        # Verify data was written to BigQuery
        test_logger.info("🗄️ Verifying data was written to BigQuery")
        verify_bigquery_data_written(test_logger, snapshot_id, environment)
        
        # Check if Pub/Sub event was published (if applicable)
        test_logger.info("📤 Verifying Pub/Sub event publication")
        verify_pubsub_event_published(test_logger, snapshot_id, environment)
        
        # Success - log final summary
        test_logger.info("✅ End-to-end pipeline test completed successfully")
        test_logger.info(f"📈 Summary: {total_records} records processed in {execution_time:.2f}s")
        test_logger.info(f"💾 Data persisted in {environment} environment tables")
        test_logger.info(f"🔍 Snapshot ID for inspection: {snapshot_id}")
        
        # Return test metadata for potential script usage
        return {
            'snapshot_id': snapshot_id,
            'total_records': total_records,
            'execution_time': execution_time,
            'results_breakdown': results_breakdown,
            'environment': environment,
            'record_limit': record_limit
        }
        
    except ImportError as e:
        test_logger.error(f"❌ Failed to import pipeline modules: {e}")
        test_logger.error(f"❌ Import error details: {type(e).__name__}: {str(e)}")
        import traceback
        test_logger.error(f"❌ Full traceback: {traceback.format_exc()}")
        pytest.fail(f"Pipeline import error: {e}")
        
    except Exception as e:
        test_logger.error(f"❌ End-to-end pipeline test failed: {e}")
        test_logger.error(f"❌ Exception type: {type(e).__name__}")
        test_logger.error(f"❌ Exception details: {str(e)}")
        import traceback
        test_logger.error(f"❌ Full traceback: {traceback.format_exc()}")
        pytest.fail(f"End-to-end pipeline error: {e}")

def verify_bigquery_data_written(test_logger, snapshot_id: str, environment: str):
    """Verify that data was actually written to BigQuery tables"""
    try:
        from google.cloud import bigquery
        
        client = bigquery.Client()
        dataset_mapping = {
            'development': 'Hubspot_dev_ob',
            'staging': 'Hubspot_staging',
            'production': 'Hubspot_prod'
        }
        
        dataset_id = dataset_mapping.get(environment, 'Hubspot_dev_ob')
        
        # Check snapshot registry for our snapshot
        registry_table = f"{client.project}.{dataset_id}.hs_snapshot_registry"
        
        query = f"""
        SELECT snapshot_id, total_records, status
        FROM `{registry_table}`
        WHERE snapshot_id = @snapshot_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
            ]
        )
        
        result = client.query(query, job_config=job_config).result()
        rows = list(result)
        
        if len(rows) == 0:
            pytest.fail(f"Snapshot {snapshot_id} not found in snapshot registry")
        
        row = rows[0]
        test_logger.info(f"✅ BigQuery verification: Found snapshot {snapshot_id}")
        test_logger.info(f"  Records: {row.total_records}, Status: {row.status}")
        
        # Optionally verify data in main tables
        tables_to_check = ['hs_companies', 'hs_deals', 'hs_owners']
        for table in tables_to_check:
            table_id = f"{client.project}.{dataset_id}.{table}"
            try:
                count_query = f"SELECT COUNT(*) as count FROM `{table_id}` WHERE snapshot_id = @snapshot_id"
                count_result = client.query(count_query, job_config=job_config).result()
                count = next(count_result).count
                test_logger.info(f"  {table}: {count} records with snapshot_id")
            except Exception as e:
                test_logger.warning(f"  Could not verify {table}: {e}")
        
    except ImportError:
        test_logger.warning("⚠️ BigQuery client not available - skipping data verification")
    except Exception as e:
        test_logger.error(f"❌ BigQuery verification failed: {e}")
        pytest.fail(f"Could not verify BigQuery data: {e}")

def verify_pubsub_event_published(test_logger, snapshot_id: str, environment: str):
    """Verify that Pub/Sub event was published to trigger scoring"""
    try:
        # Note: This is a best-effort check since Pub/Sub messages are transient
        # In a real system, you might check Cloud Logging or scoring function logs
        
        topic_mapping = {
            'development': 'hubspot-events-dev',
            'staging': 'hubspot-events-staging',
            'production': 'hubspot-events-prod'
        }
        
        topic_name = topic_mapping.get(environment, 'hubspot-events-dev')
        test_logger.info(f"📤 Expected Pub/Sub topic: {topic_name}")
        
        # Check if topic exists (basic validation)
        from google.cloud import pubsub_v1
        
        publisher = pubsub_v1.PublisherClient()
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('BIGQUERY_PROJECT_ID')
        
        if project_id:
            topic_path = publisher.topic_path(project_id, topic_name)
            try:
                topic = publisher.get_topic(request={"topic": topic_path})
                test_logger.info(f"✅ Pub/Sub topic accessible: {topic.name}")
                test_logger.info("📨 Event should have been published (cannot verify transient message)")
            except Exception as e:
                test_logger.warning(f"⚠️ Could not verify Pub/Sub topic: {e}")
        else:
            test_logger.warning("⚠️ No project ID available for Pub/Sub verification")
        
    except ImportError:
        test_logger.warning("⚠️ Pub/Sub client not available - skipping event verification")
    except Exception as e:
        test_logger.warning(f"⚠️ Pub/Sub verification failed: {e}")

# ===============================================================================
# Additional Integration Test Placeholders
# ===============================================================================

@pytest.mark.deployment
@pytest.mark.production_safe
def test_pipeline_error_handling(test_logger, environment, function_type):
    """
    Test pipeline error handling with invalid/missing data
    
    TODO: Implement tests for:
    - Invalid API credentials
    - Network connectivity issues
    - BigQuery permission errors
    - Malformed data handling
    """
    if environment == 'production' or function_type != 'ingest':
        pytest.skip("Error handling test only for non-production ingest")
    
    test_logger.info("🚧 Pipeline error handling test - placeholder for future implementation")
    pytest.skip("Error handling integration test not yet implemented")

@pytest.mark.deployment 
@pytest.mark.production_safe
def test_incremental_sync_logic(test_logger, environment, function_type):
    """
    Test incremental sync functionality
    
    TODO: Implement tests for:
    - Delta detection
    - Timestamp-based filtering
    - Deduplication logic
    - Update vs insert behavior
    """
    if environment == 'production' or function_type != 'ingest':
        pytest.skip("Incremental sync test only for non-production ingest")
    
    test_logger.info("🚧 Incremental sync test - placeholder for future implementation")
    pytest.skip("Incremental sync integration test not yet implemented")

@pytest.mark.deployment
@pytest.mark.production_safe  
def test_scoring_function_integration(test_logger, environment, function_type):
    """
    Test scoring function pipeline integration
    
    TODO: Implement tests for:
    - Pub/Sub message consumption
    - Scoring algorithm execution
    - Results writing to BigQuery
    - End-to-end scoring pipeline
    """
    if environment == 'production' or function_type != 'scoring':
        pytest.skip("Scoring integration test only for non-production scoring")
    
    test_logger.info("🚧 Scoring function integration test - placeholder for future implementation")  
    pytest.skip("Scoring integration test not yet implemented")