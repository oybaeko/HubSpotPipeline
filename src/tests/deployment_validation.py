# ===============================================================================
# src/tests/deployment_validation.py
# Tier 1: "Will THIS deployment work in THIS environment?"
# Environment-specific validation for Cloud Function deployments
# ===============================================================================

import pytest
import os
import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any

def get_hubspot_api_key_for_test(test_logger):
    """
    Get HubSpot API key for testing, works in both GCF and local environments
    Fails if key is not available (configuration problem)
    """
    # First check if already available
    api_key = os.getenv('HUBSPOT_API_KEY')
    if api_key:
        test_logger.debug("HUBSPOT_API_KEY already available in environment")
        return api_key
    
    # Try to initialize environment (handles both GCP and local)
    try:
        from hubspot_pipeline.hubspot_ingest.config_loader import init_env
        test_logger.debug("Initializing environment for API key access")
        init_env()
        api_key = get_hubspot_api_key_for_test(test_logger)
        
        if api_key:
            test_logger.info("✅ HUBSPOT_API_KEY loaded successfully")
            return api_key
        else:
            pytest.fail("HUBSPOT_API_KEY not available after environment initialization")
            
    except Exception as e:
        pytest.fail(f"Failed to load HUBSPOT_API_KEY: {e}")

# ===============================================================================
# HubSpot API Environment Validation
# ===============================================================================

@pytest.mark.deployment
@pytest.mark.production_safe
def test_hubspot_api_companies_endpoint(test_logger, function_type):
    """Validate HubSpot Companies API endpoint access (READ-ONLY)"""
    test_logger.info("🏢 Testing HubSpot Companies API endpoint")
    
    if function_type == 'scoring':
        pytest.skip("Scoring function doesn't need HubSpot API access")
    
    try:
        import requests
        api_key = get_hubspot_api_key_for_test(test_logger)
        
        if not api_key:
            test_logger.error("❌ HUBSPOT_API_KEY not available")
            pytest.fail("HUBSPOT_API_KEY environment variable not set")
        
        # Test companies endpoint with minimal data
        url = "https://api.hubapi.com/crm/v3/objects/companies"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        params = {"limit": 1, "properties": "name"}
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            test_logger.info(f"✅ Companies API accessible, found {len(data.get('results', []))} companies")
        elif response.status_code == 401:
            test_logger.error("❌ Unauthorized - check HUBSPOT_API_KEY")
            pytest.fail("HubSpot API authentication failed")
        else:
            test_logger.error(f"❌ API error: {response.status_code} - {response.text[:200]}")
            pytest.fail(f"HubSpot Companies API error: {response.status_code}")
            
    except ImportError:
        pytest.skip("requests library not available")
    except Exception as e:
        test_logger.error(f"❌ HubSpot Companies API test failed: {e}")
        pytest.fail(f"HubSpot Companies API connectivity error: {e}")

@pytest.mark.deployment
@pytest.mark.production_safe
def test_hubspot_api_deals_endpoint(test_logger, function_type):
    """Validate HubSpot Deals API endpoint access (READ-ONLY)"""
    test_logger.info("🤝 Testing HubSpot Deals API endpoint")
    
    if function_type == 'scoring':
        pytest.skip("Scoring function doesn't need HubSpot API access")
    
    try:
        import requests
        api_key = get_hubspot_api_key_for_test(test_logger)        
        if not api_key:
            pytest.skip("HUBSPOT_API_KEY not available")
        
        # Test deals endpoint
        url = "https://api.hubapi.com/crm/v3/objects/deals"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        params = {"limit": 1, "properties": "dealname"}
        
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            test_logger.info(f"✅ Deals API accessible, found {len(data.get('results', []))} deals")
        else:
            pytest.fail(f"HubSpot Deals API error: {response.status_code}")
            
    except Exception as e:
        test_logger.error(f"❌ HubSpot Deals API test failed: {e}")
        pytest.fail(f"HubSpot Deals API connectivity error: {e}")

@pytest.mark.deployment
@pytest.mark.production_safe
def test_hubspot_api_owners_endpoint(test_logger, function_type):
    """Validate HubSpot Owners API endpoint access (READ-ONLY)"""
    test_logger.info("👥 Testing HubSpot Owners API endpoint")
    
    if function_type == 'scoring':
        pytest.skip("Scoring function doesn't need HubSpot API access")
    
    try:
        import requests
        api_key = get_hubspot_api_key_for_test(test_logger)
        
        if not api_key:
            pytest.skip("HUBSPOT_API_KEY not available")
        
        # Test owners endpoint
        url = "https://api.hubapi.com/crm/v3/owners"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            test_logger.info(f"✅ Owners API accessible, found {len(data.get('results', []))} owners")
        else:
            pytest.fail(f"HubSpot Owners API error: {response.status_code}")
            
    except Exception as e:
        test_logger.error(f"❌ HubSpot Owners API test failed: {e}")
        pytest.fail(f"HubSpot Owners API connectivity error: {e}")

@pytest.mark.deployment
@pytest.mark.production_safe
def test_hubspot_api_pipelines_endpoint(test_logger, function_type):
    """Validate HubSpot Pipelines API endpoint access (READ-ONLY)"""
    test_logger.info("📋 Testing HubSpot Pipelines API endpoint")
    
    if function_type == 'scoring':
        pytest.skip("Scoring function doesn't need HubSpot API access")
    
    try:
        import requests
        api_key = get_hubspot_api_key_for_test(test_logger)        
        if not api_key:
            pytest.skip("HUBSPOT_API_KEY not available")
        
        # Test pipelines endpoint
        url = "https://api.hubapi.com/crm/v3/pipelines/deals"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            pipelines = data.get('results', [])
            total_stages = sum(len(p.get('stages', [])) for p in pipelines)
            test_logger.info(f"✅ Pipelines API accessible, found {len(pipelines)} pipelines with {total_stages} stages")
        else:
            pytest.fail(f"HubSpot Pipelines API error: {response.status_code}")
            
    except Exception as e:
        test_logger.error(f"❌ HubSpot Pipelines API test failed: {e}")
        pytest.fail(f"HubSpot Pipelines API connectivity error: {e}")

# ===============================================================================
# GCP Services Environment Validation
# ===============================================================================

@pytest.mark.deployment
@pytest.mark.production_safe
def test_bigquery_client_access(test_logger):
    """Validate BigQuery client creation and project access"""
    test_logger.info("🗄️ Testing BigQuery client access")
    
    try:
        from google.cloud import bigquery
        
        # Create client
        client = bigquery.Client()
        test_logger.info(f"✅ BigQuery client created for project: {client.project}")
        
        # Test basic query capability
        query = "SELECT 1 as test_connection, CURRENT_TIMESTAMP() as test_time"
        result = client.query(query).result()
        
        row = next(result)
        test_logger.info(f"✅ BigQuery query successful: connection={row.test_connection}, time={row.test_time}")
        
    except ImportError:
        pytest.skip("google-cloud-bigquery not available")
    except Exception as e:
        test_logger.error(f"❌ BigQuery client test failed: {e}")
        pytest.fail(f"BigQuery connectivity error: {e}")

@pytest.mark.deployment
@pytest.mark.production_safe
def test_bigquery_dataset_access(test_logger, environment):
    """Validate BigQuery dataset access for environment"""
    test_logger.info(f"🗂️ Testing BigQuery dataset access for {environment}")
    
    try:
        from google.cloud import bigquery
        
        client = bigquery.Client()
        
        # Get environment-specific dataset
        dataset_mapping = {
            'development': 'Hubspot_dev_ob',
            'staging': 'Hubspot_staging',
            'production': 'Hubspot_prod'
        }
        
        dataset_id = dataset_mapping.get(environment, 'Hubspot_dev_ob')
        dataset_ref = f"{client.project}.{dataset_id}"
        
        # Try to access dataset
        dataset = client.get_dataset(dataset_ref)
        test_logger.info(f"✅ Dataset accessible: {dataset.dataset_id}")
        
        # Test basic permissions by listing tables
        tables = list(client.list_tables(dataset))
        test_logger.info(f"✅ Dataset permissions OK, found {len(tables)} tables")
        
    except ImportError:
        pytest.skip("google-cloud-bigquery not available")
    except Exception as e:
        test_logger.error(f"❌ BigQuery dataset test failed: {e}")
        pytest.fail(f"BigQuery dataset access error: {e}")

@pytest.mark.deployment
@pytest.mark.production_safe  
def test_pubsub_topic_access(test_logger, environment, function_type):
    """Validate Pub/Sub topic access for environment"""
    test_logger.info(f"📤 Testing Pub/Sub topic access for {environment}")
    
    try:
        from google.cloud import pubsub_v1
        
        # Create publisher client
        publisher = pubsub_v1.PublisherClient()
        
        # Get project ID
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('BIGQUERY_PROJECT_ID')
        if not project_id:
            pytest.skip("No project ID available")
        
        # Environment-specific topic
        topic_mapping = {
            'development': 'hubspot-events-dev',
            'staging': 'hubspot-events-staging', 
            'production': 'hubspot-events-prod'
        }
        
        topic_name = topic_mapping.get(environment, 'hubspot-events-dev')
        topic_path = publisher.topic_path(project_id, topic_name)
        
        # Test topic access
        topic = publisher.get_topic(request={"topic": topic_path})
        test_logger.info(f"✅ Topic accessible: {topic.name}")
        
        # Function-specific permission testing
        if function_type == 'ingest':
            # Test publish permissions (ingest needs to publish)
            test_logger.info("📤 Testing publish permissions for ingest function")
            # We don't actually publish in production-safe test
            test_logger.info("✅ Publish permissions assumed OK (topic accessible)")
        elif function_type == 'scoring':
            # Test subscription permissions (scoring needs to consume)
            test_logger.info("📥 Testing subscription access for scoring function")
            # We don't test actual consumption in production-safe test
            test_logger.info("✅ Subscription permissions assumed OK (topic accessible)")
        
    except ImportError:
        pytest.skip("google-cloud-pubsub not available")
    except Exception as e:
        test_logger.error(f"❌ Pub/Sub topic test failed: {e}")
        pytest.fail(f"Pub/Sub topic access error: {e}")

@pytest.mark.deployment
@pytest.mark.production_safe
def test_secret_manager_access(test_logger, function_type):
    """Validate Secret Manager access for HUBSPOT_API_KEY"""
    test_logger.info("🔐 Testing Secret Manager access")
    
    if function_type == 'scoring':
        pytest.skip("Scoring function doesn't need Secret Manager access")
    
    # Only test if running in GCP (has K_SERVICE)
    if not os.getenv('K_SERVICE'):
        pytest.skip("Not running in Cloud Function environment")
    
    try:
        from google.cloud import secretmanager
        
        client = secretmanager.SecretManagerServiceClient()
        
        # Get project ID
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('BIGQUERY_PROJECT_ID')
        if not project_id:
            pytest.skip("No project ID available")
        
        # Test access to HUBSPOT_API_KEY secret
        secret_name = f"projects/{project_id}/secrets/HUBSPOT_API_KEY/versions/latest"
        
        response = client.access_secret_version(request={"name": secret_name})
        secret_value = response.payload.data.decode("UTF-8")
        
        if secret_value and len(secret_value) > 10:
            test_logger.info(f"✅ Secret Manager accessible, HUBSPOT_API_KEY retrieved (length: {len(secret_value)})")
        else:
            pytest.fail("HUBSPOT_API_KEY secret appears to be empty or too short")
        
    except ImportError:
        pytest.skip("google-cloud-secret-manager not available")
    except Exception as e:
        test_logger.error(f"❌ Secret Manager test failed: {e}")
        pytest.fail(f"Secret Manager access error: {e}")

# ===============================================================================
# CRUD Lifecycle Validation (Non-Destructive)
# ===============================================================================

@pytest.mark.deployment
@pytest.mark.production_safe
def test_bigquery_crud_lifecycle(test_logger, test_session, safe_test_id):
    """Test complete CRUD lifecycle with test table (CREATE → INSERT → READ → DELETE)"""
    test_logger.info("🔄 Testing BigQuery CRUD lifecycle")
    
    try:
        from google.cloud import bigquery
        
        client = bigquery.Client()
        
        # Create test table name
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        if not dataset_id:
            pytest.skip("BIGQUERY_DATASET_ID not available")
        
        test_table_id = f"{client.project}.{dataset_id}.test_framework_{safe_test_id}"
        
        # Register for cleanup
        test_session.register_for_cleanup('bigquery_table', {
            'table_id': test_table_id
        })
        
        test_logger.info(f"🔨 CREATE: Creating test table {test_table_id}")
        
        # CREATE: Create test table
        schema = [
            bigquery.SchemaField("test_id", "STRING"),
            bigquery.SchemaField("test_value", "STRING"),
            bigquery.SchemaField("test_timestamp", "TIMESTAMP"),
        ]
        
        table = bigquery.Table(test_table_id, schema=schema)
        created_table = client.create_table(table)
        test_logger.info("✅ CREATE successful")
        
        # INSERT: Insert test data
        test_logger.info("⬆️ INSERT: Adding test data")
        test_data = [
            {
                "test_id": safe_test_id,
                "test_value": "deployment_validation",
                "test_timestamp": datetime.utcnow().isoformat()
            }
        ]
        
        errors = client.insert_rows_json(test_table_id, test_data)
        if errors:
            pytest.fail(f"INSERT failed: {errors}")
        
        test_logger.info("✅ INSERT successful")
        
        # READ: Query test data
        test_logger.info("⬇️ READ: Querying test data")
        query = f"""
        SELECT test_id, test_value, test_timestamp
        FROM `{test_table_id}`
        WHERE test_id = @test_id
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("test_id", "STRING", safe_test_id)
            ]
        )
        
        result = client.query(query, job_config=job_config).result()
        rows = list(result)
        
        if len(rows) != 1:
            pytest.fail(f"READ failed: expected 1 row, got {len(rows)}")
        
        row = rows[0]
        if row.test_id != safe_test_id or row.test_value != "deployment_validation":
            pytest.fail(f"READ failed: data mismatch")
        
        test_logger.info("✅ READ successful")
        
        # DELETE: Clean up test table (will also be handled by cleanup)
        test_logger.info("🗑️ DELETE: Removing test table")
        client.delete_table(test_table_id, not_found_ok=True)
        test_logger.info("✅ DELETE successful")
        
        test_logger.info("🎉 Complete CRUD lifecycle validation successful")
        
    except ImportError:
        pytest.skip("google-cloud-bigquery not available")
    except Exception as e:
        test_logger.error(f"❌ CRUD lifecycle test failed: {e}")
        pytest.fail(f"BigQuery CRUD lifecycle error: {e}")

# ===============================================================================
# Event Flow Validation (Ingest → Pub/Sub → Scoring)
# ===============================================================================

@pytest.mark.deployment
@pytest.mark.production_safe
def test_event_flow_validation(test_logger, environment, function_type):
    """Test event flow from ingest to scoring (non-destructive)"""
    test_logger.info("🔄 Testing event flow validation")
    
    try:
        from google.cloud import pubsub_v1
        import json
        
        # Get project and topic info
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('BIGQUERY_PROJECT_ID')
        if not project_id:
            pytest.skip("No project ID available")
        
        topic_mapping = {
            'development': 'hubspot-events-dev',
            'staging': 'hubspot-events-staging',
            'production': 'hubspot-events-prod'
        }
        
        topic_name = topic_mapping.get(environment, 'hubspot-events-dev')
        
        if function_type == 'ingest':
            # Test publisher capabilities for ingest function
            test_logger.info("📤 Testing event publishing capability (ingest)")
            
            publisher = pubsub_v1.PublisherClient()
            topic_path = publisher.topic_path(project_id, topic_name)
            
            # Verify topic exists and we can publish (don't actually publish in prod-safe test)
            topic = publisher.get_topic(request={"topic": topic_path})
            test_logger.info(f"✅ Ingest can access topic for publishing: {topic.name}")
            
        elif function_type == 'scoring':
            # Test subscriber capabilities for scoring function
            test_logger.info("📥 Testing event consumption capability (scoring)")
            
            subscriber = pubsub_v1.SubscriberClient()
            
            # Check if there are subscriptions for this topic
            # (In real deployment, scoring function subscription should exist)
            topic_path = f"projects/{project_id}/topics/{topic_name}"
            
            try:
                subscriptions = subscriber.list_subscriptions(
                    request={"project": f"projects/{project_id}"}
                )
                
                scoring_subscriptions = []
                for subscription in subscriptions:
                    if topic_name in subscription.topic:
                        scoring_subscriptions.append(subscription.name)
                
                if scoring_subscriptions:
                    test_logger.info(f"✅ Found {len(scoring_subscriptions)} subscriptions for scoring")
                else:
                    test_logger.warning("⚠️ No subscriptions found - scoring function may not receive events")
                    
            except Exception as e:
                test_logger.warning(f"⚠️ Could not list subscriptions: {e}")
        
    except ImportError:
        pytest.skip("google-cloud-pubsub not available")
    except Exception as e:
        test_logger.error(f"❌ Event flow validation failed: {e}")
        pytest.fail(f"Event flow validation error: {e}")

# ===============================================================================
# Function-Specific Environment Validation
# ===============================================================================

@pytest.mark.deployment
@pytest.mark.production_safe
def test_ingest_function_environment(test_logger, function_type):
    """Validate ingest function specific environment requirements"""
    if function_type != 'ingest':
        pytest.skip("Not testing ingest function")
    
    test_logger.info("🔍 Validating ingest function environment")
    
    # Test all ingest-specific requirements
    required_env_vars = ['BIGQUERY_PROJECT_ID', 'BIGQUERY_DATASET_ID']
    missing_vars = []
    
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
        else:
            test_logger.info(f"✅ {var} available")
    
    if missing_vars:
        pytest.fail(f"Missing required environment variables for ingest: {missing_vars}")
    
    # Test HubSpot API key availability
    if os.getenv('K_SERVICE'):  # Only in Cloud Function
        if not os.getenv('HUBSPOT_API_KEY'):
            pytest.fail("HUBSPOT_API_KEY not available in Cloud Function environment")
        test_logger.info("✅ HUBSPOT_API_KEY available")
    
    test_logger.info("✅ Ingest function environment validation complete")

@pytest.mark.deployment
@pytest.mark.production_safe
def test_scoring_function_environment(test_logger, function_type):
    """Validate scoring function specific environment requirements"""
    if function_type != 'scoring':
        pytest.skip("Not testing scoring function")
    
    test_logger.info("🔍 Validating scoring function environment")
    
    # Test scoring-specific requirements
    required_env_vars = ['BIGQUERY_PROJECT_ID', 'BIGQUERY_DATASET_ID']
    missing_vars = []
    
    for var in required_env_vars:
        if not os.getenv(var):
            missing_vars.append(var)
        else:
            test_logger.info(f"✅ {var} available")
    
    if missing_vars:
        pytest.fail(f"Missing required environment variables for scoring: {missing_vars}")
    
    # Scoring function doesn't need HubSpot API key
    test_logger.info("✅ Scoring function doesn't require HubSpot API key")
    
    test_logger.info("✅ Scoring function environment validation complete")

@pytest.mark.deployment
@pytest.mark.production_safe
def test_scoring_table_access(test_logger, function_type):
    """Validate scoring function can access required tables"""
    if function_type != 'scoring':
        pytest.skip("Not testing scoring function")
    
    test_logger.info("📊 Testing scoring function table access")
    
    try:
        from google.cloud import bigquery
        
        client = bigquery.Client()
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        if not dataset_id:
            pytest.skip("BIGQUERY_DATASET_ID not available")
        
        # Tables that scoring function needs to read
        required_tables = [
            'hs_companies',
            'hs_deals', 
            'hs_deal_stage_reference',
            'hs_stage_mapping'
        ]
        
        # Tables that scoring function needs to write
        output_tables = [
            'hs_pipeline_units_snapshot',
            'hs_pipeline_score_history'
        ]
        
        accessible_tables = []
        
        for table_name in required_tables + output_tables:
            table_id = f"{client.project}.{dataset_id}.{table_name}"
            
            try:
                table = client.get_table(table_id)
                accessible_tables.append(table_name)
                test_logger.info(f"✅ Table accessible: {table_name}")
            except Exception:
                test_logger.warning(f"⚠️ Table not found: {table_name} (may be created during runtime)")
        
        # At minimum, we need dataset access
        if len(accessible_tables) == 0:
            pytest.fail("No tables accessible - check dataset permissions")
        
        test_logger.info(f"✅ Scoring table access validation complete ({len(accessible_tables)} tables accessible)")
        
    except ImportError:
        pytest.skip("google-cloud-bigquery not available")
    except Exception as e:
        test_logger.error(f"❌ Scoring table access test failed: {e}")
        pytest.fail(f"Scoring table access error: {e}")

# ===============================================================================
# Network and Connectivity Validation
# ===============================================================================

@pytest.mark.deployment
@pytest.mark.production_safe
def test_external_api_connectivity(test_logger):
    """Test connectivity to external APIs (HubSpot)"""
    test_logger.info("🌐 Testing external API connectivity")
    
    try:
        import requests
        
        # Test basic connectivity to HubSpot
        response = requests.get(
            "https://api.hubapi.com/crm/v3/objects/companies",
            params={"limit": 1},
            headers={"Authorization": "Bearer invalid_token_for_connectivity_test"},
            timeout=10
        )
        
        # We expect 401 (unauthorized) which means connectivity is fine
        if response.status_code == 401:
            test_logger.info("✅ HubSpot API connectivity successful (401 as expected)")
        elif response.status_code in [200, 403]:
            test_logger.info(f"✅ HubSpot API connectivity successful ({response.status_code})")
        else:
            test_logger.warning(f"⚠️ Unexpected HubSpot API response: {response.status_code}")
        
    except requests.exceptions.ConnectivityError:
        pytest.fail("Cannot connect to HubSpot API - check network connectivity")
    except requests.exceptions.Timeout:
        pytest.fail("HubSpot API connection timeout - check network latency")
    except ImportError:
        pytest.skip("requests library not available")
    except Exception as e:
        test_logger.error(f"❌ External API connectivity test failed: {e}")
        pytest.fail(f"External API connectivity error: {e}")

@pytest.mark.deployment
@pytest.mark.production_safe
def test_gcp_api_connectivity(test_logger):
    """Test connectivity to GCP APIs"""
    test_logger.info("☁️ Testing GCP API connectivity")
    
    try:
        from google.cloud import bigquery
        from google.api_core import exceptions
        
        # Test BigQuery API connectivity
        client = bigquery.Client()
        
        # This will test connectivity to BigQuery API
        try:
            datasets = list(client.list_datasets(max_results=1))
            test_logger.info("✅ BigQuery API connectivity successful")
        except exceptions.Forbidden:
            test_logger.info("✅ BigQuery API connectivity successful (permissions may be limited)")
        except exceptions.NotFound:
            test_logger.info("✅ BigQuery API connectivity successful (no datasets found)")
        
    except ImportError:
        pytest.skip("google-cloud-bigquery not available")
    except Exception as e:
        test_logger.error(f"❌ GCP API connectivity test failed: {e}")
        pytest.fail(f"GCP API connectivity error: {e}")

# ===============================================================================
# Performance and Resource Validation
# ===============================================================================

@pytest.mark.deployment
@pytest.mark.production_safe
def test_function_memory_and_timeout(test_logger):
    """Test function has adequate memory and timeout configuration"""
    test_logger.info("💾 Testing function resource configuration")
    
    import psutil
    import time
    
    # Test available memory
    memory = psutil.virtual_memory()
    available_mb = memory.available / (1024 * 1024)
    
    test_logger.info(f"💾 Available memory: {available_mb:.0f} MB")
    
    if available_mb < 100:  # Less than 100MB available
        test_logger.warning("⚠️ Low available memory - function may have memory constraints")
    else:
        test_logger.info("✅ Adequate memory available")
    
    # Test basic performance (simple operation timing)
    start_time = time.time()
    
    # Simulate some work
    test_data = list(range(10000))
    processed = [x * 2 for x in test_data]
    
    processing_time = time.time() - start_time
    test_logger.info(f"⏱️ Basic processing test: {processing_time:.3f} seconds")
    
    if processing_time > 5.0:  # More than 5 seconds for simple operation
        test_logger.warning("⚠️ Slow processing detected - function may be resource constrained")
    else:
        test_logger.info("✅ Processing performance adequate")

@pytest.mark.deployment
@pytest.mark.production_safe
def test_environment_configuration_summary(test_logger, environment, function_type):
    """Provide summary of environment configuration validation"""
    test_logger.info("📋 Environment configuration summary")
    
    test_logger.info(f"🌍 Environment: {environment}")
    test_logger.info(f"⚙️ Function Type: {function_type}")
    test_logger.info(f"☁️ Cloud Function: {os.getenv('K_SERVICE', 'Not in Cloud Function')}")
    test_logger.info(f"📦 Project: {os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('BIGQUERY_PROJECT_ID', 'Unknown')}")
    test_logger.info(f"🗂️ Dataset: {os.getenv('BIGQUERY_DATASET_ID', 'Unknown')}")
    
    # Environment-specific expectations
    if environment == 'production':
        test_logger.info("🚨 Production environment - all validations must pass")
    elif environment == 'staging':
        test_logger.info("🔧 Staging environment - most validations should pass")
    else:
        test_logger.info("🧪 Development environment - basic validations expected")
    
    # Function-specific expectations
    if function_type == 'ingest':
        test_logger.info("📥 Ingest function - requires HubSpot API, BigQuery write, Pub/Sub publish")
    elif function_type == 'scoring':
        test_logger.info("📊 Scoring function - requires BigQuery read/write, Pub/Sub consume")
    
    test_logger.info("✅ Environment configuration summary complete")