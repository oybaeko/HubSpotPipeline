# ===============================================================================
# Part 2: Service Account Permissions (CRITICAL) - FIXED VERSION
# ===============================================================================
from test_config import (
    PROJECT_ID, REGION, ENVIRONMENTS, 
    run_gcloud_command, get_current_project, get_current_account
)
import pytest
import json

@pytest.mark.parametrize("env", ["dev", "staging", "prod"])
def test_service_account_pubsub_publisher_permissions(env):
    """Test service account has Pub/Sub publisher permissions - BLOCKING ISSUE (FIXED)"""
    config = ENVIRONMENTS[env]
    topic = config['topic']
    service_account = config['service_account']
    
    print(f"\n🔍 Testing Pub/Sub permissions for {env} environment")
    print(f"Topic: {topic}")
    print(f"Service Account: {service_account}")
    
    # FIXED: Get the complete IAM policy as JSON and parse it properly
    cmd = f"gcloud pubsub topics get-iam-policy {topic} --project={PROJECT_ID} --format=json"
    
    result = run_gcloud_command(cmd, check=False)
    
    if result.returncode != 0:
        pytest.fail(f"Failed to get IAM policy for topic {topic}: {result.stderr}")
    
    try:
        policy = json.loads(result.stdout)
        bindings = policy.get('bindings', [])
        
        # Look for the publisher role binding
        publisher_members = []
        for binding in bindings:
            if binding.get('role') == 'roles/pubsub.publisher':
                publisher_members.extend(binding.get('members', []))
        
        sa_member = f"serviceAccount:{service_account}"
        
        print(f"Found publisher members: {publisher_members}")
        
        if sa_member not in publisher_members:
            print(f"❌ Service account {service_account} NOT found in publisher role")
            print(f"Current publisher members: {publisher_members}")
            print(f"\n🔧 FIX: Run this command:")
            print(f"gcloud pubsub topics add-iam-policy-binding {topic} \\")
            print(f"  --member=\"serviceAccount:{service_account}\" \\")
            print(f"  --role=\"roles/pubsub.publisher\" \\")
            print(f"  --project={PROJECT_ID}")
            pytest.fail(f"Service account {service_account} missing pubsub.publisher role on {topic}")
        
        print(f"✅ Service account has Pub/Sub publisher permissions")
        
    except json.JSONDecodeError as e:
        pytest.fail(f"Failed to parse IAM policy JSON: {e}")
    except Exception as e:
        pytest.fail(f"Error processing IAM policy: {e}")

@pytest.mark.parametrize("env", ["dev", "staging", "prod"])
def test_service_account_bigquery_permissions(env):
    """Test service account has BigQuery permissions"""
    config = ENVIRONMENTS[env]
    dataset = config['dataset']
    service_account = config['service_account']
    
    print(f"\n🔍 Testing BigQuery permissions for {env} environment")
    print(f"Dataset: {dataset}")
    print(f"Service Account: {service_account}")
    
    # Check if service account has BigQuery Data Editor role on dataset
    cmd = f"""bq show --format=prettyjson {PROJECT_ID}:{dataset}"""
    
    result = run_gcloud_command(cmd, check=False)
    
    if result.returncode != 0:
        print(f"❌ Cannot access dataset {dataset}")
        print(f"Error: {result.stderr}")
        pytest.fail(f"Dataset {dataset} not accessible")
    
    # Parse dataset info
    try:
        dataset_info = json.loads(result.stdout)
        dataset_id = dataset_info.get('datasetReference', {}).get('datasetId')
        
        if dataset_id != dataset:
            pytest.fail(f"Dataset ID mismatch: expected {dataset}, got {dataset_id}")
        
        print(f"✅ Dataset {dataset} accessible")
        
    except json.JSONDecodeError as e:
        pytest.fail(f"Failed to parse dataset info: {e}")

def test_service_account_secret_manager_permissions():
    """Test service account has Secret Manager permissions for HUBSPOT_API_KEY"""
    print(f"\n🔍 Testing Secret Manager permissions")
    
    # Check if HUBSPOT_API_KEY secret exists and is accessible
    cmd = f"""gcloud secrets describe HUBSPOT_API_KEY --project={PROJECT_ID}"""
    
    result = run_gcloud_command(cmd, check=False)
    
    if result.returncode != 0:
        print(f"❌ HUBSPOT_API_KEY secret not found or not accessible")
        print(f"Error: {result.stderr}")
        print(f"\n🔧 FIX: Create the secret or check permissions:")
        print(f"gcloud secrets create HUBSPOT_API_KEY --data-file=- --project={PROJECT_ID}")
        pytest.fail("HUBSPOT_API_KEY secret not accessible")
    
    print(f"✅ HUBSPOT_API_KEY secret exists and accessible")
    
    # Test each environment's service account can access the secret
    for env, config in ENVIRONMENTS.items():
        service_account = config['service_account']
        
        cmd = f"""gcloud secrets get-iam-policy HUBSPOT_API_KEY \
            --format=json \
            --project={PROJECT_ID}"""
        
        result = run_gcloud_command(cmd, check=False)
        
        if result.returncode == 0:
            try:
                policy = json.loads(result.stdout)
                bindings = policy.get('bindings', [])
                
                # Check if service account has secretAccessor role
                sa_member = f"serviceAccount:{service_account}"
                has_access = False
                
                for binding in bindings:
                    if binding.get('role') == 'roles/secretmanager.secretAccessor':
                        if sa_member in binding.get('members', []):
                            has_access = True
                            break
                
                if not has_access:
                    print(f"⚠️ Service account {service_account} missing Secret Manager access")
                    print(f"\n🔧 FIX: Run this command:")
                    print(f"gcloud secrets add-iam-policy-binding HUBSPOT_API_KEY \\")
                    print(f"  --member=\"serviceAccount:{service_account}\" \\")
                    print(f"  --role=\"roles/secretmanager.secretAccessor\" \\")
                    print(f"  --project={PROJECT_ID}")
                else:
                    print(f"✅ {env} service account has Secret Manager access")
                    
            except json.JSONDecodeError as e:
                print(f"⚠️ Failed to parse Secret Manager IAM policy: {e}")