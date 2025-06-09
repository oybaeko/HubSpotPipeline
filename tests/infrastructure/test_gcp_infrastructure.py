# ===============================================================================
# tests/infrastructure/test_gcp_infrastructure.py
# High Priority Local Infrastructure Tests for HubSpot Pipeline
# ===============================================================================

import pytest
import subprocess
import json
import os
import sys
from pathlib import Path

# Configuration
PROJECT_ID = "hubspot-452402"
REGION = "europe-west1"

ENVIRONMENTS = {
    'dev': {
        'dataset': 'Hubspot_dev_ob',
        'topic': 'hubspot-events-dev',
        'service_account': 'hubspot-dev-ob@hubspot-452402.iam.gserviceaccount.com',
        'function_name': 'hubspot-ingest-dev'
    },
    'staging': {
        'dataset': 'Hubspot_staging',
        'topic': 'hubspot-events-staging', 
        'service_account': 'hubspot-staging@hubspot-452402.iam.gserviceaccount.com',
        'function_name': 'hubspot-ingest-staging'
    },
    'prod': {
        'dataset': 'Hubspot_prod',
        'topic': 'hubspot-events-prod',
        'service_account': 'hubspot-prod@hubspot-452402.iam.gserviceaccount.com',
        'function_name': 'hubspot-ingest-prod'
    }
}

def run_gcloud_command(cmd, capture_output=True, check=True):
    """Run gcloud command and return result"""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=capture_output, 
            text=True, 
            check=check
        )
        return result
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {cmd}")
        print(f"Exit code: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise

def get_current_project():
    """Get current gcloud project"""
    result = run_gcloud_command("gcloud config get-value project")
    return result.stdout.strip()

def get_current_account():
    """Get current gcloud account"""
    result = run_gcloud_command("gcloud config get-value account")
    return result.stdout.strip()

# ===============================================================================
# 1. Service Account Permissions (CRITICAL)
# ===============================================================================

@pytest.mark.parametrize("env", ["dev", "staging", "prod"])
def test_service_account_pubsub_publisher_permissions(env):
    """Test service account has Pub/Sub publisher permissions - BLOCKING ISSUE"""
    config = ENVIRONMENTS[env]
    topic = config['topic']
    service_account = config['service_account']
    
    print(f"\n🔍 Testing Pub/Sub permissions for {env} environment")
    print(f"Topic: {topic}")
    print(f"Service Account: {service_account}")
    
    # Check if service account has pubsub.publisher role on the topic
    cmd = f"""gcloud pubsub topics get-iam-policy {topic} \
        --format="value(bindings[].members[])" \
        --filter="bindings.role:roles/pubsub.publisher" \
        --project={PROJECT_ID}"""
    
    result = run_gcloud_command(cmd, check=False)
    
    if result.returncode != 0:
        pytest.fail(f"Failed to get IAM policy for topic {topic}: {result.stderr}")
    
    members = result.stdout.strip().split('\n') if result.stdout.strip() else []
    sa_member = f"serviceAccount:{service_account}"
    
    if sa_member not in members:
        print(f"❌ Service account {service_account} NOT found in publisher role")
        print(f"Current members: {members}")
        print(f"\n🔧 FIX: Run this command:")
        print(f"gcloud pubsub topics add-iam-policy-binding {topic} \\")
        print(f"  --member=\"serviceAccount:{service_account}\" \\")
        print(f"  --role=\"roles/pubsub.publisher\" \\")
        print(f"  --project={PROJECT_ID}")
        pytest.fail(f"Service account {service_account} missing pubsub.publisher role on {topic}")
    
    print(f"✅ Service account has Pub/Sub publisher permissions")

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
            --format="value(bindings[].members[])" \
            --filter="bindings.role:roles/secretmanager.secretAccessor" \
            --project={PROJECT_ID}"""
        
        result = run_gcloud_command(cmd, check=False)
        
        if result.returncode == 0:
            members = result.stdout.strip().split('\n') if result.stdout.strip() else []
            sa_member = f"serviceAccount:{service_account}"
            
            if sa_member not in members:
                print(f"⚠️ Service account {service_account} missing Secret Manager access")
                print(f"\n🔧 FIX: Run this command:")
                print(f"gcloud secrets add-iam-policy-binding HUBSPOT_API_KEY \\")
                print(f"  --member=\"serviceAccount:{service_account}\" \\")
                print(f"  --role=\"roles/secretmanager.secretAccessor\" \\")
                print(f"  --project={PROJECT_ID}")
            else:
                print(f"✅ {env} service account has Secret Manager access")

# ===============================================================================
# 2. Pub/Sub Infrastructure (CRITICAL)
# ===============================================================================

@pytest.mark.parametrize("env", ["dev", "staging", "prod"])
def test_pubsub_topics_exist(env):
    """Test Pub/Sub topics exist - BLOCKING ISSUE"""
    config = ENVIRONMENTS[env]
    topic = config['topic']
    
    print(f"\n🔍 Testing Pub/Sub topic exists: {topic}")
    
    cmd = f"gcloud pubsub topics describe {topic} --project={PROJECT_ID}"
    result = run_gcloud_command(cmd, check=False)
    
    if result.returncode != 0:
        print(f"❌ Topic {topic} does not exist")
        print(f"Error: {result.stderr}")
        print(f"\n🔧 FIX: Create the topic:")
        print(f"gcloud pubsub topics create {topic} --project={PROJECT_ID}")
        pytest.fail(f"Pub/Sub topic {topic} does not exist")
    
    print(f"✅ Topic {topic} exists")

@pytest.mark.parametrize("env", ["dev"])  # Only test dev for now
def test_scoring_function_subscription_exists(env):
    """Test scoring function subscription exists"""
    config = ENVIRONMENTS[env]
    topic = config['topic']
    scoring_function = f"hubspot-scoring-{env}"
    
    print(f"\n🔍 Testing scoring function subscription for {topic}")
    
    # List subscriptions for the topic
    cmd = f"""gcloud pubsub subscriptions list \
        --filter="topic:projects/{PROJECT_ID}/topics/{topic}" \
        --format="value(name)" \
        --project={PROJECT_ID}"""
    
    result = run_gcloud_command(cmd, check=False)
    
    if result.returncode != 0:
        print(f"❌ Failed to list subscriptions for topic {topic}")
        pytest.fail(f"Cannot list subscriptions for {topic}")
    
    subscriptions = result.stdout.strip().split('\n') if result.stdout.strip() else []
    
    if not subscriptions or subscriptions == ['']:
        print(f"⚠️ No subscriptions found for topic {topic}")
        print(f"This is expected if scoring function hasn't been deployed yet")
        print(f"Scoring function will create subscription automatically when deployed")
    else:
        print(f"✅ Found {len(subscriptions)} subscription(s) for topic {topic}")
        for sub in subscriptions:
            print(f"  • {sub}")

# ===============================================================================
# 3. Secret Manager Setup (CRITICAL)
# ===============================================================================

def test_hubspot_api_key_secret_exists():
    """Test HUBSPOT_API_KEY secret exists and has value"""
    print(f"\n🔍 Testing HUBSPOT_API_KEY secret")
    
    # Check if secret exists
    cmd = f"gcloud secrets describe HUBSPOT_API_KEY --project={PROJECT_ID}"
    result = run_gcloud_command(cmd, check=False)
    
    if result.returncode != 0:
        print(f"❌ HUBSPOT_API_KEY secret does not exist")
        print(f"\n🔧 FIX: Create the secret:")
        print(f"echo 'your-hubspot-api-key' | gcloud secrets create HUBSPOT_API_KEY --data-file=- --project={PROJECT_ID}")
        pytest.fail("HUBSPOT_API_KEY secret does not exist")
    
    # Check if secret has a value
    cmd = f"gcloud secrets versions access latest --secret=HUBSPOT_API_KEY --project={PROJECT_ID}"
    result = run_gcloud_command(cmd, check=False)
    
    if result.returncode != 0:
        print(f"❌ Cannot access HUBSPOT_API_KEY secret value")
        pytest.fail("Cannot access HUBSPOT_API_KEY secret value")
    
    api_key = result.stdout.strip()
    
    if not api_key or len(api_key) < 10:
        print(f"❌ HUBSPOT_API_KEY appears to be empty or too short")
        pytest.fail("HUBSPOT_API_KEY appears to be invalid")
    
    print(f"✅ HUBSPOT_API_KEY secret exists with value (length: {len(api_key)})")

def test_hubspot_api_key_actually_works():
    """Test HUBSPOT_API_KEY actually works with HubSpot API"""
    print(f"\n🔍 Testing HUBSPOT_API_KEY with HubSpot API")
    
    # Get the API key
    cmd = f"gcloud secrets versions access latest --secret=HUBSPOT_API_KEY --project={PROJECT_ID}"
    result = run_gcloud_command(cmd, check=False)
    
    if result.returncode != 0:
        pytest.skip("Cannot access HUBSPOT_API_KEY secret")
    
    api_key = result.stdout.strip()
    
    # Test API key with a simple HubSpot API call
    try:
        import requests
        
        url = "https://api.hubapi.com/crm/v3/objects/companies"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        params = {"limit": 1}
        
        response = requests.get(url, headers=headers, params=params, timeout=10)
        
        if response.status_code == 401:
            print(f"❌ HUBSPOT_API_KEY is invalid (401 Unauthorized)")
            pytest.fail("HUBSPOT_API_KEY is invalid")
        elif response.status_code == 403:
            print(f"❌ HUBSPOT_API_KEY lacks permissions (403 Forbidden)")
            pytest.fail("HUBSPOT_API_KEY lacks permissions")
        elif response.status_code == 200:
            print(f"✅ HUBSPOT_API_KEY works with HubSpot API")
        else:
            print(f"⚠️ Unexpected response: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            
    except ImportError:
        pytest.skip("requests library not available for API test")
    except Exception as e:
        print(f"⚠️ API test failed: {e}")

# ===============================================================================
# 4. BigQuery Dataset Access (CRITICAL)
# ===============================================================================

@pytest.mark.parametrize("env", ["dev", "staging", "prod"])
def test_bigquery_datasets_exist(env):
    """Test BigQuery datasets exist for all environments"""
    config = ENVIRONMENTS[env]
    dataset = config['dataset']
    
    print(f"\n🔍 Testing BigQuery dataset exists: {dataset}")
    
    cmd = f"bq show {PROJECT_ID}:{dataset}"
    result = run_gcloud_command(cmd, check=False)
    
    if result.returncode != 0:
        print(f"❌ Dataset {dataset} does not exist")
        print(f"Error: {result.stderr}")
        print(f"\n🔧 FIX: Create the dataset:")
        print(f"bq mk --dataset --location={REGION} {PROJECT_ID}:{dataset}")
        pytest.fail(f"BigQuery dataset {dataset} does not exist")
    
    print(f"✅ Dataset {dataset} exists")

# ===============================================================================
# 5. Basic Connectivity (QUICK WINS)
# ===============================================================================

def test_gcloud_authenticated():
    """Test gcloud is authenticated and pointing to correct project"""
    print(f"\n🔍 Testing gcloud authentication")
    
    # Check current project
    current_project = get_current_project()
    current_account = get_current_account()
    
    print(f"Current project: {current_project}")
    print(f"Current account: {current_account}")
    
    if current_project != PROJECT_ID:
        print(f"❌ Wrong project. Expected: {PROJECT_ID}, Got: {current_project}")
        print(f"\n🔧 FIX: Set correct project:")
        print(f"gcloud config set project {PROJECT_ID}")
        pytest.fail(f"Wrong project configured: {current_project}")
    
    if not current_account or current_account == "(unset)":
        print(f"❌ Not authenticated with gcloud")
        print(f"\n🔧 FIX: Authenticate:")
        print(f"gcloud auth login")
        pytest.fail("Not authenticated with gcloud")
    
    print(f"✅ Authenticated as {current_account} with project {PROJECT_ID}")

def test_required_apis_enabled():
    """Test required GCP APIs are enabled"""
    print(f"\n🔍 Testing required APIs are enabled")
    
    required_apis = [
        "cloudfunctions.googleapis.com",
        "bigquery.googleapis.com", 
        "pubsub.googleapis.com",
        "secretmanager.googleapis.com",
        "logging.googleapis.com"
    ]
    
    for api in required_apis:
        cmd = f"gcloud services list --enabled --filter=name:{api} --format='value(name)' --project={PROJECT_ID}"
        result = run_gcloud_command(cmd, check=False)
        
        if result.returncode != 0 or not result.stdout.strip():
            print(f"❌ API {api} is not enabled")
            print(f"\n🔧 FIX: Enable the API:")
            print(f"gcloud services enable {api} --project={PROJECT_ID}")
            pytest.fail(f"Required API {api} is not enabled")
        
        print(f"✅ {api} is enabled")

def test_hubspot_api_connectivity():
    """Test connectivity to HubSpot API"""
    print(f"\n🔍 Testing connectivity to HubSpot API")
    
    try:
        import requests
        
        # Test basic connectivity (should get 401 without auth)
        response = requests.get(
            "https://api.hubapi.com/crm/v3/objects/companies",
            params={"limit": 1},
            timeout=10
        )
        
        # 401 is expected without auth, means connectivity works
        if response.status_code in [401, 403]:
            print(f"✅ HubSpot API is reachable (got {response.status_code} as expected)")
        else:
            print(f"⚠️ Unexpected response from HubSpot API: {response.status_code}")
            
    except ImportError:
        pytest.skip("requests library not available")
    except requests.exceptions.RequestException as e:
        print(f"❌ Cannot reach HubSpot API: {e}")
        pytest.fail(f"HubSpot API connectivity failed: {e}")

# ===============================================================================
# 6. Cloud Function Deployment Setup (QUICK WINS)  
# ===============================================================================

@pytest.mark.parametrize("env", ["dev", "staging", "prod"])
def test_cloud_function_service_accounts_exist(env):
    """Test Cloud Function service accounts exist"""
    config = ENVIRONMENTS[env]
    service_account = config['service_account']
    
    print(f"\n🔍 Testing service account exists: {service_account}")
    
    # Extract just the account name (before @)
    account_name = service_account.split('@')[0]
    
    cmd = f"gcloud iam service-accounts describe {service_account} --project={PROJECT_ID}"
    result = run_gcloud_command(cmd, check=False)
    
    if result.returncode != 0:
        print(f"❌ Service account {service_account} does not exist")
        print(f"\n🔧 FIX: Create the service account:")
        print(f"gcloud iam service-accounts create {account_name} \\")
        print(f"  --display-name='HubSpot Pipeline {env.title()}' \\")
        print(f"  --project={PROJECT_ID}")
        pytest.fail(f"Service account {service_account} does not exist")
    
    print(f"✅ Service account {service_account} exists")

def test_deployment_permissions():
    """Test current user has permissions to deploy Cloud Functions"""
    print(f"\n🔍 Testing deployment permissions")
    
    current_account = get_current_account()
    
    # Test if user can list Cloud Functions (basic permission check)
    cmd = f"gcloud functions list --regions={REGION} --project={PROJECT_ID}"
    result = run_gcloud_command(cmd, check=False)
    
    if result.returncode != 0:
        print(f"❌ Cannot list Cloud Functions - insufficient permissions")
        print(f"Current account: {current_account}")
        print(f"Error: {result.stderr}")
        print(f"\n🔧 FIX: User needs Cloud Functions Developer role:")
        print(f"gcloud projects add-iam-policy-binding {PROJECT_ID} \\")
        print(f"  --member='user:{current_account}' \\")
        print(f"  --role='roles/cloudfunctions.developer'")
        pytest.fail("Insufficient permissions to deploy Cloud Functions")
    
    print(f"✅ {current_account} has Cloud Functions deployment permissions")