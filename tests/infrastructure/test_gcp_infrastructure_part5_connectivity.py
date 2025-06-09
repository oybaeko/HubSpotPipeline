# ===============================================================================
# Part 5: Basic Connectivity (QUICK WINS)
# ===============================================================================
from test_config import (
    PROJECT_ID, REGION, ENVIRONMENTS, 
    run_gcloud_command, get_current_project, get_current_account
)
import pytest

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
# Cloud Function Deployment Setup (QUICK WINS)  
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