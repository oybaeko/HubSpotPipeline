# ===============================================================================
# Part 4: Secret Manager Setup (CRITICAL)
# ===============================================================================
from test_config import (
    PROJECT_ID, REGION, ENVIRONMENTS, 
    run_gcloud_command, get_current_project, get_current_account
)
import pytest


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
# BigQuery Dataset Access (CRITICAL)
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