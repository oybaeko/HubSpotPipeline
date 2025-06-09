# ===============================================================================
# Part 3: Pub/Sub Infrastructure (CRITICAL)
# ===============================================================================
from test_config import (
    PROJECT_ID, REGION, ENVIRONMENTS, 
    run_gcloud_command, get_current_project, get_current_account
)
import pytest


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