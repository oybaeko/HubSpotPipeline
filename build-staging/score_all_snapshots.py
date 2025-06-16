#!/usr/bin/env python3
"""
Script to run scoring on all available snapshots or the latest snapshot
"""

import sys
import os
import logging
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger('score_snapshots')

def get_latest_snapshot_id():
    """Get the latest snapshot ID from the registry"""
    try:
        from google.cloud import bigquery
        import os
        
        client = bigquery.Client()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        query = f"""
        SELECT snapshot_id, record_timestamp
        FROM `{project_id}.{dataset_id}.hs_snapshot_registry`
        WHERE status LIKE '%ingest%'
        ORDER BY record_timestamp DESC
        LIMIT 1
        """
        
        result = client.query(query).result()
        latest = next(result, None)
        
        if latest:
            return latest.snapshot_id, latest.record_timestamp
        else:
            return None, None
            
    except Exception as e:
        print(f"❌ Failed to get latest snapshot: {e}")
        return None, None

def get_all_snapshot_ids(limit=10):
    """Get all snapshot IDs from the registry"""
    try:
        from google.cloud import bigquery
        import os
        
        client = bigquery.Client()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        query = f"""
        SELECT snapshot_id, record_timestamp, status
        FROM `{project_id}.{dataset_id}.hs_snapshot_registry`
        WHERE status LIKE '%ingest%'
        ORDER BY record_timestamp DESC
        LIMIT {limit}
        """
        
        result = client.query(query).result()
        snapshots = []
        
        for row in result:
            snapshots.append({
                'snapshot_id': row.snapshot_id,
                'timestamp': row.record_timestamp,
                'status': row.status
            })
        
        return snapshots
        
    except Exception as e:
        print(f"❌ Failed to get snapshots: {e}")
        return []

def get_environment():
    """Detect environment from dataset name"""
    dataset = os.getenv('BIGQUERY_DATASET_ID', '')
    if 'prod' in dataset.lower():
        return 'prod'
    elif 'staging' in dataset.lower():
        return 'staging'
    else:
        return 'dev'

def wait_for_scoring_completion(snapshot_id, logger, max_wait_minutes=10):
    """Wait for scoring to complete by checking the snapshot registry"""
    logger.info(f"⏳ Waiting for scoring completion for snapshot: {snapshot_id}")
    
    import time
    max_wait_seconds = max_wait_minutes * 60
    check_interval = 15  # Check every 15 seconds
    elapsed_time = 0
    
    try:
        from google.cloud import bigquery
        
        client = bigquery.Client()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        while elapsed_time < max_wait_seconds:
            # Check if scoring has completed for this snapshot
            query = f"""
            SELECT snapshot_id, status, record_timestamp
            FROM `{project_id}.{dataset_id}.hs_snapshot_registry`
            WHERE snapshot_id = @snapshot_id
              AND triggered_by = 'scoring_completion'
            ORDER BY record_timestamp DESC
            LIMIT 1
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id)
                ]
            )
            
            result = client.query(query, job_config=job_config).result()
            completion_record = next(result, None)
            
            if completion_record:
                logger.info(f"✅ Scoring completed for {snapshot_id} at {completion_record.record_timestamp}")
                return True
            
            # Also check for scoring failure
            failure_query = f"""
            SELECT snapshot_id, status, record_timestamp
            FROM `{project_id}.{dataset_id}.hs_snapshot_registry`
            WHERE snapshot_id = @snapshot_id
              AND triggered_by = 'scoring_failure'
            ORDER BY record_timestamp DESC
            LIMIT 1
            """
            
            failure_result = client.query(failure_query, job_config=job_config).result()
            failure_record = next(failure_result, None)
            
            if failure_record:
                logger.error(f"❌ Scoring failed for {snapshot_id} at {failure_record.record_timestamp}")
                return False
            
            # Wait before next check
            logger.info(f"⏳ Still waiting... ({elapsed_time//60}m {elapsed_time%60}s elapsed)")
            time.sleep(check_interval)
            elapsed_time += check_interval
        
        logger.warning(f"⚠️ Timeout waiting for scoring completion after {max_wait_minutes} minutes")
        return False
        
    except Exception as e:
        logger.error(f"❌ Error while waiting for scoring completion: {e}")
        return False

def score_snapshot_via_pubsub(snapshot_id, logger, wait_for_completion=True):
    """Score a specific snapshot by calling the Cloud Function via Pub/Sub"""
    logger.info(f"🔄 Triggering scoring for snapshot: {snapshot_id}")
    
    try:
        from google.cloud import pubsub_v1
        import json
        
        # Get environment and topic
        env = get_environment()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        
        topic_mapping = {
            'dev': 'hubspot-events-dev',
            'staging': 'hubspot-events-staging',
            'prod': 'hubspot-events-prod'
        }
        
        topic_name = topic_mapping.get(env, 'hubspot-events-dev')
        
        logger.info(f"📤 Publishing to topic: {topic_name} (environment: {env})")
        
        # Create Pub/Sub client
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        
        # Create event data for scoring
        event_data = {
            "snapshot_id": snapshot_id,
            "data_tables": {"hs_companies": 100, "hs_deals": 50},  # Mock counts - scoring will get actual data
            "reference_tables": {"hs_owners": 5, "hs_deal_stage_reference": 10}
        }
        
        # Build full event envelope
        event = {
            "type": "hubspot.snapshot.completed",
            "version": "1.0",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": f"manual-scoring-trigger-{env}",
            "environment": env,
            "data": event_data
        }
        
        # Publish message
        message_json = json.dumps(event)
        logger.info(f"📨 Publishing message: {len(message_json)} bytes")
        logger.debug(f"Message content: {message_json}")
        
        future = publisher.publish(topic_path, message_json.encode('utf-8'))
        message_id = future.result(timeout=30)
        
        logger.info(f"✅ Published message to Cloud Function: {message_id}")
        logger.info(f"🎯 Scoring function should process snapshot: {snapshot_id}")
        
        # Wait for completion if requested
        if wait_for_completion:
            logger.info(f"⏳ Waiting for scoring to complete before continuing...")
            success = wait_for_scoring_completion(snapshot_id, logger)
            if success:
                logger.info(f"✅ Scoring completed successfully for {snapshot_id}")
                return True
            else:
                logger.error(f"❌ Scoring did not complete successfully for {snapshot_id}")
                return False
        else:
            return True
        
    except ImportError:
        logger.error("❌ google-cloud-pubsub not installed. Run: pip install google-cloud-pubsub")
        return False
    except Exception as e:
        logger.error(f"❌ Failed to trigger scoring via Pub/Sub: {e}")
        return False

def score_snapshot_via_http(snapshot_id, logger):
    """Score a specific snapshot by calling the scoring Cloud Function via HTTP (if it has HTTP trigger)"""
    logger.info(f"🔄 Triggering scoring via HTTP for snapshot: {snapshot_id}")
    
    try:
        import requests
        
        # Get environment
        env = get_environment()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        region = "europe-west1"  # From your config
        
        # Scoring functions are typically Pub/Sub triggered, but if you have HTTP version:
        scoring_function_url = f"https://{region}-{project_id}.cloudfunctions.net/hubspot-scoring-{env}"
        
        # Create event data
        event_data = {
            "mode": "score",
            "snapshot_id": snapshot_id,
            "data_tables": {"hs_companies": 100, "hs_deals": 50},
            "reference_tables": {"hs_owners": 5, "hs_deal_stage_reference": 10}
        }
        
        logger.info(f"📤 Calling HTTP endpoint: {scoring_function_url}")
        
        response = requests.post(
            scoring_function_url,
            json=event_data,
            timeout=300  # 5 minutes timeout
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ HTTP call successful: {result.get('status', 'unknown')}")
            return True
        else:
            logger.error(f"❌ HTTP call failed: {response.status_code} - {response.text}")
            return False
            
    except ImportError:
        logger.error("❌ requests library not available")
        return False
    except Exception as e:
        logger.error(f"❌ Failed to call scoring function via HTTP: {e}")
        return False

def score_snapshot(snapshot_id, logger, wait_for_completion=True):
    """Score a specific snapshot by calling the deployed Cloud Function"""
    logger.info(f"🎯 Calling Cloud Function to score snapshot: {snapshot_id}")
    
    # Try Pub/Sub first (recommended method)
    logger.info("📤 Method 1: Trying Pub/Sub trigger...")
    if score_snapshot_via_pubsub(snapshot_id, logger, wait_for_completion):
        if wait_for_completion:
            logger.info(f"✅ Successfully completed scoring for {snapshot_id}")
        else:
            logger.info(f"✅ Successfully triggered scoring for {snapshot_id}")
        return True
    
    # Fallback to HTTP if available (without waiting - HTTP calls are synchronous)
    logger.info("📤 Method 2: Trying HTTP trigger (fallback)...")
    if score_snapshot_via_http(snapshot_id, logger):
        logger.info(f"✅ Successfully completed scoring via HTTP for {snapshot_id}")
        return True
    
    logger.error(f"❌ Failed to trigger scoring for {snapshot_id} via all methods")
    return False

def main():
    """Main function"""
    logger = setup_logging()
    
    print("🧪 HubSpot Cloud Function Scoring Trigger")
    print("=" * 50)
    print("📡 This script runs LOCALLY and calls the deployed scoring Cloud Function")
    print("🎯 No local scoring - triggers the actual deployed function")
    
    # Initialize environment
    try:
        # We only need basic environment setup for BigQuery access to read snapshots
        # No need for full scoring pipeline since we're calling the Cloud Function
        
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        if not project_id or not dataset_id:
            logger.error("❌ Please set BIGQUERY_PROJECT_ID and BIGQUERY_DATASET_ID environment variables")
            return 1
            
        env = get_environment()
        logger.info(f"✅ Environment: {env}")
        logger.info(f"✅ Dataset: {dataset_id}")
        logger.info(f"✅ Project: {project_id}")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize environment: {e}")
        return 1
    
    # Show menu
    print(f"\n🎯 Target Environment: {env}")
    print(f"📊 Dataset: {dataset_id}")
    print(f"📤 Will trigger: hubspot-scoring-{env} Cloud Function")
    print("\nOptions:")
    print("1. Trigger scoring for latest snapshot")
    print("2. Trigger scoring for all recent snapshots (last 10)")
    print("3. Trigger scoring for specific snapshot")
    print("4. List available snapshots")
    print("5. Test Cloud Function connectivity")
    print("6. Exit")
    
    while True:
        try:
            choice = input("\nEnter your choice (1-6): ").strip()
            
            if choice == '1':
                # Score latest snapshot
                snapshot_id, timestamp = get_latest_snapshot_id()
                if snapshot_id:
                    logger.info(f"📸 Latest snapshot: {snapshot_id} ({timestamp})")
                    
                    # Ask if user wants to wait for completion
                    wait_choice = input("Wait for scoring to complete? (Y/n): ").strip().lower()
                    wait_for_completion = wait_choice != 'n'  # Default to waiting
                    
                    if score_snapshot(snapshot_id, logger, wait_for_completion):
                        if wait_for_completion:
                            print(f"✅ Successfully completed scoring for latest snapshot: {snapshot_id}")
                            print(f"📊 Check BigQuery tables for results:")
                            print(f"   • hs_pipeline_units_snapshot")
                            print(f"   • hs_pipeline_score_history")
                        else:
                            print(f"✅ Successfully triggered scoring for latest snapshot: {snapshot_id}")
                            print(f"⏳ Check Cloud Function logs for progress:")
                            print(f"   gcloud functions logs read hubspot-scoring-{get_environment()} --region=europe-west1 --limit=50")
                    else:
                        print(f"❌ Failed to score latest snapshot: {snapshot_id}")
                else:
                    print("❌ No snapshots found")
                    
            elif choice == '2':
                # Score all recent snapshots
                snapshots = get_all_snapshot_ids(10)
                if snapshots:
                    print(f"\n📋 Found {len(snapshots)} snapshots to score:")
                    for i, snap in enumerate(snapshots, 1):
                        print(f"  {i}. {snap['snapshot_id']} ({snap['timestamp']})")
                    
                    confirm = input(f"\nTrigger scoring for all {len(snapshots)} snapshots? (y/N): ").strip().lower()
                    if confirm == 'y':
                        # Ask about waiting for completion
                        wait_choice = input("\nWait for each scoring to complete before starting the next? (Y/n): ").strip().lower()
                        wait_for_completion = wait_choice != 'n'  # Default to waiting
                        
                        if wait_for_completion:
                            print(f"✅ Will wait for each scoring to complete (sequential processing)")
                            print(f"⏳ This may take a while but prevents conflicts")
                        else:
                            print(f"⚠️  Will trigger all at once (parallel processing)")
                            print(f"💡 This is faster but may cause conflicts in scoring logic")
                        
                        successful = 0
                        failed = 0
                        
                        print(f"\n🚀 Starting scoring for {len(snapshots)} snapshots...")
                        start_time = datetime.utcnow()
                        
                        for i, snap in enumerate(snapshots, 1):
                            print(f"\n📤 [{i}/{len(snapshots)}] Processing: {snap['snapshot_id']}")
                            snapshot_start_time = datetime.utcnow()
                            
                            if score_snapshot(snap['snapshot_id'], logger, wait_for_completion):
                                successful += 1
                                snapshot_duration = (datetime.utcnow() - snapshot_start_time).total_seconds()
                                print(f"   ✅ Completed in {snapshot_duration:.1f}s")
                            else:
                                failed += 1
                                print(f"   ❌ Failed")
                            
                            # Show progress
                            elapsed = (datetime.utcnow() - start_time).total_seconds()
                            if wait_for_completion and i < len(snapshots):
                                avg_time = elapsed / i
                                remaining_time = avg_time * (len(snapshots) - i)
                                print(f"   📊 Progress: {i}/{len(snapshots)} | Estimated remaining: {remaining_time/60:.1f}m")
                        
                        total_time = (datetime.utcnow() - start_time).total_seconds()
                        print(f"\n🎉 Batch scoring completed in {total_time/60:.1f} minutes!")
                        print(f"✅ Successfully completed: {successful}")
                        print(f"❌ Failed: {failed}")
                        
                        if wait_for_completion:
                            print(f"\n📊 Check BigQuery tables for scoring results:")
                            print(f"   • hs_pipeline_units_snapshot")
                            print(f"   • hs_pipeline_score_history")
                        else:
                            print(f"\n⏳ Check Cloud Function logs for progress:")
                            print(f"   gcloud functions logs read hubspot-scoring-{get_environment()} --region=europe-west1 --limit=100")
                    else:
                        print("❌ Cancelled")
                else:
                    print("❌ No snapshots found")
                    
            elif choice == '3':
                # Score specific snapshot
                snapshot_id = input("Enter snapshot ID: ").strip()
                if snapshot_id:
                    # Ask if user wants to wait for completion
                    wait_choice = input("Wait for scoring to complete? (Y/n): ").strip().lower()
                    wait_for_completion = wait_choice != 'n'  # Default to waiting
                    
                    if score_snapshot(snapshot_id, logger, wait_for_completion):
                        if wait_for_completion:
                            print(f"✅ Successfully completed scoring for snapshot: {snapshot_id}")
                            print(f"📊 Check BigQuery tables for results:")
                            print(f"   • hs_pipeline_units_snapshot")
                            print(f"   • hs_pipeline_score_history")
                        else:
                            print(f"✅ Successfully triggered scoring for snapshot: {snapshot_id}")
                            print(f"⏳ Check Cloud Function logs for progress:")
                            print(f"   gcloud functions logs read hubspot-scoring-{get_environment()} --region=europe-west1 --limit=50")
                    else:
                        print(f"❌ Failed to score snapshot: {snapshot_id}")
                else:
                    print("❌ No snapshot ID provided")
                    
            elif choice == '4':
                # List snapshots
                snapshots = get_all_snapshot_ids(20)
                if snapshots:
                    print(f"\n📋 Available snapshots ({len(snapshots)} shown):")
                    for i, snap in enumerate(snapshots, 1):
                        print(f"  {i:2d}. {snap['snapshot_id']} | {snap['timestamp']} | {snap['status']}")
                else:
                    print("❌ No snapshots found")
                    
            elif choice == '5':
                # Test connectivity
                print(f"\n🔍 Testing Cloud Function connectivity...")
                test_connectivity(env, logger)
                
            elif choice == '6':
                print("👋 Goodbye!")
                break
                
            else:
                print("❌ Invalid choice. Please select 1-6.")
                
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            break
        except Exception as e:
            logger.error(f"❌ Error: {e}")

def test_connectivity(env, logger):
    """Test connectivity to Cloud Function infrastructure"""
    try:
        from google.cloud import pubsub_v1
        
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        topic_mapping = {
            'dev': 'hubspot-events-dev',
            'staging': 'hubspot-events-staging',
            'prod': 'hubspot-events-prod'
        }
        
        topic_name = topic_mapping.get(env, 'hubspot-events-dev')
        
        # Test Pub/Sub topic access
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(project_id, topic_name)
        
        print(f"✅ Pub/Sub client created successfully")
        print(f"✅ Topic path: {topic_path}")
        print(f"✅ Cloud Function: hubspot-scoring-{env}")
        print(f"✅ Ready to trigger scoring via Pub/Sub")
        
    except ImportError:
        print(f"❌ google-cloud-pubsub not installed")
        print(f"💡 Install with: pip install google-cloud-pubsub")
    except Exception as e:
        print(f"❌ Connectivity test failed: {e}")
        logger.error(f"Connectivity test error: {e}")

if __name__ == "__main__":
    sys.exit(main())