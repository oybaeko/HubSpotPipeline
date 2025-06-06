# main.py

import sys
import argparse
import logging
from flask import Request
from werkzeug.test import EnvironBuilder
from werkzeug.wrappers import Request as WerkzeugRequest

# Configure logging FIRST, before any other imports
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True  # Override any existing configuration
)

from src.ingest_main import main as ingest_cloud_main
from src.scoring_main import main as scoring_cloud_main

def get_environment_info():
    """Get current environment and dataset information"""
    try:
        from src.hubspot_pipeline.hubspot_ingest.config_loader import init_env, get_config
        
        # Initialize to get config
        init_env()
        config = get_config()
        
        env = config.get('ENVIRONMENT', 'unknown')
        dataset = config.get('BIGQUERY_DATASET_ID', 'unknown')
        project = config.get('BIGQUERY_PROJECT_ID', 'unknown')
        
        return {
            'environment': env,
            'dataset': dataset,
            'project': project,
            'is_production': env.lower() == 'production',
            'is_staging': env.lower() == 'staging'
        }
    except Exception as e:
        logging.error(f"Failed to get environment info: {e}")
        return {
            'environment': 'ERROR',
            'dataset': 'ERROR',
            'project': 'ERROR',
            'is_production': False,
            'is_staging': False
        }

def show_environment_warning(env_info):
    """Show environment information with warnings for prod/staging"""
    print("\n" + "="*80)
    print("🌍 CURRENT ENVIRONMENT")
    print("="*80)
    print(f"Environment: {env_info['environment']}")
    print(f"Project: {env_info['project']}")
    print(f"Dataset: {env_info['dataset']}")
    
    if env_info['is_production']:
        print("\n" + "🚨"*20)
        print("⚠️  WARNING: YOU ARE IN PRODUCTION ENVIRONMENT!")
        print("⚠️  ALL CHANGES WILL AFFECT LIVE DATA!")
        print("🚨"*20)
    elif env_info['is_staging']:
        print("\n" + "⚠️"*20)
        print("🔶 CAUTION: You are in STAGING environment")
        print("🔶 Changes will affect staging data")
        print("⚠️"*20)
    else:
        print("\n✅ Safe development environment")
    
    print("="*80)

def confirm_environment_action(env_info, action_description):
    """Get confirmation for actions in prod/staging environments"""
    if env_info['is_production']:
        print(f"\n🚨 PRODUCTION CONFIRMATION REQUIRED 🚨")
        print(f"Action: {action_description}")
        print(f"Environment: {env_info['environment']}")
        print(f"Dataset: {env_info['dataset']}")
        
        confirm1 = input("\nType 'PRODUCTION' to confirm you want to proceed: ").strip()
        if confirm1 != 'PRODUCTION':
            print("❌ Action cancelled - incorrect confirmation")
            return False
            
        confirm2 = input("Type 'YES' to double confirm: ").strip().upper()
        if confirm2 != 'YES':
            print("❌ Action cancelled - double confirmation failed")
            return False
            
        return True
        
    elif env_info['is_staging']:
        print(f"\n🔶 STAGING CONFIRMATION REQUIRED")
        print(f"Action: {action_description}")
        print(f"Environment: {env_info['environment']}")
        print(f"Dataset: {env_info['dataset']}")
        
        confirm = input("\nType 'YES' to confirm: ").strip().upper()
        return confirm == 'YES'
    
    # Development - simple confirmation
    confirm = input(f"\nConfirm: {action_description} (y/n): ").strip().lower()
    return confirm == 'y'

def show_main_menu(env_info):
    """Display main test menu with environment info"""
    print("\n" + "="*80)
    print("🧪 HubSpot Pipeline Testing & Debugging Tool")
    print("="*80)
    print(f"Environment: {env_info['environment']} | Dataset: {env_info['dataset']}")
    print("="*80)
    print()
    print("🔹 INGEST TESTING (Cloud Function)")
    print("  1. 🧪 Dry Run - Test ingest logic (no BigQuery writes)")
    print("  2. 🔬 Small Live Run - 10 records with full pipeline")
    print("  3. 📊 Medium Run - 50 records with full pipeline") 
    print("  4. 🚀 Full Run - All records (production-like)")
    print("  5. 🔧 Custom Ingest - Specify your own parameters")
    print("  6. 🗑️ Clean Ingest Data - Delete snapshot tables")
    print()
    print("🔹 SCORING TESTING (Cloud Function)")
    print("  7. 📈 Score Latest Snapshot - Cloud Function simulation")
    print("  8. 📈 Score Specific Snapshot - Cloud Function simulation")
    print("  9. 🔄 Direct Score Latest - Use scoring modules directly")
    print("  10. 🔄 Direct Score Specific - Use scoring modules directly")
    print("  11. 📋 Populate Stage Mapping - Update scoring reference data")
    print("  12. 🗑️ Clean Scoring Data - Delete scoring tables")
    print()
    print("🔹 UTILITIES")
    print("  13. 📋 View Recent Snapshots - Check registry")
    print("  14. 🌐 Flask Mode - Simulate HTTP Cloud Function")
    print("  15. 📝 Debug Mode - Verbose logging test")
    print("  16. 🗑️ Clean ALL Data - Fresh start (delete everything)")
    print()
    print("  0. ❌ Exit")
    print("="*80)

def get_user_choice():
    """Get and validate user menu choice"""
    while True:
        try:
            choice = input("\n🔹 Enter your choice (0-16): ").strip()
            if choice in [str(i) for i in range(17)]:
                return choice
            else:
                print("❌ Invalid choice. Please enter a number between 0-16.")
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            sys.exit(0)
        except EOFError:
            print("\n👋 Goodbye!")
            sys.exit(0)

def run_ingest_test(event_data, env_info):
    """Run ingest test with environment-aware confirmations"""
    action_desc = f"Run ingest with {event_data.get('limit', 'default')} records"
    if not event_data.get('dry_run', True):
        action_desc += " (LIVE - will write to BigQuery)"
    
    if not confirm_environment_action(env_info, action_desc):
        return None
    
    print(f"\n🚀 Running ingest with parameters: {event_data}")
    print("-" * 50)
    
    try:
        from src.hubspot_pipeline.hubspot_ingest.config_loader import init_env
        from src.hubspot_pipeline.hubspot_ingest.main import main as ingest_main
        
        # Initialize environment
        init_env(log_level=event_data.get('log_level', 'INFO'))
        
        # Run ingest
        result = ingest_main(event=event_data)
        
        print("-" * 50)
        print("✅ Ingest test completed successfully!")
        print(f"📤 Result: {result}")
        
        return result
        
    except Exception as e:
        print("-" * 50)
        print(f"❌ Ingest test failed: {e}")
        logging.error(f"Ingest test execution failed: {e}", exc_info=True)
        return None

def run_scoring_test(snapshot_id, env_info):
    """Run scoring test using the actual scoring Cloud Function"""
    action_desc = f"Run scoring on snapshot: {snapshot_id or 'latest'}"
    
    if not confirm_environment_action(env_info, action_desc):
        return None
    
    print(f"\n📊 Testing Scoring Cloud Function")
    print(f"🎯 Target snapshot: {snapshot_id or 'latest'}")
    print("-" * 50)
    
    try:
        # If no snapshot_id provided, get the latest one
        if not snapshot_id:
            snapshot_id = get_latest_snapshot_id()
            if not snapshot_id:
                print("❌ No snapshots found in registry")
                return None
        
        print(f"🎯 Processing snapshot: {snapshot_id}")
        
        # Create a mock Pub/Sub event for the scoring function
        import json
        import base64
        
        mock_event_data = {
            "type": "hubspot.snapshot.completed",
            "data": {
                "snapshot_id": snapshot_id,
                "data_tables": {"hs_companies": 10, "hs_deals": 5},  # Mock counts
                "reference_tables": {"hs_owners": 3, "hs_deal_stage_reference": 8}
            }
        }
        
        mock_pubsub_event = {
            'data': base64.b64encode(json.dumps(mock_event_data).encode('utf-8'))
        }
        
        print("📤 Simulating Pub/Sub event for scoring function...")
        
        # Call the scoring Cloud Function
        result = scoring_cloud_main(mock_pubsub_event, None)
        
        print("-" * 50)
        if result.get('status') == 'success':
            print("✅ Scoring test completed successfully!")
            print(f"📊 Result: {result}")
        else:
            print(f"❌ Scoring test failed: {result}")
        
        return result
        
    except Exception as e:
        print("-" * 50)
        print(f"❌ Scoring test failed: {e}")
        logging.error(f"Scoring test failed: {e}", exc_info=True)
        return None

def run_direct_scoring_test(snapshot_id, env_info):
    """Run scoring test using scoring modules directly (alternative to Cloud Function test)"""
    action_desc = f"Run direct scoring on snapshot: {snapshot_id or 'latest'}"
    
    if not confirm_environment_action(env_info, action_desc):
        return None
    
    print(f"\n📊 Testing Scoring Modules Directly")
    print(f"🎯 Target snapshot: {snapshot_id or 'latest'}")
    print("-" * 50)
    
    try:
        # If no snapshot_id provided, get the latest one
        if not snapshot_id:
            snapshot_id = get_latest_snapshot_id()
            if not snapshot_id:
                print("❌ No snapshots found in registry")
                return None
        
        print(f"🎯 Processing snapshot: {snapshot_id}")
        
        # Use scoring modules directly
        from src.hubspot_pipeline.hubspot_scoring.config import init_env as scoring_init_env
        from src.hubspot_pipeline.hubspot_scoring.main import process_snapshot_event
        
        # Initialize scoring environment
        scoring_init_env(log_level='INFO')
        
        # Create event data
        event_data = {
            "snapshot_id": snapshot_id,
            "data_tables": {"hs_companies": 10, "hs_deals": 5},
            "reference_tables": {"hs_owners": 3, "hs_deal_stage_reference": 8}
        }
        
        print("⚙️ Running scoring pipeline directly...")
        
        # Process the scoring event
        result = process_snapshot_event(event_data)
        
        print("-" * 50)
        if result.get('status') == 'success':
            print("✅ Direct scoring test completed successfully!")
            print(f"📊 Result: {result}")
        else:
            print(f"❌ Direct scoring test failed: {result}")
        
        return result
        
    except Exception as e:
        print("-" * 50)
        print(f"❌ Direct scoring test failed: {e}")
        logging.error(f"Direct scoring test failed: {e}", exc_info=True)
        return None

def clean_data_tables(table_type, env_info):
    """Clean specific data tables with confirmations"""
    table_sets = {
        'ingest': ['hs_companies', 'hs_deals', 'hs_owners', 'hs_deal_stage_reference', 'hs_snapshot_registry'],
        'scoring': ['hs_pipeline_units_snapshot', 'hs_pipeline_score_history', 'hs_stage_mapping'],
        'all': ['hs_companies', 'hs_deals', 'hs_owners', 'hs_deal_stage_reference', 'hs_snapshot_registry',
                'hs_pipeline_units_snapshot', 'hs_pipeline_score_history', 'hs_stage_mapping']
    }
    
    tables_to_delete = table_sets.get(table_type, [])
    action_desc = f"DELETE {len(tables_to_delete)} {table_type} tables: {', '.join(tables_to_delete)}"
    
    if not confirm_environment_action(env_info, action_desc):
        return False
    
    print(f"\n🗑️ Cleaning {table_type} data tables...")
    
    try:
        from google.cloud import bigquery
        
        client = bigquery.Client()
        dataset = env_info['dataset']
        project = env_info['project']
        
        deleted_count = 0
        for table_name in tables_to_delete:
            full_table = f"{project}.{dataset}.{table_name}"
            try:
                client.delete_table(full_table, not_found_ok=True)
                print(f"✅ Deleted: {table_name}")
                deleted_count += 1
            except Exception as e:
                print(f"⚠️ Failed to delete {table_name}: {e}")
        
        print(f"\n✅ Cleanup completed: {deleted_count}/{len(tables_to_delete)} tables deleted")
        return True
        
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False

def get_latest_snapshot_id():
    """Get the latest snapshot ID from the registry"""
    try:
        from google.cloud import bigquery
        import os
        
        client = bigquery.Client()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        query = f"""
        SELECT snapshot_id
        FROM `{project_id}.{dataset_id}.hs_snapshot_registry`
        WHERE status LIKE '%ingest%'
        ORDER BY snapshot_timestamp DESC
        LIMIT 1
        """
        
        result = client.query(query).result()
        latest = next(result, None)
        
        return latest.snapshot_id if latest else None
        
    except Exception as e:
        print(f"❌ Failed to get latest snapshot: {e}")
        return None

def view_recent_snapshots(env_info):
    """View recent snapshots from registry"""
    print("\n📋 Recent Snapshots")
    print("-" * 80)
    
    try:
        from google.cloud import bigquery
        
        client = bigquery.Client()
        project = env_info['project']
        dataset = env_info['dataset']
        
        query = f"""
        SELECT 
            snapshot_id,
            snapshot_timestamp,
            triggered_by,
            status,
            notes
        FROM `{project}.{dataset}.hs_snapshot_registry`
        ORDER BY snapshot_timestamp DESC
        LIMIT 10
        """
        
        results = client.query(query).result()
        
        for i, row in enumerate(results, 1):
            print(f"\n{i}. 📸 {row.snapshot_id}")
            print(f"   🕐 {row.snapshot_timestamp}")
            print(f"   🎯 Triggered by: {row.triggered_by}")
            print(f"   📊 Status: {row.status}")
            print(f"   📝 Notes: {row.notes}")
            print("-" * 80)
            
    except Exception as e:
        print(f"❌ Failed to get snapshots: {e}")

def get_custom_ingest_parameters():
    """Get custom parameters for ingest testing"""
    print("\n🔧 Custom Ingest Parameters")
    print("-" * 40)
    
    try:
        limit = input("📊 Record limit (or 'none' for no limit): ").strip()
        if limit.lower() == 'none':
            limit = None
        else:
            limit = int(limit)
        
        dry_run = input("🛑 Dry run? (y/n): ").strip().lower() == 'y'
        
        log_level = input("📝 Log level (DEBUG/INFO/WARN): ").strip().upper()
        if log_level not in ['DEBUG', 'INFO', 'WARN']:
            log_level = 'INFO'
        
        trigger_source = input("🎯 Trigger source (default: custom_test): ").strip()
        if not trigger_source:
            trigger_source = "custom_test"
        
        return {
            "limit": limit,
            "dry_run": dry_run,
            "log_level": log_level,
            "trigger_source": trigger_source
        }
        
    except ValueError:
        print("❌ Invalid input. Using defaults.")
        return {"limit": 10, "dry_run": True, "log_level": "INFO", "trigger_source": "custom_test"}
    except KeyboardInterrupt:
        print("\n❌ Cancelled. Using defaults.")
        return {"limit": 10, "dry_run": True, "log_level": "INFO", "trigger_source": "custom_test"}

def run_flask_simulation():
    """Simulate Flask/Cloud Function request for ingest"""
    print("\n🌐 Flask Mode - Ingest Cloud Function Simulation")
    
    event_data = {"flask_mode": True, "limit": 5, "dry_run": True}
    
    builder = EnvironBuilder(method='POST', json=event_data)
    env = builder.get_environ()
    request = Request(WerkzeugRequest(env))
    
    try:
        response = ingest_cloud_main(request)
        print("📤 Ingest Cloud Function Response:")
        print(response[0])
        return response
    except Exception as e:
        print(f"❌ Flask simulation failed: {e}")
        logging.error(f"Flask execution failed: {e}", exc_info=True)
        return None

def main():
    """Main interactive menu loop with environment awareness"""
    print("🧪 HubSpot Pipeline Testing & Debugging Tool")
    print("This tool helps you test ingest and scoring functions with environment safety")
    
    # Get environment info
    env_info = get_environment_info()
    
    while True:
        show_environment_warning(env_info)
        show_main_menu(env_info)
        choice = get_user_choice()
        
        if choice == '0':
            print("\n👋 Goodbye!")
            break
            
        elif choice == '1':
            # Ingest dry run
            event_data = {
                "limit": 5,
                "dry_run": True,
                "log_level": "INFO",
                "trigger_source": "test_dry_run"
            }
            run_ingest_test(event_data, env_info)
            
        elif choice == '2':
            # Small live ingest
            event_data = {
                "limit": 10,
                "dry_run": False,
                "log_level": "INFO",
                "trigger_source": "test_small_live"
            }
            run_ingest_test(event_data, env_info)
            
        elif choice == '3':
            # Medium ingest
            event_data = {
                "limit": 50,
                "dry_run": False,
                "log_level": "INFO",
                "trigger_source": "test_medium"
            }
            run_ingest_test(event_data, env_info)
            
        elif choice == '4':
            # Full ingest
            event_data = {
                "no_limit": True,
                "dry_run": False,
                "log_level": "INFO",
                "trigger_source": "test_full"
            }
            run_ingest_test(event_data, env_info)
            
        elif choice == '5':
            # Custom ingest
            event_data = get_custom_ingest_parameters()
            run_ingest_test(event_data, env_info)
            
        elif choice == '6':
            # Clean ingest data
            clean_data_tables('ingest', env_info)
            
        elif choice == '7':
            # Score latest snapshot (Cloud Function)
            run_scoring_test(None, env_info)
            
        elif choice == '8':
            # Score specific snapshot (Cloud Function)
            snapshot_id = input("\n📸 Enter snapshot ID: ").strip()
            if snapshot_id:
                run_scoring_test(snapshot_id, env_info)
            else:
                print("❌ No snapshot ID provided")
                
        elif choice == '9':
            # Direct score latest (modules)
            run_direct_scoring_test(None, env_info)
            
        elif choice == '10':
            # Direct score specific (modules)
            snapshot_id = input("\n📸 Enter snapshot ID: ").strip()
            if snapshot_id:
                run_direct_scoring_test(snapshot_id, env_info)
            else:
                print("❌ No snapshot ID provided")
                
        elif choice == '11':
            # Populate stage mapping
            action_desc = "Populate stage mapping reference data"
            if confirm_environment_action(env_info, action_desc):
                try:
                    from src.hubspot_pipeline.hubspot_scoring.config import init_env as scoring_init_env
                    from src.hubspot_pipeline.hubspot_scoring.stage_mapping import populate_stage_mapping
                    
                    scoring_init_env()
                    mapping_count = populate_stage_mapping()
                    print(f"✅ Stage mapping populated successfully: {mapping_count} records")
                except Exception as e:
                    print(f"❌ Failed to populate stage mapping: {e}")
            
        elif choice == '12':
            # Clean scoring data
            clean_data_tables('scoring', env_info)
            
        elif choice == '13':
            # View snapshots
            view_recent_snapshots(env_info)
            
        elif choice == '14':
            # Flask mode - test ingest function
            run_flask_simulation()
            
        elif choice == '15':
            # Debug mode
            event_data = {
                "limit": 3,
                "dry_run": True,
                "log_level": "DEBUG",
                "trigger_source": "test_debug"
            }
            run_ingest_test(event_data, env_info)
            
        elif choice == '16':
            # Clean all data
            clean_data_tables('all', env_info)
        
        # Ask if user wants to continue
        if choice != '0':
            input("\n⏸️ Press Enter to return to menu...")

if __name__ == "__main__":
    # Check for CLI arguments for backward compatibility
    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser()
        parser.add_argument("--mode", choices=["flask", "cli"], default="cli")
        args = parser.parse_args()
        
        print("🚀 Running in legacy CLI mode")
        if args.mode == "flask":
            run_flask_simulation()
        else:
            # Simple ingest test run
            env_info = get_environment_info()
            event_data = {"limit": 5, "dry_run": True, "log_level": "INFO", "trigger_source": "legacy_cli"}
            run_ingest_test(event_data, env_info)
    else:
        # Run interactive menu
        main()