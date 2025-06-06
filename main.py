import sys
import argparse
import logging
from flask import Request
from werkzeug.test import EnvironBuilder
from werkzeug.wrappers import Request as WerkzeugRequest
from hubspot_pipeline.hubspot_ingest.store import store_to_bigquery, upsert_to_bigquery, publish_snapshot_completed_event, register_snapshot_ingest


# Configure logging FIRST, before any other imports
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    force=True  # Override any existing configuration
)

from src.main import main as cloud_main

def show_menu():
    """Display interactive test menu"""
    print("\n" + "="*60)
    print("🚀 HubSpot Pipeline Test Menu")
    print("="*60)
    print("1. 🧪 Dry Run (5 records) - Safe test with no BigQuery writes")
    print("2. 🔬 Small Live Run (10 records) - Minimal live test")
    print("3. 📊 Medium Run (50 records) - Standard test")
    print("4. 🚀 Full Run (no limit) - Production-like test")
    print("5. 🔧 Custom Run - Specify your own parameters")
    print("6. 🌐 Flask Mode - Simulate Cloud Function HTTP request")
    print("7. ⚙️  CLI Mode - Direct function call")
    print("8. 📋 Debug Mode - Verbose logging")
    print("9. 📝 Check Registry - View recent snapshot entries")
    print("10. 🎯 Test Scoring Only - Mock scoring function")
    print("0. ❌ Exit")
    print("="*60)

def get_user_choice():
    """Get and validate user menu choice"""
    while True:
        try:
            choice = input("\n🔹 Enter your choice (0-10): ").strip()
            if choice in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']:
                return choice
            else:
                print("❌ Invalid choice. Please enter a number between 0-10.")
        except KeyboardInterrupt:
            print("\n👋 Goodbye!")
            sys.exit(0)
        except EOFError:
            print("\n👋 Goodbye!")
            sys.exit(0)

def run_ingest_test(event_data):
    """Run ingest with specified parameters"""
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
        print("✅ Test completed successfully!")
        print(f"📤 Result: {result}")
        
        return result
        
    except Exception as e:
        print("-" * 50)
        print(f"❌ Test failed: {e}")
        logging.error(f"Test execution failed: {e}", exc_info=True)
        return None

def run_as_flask(event_data=None):
    """Simulate a Flask/Cloud Function request locally"""
    print("\n🌐 Running in Flask mode (simulating Cloud Function HTTP request)")
    
    if event_data is None:
        event_data = {"flask_mode": True, "limit": 5, "dry_run": True}
    
    builder = EnvironBuilder(method='POST', json=event_data)
    env = builder.get_environ()
    request = Request(WerkzeugRequest(env))
    
    try:
        response = cloud_main(request)
        print("📤 Response:")
        print(response[0])
        return response
    except Exception as e:
        print(f"❌ Flask mode failed: {e}")
        logging.error(f"Flask execution failed: {e}", exc_info=True)
        return None

def check_registry():
    """Check recent snapshot registry entries"""
    print("\n📋 Checking recent snapshot registry entries...")
    
    try:
        from google.cloud import bigquery
        import os
        
        # Initialize environment to get config
        from src.hubspot_pipeline.hubspot_ingest.config_loader import init_env
        init_env()
        
        client = bigquery.Client()
        project_id = os.getenv('BIGQUERY_PROJECT_ID')
        dataset_id = os.getenv('BIGQUERY_DATASET_ID')
        
        query = f"""
        SELECT 
            snapshot_id,
            snapshot_timestamp,
            triggered_by,
            status,
            notes
        FROM `{project_id}.{dataset_id}.hs_snapshot_registry`
        ORDER BY snapshot_timestamp DESC
        LIMIT 5
        """
        
        results = client.query(query).result()
        
        print("\n📊 Recent Snapshot Entries:")
        print("-" * 80)
        for row in results:
            print(f"🕐 {row.snapshot_timestamp}")
            print(f"   ID: {row.snapshot_id}")
            print(f"   Status: {row.status}")
            print(f"   Triggered by: {row.triggered_by}")
            print(f"   Notes: {row.notes}")
            print("-" * 80)
            
    except Exception as e:
        print(f"❌ Failed to check registry: {e}")
        logging.error(f"Registry check failed: {e}", exc_info=True)

def test_scoring_only():
    """Test scoring function with mock event"""
    print("\n🎯 Testing scoring function with mock event...")
    
    try:
        # Create mock event
        import json
        import base64
        
        mock_event_data = {
            "type": "hubspot.snapshot.completed",
            "data": {
                "snapshot_id": "2025-06-06T15:30:00",
                "data_tables": {"hs_companies": 10, "hs_deals": 5},
                "reference_tables": {"hs_owners": 3, "hs_deal_stage_reference": 8}
            }
        }
        
        mock_pubsub_event = {
            'data': base64.b64encode(json.dumps(mock_event_data).encode('utf-8'))
        }
        
        # Import and run scoring function
        # Note: This assumes scoring function is available locally
        print("📊 Mock scoring event created:")
        print(f"   Snapshot ID: {mock_event_data['data']['snapshot_id']}")
        print(f"   Data tables: {mock_event_data['data']['data_tables']}")
        print(f"   Reference tables: {mock_event_data['data']['reference_tables']}")
        print("\n⚠️ To fully test scoring, deploy the scoring function and trigger via Pub/Sub")
        
    except Exception as e:
        print(f"❌ Scoring test failed: {e}")

def get_custom_parameters():
    """Get custom parameters from user"""
    print("\n🔧 Custom Run - Enter your parameters:")
    
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
        
        trigger_source = input("🎯 Trigger source (default: manual_test): ").strip()
        if not trigger_source:
            trigger_source = "manual_test"
        
        return {
            "limit": limit,
            "dry_run": dry_run,
            "log_level": log_level,
            "trigger_source": trigger_source
        }
        
    except ValueError:
        print("❌ Invalid input. Using defaults.")
        return {"limit": 10, "dry_run": True, "log_level": "INFO", "trigger_source": "manual_test"}
    except KeyboardInterrupt:
        print("\n❌ Cancelled. Using defaults.")
        return {"limit": 10, "dry_run": True, "log_level": "INFO", "trigger_source": "manual_test"}

def main():
    """Main interactive menu loop"""
    print("🚀 HubSpot Pipeline Testing Tool")
    print("This tool helps you test your ingest and scoring functions locally")
    
    while True:
        show_menu()
        choice = get_user_choice()
        
        if choice == '0':
            print("\n👋 Goodbye!")
            break
            
        elif choice == '1':
            # Dry run (5 records)
            event_data = {
                "limit": 5,
                "dry_run": True,
                "log_level": "INFO",
                "trigger_source": "test_menu_dry_run"
            }
            run_ingest_test(event_data)
            
        elif choice == '2':
            # Small live run (10 records)
            event_data = {
                "limit": 10,
                "dry_run": False,
                "log_level": "INFO",
                "trigger_source": "test_menu_small_live"
            }
            confirm = input("\n⚠️ This will write to BigQuery. Continue? (y/n): ")
            if confirm.lower() == 'y':
                run_ingest_test(event_data)
            else:
                print("❌ Cancelled.")
                
        elif choice == '3':
            # Medium run (50 records)
            event_data = {
                "limit": 50,
                "dry_run": False,
                "log_level": "INFO",
                "trigger_source": "test_menu_medium"
            }
            confirm = input("\n⚠️ This will write 50 records to BigQuery. Continue? (y/n): ")
            if confirm.lower() == 'y':
                run_ingest_test(event_data)
            else:
                print("❌ Cancelled.")
                
        elif choice == '4':
            # Full run (no limit)
            event_data = {
                "no_limit": True,
                "dry_run": False,
                "log_level": "INFO",
                "trigger_source": "test_menu_full"
            }
            confirm = input("\n⚠️ This will fetch ALL records! Continue? (y/n): ")
            if confirm.lower() == 'y':
                run_ingest_test(event_data)
            else:
                print("❌ Cancelled.")
                
        elif choice == '5':
            # Custom run
            event_data = get_custom_parameters()
            if not event_data["dry_run"]:
                confirm = input(f"\n⚠️ This will write to BigQuery. Continue? (y/n): ")
                if confirm.lower() != 'y':
                    print("❌ Cancelled.")
                    continue
            run_ingest_test(event_data)
            
        elif choice == '6':
            # Flask mode
            run_as_flask()
            
        elif choice == '7':
            # CLI mode (same as option 1)
            event_data = {
                "limit": 5,
                "dry_run": True,
                "log_level": "INFO",
                "trigger_source": "test_menu_cli"
            }
            run_ingest_test(event_data)
            
        elif choice == '8':
            # Debug mode
            event_data = {
                "limit": 3,
                "dry_run": True,
                "log_level": "DEBUG",
                "trigger_source": "test_menu_debug"
            }
            run_ingest_test(event_data)
            
        elif choice == '9':
            # Check registry
            check_registry()
            
        elif choice == '10':
            # Test scoring only
            test_scoring_only()
        
        # Ask if user wants to continue
        if choice != '0':
            input("\n⏸️ Press Enter to return to menu...")

def run_as_cli():
    """Legacy CLI mode for backward compatibility"""
    print("🚀 Running in legacy CLI mode")
    event_data = {
        "limit": 5,
        "dry_run": True,
        "log_level": "INFO",
        "trigger_source": "legacy_cli"
    }
    return run_ingest_test(event_data)

if __name__ == "__main__":
    # Check if running with old arguments for backward compatibility
    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser()
        parser.add_argument("--mode", choices=["flask", "cli"], default="cli", help="Execution mode: flask or cli (default)")
        args = parser.parse_args()
        
        if args.mode == "flask":
            run_as_flask()
        else:
            run_as_cli()
    else:
        # Run interactive menu
        main()