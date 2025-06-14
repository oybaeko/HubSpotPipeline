#!/usr/bin/env python3
"""
Main Migration Orchestrator
Orchestrates the complete staging environment setup and data migration from production
"""

import sys
import os
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Clear service account credentials to use user auth
if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
    del os.environ['GOOGLE_APPLICATION_CREDENTIALS']

try:
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    BIGQUERY_AVAILABLE = True
except ImportError:
    print("❌ BigQuery not available. Install: pip install google-cloud-bigquery")
    BIGQUERY_AVAILABLE = False

class MainMigrationOrchestrator:
    """Orchestrates the complete staging setup and data migration"""
    
    def __init__(self):
        self.project_id = "hubspot-452402"
        self.staging_dataset = "Hubspot_staging"
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        self.logger = logging.getLogger('migration.orchestrator')
        
        # Initialize BigQuery client
        if BIGQUERY_AVAILABLE:
            self.client = bigquery.Client(project=self.project_id)
        else:
            self.client = None
            
        # Get authentication info
        self.auth_info = self._get_auth_info()
            
        # Migration steps status
        self.steps_completed = {
            'clean_dataset': False,
            'create_tables': False,
            'clear_data': False,
            'migrate_data': False
        }
    
    def _get_auth_info(self) -> Dict:
        """Get current authentication information"""
        auth_info = {
            'bigquery_user': 'Unknown',
            'gcloud_user': 'Unknown',
            'current_env': 'Unknown'
        }
        
        try:
            # Get gcloud user
            import subprocess
            result = subprocess.run("gcloud config get-value account", 
                                  shell=True, capture_output=True, text=True, check=False)
            if result.returncode == 0:
                auth_info['gcloud_user'] = result.stdout.strip()
            
            # Get BigQuery client credentials info
            from google.auth import default
            credentials, project = default()
            
            # Try to get user email from credentials
            if hasattr(credentials, 'service_account_email'):
                auth_info['bigquery_user'] = credentials.service_account_email
            else:
                auth_info['bigquery_user'] = auth_info['gcloud_user']
            
            # Determine current environment based on which dataset we're targeting
            auth_info['current_env'] = 'staging'
            
        except Exception as e:
            auth_info['error'] = str(e)
        
        return auth_info
    
    def _log_step_start(self, step_num: int, step_name: str):
        """Log step start with authentication and environment info"""
        self.logger.info(f"🎯 Running Step {step_num}: {step_name}")
        self.logger.info(f"👤 User: {self.auth_info.get('bigquery_user', 'Unknown')}")
        self.logger.info(f"🌍 Target Environment: staging ({self.staging_dataset})")
        self.logger.info(f"📂 Target Dataset: {self.project_id}.{self.staging_dataset}")
    
    def run_command(self, cmd: str, description: str = "") -> bool:
        """Run shell command and return success status"""
        if description:
            self.logger.info(f"🔧 {description}")
        
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
            if result.stdout.strip():
                self.logger.debug(f"Output: {result.stdout.strip()}")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ Command failed: {cmd}")
            if e.stdout:
                self.logger.error(f"Stdout: {e.stdout}")
            if e.stderr:
                self.logger.error(f"Stderr: {e.stderr}")
            return False
    
    def step_1_clean_staging_dataset(self) -> bool:
        """Step 1: Clean staging dataset - remove all tables"""
        self.logger.info("🗑️  STEP 1: Cleaning staging dataset")
        
        try:
            # List all tables in staging dataset
            dataset_ref = self.client.dataset(self.staging_dataset)
            tables = list(self.client.list_tables(dataset_ref))
            
            if not tables:
                self.logger.info("✅ Staging dataset is already clean (no tables)")
                self.steps_completed['clean_dataset'] = True
                return True
            
            self.logger.info(f"📋 Found {len(tables)} tables to delete:")
            for table in tables:
                self.logger.info(f"  • {table.table_id}")
            
            # Confirm deletion
            print(f"\n⚠️  This will DELETE {len(tables)} tables from staging!")
            confirm = input("Type 'DELETE TABLES' to confirm: ").strip()
            
            if confirm != 'DELETE TABLES':
                self.logger.info("❌ Operation cancelled")
                return False
            
            # Delete all tables
            deleted_count = 0
            for table in tables:
                try:
                    self.client.delete_table(table, not_found_ok=True)
                    self.logger.info(f"🗑️  Deleted {table.table_id}")
                    deleted_count += 1
                except Exception as e:
                    self.logger.error(f"❌ Failed to delete {table.table_id}: {e}")
            
            self.logger.info(f"✅ Deleted {deleted_count}/{len(tables)} tables")
            self.steps_completed['clean_dataset'] = deleted_count == len(tables)
            return self.steps_completed['clean_dataset']
            
        except NotFound:
            self.logger.warning(f"⚠️  Dataset {self.staging_dataset} does not exist")
            return False
        except Exception as e:
            self.logger.error(f"❌ Failed to clean dataset: {e}")
            return False
    
    def step_2_create_tables_via_e2e_test(self) -> bool:
        """Step 2: Create tables by calling staging cloud function E2E test"""
        self.logger.info("🏗️  STEP 2: Creating tables via staging cloud function")
        self.logger.info("🎯 Calling staging E2E test to create table structure")
        
        try:
            import subprocess
            import json
            import tempfile
            
            # Staging cloud function URL
            staging_url = "https://europe-west1-hubspot-452402.cloudfunctions.net/hubspot-ingest-staging"
            
            # Create payload for E2E test with small limit
            payload = {
                "mode": "test",
                "test_type": "integration", 
                "record_limit": 1,  # Very small limit just for table creation
                "trigger_source": "migration_orchestrator_table_creation"
            }
            
            # Create temporary files for response and error output
            with tempfile.NamedTemporaryFile(mode='w+', suffix='.json', delete=False) as temp_file:
                temp_file_path = temp_file.name
            
            with tempfile.NamedTemporaryFile(mode='w+', suffix='.txt', delete=False) as error_file:
                error_file_path = error_file.name
            
            self.logger.info(f"🚀 Calling staging cloud function...")
            self.logger.info(f"📡 URL: {staging_url}")
            self.logger.info(f"📦 Payload: {json.dumps(payload, indent=2)}")
            
            # First, let's try a simple health check
            self.logger.info("🏥 Testing basic connectivity...")
            ping_payload = {"log_level": "INFO"}
            
            ping_cmd = [
                'curl', '-X', 'POST', staging_url,
                '-H', 'Content-Type: application/json',
                '-d', json.dumps(ping_payload),
                '-w', 'HTTP:%{http_code} Time:%{time_total}s',
                '--max-time', '30',
                '--connect-timeout', '10'
            ]
            
            ping_result = subprocess.run(ping_cmd, capture_output=True, text=True, timeout=60)
            self.logger.info(f"🏥 Ping result: {ping_result.stdout}")
            if ping_result.stderr:
                self.logger.warning(f"🏥 Ping stderr: {ping_result.stderr}")
            
            # Now try the actual E2E test with more debugging
            self.logger.info("🔬 Running E2E test with full debugging...")
            
            # Execute curl command with verbose output
            curl_cmd = [
                'curl', '-X', 'POST', staging_url,
                '-H', 'Content-Type: application/json',
                '-d', json.dumps(payload),
                '-o', temp_file_path,
                '-w', 'HTTP:%{http_code} Time:%{time_total}s Size:%{size_download}bytes',
                '--max-time', '300',  # 5 minute timeout
                '--connect-timeout', '30',
                '-v'  # Verbose output for debugging
            ]
            
            self.logger.info(f"🔧 Full curl command: {' '.join(curl_cmd)}")
            
            result = subprocess.run(curl_cmd, capture_output=True, text=True, timeout=320)
            
            self.logger.info(f"📊 Curl output: {result.stdout}")
            if result.stderr:
                self.logger.info(f"📊 Curl verbose: {result.stderr}")
            
            # Parse the metrics from stdout
            metrics = result.stdout.strip()
            self.logger.info(f"📈 Response metrics: {metrics}")
            
            # Read response content
            response_content = ""
            response_size = 0
            try:
                with open(temp_file_path, 'r') as f:
                    response_content = f.read()
                response_size = len(response_content)
                self.logger.info(f"📄 Response size: {response_size} characters")
                self.logger.info(f"📄 Response preview: {response_content[:500]}...")
                
                os.unlink(temp_file_path)  # Clean up temp file
            except Exception as e:
                self.logger.error(f"❌ Failed to read response file: {e}")
                return False
            
            # Try to parse as JSON
            try:
                if response_content.strip():
                    response_data = json.loads(response_content)
                    self.logger.info(f"✅ Successfully parsed JSON response")
                    self.logger.info(f"📋 Response keys: {list(response_data.keys())}")
                else:
                    self.logger.error("❌ Empty response content")
                    return False
            except json.JSONDecodeError as e:
                self.logger.error(f"❌ Failed to parse JSON response: {e}")
                self.logger.error(f"📄 Raw response: {response_content}")
                return False
            
            # Check HTTP status from metrics
            if 'HTTP:200' in metrics:
                self.logger.info("✅ HTTP 200 - Success")
            elif 'HTTP:206' in metrics:
                self.logger.info("✅ HTTP 206 - Partial Success")
            else:
                self.logger.error(f"❌ Non-success HTTP status in: {metrics}")
                self.logger.error(f"📄 Response: {response_data}")
                return False
            
            # Analyze response structure
            if response_data.get('test_mode') == True:
                self.logger.info("🧪 Detected test framework response")
                return self._handle_test_framework_response(response_data)
            elif 'status' in response_data:
                self.logger.info("⚙️ Detected functional pipeline response")
                return self._handle_functional_response(response_data)
            else:
                self.logger.error(f"❌ Unknown response format: {response_data}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("❌ Cloud function call timed out (5+ minutes)")
            return False
        except Exception as e:
            self.logger.error(f"❌ Failed to call cloud function: {e}")
            import traceback
            self.logger.debug(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _handle_test_framework_response(self, response_data: Dict) -> bool:
        """Handle test framework response"""
        status = response_data.get('status', 'unknown')
        summary = response_data.get('summary', {})
        
        self.logger.info(f"🧪 Test framework status: {status}")
        self.logger.info(f"📊 Test summary: {summary}")
        
        if status in ['success', 'partial_success']:
            passed = summary.get('passed', 0)
            total = summary.get('total', 0)
            self.logger.info(f"✅ E2E test passed: {passed}/{total} tests")
            
            # Verify tables were created in staging
            if self._verify_staging_tables():
                self.steps_completed['create_tables'] = True
                return True
            else:
                self.logger.warning("⚠️ Test passed but no tables found in staging")
                return False
        else:
            error = response_data.get('error', 'Unknown error')
            self.logger.error(f"❌ E2E test failed: {error}")
            
            # Show failed tests
            failed_tests = response_data.get('details', {}).get('failed_tests', [])
            for test in failed_tests:
                self.logger.error(f"  • {test.get('name', 'Unknown')}: {test.get('error', 'No details')}")
            
            return False
    
    def _handle_functional_response(self, response_data: Dict) -> bool:
        """Handle functional pipeline response"""
        status = response_data.get('status', 'unknown')
        
        self.logger.info(f"⚙️ Pipeline status: {status}")
        
        if status == 'success':
            total_records = response_data.get('total_records', 0)
            snapshot_id = response_data.get('snapshot_id', 'N/A')
            results = response_data.get('results', {})
            
            self.logger.info(f"✅ Pipeline completed: {total_records} records, snapshot: {snapshot_id}")
            self.logger.info(f"📊 Results breakdown: {results}")
            
            # Verify tables were created in staging
            if self._verify_staging_tables():
                self.steps_completed['create_tables'] = True
                return True
            else:
                self.logger.warning("⚠️ Pipeline succeeded but no tables found in staging")
                return False
        else:
            error = response_data.get('error', 'Unknown error')
            self.logger.error(f"❌ Pipeline failed: {error}")
            return False
    
    def _verify_staging_tables(self):
        """Verify that tables were created in staging dataset"""
        try:
            dataset_ref = self.client.dataset(self.staging_dataset)
            tables = list(self.client.list_tables(dataset_ref))
            
            if tables:
                self.logger.info(f"✅ Verified: {len(tables)} tables created in staging:")
                for table in tables:
                    self.logger.info(f"  • {table.table_id}")
                return True
            else:
                self.logger.warning("⚠️  No tables found in staging after pipeline execution")
                return False
                
        except Exception as e:
            self.logger.warning(f"⚠️  Could not verify staging tables: {e}")
            return False
    
    def step_3_clear_all_data(self) -> bool:
        """Step 3: Delete all rows in all tables (skip views)"""
        self.logger.info("🧹 STEP 3: Clearing all data from tables")
        
        try:
            # List tables
            dataset_ref = self.client.dataset(self.staging_dataset)
            all_objects = list(self.client.list_tables(dataset_ref))
            
            # Filter to only tables (not views)
            tables = [obj for obj in all_objects if obj.table_type == 'TABLE']
            views = [obj for obj in all_objects if obj.table_type == 'VIEW']
            
            if not tables:
                self.logger.warning("⚠️  No tables found to clear")
                return False
            
            self.logger.info(f"📋 Found {len(tables)} tables and {len(views)} views")
            self.logger.info(f"🧹 Clearing data from {len(tables)} tables (skipping {len(views)} views):")
            
            # Log what we're skipping
            if views:
                self.logger.info(f"⏭️  Skipping views (cannot be truncated):")
                for view in views:
                    self.logger.info(f"  • {view.table_id} (VIEW)")
            
            cleared_count = 0
            for table in tables:
                try:
                    # TRUNCATE is faster than DELETE for clearing all data
                    table_ref = f"{self.project_id}.{self.staging_dataset}.{table.table_id}"
                    truncate_query = f"TRUNCATE TABLE `{table_ref}`"
                    
                    job = self.client.query(truncate_query)
                    job.result()  # Wait for completion
                    
                    self.logger.info(f"🧹 Cleared {table.table_id}")
                    cleared_count += 1
                    
                except Exception as e:
                    self.logger.error(f"❌ Failed to clear {table.table_id}: {e}")
            
            self.logger.info(f"✅ Cleared {cleared_count}/{len(tables)} tables")
            self.steps_completed['clear_data'] = cleared_count == len(tables)
            return self.steps_completed['clear_data']
            
        except Exception as e:
            self.logger.error(f"❌ Failed to clear data: {e}")
            return False
    
    def step_4_migrate_prod_to_staging(self) -> bool:
        """Step 4: Migrate data from prod to staging"""
        self.logger.info("🚀 STEP 4: Migrating data from production to staging")
        
        try:
            # Import the migration manager
            from data_migration_script import DataMigrationManager
            
            # Initialize migration manager
            migration_manager = DataMigrationManager()
            
            # Run full migration pipeline (live mode)
            self.logger.info("🎯 Running full migration pipeline...")
            success = migration_manager.full_migration_pipeline(dry_run=False)
            
            if success:
                self.logger.info("✅ Production data migrated successfully")
                self.steps_completed['migrate_data'] = True
                return True
            else:
                self.logger.error("❌ Migration failed")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Failed to migrate data: {e}")
            import traceback
            self.logger.debug(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def run_single_step(self, step_number: int) -> bool:
        """Run a single step"""
        steps = {
            1: ("Clean staging dataset", self.step_1_clean_staging_dataset),
            2: ("Create tables via staging cloud function", self.step_2_create_tables_via_e2e_test),
            3: ("Clear all data", self.step_3_clear_all_data),
            4: ("Migrate prod to staging", self.step_4_migrate_prod_to_staging)
        }
        
        if step_number not in steps:
            self.logger.error(f"❌ Invalid step number: {step_number}")
            return False
        
        step_name, step_function = steps[step_number]
        
        # Log step start with auth/env info
        self._log_step_start(step_number, step_name)
        start_time = datetime.now()
        
        success = step_function()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        if success:
            self.logger.info(f"✅ Step {step_number} completed successfully in {duration:.1f}s")
        else:
            self.logger.error(f"❌ Step {step_number} failed after {duration:.1f}s")
        
        return success
    
    def run_all_steps(self) -> bool:
        """Run all migration steps in sequence"""
        self.logger.info("🚀 FULL MIGRATION: Running all steps")
        
        steps = [1, 2, 3, 4]
        
        for step_num in steps:
            if not self.run_single_step(step_num):
                self.logger.error(f"❌ Migration failed at step {step_num}")
                return False
            
            # Brief pause between steps
            import time
            time.sleep(1)
        
        self.logger.info("🎉 FULL MIGRATION COMPLETED SUCCESSFULLY!")
        self.show_final_summary()
        return True
    
    def show_status(self) -> None:
        """Show current migration status"""
        print(f"\n📊 MIGRATION STATUS")
        print("=" * 50)
        
        steps = [
            ("1. Clean staging dataset", self.steps_completed['clean_dataset']),
            ("2. Create tables via staging cloud function", self.steps_completed['create_tables']),
            ("3. Clear all data", self.steps_completed['clear_data']),
            ("4. Migrate prod to staging", self.steps_completed['migrate_data'])
        ]
        
        for step_name, completed in steps:
            status = "✅" if completed else "⏸️"
            print(f"  {status} {step_name}")
        
        completed_count = sum(self.steps_completed.values())
        print(f"\nProgress: {completed_count}/4 steps completed")
    
    def show_final_summary(self) -> None:
        """Show final migration summary"""
        print(f"\n🎉 MIGRATION SUMMARY")
        print("=" * 50)
        
        try:
            # Get final data counts
            dataset_ref = self.client.dataset(self.staging_dataset)
            tables = list(self.client.list_tables(dataset_ref))
            
            print(f"📊 Staging environment ready:")
            print(f"  • Dataset: {self.staging_dataset}")
            print(f"  • Tables: {len(tables)}")
            
            # Count records in main tables
            main_tables = ['hs_companies', 'hs_deals']
            for table_name in main_tables:
                try:
                    table_ref = f"{self.project_id}.{self.staging_dataset}.{table_name}"
                    count_query = f"SELECT COUNT(*) as count FROM `{table_ref}`"
                    result = self.client.query(count_query).result()
                    
                    for row in result:
                        print(f"  • {table_name}: {row.count:,} records")
                        break
                except:
                    print(f"  • {table_name}: Could not count")
                    
        except Exception as e:
            self.logger.debug(f"Could not generate summary: {e}")
        
        print(f"\n✅ Staging environment is ready for testing and development!")
    
    def interactive_menu(self):
        """Interactive menu for migration orchestration"""
        while True:
            print(f"\n{'='*60}")
            print(f"🚀 MAIN MIGRATION ORCHESTRATOR")
            print(f"{'='*60}")
            print(f"Project: {self.project_id}")
            print(f"Target: {self.staging_dataset}")
            
            # Display authentication info
            if self.auth_info:
                print(f"👤 User: {self.auth_info.get('bigquery_user', 'Unknown')}")
                print(f"🌍 Environment: {self.auth_info.get('current_env', 'Unknown')}")
            
            print(f"{'='*60}")
            
            self.show_status()
            
            print(f"\n📋 OPERATIONS")
            print(f"  1) 🗑️  Step 1: Clean staging dataset")
            print(f"  2) 🏗️  Step 2: Create tables via staging cloud function")
            print(f"  3) 🧹 Step 3: Clear all data")
            print(f"  4) 🚀 Step 4: Migrate prod to staging")
            print(f"  9) 🎯 Run ALL steps")
            print(f"  0) ❌ Exit")
            
            try:
                choice = input(f"\n🔹 Enter choice (0-4, 9): ").strip()
                
                if choice == '0':
                    print("\n👋 Goodbye!")
                    break
                elif choice in ['1', '2', '3', '4']:
                    self.run_single_step(int(choice))
                elif choice == '9':
                    print(f"\n🚨 FULL MIGRATION CONFIRMATION")
                    print(f"This will:")
                    print(f"  1. DELETE all tables in staging")
                    print(f"  2. Create new table structure")
                    print(f"  3. Clear any sample data")
                    print(f"  4. Migrate ALL production data")
                    
                    confirm = input(f"\nType 'RUN FULL MIGRATION' to continue: ").strip()
                    if confirm == 'RUN FULL MIGRATION':
                        self.run_all_steps()
                    else:
                        print("❌ Operation cancelled")
                else:
                    print("❌ Invalid choice. Please select 0-4 or 9.")
                    
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
            
            if choice != '0':
                input("\n⏸️  Press Enter to continue...")


def main():
    """Main entry point"""
    if not BIGQUERY_AVAILABLE:
        print("❌ BigQuery client library not available")
        sys.exit(1)
    
    orchestrator = MainMigrationOrchestrator()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'status':
            orchestrator.show_status()
        elif command in ['1', '2', '3', '4']:
            orchestrator.run_single_step(int(command))
        elif command == 'all':
            orchestrator.run_all_steps()
        else:
            print(f"Unknown command: {command}")
            print("Available commands: status, 1, 2, 3, 4, all")
    else:
        orchestrator.interactive_menu()


if __name__ == "__main__":
    main()