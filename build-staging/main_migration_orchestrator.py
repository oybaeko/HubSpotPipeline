#!/usr/bin/env python3
"""
Main Migration Orchestrator - UPDATED VERSION
Now uses external step files for better modularity
3-Step Process: Create Tables → Import Excel → Migrate Production → Rescore
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
    """Migration orchestration using external step modules"""
    
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
        
        # Setup paths for external steps
        self._setup_step_paths()
        
        # Migration steps status
        self.steps_completed = {
            'create_tables': False,
            'import_excel': False,
            'migrate_production': False,
            'rescore_all': False
        }
    
    def _setup_step_paths(self):
        """Setup paths for external step modules"""
        # Add steps directory to path
        steps_path = Path(__file__).parent / "steps"
        if str(steps_path) not in sys.path:
            sys.path.insert(0, str(steps_path))
        
        self.logger.debug(f"Added steps path: {steps_path}")
    
    def _get_auth_info(self) -> Dict:
        """Get current authentication information"""
        auth_info = {
            'bigquery_user': 'Unknown',
            'gcloud_user': 'Unknown',
            'current_env': 'staging'
        }
        
        try:
            # Get gcloud user
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
            
        except Exception as e:
            auth_info['error'] = str(e)
        
        return auth_info
    
    def _log_step_start(self, step_num: int, step_name: str):
        """Log step start with authentication and environment info"""
        self.logger.info(f"🎯 Running Step {step_num}: {step_name}")
        self.logger.info(f"👤 User: {self.auth_info.get('bigquery_user', 'Unknown')}")
        self.logger.info(f"🌍 Target Environment: staging ({self.staging_dataset})")
        self.logger.info(f"📂 Target Dataset: {self.project_id}.{self.staging_dataset}")
    
    def step_1_create_tables(self, recreate: bool = False, clear_all: bool = False) -> bool:
        """Step 1: Create tables using external step module"""
        self.logger.info("🏗️ STEP 1: Creating tables using external step module")
        
        try:
            # Import external step
            from steps.step1_table_creation import TableCreationStep
            
            # Create step instance
            step = TableCreationStep(self.project_id, self.staging_dataset)
            
            # Execute step
            self.logger.info("🚀 Executing table creation step...")
            success = step.execute(
                recreate=recreate, 
                interactive=False,  # Non-interactive for orchestration
                clear_all=clear_all
            )
            
            if success:
                results = step.get_results()
                self.logger.info("✅ Table creation completed successfully")
                self.logger.info(f"📊 Tables created: {results['results'].get('tables_created', 'Unknown')}")
                self.steps_completed['create_tables'] = True
                return True
            else:
                self.logger.error("❌ Table creation failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"❌ Failed to import table creation step: {e}")
            self.logger.error("💡 Check that steps/step1_table_creation.py exists")
            return False
        except Exception as e:
            self.logger.error(f"❌ Table creation step failed: {e}")
            return False
    
    def step_2_import_excel(self, excel_file: str = None, dry_run: bool = False) -> bool:
        """Step 2: Import Excel data using external step module"""
        self.logger.info("📥 STEP 2: Importing Excel data using external step module")
        
        try:
            # Import external step
            from steps.step2_import_excel import ExcelImportStep
            
            # Create step instance
            step = ExcelImportStep(self.project_id, self.staging_dataset)
            
            # Execute step
            self.logger.info("🚀 Executing Excel import step...")
            success = step.execute(excel_file=excel_file, dry_run=dry_run)
            
            if success:
                results = step.get_results()
                self.logger.info("✅ Excel import completed successfully")
                self.logger.info(f"📊 Records imported: {results['results'].get('total_records', 'Unknown')}")
                self.logger.info(f"📸 Snapshots: {results['results'].get('snapshots_imported', 'Unknown')}")
                self.steps_completed['import_excel'] = True
                return True
            else:
                self.logger.error("❌ Excel import failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"❌ Failed to import Excel import step: {e}")
            self.logger.error("💡 Check that steps/step2_import_excel.py exists")
            return False
        except Exception as e:
            self.logger.error(f"❌ Excel import step failed: {e}")
            return False
    
    def step_3_migrate_production(self, dry_run: bool = False) -> bool:
        """Step 3: Migrate production data (placeholder - to be implemented)"""
        self.logger.info("🚀 STEP 3: Migrating production data")
        self.logger.info("💡 This step will be implemented as step3_migrate_production.py")
        
        # TODO: Implement step3_migrate_production.py
        # For now, mark as completed for testing
        self.logger.info("⏭️ Step 3 placeholder - marking as completed")
        self.steps_completed['migrate_production'] = True
        return True
    
    def step_4_rescore_all_snapshots(self) -> bool:
        """Step 4: Rescore all snapshots using Cloud Function"""
        self.logger.info("🔄 STEP 4: Rescoring all snapshots via Cloud Function")
        self.logger.info("📊 This will trigger the scoring function to process ALL snapshots")
        self.logger.info("⚡ Uses Pub/Sub to call the deployed scoring Cloud Function")
        
        try:
            # Check if gcloud is available
            result = subprocess.run("gcloud --version", shell=True, capture_output=True, text=True, check=False)
            if result.returncode != 0:
                self.logger.error("❌ gcloud CLI not available")
                self.logger.error("💡 Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install")
                return False
            
            # Try to estimate snapshot count
            try:
                table_ref = f"{self.project_id}.{self.staging_dataset}.hs_companies"
                count_query = f"SELECT COUNT(DISTINCT snapshot_id) as snapshots FROM `{table_ref}`"
                result = self.client.query(count_query).result()
                
                for row in result:
                    snapshot_count = row.snapshots
                    estimated_time = snapshot_count * 35  # seconds
                    self.logger.info(f"📊 Estimated snapshots: {snapshot_count}")
                    self.logger.info(f"⏱️ Estimated duration: {estimated_time//60}m {estimated_time%60}s")
                    break
            except Exception as e:
                self.logger.debug(f"Could not estimate snapshot count: {e}")
                self.logger.info(f"📊 Snapshot count: Unknown")
            
            # Execute gcloud pubsub command
            self.logger.info("🚀 Triggering rescore-all via Pub/Sub...")
            
            pubsub_cmd = [
                "gcloud", "pubsub", "topics", "publish", "hubspot-events-staging",
                '--message={"type":"hubspot.rescore.all","data":{}}',
                "--project", self.project_id
            ]
            
            self.logger.info(f"📤 Publishing message to hubspot-events-staging topic...")
            result = subprocess.run(pubsub_cmd, capture_output=True, text=True, check=True)
            
            if result.returncode == 0:
                # Extract message ID from output
                output = result.stdout.strip()
                self.logger.info(f"✅ Pub/Sub message published successfully")
                if "messageIds:" in output:
                    # Extract message ID
                    lines = output.split('\n')
                    for line in lines:
                        if line.strip().startswith('- '):
                            message_id = line.strip()[2:].strip("'\"")
                            self.logger.info(f"📨 Message ID: {message_id}")
                            break
                
                self.logger.info("🔄 Rescore-all operation initiated")
                self.logger.info("📊 The scoring function is now processing all snapshots")
                
                # Show monitoring instructions
                self.logger.info("📊 Monitor progress in Google Cloud Console:")
                self.logger.info("   • Logging > Logs Explorer")
                self.logger.info("   • Query: resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"hubspot-scoring-staging\"")
                
                self.steps_completed['rescore_all'] = True
                return True
            else:
                self.logger.error(f"❌ Pub/Sub command failed")
                self.logger.error(f"Exit code: {result.returncode}")
                if result.stderr:
                    self.logger.error(f"Error: {result.stderr}")
                return False
                
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ gcloud command failed: {e}")
            if e.stderr:
                self.logger.error(f"Error output: {e.stderr}")
            if e.stdout:
                self.logger.error(f"Standard output: {e.stdout}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Failed to trigger rescore-all: {e}")
            return False   
    
    def run_single_step(self, step_number: int, **kwargs) -> bool:
        """Run a single step with optional parameters"""
        steps = {
            1: ("Create tables using external step", lambda: self.step_1_create_tables(**kwargs)),
            2: ("Import Excel data using external step", lambda: self.step_2_import_excel(**kwargs)),
            3: ("Migrate production data", lambda: self.step_3_migrate_production(**kwargs)),
            4: ("Rescore all snapshots via Cloud Function", lambda: self.step_4_rescore_all_snapshots())
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
    
    def run_all_steps(self, **kwargs) -> bool:
        """Run all migration steps in sequence"""
        self.logger.info("🚀 FULL MIGRATION: Running all steps with external modules")
        
        steps = [1, 2, 3, 4]
        
        for step_num in steps:
            # Extract step-specific kwargs
            step_kwargs = {}
            if step_num == 1:
                step_kwargs = {k: v for k, v in kwargs.items() if k in ['recreate', 'clear_all']}
            elif step_num == 2:
                step_kwargs = {k: v for k, v in kwargs.items() if k in ['excel_file', 'dry_run']}
            elif step_num == 3:
                step_kwargs = {k: v for k, v in kwargs.items() if k in ['dry_run']}
            
            if not self.run_single_step(step_num, **step_kwargs):
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
        print(f"\n📊 MIGRATION STATUS (External Steps)")
        print("=" * 50)
        
        steps = [
            ("1. Create tables (external step)", self.steps_completed['create_tables']),
            ("2. Import Excel data (external step)", self.steps_completed['import_excel']),
            ("3. Migrate production data (external step)", self.steps_completed['migrate_production']),
            ("4. Rescore all snapshots via Cloud Function", self.steps_completed['rescore_all'])
        ]
        
        for step_name, completed in steps:
            status = "✅" if completed else "⏸️"
            print(f"  {status} {step_name}")
        
        completed_count = sum(self.steps_completed.values())
        print(f"\nProgress: {completed_count}/4 steps completed")
        
        print(f"\n💡 EXTERNAL STEP WORKFLOW:")
        print(f"  • Step 1: steps/step1_table_creation.py")
        print(f"  • Step 2: steps/step2_import_excel.py")
        print(f"  • Step 3: steps/step3_migrate_production.py (to be created)")
        print(f"  • Step 4: Cloud Function via Pub/Sub")
    
    def show_final_summary(self) -> None:
        """Show final migration summary"""
        print(f"\n🎉 MIGRATION SUMMARY (External Steps)")
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
            
            # Count snapshots
            try:
                table_ref = f"{self.project_id}.{self.staging_dataset}.hs_companies"
                snapshot_query = f"SELECT COUNT(DISTINCT snapshot_id) as snapshots FROM `{table_ref}`"
                result = self.client.query(snapshot_query).result()
                
                for row in result:
                    print(f"  • Unique snapshots: {row.snapshots}")
                    break
            except:
                print(f"  • Snapshots: Could not count")
                    
        except Exception as e:
            self.logger.debug(f"Could not generate summary: {e}")
        
        print(f"\n✅ Staging environment ready with external step architecture!")
        print(f"📊 Contains imported data processed by modular steps")
        print(f"🏗️ External steps: steps/step1_*.py, steps/step2_*.py, etc.")
    
    def interactive_menu(self):
        """Interactive menu for migration orchestration with external steps"""
        while True:
            print(f"\n{'='*60}")
            print(f"🚀 MAIN MIGRATION ORCHESTRATOR (External Steps)")
            print(f"{'='*60}")
            print(f"Project: {self.project_id}")
            print(f"Target: {self.staging_dataset}")
            
            # Display authentication info
            if self.auth_info:
                print(f"👤 User: {self.auth_info.get('bigquery_user', 'Unknown')}")
                print(f"🌍 Environment: {self.auth_info.get('current_env', 'Unknown')}")
            
            print(f"{'='*60}")
            
            self.show_status()
            
            print(f"\n📋 OPERATIONS (External Step Architecture)")
            print(f"  1) 🏗️  Step 1: Create tables (external: step1_table_creation.py)")
            print(f"  2) 📥 Step 2: Import Excel data (external: step2_import_excel.py)")
            print(f"  3) 🚀 Step 3: Migrate production (external: step3_migrate_production.py)")
            print(f"  4) 🔄 Step 4: Rescore all snapshots (Cloud Function)")
            print(f"  9) 🎯 Run ALL steps")
            print(f"  0) ❌ Exit")
            
            try:
                choice = input(f"\n🔹 Enter choice (0-4, 9): ").strip()
                
                if choice == '0':
                    print("\n👋 Goodbye!")
                    break
                elif choice in ['1', '2', '3', '4']:
                    # Get step-specific options
                    kwargs = {}
                    if choice == '1':
                        clear_all = input("Clear all tables? (y/n): ").strip().lower() == 'y'
                        kwargs = {'clear_all': clear_all}
                    elif choice == '2':
                        dry_run = input("Dry run? (y/n): ").strip().lower() == 'y'
                        kwargs = {'dry_run': dry_run}
                    elif choice == '3':
                        dry_run = input("Dry run? (y/n): ").strip().lower() == 'y'
                        kwargs = {'dry_run': dry_run}
                    
                    self.run_single_step(int(choice), **kwargs)
                elif choice == '9':
                    print(f"\n🚨 FULL MIGRATION CONFIRMATION")
                    print(f"This will run the complete 4-step process using external modules:")
                    print(f"  1. Create table structure")
                    print(f"  2. Import Excel data")
                    print(f"  3. Migrate production data")
                    print(f"  4. Rescore all snapshots")
                    
                    # Get global options
                    clear_all = input("Clear all tables in step 1? (y/n): ").strip().lower() == 'y'
                    
                    confirm = input(f"\nType 'RUN FULL MIGRATION' to continue: ").strip()
                    if confirm == 'RUN FULL MIGRATION':
                        kwargs = {'clear_all': clear_all}
                        self.run_all_steps(**kwargs)
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