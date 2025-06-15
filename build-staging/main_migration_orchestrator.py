#!/usr/bin/env python3
"""
Main Migration Orchestrator - UPDATED VERSION
Simplified to use simple_table_creator.py for table operations
3-Step Process: Create Tables → Migrate → Import Historical
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
    """Simplified migration orchestration using simple_table_creator"""
    
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
        
        # Path to simple table creator
        self.table_creator_script = Path(__file__).parent / "simple_table_creator.py"
        
        # Migration steps status - SIMPLIFIED 3-step process
        self.steps_completed = {
            'create_tables': False,
            'migrate_data': False,
            'import_excel': False
        }
    
    def _get_auth_info(self) -> Dict:
        """Get current authentication information"""
        auth_info = {
            'bigquery_user': 'Unknown',
            'gcloud_user': 'Unknown',
            'current_env': 'staging'
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
            
        except Exception as e:
            auth_info['error'] = str(e)
        
        return auth_info
    
    def _log_step_start(self, step_num: int, step_name: str):
        """Log step start with authentication and environment info"""
        self.logger.info(f"🎯 Running Step {step_num}: {step_name}")
        self.logger.info(f"👤 User: {self.auth_info.get('bigquery_user', 'Unknown')}")
        self.logger.info(f"🌍 Target Environment: staging ({self.staging_dataset})")
        self.logger.info(f"📂 Target Dataset: {self.project_id}.{self.staging_dataset}")
    
    def run_python_script(self, script_path: str, args: List[str] = None) -> bool:
        """Run a Python script and return success status"""
        if args is None:
            args = []
        
        cmd = [sys.executable, str(script_path)] + args
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Log output
            if result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    self.logger.info(f"  {line}")
            
            return True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ Script failed: {script_path}")
            self.logger.error(f"Exit code: {e.returncode}")
            if e.stdout:
                self.logger.error(f"Stdout: {e.stdout}")
            if e.stderr:
                self.logger.error(f"Stderr: {e.stderr}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Failed to run script: {e}")
            return False
    
    def step_1_create_tables_via_simple_creator(self) -> bool:
        """Step 1: Create tables using simple_table_creator.py"""
        self.logger.info("🏗️  STEP 1: Creating tables using simple table creator")
        
        if not self.table_creator_script.exists():
            self.logger.error(f"❌ Table creator script not found: {self.table_creator_script}")
            self.logger.error("💡 Make sure simple_table_creator.py exists in build-staging/")
            return False
        
        try:
            # Import the simple table creator to use programmatically
            sys.path.insert(0, str(self.table_creator_script.parent))
            from simple_table_creator import SimpleTableCreator
            
            self.logger.info("🔧 Initializing simple table creator...")
            creator = SimpleTableCreator()
            creator.environment = 'staging'  # Force staging environment
            creator.dataset = creator.environments['staging']
            
            self.logger.info(f"🎯 Target: {creator.environment} ({creator.dataset})")
            
            # Check if tables already exist
            self.logger.info("🔍 Checking existing tables...")
            creator.check_tables()
            
            # Ask user what to do
            print(f"\n🏗️  TABLE CREATION OPTIONS")
            print(f"1) Create missing tables only")
            print(f"2) Recreate all tables (DELETE + CREATE)")
            print(f"3) Skip table creation")
            
            while True:
                choice = input("\nChoose option (1-3): ").strip()
                
                if choice == '1':
                    recreate = False
                    break
                elif choice == '2':
                    print(f"\n⚠️  RECREATE ALL TABLES WARNING")
                    print(f"This will DELETE existing tables in staging!")
                    confirm = input("Type 'RECREATE TABLES' to confirm: ")
                    if confirm == 'RECREATE TABLES':
                        recreate = True
                        break
                    else:
                        self.logger.info("❌ Operation cancelled")
                        return False
                elif choice == '3':
                    self.logger.info("⏭️  Skipping table creation")
                    self.steps_completed['create_tables'] = True
                    return True
                else:
                    print("❌ Invalid choice")
            
            # Create tables
            self.logger.info(f"🚀 Creating core tables (recreate={recreate})...")
            success = creator.create_all_core_tables(recreate=recreate)
            
            if success:
                self.logger.info("✅ Table creation completed successfully")
                self.steps_completed['create_tables'] = True
                return True
            else:
                self.logger.error("❌ Table creation failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"❌ Failed to import simple_table_creator: {e}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Table creation failed: {e}")
            return False
    
    def step_2_migrate_prod_to_staging(self) -> bool:
        """Step 2: Migrate ONLY companies and deals from prod to staging"""
        self.logger.info("🚀 STEP 2: Migrating data from production to staging")
        self.logger.info("📊 This will migrate ONLY companies and deals")
        self.logger.info("🧹 Migration includes automatic table clearing")
        
        try:
            # Import migration components directly from build-staging/migration/
            migration_dir = Path(__file__).parent / "migration"
            if not migration_dir.exists():
                self.logger.error(f"❌ Migration directory not found: {migration_dir}")
                self.logger.error(f"💡 Expected path: {migration_dir}")
                return False
            
            # Add migration directory to path
            sys.path.insert(0, str(Path(__file__).parent))
            
            # Import the data migrator
            from migration.data_migrator import DataMigrator
            
            # Create data migrator
            data_migrator = DataMigrator()
            
            # Run migration
            self.logger.info("🎯 Running data migration (companies + deals with auto-clear)...")
            success = data_migrator.migrate_prod_to_staging(dry_run=False)
            
            if success:
                self.logger.info("✅ Production data migrated successfully")
                self.logger.info("📊 Migrated: companies and deals")
                self.logger.info("💡 Tables were automatically cleared before migration")
                self.logger.info("💡 Reference data will come from ingest pipeline")
                self.steps_completed['migrate_data'] = True
                return True
            else:
                self.logger.error("❌ Migration failed")
                return False
                
        except ImportError as e:
            self.logger.error(f"❌ Failed to import migration modules: {e}")
            self.logger.error(f"💡 Check that migration/ directory exists with required modules:")
            self.logger.error(f"   • migration/__init__.py")
            self.logger.error(f"   • migration/data_migrator.py")
            self.logger.error(f"   • migration/schema_analyzer.py")
            self.logger.error(f"   • migration/config.py")
            return False
        except Exception as e:
            self.logger.error(f"❌ Failed to migrate data: {e}")
            import traceback
            self.logger.debug(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def step_3_import_historical_excel_data(self) -> bool:
        """Step 3: Import historical Excel data with CRM metadata support"""
        self.logger.info("📥 STEP 3: Importing historical Excel data")
        self.logger.info("📊 This will import multiple historical snapshots")
        self.logger.info("🕐 Will use CRM file timestamps if available")
        
        try:
            # Check Excel import availability
            if not self._check_excel_import_availability():
                return False
            
            # Excel file selection
            excel_file = self._select_excel_file()
            if not excel_file:
                self.logger.error("❌ No Excel file selected")
                return False
            
            # Check for CRM files in the same directory
            import_dir = Path(excel_file).parent
            crm_metadata = self._extract_crm_metadata(import_dir)
            
            if crm_metadata:
                self.logger.info(f"✅ Found CRM metadata for {len(crm_metadata)} snapshots")
                self.logger.info("🕐 Will use actual HubSpot export timestamps")
            else:
                self.logger.info("📅 No CRM files found, will use Excel sheet dates")
            
            # Confirm operation
            if not self._confirm_excel_import(excel_file, crm_metadata):
                self.logger.info("❌ Excel import cancelled")
                return False
            
            # Import Excel functionality
            excel_import_path = Path(__file__).parent / "first_stage_data"
            sys.path.insert(0, str(excel_import_path))
            
            from excel_import import ExcelProcessor, SnapshotProcessor
            from excel_import.bigquery_loader import load_multiple_snapshots
            
            # Process Excel file
            self.logger.info(f"📂 Processing Excel file: {Path(excel_file).name}")
            processor = ExcelProcessor(excel_file)
            snapshot_processor = SnapshotProcessor(processor)
            
            # Validate sheets exist
            found_sheets, missing_sheets = processor.validate_snapshot_sheets()
            if missing_sheets:
                self.logger.warning(f"⚠️ Missing {len(missing_sheets)} expected sheets")
                if not self._confirm_proceed_with_missing_sheets(missing_sheets):
                    return False
            
            # Process snapshots with or without CRM metadata
            self.logger.info("🔄 Processing Excel snapshots...")
            if crm_metadata:
                # Use CRM timestamps
                result = snapshot_processor.process_all_snapshots_with_crm_metadata(crm_metadata)
                self.logger.info("🕐 Using CRM file download timestamps as snapshot_id")
            else:
                # Use Excel dates
                result = snapshot_processor.process_all_snapshots()
                self.logger.info("📅 Using Excel sheet dates as snapshot_id")
            
            snapshots_data = result['snapshots']
            
            if not snapshots_data:
                self.logger.error("❌ No snapshot data extracted from Excel")
                return False
            
            self.logger.info(f"✅ Extracted {len(snapshots_data)} snapshots")
            self.logger.info(f"📊 Total records: {result['totals']['total_records']}")
            self.logger.info(f"🏢 Companies: {result['totals']['companies']}")
            self.logger.info(f"🤝 Deals: {result['totals']['deals']}")
            
            # Set environment for Excel import
            import os
            os.environ['BIGQUERY_PROJECT_ID'] = self.project_id
            os.environ['BIGQUERY_DATASET_ID'] = self.staging_dataset
            
            # Load to BigQuery (staging environment) 
            self.logger.info("📤 Loading historical data to staging...")
            self.logger.info("🕐 Adding current timestamp to all Excel records...")
            load_multiple_snapshots(snapshots_data, dry_run=False)
            
            self.logger.info("✅ Historical Excel data imported successfully")
            self.logger.info(f"📸 Snapshots imported: {len(snapshots_data)}")
            
            if crm_metadata:
                self.logger.info("🕐 Snapshot IDs contain precise HubSpot export timestamps")
                # Show sample timestamps
                sample_snapshots = list(snapshots_data.keys())[:3]
                for snapshot_id in sample_snapshots:
                    self.logger.info(f"   📅 Example: {snapshot_id}")
            else:
                self.logger.info("📅 Snapshot IDs are Excel sheet dates")
            
            self.steps_completed['import_excel'] = True
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to import Excel data: {e}")
            import traceback
            self.logger.debug(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _extract_crm_metadata(self, import_dir: Path) -> Dict:
        """Extract CRM metadata from CSV files in import directory"""
        try:
            # Find CRM CSV files
            csv_files = list(import_dir.glob("hubspot-crm-exports-*.csv"))
            
            if not csv_files:
                return {}
            
            self.logger.info(f"🔍 Analyzing {len(csv_files)} CRM CSV files...")
            
            # Group files by date
            crm_metadata = {}
            
            for csv_file in csv_files:
                # Extract date from filename
                import re
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', csv_file.name)
                if not date_match:
                    continue
                
                snapshot_date = date_match.group(1)
                
                # Get file timestamp
                timestamp = self._get_file_timestamp(csv_file)
                if not timestamp:
                    # Fallback: use current time if file timestamp extraction fails
                    import datetime
                    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
                    self.logger.debug(f"Using current time as fallback for {csv_file.name}: {timestamp}")
                
                # Initialize metadata for this date
                if snapshot_date not in crm_metadata:
                    crm_metadata[snapshot_date] = {
                        'company_file': None,
                        'deals_file': None,
                        'company_timestamp': None,
                        'deals_timestamp': None
                    }
                
                # Classify file type and store info
                if 'company' in csv_file.name.lower():
                    crm_metadata[snapshot_date]['company_file'] = str(csv_file)
                    crm_metadata[snapshot_date]['company_timestamp'] = timestamp
                elif 'deal' in csv_file.name.lower():
                    crm_metadata[snapshot_date]['deals_file'] = str(csv_file)
                    crm_metadata[snapshot_date]['deals_timestamp'] = timestamp
            
            # Create final metadata with snapshot_id
            final_metadata = {}
            for snapshot_date, files in crm_metadata.items():
                if files['company_file'] and files['deals_file']:
                    # Use the earlier timestamp as snapshot_id
                    company_time = files['company_timestamp']
                    deals_time = files['deals_timestamp']
                    snapshot_id = min(company_time, deals_time) if company_time and deals_time else company_time or deals_time
                    
                    final_metadata[snapshot_date] = {
                        'snapshot_id': snapshot_id,
                        **files
                    }
            
            self.logger.info(f"✅ Extracted CRM metadata for {len(final_metadata)} complete snapshot dates")
            return final_metadata
            
        except Exception as e:
            self.logger.warning(f"⚠️  Failed to extract CRM metadata: {e}")
            return {}
    
    def _get_file_timestamp(self, file_path: Path) -> Optional[str]:
        """Get file modification timestamp in Z format to match production format"""
        try:
            import datetime
            
            # Get file modification time
            mtime = file_path.stat().st_mtime
            dt = datetime.datetime.fromtimestamp(mtime, tz=datetime.timezone.utc)
            
            # Return in Z format: 2025-06-08T04:00:11.000000Z
            return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
        except Exception as e:
            self.logger.debug(f"Failed to get timestamp for {file_path}: {e}")
            # Fallback: use current time if file timestamp fails
            return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    def _check_excel_import_availability(self) -> bool:
        """Check if Excel import modules are available"""
        try:
            excel_import_path = Path(__file__).parent / "first_stage_data"
            if not excel_import_path.exists():
                self.logger.error(f"❌ Excel import directory not found: {excel_import_path}")
                return False
            
            # Check key files exist
            required_files = [
                "excel_import/__init__.py",
                "excel_import/excel_processor.py",
                "excel_import/bigquery_loader.py"
            ]
            
            for file in required_files:
                if not (excel_import_path / file).exists():
                    self.logger.error(f"❌ Required Excel import file not found: {file}")
                    return False
            
            self.logger.info("✅ Excel import modules available")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error checking Excel import availability: {e}")
            return False
    
    def _select_excel_file(self) -> Optional[str]:
        """Select Excel file for import"""
        import_dir = Path(__file__).parent / "import_data"
        
        print(f"\n📁 Import file selection:")
        print(f"   Directory: {import_dir}")
        
        # Create directory if it doesn't exist
        if not import_dir.exists():
            print(f"📁 Creating import directory: {import_dir}")
            import_dir.mkdir(parents=True, exist_ok=True)
        
        # List available files
        excel_files = list(import_dir.glob("*.xlsx")) + list(import_dir.glob("*.xls"))
        csv_files = list(import_dir.glob("hubspot-crm-exports-*.csv"))
        
        print(f"\n📊 Available files:")
        print(f"   📋 Excel files: {len(excel_files)}")
        print(f"   📄 CRM CSV files: {len(csv_files)}")
        
        if excel_files:
            print(f"\n📋 Excel files ({len(excel_files)}):")
            for i, file in enumerate(excel_files, 1):
                try:
                    size_mb = file.stat().st_size / 1024 / 1024
                    print(f"   {i:2}. {file.name} ({size_mb:.1f} MB)")
                except:
                    print(f"   {i:2}. {file.name}")
        
        if csv_files:
            print(f"\n📄 CRM CSV files found ({len(csv_files)}):")
            # Group by date
            csv_by_date = {}
            for csv_file in csv_files:
                # Extract date from filename: hubspot-crm-exports-weekly-status-company-2025-03-21.csv
                import re
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', csv_file.name)
                if date_match:
                    date = date_match.group(1)
                    if date not in csv_by_date:
                        csv_by_date[date] = []
                    csv_by_date[date].append(csv_file)
            
            for date in sorted(csv_by_date.keys()):
                files = csv_by_date[date]
                print(f"   📅 {date}: {len(files)} files")
                for file in files:
                    file_type = "companies" if "company" in file.name else "deals" if "deal" in file.name else "other"
                    print(f"      • {file_type}: {file.name}")
        
        if excel_files:
            print(f"\n   Options:")
            print(f"   • Enter number (1-{len(excel_files)}) to select Excel file")
            print(f"   • Enter full path for custom file")
            print(f"   • Press Enter to cancel")
            
            while True:
                choice = input(f"\nYour choice: ").strip()
                
                if not choice:
                    return None
                
                # Select by number
                try:
                    file_num = int(choice)
                    if 1 <= file_num <= len(excel_files):
                        selected_file = str(excel_files[file_num - 1])
                        
                        # Check if CRM files are available for timestamp extraction
                        if csv_files:
                            print(f"\n💡 CRM CSV files detected!")
                            print(f"   ✅ Will use actual HubSpot export timestamps")
                            print(f"   ✅ More precise than Excel sheet dates")
                        else:
                            print(f"\n⚠️  No CRM CSV files found")
                            print(f"   📅 Will use Excel sheet dates as timestamps")
                        
                        return selected_file
                    else:
                        print(f"❌ Invalid number. Please enter 1-{len(excel_files)}")
                        continue
                except ValueError:
                    pass
                
                # Custom path
                file_path = Path(choice).expanduser()
                if not file_path.is_absolute():
                    file_path = import_dir / file_path
                
                if file_path.exists() and file_path.suffix.lower() in ['.xlsx', '.xls']:
                    return str(file_path)
                else:
                    print(f"❌ File not found or not Excel format: {file_path}")
                    retry = input("   Try again? (y/n): ").strip().lower()
                    if retry not in ['y', 'yes']:
                        return None
        else:
            print(f"\n⚠️  No Excel files found in {import_dir}")
            print(f"💡 Place Excel files in: {import_dir}")
            print(f"💡 Expected files:")
            print(f"   📋 pipeline-import.xlsx")
            print(f"   📄 hubspot-crm-exports-weekly-status-company-*.csv")
            print(f"   📄 hubspot-crm-exports-weekly-status-deals-*.csv")
            
            # Manual file entry option
            manual_path = input(f"\nEnter full path to Excel file (or press Enter to skip): ").strip()
            if manual_path:
                file_path = Path(manual_path).expanduser()
                if file_path.exists() and file_path.suffix.lower() in ['.xlsx', '.xls']:
                    return str(file_path)
                else:
                    print(f"❌ File not found: {file_path}")
        
        return None
    
    def _confirm_excel_import(self, excel_file: str, crm_metadata: Dict = None) -> bool:
        """Confirm Excel import operation"""
        print(f"\n📊 EXCEL IMPORT CONFIRMATION")
        print(f"File: {Path(excel_file).name}")
        print(f"Target: staging environment ({self.staging_dataset})")
        
        if crm_metadata:
            print(f"🕐 CRM Timestamps: YES ({len(crm_metadata)} snapshots)")
            print(f"   ✅ Will use actual HubSpot export timestamps as snapshot_id")
            print(f"   ✅ More precise timing than Excel sheet dates")
            
            # Show sample timestamps
            sample_dates = list(crm_metadata.keys())[:3]
            for date in sample_dates:
                snapshot_id = crm_metadata[date]['snapshot_id']
                print(f"   📅 {date} → {snapshot_id}")
            if len(crm_metadata) > 3:
                print(f"   📅 ... and {len(crm_metadata) - 3} more")
        else:
            print(f"📅 CRM Timestamps: NO")
            print(f"   ⚠️  Will use Excel sheet dates as snapshot_id")
            print(f"   💡 For precise timestamps, place CRM CSV files in import_data/")
        
        print(f"\nThis will import historical snapshots to staging")
        print(f"Data will be ADDED to existing production data")
        
        confirm = input(f"\nType 'IMPORT EXCEL' to continue: ").strip()
        return confirm == 'IMPORT EXCEL'
    
    def _confirm_proceed_with_missing_sheets(self, missing_sheets: List[str]) -> bool:
        """Confirm proceeding with missing sheets"""
        print(f"\n⚠️ MISSING SHEETS WARNING")
        print(f"Missing {len(missing_sheets)} expected sheets:")
        for sheet in missing_sheets[:5]:
            print(f"  • {sheet}")
        if len(missing_sheets) > 5:
            print(f"  • ... and {len(missing_sheets) - 5} more")
        
        print(f"\nThis will import only the available sheets.")
        confirm = input(f"Proceed with available sheets? (y/n): ").strip().lower()
        return confirm in ['y', 'yes']
    
    def run_single_step(self, step_number: int) -> bool:
        """Run a single step - SIMPLIFIED 3-step process"""
        steps = {
            1: ("Create tables using simple table creator", self.step_1_create_tables_via_simple_creator),
            2: ("Migrate prod to staging (companies + deals)", self.step_2_migrate_prod_to_staging),
            3: ("Import historical Excel data", self.step_3_import_historical_excel_data)
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
        """Run all migration steps in sequence - SIMPLIFIED 3-step process"""
        self.logger.info("🚀 FULL MIGRATION: Running all steps")
        
        steps = [1, 2, 3]  # Simplified 3-step process
        
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
        """Show current migration status - SIMPLIFIED 3-step process"""
        print(f"\n📊 MIGRATION STATUS")
        print("=" * 50)
        
        steps = [
            ("1. Create tables (using simple_table_creator.py)", self.steps_completed['create_tables']),
            ("2. Migrate prod to staging (companies + deals)", self.steps_completed['migrate_data']),
            ("3. Import historical Excel data (multiple snapshots)", self.steps_completed['import_excel'])
        ]
        
        for step_name, completed in steps:
            status = "✅" if completed else "⏸️"
            print(f"  {status} {step_name}")
        
        completed_count = sum(self.steps_completed.values())
        print(f"\nProgress: {completed_count}/3 steps completed")
        
        print(f"\n💡 SIMPLIFIED WORKFLOW:")
        print(f"  • Step 1: Create table structure (via simple_table_creator.py)")
        print(f"  • Step 2: Import current production data")
        print(f"  • Step 3: Import historical Excel data")
    
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
        
        print(f"\n✅ Staging environment ready!")
        print(f"📊 Contains production + historical data")
        print(f"💡 Run ingest pipeline to populate reference data")
    
    def interactive_menu(self):
        """Interactive menu for migration orchestration - SIMPLIFIED 3-step process"""
        while True:
            print(f"\n{'='*60}")
            print(f"🚀 MAIN MIGRATION ORCHESTRATOR (SIMPLIFIED)")
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
            print(f"  1) 🏗️  Step 1: Create tables (via simple_table_creator.py)")
            print(f"  2) 🚀 Step 2: Migrate prod to staging (companies + deals)")
            print(f"  3) 📥 Step 3: Import historical Excel data")
            print(f"  9) 🎯 Run ALL steps")
            print(f"  0) ❌ Exit")
            
            try:
                choice = input(f"\n🔹 Enter choice (0-3, 9): ").strip()
                
                if choice == '0':
                    print("\n👋 Goodbye!")
                    break
                elif choice in ['1', '2', '3']:
                    self.run_single_step(int(choice))
                elif choice == '9':
                    print(f"\n🚨 FULL MIGRATION CONFIRMATION")
                    print(f"This will run the complete 3-step process:")
                    print(f"  1. Create table structure using simple_table_creator.py")
                    print(f"  2. Migrate companies and deals from production")
                    print(f"  3. Import historical Excel data (multiple snapshots)")
                    print(f"  💡 Complete staging setup with production + historical data")
                    print(f"  ⚡ Reference data will come from ingest pipeline")
                    
                    confirm = input(f"\nType 'RUN FULL MIGRATION' to continue: ").strip()
                    if confirm == 'RUN FULL MIGRATION':
                        self.run_all_steps()
                    else:
                        print("❌ Operation cancelled")
                else:
                    print("❌ Invalid choice. Please select 0-3 or 9.")
                    
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
        elif command in ['1', '2', '3']:
            orchestrator.run_single_step(int(command))
        elif command == 'all':
            orchestrator.run_all_steps()
        else:
            print(f"Unknown command: {command}")
            print("Available commands: status, 1, 2, 3, all")
    else:
        orchestrator.interactive_menu()


if __name__ == "__main__":
    main()