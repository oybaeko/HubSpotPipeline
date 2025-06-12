#!/usr/bin/env python3
"""
File: build-staging/excel_import.py
Path: build-staging/excel_import.py

Excel Import Module - Focus on importing Excel data to staging
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import json

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import existing excel modules
try:
    from src.first_stage_data.excel_import import ExcelProcessor, SnapshotProcessor
    from src.first_stage_data.excel_import.data_mapper import map_excel_to_schema, get_snapshot_configurations
    from src.first_stage_data.excel_import.bigquery_loader import load_to_bigquery, load_multiple_snapshots
    EXCEL_MODULES_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  Excel modules not available: {e}")
    EXCEL_MODULES_AVAILABLE = False

# Import BigQuery
try:
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    BIGQUERY_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  BigQuery not available: {e}")
    BIGQUERY_AVAILABLE = False

class ExcelImportManager:
    """Excel import operations manager"""
    
    def __init__(self, environment: Optional[str] = None):
        self.logger = self._setup_logging()
        self.config = self._load_simple_config()
        
        # Set environment
        if environment:
            self.config['environment'] = environment
        
        self.config['dataset'] = self._get_dataset_for_env(self.config['environment'])
        
        # Initialize BigQuery client
        self.bq_client = None
        if BIGQUERY_AVAILABLE:
            try:
                self.bq_client = bigquery.Client(project=self.config['project'])
                self.logger.info("✅ BigQuery client initialized")
            except Exception as e:
                self.logger.warning(f"⚠️  BigQuery client failed: {e}")
        
        # Session data
        self.session_data = {
            'last_import': None,
            'current_excel_file': None
        }
        
    def _setup_logging(self) -> logging.Logger:
        """Simple logging setup"""
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        return logging.getLogger('staging.excel')
    
    def _load_simple_config(self) -> Dict[str, Any]:
        """Simple configuration"""
        return {
            'environment': 'dev',
            'project': 'hubspot-452402',
            'dataset': 'Hubspot_dev_ob',
            'excel_directory': 'import_data',  # Updated to new directory
            'default_excel_file': 'pipeline-import.xlsx'
        }
    
    def _get_dataset_for_env(self, environment: str) -> str:
        """Get BigQuery dataset for environment"""
        datasets = {
            'dev': 'Hubspot_dev_ob',
            'staging': 'Hubspot_staging', 
            'prod': 'Hubspot_prod'
        }
        return datasets.get(environment, 'Hubspot_dev_ob')
    
    def _get_env_color(self, env: str) -> str:
        """Get color emoji for environment"""
        colors = {'dev': '🟢', 'staging': '🟡', 'prod': '🔴'}
        return colors.get(env, '⚪')

    def show_import_menu(self):
        """Main import menu - ROOT FUNCTION for VSCode debugging"""
        # Select environment first
        if not hasattr(self, '_env_selected'):
            self._select_environment()
            self._env_selected = True
        
        while True:
            self._print_header()
            self._print_menu()
            
            try:
                choice = input("\n🔹 Enter choice (0-6): ").strip()
                
                if choice == '0':
                    print("\n👋 Exiting")
                    break
                elif choice == '1':
                    self._import_excel_snapshots()
                elif choice == '2':
                    self._import_excel_auto()
                elif choice == '3':
                    self._validate_excel_file()
                elif choice == '4':
                    self._show_import_status()
                elif choice == '5':
                    self._switch_environment()
                elif choice == '6':
                    self._test_excel_modules()
                else:
                    print("❌ Invalid choice. Please select 0-6.")
                    
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
                self.logger.error(f"Error: {e}")
            
            if choice != '0':
                input("\n⏸️  Press Enter to continue...")
    
    def _select_environment(self):
        """Select target environment"""
        print("\n🌍 ENVIRONMENT SELECTION")
        print("="*40)
        print("1) 🟢 dev        (Development)")
        print("2) 🟡 staging    (Staging)")
        print("3) 🔴 prod       (Production)")
        
        while True:
            choice = input("\nChoose environment (1-3, default=1): ").strip()
            
            if choice in ['', '1']:
                env = 'dev'
                break
            elif choice == '2':
                env = 'staging'
                break
            elif choice == '3':
                env = 'prod'
                if self._confirm_production():
                    env = 'prod'
                    break
                else:
                    continue
            else:
                print("❌ Invalid choice")
                continue
        
        self.config['environment'] = env
        self.config['dataset'] = self._get_dataset_for_env(env)
        print(f"✅ Environment: {self._get_env_color(env)} {env}")
    
    def _confirm_production(self) -> bool:
        """Confirm production access"""
        print("\n🔴 PRODUCTION WARNING")
        print("This will import to live business data!")
        confirm = input("Type 'IMPORT TO PRODUCTION' to continue: ").strip()
        return confirm == 'IMPORT TO PRODUCTION'
    
    def _switch_environment(self):
        """Switch environment"""
        print(f"\n🔄 Current: {self._get_env_color(self.config['environment'])} {self.config['environment']}")
        self._select_environment()

    def _print_header(self):
        """Print header"""
        env = self.config['environment']
        print(f"\n{'='*60}")
        print(f"📥 EXCEL IMPORT OPERATIONS")
        print(f"{'='*60}")
        print(f"Environment: {self._get_env_color(env)} {env.upper()}")
        print(f"Project: {self.config['project']}")
        print(f"Dataset: {self.config['dataset']}")
        print(f"Excel Dir: {self.config['excel_directory']}")
        if self.session_data['current_excel_file']:
            print(f"Current File: {Path(self.session_data['current_excel_file']).name}")
        print(f"{'='*60}")
    
    def _print_menu(self):
        """Print menu options"""
        env = self.config['environment']
        
        print("\n📊 EXCEL IMPORT OPERATIONS")
        print("  1) 📸 Import Snapshots (configured snapshots)")
        print("  2) 🔍 Import Auto-detect (single snapshot)")
        print("  3) ✅ Validate Excel File")
        print("  4) 📋 Show Import Status")
        print("  5) 🔄 Switch Environment")
        print("  6) 🔧 Test Excel Modules")
        print("  0) ❌ Exit")
        
        # Environment warnings
        if env == 'prod':
            print(f"\n🔴 PRODUCTION: Import operations require confirmation")
        elif env == 'staging':
            print(f"\n🟡 STAGING: Import to staging environment")
        else:
            print(f"\n🟢 DEVELOPMENT: Safe for import testing")
    
    def _import_excel_snapshots(self):
        """Import Excel using configured snapshots"""
        print("\n📸 IMPORT SNAPSHOTS")
        print("-" * 40)
        
        if not self._check_prerequisites():
            return
        
        # Get Excel file
        excel_file = self._select_excel_file()
        if not excel_file:
            return
        
        # Dry run option
        dry_run = self._confirm_dry_run()
        
        # Execute import
        try:
            print(f"\n🚀 Starting snapshots import...")
            print(f"   File: {excel_file}")
            print(f"   Mode: snapshots")
            print(f"   Dry run: {dry_run}")
            print(f"   Target: {self.config['dataset']}")
            
            # Create processor
            processor = ExcelProcessor(excel_file)
            snapshot_processor = SnapshotProcessor(processor)
            
            # Process all snapshots
            result = snapshot_processor.process_all_snapshots()
            
            # Load to BigQuery
            snapshots_data = result['snapshots']
            load_multiple_snapshots(snapshots_data, dry_run=dry_run)
            
            # Show results
            print(f"\n✅ Import completed!")
            print(f"📊 Total records: {result['totals']['total_records']}")
            print(f"📸 Snapshots: {result['totals']['snapshots']}")
            print(f"🏢 Companies: {result['totals']['companies']}")
            print(f"🤝 Deals: {result['totals']['deals']}")
            
            if dry_run:
                print(f"🛑 DRY RUN: No data written to BigQuery")
            else:
                print(f"✅ Data written to {self.config['environment']} environment")
            
            # Update session
            self._update_session_data(excel_file, 'snapshots', result, dry_run)
            
        except Exception as e:
            print(f"❌ Import failed: {e}")
            self.logger.error(f"Snapshots import failed: {e}")
    
    def _import_excel_auto(self):
        """Import Excel using auto-detection"""
        print("\n🔍 IMPORT AUTO-DETECT")
        print("-" * 40)
        
        if not self._check_prerequisites():
            return
        
        # Get Excel file
        excel_file = self._select_excel_file()
        if not excel_file:
            return
        
        # Dry run option
        dry_run = self._confirm_dry_run()
        
        # Execute import
        try:
            print(f"\n🚀 Starting auto-detect import...")
            print(f"   File: {excel_file}")
            print(f"   Mode: auto-detect")
            print(f"   Dry run: {dry_run}")
            print(f"   Target: {self.config['dataset']}")
            
            # Create processor
            processor = ExcelProcessor(excel_file)
            
            # Auto-detect sheets
            sheet_data = processor.extract_hubspot_sheets()
            
            if not sheet_data:
                print("❌ No HubSpot sheets detected")
                return
            
            print(f"📋 Detected {len(sheet_data)} sheets:")
            for sheet_name, df in sheet_data.items():
                print(f"   • {sheet_name}: {len(df)} rows")
            
            # Generate snapshot ID
            snapshot_id = datetime.now().isoformat()
            
            # Map and load data
            mapped_data = map_excel_to_schema(sheet_data, snapshot_id)
            load_to_bigquery(mapped_data, dry_run=dry_run)
            
            # Show results
            total_records = sum(len(records) for records in mapped_data.values())
            print(f"\n✅ Import completed!")
            print(f"📊 Total records: {total_records}")
            print(f"📸 Snapshot ID: {snapshot_id}")
            
            for data_type, records in mapped_data.items():
                print(f"   • {data_type}: {len(records)} records")
            
            if dry_run:
                print(f"🛑 DRY RUN: No data written to BigQuery")
            else:
                print(f"✅ Data written to {self.config['environment']} environment")
            
            # Update session
            result = {'total_records': total_records, 'snapshots_count': 1, 'snapshot_id': snapshot_id}
            self._update_session_data(excel_file, 'auto', result, dry_run)
            
        except Exception as e:
            print(f"❌ Import failed: {e}")
            self.logger.error(f"Auto-detect import failed: {e}")
    
    def _validate_excel_file(self):
        """Validate Excel file without importing"""
        print("\n✅ VALIDATE EXCEL FILE")
        print("-" * 40)
        
        if not EXCEL_MODULES_AVAILABLE:
            print("❌ Excel modules not available")
            return
        
        # Get Excel file
        excel_file = self._select_excel_file()
        if not excel_file:
            return
        
        try:
            print(f"\n🔍 Validating: {excel_file}")
            
            # Create processor
            processor = ExcelProcessor(excel_file)
            
            # Check available sheets
            available_sheets = processor.get_available_sheets()
            print(f"\n📋 Found {len(available_sheets)} sheets:")
            for i, sheet in enumerate(available_sheets, 1):
                print(f"   {i:2}. {sheet}")
            
            # Validate snapshot sheets
            found_sheets, missing_sheets = processor.validate_snapshot_sheets()
            print(f"\n📸 Snapshot validation:")
            print(f"   • Expected sheets: {len(found_sheets) + len(missing_sheets)}")
            print(f"   • Found: {len(found_sheets)}")
            print(f"   • Missing: {len(missing_sheets)}")
            
            if missing_sheets:
                print(f"\n❌ Missing sheets:")
                for sheet in missing_sheets:
                    print(f"   • {sheet}")
            
            # Try auto-detection
            hubspot_sheets = processor.extract_hubspot_sheets()
            print(f"\n🔍 Auto-detection:")
            if hubspot_sheets:
                print(f"   ✅ Detected {len(hubspot_sheets)} HubSpot sheets:")
                for sheet_name, df in hubspot_sheets.items():
                    print(f"   • {sheet_name}: {len(df)} rows")
            else:
                print(f"   ❌ No HubSpot sheets auto-detected")
            
            # Show configured snapshots
            snapshots = get_snapshot_configurations()
            print(f"\n📊 Configured snapshots: {len(snapshots)}")
            for snapshot in snapshots[:3]:  # Show first 3
                print(f"   • {snapshot['date']}")
            if len(snapshots) > 3:
                print(f"   • ... and {len(snapshots) - 3} more")
            
            print(f"\n✅ Validation completed")
            
        except Exception as e:
            print(f"❌ Validation failed: {e}")
            self.logger.error(f"Excel validation failed: {e}")
    
    def _show_import_status(self):
        """Show import status"""
        print("\n📋 IMPORT STATUS")
        print("-" * 40)
        
        if self.session_data['last_import']:
            last = self.session_data['last_import']
            print(f"📊 Last Import:")
            print(f"   • Timestamp: {last['timestamp']}")
            print(f"   • File: {Path(last['file']).name}")
            print(f"   • Mode: {last['mode']}")
            print(f"   • Records: {last['records']}")
            print(f"   • Dry Run: {last['dry_run']}")
            print(f"   • Environment: {last['environment']}")
        else:
            print("📊 No imports in current session")
        
        # Show current Excel file
        if self.session_data['current_excel_file']:
            print(f"\n📁 Current Excel file:")
            print(f"   • {self.session_data['current_excel_file']}")
        
        # TODO: Check BigQuery for recent imports
        print("\n💡 BigQuery import history not implemented yet")
    
    def _test_excel_modules(self):
        """Test Excel module availability"""
        print("\n🔧 EXCEL MODULES TEST")
        print("-" * 40)
        
        print(f"Excel modules available: {EXCEL_MODULES_AVAILABLE}")
        print(f"BigQuery available: {BIGQUERY_AVAILABLE}")
        
        if EXCEL_MODULES_AVAILABLE:
            print("\n✅ Excel modules:")
            print(f"   • ExcelProcessor: Available")
            print(f"   • SnapshotProcessor: Available")
            print(f"   • map_excel_to_schema: Available")
            print(f"   • load_to_bigquery: Available")
            
            # Test basic functionality
            try:
                snapshots = get_snapshot_configurations()
                print(f"   • Snapshot configurations: {len(snapshots)} found")
            except Exception as e:
                print(f"   • Snapshot configurations: Error - {e}")
        else:
            print("\n❌ Excel modules not available")
            print("   Check import paths and dependencies")
        
        if self.bq_client:
            print(f"\n✅ BigQuery client: Connected")
            print(f"   • Project: {self.config['project']}")
            print(f"   • Dataset: {self.config['dataset']}")
        else:
            print(f"\n❌ BigQuery client: Not available")
    
    def _check_prerequisites(self) -> bool:
        """Check if prerequisites are available"""
        if not EXCEL_MODULES_AVAILABLE:
            print("❌ Excel modules not available")
            print("💡 Check import paths: src/first_stage_data/excel_import/")
            return False
        
        if not self.bq_client:
            print("❌ BigQuery client not available")
            print("💡 Check authentication and permissions")
            return False
        
        return True
    
    def _select_excel_file(self) -> Optional[str]:
        """Select Excel file for import"""
        # import_data is relative to build-staging directory (where this script is)
        script_dir = Path(__file__).parent
        excel_dir = script_dir / self.config['excel_directory']  # build-staging/import_data
        
        default_file = excel_dir / self.config['default_excel_file']
        
        print(f"\n📁 Excel file selection:")
        print(f"   Directory: {excel_dir}")
        print(f"   Default: {self.config['default_excel_file']}")
        
        # List available Excel files in the directory
        if excel_dir.exists():
            excel_files = list(excel_dir.glob("*.xlsx")) + list(excel_dir.glob("*.xls"))
            if excel_files:
                print(f"\n📋 Available Excel files ({len(excel_files)}):")
                for i, file in enumerate(excel_files, 1):
                    size_mb = file.stat().st_size / 1024 / 1024
                    print(f"   {i:2}. {file.name} ({size_mb:.1f} MB)")
                
                # Option to select by number
                print(f"\n   Options:")
                if default_file.exists():
                    print(f"   • Press Enter for default: {self.config['default_excel_file']}")
                print(f"   • Enter number (1-{len(excel_files)}) to select file")
                print(f"   • Enter full path for custom file")
                
                choice = input("\n   Your choice: ").strip()
                
                # Default file
                if choice == '' and default_file.exists():
                    return str(default_file)
                
                # Select by number
                try:
                    file_num = int(choice)
                    if 1 <= file_num <= len(excel_files):
                        return str(excel_files[file_num - 1])
                except ValueError:
                    pass
                
                # Custom path
                if choice:
                    file_path = Path(choice).expanduser()
                    if not file_path.is_absolute():
                        file_path = excel_dir / file_path
                    
                    if file_path.exists() and file_path.suffix.lower() in ['.xlsx', '.xls']:
                        return str(file_path)
                    else:
                        print(f"❌ File not found: {file_path}")
                        return None
            else:
                print(f"\n⚠️  No Excel files found in {excel_dir}")
                print(f"💡 Place Excel files in: {excel_dir}")
        else:
            print(f"\n❌ Directory not found: {excel_dir}")
            print(f"💡 Create directory: mkdir -p {excel_dir}")
        
        # Manual file selection fallback
        print(f"\n📝 Manual file entry:")
        while True:
            file_path = input("   Enter Excel file path (or 'q' to quit): ").strip()
            if file_path.lower() == 'q':
                return None
            if not file_path:
                print("❌ File path required")
                continue
                
            file_path = Path(file_path).expanduser()
            if not file_path.is_absolute():
                file_path = excel_dir / file_path
                
            if file_path.exists() and file_path.suffix.lower() in ['.xlsx', '.xls']:
                return str(file_path)
            else:
                print(f"❌ File not found or not Excel format: {file_path}")
                retry = input("   Try again? (y/n): ").strip().lower()
                if retry not in ['y', 'yes']:
                    return None
    
    def _confirm_dry_run(self) -> bool:
        """Confirm dry run mode"""
        env = self.config['environment']
        default_dry_run = (env == 'prod')  # Default to dry run for production
        
        print(f"\n🛑 Import mode:")
        print("   Dry run: Preview import without writing to BigQuery")
        print("   Live run: Actually import data to BigQuery")
        
        if env == 'prod':
            print("   🔴 Production environment - dry run recommended")
        
        default_text = "y" if default_dry_run else "n"
        choice = input(f"   Use dry run mode? (y/n, default={default_text}): ").strip().lower()
        
        if choice == '':
            return default_dry_run
        return choice in ['y', 'yes']
    
    def _update_session_data(self, excel_file: str, mode: str, result: Dict[str, Any], dry_run: bool):
        """Update session data after import"""
        self.session_data['last_import'] = {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'file': excel_file,
            'mode': mode,
            'records': result.get('total_records', 0),
            'dry_run': dry_run,
            'environment': self.config['environment']
        }
        self.session_data['current_excel_file'] = excel_file


# CLI wrapper
def run_cli():
    """Simple CLI wrapper"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Excel Import Operations")
    parser.add_argument("--environment", choices=['dev', 'staging', 'prod'], help="Target environment")
    parser.add_argument("--file", help="Excel file to import")
    parser.add_argument("--mode", choices=['snapshots', 'auto', 'validate'], help="Import mode")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")
    args = parser.parse_args()
    
    manager = ExcelImportManager(args.environment)
    
    if args.file and args.mode:
        # Direct import mode
        print(f"🚀 Direct import: {args.file} ({args.mode})")
        # TODO: Implement direct CLI import
        print("💡 Direct CLI import not implemented yet - use interactive mode")
    
    manager.show_import_menu()


# ROOT FUNCTION for VSCode debugging
def main():
    """Main entry point - ROOT function for VSCode debugging"""
    print("📥 Starting Excel Import Operations")
    print("💡 Focused on importing Excel data to staging environments")
    
    try:
        manager = ExcelImportManager()
        manager.show_import_menu()
    except Exception as e:
        print(f"❌ Error: {e}")
        logging.error(f"Error: {e}", exc_info=True)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_cli()
    else:
        main()