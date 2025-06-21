#!/usr/bin/env python3
"""
Step 2: Excel Import - UPDATED VERSION
Uses existing excel_import modules with consistent timestamp formatting (no microseconds)
"""

import sys
import os
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Optional
import time

class ExcelImportStep:
    """Excel import step using existing modules with consistent timestamp format"""
    
    def __init__(self, project_id: str = "hubspot-452402", dataset: str = "Hubspot_staging"):
        self.project_id = project_id
        self.staging_dataset = dataset
        
        # Setup logging
        self.logger = logging.getLogger('excel_import_step')
        
        # Setup paths and imports
        self._setup_environment()
        
        # Track results
        self.results = {}
        self.completed = False
    
    def _setup_environment(self):
        """Setup paths and import existing modules"""
        # Clear service account credentials to use user auth (from orchestration)
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
            self.logger.debug("Cleared GOOGLE_APPLICATION_CREDENTIALS to use user auth")
        
        # Excel modules are now co-located in same directory
        excel_path = Path(__file__).parent / "excel_import"
        if str(excel_path) not in sys.path:
            sys.path.insert(0, str(excel_path))
        
        # BigQuery utils stays in src (shared utility)
        src_path = Path(__file__).parent.parent.parent / "src" / "hubspot_pipeline"
        if str(src_path) not in sys.path:
            sys.path.insert(0, str(src_path))
        
        self.logger.debug(f"Added paths: {excel_path}, {src_path}")
    
    def validate_prerequisites(self) -> bool:
        """Check if all required modules are available"""
        try:
            # Test Excel import modules
            from excel_import import ExcelProcessor, SnapshotProcessor
            from excel_import.bigquery_loader import load_multiple_snapshots
            
            # Test BigQuery utilities
            from bigquery_utils import get_bigquery_client
            
            self.logger.info("✅ All required modules available")
            return True
            
        except ImportError as e:
            self.logger.error(f"❌ Missing required module: {e}")
            return False
    
    def _cleanup_previous_excel_imports(self):
        """Clean up all tables for fresh Excel import using TRUNCATE"""
        try:
            from bigquery_utils import get_bigquery_client
            
            client = get_bigquery_client(self.project_id)
            
            self.logger.info("🗑️ Cleaning up tables for fresh Excel import...")
            
            # Tables to truncate for clean Excel import
            tables_to_truncate = [
                ('hs_companies', 'companies'),
                ('hs_deals', 'deals'),
                ('hs_snapshot_registry', 'registry entries')
            ]
            
            for table_name, description in tables_to_truncate:
                try:
                    query = f"TRUNCATE TABLE `{self.project_id}.{self.staging_dataset}.{table_name}`"
                    job = client.query(query)
                    job.result()
                    self.logger.info(f"✅ Truncated {description} table")
                except Exception as e:
                    self.logger.warning(f"⚠️ Failed to truncate {description}: {e}")
                        
        except Exception as e:
            self.logger.warning(f"⚠️ Cleanup failed: {e}")
    
    def execute(self, excel_file: str = None, dry_run: bool = False) -> bool:
        """Execute Excel import using existing modules"""
        
        # If no excel_file specified, look in co-located import_data
        if excel_file is None:
            import_data_dir = Path(__file__).parent / "excel_import" / "import_data"
            excel_files = list(import_data_dir.glob("*.xlsx")) + list(import_data_dir.glob("*.xls"))
            
            if excel_files:
                excel_file = str(excel_files[0])  # Use first Excel file found
                self.logger.info(f"📂 Using auto-detected Excel file: {Path(excel_file).name}")
            else:
                self.logger.error(f"❌ No Excel files found in {import_data_dir}")
                return False
        
        self.logger.info(f"📥 Starting Excel import: {Path(excel_file).name}")
        
        try:
            # Validate prerequisites
            if not self.validate_prerequisites():
                return False
            
            # CLEANUP FIRST - TRUNCATE ALL TABLES FOR FRESH START
            if not dry_run:
                self._cleanup_previous_excel_imports()
            else:
                self.logger.info("🛑 DRY RUN: Skipping cleanup")
            
            # Import required modules
            from excel_import import ExcelProcessor, SnapshotProcessor
            from excel_import.bigquery_loader import load_multiple_snapshots
            
            # Validate Excel file exists
            if not Path(excel_file).exists():
                self.logger.error(f"❌ Excel file not found: {excel_file}")
                return False
            
            # Extract CRM metadata (will auto-use co-located import_data if file is there)
            excel_dir = Path(excel_file).parent
            import_data_dir = Path(__file__).parent / "excel_import" / "import_data"
            
            # Check if Excel file is in our co-located import_data
            if excel_dir == import_data_dir:
                crm_metadata = self._extract_crm_metadata()  # Use default location
            else:
                crm_metadata = self._extract_crm_metadata(excel_dir)  # Use Excel file's directory
            
            if not crm_metadata:
                self.logger.error("❌ No CRM CSV files found - required for timestamp extraction")
                self.logger.error(f"💡 Expected locations:")
                self.logger.error(f"   1. Same directory as Excel file: {excel_dir}")
                self.logger.error(f"   2. Co-located import_data: {import_data_dir}")
                return False
            
            self.logger.info(f"✅ Found CRM metadata for {len(crm_metadata)} snapshots")
            
            # Process Excel file using existing modules
            self.logger.info("🔄 Processing Excel file...")
            processor = ExcelProcessor(excel_file)
            snapshot_processor = SnapshotProcessor(processor)
            
            # Process snapshots with CRM metadata
            result = snapshot_processor.process_all_snapshots_with_crm_metadata(crm_metadata)
            snapshots_data = result['snapshots']
            
            if not snapshots_data:
                self.logger.error("❌ No snapshot data extracted")
                return False
            
            self.logger.info(f"✅ Extracted {len(snapshots_data)} snapshots")
            self.logger.info(f"📊 Total records: {result['totals']['total_records']}")
            
            # Set environment variables for BigQuery loader
            os.environ['BIGQUERY_PROJECT_ID'] = self.project_id
            os.environ['BIGQUERY_DATASET_ID'] = self.staging_dataset
            
            # Load to BigQuery using existing loader
            self.logger.info(f"📤 Loading to BigQuery (dry_run={dry_run})...")
            load_multiple_snapshots(snapshots_data, dry_run=dry_run)
            
            # Populate registry if not dry run
            if not dry_run:
                registry_count = self._populate_registry(snapshots_data, crm_metadata)
                self.logger.info(f"📋 Created {registry_count} registry entries")
            
            # Store results
            self.results = {
                'snapshots_imported': len(snapshots_data),
                'total_records': result['totals']['total_records'],
                'companies': result['totals']['companies'],
                'deals': result['totals']['deals'],
                'dry_run': dry_run,
                'excel_file': excel_file
            }
            
            self.completed = True
            self.logger.info("✅ Excel import completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Excel import failed: {e}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            return False
    
    def _extract_crm_metadata(self, import_dir: Path = None) -> Dict:
        """Extract CRM metadata - looks in excel_import/import_data/ by default"""
        try:
            # Use co-located import_data directory if no path specified
            if import_dir is None:
                import_dir = Path(__file__).parent / "excel_import" / "import_data"
            
            # Find CRM CSV files
            csv_files = list(import_dir.glob("hubspot-crm-exports-*.csv"))
            if not csv_files:
                self.logger.warning(f"⚠️ No CRM CSV files found in {import_dir}")
                self.logger.info(f"💡 Expected files:")
                self.logger.info(f"   📄 hubspot-crm-exports-weekly-status-company-YYYY-MM-DD.csv")
                self.logger.info(f"   📄 hubspot-crm-exports-weekly-status-deals-YYYY-MM-DD.csv")
                return {}
            
            self.logger.info(f"🔍 Found {len(csv_files)} CRM CSV files in {import_dir}")
            
            # Group files by date
            crm_metadata = {}
            
            for csv_file in csv_files:
                # Extract date from filename
                import re
                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', csv_file.name)
                if not date_match:
                    continue
                
                snapshot_date = date_match.group(1)
                
                # Get file timestamp (UPDATED: no microseconds)
                timestamp = self._get_file_timestamp(csv_file)
                
                # Initialize metadata for this date
                if snapshot_date not in crm_metadata:
                    crm_metadata[snapshot_date] = {
                        'company_file': None,
                        'deals_file': None,
                        'company_timestamp': None,
                        'deals_timestamp': None
                    }
                
                # Classify file type
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
                    # Use earlier timestamp as snapshot_id
                    company_time = files['company_timestamp']
                    deals_time = files['deals_timestamp']
                    snapshot_id = min(company_time, deals_time) if company_time and deals_time else company_time or deals_time
                    
                    final_metadata[snapshot_date] = {
                        'snapshot_id': snapshot_id,
                        **files
                    }
            
            self.logger.info(f"✅ CRM metadata for {len(final_metadata)} complete snapshots")
            return final_metadata
            
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to extract CRM metadata: {e}")
            return {}
    
    def _get_file_timestamp(self, file_path: Path) -> str:
        """Get file modification timestamp without microseconds (UPDATED)"""
        try:
            mtime = file_path.stat().st_mtime
            dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
            
            # Remove microseconds but keep exact time
            dt_no_microsec = dt.replace(microsecond=0)
            
            # Format: YYYY-MM-DDTHH:MM:SSZ (no microseconds)
            return dt_no_microsec.strftime('%Y-%m-%dT%H:%M:%SZ')
            
        except Exception:
            # Fallback to current time without microseconds
            return datetime.now(timezone.utc).replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    def _populate_registry(self, snapshots_data: Dict, crm_metadata: Dict) -> int:
        """Populate snapshot registry for scoring readiness"""
        try:
            from bigquery_utils import get_bigquery_client, insert_rows_with_smart_retry
            
            client = get_bigquery_client(self.project_id)
            table_ref = f"{self.project_id}.{self.staging_dataset}.hs_snapshot_registry"
            
            # Create registry entries (table was already truncated in cleanup)
            registry_entries = []
            # Use current timestamp WITH microseconds for record_timestamp
            current_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
            for snapshot_id, snapshot_data in snapshots_data.items():
                companies = snapshot_data.get('companies', [])
                deals = snapshot_data.get('deals', [])
                
                registry_entry = {
                    'snapshot_id': snapshot_id,
                    'record_timestamp': current_time,
                    'triggered_by': 'excel_import',
                    'status': 'completed',
                    'notes': f"Excel+CRM import | Companies: {len(companies)}, Deals: {len(deals)}"
                }
                
                registry_entries.append(registry_entry)
            
            # Insert new registry entries
            if registry_entries:
                insert_rows_with_smart_retry(
                    client, 
                    table_ref, 
                    registry_entries, 
                    operation_name="Excel snapshot registry"
                )
            
            return len(registry_entries)
            
        except Exception as e:
            self.logger.warning(f"⚠️ Registry population failed: {e}")
            return 0
    
    def get_results(self) -> Dict:
        """Get execution results"""
        return {
            'completed': self.completed,
            'results': self.results
        }


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Excel Import Step")
    parser.add_argument('--excel-file', help='Path to Excel file (auto-detects if not specified)')
    parser.add_argument('--project', default='hubspot-452402', help='BigQuery project')
    parser.add_argument('--dataset', default='Hubspot_staging', help='BigQuery dataset')
    parser.add_argument('--dry-run', action='store_true', help='Preview only')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    parser.add_argument('--check-auth', action='store_true', help='Check authentication and datasets')
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    # Check authentication if requested
    if args.check_auth:
        print("🔍 AUTHENTICATION & ACCESS CHECK")
        print("=" * 50)
        
        # Check gcloud user
        try:
            import subprocess
            result = subprocess.run("gcloud config get-value account", shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"👤 Authenticated user: {result.stdout.strip()}")
            else:
                print("❌ No gcloud authentication")
        except Exception as e:
            print(f"❌ Error checking gcloud auth: {e}")
        
        # Check available datasets
        try:
            result = subprocess.run(f"bq ls {args.project}", shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"📊 Available datasets in {args.project}:")
                for line in result.stdout.strip().split('\n')[2:]:  # Skip header
                    if line.strip():
                        dataset_id = line.split()[0]
                        print(f"   • {dataset_id}")
            else:
                print(f"❌ Cannot list datasets: {result.stderr}")
        except Exception as e:
            print(f"❌ Error checking datasets: {e}")
        
        # Check specific table
        try:
            table_ref = f"{args.project}:{args.dataset}.hs_companies"
            result = subprocess.run(f"bq show {table_ref}", shell=True, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Table exists: {table_ref}")
            else:
                print(f"❌ Table missing or no access: {table_ref}")
                print(f"   Error: {result.stderr.strip()}")
        except Exception as e:
            print(f"❌ Error checking table: {e}")
        
        return 0
    
    # Create and run step
    step = ExcelImportStep(args.project, args.dataset)
    
    print(f"🚀 Excel Import Step")
    if args.excel_file:
        print(f"File: {args.excel_file}")
    else:
        print(f"File: Auto-detect from excel_import/import_data/")
    print(f"Target: {args.project}.{args.dataset}")
    print(f"Dry run: {args.dry_run}")
    
    success = step.execute(args.excel_file, args.dry_run)
    
    if success:
        results = step.get_results()
        print(f"\n✅ Import completed!")
        print(f"📊 Results: {results['results']}")
    else:
        print(f"\n❌ Import failed!")
        
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())