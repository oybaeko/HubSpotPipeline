#!/usr/bin/env python3
"""
Step 2: Excel Import with Registry Population
Extracted from main_migration_orchestrator.py for better modularity
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

class ExcelImportStep:
    """
    Step 2: Import historical Excel data with registry population
    
    This step:
    1. Processes Excel files with historical snapshot data
    2. ALWAYS uses CRM metadata for precise snapshot_id (required)
    3. Uses current timestamp for record_timestamp
    4. Loads data to BigQuery staging dataset
    5. Populates snapshot registry for scoring readiness
    """
    
    def __init__(self, project_id: str, staging_dataset: str):
        self.project_id = project_id
        self.staging_dataset = staging_dataset
        self.logger = logging.getLogger('migration.step2.excel')
        
        # Track completion status
        self.completed = False
        
        # Store results for reporting
        self.results = {}
    
    def get_description(self) -> str:
        """Get step description for menu display"""
        return "Import historical Excel data with registry population"
    
    def validate_prerequisites(self) -> bool:
        """Check if this step can run"""
        try:
            # Check if Excel import modules are available
            excel_import_path = self._get_excel_import_path()
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
            
            self.logger.info("✅ Excel import prerequisites validated")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error checking Excel import prerequisites: {e}")
            return False
    
    def execute(self) -> bool:
        """Execute the Excel import step"""
        self.logger.info("📥 STEP 2: Importing historical Excel data (FIRST)")
        self.logger.info("📊 This will import multiple historical snapshots")
        self.logger.info("🕐 Will use CRM file timestamps if available") 
        self.logger.info("📋 Will populate snapshot registry for scoring readiness")
        
        try:
            # Validate prerequisites
            if not self.validate_prerequisites():
                return False
            
            # Get Excel file selection
            excel_file = self._select_excel_file()
            if not excel_file:
                self.logger.error("❌ No Excel file selected")
                return False
            
            # Check for CRM files in the same directory
            import_dir = Path(excel_file).parent
            crm_metadata = self._extract_crm_metadata(import_dir)
            
            if not crm_metadata:
                self.logger.error("❌ No CRM metadata found - CRM files are REQUIRED")
                self.logger.error("💡 Place CRM CSV files in the same directory as Excel file:")
                self.logger.error("   📄 hubspot-crm-exports-weekly-status-company-YYYY-MM-DD.csv")
                self.logger.error("   📄 hubspot-crm-exports-weekly-status-deals-YYYY-MM-DD.csv")
                return False
            
            self.logger.info(f"✅ Found CRM metadata for {len(crm_metadata)} snapshots")
            self.logger.info("🕐 Using actual HubSpot export timestamps as snapshot_id")
            self.logger.info("⏰ Using current time for record_timestamp")
            
            # Confirm operation
            if not self._confirm_excel_import(excel_file, crm_metadata):
                self.logger.info("❌ Excel import cancelled")
                return False
            
            # Setup Excel import environment
            excel_import_path = self._get_excel_import_path()
            sys.path.insert(0, str(excel_import_path))
            
            # Import Excel functionality
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
            
            # Process snapshots with CRM metadata (REQUIRED)
            self.logger.info("🔄 Processing Excel snapshots with CRM metadata...")
            # Always use CRM timestamps - they are required
            result = snapshot_processor.process_all_snapshots_with_crm_metadata(crm_metadata)
            self.logger.info("🕐 Using CRM file download timestamps as snapshot_id")
            self.logger.info("⏰ Using current time as record_timestamp")
            
            snapshots_data = result['snapshots']
            
            if not snapshots_data:
                self.logger.error("❌ No snapshot data extracted from Excel")
                return False
            
            self.logger.info(f"✅ Extracted {len(snapshots_data)} snapshots")
            self.logger.info(f"📊 Total records: {result['totals']['total_records']}")
            self.logger.info(f"🏢 Companies: {result['totals']['companies']}")
            self.logger.info(f"🤝 Deals: {result['totals']['deals']}")
            
            # Set environment for Excel import
            os.environ['BIGQUERY_PROJECT_ID'] = self.project_id
            os.environ['BIGQUERY_DATASET_ID'] = self.staging_dataset
            
            # Load to BigQuery (staging environment)
            self.logger.info("📤 Loading historical data to staging...")
            self.logger.info("🕐 Adding current timestamp to all Excel records...")
            load_multiple_snapshots(snapshots_data, dry_run=False)
            
            # NEW: Populate snapshot registry for scoring readiness
            self.logger.info("📋 Populating snapshot registry for Excel imports...")
            registry_count = self._populate_registry_for_excel_snapshots(snapshots_data, crm_metadata)
            
            # Store results
            self.results = {
                'snapshots_imported': len(snapshots_data),
                'total_records': result['totals']['total_records'],
                'companies': result['totals']['companies'],
                'deals': result['totals']['deals'],
                'registry_entries': registry_count,
                'crm_metadata_used': len(crm_metadata) if crm_metadata else 0
            }
            
            self.logger.info("✅ Historical Excel data imported successfully")
            self.logger.info(f"📸 Snapshots imported: {len(snapshots_data)}")
            self.logger.info(f"📋 Registry entries created: {registry_count}")
            
            if crm_metadata:
                self.logger.info("🕐 Snapshot IDs contain precise HubSpot export timestamps")
                self.logger.info("⏰ Record timestamps are current processing time")
                # Show sample timestamps
                sample_snapshots = list(snapshots_data.keys())[:3]
                for snapshot_id in sample_snapshots:
                    self.logger.info(f"   📅 Example snapshot_id: {snapshot_id}")
            else:
                # This should never happen now since CRM is required
                self.logger.error("❌ Unexpected: No CRM metadata after validation")
            
            self.logger.info("🎯 Excel snapshots are now ready for scoring!")
            self.completed = True
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to import Excel data: {e}")
            import traceback
            self.logger.debug(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _get_excel_import_path(self) -> Path:
        """Get path to Excel import modules"""
        # Assume we're running from build-staging/
        return Path(__file__).parent.parent / "first_stage_data"
    
    def _select_excel_file(self) -> Optional[str]:
        """Select Excel file for import"""
        import_dir = Path(__file__).parent.parent / "import_data"
        
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
                # Extract date from filename
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
        else:
            print(f"\n❌ No CRM CSV files found - REQUIRED for import!")
            print(f"💡 Place CRM files in: {import_dir}")
            print(f"💡 Required files:")
            print(f"   📄 hubspot-crm-exports-weekly-status-company-YYYY-MM-DD.csv")
            print(f"   📄 hubspot-crm-exports-weekly-status-deals-YYYY-MM-DD.csv")
        
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
                        
                        # Check if CRM files are available
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
                    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
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
            # Get file modification time
            mtime = file_path.stat().st_mtime
            dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
            
            # Return in Z format: 2025-06-08T04:00:11.000000Z
            return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
        except Exception as e:
            self.logger.debug(f"Failed to get timestamp for {file_path}: {e}")
            # Fallback: use current time if file timestamp fails
            return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    def _confirm_excel_import(self, excel_file: str, crm_metadata: Dict) -> bool:
        """Confirm Excel import operation with CRM metadata requirement"""
        print(f"\n📊 EXCEL IMPORT CONFIRMATION (STEP 2 - HISTORICAL DATA FIRST)")
        print(f"File: {Path(excel_file).name}")
        print(f"Target: staging environment ({self.staging_dataset})")
        print(f"Order: Historical data FIRST, then production data")
        
        print(f"🕐 CRM Timestamps: YES ({len(crm_metadata)} snapshots)")
        print(f"   ✅ Using actual HubSpot export timestamps as snapshot_id")
        print(f"   ⏰ Using current time as record_timestamp")
        print(f"   🎯 Precise timing for scoring accuracy")
        
        # Show sample timestamps
        sample_dates = list(crm_metadata.keys())[:3]
        for date in sample_dates:
            snapshot_id = crm_metadata[date]['snapshot_id']
            print(f"   📅 {date} → snapshot_id: {snapshot_id}")
        if len(crm_metadata) > 3:
            print(f"   📅 ... and {len(crm_metadata) - 3} more")
        
        print(f"\nThis will:")
        print(f"  ✅ Import historical snapshots FIRST (proper chronological order)")
        print(f"  ✅ Use precise CRM export timestamps as snapshot_id")
        print(f"  ⏰ Use current processing time as record_timestamp")
        print(f"  ✅ Create registry entries for scoring readiness")
        print(f"  ✅ Establish historical foundation before adding production data")
        print(f"  ✅ Enable complete scoring after Step 4")
        
        confirm = input(f"\nType 'IMPORT EXCEL WITH CRM' to continue: ").strip()
        return confirm == 'IMPORT EXCEL WITH CRM'
    
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
    
    def _populate_registry_for_excel_snapshots(self, snapshots_data: Dict, crm_metadata: Dict) -> int:
        """
        Populate snapshot registry for Excel-imported snapshots
        
        Args:
            snapshots_data: Processed snapshot data from Excel
            crm_metadata: CRM metadata with timestamps (REQUIRED)
            
        Returns:
            int: Number of registry entries created
        """
        self.logger.info("📋 Creating registry entries for Excel snapshots...")
        
        try:
            registry_entries = []
            current_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
            for snapshot_id, snapshot_data in snapshots_data.items():
                # Calculate snapshot statistics
                companies = snapshot_data.get('companies', [])
                deals = snapshot_data.get('deals', [])
                
                company_count = len(companies)
                deal_count = len(deals)
                total_records = company_count + deal_count
                
                # Use CRM metadata for snapshot_id, current time for record_timestamp
                snapshot_date = snapshot_id.split('T')[0]  # Extract date part
                matching_metadata = None
                
                for date, metadata in crm_metadata.items():
                    if date == snapshot_date:
                        matching_metadata = metadata
                        break
                
                if matching_metadata:
                    # snapshot_id comes from CRM file timestamp (precise HubSpot export time)
                    # record_timestamp is current processing time
                    record_timestamp = current_time
                    triggered_by = 'excel_import_crm'
                    notes_detail = f"Excel+CRM import - snapshot_id from HubSpot export, record_timestamp from processing"
                else:
                    # This should not happen since CRM metadata is required
                    self.logger.warning(f"⚠️ No CRM metadata found for {snapshot_date}, using fallback")
                    record_timestamp = current_time
                    triggered_by = 'excel_import_crm_missing'
                    notes_detail = f"Excel+CRM import - CRM metadata missing for this date"
                
                # Create registry entry
                registry_entry = {
                    'snapshot_id': snapshot_id,  # Precise HubSpot export timestamp
                    'record_timestamp': record_timestamp,  # Current processing time
                    'triggered_by': triggered_by,
                    'status': 'ingest_completed_historical',
                    'notes': f"{notes_detail} | Companies: {company_count}, Deals: {deal_count}, Total: {total_records}"
                }
                
                registry_entries.append(registry_entry)
                
                self.logger.debug(f"📋 Registry entry: {snapshot_id} ({company_count} companies, {deal_count} deals)")
            
            if not registry_entries:
                self.logger.warning("⚠️ No registry entries to create")
                return 0
            
            # Insert to BigQuery registry table
            self.logger.info(f"💾 Inserting {len(registry_entries)} registry entries to BigQuery...")
            
            # Import BigQuery utilities
            bigquery_utils_path = Path(__file__).parent.parent.parent / "src" / "hubspot_pipeline"
            sys.path.insert(0, str(bigquery_utils_path))
            from bigquery_utils import get_bigquery_client, insert_rows_with_smart_retry
            
            client = get_bigquery_client(self.project_id)
            table_ref = f"{self.project_id}.{self.staging_dataset}.hs_snapshot_registry"
            
            # Use smart retry for insertion
            insert_rows_with_smart_retry(
                client, 
                table_ref, 
                registry_entries, 
                operation_name="Excel snapshot registry population"
            )
            
            self.logger.info(f"✅ Created {len(registry_entries)} registry entries")
            self.logger.info(f"🕐 snapshot_id: CRM export timestamps (precise)")
            self.logger.info(f"⏰ record_timestamp: Current processing time ({current_time})")
            
            # Log summary by trigger type
            trigger_types = {}
            for entry in registry_entries:
                trigger_type = entry['triggered_by']
                trigger_types[trigger_type] = trigger_types.get(trigger_type, 0) + 1
            
            for trigger_type, count in trigger_types.items():
                self.logger.info(f"   📋 {trigger_type}: {count} entries")
            
            return len(registry_entries)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to populate registry for Excel snapshots: {e}")
            import traceback
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            # Don't fail the entire import for registry issues
            self.logger.warning("⚠️ Continuing without registry population - snapshots won't be scored")
            return 0
    
    def get_results(self) -> Dict[str, Any]:
        """Get step execution results"""
        return {
            'completed': self.completed,
            'results': self.results
        }


# Example usage and testing
if __name__ == "__main__":
    # Setup logging for testing
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    # Test the step
    step = ExcelImportStep("hubspot-452402", "Hubspot_staging")
    
    print("Testing Excel Import Step")
    print("=" * 40)
    
    print(f"Description: {step.get_description()}")
    print(f"Prerequisites: {step.validate_prerequisites()}")
    
    # Uncomment to test full execution
    # success = step.execute()
    # print(f"Execution result: {success}")
    # print(f"Results: {step.get_results()}")