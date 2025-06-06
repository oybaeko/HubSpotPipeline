#!/usr/bin/env python3
"""
Local Excel import script for HubSpot pipeline - Companies and Deals only.
This script is designed for local use only and should not be deployed to GCP.

Usage:
    python import_excel.py path/to/hubspot_export.xlsx
    python import_excel.py path/to/hubspot_export.xlsx --dry-run
    python import_excel.py path/to/hubspot_export.xlsx --mode auto
    python import_excel.py path/to/hubspot_export.xlsx --mode snapshots
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime
import argparse

# Add src to path for local imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from hubspot_pipeline.excel_import import ExcelProcessor, SnapshotProcessor
from hubspot_pipeline.excel_import.data_mapper import map_excel_to_schema, get_snapshot_configurations
from hubspot_pipeline.excel_import.bigquery_loader import load_to_bigquery, load_multiple_snapshots

def setup_environment():
    """Setup environment for local BigQuery access"""
    # Check if required packages are installed
    try:
        import pandas
        import openpyxl
        from google.cloud import bigquery
        from dotenv import load_dotenv
    except ImportError as e:
        logging.error(f"Missing required package: {e}")
        logging.error("Please install Excel import dependencies:")
        logging.error("  pip install -r excel_import_requirements.txt")
        sys.exit(1)
    
    # Load environment variables
    load_dotenv()
    
    # Verify required environment variables
    required_vars = ["BIGQUERY_PROJECT_ID", "BIGQUERY_DATASET_ID", "GOOGLE_APPLICATION_CREDENTIALS"]
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        logging.error(f"Missing required environment variables: {missing}")
        logging.error("Please ensure your .env file contains:")
        logging.error("- BIGQUERY_PROJECT_ID=your-project-id")
        logging.error("- BIGQUERY_DATASET_ID=your-dataset-id") 
        logging.error("- GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json")
        sys.exit(1)

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Import HubSpot Excel exports to BigQuery (Companies and Deals)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Import Modes:
  snapshots   Import specific configured snapshots (default)
  auto        Auto-detect HubSpot sheets and import as single snapshot
  validate    Validate Excel file structure without importing

Examples:
  python import_excel.py hubspot_export.xlsx --dry-run
  python import_excel.py hubspot_export.xlsx --mode snapshots
  python import_excel.py hubspot_export.xlsx --mode auto --snapshot-id "manual-import"
  python import_excel.py hubspot_export.xlsx --mode validate
        """
    )
    
    parser.add_argument("excel_file", help="Path to Excel file containing HubSpot exports")
    parser.add_argument("--mode", choices=["snapshots", "auto", "validate"], default="snapshots",
                       help="Import mode (default: snapshots)")
    parser.add_argument("--snapshot-id", help="Custom snapshot ID for auto mode (default: current timestamp)")
    parser.add_argument("--dry-run", action="store_true", 
                       help="Preview data without writing to BigQuery")
    parser.add_argument("--log-level", default="INFO", 
                       choices=["DEBUG", "INFO", "WARN", "ERROR"],
                       help="Set logging level")
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    logger = logging.getLogger('hubspot.excel_import')
    
    # Validate Excel file exists
    excel_path = Path(args.excel_file)
    if not excel_path.exists():
        logger.error(f"❌ Excel file not found: {excel_path}")
        sys.exit(1)
    
    # Setup environment (skip for validate mode)
    if args.mode != "validate":
        try:
            setup_environment()
            logger.info("✅ Environment setup complete")
        except Exception as e:
            logger.error(f"❌ Environment setup failed: {e}")
            sys.exit(1)
    
    logger.info("=" * 80)
    logger.info("🚀 HubSpot Excel Import Starting")
    logger.info("=" * 80)
    logger.info(f"📂 Excel file: {excel_path.absolute()}")
    logger.info(f"🔧 Mode: {args.mode}")
    logger.info(f"🛑 Dry run: {'Yes' if args.dry_run else 'No'}")
    if args.mode != "validate":
        logger.info(f"📊 Project: {os.getenv('BIGQUERY_PROJECT_ID')}")
        logger.info(f"📊 Dataset: {os.getenv('BIGQUERY_DATASET_ID')}")
    logger.info("=" * 80)
    
    try:
        processor = ExcelProcessor(str(excel_path))
        
        if args.mode == "validate":
            _validate_mode(processor, logger)
        elif args.mode == "snapshots":
            _snapshots_mode(processor, logger, args.dry_run)
        elif args.mode == "auto":
            _auto_mode(processor, logger, args.snapshot_id, args.dry_run)
        
    except KeyboardInterrupt:
        logger.info("⚠️ Import cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error("=" * 80)
        logger.error("❌ IMPORT FAILED")
        logger.error(f"Error: {e}")
        logger.error("=" * 80)
        
        if args.log_level == "DEBUG":
            logger.exception("Full error details:")
        else:
            logger.error("Run with --log-level DEBUG for detailed error information")
        
        sys.exit(1)

def _validate_mode(processor: ExcelProcessor, logger):
    """Validation mode - check file structure"""
    logger.info("🔍 VALIDATION MODE - Checking Excel file structure")
    
    # Get all sheets
    available_sheets = processor.get_available_sheets()
    logger.info(f"📋 Found {len(available_sheets)} sheets in Excel file")
    
    # Check configured snapshots
    found_sheets, missing_sheets = processor.validate_snapshot_sheets()
    
    logger.info(f"\n📸 Snapshot Configuration Analysis:")
    logger.info(f"  • Expected sheets: {len(found_sheets) + len(missing_sheets)}")
    logger.info(f"  • Found sheets: {len(found_sheets)}")
    logger.info(f"  • Missing sheets: {len(missing_sheets)}")
    
    if missing_sheets:
        logger.warning("\n❌ Missing expected sheets:")
        for sheet in missing_sheets:
            logger.warning(f"  • {sheet}")
    
    # Try auto-detection
    logger.info("\n🔍 Auto-detection Analysis:")
    hubspot_sheets = processor.extract_hubspot_sheets()
    
    if hubspot_sheets:
        logger.info(f"✅ Auto-detected {len(hubspot_sheets)} HubSpot sheets:")
        for sheet_name, df in hubspot_sheets.items():
            logger.info(f"  • {sheet_name}: {len(df)} rows")
    else:
        logger.warning("⚠️ No sheets auto-detected as HubSpot exports")
    
    # Show configured snapshots
    snapshots = get_snapshot_configurations()
    logger.info(f"\n📸 Configured snapshots ({len(snapshots)}):")
    for snapshot in snapshots:
        date = snapshot["date"]
        company_status = "✅" if snapshot["company_sheet"] in available_sheets else "❌"
        deal_status = "✅" if snapshot["deal_sheet"] in available_sheets else "❌"
        logger.info(f"  • {date}: Companies {company_status} Deals {deal_status}")
    
    logger.info("\n✅ VALIDATION COMPLETED")

def _snapshots_mode(processor: ExcelProcessor, logger, dry_run: bool):
    """Snapshots mode - import configured snapshots"""
    logger.info("📸 SNAPSHOTS MODE - Importing configured snapshots")
    
    # Validate first
    found_sheets, missing_sheets = processor.validate_snapshot_sheets()
    
    if missing_sheets:
        logger.error(f"❌ Missing {len(missing_sheets)} required sheets")
        logger.error("Run with --mode validate to see details")
        raise RuntimeError("Missing required sheets for snapshot import")
    
    # Process all snapshots
    snapshot_processor = SnapshotProcessor(processor)
    result = snapshot_processor.process_all_snapshots()
    
    # Load to BigQuery
    snapshots_data = result['snapshots']
    load_multiple_snapshots(snapshots_data, dry_run=dry_run)

def _auto_mode(processor: ExcelProcessor, logger, snapshot_id: str, dry_run: bool):
    """Auto mode - auto-detect and import as single snapshot"""
    logger.info("🔍 AUTO MODE - Auto-detecting HubSpot sheets")
    
    # Generate snapshot ID
    if not snapshot_id:
        snapshot_id = datetime.utcnow().isoformat(timespec='seconds')
    
    logger.info(f"📸 Using snapshot ID: {snapshot_id}")
    
    # Auto-detect sheets
    sheet_data = processor.extract_hubspot_sheets()
    
    if not sheet_data:
        logger.error("❌ No HubSpot export sheets found with auto-detection")
        logger.error("Try --mode validate to analyze the file structure")
        raise RuntimeError("No HubSpot sheets detected")
    
    # Map and load data
    mapped_data = map_excel_to_schema(sheet_data, snapshot_id)
    
    total_records = sum(len(records) for records in mapped_data.values())
    logger.info(f"✅ Mapped {total_records} total records")
    
    for data_type, records in mapped_data.items():
        logger.info(f"  • {data_type}: {len(records)} records")
    
    # Load to BigQuery
    load_to_bigquery(mapped_data, dry_run=dry_run)
    
    # Summary
    logger.info("=" * 80)
    if dry_run:
        logger.info("🛑 DRY RUN COMPLETED - No data was written to BigQuery")
        logger.info("Remove --dry-run flag to actually load the data")
    else:
        logger.info("✅ AUTO IMPORT COMPLETED SUCCESSFULLY")
        logger.info(f"📸 Data loaded with snapshot ID: {snapshot_id}")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()