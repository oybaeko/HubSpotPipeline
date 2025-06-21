#!/usr/bin/env python3
"""
Step 5: Data Integrity Verification - Main Entry Point
Comprehensive data quality and referential integrity checks for staging environment
"""

import sys
import os
import logging
import argparse
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Add integrity_test module to path
integrity_test_path = Path(__file__).parent / "integrity_test"
sys.path.insert(0, str(integrity_test_path))

try:
    # Use absolute imports to avoid relative import issues
    from integrity_test.data_integrity_step import DataIntegrityStep
    INTEGRITY_TEST_AVAILABLE = True
except ImportError as e:
    print(f"❌ Failed to import integrity test modules: {e}")
    print(f"💡 Make sure integrity_test/ directory exists with all required modules")
    INTEGRITY_TEST_AVAILABLE = False

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Data Integrity Verification Step")
    parser.add_argument('--project', default='hubspot-452402', help='BigQuery project')
    parser.add_argument('--dataset', default='Hubspot_staging', help='Staging dataset')
    parser.add_argument('--save-report', action='store_true', help='Save report to file')
    parser.add_argument('--report-format', choices=['console', 'json', 'text', 'both'], 
                       default='console', help='Report format')
    parser.add_argument('--output-dir', help='Output directory for reports')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    parser.add_argument('--check-prereqs', action='store_true', help='Check prerequisites only')
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    # Check if integrity test modules are available
    if not INTEGRITY_TEST_AVAILABLE:
        print("❌ Integrity test modules not available")
        print("💡 Expected structure:")
        print("   build-staging/steps/integrity_test/")
        print("   ├── __init__.py")
        print("   ├── models.py") 
        print("   ├── config.py")
        print("   ├── integrity_checks.py")
        print("   ├── report_generator.py")
        print("   └── data_integrity_step.py")
        return 1
    
    # Create integrity verification step
    step = DataIntegrityStep(args.project, args.dataset)
    
    print(f"🔍 Data Integrity Verification Step")
    print(f"Project: {args.project}")
    print(f"Dataset: {args.dataset}")
    print(f"Save Report: {args.save_report}")
    print(f"Report Format: {args.report_format}")
    if args.output_dir:
        print(f"Output Directory: {args.output_dir}")
    
    # Check prerequisites only
    if args.check_prereqs:
        print("\n🔍 CHECKING PREREQUISITES...")
        if step.validate_prerequisites():
            print("✅ All prerequisites satisfied")
            return 0
        else:
            print("❌ Prerequisites not met")
            return 1
    
    # Execute integrity verification
    try:
        success = step.execute(
            save_report=args.save_report,
            report_format=args.report_format,
            output_dir=args.output_dir
        )
        
        if success:
            print(f"\n✅ Data integrity verification completed!")
            
            # Determine exit code based on critical issues
            critical_issues = step.get_critical_issue_count()
            if critical_issues == 0:
                print(f"🎉 No critical issues found - data integrity is good!")
                return_code = 0
            else:
                print(f"⚠️ {critical_issues} critical issues found - requires attention")
                return_code = 1
            
            step.show_status()
            return return_code
        else:
            print(f"\n❌ Data integrity verification failed!")
            return 1
            
    except KeyboardInterrupt:
        print(f"\n⚠️ Operation cancelled by user")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Data Integrity Verification Step")
    parser.add_argument('--project', default='hubspot-452402', help='BigQuery project')
    parser.add_argument('--dataset', default='Hubspot_staging', help='Staging dataset')
    parser.add_argument('--save-report', action='store_true', help='Save report to file')
    parser.add_argument('--report-format', choices=['console', 'json', 'text', 'both'], 
                       default='console', help='Report format')
    parser.add_argument('--output-dir', help='Output directory for reports')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    parser.add_argument('--check-prereqs', action='store_true', help='Check prerequisites only')
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    # Check if integrity test modules are available
    if not INTEGRITY_TEST_AVAILABLE:
        print("❌ Integrity test modules not available")
        print("💡 Expected structure:")
        print("   build-staging/steps/integrity_test/")
        print("   ├── __init__.py")
        print("   ├── models.py") 
        print("   ├── config.py")
        print("   ├── integrity_checks.py")
        print("   ├── report_generator.py")
        print("   └── data_integrity_step.py")
        return 1
    
    # Create integrity verification step
    step = DataIntegrityStep(args.project, args.dataset)
    
    print(f"🔍 Data Integrity Verification Step")
    print(f"Project: {args.project}")
    print(f"Dataset: {args.dataset}")
    print(f"Save Report: {args.save_report}")
    print(f"Report Format: {args.report_format}")
    if args.output_dir:
        print(f"Output Directory: {args.output_dir}")
    
    # Check prerequisites only
    if args.check_prereqs:
        print("\n🔍 CHECKING PREREQUISITES...")
        if step.validate_prerequisites():
            print("✅ All prerequisites satisfied")
            return 0
        else:
            print("❌ Prerequisites not met")
            return 1
    
    # Execute integrity verification
    try:
        success = step.execute(
            save_report=args.save_report,
            report_format=args.report_format,
            output_dir=args.output_dir
        )
        
        if success:
            print(f"\n✅ Data integrity verification completed!")
            
            # Determine exit code based on critical issues
            critical_issues = step.get_critical_issue_count()
            if critical_issues == 0:
                print(f"🎉 No critical issues found - data integrity is good!")
                return_code = 0
            else:
                print(f"⚠️ {critical_issues} critical issues found - requires attention")
                return_code = 1
            
            step.show_status()
            return return_code
        else:
            print(f"\n❌ Data integrity verification failed!")
            return 1
            
    except KeyboardInterrupt:
        print(f"\n⚠️ Operation cancelled by user")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())