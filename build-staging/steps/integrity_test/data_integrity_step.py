#!/usr/bin/env python3
"""
Main data integrity verification step - UPDATED VERSION with lowercase normalization
"""

import os
import logging
from datetime import datetime
from typing import Dict, Any
from google.cloud import bigquery

from models import IntegrityReport
from config import CORE_TABLES
from integrity_checks import IntegrityChecker
from report_generator import ReportGenerator

class DataIntegrityStep:
    """
    Step 5: Comprehensive data integrity verification
    
    This step:
    1. Checks referential integrity (foreign key relationships)
    2. Validates data formats and constraints
    3. Identifies orphaned records
    4. Checks for blank/empty reference fields
    5. Validates snapshot consistency
    6. Checks lowercase normalization for consistent filtering
    7. Generates comprehensive integrity report
    """
    
    def __init__(self, project_id: str = "hubspot-452402", dataset: str = "Hubspot_staging"):
        self.project_id = project_id
        self.staging_dataset = dataset
        
        # Setup logging
        self.logger = logging.getLogger('data_integrity_step')
        
        # Setup environment
        self._setup_environment()
        
        # Initialize components
        self.checker = IntegrityChecker(project_id, dataset)
        self.report_generator = ReportGenerator()
        
        # Track results
        self.results = {}
        self.completed = False
        self.integrity_report = None
    
    def _setup_environment(self):
        """Setup environment and clear service account credentials"""
        # Clear service account credentials to use user auth
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
            self.logger.debug("Cleared GOOGLE_APPLICATION_CREDENTIALS to use user auth")
    
    def validate_prerequisites(self) -> bool:
        """Check if BigQuery access and required tables exist"""
        try:
            # Test BigQuery client
            client = bigquery.Client(project=self.project_id)
            
            # Test staging dataset access
            staging_dataset_ref = client.dataset(self.staging_dataset)
            list(client.list_tables(staging_dataset_ref, max_results=1))
            self.logger.info("✅ Staging dataset access confirmed")
            
            # Check if core tables exist
            missing_tables = []
            
            for table in CORE_TABLES:
                try:
                    table_ref = f"{self.project_id}.{self.staging_dataset}.{table}"
                    client.get_table(table_ref)
                except Exception:
                    missing_tables.append(table)
            
            if missing_tables:
                self.logger.error(f"❌ Missing core tables: {missing_tables}")
                return False
            
            self.logger.info("✅ Core tables exist")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Prerequisites check failed: {e}")
            return False
    
    def create_verification_registry_entry(self, client: bigquery.Client):
        """Create registry entry for integrity verification - FIXED timestamp handling"""
        try:
            # FIXED: Use proper timestamp format that BigQuery can handle
            verification_timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            verification_id = f"integrity_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            notes = f"Data integrity verification | Issues: {self.integrity_report.total_issues_found} | Score: {self.integrity_report.summary['integrity_score']}/100"
            
            # FIXED: Use PARSE_TIMESTAMP to properly handle the verification timestamp
            registry_query = f"""
            INSERT INTO `{self.project_id}.{self.staging_dataset}.hs_snapshot_registry` 
            (snapshot_id, record_timestamp, triggered_by, status, notes)
            VALUES (
                PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', @verification_timestamp),
                CURRENT_TIMESTAMP(),
                'integrity_verification_step5',
                'completed',
                @notes
            )
            """
            
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("verification_timestamp", "STRING", verification_timestamp),
                    bigquery.ScalarQueryParameter("notes", "STRING", notes)
                ]
            )
            
            client.query(registry_query, job_config=job_config).result()
            self.logger.info(f"📝 Created verification registry entry: {verification_id}")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Could not create verification registry entry: {e}")
            self.logger.debug(f"Registry entry error details: {e}")
    
    def execute(self, save_report: bool = True, report_format: str = 'console', 
               output_dir: str = None) -> bool:
        """Execute data integrity verification"""
        self.logger.info("🔍 STEP 5: Starting data integrity verification")
        
        try:
            # Validate prerequisites
            if not self.validate_prerequisites():
                return False
            
            # Initialize BigQuery client
            client = bigquery.Client(project=self.project_id)
            
            # Get table record counts
            table_counts = self.checker.get_table_record_counts(client)
            
            if sum(table_counts.values()) == 0:
                self.logger.warning("⚠️ No data found in staging tables")
                return False
            
            # Run all integrity checks
            all_issues = []
            
            self.logger.info("🔍 Running comprehensive integrity checks...")
            
            # 1. Check blank reference fields
            blank_issues = self.checker.check_blank_reference_fields(client)
            all_issues.extend(blank_issues)
            
            # 2. Check referential integrity
            ref_issues = self.checker.check_referential_integrity(client)
            all_issues.extend(ref_issues)
            
            # 3. Check required fields
            required_issues = self.checker.check_required_fields(client)
            all_issues.extend(required_issues)
            
            # 4. Check format validations
            format_issues = self.checker.check_format_validations(client)
            all_issues.extend(format_issues)
            
            # 5. NEW: Check lowercase normalization
            lowercase_issues = self.checker.check_lowercase_normalization(client)
            all_issues.extend(lowercase_issues)
            
            # 6. Check snapshot consistency
            snapshot_issues = self.checker.check_snapshot_consistency(client)
            all_issues.extend(snapshot_issues)
            
            # 7. Check duplicate records
            duplicate_issues = self.checker.check_duplicate_records(client)
            all_issues.extend(duplicate_issues)
            
            # 8. Check data distribution
            distribution_issues = self.checker.check_data_distribution(client)
            all_issues.extend(distribution_issues)
            
            # Generate comprehensive report
            self.integrity_report = self.report_generator.generate_integrity_report(
                all_issues, table_counts, 'staging', self.staging_dataset
            )
            
            # Store results
            self.results = {
                'total_issues': len(all_issues),
                'critical_issues': self.integrity_report.critical_issues,
                'warning_issues': self.integrity_report.warning_issues,
                'info_issues': self.integrity_report.info_issues,
                'integrity_score': self.integrity_report.summary['integrity_score'],
                'total_records': self.integrity_report.summary['total_records'],
                'recommendations': self.integrity_report.summary['recommendations']
            }
            
            # Print report
            if report_format in ['console', 'both']:
                self.report_generator.print_integrity_report(self.integrity_report)
            
            # Save report if requested
            if save_report:
                saved_file = self.report_generator.save_integrity_report(
                    self.integrity_report, 
                    'json' if report_format == 'console' else report_format,
                    output_dir
                )
                if saved_file:
                    self.results['report_file'] = saved_file
            
            # Create registry entry for this verification
            self.create_verification_registry_entry(client)
            
            self.completed = True
            
            # Determine success based on critical issues
            success = self.integrity_report.critical_issues == 0
            
            if success:
                self.logger.info("✅ Data integrity verification completed - no critical issues")
            else:
                self.logger.warning(f"⚠️ Data integrity verification completed - {self.integrity_report.critical_issues} critical issues found")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Data integrity verification failed: {e}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            return False
    
    def get_results(self) -> Dict[str, Any]:
        """Get step execution results"""
        return {
            'completed': self.completed,
            'results': self.results,
            'integrity_report': self.integrity_report
        }
    
    def show_status(self):
        """Show current status and results"""
        print(f"\n🔍 DATA INTEGRITY VERIFICATION STATUS")
        print("=" * 50)
        print(f"Project: {self.project_id}")
        print(f"Dataset: {self.staging_dataset}")
        print(f"Completed: {'✅' if self.completed else '❌'}")
        
        if self.results:
            print(f"\n📈 RESULTS:")
            print(f"  • Total Issues: {self.results.get('total_issues', 0)}")
            print(f"  • Critical Issues: {self.results.get('critical_issues', 0)}")
            print(f"  • Warning Issues: {self.results.get('warning_issues', 0)}")
            print(f"  • Info Issues: {self.results.get('info_issues', 0)}")
            print(f"  • Integrity Score: {self.results.get('integrity_score', 0)}/100")
            print(f"  • Total Records: {self.results.get('total_records', 0):,}")
            
            recommendations = self.results.get('recommendations', [])
            if recommendations:
                print(f"\n💡 KEY RECOMMENDATIONS:")
                for i, rec in enumerate(recommendations[:3], 1):
                    print(f"  {i}. {rec}")
            
            report_file = self.results.get('report_file')
            if report_file:
                print(f"\n📄 Report saved: {report_file}")
    
    def get_critical_issue_count(self) -> int:
        """Get count of critical issues (for exit code)"""
        return self.results.get('critical_issues', 0) if self.completed else 1