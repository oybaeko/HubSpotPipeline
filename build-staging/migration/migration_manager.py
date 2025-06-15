#!/usr/bin/env python3
"""
Migration Manager - Main interface for data migration operations
Handles CLI, interactive menu, and orchestration
"""

import sys
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from .config import PROJECT_ID, ENVIRONMENTS, get_migration_config
from .schema_analyzer import SchemaAnalyzer
from .data_migrator import DataMigrator

class MigrationManager:
    """Main interface for data migration operations"""
    
    def __init__(self):
        self.project_id = PROJECT_ID
        self.environments = ENVIRONMENTS
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        self.logger = logging.getLogger('migration.manager')
        
        # Force use of user credentials
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            self.logger.info("🔧 Clearing service account credentials to use user auth")
            del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        
        # Initialize components
        self.schema_analyzer = SchemaAnalyzer()
        self.data_migrator = DataMigrator()
        
        # Initialize BigQuery client
        try:
            self.client = bigquery.Client(project=self.project_id)
            self.auth_info = self._get_auth_info()
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery client: {e}")
            self.client = None
            self.auth_info = None
    
    def _get_auth_info(self) -> Dict:
        """Get current authentication information"""
        auth_info = {
            'bigquery_user': 'Unknown',
            'gcloud_user': 'Unknown',
            'credentials_type': 'Unknown',
            'using_service_account': False,
            'env_var_set': False
        }
        
        try:
            # Check environment variable
            import os
            env_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if env_creds:
                auth_info['env_var_set'] = True
                auth_info['env_var_path'] = env_creds
                auth_info['using_service_account'] = True
            
            # Get gcloud user
            import subprocess
            result = subprocess.run("gcloud config get-value account", 
                                  shell=True, capture_output=True, text=True, check=False)
            if result.returncode == 0:
                auth_info['gcloud_user'] = result.stdout.strip()
            
            # Get BigQuery client credentials info
            from google.auth import default
            credentials, project = default()
            auth_info['credentials_type'] = type(credentials).__name__
            auth_info['bigquery_project'] = project
            
            # Try to get user email from credentials
            if hasattr(credentials, 'service_account_email'):
                auth_info['bigquery_user'] = credentials.service_account_email
                auth_info['using_service_account'] = True
            elif hasattr(credentials, 'token') and hasattr(credentials, '_id_token'):
                # User credentials - try to extract email
                try:
                    import jwt
                    decoded = jwt.decode(credentials._id_token, options={"verify_signature": False})
                    auth_info['bigquery_user'] = decoded.get('email', 'User account')
                except:
                    auth_info['bigquery_user'] = 'User account'
            
        except Exception as e:
            auth_info['error'] = str(e)
        
        return auth_info
    
    def get_data_counts(self) -> Dict:
        """Get record counts using BigQuery client with proper error handling"""
        self.logger.info("📊 Getting data counts")
        
        counts = {}
        tables = ['hs_companies', 'hs_deals', 'hs_owners', 'hs_snapshot_registry']
        
        for env, dataset in self.environments.items():
            counts[env] = {}
            
            for table in tables:
                try:
                    # Check if table exists first
                    table_ref = f"{self.project_id}.{dataset}.{table}"
                    
                    try:
                        # Try to get table metadata first
                        table_obj = self.client.get_table(table_ref)
                        
                        # If we can access metadata, try to count
                        count_query = f"SELECT COUNT(*) as count FROM `{table_ref}`"
                        result = self.data_migrator.run_query(count_query)
                        
                        if result:
                            for row in result:
                                counts[env][table] = row.count
                                break
                        else:
                            counts[env][table] = "Query failed"
                            
                    except NotFound:
                        counts[env][table] = "N/A"
                    except Exception as access_error:
                        # Check if it's a permission error
                        error_msg = str(access_error)
                        if "Access Denied" in error_msg or "permission" in error_msg.lower():
                            counts[env][table] = "No access"
                        elif "does not exist" in error_msg:
                            counts[env][table] = "N/A"
                        else:
                            counts[env][table] = "Error"
                            self.logger.debug(f"Table {table} in {env}: {error_msg}")
                            
                except Exception as e:
                    counts[env][table] = "Error"
                    self.logger.debug(f"Unexpected error for {table} in {env}: {str(e)[:100]}")
        
        return counts
    
    def show_schema_comparison(self):
        """Show schema comparison between environments"""
        print(f"\n🔍 SCHEMA COMPARISON")
        print("=" * 50)
        
        tables = ['hs_companies', 'hs_deals']
        
        for table in tables:
            comparison = self.schema_analyzer.compare_schemas(table)
            
            print(f"\n📋 {table.upper()}")
            print("-" * 30)
            
            if comparison['compatible']:
                print(f"✅ Compatible for migration")
            else:
                print(f"❌ Schema issues found:")
                for diff in comparison['differences']:
                    print(f"  • {diff}")
            
            # Show field counts
            for env, schema in comparison['schemas'].items():
                if schema:
                    print(f"  {env}: {len(schema)} fields")
                else:
                    print(f"  {env}: Table not found")
    
    def show_data_counts(self):
        """Show data counts across environments"""
        print(f"\n📊 DATA COUNTS")
        print("=" * 50)
        
        counts = self.get_data_counts()
        
        # Print in table format
        print(f"{'Table':<25} {'Dev':<10} {'Staging':<10} {'Prod':<10}")
        print("-" * 60)
        
        tables = ['hs_companies', 'hs_deals', 'hs_owners', 'hs_snapshot_registry']
        
        for table in tables:
            dev_count = counts.get('dev', {}).get(table, 'N/A')
            staging_count = counts.get('staging', {}).get(table, 'N/A')
            prod_count = counts.get('prod', {}).get(table, 'N/A')
            
            print(f"{table:<25} {str(dev_count):<10} {str(staging_count):<10} {str(prod_count):<10}")
    
    def show_prod_snapshot_formats(self):
        """Show production snapshot_id formats with better error handling"""
        print(f"\n🔍 PRODUCTION SNAPSHOT FORMATS")
        print("=" * 50)
        
        format_analysis = self.schema_analyzer.check_prod_snapshot_format()
        
        if not format_analysis:
            print("❌ No data available for analysis")
            return
        
        for table, snapshots in format_analysis.items():
            print(f"\n📋 {table.upper()}")
            print("-" * 30)
            
            if isinstance(snapshots, str):
                # Error message
                if "No access" in snapshots:
                    print(f"  🔒 {snapshots} - You may need production read permissions")
                    print(f"  💡 Contact admin to grant BigQuery Data Viewer role for prod dataset")
                elif "Error" in snapshots:
                    print(f"  ❌ {snapshots}")
                else:
                    print(f"  ℹ️  {snapshots}")
            elif isinstance(snapshots, list):
                # Actual snapshot data
                for snapshot_info in snapshots:
                    snapshot_id = snapshot_info['snapshot_id']
                    count = snapshot_info['count']
                    format_info = snapshot_info['format']
                    
                    print(f"  📸 {snapshot_id}")
                    print(f"      Records: {count:,}")
                    print(f"      Format: {format_info['format_type']}")
                    print(f"      Length: {format_info['length']} chars")
                    print(f"      Microseconds: {'✅' if format_info['has_microseconds'] else '❌'}")
                    print(f"      Timezone: {'✅' if format_info['has_timezone'] else '❌'}")
                    
                    if format_info['format_type'] != 'datetime_microseconds_utc':
                        print(f"      ⚠️  Old format - migration will update to: {self.data_migrator.migration_snapshot_id}")
                    print()
            else:
                print(f"  ❌ Unexpected data format")
        
        print(f"💡 Migration will standardize all data to: {self.data_migrator.migration_snapshot_id}")
        print(f"💡 This preserves the original timestamp but adds microseconds + timezone")
        
        # Check if we have production access issues
        if any("No access" in str(v) for v in format_analysis.values()):
            print(f"\n🔒 PRODUCTION ACCESS REQUIRED")
            print(f"To use this migration tool, you need:")
            print(f"  • BigQuery Data Viewer role on Hubspot_prod dataset")
            print(f"  • Or copy the migration queries and run them with proper access")
            print(f"  • Contact your GCP admin to grant these permissions")
    
    def confirm_live_migration(self) -> bool:
        """Confirm live migration operation"""
        print(f"\n🚨 LIVE MIGRATION CONFIRMATION")
        print(f"This will:")
        print(f"  • Clear existing data in staging")
        print(f"  • Copy prod data (companies + deals)")
        print(f"  • Set snapshot_id to: {self.data_migrator.migration_snapshot_id}")
        print(f"  • Add current timestamp to all records")
        
        confirm = input(f"\nType 'MIGRATE PROD TO STAGING' to continue: ").strip()
        return confirm == 'MIGRATE PROD TO STAGING'
    
    def confirm_live_operation(self, operation: str) -> bool:
        """Confirm live operation"""
        print(f"\n⚠️  LIVE OPERATION CONFIRMATION")
        print(f"This will {operation} in staging environment.")
        
        confirm = input(f"\nType 'yes' to continue: ").strip().lower()
        return confirm == 'yes'
    
    def confirm_full_migration(self) -> bool:
        """Confirm full migration pipeline"""
        print(f"\n🚨 FULL MIGRATION PIPELINE CONFIRMATION")
        print(f"This will execute the complete migration:")
        print(f"  1. Migrate prod data to staging")
        print(f"  2. Copy reference data from dev")
        print(f"  3. Create snapshot registry entry")
        print(f"  4. Ready for scoring pipeline")
        
        confirm = input(f"\nType 'FULL MIGRATION' to continue: ").strip()
        return confirm == 'FULL MIGRATION'
    
    def full_migration_pipeline(self, dry_run: bool = True) -> bool:
        """Execute full migration pipeline"""
        self.logger.info(f"🎯 Starting full migration pipeline (dry_run={dry_run})")
        
        success = True
        
        # Step 1: Migrate prod data
        self.logger.info("Step 1: Migrating production data")
        if not self.data_migrator.migrate_prod_to_staging(dry_run):
            success = False
        
        # Step 2: Copy reference data
        self.logger.info("Step 2: Copying reference data")
        if not self.data_migrator.copy_reference_data_from_dev(dry_run):
            success = False
        
        # Step 3: Create registry entry
        self.logger.info("Step 3: Creating snapshot registry entry")
        if not self.data_migrator.create_snapshot_registry_entry(dry_run):
            success = False
        
        if success:
            self.logger.info("🎉 Full migration pipeline completed successfully!")
            if not dry_run:
                self.logger.info("🚀 Ready for scoring pipeline execution")
        else:
            self.logger.error("❌ Full migration pipeline failed")
        
        return success
    
    def interactive_menu(self):
        """Interactive menu for data migration"""
        while True:
            print(f"\n{'='*70}")
            print(f"🚀 HUBSPOT DATA MIGRATION")
            print(f"{'='*70}")
            print(f"Project: {self.project_id}")
            
            # Display authentication info
            if self.auth_info:
                print(f"{'='*70}")
                print(f"🔐 AUTHENTICATION INFO")
                
                if self.auth_info.get('using_service_account'):
                    print(f"👤 BigQuery User: {self.auth_info.get('bigquery_user', 'Unknown')} (Service Account)")
                    if self.auth_info.get('env_var_set'):
                        env_path = self.auth_info.get('env_var_path', '')
                        filename = env_path.split('/')[-1] if env_path else ''
                        print(f"🔑 Credentials: Environment variable ({filename})")
                        
                        # Warning for service account access
                        if 'dev' in filename.lower():
                            print(f"⚠️  WARNING: Using dev service account - may lack prod access!")
                        
                else:
                    print(f"👤 BigQuery User: {self.auth_info.get('bigquery_user', 'Unknown')} (User Account)")
                    print(f"🔑 Credentials: User authentication")
                
                print(f"🌐 gcloud User: {self.auth_info.get('gcloud_user', 'Unknown')}")
                print(f"🔧 Credentials Type: {self.auth_info.get('credentials_type', 'Unknown')}")
                
                if self.auth_info.get('error'):
                    print(f"❌ Auth Error: {self.auth_info['error']}")
            
            print(f"{'='*70}")
            
            print(f"\n📋 OPERATIONS")
            print(f"  1) 🔍 Compare Schemas")
            print(f"  2) 📊 Show Data Counts")
            print(f"  3) 🔍 Check Prod Snapshot Formats")
            print(f"  4) 🧪 Test Migration (Dry Run)")
            print(f"  5) 🚀 Execute Migration (LIVE)")
            print(f"  6) 📋 Copy Reference Data (Dry Run)")
            print(f"  7) 📋 Copy Reference Data (LIVE)")
            print(f"  8) 📝 Create Registry Entry (Dry Run)")
            print(f"  9) 📝 Create Registry Entry (LIVE)")
            print(f"  10) 🎯 Full Migration Pipeline (Dry Run)")
            print(f"  11) 🎯 Full Migration Pipeline (LIVE)")
            print(f"  0) ❌ Exit")
            
            try:
                choice = input(f"\n🔹 Enter choice (0-11): ").strip()
                
                if choice == '0':
                    print("\n👋 Goodbye!")
                    break
                elif choice == '1':
                    self.show_schema_comparison()
                elif choice == '2':
                    self.show_data_counts()
                elif choice == '3':
                    self.show_prod_snapshot_formats()
                elif choice == '4':
                    self.data_migrator.migrate_prod_to_staging(dry_run=True)
                elif choice == '5':
                    if self.confirm_live_migration():
                        self.data_migrator.migrate_prod_to_staging(dry_run=False)
                elif choice == '6':
                    self.data_migrator.copy_reference_data_from_dev(dry_run=True)
                elif choice == '7':
                    if self.confirm_live_operation("copy reference data"):
                        self.data_migrator.copy_reference_data_from_dev(dry_run=False)
                elif choice == '8':
                    self.data_migrator.create_snapshot_registry_entry(dry_run=True)
                elif choice == '9':
                    if self.confirm_live_operation("create registry entry"):
                        self.data_migrator.create_snapshot_registry_entry(dry_run=False)
                elif choice == '10':
                    self.full_migration_pipeline(dry_run=True)
                elif choice == '11':
                    if self.confirm_full_migration():
                        self.full_migration_pipeline(dry_run=False)
                else:
                    print("❌ Invalid choice. Please select 0-11.")
                    
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
            
            if choice != '0':
                input("\n⏸️  Press Enter to continue...")