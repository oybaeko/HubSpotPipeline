#!/usr/bin/env python3
"""
HubSpot Data Migration Script
Migrates production data to staging with correct timestamps and schema alignment
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
import json
import subprocess

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    BIGQUERY_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  BigQuery not available: {e}")
    BIGQUERY_AVAILABLE = False

class DataMigrationManager:
    """Manages HubSpot data migration between environments"""
    
    def __init__(self):
        self.project_id = "hubspot-452402"
        self.environments = {
            'dev': 'Hubspot_dev_ob',
            'staging': 'Hubspot_staging',
            'prod': 'Hubspot_prod'
        }
        
        # Migration configuration
        self.prod_snapshot_timestamp = "2025-06-08T04:00:11Z"  # Jun 8, 14:00 UTC+10 -> UTC
        self.migration_snapshot_id = self.prod_snapshot_timestamp
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        self.logger = logging.getLogger('data_migration')
        
        # Initialize BigQuery client
        if BIGQUERY_AVAILABLE:
            self.client = bigquery.Client(project=self.project_id)
        else:
            self.client = None
    
    def run_query(self, query: str, dry_run: bool = False) -> Optional[bigquery.QueryJob]:
        """Execute BigQuery query"""
        if not self.client:
            self.logger.error("BigQuery client not available")
            return None
        
        job_config = bigquery.QueryJobConfig()
        job_config.dry_run = dry_run
        
        try:
            query_job = self.client.query(query, job_config=job_config)
            
            if dry_run:
                self.logger.info(f"✅ Query validated (dry run)")
                self.logger.info(f"📊 Bytes processed: {query_job.total_bytes_processed:,}")
                return query_job
            else:
                result = query_job.result()
                self.logger.info(f"✅ Query executed successfully")
                return query_job
                
        except Exception as e:
            self.logger.error(f"❌ Query failed: {e}")
            return None
    
    def get_table_schema(self, dataset: str, table: str) -> Optional[List]:
        """Get table schema using bq CLI"""
        try:
            cmd = f"bq show --schema --format=json {self.project_id}:{dataset}.{table}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False)
            
            if result.returncode == 0:
                schema_data = json.loads(result.stdout)
                return [{"name": field["name"], "type": field["type"], "mode": field.get("mode", "NULLABLE")} 
                       for field in schema_data]
            else:
                if "Not found" in result.stderr or "does not exist" in result.stderr:
                    self.logger.debug(f"Table {table} not found in {dataset}")
                    return None
                else:
                    self.logger.error(f"Error getting schema for {table} in {dataset}: {result.stderr}")
                    return None
        except Exception as e:
            self.logger.error(f"Error getting schema for {table}: {e}")
            return None
    
    def compare_schemas(self, table: str) -> Dict:
        """Compare table schemas across environments"""
        self.logger.info(f"🔍 Comparing schemas for {table}")
        
        schemas = {}
        for env, dataset in self.environments.items():
            schema = self.get_table_schema(dataset, table)
            schemas[env] = schema
        
        # Analyze differences
        comparison = {
            'table': table,
            'schemas': schemas,
            'compatible': True,
            'differences': []
        }
        
        # Compare prod vs staging (our migration path)
        prod_schema = schemas.get('prod')
        staging_schema = schemas.get('staging')
        
        if not prod_schema:
            comparison['differences'].append(f"Table {table} does not exist in prod")
            comparison['compatible'] = False
        elif not staging_schema:
            comparison['differences'].append(f"Table {table} does not exist in staging")
            comparison['compatible'] = False
        else:
            # Compare field by field
            prod_fields = {f['name']: f for f in prod_schema}
            staging_fields = {f['name']: f for f in staging_schema}
            
            # Check for missing fields in staging
            for field_name, field_info in prod_fields.items():
                if field_name not in staging_fields:
                    comparison['differences'].append(f"Field '{field_name}' missing in staging")
                    comparison['compatible'] = False
                elif staging_fields[field_name]['type'] != field_info['type']:
                    comparison['differences'].append(
                        f"Field '{field_name}' type mismatch: "
                        f"prod={field_info['type']}, staging={staging_fields[field_name]['type']}"
                    )
                    comparison['compatible'] = False
            
            # Check for extra fields in staging (usually OK)
            for field_name in staging_fields:
                if field_name not in prod_fields:
                    comparison['differences'].append(f"Extra field '{field_name}' in staging (OK)")
        
        return comparison
    
    def get_data_counts(self) -> Dict:
        """Get record counts using bq CLI"""
        self.logger.info("📊 Getting data counts")
        
        counts = {}
        tables = ['hs_companies', 'hs_deals', 'hs_owners', 'hs_snapshot_registry']
        
        for env, dataset in self.environments.items():
            counts[env] = {}
            
            for table in tables:
                try:
                    cmd = f'bq query --nouse_legacy_sql --format=csv --quiet "SELECT COUNT(*) as count FROM `{self.project_id}.{dataset}.{table}`"'
                    result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False)
                    
                    if result.returncode == 0:
                        lines = result.stdout.strip().split('\n')
                        if len(lines) >= 2:  # Header + data
                            count_str = lines[1].strip()
                            counts[env][table] = int(count_str) if count_str.isdigit() else count_str
                        else:
                            counts[env][table] = 0
                    else:
                        if "Not found" in result.stderr or "does not exist" in result.stderr:
                            counts[env][table] = "N/A"
                        else:
                            counts[env][table] = f"Error"
                except Exception as e:
                    counts[env][table] = f"Error: {str(e)[:50]}"
        
        return counts
    
    def backup_staging_data(self) -> bool:
        """Create backup of existing staging data"""
        self.logger.info("💾 Creating backup of staging data")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dataset = f"Hubspot_staging_backup_{timestamp}"
        
        # Create backup dataset
        try:
            dataset = bigquery.Dataset(f"{self.project_id}.{backup_dataset}")
            dataset.location = "europe-west1"
            dataset.description = f"Backup of Hubspot_staging before migration at {timestamp}"
            
            self.client.create_dataset(dataset, exists_ok=True)
            self.logger.info(f"✅ Created backup dataset: {backup_dataset}")
            
            # Copy tables to backup
            tables_to_backup = ['hs_companies', 'hs_deals']
            
            for table in tables_to_backup:
                source_table = f"{self.project_id}.Hubspot_staging.{table}"
                backup_table = f"{self.project_id}.{backup_dataset}.{table}"
                
                # Check if source table has data
                count_query = f"SELECT COUNT(*) as count FROM `{source_table}`"
                result = self.run_query(count_query)
                
                if result:
                    for row in result:
                        if row.count > 0:
                            # Copy table
                            copy_query = f"""
                            CREATE OR REPLACE TABLE `{backup_table}` AS
                            SELECT * FROM `{source_table}`
                            """
                            
                            if self.run_query(copy_query):
                                self.logger.info(f"✅ Backed up {table} ({row.count:,} rows)")
                            else:
                                self.logger.error(f"❌ Failed to backup {table}")
                                return False
                        else:
                            self.logger.info(f"⏭️  {table} is empty, skipping backup")
                        break
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Backup failed: {e}")
            return False
    
    def migrate_prod_to_staging(self, dry_run: bool = True) -> bool:
        """Migrate production data to staging with correct timestamp"""
        self.logger.info(f"🚀 Starting prod to staging migration (dry_run={dry_run})")
        
        if not dry_run:
            # Create backup first
            if not self.backup_staging_data():
                self.logger.error("❌ Backup failed, aborting migration")
                return False
        
        tables_to_migrate = ['hs_companies', 'hs_deals']
        success_count = 0
        
        for table in tables_to_migrate:
            self.logger.info(f"📦 Migrating {table}")
            
            # Get source data count
            count_query = f"SELECT COUNT(*) as count FROM `{self.project_id}.Hubspot_prod.{table}`"
            result = self.run_query(count_query)
            
            if not result:
                self.logger.error(f"❌ Failed to get count for {table}")
                continue
            
            source_count = 0
            for row in result:
                source_count = row.count
                break
            
            if source_count == 0:
                self.logger.warning(f"⚠️  {table} is empty in prod, skipping")
                continue
            
            self.logger.info(f"📊 Source records: {source_count:,}")
            
            # Clear existing data in staging
            if not dry_run:
                clear_query = f"DELETE FROM `{self.project_id}.Hubspot_staging.{table}` WHERE TRUE"
                if not self.run_query(clear_query):
                    self.logger.error(f"❌ Failed to clear {table} in staging")
                    continue
                self.logger.info(f"🗑️  Cleared existing data in staging.{table}")
            
            # Migration query - add snapshot_id and preserve all other fields
            migration_query = f"""
            INSERT INTO `{self.project_id}.Hubspot_staging.{table}` 
            SELECT 
                *,
                '{self.migration_snapshot_id}' as snapshot_id
            FROM `{self.project_id}.Hubspot_prod.{table}`
            """
            
            # Execute migration
            if dry_run:
                self.logger.info(f"🧪 DRY RUN: Would migrate {source_count:,} records")
                if self.run_query(migration_query, dry_run=True):
                    self.logger.info(f"✅ Migration query validated for {table}")
                    success_count += 1
                else:
                    self.logger.error(f"❌ Migration query validation failed for {table}")
            else:
                if self.run_query(migration_query):
                    # Verify migration
                    verify_query = f"SELECT COUNT(*) as count FROM `{self.project_id}.Hubspot_staging.{table}` WHERE snapshot_id = '{self.migration_snapshot_id}'"
                    verify_result = self.run_query(verify_query)
                    
                    if verify_result:
                        for row in verify_result:
                            migrated_count = row.count
                            if migrated_count == source_count:
                                self.logger.info(f"✅ Successfully migrated {table}: {migrated_count:,} records")
                                success_count += 1
                            else:
                                self.logger.error(f"❌ Migration verification failed for {table}: expected {source_count}, got {migrated_count}")
                            break
                else:
                    self.logger.error(f"❌ Migration failed for {table}")
        
        migration_success = success_count == len(tables_to_migrate)
        
        if migration_success:
            self.logger.info(f"🎉 Migration completed successfully!")
            self.logger.info(f"📸 Snapshot ID: {self.migration_snapshot_id}")
            self.logger.info(f"📊 Tables migrated: {success_count}/{len(tables_to_migrate)}")
        else:
            self.logger.error(f"❌ Migration partially failed: {success_count}/{len(tables_to_migrate)} tables")
        
        return migration_success
    
    def copy_reference_data_from_dev(self, dry_run: bool = True) -> bool:
        """Copy reference data from dev to staging"""
        self.logger.info(f"📋 Copying reference data from dev to staging (dry_run={dry_run})")
        
        reference_tables = ['hs_owners', 'hs_deal_stage_reference']
        success_count = 0
        
        for table in reference_tables:
            self.logger.info(f"📦 Copying {table}")
            
            # Get source data count
            count_query = f"SELECT COUNT(*) as count FROM `{self.project_id}.Hubspot_dev_ob.{table}`"
            result = self.run_query(count_query)
            
            if not result:
                self.logger.error(f"❌ Failed to get count for {table}")
                continue
            
            source_count = 0
            for row in result:
                source_count = row.count
                break
            
            if source_count == 0:
                self.logger.warning(f"⚠️  {table} is empty in dev, skipping")
                continue
            
            self.logger.info(f"📊 Source records: {source_count:,}")
            
            # Clear existing data in staging
            if not dry_run:
                clear_query = f"DELETE FROM `{self.project_id}.Hubspot_staging.{table}` WHERE TRUE"
                if not self.run_query(clear_query):
                    self.logger.error(f"❌ Failed to clear {table} in staging")
                    continue
                self.logger.info(f"🗑️  Cleared existing data in staging.{table}")
            
            # Copy query
            copy_query = f"""
            INSERT INTO `{self.project_id}.Hubspot_staging.{table}` 
            SELECT * FROM `{self.project_id}.Hubspot_dev_ob.{table}`
            """
            
            # Execute copy
            if dry_run:
                self.logger.info(f"🧪 DRY RUN: Would copy {source_count:,} records")
                if self.run_query(copy_query, dry_run=True):
                    self.logger.info(f"✅ Copy query validated for {table}")
                    success_count += 1
                else:
                    self.logger.error(f"❌ Copy query validation failed for {table}")
            else:
                if self.run_query(copy_query):
                    self.logger.info(f"✅ Successfully copied {table}: {source_count:,} records")
                    success_count += 1
                else:
                    self.logger.error(f"❌ Copy failed for {table}")
        
        copy_success = success_count == len(reference_tables)
        
        if copy_success:
            self.logger.info(f"🎉 Reference data copy completed successfully!")
        else:
            self.logger.error(f"❌ Reference data copy partially failed: {success_count}/{len(reference_tables)} tables")
        
        return copy_success
    
    def create_snapshot_registry_entry(self, dry_run: bool = True) -> bool:
        """Create snapshot registry entry for the migrated data"""
        self.logger.info(f"📝 Creating snapshot registry entry (dry_run={dry_run})")
        
        registry_query = f"""
        INSERT INTO `{self.project_id}.Hubspot_staging.hs_snapshot_registry` 
        (snapshot_id, snapshot_timestamp, triggered_by, status, notes, ingest_stats)
        VALUES (
            '{self.migration_snapshot_id}',
            TIMESTAMP('{self.migration_snapshot_id}'),
            'data_migration_script',
            'migration_completed_ingest_completed_scoring_pending',
            'Migrated from production data with original timestamp from Jun 8, 2025 14:00 UTC+10',
            JSON_OBJECT(
                'migration_source', 'production',
                'migration_timestamp', CURRENT_TIMESTAMP(),
                'companies_migrated', (SELECT COUNT(*) FROM `{self.project_id}.Hubspot_staging.hs_companies` WHERE snapshot_id = '{self.migration_snapshot_id}'),
                'deals_migrated', (SELECT COUNT(*) FROM `{self.project_id}.Hubspot_staging.hs_deals` WHERE snapshot_id = '{self.migration_snapshot_id}')
            )
        )
        """
        
        if dry_run:
            self.logger.info("🧪 DRY RUN: Would create snapshot registry entry")
            return self.run_query(registry_query, dry_run=True) is not None
        else:
            if self.run_query(registry_query):
                self.logger.info(f"✅ Created snapshot registry entry")
                return True
            else:
                self.logger.error(f"❌ Failed to create snapshot registry entry")
                return False
    
    def interactive_menu(self):
        """Interactive menu for data migration"""
        while True:
            print(f"\n{'='*70}")
            print(f"🚀 HUBSPOT DATA MIGRATION")
            print(f"{'='*70}")
            print(f"Project: {self.project_id}")
            print(f"Migration Timestamp: {self.prod_snapshot_timestamp}")
            print(f"{'='*70}")
            
            print(f"\n📋 OPERATIONS")
            print(f"  1) 🔍 Compare Schemas")
            print(f"  2) 📊 Show Data Counts")
            print(f"  3) 🧪 Test Migration (Dry Run)")
            print(f"  4) 🚀 Execute Migration (LIVE)")
            print(f"  5) 📋 Copy Reference Data (Dry Run)")
            print(f"  6) 📋 Copy Reference Data (LIVE)")
            print(f"  7) 📝 Create Registry Entry (Dry Run)")
            print(f"  8) 📝 Create Registry Entry (LIVE)")
            print(f"  9) 🎯 Full Migration Pipeline (Dry Run)")
            print(f"  10) 🎯 Full Migration Pipeline (LIVE)")
            print(f"  0) ❌ Exit")
            
            try:
                choice = input(f"\n🔹 Enter choice (0-10): ").strip()
                
                if choice == '0':
                    print("\n👋 Goodbye!")
                    break
                elif choice == '1':
                    self.show_schema_comparison()
                elif choice == '2':
                    self.show_data_counts()
                elif choice == '3':
                    self.migrate_prod_to_staging(dry_run=True)
                elif choice == '4':
                    if self.confirm_live_migration():
                        self.migrate_prod_to_staging(dry_run=False)
                elif choice == '5':
                    self.copy_reference_data_from_dev(dry_run=True)
                elif choice == '6':
                    if self.confirm_live_operation("copy reference data"):
                        self.copy_reference_data_from_dev(dry_run=False)
                elif choice == '7':
                    self.create_snapshot_registry_entry(dry_run=True)
                elif choice == '8':
                    if self.confirm_live_operation("create registry entry"):
                        self.create_snapshot_registry_entry(dry_run=False)
                elif choice == '9':
                    self.full_migration_pipeline(dry_run=True)
                elif choice == '10':
                    if self.confirm_full_migration():
                        self.full_migration_pipeline(dry_run=False)
                else:
                    print("❌ Invalid choice. Please select 0-10.")
                    
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
            
            if choice != '0':
                input("\n⏸️  Press Enter to continue...")
    
    def show_schema_comparison(self):
        """Show schema comparison between environments"""
        print(f"\n🔍 SCHEMA COMPARISON")
        print("=" * 50)
        
        tables = ['hs_companies', 'hs_deals']
        
        for table in tables:
            comparison = self.compare_schemas(table)
            
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
    
    def confirm_live_migration(self) -> bool:
        """Confirm live migration operation"""
        print(f"\n🚨 LIVE MIGRATION CONFIRMATION")
        print(f"This will:")
        print(f"  • Clear existing data in staging")
        print(f"  • Copy prod data (2,212 companies + 103 deals)")
        print(f"  • Set snapshot_id to: {self.migration_snapshot_id}")
        print(f"  • Create backup of current staging data")
        
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
        if not self.migrate_prod_to_staging(dry_run):
            success = False
        
        # Step 2: Copy reference data
        self.logger.info("Step 2: Copying reference data")
        if not self.copy_reference_data_from_dev(dry_run):
            success = False
        
        # Step 3: Create registry entry
        self.logger.info("Step 3: Creating snapshot registry entry")
        if not self.create_snapshot_registry_entry(dry_run):
            success = False
        
        if success:
            self.logger.info("🎉 Full migration pipeline completed successfully!")
            if not dry_run:
                self.logger.info("🚀 Ready for scoring pipeline execution")
        else:
            self.logger.error("❌ Full migration pipeline failed")
        
        return success


def main():
    """Main entry point"""
    if not BIGQUERY_AVAILABLE:
        print("❌ BigQuery client library not available")
        print("💡 Install: pip install google-cloud-bigquery")
        sys.exit(1)
    
    manager = DataMigrationManager()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'schemas':
            manager.show_schema_comparison()
        elif command == 'counts':
            manager.show_data_counts()
        elif command == 'test':
            manager.migrate_prod_to_staging(dry_run=True)
        elif command == 'migrate':
            if manager.confirm_live_migration():
                manager.migrate_prod_to_staging(dry_run=False)
        elif command == 'full-test':
            manager.full_migration_pipeline(dry_run=True)
        elif command == 'full-migrate':
            if manager.confirm_full_migration():
                manager.full_migration_pipeline(dry_run=False)
        else:
            print(f"Unknown command: {command}")
            print("Available commands: schemas, counts, test, migrate, full-test, full-migrate")
    else:
        manager.interactive_menu()


if __name__ == "__main__":
    main()