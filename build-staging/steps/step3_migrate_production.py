#!/usr/bin/env python3
"""
Step 3: Migrate Production Data - CLEAN VERSION
Migrates data from production to staging with schema adaptation using authoritative schemas
Preserves production snapshot dates with canonical time conversion
"""

import sys
import os
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from google.cloud import bigquery

class ProductionMigrationStep:
    """
    Step 3: Migrate production data to staging with schema adaptation
    
    This step:
    1. Uses authoritative schemas from src/hubspot_pipeline/schema.py
    2. Preserves production snapshot dates, adds canonical time
    3. Migrates data with appropriate field mappings and type conversions
    4. Creates snapshot registry entries for each preserved snapshot
    """
    
    def __init__(self, project_id: str = "hubspot-452402", dataset: str = "Hubspot_staging"):
        self.project_id = project_id
        self.staging_dataset = dataset
        self.prod_dataset = "Hubspot_prod"
        
        # Setup logging
        self.logger = logging.getLogger('production_migration_step')
        
        # Setup environment and imports
        self._setup_environment()
        
        # Track results
        self.results = {}
        self.completed = False
        
        # Migration configuration - canonical time to add to production dates
        self.canonical_time = "04:00:11.000000Z"  # From migration config
        
    def _setup_environment(self):
        """Setup paths and clear service account credentials"""
        # Clear service account credentials to use user auth
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
            self.logger.debug("Cleared GOOGLE_APPLICATION_CREDENTIALS to use user auth")
        
        # Add path for authoritative schema imports
        src_path = Path(__file__).parent.parent.parent / "src" / "hubspot_pipeline"
        if str(src_path) not in sys.path:
            sys.path.insert(0, str(src_path))
        
        # Add path for migration utilities (now in steps/)
        migration_utils_path = Path(__file__).parent / "migration_utils"
        if str(migration_utils_path) not in sys.path:
            sys.path.insert(0, str(migration_utils_path))
        
        self.logger.debug(f"Added paths: {src_path}, {migration_utils_path}")
    
    def validate_prerequisites(self) -> bool:
        """Check if all required modules and access are available"""
        try:
            # Test authoritative schema import
            from schema import SCHEMA_COMPANIES, SCHEMA_DEALS
            self.logger.info("✅ Authoritative schemas imported successfully")
            
            # Test BigQuery client and access
            client = bigquery.Client(project=self.project_id)
            
            # Test production access
            prod_dataset_ref = client.dataset(self.prod_dataset)
            list(client.list_tables(prod_dataset_ref, max_results=1))
            self.logger.info("✅ Production dataset access confirmed")
            
            # Test staging access
            staging_dataset_ref = client.dataset(self.staging_dataset)
            list(client.list_tables(staging_dataset_ref, max_results=1))
            self.logger.info("✅ Staging dataset access confirmed")
            
            return True
            
        except ImportError as e:
            self.logger.error(f"❌ Failed to import required modules: {e}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Prerequisites check failed: {e}")
            return False
    
    def analyze_schemas(self) -> Dict[str, Any]:
        """Analyze schema differences between production and staging"""
        self.logger.info("🔍 Analyzing schema differences between prod and staging")
        
        try:
            # Import authoritative schemas
            from schema import SCHEMA_COMPANIES, SCHEMA_DEALS
            
            # Import schema analyzer
            from migration_utils.schema_analyzer import SchemaAnalyzer
            
            analyzer = SchemaAnalyzer()
            
            analysis = {
                'companies': analyzer.compare_schemas('hs_companies'),
                'deals': analyzer.compare_schemas('hs_deals'),
                'authoritative_schemas': {
                    'companies': SCHEMA_COMPANIES,
                    'deals': SCHEMA_DEALS
                }
            }
            
            # Log analysis results
            for table_name, comparison in analysis.items():
                if table_name == 'authoritative_schemas':
                    continue
                    
                self.logger.info(f"📋 {table_name.upper()} Analysis:")
                if comparison['compatible']:
                    self.logger.info(f"  ✅ Schema compatible for migration")
                else:
                    self.logger.warning(f"  ⚠️ Schema differences found:")
                    for diff in comparison['differences']:
                        if 'type mismatch' in diff:
                            self.logger.warning(f"    🔄 {diff} - Will apply conversion")
                        else:
                            self.logger.info(f"    ℹ️  {diff}")
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"❌ Schema analysis failed: {e}")
            return {}
    
    def get_production_snapshots(self, client: bigquery.Client) -> List[str]:
        """Get unique snapshot IDs from production data"""
        try:
            query = f"""
            SELECT DISTINCT snapshot_id
            FROM `{self.project_id}.{self.prod_dataset}.hs_companies`
            ORDER BY snapshot_id
            """
            
            result = client.query(query).result()
            snapshot_ids = [row.snapshot_id for row in result]
            
            self.logger.info(f"📸 Found {len(snapshot_ids)} unique snapshots in production")
            for snapshot_id in snapshot_ids:
                self.logger.info(f"  📅 {snapshot_id}")
            
            return snapshot_ids
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get production snapshot IDs: {e}")
            return []
    
    def convert_snapshot_to_timestamp(self, snapshot_date: str) -> str:
        """Convert production snapshot date to full timestamp format with microseconds"""
        try:
            # Check if it already has the proper microsecond format
            if snapshot_date.endswith('.000000Z'):
                return snapshot_date
            
            # If it has 'T' but not microseconds, extract date and time parts
            if 'T' in snapshot_date:
                # Format like "2025-06-08T04:00:05" - needs microseconds added
                date_part = snapshot_date.split('T')[0]  # "2025-06-08"
                return f"{date_part}T{self.canonical_time}"  # "2025-06-08T04:00:11.000000Z"
            else:
                # It's just a date like "2025-06-08", add the canonical time
                return f"{snapshot_date}T{self.canonical_time}"
                
        except Exception as e:
            self.logger.warning(f"⚠️ Could not convert snapshot date {snapshot_date}: {e}")
            # Fallback to current time
            return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    def get_snapshot_count(self, client: bigquery.Client, dataset: str, table: str, snapshot_id: str) -> int:
        """Get record count for a specific snapshot in a table"""
        try:
            query = f"""
            SELECT COUNT(*) as count 
            FROM `{self.project_id}.{dataset}.{table}` 
            WHERE snapshot_id = '{snapshot_id}'
            """
            result = client.query(query).result()
            
            for row in result:
                return row.count
            return 0
            
        except Exception as e:
            self.logger.debug(f"Could not count {table} snapshot {snapshot_id} in {dataset}: {e}")
            return 0
    
    def clear_previous_production_migration(self, client: bigquery.Client, table_name: str):
        """Clear only data from previous production migrations, preserve Excel imports"""
        try:
            # Get snapshot IDs that were created by previous production migrations
            registry_query = f"""
            SELECT DISTINCT snapshot_id 
            FROM `{self.project_id}.{self.staging_dataset}.hs_snapshot_registry`
            WHERE triggered_by = 'production_migration_step3'
            """
            
            result = client.query(registry_query).result()
            migration_snapshots = [row.snapshot_id for row in result]
            
            if migration_snapshots:
                # Delete only records from previous production migrations
                snapshot_list = "', '".join(migration_snapshots)
                delete_query = f"""
                DELETE FROM `{self.project_id}.{self.staging_dataset}.{table_name}`
                WHERE snapshot_id IN ('{snapshot_list}')
                """
                
                deleted_result = client.query(delete_query).result()
                self.logger.info(f"🗑️ Cleared {len(migration_snapshots)} previous production snapshots from {table_name}")
                self.logger.info(f"  📸 Cleared snapshots: {', '.join(migration_snapshots)}")
                
                # Also clean up the old registry entries
                registry_cleanup_query = f"""
                DELETE FROM `{self.project_id}.{self.staging_dataset}.hs_snapshot_registry`
                WHERE triggered_by = 'production_migration_step3'
                """
                client.query(registry_cleanup_query).result()
                self.logger.info(f"📝 Cleaned up {len(migration_snapshots)} old registry entries")
                
            else:
                self.logger.info(f"📭 No previous production migration data found in {table_name}")
                
        except Exception as e:
            self.logger.warning(f"⚠️ Could not clear previous migration data from {table_name}: {e}")
            self.logger.debug(f"Error details: {e}")

    def clear_staging_table(self, client: bigquery.Client, table_name: str):
        """Clear staging table completely (for testing scenarios)"""
        try:
            query = f"TRUNCATE TABLE `{self.project_id}.{self.staging_dataset}.{table_name}`"
            client.query(query).result()
            self.logger.info(f"🗑️ COMPLETELY cleared staging table: {table_name}")
            self.logger.warning(f"⚠️ This removed ALL data including Excel imports!")
        except Exception as e:
            self.logger.warning(f"⚠️ Could not clear {table_name}: {e}")
    
    def build_migration_query(self, table_name: str, data_type: str, schema_analysis: Dict, 
                             prod_snapshot_id: str, target_snapshot_id: str) -> Optional[str]:
        """Build migration query for a specific snapshot with schema adaptation"""
        try:
            # Get authoritative schema for target structure
            auth_schemas = schema_analysis['authoritative_schemas']
            target_schema = auth_schemas[data_type]
            
            # Get production schema for field mapping
            comparison = schema_analysis[data_type]
            prod_fields = {f['name']: f for f in comparison['schemas'].get('prod', [])} if comparison['schemas'].get('prod') else {}
            
            # Build field mappings based on authoritative schema
            select_fields = []
            
            for field_name, field_type in target_schema:
                if field_name == 'snapshot_id':
                    select_fields.append(f"'{target_snapshot_id}' as snapshot_id")
                elif field_name == 'record_timestamp':
                    select_fields.append('CURRENT_TIMESTAMP() as record_timestamp')
                elif field_name in prod_fields:
                    # Field exists in production - handle type conversions
                    prod_field = prod_fields[field_name]
                    
                    if field_name == 'amount' and data_type == 'deals':
                        # Convert STRING to FLOAT for deals amount
                        if prod_field['type'] == 'STRING' and field_type == 'FLOAT':
                            select_fields.append('SAFE_CAST(amount AS FLOAT64) as amount')
                        else:
                            select_fields.append(field_name)
                    elif field_name in ['company_id', 'deal_id'] and prod_field['type'] in ['INTEGER', 'INT64']:
                        # Convert numeric IDs to STRING
                        select_fields.append(f'CAST({field_name} AS STRING) as {field_name}')
                    else:
                        # Direct mapping
                        select_fields.append(field_name)
                else:
                    # Field doesn't exist in production - set to NULL
                    select_fields.append(f'NULL as {field_name}')
            
            # Build the complete query for this specific snapshot
            query = f"""
            INSERT INTO `{self.project_id}.{self.staging_dataset}.{table_name}` 
            SELECT 
                {', '.join(['    ' + field for field in select_fields])}
            FROM `{self.project_id}.{self.prod_dataset}.{table_name}`
            WHERE snapshot_id = '{prod_snapshot_id}'
            """
            
            return query
            
        except Exception as e:
            self.logger.error(f"❌ Failed to build migration query: {e}")
            return None
    
    def validate_query(self, client: bigquery.Client, query: str) -> bool:
        """Validate query with dry run"""
        try:
            job_config = bigquery.QueryJobConfig(dry_run=True)
            query_job = client.query(query, job_config=job_config)
            
            self.logger.info(f"✅ Query validated")
            self.logger.info(f"📊 Bytes to process: {query_job.total_bytes_processed:,}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Query validation failed: {e}")
            return False
    
    def execute_migration_query(self, client: bigquery.Client, query: str, target_snapshot_id: str, table_name: str) -> Optional[int]:
        """Execute migration query and return migrated count"""
        try:
            # Execute the migration
            query_job = client.query(query)
            query_job.result()  # Wait for completion
            
            # Verify the migration by counting records with target snapshot_id
            verify_query = f"""
            SELECT COUNT(*) as count 
            FROM `{self.project_id}.{self.staging_dataset}.{table_name}` 
            WHERE snapshot_id = '{target_snapshot_id}'
            """
            
            verify_result = client.query(verify_query).result()
            for row in verify_result:
                return row.count
            
            return 0
            
        except Exception as e:
            self.logger.error(f"❌ Migration execution failed: {e}")
            return None
    
    def copy_reference_data(self, client: bigquery.Client):
        """Copy reference data from dev to staging with schema awareness"""
        self.logger.info("📋 Copying reference data")
        
        reference_tables = ['hs_owners', 'hs_deal_stage_reference']
        
        for table in reference_tables:
            try:
                # Get staging table schema to build proper field list
                staging_table = client.get_table(f"{self.project_id}.{self.staging_dataset}.{table}")
                staging_fields = [field.name for field in staging_table.schema]
                
                # Get dev table schema
                dev_table = client.get_table(f"{self.project_id}.Hubspot_dev_ob.{table}")
                dev_fields = [field.name for field in dev_table.schema]
                
                self.logger.info(f"📊 {table}: staging({len(staging_fields)}) vs dev({len(dev_fields)}) fields")
                
                # Build field mapping
                select_fields = []
                for staging_field in staging_fields:
                    if staging_field == 'record_timestamp':
                        select_fields.append('CURRENT_TIMESTAMP() as record_timestamp')
                    elif staging_field in dev_fields:
                        select_fields.append(staging_field)
                    else:
                        select_fields.append(f'NULL as {staging_field}')
                
                # Clear existing data
                clear_query = f"TRUNCATE TABLE `{self.project_id}.{self.staging_dataset}.{table}`"
                client.query(clear_query).result()
                
                # Copy with schema-aware field mapping
                copy_query = f"""
                INSERT INTO `{self.project_id}.{self.staging_dataset}.{table}` 
                SELECT 
                    {', '.join(select_fields)}
                FROM `{self.project_id}.Hubspot_dev_ob.{table}`
                """
                
                client.query(copy_query).result()
                
                # Count copied records
                count_query = f"SELECT COUNT(*) as count FROM `{self.project_id}.{self.staging_dataset}.{table}`"
                result = client.query(count_query).result()
                
                for row in result:
                    self.logger.info(f"✅ Copied {table}: {row.count} records")
                    break
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Failed to copy {table}: {e}")
    
    def create_registry_entries(self, client: bigquery.Client, migration_results: Dict, production_snapshots: List[str]):
        """Create snapshot registry entries for each migrated snapshot"""
        try:
            for prod_snapshot_id in production_snapshots:
                target_snapshot_id = self.convert_snapshot_to_timestamp(prod_snapshot_id)
                
                # Count total records for this snapshot across all tables
                total_records = 0
                tables_migrated = []
                
                for table_name, table_results in migration_results.items():
                    for snapshot_result in table_results['snapshots']:
                        if snapshot_result['snapshot'] == prod_snapshot_id and snapshot_result.get('migrated', False):
                            total_records += snapshot_result['count']
                            if table_name not in tables_migrated:
                                tables_migrated.append(table_name)
                
                if total_records > 0:  # Only create entry if we actually migrated data
                    registry_query = f"""
                    INSERT INTO `{self.project_id}.{self.staging_dataset}.hs_snapshot_registry` 
                    (snapshot_id, record_timestamp, triggered_by, status, notes)
                    VALUES (
                        '{target_snapshot_id}',
                        CURRENT_TIMESTAMP(),
                        'production_migration_step3',
                        'completed',
                        'Migrated from production snapshot {prod_snapshot_id} | Tables: {", ".join(tables_migrated)} | Records: {total_records:,}'
                    )
                    """
                    
                    client.query(registry_query).result()
                    self.logger.info(f"📝 Created registry entry for snapshot {target_snapshot_id}")
                    
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to create registry entries: {e}")
    
    def execute(self, dry_run: bool = True, clear_staging: bool = False, clear_all: bool = False) -> bool:
        """Execute production migration with snapshot preservation"""
        self.logger.info(f"🚀 STEP 3: Migrating production data (dry_run={dry_run})")
        
        if clear_all:
            self.logger.warning("⚠️ CLEAR ALL mode enabled - will remove Excel import data too!")
        elif clear_staging:
            self.logger.info("🔄 Clear mode: Only previous production migration data")
        else:
            self.logger.info("🔄 Clear mode: None (will add to existing data)")
        
        try:
            # Validate prerequisites
            if not self.validate_prerequisites():
                return False
            
            # Analyze schemas
            schema_analysis = self.analyze_schemas()
            if not schema_analysis:
                self.logger.error("❌ Schema analysis failed")
                return False
            
            # Initialize BigQuery client
            client = bigquery.Client(project=self.project_id)
            
            # Get production snapshots to preserve
            production_snapshots = self.get_production_snapshots(client)
            if not production_snapshots:
                self.logger.error("❌ No production snapshots found")
                return False
            
            # Tables to migrate
            tables_to_migrate = ['hs_companies', 'hs_deals']
            migration_results = {}
            
            for table_name in tables_to_migrate:
                self.logger.info(f"📦 Migrating {table_name}")
                
                # Get data type for schema lookup
                data_type = table_name.replace('hs_', '')
                
                # Handle clearing based on mode
                if not dry_run:
                    if clear_all:
                        # DANGER: Clear everything including Excel imports
                        self.clear_staging_table(client, table_name)
                    elif clear_staging:
                        # Safe: Clear only previous production migration data
                        self.clear_previous_production_migration(client, table_name)
                    # If neither, don't clear anything (add to existing data)
                
                # Migrate each snapshot separately to preserve snapshot IDs
                table_migration_results = []
                
                for prod_snapshot_id in production_snapshots:
                    # Convert snapshot date to full timestamp
                    target_snapshot_id = self.convert_snapshot_to_timestamp(prod_snapshot_id)
                    
                    # Get production data count for this snapshot
                    snapshot_count = self.get_snapshot_count(client, self.prod_dataset, table_name, prod_snapshot_id)
                    if snapshot_count == 0:
                        self.logger.info(f"  📭 No {table_name} data for snapshot {prod_snapshot_id}")
                        continue
                    
                    self.logger.info(f"  📸 Snapshot {prod_snapshot_id}: {snapshot_count:,} records → {target_snapshot_id}")
                    
                    # Build migration query for this specific snapshot
                    migration_query = self.build_migration_query(
                        table_name, 
                        data_type, 
                        schema_analysis,
                        prod_snapshot_id,
                        target_snapshot_id
                    )
                    
                    if not migration_query:
                        self.logger.error(f"❌ Failed to build migration query for {table_name} snapshot {prod_snapshot_id}")
                        continue
                    
                    # Execute migration for this snapshot
                    if dry_run:
                        self.logger.info(f"    🧪 DRY RUN: Would migrate {snapshot_count:,} records")
                        if self.validate_query(client, migration_query):
                            self.logger.info(f"    ✅ Migration query validated")
                            table_migration_results.append({'snapshot': prod_snapshot_id, 'validated': True, 'count': snapshot_count})
                        else:
                            self.logger.error(f"    ❌ Migration query validation failed")
                            table_migration_results.append({'snapshot': prod_snapshot_id, 'validated': False, 'count': 0})
                    else:
                        migrated_count = self.execute_migration_query(client, migration_query, target_snapshot_id, table_name)
                        if migrated_count is not None:
                            self.logger.info(f"    ✅ Migrated snapshot {prod_snapshot_id}: {migrated_count:,} records")
                            table_migration_results.append({'snapshot': prod_snapshot_id, 'migrated': True, 'count': migrated_count})
                        else:
                            self.logger.error(f"    ❌ Migration failed for snapshot {prod_snapshot_id}")
                            table_migration_results.append({'snapshot': prod_snapshot_id, 'migrated': False, 'count': 0})
                
                # Summarize table migration
                total_count = sum(result['count'] for result in table_migration_results)
                successful_snapshots = [r for r in table_migration_results if r.get('migrated', r.get('validated', False))]
                
                migration_results[table_name] = {
                    'snapshots': table_migration_results,
                    'total_count': total_count,
                    'successful_snapshots': len(successful_snapshots)
                }
                
                self.logger.info(f"✅ {table_name}: {len(successful_snapshots)}/{len(production_snapshots)} snapshots, {total_count:,} total records")
            
            # Create snapshot registry entries if not dry run
            if not dry_run and migration_results:
                self.create_registry_entries(client, migration_results, production_snapshots)
            
            # Copy reference data if not dry run
            if not dry_run:
                self.copy_reference_data(client)
            
            # Store results
            self.results = {
                'production_snapshots': production_snapshots,
                'tables_migrated': migration_results,
                'dry_run': dry_run,
                'clear_staging': clear_staging,
                'clear_all': clear_all,
                'schema_analysis': schema_analysis
            }
            
            # Determine success
            success = all(
                table_result['successful_snapshots'] > 0
                for table_result in migration_results.values()
            )
            
            # Debug logging for success determination
            self.logger.info("🔍 Success determination:")
            for table_name, table_result in migration_results.items():
                successful = table_result['successful_snapshots']
                total = len(table_result['snapshots'])
                self.logger.info(f"  {table_name}: {successful}/{total} snapshots successful")
            
            self.logger.info(f"Overall success: {success}")
            
            if success:
                self.completed = True
                self.logger.info("✅ Production migration completed successfully")
                if not dry_run:
                    production_snapshots = self.results.get('production_snapshots', [])
                    self.logger.info(f"📸 Production snapshots migrated: {len(production_snapshots)}")
                return True
            else:
                self.logger.error("❌ Production migration failed")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Production migration failed: {e}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            return False
    
    def get_results(self) -> Dict[str, Any]:
        """Get step execution results"""
        return {
            'completed': self.completed,
            'results': self.results
        }
    
    def show_status(self):
        """Show current status and results"""
        print(f"\n🚀 PRODUCTION MIGRATION STATUS")
        print("=" * 50)
        print(f"Project: {self.project_id}")
        print(f"Source: {self.prod_dataset}")
        print(f"Target: {self.staging_dataset}")
        print(f"Completed: {'✅' if self.completed else '❌'}")
        
        if self.results:
            print(f"\n📈 RESULTS:")
            print(f"  • Production Snapshots: {len(self.results.get('production_snapshots', []))}")
            for snapshot in self.results.get('production_snapshots', []):
                print(f"    📅 {snapshot}")
            print(f"  • Dry Run: {self.results.get('dry_run', 'Unknown')}")
            print(f"  • Clear Staging: {self.results.get('clear_staging', 'Unknown')}")
            
            tables_migrated = self.results.get('tables_migrated', {})
            if tables_migrated:
                print(f"  • Tables Migrated:")
                for table, result in tables_migrated.items():
                    total_count = result.get('total_count', 0)
                    successful = result.get('successful_snapshots', 0)
                    total_snapshots = len(result.get('snapshots', []))
                    status = '✅' if successful > 0 else '❌'
                    print(f"    {status} {table}: {successful}/{total_snapshots} snapshots, {total_count:,} records")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Production Migration Step")
    parser.add_argument('--project', default='hubspot-452402', help='BigQuery project')
    parser.add_argument('--dataset', default='Hubspot_staging', help='Staging dataset')
    parser.add_argument('--dry-run', action='store_true', help='Preview migration only')
    parser.add_argument('--clear-all', action='store_true', help='DANGER: Clear ALL staging tables (removes Excel imports too)')
    parser.add_argument('--clear-staging', action='store_true', help='Clear only previous production migration data (preserves Excel imports)')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    parser.add_argument('--check-prereqs', action='store_true', help='Check prerequisites only')
    parser.add_argument('--analyze-schemas', action='store_true', help='Analyze schemas only')
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    # Create migration step
    step = ProductionMigrationStep(args.project, args.dataset)
    
    print(f"🚀 Production Migration Step")
    print(f"Project: {args.project}")
    print(f"Source: Hubspot_prod")
    print(f"Target: {args.dataset}")
    print(f"Dry run: {args.dry_run}")
    print(f"Clear staging: {args.clear_staging}")
    
    # Check prerequisites only
    if args.check_prereqs:
        print("\n🔍 CHECKING PREREQUISITES...")
        if step.validate_prerequisites():
            print("✅ All prerequisites satisfied")
            return 0
        else:
            print("❌ Prerequisites not met")
            return 1
    
    # Analyze schemas only
    if args.analyze_schemas:
        print("\n🔍 ANALYZING SCHEMAS...")
        analysis = step.analyze_schemas()
        if analysis:
            print("✅ Schema analysis completed")
            return 0
        else:
            print("❌ Schema analysis failed")
            return 1
    
    # Execute migration
    try:
        success = step.execute(
            dry_run=args.dry_run,
            clear_staging=args.clear_staging,
            clear_all=args.clear_all
        )
        
        if success:
            print(f"\n✅ Production migration completed!")
            step.show_status()
            return 0
        else:
            print(f"\n❌ Production migration failed!")
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
    exit(main())