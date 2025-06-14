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
        
        # Migration configuration - Updated to match new microsecond format
        self.prod_snapshot_timestamp = "2025-06-08T04:00:11.000000Z"  # Jun 8, 14:00 UTC+10 -> UTC
        self.migration_snapshot_id = self.prod_snapshot_timestamp
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        self.logger = logging.getLogger('data_migration')
        
        # Force use of user credentials by clearing service account env var
        import os
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            self.logger.info("🔧 Clearing service account credentials to use user auth")
            del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        
        # Initialize BigQuery client and get auth info (same as access manager)
        if BIGQUERY_AVAILABLE:
            self.client = bigquery.Client(project=self.project_id)
            self.auth_info = self._get_auth_info()
        else:
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
                        result = self.run_query(count_query)
                        
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
        """Migrate production data to staging with correct timestamp and schema conversion"""
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
            
            # Check for schema compatibility and log issues
            schema_comparison = self.compare_schemas(table)
            if not schema_comparison['compatible']:
                self.logger.warning(f"⚠️  Schema mismatch detected for {table}:")
                for diff in schema_comparison['differences']:
                    if 'type mismatch' in diff:
                        self.logger.warning(f"  🔄 {diff} - Will apply auto-conversion")
                    else:
                        self.logger.info(f"  ℹ️  {diff}")
            
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
            
            # Build migration query with schema-aware field handling
            migration_query = self._build_migration_query(table, schema_comparison)
            
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
        
    def _build_migration_query(self, table: str, schema_comparison: Dict) -> str:
        """Build migration query with schema-aware field conversions"""
        
        # Get actual field names from production and staging
        prod_schema = schema_comparison['schemas'].get('prod', [])
        staging_schema = schema_comparison['schemas'].get('staging', [])
        
        prod_fields = [f['name'] for f in prod_schema] if prod_schema else []
        staging_fields = [f['name'] for f in staging_schema] if staging_schema else []
        
        self.logger.info(f"🔍 Production fields ({len(prod_fields)}): {prod_fields}")
        self.logger.info(f"🔍 Staging fields ({len(staging_fields)}): {staging_fields}")
        
        if table == 'hs_deals':
            # Special handling for hs_deals with type conversions
            self.logger.info("🔄 Building schema-aware migration query for hs_deals")
            
            # Build exact field mapping to match staging schema
            select_fields = []
            
            # Map each staging field to production or create it
            for staging_field in staging_fields:
                if staging_field == 'snapshot_id':
                    select_fields.append(f"'{self.migration_snapshot_id}' as snapshot_id")
                elif staging_field == 'timestamp' and 'timestamp' not in prod_fields:
                    select_fields.append('CURRENT_TIMESTAMP() as timestamp')
                elif staging_field == 'amount' and 'amount' in prod_fields:
                    # Convert STRING to FLOAT
                    select_fields.append('SAFE_CAST(amount AS FLOAT64) as amount')
                elif staging_field in prod_fields:
                    # Direct mapping
                    select_fields.append(staging_field)
                else:
                    # Field missing in production - try alternatives or NULL
                    if staging_field == 'closedate':
                        if 'close_date' in prod_fields:
                            select_fields.append('close_date as closedate')
                        else:
                            select_fields.append('NULL as closedate')
                    elif staging_field == 'createdate':
                        if 'create_date' in prod_fields:
                            select_fields.append('create_date as createdate')
                        else:
                            select_fields.append('NULL as createdate')
                    else:
                        select_fields.append(f'NULL as {staging_field}')
            
            self.logger.info(f"📋 Select fields ({len(select_fields)}): {select_fields}")
            
            query = f"""
            INSERT INTO `{self.project_id}.Hubspot_staging.{table}` 
            SELECT 
                {', '.join(select_fields)}
            FROM `{self.project_id}.Hubspot_prod.{table}`
            """
            
            self.logger.info("✅ Applied field conversions and mappings")
            
        elif table == 'hs_companies':
            # Handle companies with exact field mapping
            self.logger.info("🔄 Building schema-aware migration query for hs_companies")
            
            select_fields = []
            
            # Map each staging field to production or create it
            for staging_field in staging_fields:
                if staging_field == 'snapshot_id':
                    select_fields.append(f"'{self.migration_snapshot_id}' as snapshot_id")
                elif staging_field == 'timestamp' and 'timestamp' not in prod_fields:
                    select_fields.append('CURRENT_TIMESTAMP() as timestamp')
                elif staging_field in prod_fields:
                    # Direct mapping
                    select_fields.append(staging_field)
                else:
                    # Field missing in production
                    select_fields.append(f'NULL as {staging_field}')
            
            self.logger.info(f"📋 Select fields ({len(select_fields)}): {select_fields}")
            
            query = f"""
            INSERT INTO `{self.project_id}.Hubspot_staging.{table}` 
            SELECT 
                {', '.join(select_fields)}
            FROM `{self.project_id}.Hubspot_prod.{table}`
            """
            
            self.logger.info("✅ Built exact field mapping")
            
        else:
            # Default migration for other tables
            self.logger.warning(f"⚠️  Using default migration for {table} - manual review may be needed")
            query = f"""
            INSERT INTO `{self.project_id}.Hubspot_staging.{table}` 
            SELECT 
                *,
                '{self.migration_snapshot_id}' as snapshot_id
            FROM `{self.project_id}.Hubspot_prod.{table}`
            """
        
        return query
    
    def check_prod_snapshot_format(self) -> Dict:
        """Check snapshot_id format in production data with proper error handling"""
        self.logger.info("🔍 Checking production snapshot_id formats")
        
        format_analysis = {}
        
        # Check companies table
        try:
            table_ref = f"{self.project_id}.Hubspot_prod.hs_companies"
            
            # First check if we have access to the table
            try:
                self.client.get_table(table_ref)
            except NotFound:
                self.logger.warning("Production companies table not found")
                format_analysis['hs_companies'] = "Table not found"
                return format_analysis
            except Exception as access_error:
                if "Access Denied" in str(access_error) or "permission" in str(access_error).lower():
                    self.logger.warning("No access to production companies table")
                    format_analysis['hs_companies'] = "No access"
                    format_analysis['hs_deals'] = "No access" 
                    return format_analysis
                else:
                    raise
            
            query = f"""
            SELECT 
                snapshot_id,
                COUNT(*) as record_count
            FROM `{table_ref}`
            GROUP BY snapshot_id
            ORDER BY record_count DESC
            LIMIT 5
            """
            
            result = self.run_query(query)
            if result:
                format_analysis['hs_companies'] = []
                for row in result:
                    snapshot_format = self._analyze_snapshot_format(row.snapshot_id)
                    format_analysis['hs_companies'].append({
                        'snapshot_id': row.snapshot_id,
                        'count': row.record_count,
                        'format': snapshot_format
                    })
                    
        except Exception as e:
            self.logger.warning(f"Could not analyze companies snapshot format: {str(e)[:100]}")
            format_analysis['hs_companies'] = f"Error: {str(e)[:50]}"
        
        # Check deals table  
        try:
            table_ref = f"{self.project_id}.Hubspot_prod.hs_deals"
            
            query = f"""
            SELECT 
                snapshot_id,
                COUNT(*) as record_count
            FROM `{table_ref}`
            GROUP BY snapshot_id
            ORDER BY record_count DESC
            LIMIT 5
            """
            
            result = self.run_query(query)
            if result:
                format_analysis['hs_deals'] = []
                for row in result:
                    snapshot_format = self._analyze_snapshot_format(row.snapshot_id)
                    format_analysis['hs_deals'].append({
                        'snapshot_id': row.snapshot_id,
                        'count': row.record_count,
                        'format': snapshot_format
                    })
                    
        except Exception as e:
            self.logger.warning(f"Could not analyze deals snapshot format: {str(e)[:100]}")
            format_analysis['hs_deals'] = f"Error: {str(e)[:50]}"
        
        return format_analysis
    
    def _analyze_snapshot_format(self, snapshot_id: str) -> Dict:
        """Analyze the format of a snapshot_id"""
        analysis = {
            'has_microseconds': '.000000' in snapshot_id or '.' in snapshot_id,
            'has_timezone': snapshot_id.endswith('Z') or '+' in snapshot_id or snapshot_id.endswith('UTC'),
            'length': len(snapshot_id),
            'format_type': 'unknown'
        }
        
        if len(snapshot_id) == 10:  # YYYY-MM-DD
            analysis['format_type'] = 'date_only'
        elif len(snapshot_id) == 19:  # YYYY-MM-DDTHH:MM:SS
            analysis['format_type'] = 'datetime_no_tz'
        elif len(snapshot_id) == 20 and snapshot_id.endswith('Z'):  # YYYY-MM-DDTHH:MM:SSZ
            analysis['format_type'] = 'datetime_utc'
        elif len(snapshot_id) == 27 and snapshot_id.endswith('Z'):  # YYYY-MM-DDTHH:MM:SS.FFFFFFZ
            analysis['format_type'] = 'datetime_microseconds_utc'
        else:
            analysis['format_type'] = 'custom'
        
        return analysis
    
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
                    self.migrate_prod_to_staging(dry_run=True)
                elif choice == '5':
                    if self.confirm_live_migration():
                        self.migrate_prod_to_staging(dry_run=False)
                elif choice == '6':
                    self.copy_reference_data_from_dev(dry_run=True)
                elif choice == '7':
                    if self.confirm_live_operation("copy reference data"):
                        self.copy_reference_data_from_dev(dry_run=False)
                elif choice == '8':
                    self.create_snapshot_registry_entry(dry_run=True)
                elif choice == '9':
                    if self.confirm_live_operation("create registry entry"):
                        self.create_snapshot_registry_entry(dry_run=False)
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
    
    def show_prod_snapshot_formats(self):
        """Show production snapshot_id formats with better error handling"""
        print(f"\n🔍 PRODUCTION SNAPSHOT FORMATS")
        print("=" * 50)
        
        format_analysis = self.check_prod_snapshot_format()
        
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
                        print(f"      ⚠️  Old format - migration will update to: {self.migration_snapshot_id}")
                    print()
            else:
                print(f"  ❌ Unexpected data format")
        
        print(f"💡 Migration will standardize all data to: {self.migration_snapshot_id}")
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