#!/usr/bin/env python3
"""
Core data migration operations
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional
from google.cloud import bigquery
from .config import PROJECT_ID, ENVIRONMENTS, MIGRATION_SNAPSHOT_TIMESTAMP, normalize_snapshot_id

class DataMigrator:
    """Handles core data migration operations"""
    
    def __init__(self):
        self.project_id = PROJECT_ID
        self.environments = ENVIRONMENTS
        self.migration_snapshot_id = normalize_snapshot_id(MIGRATION_SNAPSHOT_TIMESTAMP)
        self.logger = logging.getLogger('migration.data')
        
        # Force use of user credentials
        import os
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            self.logger.info("🔧 Clearing service account credentials to use user auth")
            del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        
        try:
            self.client = bigquery.Client(project=self.project_id)
        except Exception as e:
            self.logger.error(f"Failed to initialize BigQuery client: {e}")
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
    
    def migrate_prod_to_staging(self, dry_run: bool = True) -> bool:
        """Migrate production data to staging with correct timestamp and schema conversion"""
        self.logger.info(f"🚀 Starting prod to staging migration (dry_run={dry_run})")
        
        tables_to_migrate = ['hs_companies', 'hs_deals']
        success_count = 0
        
        # Import schema analyzer for compatibility check
        from .schema_analyzer import SchemaAnalyzer
        schema_analyzer = SchemaAnalyzer()
        
        for table in tables_to_migrate:
            self.logger.info(f"📦 Migrating {table}")
            
            # Check for schema compatibility and log issues
            schema_comparison = schema_analyzer.compare_schemas(table)
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
                clear_query = f"TRUNCATE TABLE `{self.project_id}.Hubspot_staging.{table}`"
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
        return migration_success  
      
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
                elif staging_field == 'record_timestamp':
                    # Use current timestamp for all migrated records
                    select_fields.append('CURRENT_TIMESTAMP() as record_timestamp')
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
                elif staging_field == 'record_timestamp':
                    # Use current timestamp for all migrated records
                    select_fields.append('CURRENT_TIMESTAMP() as record_timestamp')
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
                '{self.migration_snapshot_id}' as snapshot_id,
                CURRENT_TIMESTAMP() as record_timestamp
            FROM `{self.project_id}.Hubspot_prod.{table}`
            """
        
        return query
    
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
                clear_query = f"TRUNCATE TABLE `{self.project_id}.Hubspot_staging.{table}`"
                if not self.run_query(clear_query):
                    self.logger.error(f"❌ Failed to clear {table} in staging")
                    continue
                self.logger.info(f"🗑️  Cleared existing data in staging.{table}")
            
            # Copy query with record_timestamp
            copy_query = f"""
            INSERT INTO `{self.project_id}.Hubspot_staging.{table}` 
            SELECT 
                *,
                CURRENT_TIMESTAMP() as record_timestamp
            FROM `{self.project_id}.Hubspot_dev_ob.{table}`
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
        (snapshot_id, record_timestamp, triggered_by, status, notes)
        VALUES (
            '{self.migration_snapshot_id}',
            CURRENT_TIMESTAMP(),
            'data_migration_script',
            'migration_completed_ingest_completed_scoring_pending',
            'Migrated from production data with original timestamp from Jun 8, 2025 14:00 UTC+10'
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