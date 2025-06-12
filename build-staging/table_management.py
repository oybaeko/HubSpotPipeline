#!/usr/bin/env python3
"""
File: build-staging/staging_data.py
Path: build-staging/staging_data.py

Simplified Staging Data Operations - Focus on Table Management
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import json

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import BigQuery
try:
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    BIGQUERY_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  BigQuery not available: {e}")
    print("💡 Install: pip install google-cloud-bigquery")
    BIGQUERY_AVAILABLE = False

class StagingDataManager:
    """Simplified staging data operations manager - focus on table management"""
    
    def __init__(self, environment: Optional[str] = None):
        self.logger = self._setup_logging()
        self.config = self._load_simple_config()
        
        # Set environment
        if environment:
            self.config['environment'] = environment
        
        self.config['dataset'] = self._get_dataset_for_env(self.config['environment'])
        
        # Initialize BigQuery client
        self.bq_client = None
        if BIGQUERY_AVAILABLE:
            try:
                self.bq_client = bigquery.Client(project=self.config['project'])
                self.logger.info("✅ BigQuery client initialized")
            except Exception as e:
                self.logger.warning(f"⚠️  BigQuery client failed: {e}")
                self.bq_client = None
        
    def _setup_logging(self) -> logging.Logger:
        """Simple logging setup"""
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        return logging.getLogger('staging.data')
    
    def _load_simple_config(self) -> Dict[str, Any]:
        """Simple configuration"""
        return {
            'environment': 'dev',
            'project': 'hubspot-452402',
            'dataset': 'Hubspot_dev_ob'
        }
    
    def _get_dataset_for_env(self, environment: str) -> str:
        """Get BigQuery dataset for environment"""
        datasets = {
            'dev': 'Hubspot_dev_ob',
            'staging': 'Hubspot_staging', 
            'prod': 'Hubspot_prod'
        }
        return datasets.get(environment, 'Hubspot_dev_ob')
    
    def _get_env_color(self, env: str) -> str:
        """Get color emoji for environment"""
        colors = {'dev': '🟢', 'staging': '🟡', 'prod': '🔴'}
        return colors.get(env, '⚪')

    def show_data_menu(self):
        """Main menu - ROOT FUNCTION for VSCode debugging"""
        # Select environment first
        if not hasattr(self, '_env_selected'):
            self._select_environment()
            self._env_selected = True
        
        while True:
            self._print_header()
            self._print_menu()
            
            try:
                choice = input("\n🔹 Enter choice (0-5): ").strip()
                
                if choice == '0':
                    print("\n👋 Exiting")
                    break
                elif choice == '1':
                    self._show_table_status()
                elif choice == '2':
                    self._clear_tables()
                elif choice == '3':
                    self._recreate_tables()
                elif choice == '4':
                    self._switch_environment()
                elif choice == '5':
                    self._test_bigquery_connection()
                else:
                    print("❌ Invalid choice. Please select 0-5.")
                    
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
            
            if choice != '0':
                input("\n⏸️  Press Enter to continue...")
    
    def _select_environment(self):
        """Select target environment"""
        print("\n🌍 ENVIRONMENT SELECTION")
        print("="*40)
        print("1) 🟢 dev        (Development)")
        print("2) 🟡 staging    (Staging)")
        print("3) 🔴 prod       (Production)")
        
        while True:
            choice = input("\nChoose environment (1-3, default=1): ").strip()
            
            if choice in ['', '1']:
                env = 'dev'
                break
            elif choice == '2':
                env = 'staging'
                break
            elif choice == '3':
                env = 'prod'
                if self._confirm_production():
                    env = 'prod'
                    break
                else:
                    continue
            else:
                print("❌ Invalid choice")
                continue
        
        self.config['environment'] = env
        self.config['dataset'] = self._get_dataset_for_env(env)
        print(f"✅ Environment: {self._get_env_color(env)} {env}")
    
    def _confirm_production(self) -> bool:
        """Confirm production access"""
        print("\n🔴 PRODUCTION WARNING")
        print("This affects live business data!")
        confirm = input("Type 'ACCESS PRODUCTION' to continue: ").strip()
        return confirm == 'ACCESS PRODUCTION'
    
    def _switch_environment(self):
        """Switch environment"""
        print(f"\n🔄 Current: {self._get_env_color(self.config['environment'])} {self.config['environment']}")
        self._select_environment()

    def _print_header(self):
        """Print header"""
        env = self.config['environment']
        print(f"\n{'='*60}")
        print(f"🏗️  STAGING TABLE MANAGEMENT")
        print(f"{'='*60}")
        print(f"Environment: {self._get_env_color(env)} {env.upper()}")
        print(f"Project: {self.config['project']}")
        print(f"Dataset: {self.config['dataset']}")
        print(f"{'='*60}")
    
    def _print_menu(self):
        """Print menu options"""
        env = self.config['environment']
        
        print("\n📊 TABLE OPERATIONS")
        print("  1) 📋 Show Table Status")
        print("  2) 🗑️  Clear Tables (TRUNCATE)")
        print("  3) 🔥 Recreate Tables (DROP + CREATE)")
        print("  4) 🔄 Switch Environment")
        print("  5) 🔧 Test BigQuery Connection")
        print("  0) ❌ Exit")
        
        # Environment warnings
        if env == 'prod':
            print(f"\n🔴 PRODUCTION: Write operations require confirmation")
        elif env == 'staging':
            print(f"\n🟡 STAGING: Test environment")
        else:
            print(f"\n🟢 DEVELOPMENT: Safe for all operations")
    
    def _show_table_status(self):
        """Show table status"""
        print("\n📋 TABLE STATUS")
        print("-" * 40)
        
        if not self.bq_client:
            print("❌ BigQuery client not available")
            return
        
        tables = self._get_tables()
        
        print(f"Environment: {self._get_env_color(self.config['environment'])} {self.config['environment']}")
        print(f"Dataset: {self.config['dataset']}")
        print(f"Expected tables: {len(tables)}")
        print()
        
        existing_count = 0
        for i, table in enumerate(tables, 1):
            try:
                table_ref = f"{self.config['project']}.{self.config['dataset']}.{table}"
                table_obj = self.bq_client.get_table(table_ref)
                row_count = table_obj.num_rows
                print(f"{i:2}. {table:<30} ✅ {row_count:,} rows")
                existing_count += 1
            except NotFound:
                print(f"{i:2}. {table:<30} ❌ Not found")
            except Exception as e:
                print(f"{i:2}. {table:<30} ⚠️  Error: {e}")
        
        print(f"\n📊 Summary: {existing_count}/{len(tables)} tables exist")
    
    def _clear_tables(self):
        """Clear all tables (TRUNCATE)"""
        env = self.config['environment']
        
        print("\n🗑️  CLEAR TABLES (TRUNCATE)")
        print("-" * 40)
        
        if not self.bq_client:
            print("❌ BigQuery client not available")
            return
        
        # Production safety
        if env == 'prod':
            print("🔴 PRODUCTION: Clear operations blocked")
            return
        
        tables = self._get_tables()
        print(f"This will clear {len(tables)} tables:")
        for table in tables:
            print(f"  • {table}")
        
        confirm = input(f"\nType 'CLEAR {env.upper()}' to confirm: ").strip()
        if confirm != f'CLEAR {env.upper()}':
            print("❌ Operation cancelled")
            return
        
        print("\n🗑️  Clearing tables...")
        cleared_count = 0
        error_count = 0
        
        for table in tables:
            try:
                table_ref = f"{self.config['project']}.{self.config['dataset']}.{table}"
                
                # Check if table exists first
                try:
                    self.bq_client.get_table(table_ref)
                except NotFound:
                    print(f"  ⏭️  {table} - Table doesn't exist, skipping")
                    continue
                
                # TRUNCATE table
                query = f"TRUNCATE TABLE `{table_ref}`"
                job = self.bq_client.query(query)
                job.result()  # Wait for completion
                
                print(f"  ✅ {table} - Cleared")
                cleared_count += 1
                
            except Exception as e:
                print(f"  ❌ {table} - Error: {e}")
                error_count += 1
        
        print(f"\n📊 Summary: {cleared_count} cleared, {error_count} errors")
        if cleared_count > 0:
            print("✅ Tables cleared successfully")
    
    def _recreate_tables(self):
        """Recreate all tables (DROP + CREATE)"""
        env = self.config['environment']
        
        print("\n🔥 RECREATE TABLES (DROP + CREATE)")
        print("-" * 40)
        
        if not self.bq_client:
            print("❌ BigQuery client not available")
            return
        
        # Production safety
        if env == 'prod':
            print("🔴 PRODUCTION: Recreate operations blocked")
            return
        
        tables = self._get_tables()
        print("⚠️  This will DESTROY and RECREATE all tables!")
        print(f"Tables: {len(tables)}")
        for i, table in enumerate(tables, 1):
            print(f"  {i}. {table}")
        
        print(f"\n🚨 DESTRUCTIVE OPERATION")
        confirm1 = input(f"Type 'DESTROY {env.upper()}' to confirm: ").strip()
        if confirm1 != f'DESTROY {env.upper()}':
            print("❌ Operation cancelled")
            return
        
        confirm2 = input("Type 'YES I AM SURE' for final confirmation: ").strip()
        if confirm2 != 'YES I AM SURE':
            print("❌ Operation cancelled")
            return
        
        print("\n🔥 Recreating tables...")
        
        # Step 1: Drop tables in reverse order
        print("\n1️⃣ Dropping tables...")
        dropped_count = 0
        for table in reversed(tables):
            try:
                table_ref = f"{self.config['project']}.{self.config['dataset']}.{table}"
                self.bq_client.delete_table(table_ref, not_found_ok=True)
                print(f"  🔥 DROP {table}")
                dropped_count += 1
            except Exception as e:
                print(f"  ❌ DROP {table} - Error: {e}")
        
        # Step 2: Create tables in correct order
        print("\n2️⃣ Creating tables...")
        created_count = 0
        for table in tables:
            try:
                # TODO: Get actual schema for each table
                schema = self._get_table_schema(table)
                if schema:
                    table_ref = f"{self.config['project']}.{self.config['dataset']}.{table}"
                    table_obj = bigquery.Table(table_ref, schema=schema)
                    self.bq_client.create_table(table_obj)
                    print(f"  ✅ CREATE {table}")
                    created_count += 1
                else:
                    print(f"  ⚠️  CREATE {table} - No schema available")
            except Exception as e:
                print(f"  ❌ CREATE {table} - Error: {e}")
        
        print(f"\n📊 Summary: {dropped_count} dropped, {created_count} created")
        if created_count > 0:
            print("✅ Tables recreated successfully")
    
    def _get_table_schema(self, table_name: str) -> Optional[List[bigquery.SchemaField]]:
        """Get schema for table (placeholder - needs actual schemas)"""
        # TODO: Import actual schemas from your schema.py
        # This is a placeholder implementation
        
        basic_schemas = {
            'hs_owners': [
                bigquery.SchemaField("owner_id", "STRING"),
                bigquery.SchemaField("email", "STRING"),
                bigquery.SchemaField("first_name", "STRING"),
                bigquery.SchemaField("last_name", "STRING"),
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
            ],
            'hs_companies': [
                bigquery.SchemaField("company_id", "STRING"),
                bigquery.SchemaField("company_name", "STRING"),
                bigquery.SchemaField("lifecycle_stage", "STRING"),
                bigquery.SchemaField("snapshot_id", "STRING"),
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
            ],
            'hs_deals': [
                bigquery.SchemaField("deal_id", "STRING"),
                bigquery.SchemaField("deal_name", "STRING"),
                bigquery.SchemaField("deal_stage", "STRING"),
                bigquery.SchemaField("amount", "FLOAT"),
                bigquery.SchemaField("snapshot_id", "STRING"),
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
            ],
            'hs_snapshot_registry': [
                bigquery.SchemaField("snapshot_id", "STRING"),
                bigquery.SchemaField("snapshot_timestamp", "TIMESTAMP"),
                bigquery.SchemaField("triggered_by", "STRING"),
                bigquery.SchemaField("status", "STRING"),
            ]
        }
        
        return basic_schemas.get(table_name)
    
    def _test_bigquery_connection(self):
        """Test BigQuery connection"""
        print("\n🔧 BIGQUERY CONNECTION TEST")
        print("-" * 40)
        
        if not self.bq_client:
            print("❌ BigQuery client not available")
            return
        
        try:
            print(f"Project: {self.config['project']}")
            print(f"Dataset: {self.config['dataset']}")
            
            # Test 1: List datasets
            print("\n1️⃣ Testing dataset access...")
            datasets = list(self.bq_client.list_datasets())
            target_dataset = None
            for dataset in datasets:
                if dataset.dataset_id == self.config['dataset']:
                    target_dataset = dataset
                    break
            
            if target_dataset:
                print(f"  ✅ Dataset '{self.config['dataset']}' found")
            else:
                print(f"  ❌ Dataset '{self.config['dataset']}' not found")
                print(f"  Available datasets: {[d.dataset_id for d in datasets[:5]]}")
            
            # Test 2: List tables
            print("\n2️⃣ Testing table access...")
            try:
                dataset_ref = self.bq_client.dataset(self.config['dataset'])
                tables = list(self.bq_client.list_tables(dataset_ref))
                print(f"  ✅ Found {len(tables)} tables in dataset")
                if tables:
                    for table in tables[:3]:
                        print(f"    • {table.table_id}")
                    if len(tables) > 3:
                        print(f"    • ... and {len(tables) - 3} more")
            except Exception as e:
                print(f"  ❌ Table listing failed: {e}")
            
            # Test 3: Simple query
            print("\n3️⃣ Testing query execution...")
            try:
                query = "SELECT 1 as test_value"
                job = self.bq_client.query(query)
                results = job.result()
                for row in results:
                    print(f"  ✅ Query test passed: {row.test_value}")
                    break
            except Exception as e:
                print(f"  ❌ Query test failed: {e}")
            
            print("\n✅ BigQuery connection test completed")
            
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            self.logger.error(f"BigQuery connection test failed: {e}")
    
    def _get_tables(self) -> List[str]:
        """Get list of tables in dependency order"""
        return [
            'hs_owners',
            'hs_deal_stage_reference',
            'hs_snapshot_registry',
            'hs_companies',
            'hs_deals',
            'hs_stage_mapping',
            'hs_pipeline_units_snapshot',
            'hs_pipeline_score_history'
        ]


# CLI wrapper
def run_cli():
    """Simple CLI wrapper"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Staging Table Management")
    parser.add_argument("--environment", choices=['dev', 'staging', 'prod'], help="Target environment")
    args = parser.parse_args()
    
    manager = StagingDataManager(args.environment)
    manager.show_data_menu()


# ROOT FUNCTION for VSCode debugging
def main():
    """Main entry point - ROOT function for VSCode debugging"""
    print("🏗️  Starting Staging Table Management")
    print("💡 Simplified version focused on table operations")
    
    try:
        manager = StagingDataManager()
        manager.show_data_menu()
    except Exception as e:
        print(f"❌ Error: {e}")
        logging.error(f"Error: {e}", exc_info=True)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_cli()
    else:
        main()