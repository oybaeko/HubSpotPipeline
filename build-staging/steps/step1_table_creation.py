#!/usr/bin/env python3
"""
Step 1: Table Creation
Extracted from main_migration_orchestrator.py - uses simple_table_creator.py
"""

import sys
import os
import logging
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

class TableCreationStep:
    """
    Step 1: Create BigQuery tables using simple_table_creator.py
    
    This step:
    1. Uses existing simple_table_creator.py for table operations
    2. Supports multiple environments (dev, staging, prod)
    3. Can create missing tables only or recreate all tables
    4. Validates table structure and dependencies
    """
    
    def __init__(self, project_id: str = "hubspot-452402", dataset: str = "Hubspot_staging"):
        self.project_id = project_id
        self.staging_dataset = dataset
        
        # Setup logging
        self.logger = logging.getLogger('table_creation_step')
        
        # Setup paths and imports
        self._setup_environment()
        
        # Track results
        self.results = {}
        self.completed = False
    
    def _setup_environment(self):
        """Setup paths and clear service account credentials"""
        # Clear service account credentials to use user auth (from orchestration)
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
            self.logger.debug("Cleared GOOGLE_APPLICATION_CREDENTIALS to use user auth")
        
        # Add path for simple_table_creator (should be in build-staging/)
        build_staging_path = Path(__file__).parent.parent  # steps/../ = build-staging/
        if str(build_staging_path) not in sys.path:
            sys.path.insert(0, str(build_staging_path))
        
        self.logger.debug(f"Added path: {build_staging_path}")
    
    def validate_prerequisites(self) -> bool:
        """Check if simple_table_creator.py is available"""
        try:
            # Check if simple_table_creator exists
            table_creator_script = Path(__file__).parent.parent / "simple_table_creator.py"
            if not table_creator_script.exists():
                self.logger.error(f"❌ simple_table_creator.py not found: {table_creator_script}")
                self.logger.error("💡 Expected location: build-staging/simple_table_creator.py")
                return False
            
            # Try to import SimpleTableCreator
            from simple_table_creator import SimpleTableCreator
            
            self.logger.info("✅ simple_table_creator.py is available")
            return True
            
        except ImportError as e:
            self.logger.error(f"❌ Failed to import SimpleTableCreator: {e}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Error checking prerequisites: {e}")
            return False
    
    def execute(self, recreate: bool = False, interactive: bool = True, clear_all: bool = False) -> bool:
        """Execute table creation using simple_table_creator"""
        self.logger.info("🏗️ STEP 1: Creating tables using simple_table_creator.py")
        
        try:
            # Validate prerequisites
            if not self.validate_prerequisites():
                return False
            
            # Import the simple table creator
            from simple_table_creator import SimpleTableCreator
            
            # Initialize table creator
            self.logger.info("🔧 Initializing simple table creator...")
            creator = SimpleTableCreator()
            
            # Determine environment based on dataset
            environment = self._get_environment_from_dataset()
            creator.environment = environment
            creator.dataset = creator.environments[environment]
            
            self.logger.info(f"🎯 Target: {creator.environment} ({creator.dataset})")
            
            # Check existing tables
            self.logger.info("🔍 Checking existing tables...")
            creator.check_tables()
            
            # Handle recreation decision
            if interactive:
                operation = self._confirm_table_operation(recreate, clear_all)
                if operation is None:  # User cancelled
                    self.logger.info("❌ Operation cancelled")
                    return False
                recreate, clear_all = operation
            
            # Clear all tables if requested
            if clear_all:
                success = self._clear_all_staging_tables(creator)
                if not success:
                    return False
            
            # Create tables
            self.logger.info(f"🚀 Creating core tables (recreate={recreate})...")
            success = creator.create_all_core_tables(recreate=recreate)
            
            if success and clear_all:
                # If we cleared all tables, we need to recreate ALL tables, not just core
                self.logger.info("📋 Creating additional required tables after clear all...")
                additional_success = self._create_additional_tables()
                if not additional_success:
                    self.logger.warning("⚠️ Some additional tables failed to create")
            
            if success:
                # Count all tables in dataset now
                final_table_count = self._count_all_tables()
                
                self.results = {
                    'environment': environment,
                    'dataset': creator.dataset,
                    'recreate': recreate,
                    'clear_all': clear_all,
                    'tables_created': final_table_count,
                    'project': self.project_id
                }
                
                self.logger.info("✅ Table creation completed successfully")
                self.logger.info(f"📊 Environment: {environment}")
                self.logger.info(f"📂 Dataset: {creator.dataset}")
                self.logger.info(f"🏗️ Tables available: {final_table_count}")
                
                if clear_all:
                    self.logger.info("🧹 All staging tables cleared and recreated")
                
                self.completed = True
                return True
            else:
                self.logger.error("❌ Table creation failed")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Table creation failed: {e}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            return False
    
    def _get_environment_from_dataset(self) -> str:
        """Determine environment from dataset name"""
        dataset_lower = self.staging_dataset.lower()
        
        if 'dev' in dataset_lower:
            return 'dev'
        elif 'staging' in dataset_lower:
            return 'staging'
        elif 'prod' in dataset_lower:
            return 'prod'
        else:
            # Default to staging
            self.logger.warning(f"⚠️ Could not determine environment from dataset '{self.staging_dataset}', defaulting to staging")
            return 'staging'
    
    def _confirm_table_operation(self, default_recreate: bool, default_clear_all: bool) -> tuple:
        """Confirm table creation operation with clear all option"""
        environment = self._get_environment_from_dataset()
        
        print(f"\n🏗️ TABLE CREATION OPTIONS")
        print(f"Environment: {environment}")
        print(f"Dataset: {self.staging_dataset}")
        print(f"1) Create missing tables only")
        print(f"2) Recreate core tables (DELETE + CREATE core tables)")
        print(f"3) 🧹 CLEAR ALL STAGING TABLES + Recreate (COMPLETE FRESH START)")
        print(f"4) Skip table creation")
        
        while True:
            choice = input("\nChoose option (1-4): ").strip()
            
            if choice == '1':
                return (False, False)  # Don't recreate, don't clear all
            elif choice == '2':
                if environment == 'prod':
                    print(f"\n⚠️ PRODUCTION WARNING")
                    print(f"This will DELETE core tables in production!")
                    confirm = input("Type 'RECREATE PRODUCTION TABLES' to confirm: ")
                    if confirm == 'RECREATE PRODUCTION TABLES':
                        return (True, False)  # Recreate core only
                    else:
                        self.logger.info("❌ Production operation cancelled")
                        continue
                else:
                    print(f"\n⚠️ RECREATE CORE TABLES WARNING")
                    print(f"This will DELETE core tables in {environment}!")
                    confirm = input("Type 'RECREATE CORE TABLES' to confirm: ")
                    if confirm == 'RECREATE CORE TABLES':
                        return (True, False)  # Recreate core only
                    else:
                        self.logger.info("❌ Operation cancelled")
                        continue
            elif choice == '3':
                if environment == 'prod':
                    print(f"\n🚨 PRODUCTION DANGER WARNING")
                    print(f"This will DELETE ALL TABLES in production staging!")
                    print(f"This includes ALL data, registry, history, everything!")
                    confirm = input("Type 'CLEAR ALL PRODUCTION TABLES' to confirm: ")
                    if confirm == 'CLEAR ALL PRODUCTION TABLES':
                        return (True, True)  # Recreate + clear all
                    else:
                        self.logger.info("❌ Production operation cancelled")
                        continue
                else:
                    print(f"\n🧹 CLEAR ALL STAGING TABLES WARNING")
                    print(f"This will DELETE ALL TABLES in {environment} staging!")
                    print(f"Including:")
                    print(f"  • All core tables (companies, deals, etc.)")
                    print(f"  • Registry (hs_snapshot_registry)")
                    print(f"  • History tables")
                    print(f"  • Analytics views")
                    print(f"  • Everything in {self.staging_dataset}")
                    print(f"\nThis gives you a COMPLETE FRESH START!")
                    confirm = input("Type 'CLEAR ALL STAGING TABLES' to confirm: ")
                    if confirm == 'CLEAR ALL STAGING TABLES':
                        return (True, True)  # Recreate + clear all
                    else:
                        self.logger.info("❌ Operation cancelled")
                        continue
            elif choice == '4':
                self.logger.info("⏭️ Skipping table creation")
                return None  # Skip operation
            else:
                print("❌ Invalid choice")
    
    def _clear_all_staging_tables(self, creator) -> bool:
        """Clear ALL tables in the staging dataset for complete fresh start"""
        self.logger.info("🧹 CLEARING ALL STAGING TABLES...")
        
        try:
            # Get BigQuery client
            from google.cloud import bigquery
            client = bigquery.Client(project=self.project_id)
            
            # List all tables in the dataset
            dataset_ref = client.dataset(self.staging_dataset)
            
            try:
                tables = list(client.list_tables(dataset_ref))
                total_count = len(tables)
                
                if total_count == 0:
                    self.logger.info("📭 No tables found in dataset - already clean")
                    return True
                
                # Separate tables and views
                tables_to_delete = []
                views_to_skip = []
                
                for table in tables:
                    if table.table_type == 'VIEW':
                        views_to_skip.append(table)
                    else:
                        tables_to_delete.append(table)
                
                table_count = len(tables_to_delete)
                view_count = len(views_to_skip)
                
                self.logger.info(f"🔍 Found {total_count} objects: {table_count} tables to delete, {view_count} views to skip")
                
                # Show what will be deleted
                if tables_to_delete:
                    self.logger.info("📋 Tables to delete:")
                    for table in tables_to_delete:
                        self.logger.info(f"   🗑️  {table.table_id}")
                
                # Show what will be skipped
                if views_to_skip:
                    self.logger.info("👁️ Views to skip (auto-recreated by scoring):")
                    for view in views_to_skip:
                        self.logger.info(f"   ⏭️  {view.table_id}")
                
                if table_count == 0:
                    self.logger.info("📭 No tables to delete - only views found")
                    return True
                
                # Delete only tables (skip views)
                deleted_count = 0
                failed_count = 0
                
                for table in tables_to_delete:
                    try:
                        client.delete_table(table, not_found_ok=True)
                        deleted_count += 1
                        self.logger.info(f"   ✅ Deleted {table.table_id}")
                    except Exception as e:
                        failed_count += 1
                        self.logger.error(f"   ❌ Failed to delete {table.table_id}: {e}")
                
                if failed_count == 0:
                    self.logger.info(f"🎉 Successfully deleted all {deleted_count} tables")
                    if view_count > 0:
                        self.logger.info(f"👁️ Preserved {view_count} views (will be auto-recreated)")
                    self.logger.info("🧹 Dataset tables are now completely clean!")
                    return True
                else:
                    self.logger.warning(f"⚠️ Deleted {deleted_count}/{table_count} tables ({failed_count} failed)")
                    return False
                    
            except Exception as e:
                if "404" in str(e) or "not found" in str(e).lower():
                    self.logger.info(f"📭 Dataset {self.staging_dataset} is empty or doesn't exist")
                    return True
                else:
                    self.logger.error(f"❌ Error listing tables: {e}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"❌ Failed to clear staging tables: {e}")
            return False
    
    def _create_additional_tables(self) -> bool:
        """Create additional tables beyond core tables using authoritative schemas"""
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project=self.project_id)
            
            # Import authoritative schemas from src/hubspot_pipeline/schema.py
            try:
                # Add src path for schema imports
                src_path = Path(__file__).parent.parent.parent / "src" / "hubspot_pipeline"
                if str(src_path) not in sys.path:
                    sys.path.insert(0, str(src_path))
                
                from schema import (
                    SCHEMA_SNAPSHOT_REGISTRY,
                    SCHEMA_DEAL_STAGE_REFERENCE, 
                    SCHEMA_STAGE_MAPPING,
                    SCHEMA_PIPELINE_SCORE_HISTORY,
                    SCHEMA_PIPELINE_UNITS_SNAPSHOT
                )
                self.logger.info("✅ Using authoritative schemas from src/hubspot_pipeline/schema.py")
                schema_source = "src/hubspot_pipeline/schema"
            except ImportError as e:
                self.logger.error(f"❌ Failed to import authoritative schemas: {e}")
                self.logger.error("💡 Check that src/hubspot_pipeline/schema.py exists")
                return False
            
            # Map schema definitions to table names
            additional_tables = {
                'hs_snapshot_registry': SCHEMA_SNAPSHOT_REGISTRY,
                'hs_deal_stage_reference': SCHEMA_DEAL_STAGE_REFERENCE,
                'hs_stage_mapping': SCHEMA_STAGE_MAPPING,
                'hs_pipeline_score_history': SCHEMA_PIPELINE_SCORE_HISTORY,
                'hs_pipeline_units_snapshot': SCHEMA_PIPELINE_UNITS_SNAPSHOT,
            }
            
            created_count = 0
            failed_count = 0
            
            for table_name, schema_def in additional_tables.items():
                try:
                    table_id = f"{self.project_id}.{self.staging_dataset}.{table_name}"
                    
                    # Convert schema format from (name, type) tuples to BigQuery SchemaFields
                    schema = [bigquery.SchemaField(name, field_type) for name, field_type in schema_def]
                    
                    table = bigquery.Table(table_id, schema=schema)
                    
                    # Add partitioning for tables with record_timestamp
                    if any(field.name == "record_timestamp" for field in schema):
                        table.time_partitioning = bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field="record_timestamp"
                        )
                    
                    created_table = client.create_table(table)
                    created_count += 1
                    self.logger.info(f"   ✅ Created {table_name} (from {schema_source})")
                    
                except Exception as e:
                    failed_count += 1
                    self.logger.error(f"   ❌ Failed to create {table_name}: {e}")
            
            self.logger.info(f"📊 Additional tables: {created_count} created, {failed_count} failed")
            self.logger.info(f"🔗 Schemas sourced from: {schema_source}")
            return failed_count == 0
            
        except Exception as e:
            self.logger.error(f"❌ Failed to create additional tables: {e}")
            return False
    
    def _count_all_tables(self) -> int:
        """Count all tables in the dataset"""
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project=self.project_id)
            
            dataset_ref = client.dataset(self.staging_dataset)
            tables = list(client.list_tables(dataset_ref))
            return len(tables)
            
        except Exception as e:
            self.logger.debug(f"Could not count tables: {e}")
            return 0
    
    def get_results(self) -> Dict[str, Any]:
        """Get step execution results"""
        return {
            'completed': self.completed,
            'results': self.results
        }
    
    def show_status(self):
        """Show current status and results"""
        print(f"\n🏗️ TABLE CREATION STATUS")
        print("=" * 50)
        print(f"Project: {self.project_id}")
        print(f"Dataset: {self.staging_dataset}")
        print(f"Completed: {'✅' if self.completed else '❌'}")
        
        if self.results:
            print(f"\n📈 RESULTS:")
            for key, value in self.results.items():
                print(f"  • {key}: {value}")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Table Creation Step")
    parser.add_argument('--project', default='hubspot-452402', help='BigQuery project')
    parser.add_argument('--dataset', default='Hubspot_staging', help='BigQuery dataset')
    parser.add_argument('--recreate', action='store_true', help='Recreate core tables (delete + create)')
    parser.add_argument('--clear-all', action='store_true', help='Clear ALL tables in staging (complete fresh start)')
    parser.add_argument('--non-interactive', action='store_true', help='Non-interactive mode')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    parser.add_argument('--check-prereqs', action='store_true', help='Check prerequisites only')
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    # Create table creation step
    step = TableCreationStep(args.project, args.dataset)
    
    print(f"🏗️ Table Creation Step")
    print(f"Project: {args.project}")
    print(f"Dataset: {args.dataset}")
    print(f"Mode: {'Non-interactive' if args.non_interactive else 'Interactive'}")
    
    # Check prerequisites only
    if args.check_prereqs:
        print("\n🔍 CHECKING PREREQUISITES...")
        if step.validate_prerequisites():
            print("✅ All prerequisites satisfied")
            return 0
        else:
            print("❌ Prerequisites not met")
            return 1
    
    # Execute table creation
    try:
        success = step.execute(
            recreate=args.recreate,
            interactive=not args.non_interactive,
            clear_all=args.clear_all
        )
        
        if success:
            print(f"\n✅ Table creation completed!")
            step.show_status()
            return 0
        else:
            print(f"\n❌ Table creation failed!")
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