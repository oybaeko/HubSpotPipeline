#!/usr/bin/env python3
"""
Simple Table Creator Menu - For VSCode Debugging
File: build-staging/simple_table_creator.py

A simplified menu-driven table creator perfect for VSCode debugging.
Set breakpoints in main() or any menu function.
"""

import sys
import os
import logging
from pathlib import Path
from typing import List, Tuple, Dict

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import pipeline schemas
try:
    from src.hubspot_pipeline.schema import (
        SCHEMA_COMPANIES,
        SCHEMA_DEALS,
        SCHEMA_OWNERS
    )
    SCHEMAS_AVAILABLE = True
except ImportError as e:
    print(f"❌ Failed to import schemas: {e}")
    SCHEMAS_AVAILABLE = False

# Import BigQuery
try:
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound
    BIGQUERY_AVAILABLE = True
except ImportError as e:
    print(f"❌ BigQuery not available: {e}")
    BIGQUERY_AVAILABLE = False

class SimpleTableCreator:
    """Simple table creator with interactive menu"""
    
    def __init__(self):
        self.project_id = "hubspot-452402"
        self.environments = {
            'dev': 'Hubspot_dev_ob',
            'staging': 'Hubspot_staging',
            'prod': 'Hubspot_prod'
        }
        self.environment = 'staging'  # Default
        self.dataset = self.environments[self.environment]
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        self.logger = logging.getLogger('simple.table.creator')
        
        # Initialize BigQuery client
        self.client = None
        if BIGQUERY_AVAILABLE:
            try:
                # Use user credentials
                if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
                    del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
                
                self.client = bigquery.Client(project=self.project_id)
                self.logger.info("✅ BigQuery client ready")
            except Exception as e:
                self.logger.error(f"❌ BigQuery client failed: {e}")
    
    def convert_schema(self, schema: List[Tuple[str, str]]) -> List[bigquery.SchemaField]:
        """Convert pipeline schema to BigQuery fields"""
        fields = []
        for field_name, field_type in schema:
            if field_type == "STRING":
                bq_type = bigquery.SqlTypeNames.STRING
            elif field_type == "FLOAT":
                bq_type = bigquery.SqlTypeNames.FLOAT64
            elif field_type == "INTEGER":
                bq_type = bigquery.SqlTypeNames.INT64
            elif field_type == "BOOLEAN":
                bq_type = bigquery.SqlTypeNames.BOOL
            elif field_type == "TIMESTAMP":
                bq_type = bigquery.SqlTypeNames.TIMESTAMP
            else:
                bq_type = bigquery.SqlTypeNames.STRING
            
            fields.append(bigquery.SchemaField(field_name, bq_type))
        
        return fields
    
    def create_table(self, table_name: str, schema: List[Tuple[str, str]], recreate: bool = False) -> bool:
        """Create a single table"""
        if not self.client:
            print("❌ BigQuery client not available")
            return False
        
        table_id = f"{self.project_id}.{self.dataset}.{table_name}"
        print(f"🔧 Creating {table_name}...")
        print(f"📂 Table ID: {table_id}")
        
        try:
            # Check if exists
            table_exists = False
            try:
                self.client.get_table(table_id)
                table_exists = True
                print(f"📋 Table exists")
            except NotFound:
                print(f"📝 Table doesn't exist")
            
            # Delete if recreating
            if recreate and table_exists:
                self.client.delete_table(table_id)
                print(f"🗑️  Deleted existing table")
            
            # Create table
            if not table_exists or recreate:
                schema_fields = self.convert_schema(schema)
                table = bigquery.Table(table_id, schema=schema_fields)
                
                # Add partitioning for data tables
                if table_name in ['hs_companies', 'hs_deals'] and 'record_timestamp' in [f[0] for f in schema]:
                    table.time_partitioning = bigquery.TimePartitioning(
                        type_=bigquery.TimePartitioningType.DAY,
                        field='record_timestamp'
                    )
                    print(f"📅 Added partitioning by record_timestamp")
                
                self.client.create_table(table)
                print(f"✅ Created {table_name} with {len(schema_fields)} fields")
            else:
                print(f"⏭️  Table exists, skipping")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to create {table_name}: {e}")
            return False
    
    def create_companies_table(self, recreate: bool = False) -> bool:
        """Create companies table"""
        print("\n🏢 CREATING COMPANIES TABLE")
        print("-" * 40)
        
        if not SCHEMAS_AVAILABLE:
            print("❌ Schemas not available")
            return False
        
        return self.create_table('hs_companies', SCHEMA_COMPANIES, recreate)
    
    def create_deals_table(self, recreate: bool = False) -> bool:
        """Create deals table"""
        print("\n🤝 CREATING DEALS TABLE")
        print("-" * 40)
        
        if not SCHEMAS_AVAILABLE:
            print("❌ Schemas not available")
            return False
        
        return self.create_table('hs_deals', SCHEMA_DEALS, recreate)
    
    def create_owners_table(self, recreate: bool = False) -> bool:
        """Create owners table"""
        print("\n👤 CREATING OWNERS TABLE")
        print("-" * 40)
        
        if not SCHEMAS_AVAILABLE:
            print("❌ Schemas not available")
            return False
        
        return self.create_table('hs_owners', SCHEMA_OWNERS, recreate)
    
    def create_all_core_tables(self, recreate: bool = False) -> bool:
        """Create all core tables"""
        print("\n🚀 CREATING ALL CORE TABLES")
        print("=" * 50)
        print(f"Environment: {self.environment}")
        print(f"Dataset: {self.dataset}")
        print(f"Recreate: {recreate}")
        print("=" * 50)
        
        success_count = 0
        
        # Create in dependency order
        if self.create_owners_table(recreate):
            success_count += 1
        
        if self.create_companies_table(recreate):
            success_count += 1
        
        if self.create_deals_table(recreate):
            success_count += 1
        
        print(f"\n📊 SUMMARY: {success_count}/3 tables created")
        if success_count == 3:
            print("🎉 All core tables created successfully!")
            return True
        else:
            print("❌ Some tables failed")
            return False
    
    def show_schemas(self):
        """Show table schemas"""
        print("\n📋 TABLE SCHEMAS")
        print("=" * 50)
        
        if not SCHEMAS_AVAILABLE:
            print("❌ Schemas not available")
            return
        
        print("\n🏢 COMPANIES SCHEMA:")
        for i, (field, type_) in enumerate(SCHEMA_COMPANIES, 1):
            highlight = " ✅ NEW" if field == "record_timestamp" else ""
            print(f"  {i:2}. {field:<25} {type_:<10}{highlight}")
        
        print("\n🤝 DEALS SCHEMA:")
        for i, (field, type_) in enumerate(SCHEMA_DEALS, 1):
            highlight = " ✅ NEW" if field == "record_timestamp" else ""
            print(f"  {i:2}. {field:<25} {type_:<10}{highlight}")
        
        print("\n👤 OWNERS SCHEMA:")
        for i, (field, type_) in enumerate(SCHEMA_OWNERS, 1):
            highlight = " ✅ NEW" if field == "record_timestamp" else ""
            print(f"  {i:2}. {field:<25} {type_:<10}{highlight}")
    
    def check_tables(self):
        """Check if tables exist"""
        print("\n🔍 CHECKING TABLES")
        print("-" * 30)
        
        if not self.client:
            print("❌ BigQuery client not available")
            return
        
        tables = ['hs_owners', 'hs_companies', 'hs_deals']
        
        for table_name in tables:
            table_id = f"{self.project_id}.{self.dataset}.{table_name}"
            try:
                table = self.client.get_table(table_id)
                print(f"✅ {table_name:<15} EXISTS ({table.num_rows:,} rows)")
            except NotFound:
                print(f"❌ {table_name:<15} NOT FOUND")
            except Exception as e:
                print(f"⚠️  {table_name:<15} ERROR: {e}")
    
    def select_environment(self):
        """Select target environment"""
        print("\n🌍 SELECT ENVIRONMENT")
        print("-" * 25)
        print("1) 🟢 dev")
        print("2) 🟡 staging")
        print("3) 🔴 prod")
        
        while True:
            choice = input("\nChoose environment (1-3): ").strip()
            
            if choice == '1':
                self.environment = 'dev'
                break
            elif choice == '2':
                self.environment = 'staging'
                break
            elif choice == '3':
                self.environment = 'prod'
                print("🔴 PRODUCTION WARNING!")
                confirm = input("Type 'PROD' to confirm: ")
                if confirm == 'PROD':
                    self.environment = 'prod'
                    break
                else:
                    continue
            else:
                print("❌ Invalid choice")
        
        self.dataset = self.environments[self.environment]
        print(f"✅ Selected: {self.environment} ({self.dataset})")
    
    def show_menu(self):
        """Main interactive menu - ROOT FUNCTION for VSCode debugging"""
        while True:
            print(f"\n{'='*60}")
            print(f"🏗️  SIMPLE TABLE CREATOR")
            print(f"{'='*60}")
            print(f"Environment: {self.environment}")
            print(f"Dataset: {self.dataset}")
            print(f"BigQuery: {'✅' if self.client else '❌'}")
            print(f"Schemas: {'✅' if SCHEMAS_AVAILABLE else '❌'}")
            print(f"{'='*60}")
            
            print(f"\n📋 MENU OPTIONS")
            print(f"  1) 📋 Show Table Schemas")
            print(f"  2) 🔍 Check Existing Tables")
            print(f"  3) 🏢 Create Companies Table")
            print(f"  4) 🤝 Create Deals Table")
            print(f"  5) 👤 Create Owners Table")
            print(f"  6) 🚀 Create ALL Core Tables")
            print(f"  7) 🔥 Recreate ALL Tables")
            print(f"  8) 🌍 Switch Environment")
            print(f"  0) ❌ Exit")
            
            try:
                choice = input(f"\n🔹 Enter choice (0-8): ").strip()
                
                if choice == '0':
                    print("\n👋 Goodbye!")
                    break
                elif choice == '1':
                    self.show_schemas()
                elif choice == '2':
                    self.check_tables()
                elif choice == '3':
                    self.create_companies_table()
                elif choice == '4':
                    self.create_deals_table()
                elif choice == '5':
                    self.create_owners_table()
                elif choice == '6':
                    self.create_all_core_tables()
                elif choice == '7':
                    print("\n⚠️  RECREATE ALL TABLES")
                    confirm = input("Type 'RECREATE' to confirm: ")
                    if confirm == 'RECREATE':
                        self.create_all_core_tables(recreate=True)
                    else:
                        print("❌ Cancelled")
                elif choice == '8':
                    self.select_environment()
                else:
                    print("❌ Invalid choice. Please select 0-8.")
                    
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
                self.logger.error(f"Menu error: {e}")
            
            if choice != '0':
                input("\n⏸️  Press Enter to continue...")


# ROOT FUNCTION for VSCode debugging
def main():
    """Main entry point - ROOT function for VSCode debugging"""
    print("🏗️  Starting Simple Table Creator")
    print("💡 Perfect for VSCode debugging - set breakpoints here!")
    
    # Check prerequisites
    if not SCHEMAS_AVAILABLE:
        print("❌ Pipeline schemas not available")
        print("💡 Make sure you're in the project root directory")
        return
    
    if not BIGQUERY_AVAILABLE:
        print("❌ BigQuery not available")
        print("💡 Install: pip install google-cloud-bigquery")
        return
    
    # Create and run menu
    try:
        creator = SimpleTableCreator()
        creator.show_menu()  # Set breakpoint here for menu debugging
    except Exception as e:
        print(f"❌ Error: {e}")
        logging.error(f"Main error: {e}", exc_info=True)


if __name__ == "__main__":
    main()