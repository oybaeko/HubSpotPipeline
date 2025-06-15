#!/usr/bin/env python3
"""
HubSpot Data Migration Script - Lightweight Wrapper
Migrates production data to staging with correct timestamps and schema alignment

This is now a lightweight wrapper around the migration package.
For new development, use the migration/ modules directly.
"""

import sys
import os
import logging
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from migration import MigrationManager
    MIGRATION_AVAILABLE = True
except ImportError as e:
    print(f"⚠️  Migration modules not available: {e}")
    MIGRATION_AVAILABLE = False

class DataMigrationManager(MigrationManager):
    """
    Backwards compatibility wrapper for DataMigrationManager
    
    This class maintains the same interface as the original DataMigrationManager
    but now uses the modular migration package internally.
    """
    
    def __init__(self):
        if not MIGRATION_AVAILABLE:
            raise RuntimeError("Migration modules not available")
        
        super().__init__()
        
        # Migration configuration - Updated to match new microsecond format
        self.prod_snapshot_timestamp = "2025-06-08T04:00:11.000000Z"  # Jun 8, 14:00 UTC+10 -> UTC
        self.migration_snapshot_id = self.data_migrator.migration_snapshot_id
    
    # Backwards compatibility methods - delegate to new components
    
    def get_table_schema(self, dataset: str, table: str):
        """Backwards compatibility - delegate to schema analyzer"""
        return self.schema_analyzer.get_table_schema(dataset, table)
    
    def compare_schemas(self, table: str):
        """Backwards compatibility - delegate to schema analyzer"""
        return self.schema_analyzer.compare_schemas(table)
    
    def migrate_prod_to_staging(self, dry_run: bool = True) -> bool:
        """Backwards compatibility - delegate to data migrator"""
        return self.data_migrator.migrate_prod_to_staging(dry_run)
    
    def copy_reference_data_from_dev(self, dry_run: bool = True) -> bool:
        """Backwards compatibility - delegate to data migrator"""
        return self.data_migrator.copy_reference_data_from_dev(dry_run)
    
    def create_snapshot_registry_entry(self, dry_run: bool = True) -> bool:
        """Backwards compatibility - delegate to data migrator"""
        return self.data_migrator.create_snapshot_registry_entry(dry_run)
    
    def check_prod_snapshot_format(self):
        """Backwards compatibility - delegate to schema analyzer"""
        return self.schema_analyzer.check_prod_snapshot_format()
    
    def run_query(self, query: str, dry_run: bool = False):
        """Backwards compatibility - delegate to data migrator"""
        return self.data_migrator.run_query(query, dry_run)


def main():
    """Main entry point"""
    if not MIGRATION_AVAILABLE:
        print("❌ Migration modules not available")
        print("💡 Make sure the migration/ directory exists with all required modules")
        sys.exit(1)
    
    manager = DataMigrationManager()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'schemas':
            manager.show_schema_comparison()
        elif command == 'counts':
            manager.show_data_counts()
        elif command == 'test':
            manager.data_migrator.migrate_prod_to_staging(dry_run=True)
        elif command == 'migrate':
            if manager.confirm_live_migration():
                manager.data_migrator.migrate_prod_to_staging(dry_run=False)
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