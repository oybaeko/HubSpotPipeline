#!/usr/bin/env python3
"""
Migration package for HubSpot data migration operations
Provides modular components for schema analysis, data migration, and management
"""

from .migration_manager import MigrationManager
from .data_migrator import DataMigrator
from .schema_analyzer import SchemaAnalyzer
from .config import (
    PROJECT_ID, 
    ENVIRONMENTS, 
    MIGRATION_SNAPSHOT_TIMESTAMP,
    normalize_snapshot_id,
    get_migration_config
)

__all__ = [
    "MigrationManager",
    "DataMigrator", 
    "SchemaAnalyzer",
    "PROJECT_ID",
    "ENVIRONMENTS",
    "MIGRATION_SNAPSHOT_TIMESTAMP",
    "normalize_snapshot_id",
    "get_migration_config"
]