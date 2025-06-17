#!/usr/bin/env python3
"""
Migration configuration and environment settings
"""

from datetime import datetime, timezone
from typing import Dict

# Project configuration
PROJECT_ID = "hubspot-452402"

# Environment mappings
ENVIRONMENTS = {
    'dev': 'Hubspot_dev_ob',
    'staging': 'Hubspot_staging', 
    'prod': 'Hubspot_prod'
}

# Migration configuration - Updated to match new microsecond format
MIGRATION_SNAPSHOT_TIMESTAMP = "2025-06-08T04:00:11.000000Z"  # Jun 8, 14:00 UTC+10 -> UTC

def get_dataset_for_env(env: str) -> str:
    """Get BigQuery dataset for environment"""
    return ENVIRONMENTS.get(env, 'Hubspot_dev_ob')

def normalize_snapshot_id(snapshot_id: str) -> str:
    """Convert snapshot_id to current microsecond Z format"""
    if not snapshot_id:
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    # If already in correct format, return as-is
    if snapshot_id.endswith('Z') and '.000000' in snapshot_id:
        return snapshot_id
    
    try:
        # Parse existing timestamp and convert to new format
        if snapshot_id.endswith('Z'):
            dt = datetime.fromisoformat(snapshot_id.replace('Z', '+00:00'))
        elif '+' in snapshot_id:
            dt = datetime.fromisoformat(snapshot_id)
        else:
            # Assume UTC if no timezone
            dt = datetime.fromisoformat(snapshot_id).replace(tzinfo=timezone.utc)
        
        # Return in microsecond Z format
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
    except Exception:
        # Fallback: use current time if parsing fails
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

def get_migration_config() -> Dict:
    """Get migration configuration"""
    return {
        'project_id': PROJECT_ID,
        'environments': ENVIRONMENTS,
        'migration_snapshot_id': MIGRATION_SNAPSHOT_TIMESTAMP,
        'tables_to_migrate': ['hs_companies', 'hs_deals'],
        'reference_tables': ['hs_owners', 'hs_deal_stage_reference']
    }