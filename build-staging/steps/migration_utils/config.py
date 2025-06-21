#!/usr/bin/env python3
"""
Migration configuration and environment settings
UPDATED: Uses 06:00:00Z canonical time and removes microseconds for consistency
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

# UPDATED: Migration configuration - Uses Sunday 06:00:00Z (no microseconds)
MIGRATION_SNAPSHOT_TIMESTAMP = "2025-06-08T06:00:00Z"

def get_dataset_for_env(env: str) -> str:
    """Get BigQuery dataset for environment"""
    return ENVIRONMENTS.get(env, 'Hubspot_dev_ob')

def normalize_snapshot_id(snapshot_id: str) -> str:
    """Convert snapshot_id to consistent format without microseconds (business identifier only)"""
    if not snapshot_id:
        return datetime.now(timezone.utc).replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # If already in correct format (no microseconds), return as-is
    if snapshot_id.endswith('Z') and '.' not in snapshot_id:
        return snapshot_id
    
    try:
        # Parse existing timestamp and convert to format without microseconds
        if snapshot_id.endswith('Z'):
            if '.' in snapshot_id:
                # Remove microseconds: "2025-03-21T14:32:17.123456Z" → "2025-03-21T14:32:17Z"
                base_part = snapshot_id.split('.')[0]
                return f"{base_part}Z"
            else:
                dt = datetime.fromisoformat(snapshot_id.replace('Z', '+00:00'))
        elif '+' in snapshot_id:
            dt = datetime.fromisoformat(snapshot_id)
        else:
            # For date-only formats like "2025-03-21", use Sunday 06:00 UTC
            if len(snapshot_id) == 10 and '-' in snapshot_id:  # YYYY-MM-DD format
                return f"{snapshot_id}T06:00:00Z"  # Sunday snapshot time
            else:
                # Assume UTC if no timezone
                dt = datetime.fromisoformat(snapshot_id).replace(tzinfo=timezone.utc)
        
        # Return without microseconds
        return dt.replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')
        
    except Exception:
        # Fallback: use current time without microseconds
        return datetime.now(timezone.utc).replace(microsecond=0).strftime('%Y-%m-%dT%H:%M:%SZ')

def get_migration_config() -> Dict:
    """Get migration configuration"""
    return {
        'project_id': PROJECT_ID,
        'environments': ENVIRONMENTS,
        'migration_snapshot_id': MIGRATION_SNAPSHOT_TIMESTAMP,
        'tables_to_migrate': ['hs_companies', 'hs_deals'],
        'reference_tables': ['hs_owners', 'hs_deal_stage_reference'],
        'canonical_time': '06:00:00Z',
        'timestamp_format': '%Y-%m-%dT%H:%M:%SZ'  # No microseconds
    }