#!/usr/bin/env python3
"""
Configuration for integrity testing
"""

# Reference relationships for validation (foreign keys)
REFERENCE_RELATIONSHIPS = {
    'hs_companies': {
        'hubspot_owner_id': ('hs_owners', 'owner_id', 'Company owner relationship')
    },
    'hs_deals': {
        'owner_id': ('hs_owners', 'owner_id', 'Deal owner relationship'),
        'associated_company_id': ('hs_companies', 'company_id', 'Deal company relationship')
    },
    'hs_contacts': {
        'hubspot_owner_id': ('hs_owners', 'owner_id', 'Contact owner relationship')
    },
    'hs_pipeline_units_snapshot': {
        'owner_id': ('hs_owners', 'owner_id', 'Pipeline owner relationship'),
        'company_id': ('hs_companies', 'company_id', 'Pipeline company relationship'),
        'deal_id': ('hs_deals', 'deal_id', 'Pipeline deal relationship')
    }
}

# Required fields that should not be NULL/empty
REQUIRED_FIELDS = {
    'hs_companies': ['company_id', 'company_name'],
    'hs_deals': ['deal_id', 'deal_name'],
    'hs_owners': ['owner_id', 'email'],
    'hs_contacts': ['contact_id', 'email'],
    'hs_snapshot_registry': ['snapshot_id', 'triggered_by', 'status']
}

# Format validation patterns
FORMAT_VALIDATIONS = {
    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'snapshot_id': r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$',  # YYYY-MM-DDTHH:MM:SSZ format
    'phone': r'^[\+]?[1-9][\d\s\-\(\)\.]{7,15}$'  # Basic phone validation
}

# Tables to check record counts for
TABLES_TO_CHECK = [
    'hs_companies', 'hs_deals', 'hs_owners', 'hs_contacts',
    'hs_snapshot_registry', 'hs_pipeline_units_snapshot', 
    'hs_pipeline_score_history', 'hs_deal_stage_reference'
]

# Unique constraints for duplicate checking
UNIQUE_CONSTRAINTS = {
    'hs_companies': ['company_id', 'snapshot_id'],
    'hs_deals': ['deal_id', 'snapshot_id'],
    'hs_owners': ['owner_id'],
    'hs_contacts': ['contact_id', 'snapshot_id'],
    'hs_snapshot_registry': ['snapshot_id', 'triggered_by', 'record_timestamp']
}

# Email validation tables
EMAIL_TABLES = [
    ('hs_owners', 'email'),
    ('hs_contacts', 'email')
]

# Data tables for snapshot distribution analysis
DATA_TABLES = ['hs_companies', 'hs_deals']

# Core tables that must exist
CORE_TABLES = ['hs_companies', 'hs_deals', 'hs_owners']