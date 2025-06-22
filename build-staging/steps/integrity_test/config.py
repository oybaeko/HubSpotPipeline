# File: build-staging/steps/integrity_test/config.py
# Configuration for integrity testing - UPDATED with lowercase normalization (removed hs_contacts)

# Reference relationships for validation (foreign keys) - UPDATED: removed hs_contacts
REFERENCE_RELATIONSHIPS = {
    'hs_companies': {
        'hubspot_owner_id': ('hs_owners', 'owner_id', 'Company owner relationship')
    },
    'hs_deals': {
        'owner_id': ('hs_owners', 'owner_id', 'Deal owner relationship'),
        'associated_company_id': ('hs_companies', 'company_id', 'Deal company relationship')
    },
    'hs_pipeline_units_snapshot': {
        'owner_id': ('hs_owners', 'owner_id', 'Pipeline owner relationship'),
        'company_id': ('hs_companies', 'company_id', 'Pipeline company relationship'),
        'deal_id': ('hs_deals', 'deal_id', 'Pipeline deal relationship')
    }
}

# Required fields that should not be NULL/empty - UPDATED: removed hs_contacts
REQUIRED_FIELDS = {
    'hs_companies': ['company_id', 'company_name'],
    'hs_deals': ['deal_id', 'deal_name'],
    'hs_owners': ['owner_id', 'email'],
    'hs_snapshot_registry': ['snapshot_id', 'triggered_by', 'status']
}

# Format validation patterns
FORMAT_VALIDATIONS = {
    'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
    'snapshot_id': r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$',  # YYYY-MM-DDTHH:MM:SSZ format
    'phone': r'^[\+]?[1-9][\d\s\-\(\)\.]{7,15}$'  # Basic phone validation
}

# Tables to check record counts for - UPDATED: removed hs_contacts
TABLES_TO_CHECK = [
    'hs_companies', 'hs_deals', 'hs_owners',
    'hs_snapshot_registry', 'hs_pipeline_units_snapshot', 
    'hs_pipeline_score_history', 'hs_deal_stage_reference'
]

# Unique constraints for duplicate checking - UPDATED: removed hs_contacts
UNIQUE_CONSTRAINTS = {
    'hs_companies': ['company_id', 'snapshot_id'],
    'hs_deals': ['deal_id', 'snapshot_id'],
    'hs_owners': ['owner_id'],
    'hs_snapshot_registry': ['snapshot_id', 'triggered_by', 'record_timestamp']
}

# Email validation tables - UPDATED: removed hs_contacts
EMAIL_TABLES = [
    ('hs_owners', 'email')
]

# Data tables for snapshot distribution analysis
DATA_TABLES = ['hs_companies', 'hs_deals']

# Core tables that must exist
CORE_TABLES = ['hs_companies', 'hs_deals', 'hs_owners']

# UPDATED: Lowercase normalization fields - REMOVED hs_contacts references
LOWERCASE_NORMALIZATION_FIELDS = {
    'hs_owners': {
        'email': 'Email addresses should be lowercase for consistent filtering'
    },
    'hs_companies': {
        'lifecycle_stage': 'Lifecycle stage should be lowercase for consistent enum handling',
        'lead_status': 'Lead status should be lowercase for consistent enum handling',
        'company_type': 'Company type should be lowercase for consistent enum handling',
        'development_category': 'Development category should be lowercase for consistent enum handling',
        'hiring_developers': 'Hiring developers status should be lowercase for consistent enum handling',
        'inhouse_developers': 'Inhouse developers count should be lowercase for consistent enum handling',
        'proff_likviditetsgrad': 'Proff liquidity grade should be lowercase for consistent enum handling',
        'proff_lonnsomhet': 'Proff profitability should be lowercase for consistent enum handling',
        'proff_soliditet': 'Proff solidity should be lowercase for consistent enum handling',
        'proff_link': 'URLs should have lowercase domains for consistent matching'
    },
    'hs_deals': {
        'deal_stage': 'Deal stage should be lowercase for consistent enum handling',
        'deal_type': 'Deal type should be lowercase for consistent enum handling'
    }
}