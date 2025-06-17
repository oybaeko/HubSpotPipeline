# build-staging/steps/excel_import/schema.py
# EXCEL-SPECIFIC configurations and mappings only
# Imports authoritative schemas from pipeline, adds Excel-specific mappings

"""
Excel-specific configurations and data.
Imports authoritative schemas from src/hubspot_pipeline/schema.py
Contains only Excel-specific mappings and configurations.
"""

# ═══════════════════════════════════════════════════════════════════════════════
# EXCEL-SPECIFIC DATA ONLY (not schemas)
# ═══════════════════════════════════════════════════════════════════════════════

# Excel snapshot configuration - defines which sheets to import
SNAPSHOTS_TO_IMPORT = [
    {"date": "2025-03-21", "company_sheet": "company-2025-03-21", "deal_sheet": "deals-2025-03-21-1"},
    {"date": "2025-03-23", "company_sheet": "company-2025-03-23", "deal_sheet": "deals-2025-03-23"},
    {"date": "2025-04-04", "company_sheet": "company-2025-04-04", "deal_sheet": "deals-2025-04-04"},
    {"date": "2025-04-06", "company_sheet": "company-2025-04-06", "deal_sheet": "deals-2025-04-06"},
    {"date": "2025-04-14", "company_sheet": "company-2025-04-14", "deal_sheet": "deals-2025-04-14"},
    {"date": "2025-04-27", "company_sheet": "company-2025-04-27", "deal_sheet": "deals-2025-04-27"},
    {"date": "2025-05-11", "company_sheet": "company-2025-05-11", "deal_sheet": "deals-2025-05-11"},
    {"date": "2025-05-18", "company_sheet": "company-2025-05-18", "deal_sheet": "deals-2025-05-18"},
    {"date": "2025-05-25", "company_sheet": "company-2025-05-25", "deal_sheet": "deals-2025-05-25"},
    {"date": "2025-06-01", "company_sheet": "company-2025-06-01", "deal_sheet": "deals-2025-06-01"}
]

# Owner name to ID mapping for Excel import (business-specific data)
OWNERS_LOOKUP = {
    # Full names (lowercase for matching)
    "uma baeko": "35975295",
    "øystein baeko": "35975296", 
    "oystein baeko": "35975296",  # Alternative spelling
    "quang nguyen": "577605736",
    "sofie meyer": "612145716", 
    "gjermund moastuen": "677066168",
    "carl petterson": "1596892909",
    
    # First names only (for partial matching)
    "uma": "35975295",
    "øystein": "35975296",
    "oystein": "35975296",
    "quang": "577605736",
    "nguyen": "577605736", 
    "sofie": "612145716",
    "gjermund": "677066168",
    "carl": "1596892909",
}

# Table name mappings (simple reference data)
TABLE_NAMES = {
    "companies": "hs_companies", 
    "deals": "hs_deals",
    "owners": "hs_owners",
}

# ═══════════════════════════════════════════════════════════════════════════════
# EXCEL-SPECIFIC FIELD MAPPINGS
# Maps Excel column names to BigQuery field names (from authoritative schema)
# ═══════════════════════════════════════════════════════════════════════════════

# Excel column names → BigQuery field names for companies
EXCEL_COMPANY_FIELD_MAP = {
    "Record ID": "company_id",
    "Company name": "company_name",
    "Company owner": "hubspot_owner_id", 
    "Lifecycle Stage": "lifecycle_stage",
    "Lead Status": "lead_status",
    "Type": "company_type",
    "Development Category": "development_category",
    "Hiring Developers": "hiring_developers", 
    "Inhouse Developers": "inhouse_developers",
    "Proff Likviditetsgrad": "proff_likviditetsgrad",
    "Proff Link": "proff_link",
    "Proff Lønnsomhet": "proff_lonnsomhet",
    "Proff Soliditet": "proff_soliditet",
}

# Excel column names → BigQuery field names for deals
EXCEL_DEAL_FIELD_MAP = {
    "Record ID": "deal_id",
    "Deal Name": "deal_name",
    "Deal Stage": "deal_stage",
    "Deal Type": "deal_type", 
    "Amount": "amount",
    "Deal owner": "owner_id",
    "Associated Company ID": "associated_company_id",
    # Alternative column names that might appear in Excel
    "Deal name": "deal_name",
    "Dealname": "deal_name",
    "Deal stage": "deal_stage", 
    "Dealstage": "deal_stage",
    "Deal type": "deal_type",
    "Dealtype": "deal_type",
    "Owner": "owner_id",
    "Company ID": "associated_company_id",
}

# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS TO GET AUTHORITATIVE SCHEMAS
# These import from the authoritative source and provide them to excel_import modules
# ═══════════════════════════════════════════════════════════════════════════════

def get_authoritative_schemas():
    """
    Import and return authoritative schemas from pipeline module
    Returns: Dict with 'companies' and 'deals' schemas
    """
    try:
        # Import from authoritative source
        import sys
        from pathlib import Path
        
        # Add src path for schema imports
        src_path = Path(__file__).parent.parent.parent.parent / "src" / "hubspot_pipeline"
        if str(src_path) not in sys.path:
            sys.path.insert(0, str(src_path))
        
        from schema import SCHEMA_COMPANIES, SCHEMA_DEALS
        
        return {
            'companies': SCHEMA_COMPANIES,
            'deals': SCHEMA_DEALS
        }
        
    except ImportError as e:
        raise ImportError(f"Failed to import authoritative schemas: {e}")

def get_table_schemas():
    """
    Get BigQuery table schemas for excel_import (backward compatibility)
    Returns: Dict mapping data_type -> schema_definition
    """
    auth_schemas = get_authoritative_schemas()
    
    return {
        'companies': auth_schemas['companies'],
        'deals': auth_schemas['deals']
    }

# Create TABLE_SCHEMAS for backward compatibility with existing code
TABLE_SCHEMAS = get_table_schemas()