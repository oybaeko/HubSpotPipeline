# build-staging/steps/excel_import/schema.py
# MINIMAL FILE: Only Excel-specific data, no schema bridging

"""
Excel-specific configurations and data.
Does NOT re-export schemas - modules should import directly from authoritative source.
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
    {"date": "2025-04-27", "company_sheet": "company-2025-27", "deal_sheet": "deals-2025-04-27"},
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