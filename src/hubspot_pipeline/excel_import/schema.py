# src/hubspot_pipeline/excel_import/schema.py
"""
Schema definitions for Excel import module.
Separate from main schema.py to avoid affecting GCF deployments.
Updated to match the actual sheet names in your cleaned Excel file.
"""

from typing import List, Tuple, Dict

# ─────────────────────────────────────────────────────────────────────────────────
#   Companies Schema for Excel Import (matches main hs_companies)
# ─────────────────────────────────────────────────────────────────────────────────

EXCEL_COMPANIES_SCHEMA: List[Tuple[str, str]] = [
    ("company_id",             "STRING"),
    ("company_name",           "STRING"),
    ("lifecycle_stage",        "STRING"),
    ("lead_status",            "STRING"),
    ("hubspot_owner_id",       "STRING"),
    ("company_type",           "STRING"),
    ("development_category",   "STRING"),
    ("hiring_developers",      "STRING"),
    ("inhouse_developers",     "STRING"),
    ("proff_likviditetsgrad",  "STRING"),
    ("proff_link",             "STRING"),
    ("proff_lonnsomhet",       "STRING"),
    ("proff_soliditet",        "STRING"),
    ("snapshot_id",            "STRING"),
    ("timestamp",              "TIMESTAMP"),
]

# ─────────────────────────────────────────────────────────────────────────────────
#   Deals Schema for Excel Import (matches main hs_deals)
# ─────────────────────────────────────────────────────────────────────────────────

EXCEL_DEALS_SCHEMA: List[Tuple[str, str]] = [
    ("deal_id",               "STRING"),
    ("deal_name",             "STRING"),
    ("deal_stage",            "STRING"),
    ("deal_type",             "STRING"),
    ("amount",                "FLOAT"),
    ("owner_id",              "STRING"),
    ("associated_company_id", "STRING"),
    ("timestamp",             "TIMESTAMP"),
    ("snapshot_id",           "STRING"),
]

# ─────────────────────────────────────────────────────────────────────────────────
#   Owners Schema for Excel Import
# ─────────────────────────────────────────────────────────────────────────────────

EXCEL_OWNERS_SCHEMA: List[Tuple[str, str]] = [
    ("owner_id",      "STRING"),
    ("email",         "STRING"),
    ("first_name",    "STRING"),
    ("last_name",     "STRING"),
    ("user_id",       "STRING"),
    ("active",        "STRING"),
    ("timestamp",     "TIMESTAMP"),
]

# ─────────────────────────────────────────────────────────────────────────────────
#   Excel Column Mappings
# ─────────────────────────────────────────────────────────────────────────────────

EXCEL_COMPANY_FIELD_MAP: Dict[str, str] = {
    "Record ID": "company_id",
    "Company name": "company_name",
    "Company owner": "hubspot_owner_id",
    "Create Date": "timestamp",
    "Type": "company_type",
    "Lifecycle Stage": "lifecycle_stage",
    "Lead Status": "lead_status",
    # Fields not in Excel - will be set to None
    "Associated Deal IDs": None,  # Metadata only
}

EXCEL_DEAL_FIELD_MAP: Dict[str, str] = {
    "Record ID": "deal_id",
    "Deal name": "deal_name",
    "Deal stage": "deal_stage",
    "Deal type": "deal_type",
    "Amount": "amount",
    "Deal owner": "owner_id",
    "Associated company ID": "associated_company_id",
    "Create date": "timestamp",
    # Common variations
    "Dealname": "deal_name",
    "Dealstage": "deal_stage",
    "Dealtype": "deal_type",
    "Company ID": "associated_company_id",
}

EXCEL_OWNERS_FIELD_MAP: Dict[str, str] = {
    "owner_id": "owner_id",
    "email": "email", 
    "first_name": "first_name",
    "last_name": "last_name",
    "user_id": "user_id",
    "active": "active",
    "timestamp": "timestamp",
}

# ─────────────────────────────────────────────────────────────────────────────────
#   Snapshot Configuration - Updated with your actual sheet names
# ─────────────────────────────────────────────────────────────────────────────────

SNAPSHOTS_TO_IMPORT = [
    {
        "date": "2025-03-21",
        "company_sheet": "company-2025-03-21",
        "deal_sheet": "deals-2025-03-21-1"  # Note: this one has "-1" suffix
    },
    {
        "date": "2025-03-23", 
        "company_sheet": "company-2025-03-23",
        "deal_sheet": "deals-2025-03-23"
    },
    {
        "date": "2025-04-04",
        "company_sheet": "company-2025-04-04", 
        "deal_sheet": "deals-2025-04-04"
    },
    {
        "date": "2025-04-06",
        "company_sheet": "company-2025-04-06",
        "deal_sheet": "deals-2025-04-06"
    },
    {
        "date": "2025-04-14",
        "company_sheet": "company-2025-04-14",
        "deal_sheet": "deals-2025-04-14"
    },
    {
        "date": "2025-04-27",
        "company_sheet": "company-2025-04-27",
        "deal_sheet": "deals-2025-04-27"
    },
    {
        "date": "2025-05-11",
        "company_sheet": "company-2025-05-11",
        "deal_sheet": "deals-2025-05-11"
    },
    {
        "date": "2025-05-18",
        "company_sheet": "company-2025-05-18",
        "deal_sheet": "deals-2025-05-18"
    },
    {
        "date": "2025-05-25",
        "company_sheet": "company-2025-05-25",
        "deal_sheet": "deals-2025-05-25"
    },
    {
        "date": "2025-06-01",
        "company_sheet": "company-2025-06-01",
        "deal_sheet": "deals-2025-06-01"
    }
]

# Note: I didn't include these because they don't appear in your original list:
# - 2025-03-12: company-2025-03-12, deals-2025-03-12  
# - 2025-03-30: company-2025-03-30, deals-2025-03-30

# ─────────────────────────────────────────────────────────────────────────────────
#   Owners Lookup Data (based on your table)
# ─────────────────────────────────────────────────────────────────────────────────

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
    
    # Common variations seen in your data
    "carl petterson": "1596892909",
    "gjermund moastuen": "677066168",
}

# ─────────────────────────────────────────────────────────────────────────────────
#   Table Mappings
# ─────────────────────────────────────────────────────────────────────────────────

TABLE_SCHEMAS = {
    "companies": EXCEL_COMPANIES_SCHEMA,
    "deals": EXCEL_DEALS_SCHEMA,
    "owners": EXCEL_OWNERS_SCHEMA,
}

TABLE_NAMES = {
    "companies": "hs_companies", 
    "deals": "hs_deals",
    "owners": "hs_owners",
}