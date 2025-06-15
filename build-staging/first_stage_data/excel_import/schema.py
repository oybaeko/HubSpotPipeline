# File: build-staging/first_stage_data/excel_import/schema.py
# UPDATED VERSION - Fix timestamp field name to match pipeline schema

from typing import List, Tuple, Dict

# ─────────────────────────────────────────────────────────────────────────────────
#   Companies Schema for Excel Import - FIXED to use record_timestamp
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
    ("record_timestamp",       "TIMESTAMP"),  # ✅ FIXED: Changed from "timestamp"
]

# ─────────────────────────────────────────────────────────────────────────────────
#   Deals Schema for Excel Import - FIXED to use record_timestamp
# ─────────────────────────────────────────────────────────────────────────────────

EXCEL_DEALS_SCHEMA: List[Tuple[str, str]] = [
    ("deal_id",               "STRING"),
    ("deal_name",             "STRING"),
    ("deal_stage",            "STRING"),
    ("deal_type",             "STRING"),
    ("amount",                "FLOAT"),
    ("owner_id",              "STRING"),
    ("associated_company_id", "STRING"),
    ("snapshot_id",           "STRING"),
    ("record_timestamp",      "TIMESTAMP"),  # ✅ FIXED: Changed from "timestamp"
]

# ─────────────────────────────────────────────────────────────────────────────────
#   Owners Schema for Excel Import - FIXED to use record_timestamp
# ─────────────────────────────────────────────────────────────────────────────────

EXCEL_OWNERS_SCHEMA: List[Tuple[str, str]] = [
    ("owner_id",      "STRING"),
    ("email",         "STRING"),
    ("first_name",    "STRING"),
    ("last_name",     "STRING"),
    ("user_id",       "STRING"),
    ("active",        "STRING"),
    ("record_timestamp", "TIMESTAMP"),  # ✅ FIXED: Changed from "timestamp"
]

# ─────────────────────────────────────────────────────────────────────────────────
#   Excel Column Mappings - No changes needed here
# ─────────────────────────────────────────────────────────────────────────────────

EXCEL_COMPANY_FIELD_MAP: Dict[str, str] = {
    "Record ID": "company_id",
    "Company name": "company_name",
    "Company owner": "hubspot_owner_id",
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
}

# ─────────────────────────────────────────────────────────────────────────────────
#   Snapshot Configuration - No changes needed
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

# ─────────────────────────────────────────────────────────────────────────────────
#   Owners Lookup Data - No changes needed
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
#   Table Mappings - No changes needed
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