# src/hubspot_pipeline/schema.py

from typing import List, Tuple, Dict, Any


# ─────────────────────────────────────────────────────────────────────────────────
#   Companies Schema & Field Map
# ─────────────────────────────────────────────────────────────────────────────────

SCHEMA_COMPANIES: List[Tuple[str, str]] = [
    ("company_id",             "STRING"),
    ("company_name",           "STRING"),
    ("lifecycle_stage",        "STRING"),
    ("lead_status",            "STRING"),
    ("hubspot_owner_id",       "STRING"),
    ("company_type",           "STRING"),

    # ─── Development / Developer‐Count Fields ───────────────────────────────
    ("development_category",   "STRING"),   # Enum: e.g. “Full‐stack”, “Frontend”, etc.
    ("hiring_developers",      "STRING"),   # Enum: “Yes” / “No” / “Maybe”
    ("inhouse_developers",     "STRING"),   # Enum: “None”/“1 or more”/“2 or more”/“4 or more”/“10 or more”
    # ────────────────────────────────────────────────────────────────────────

    # ─── Proff‐Related Fields ───────────────────────────────────────────────
    ("proff_likviditetsgrad",  "STRING"),   # Enum of liquidity ratios from Proff
    ("proff_link",             "STRING"),   # URL to company in Proff
    ("proff_lonnsomhet",       "STRING"),   # Enum of profitability levels
    ("proff_soliditet",        "STRING"),   # Enum of solidity levels
    # ────────────────────────────────────────────────────────────────────────

    ("snapshot_id",            "STRING"),
    ("timestamp",              "TIMESTAMP"),
]

HUBSPOT_COMPANY_FIELD_MAP: Dict[str, str] = {
    "company_id":             "id",
    "company_name":           "name",
    "lifecycle_stage":        "lifecyclestage",
    "lead_status":            "hs_lead_status",
    "hubspot_owner_id":       "hubspot_owner_id",
    "company_type":           "type",

    # ─── Development / Developer‐Count Mappings ───────────────────────────
    "development_category":   "development_category",
    "hiring_developers":      "hiring_developers",
    "inhouse_developers":     "inhouse_developers",
    # ────────────────────────────────────────────────────────────────────────

    # ─── Proff‐Related Mappings ───────────────────────────────────────────
    "proff_likviditetsgrad":  "proff_likviditetsgrad",
    "proff_link":             "proff_link",
    "proff_lonnsomhet":       "proff_lonnsomhet",
    "proff_soliditet":        "proff_soliditet",
    # ────────────────────────────────────────────────────────────────────────
}


# ─────────────────────────────────────────────────────────────────────────────────
#   Contacts Schema & Field Map
# ─────────────────────────────────────────────────────────────────────────────────

SCHEMA_CONTACTS: List[Tuple[str, str]] = [
    ("contact_id",         "STRING"),
    ("email",              "STRING"),
    ("first_name",         "STRING"),
    ("last_name",          "STRING"),
    ("hubspot_owner_id",   "STRING"),
    ("phone",              "STRING"),
    ("job_title",          "STRING"),
    ("lifecycle_stage",    "STRING"),
    ("snapshot_id",        "STRING"),  
    ("timestamp",          "TIMESTAMP"),
]

HUBSPOT_CONTACT_FIELD_MAP: Dict[str, str] = {
    "contact_id":          "id",
    "email":               "email",
    "first_name":          "firstname",
    "last_name":           "lastname",
    "hubspot_owner_id":    "hubspot_owner_id",
    "phone":               "phone",
    "job_title":           "jobtitle",
    "lifecycle_stage":     "lifecyclestage",
}


# ─────────────────────────────────────────────────────────────────────────────────
#   Owners Schema & Field Map
# ─────────────────────────────────────────────────────────────────────────────────

SCHEMA_OWNERS: List[Tuple[str, str]] = [
    ("owner_id",   "STRING"),
    ("email",      "STRING"),
    ("first_name", "STRING"),
    ("last_name",  "STRING"),
    ("user_id",    "STRING"),
    ("active",     "BOOLEAN"),
    ("timestamp",  "TIMESTAMP"),
]

HUBSPOT_OWNER_FIELD_MAP: Dict[str, str] = {
    "owner_id":   "id",
    "email":      "email",
    "first_name": "firstName",
    "last_name":  "lastName",
    "user_id":    "userId",
    "active":     "active",
}


# ─────────────────────────────────────────────────────────────────────────────────
#   Deals Schema & Field Map
# ─────────────────────────────────────────────────────────────────────────────────

SCHEMA_DEALS: List[Tuple[str, str]] = [
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

HUBSPOT_DEAL_FIELD_MAP: Dict[str, str] = {
    "deal_id":                 "id",
    "deal_name":               "dealname",
    "deal_stage":              "dealstage",
    "deal_type":               "dealtype",
    "amount":                  "amount",
    "owner_id":                "hubspot_owner_id",
    "associated_company_id":   "associations",
}


# ─────────────────────────────────────────────────────────────────────────────────
#   Deal Stage Reference Schema
# ─────────────────────────────────────────────────────────────────────────────────

SCHEMA_DEAL_STAGE_REFERENCE: List[Tuple[str, str]] = [
    ("pipeline_id",    "STRING"),
    ("pipeline_label", "STRING"),
    ("stage_id",       "STRING"),
    ("stage_label",    "STRING"),
    ("is_closed",      "BOOLEAN"),
    ("probability",    "FLOAT"),
    ("display_order",  "INTEGER"),
]


# ─────────────────────────────────────────────────────────────────────────────────
#   Stage Mapping Schema
# ─────────────────────────────────────────────────────────────────────────────────

SCHEMA_STAGE_MAPPING: List[Tuple[str, str]] = [
    ("lifecycle_stage", "STRING"),
    ("lead_status",     "STRING"),
    ("deal_stage",      "STRING"),
    ("combined_stage",  "STRING"),
    ("stage_level",     "INTEGER"),
    ("adjusted_score",  "FLOAT"),
]


# ─────────────────────────────────────────────────────────────────────────────────
#   Pipeline Units Snapshot Schema
# ─────────────────────────────────────────────────────────────────────────────────

SCHEMA_PIPELINE_UNITS_SNAPSHOT: List[Tuple[str, str]] = [
    ("snapshot_id",        "STRING"),
    ("snapshot_timestamp", "TIMESTAMP"),
    ("company_id",         "STRING"),
    ("deal_id",            "STRING"),   # Nullable
    ("owner_id",           "STRING"),
    ("lifecycle_stage",    "STRING"),
    ("lead_status",        "STRING"),
    ("deal_stage",         "STRING"),
    ("combined_stage",     "STRING"),
    ("stage_level",        "INTEGER"),
    ("adjusted_score",     "FLOAT"),
    ("stage_source",       "STRING"),   # “company” or “deal”
]


# ─────────────────────────────────────────────────────────────────────────────────
#   Snapshot Registry Schema
# ─────────────────────────────────────────────────────────────────────────────────

SCHEMA_SNAPSHOT_REGISTRY: List[Tuple[str, str]] = [
    ("snapshot_id",        "STRING"),
    ("snapshot_timestamp", "TIMESTAMP"),
    ("triggered_by",       "STRING"),
    ("status",             "STRING"),
    ("notes",              "STRING"),
]


# ─────────────────────────────────────────────────────────────────────────────────
#   Pipeline Score History Schema
# ─────────────────────────────────────────────────────────────────────────────────

SCHEMA_PIPELINE_SCORE_HISTORY: List[Tuple[str, str]] = [
    ("snapshot_id",        "STRING"),
    ("owner_id",           "STRING"),
    ("combined_stage",     "STRING"),
    ("num_companies",      "INTEGER"),
    ("total_score",        "FLOAT"),
    ("snapshot_timestamp", "TIMESTAMP"),
]


# ─────────────────────────────────────────────────────────────────────────────────
#   Validation Utility: Ensure Schema & FieldMap Stay in Sync
# ─────────────────────────────────────────────────────────────────────────────────

def _validate_field_map_consistency(
    schema: List[Tuple[str, str]],
    field_map: Dict[str, str],
    schema_name: str,
    fieldmap_name: str
) -> None:
    """
    Validates that every non‐system column in `schema` appears as a key in `field_map`,
    and every key in `field_map` appears in `schema`. If any mismatch, raises ValueError.

    - `schema`: list of (column_name, type) tuples.
    - `field_map`: dict mapping schema column_name -> HubSpot propertyApiName.
    - `schema_name`/`fieldmap_name`: used in error messages to identify which mismatch.
    """

    # Extract column names from the schema
    schema_cols = [col_name for (col_name, _) in schema]

    # Exempt any system columns that do not require mapping
    exempt_columns = {"snapshot_id", "timestamp"}
    required_cols = [c for c in schema_cols if c not in exempt_columns]

    # 1) Check that each required schema column is a key in field_map
    missing_in_map = [c for c in required_cols if c not in field_map]
    if missing_in_map:
        raise ValueError(
            f"Schema‐to‐FieldMap mismatch in `{schema_name}` vs `{fieldmap_name}`:\n"
            f"  Columns in schema but missing from field_map: {missing_in_map}"
        )

    # 2) Check that each key in field_map actually appears in the schema
    extra_in_map = [k for k in field_map.keys() if k not in schema_cols]
    if extra_in_map:
        raise ValueError(
            f"Schema‐to‐FieldMap mismatch in `{schema_name}` vs `{fieldmap_name}`:\n"
            f"  Keys in field_map but missing from schema: {extra_in_map}"
        )


# ─────────────────────────────────────────────────────────────────────────────────
#   Run Validations at Import Time
# ─────────────────────────────────────────────────────────────────────────────────

# Validate Companies mapping
_validate_field_map_consistency(
    schema=SCHEMA_COMPANIES,
    field_map=HUBSPOT_COMPANY_FIELD_MAP,
    schema_name="SCHEMA_COMPANIES",
    fieldmap_name="HUBSPOT_COMPANY_FIELD_MAP"
)

# Validate Contacts mapping
_validate_field_map_consistency(
    schema=SCHEMA_CONTACTS,
    field_map=HUBSPOT_CONTACT_FIELD_MAP,
    schema_name="SCHEMA_CONTACTS",
    fieldmap_name="HUBSPOT_CONTACT_FIELD_MAP"
)

# Validate Owners mapping
_validate_field_map_consistency(
    schema=SCHEMA_OWNERS,
    field_map=HUBSPOT_OWNER_FIELD_MAP,
    schema_name="SCHEMA_OWNERS",
    fieldmap_name="HUBSPOT_OWNER_FIELD_MAP"
)

# Validate Deals mapping
_validate_field_map_consistency(
    schema=SCHEMA_DEALS,
    field_map=HUBSPOT_DEAL_FIELD_MAP,
    schema_name="SCHEMA_DEALS",
    fieldmap_name="HUBSPOT_DEAL_FIELD_MAP"
)
