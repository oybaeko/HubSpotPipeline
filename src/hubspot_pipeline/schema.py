# ─── Companies ───────────────────────────────────────────────────────────────────
SCHEMA_COMPANIES = [
    ("company_id", "STRING"),
    ("company_name", "STRING"),
    ("lifecycle_stage", "STRING"),
    ("lead_status", "STRING"),
    ("hubspot_owner_id", "STRING"),
    ("company_type", "STRING"),       
    ("snapshot_id", "STRING"),        
    ("timestamp", "TIMESTAMP")
]


HUBSPOT_COMPANY_FIELD_MAP = {
    "company_name": "name",
    "lifecycle_stage": "lifecyclestage",
    "lead_status": "hs_lead_status",
    "hubspot_owner_id": "hubspot_owner_id",
    "company_type": "type"  
}

# ─── Contacts ────────────────────────────────────────────────────────────────────
SCHEMA_CONTACTS = [
    ("contact_id", "STRING"),
    ("email", "STRING"),
    ("first_name", "STRING"),
    ("last_name", "STRING"),
    ("hubspot_owner_id", "STRING"),
    ("phone", "STRING"),
    ("job_title", "STRING"),
    ("lifecycle_stage", "STRING"),
    ("snapshot_id", "STRING"),   # for your snapshot runs
    ("timestamp", "TIMESTAMP"),
]

HUBSPOT_CONTACT_FIELD_MAP = {
    "contact_id":       "id",
    "email":            "email",
    "first_name":       "firstname",
    "last_name":        "lastname",
    "hubspot_owner_id": "hubspot_owner_id",
    "phone":            "phone",
    "job_title":        "jobtitle",
    "lifecycle_stage":  "lifecyclestage",
}

# ─── Owners / Users ──────────────────────────────────────────────────────────────

SCHEMA_OWNERS = [
    ("owner_id", "STRING"),
    ("email", "STRING"),
    ("first_name", "STRING"),
    ("last_name", "STRING"),
    ("user_id", "STRING"),
    ("active", "BOOLEAN"),
    ("timestamp", "TIMESTAMP")
]
HUBSPOT_OWNER_FIELD_MAP = {
    "owner_id": "id",
    "email": "email",
    "first_name": "firstName",
    "last_name": "lastName",
    "user_id": "userId",
    "active": "active"
}

# ─── Deals ───────────────────────────────────────────────────────────────────────

SCHEMA_DEALS = [
    ("deal_id", "STRING"),
    ("deal_name", "STRING"),
    ("deal_stage", "STRING"),
    ("deal_type", "STRING"),
    ("amount", "FLOAT"),
    ("owner_id", "STRING"),
    ("associated_company_id", "STRING"),
    ("timestamp", "TIMESTAMP"),
    ("snapshot_id", "STRING")
]


HUBSPOT_DEAL_FIELD_MAP = {
    "deal_id": "id",
    "deal_name": "dealname",
    "deal_stage": "dealstage",
    "deal_type": "dealtype",
    "amount": "amount",
    "owner_id": "hubspot_owner_id",
    "associated_company_id": "associations"
}

SCHEMA_DEAL_STAGE_REFERENCE = [
    ("pipeline_id", "STRING"),
    ("pipeline_label", "STRING"),
    ("stage_id", "STRING"),
    ("stage_label", "STRING"),
    ("is_closed", "BOOLEAN"),
    ("probability", "FLOAT"),
    ("display_order", "INTEGER")
]

SCHEMA_STAGE_MAPPING = [
    ("lifecycle_stage", "STRING"),
    ("lead_status", "STRING"),
    ("deal_stage", "STRING"),
    ("combined_stage", "STRING"),
    ("stage_level", "INTEGER"),
    ("adjusted_score", "FLOAT")
]

# ─── Pipeline  ───────────────────────────────────────────────────────────────────
SCHEMA_PIPELINE_UNITS_SNAPSHOT = [
    ("snapshot_id", "STRING"),               # ✅ NEW
    ("snapshot_timestamp", "TIMESTAMP"),
    ("company_id", "STRING"),
    ("deal_id", "STRING"),                   # Nullable
    ("owner_id", "STRING"),
    ("lifecycle_stage", "STRING"),
    ("lead_status", "STRING"),
    ("deal_stage", "STRING"),
    ("combined_stage", "STRING"),
    ("stage_level", "INTEGER"),
    ("adjusted_score", "FLOAT"),
    ("stage_source", "STRING")               # "company" or "deal"
]


SCHEMA_SNAPSHOT_REGISTRY = [
    ("snapshot_id", "STRING"),
    ("snapshot_timestamp", "TIMESTAMP"),
    ("triggered_by", "STRING"),
    ("status", "STRING"),
    ("notes", "STRING")
]

SCHEMA_PIPELINE_SCORE_HISTORY = [
    ("snapshot_id", "STRING"),
    ("owner_id", "STRING"),
    ("combined_stage", "STRING"),
    ("num_companies", "INTEGER"),
    ("total_score", "FLOAT"),
    ("snapshot_timestamp", "TIMESTAMP")
]

