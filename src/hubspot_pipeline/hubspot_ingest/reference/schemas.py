# src/hubspot_pipeline/hubspot_ingest/reference/schemas.py

from typing import List, Tuple

# ─────────────────────────────────────────────────────────────────────────────────
#   Owners Schema - Updated to use record_timestamp
# ─────────────────────────────────────────────────────────────────────────────────

OWNERS_SCHEMA: List[Tuple[str, str]] = [
    ("owner_id",           "STRING"),
    ("email",              "STRING"),
    ("first_name",         "STRING"),
    ("last_name",          "STRING"),
    ("user_id",            "STRING"),
    ("active",             "BOOLEAN"),
    ("record_timestamp",   "TIMESTAMP"),
]

# ─────────────────────────────────────────────────────────────────────────────────
#   Deal Stages Schema - No timestamp field needed
# ─────────────────────────────────────────────────────────────────────────────────

DEAL_STAGES_SCHEMA: List[Tuple[str, str]] = [
    ("pipeline_id",      "STRING"),
    ("pipeline_label",   "STRING"),
    ("stage_id",         "STRING"),
    ("stage_label",      "STRING"),
    ("is_closed",        "BOOLEAN"),
    ("probability",      "FLOAT"),
    ("display_order",    "INTEGER"),
    ("record_timestamp", "TIMESTAMP"),
]

# ─────────────────────────────────────────────────────────────────────────────────
#   Snapshot Registry Schema - Updated to use record_timestamp
# ─────────────────────────────────────────────────────────────────────────────────

SNAPSHOT_REGISTRY_SCHEMA: List[Tuple[str, str]] = [
    ("triggered_by",       "STRING"),
    ("status",             "STRING"),
    ("notes",              "STRING"),
    ("snapshot_id",        "STRING"),
    ("record_timestamp",   "TIMESTAMP"),
]