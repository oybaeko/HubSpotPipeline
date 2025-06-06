# src/hubspot_pipeline/hubspot_ingest/reference/schemas.py

from typing import List, Tuple

# ─────────────────────────────────────────────────────────────────────────────────
#   Owners Schema
# ─────────────────────────────────────────────────────────────────────────────────

OWNERS_SCHEMA: List[Tuple[str, str]] = [
    ("owner_id",   "STRING"),
    ("email",      "STRING"),
    ("first_name", "STRING"),
    ("last_name",  "STRING"),
    ("user_id",    "STRING"),
    ("active",     "BOOLEAN"),
    ("timestamp",  "TIMESTAMP"),
]

# ─────────────────────────────────────────────────────────────────────────────────
#   Deal Stages Schema
# ─────────────────────────────────────────────────────────────────────────────────

DEAL_STAGES_SCHEMA: List[Tuple[str, str]] = [
    ("pipeline_id",    "STRING"),
    ("pipeline_label", "STRING"),
    ("stage_id",       "STRING"),
    ("stage_label",    "STRING"),
    ("is_closed",      "BOOLEAN"),
    ("probability",    "FLOAT"),
    ("display_order",  "INTEGER"),
]

# ─────────────────────────────────────────────────────────────────────────────────
#   Snapshot Registry Schema
# ─────────────────────────────────────────────────────────────────────────────────

SNAPSHOT_REGISTRY_SCHEMA: List[Tuple[str, str]] = [
    ("snapshot_id",        "STRING"),
    ("snapshot_timestamp", "TIMESTAMP"),
    ("triggered_by",       "STRING"),
    ("status",             "STRING"),
    ("notes",              "STRING"),
]