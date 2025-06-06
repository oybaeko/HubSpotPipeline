# src/hubspot_pipeline/scoring/__init__.py

"""
Scoring pipeline for HubSpot data.

This package handles the scoring of snapshot data:
- Stage mapping configuration
- Pipeline unit scoring
- Score history aggregation
- Registry tracking

Independent from ingest pipeline - can be deployed as separate Cloud Function.
"""

from .main import process_snapshot_event
from .processor import process_snapshot
from .stage_mapping import populate_stage_mapping
from .registry import register_scoring_start, register_scoring_completion, register_scoring_failure
from .config import init_env, get_config, validate_config

__all__ = [
    "process_snapshot_event",
    "process_snapshot", 
    "populate_stage_mapping",
    "register_scoring_start",
    "register_scoring_completion", 
    "register_scoring_failure",
    "init_env",
    "get_config",
    "validate_config",
]