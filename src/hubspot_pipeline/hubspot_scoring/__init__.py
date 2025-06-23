# src/hubspot_pipeline/hubspot_scoring/__init__.py

"""
Scoring pipeline for HubSpot data with data normalization validation.

This package handles the scoring of snapshot data:
- Stage mapping configuration with normalized lowercase values
- Pipeline unit scoring with normalization validation
- Score history aggregation
- Registry tracking
- Data integrity checks

Independent from ingest pipeline - can be deployed as separate Cloud Function.
Validates that input data from ingest pipeline is properly normalized.
"""

from .main import process_snapshot_event
from .processor import process_snapshot, validate_data_normalization, debug_snapshot_data
from .stage_mapping import (
    populate_stage_mapping, 
    validate_stage_mapping_normalization,
    get_stage_mapping_data
)
from .registry import register_scoring_start, register_scoring_completion, register_scoring_failure
from .config import init_env, get_config, validate_config

__all__ = [
    # Main entry points
    "process_snapshot_event",
    "process_snapshot", 
    
    # Stage mapping with normalization
    "populate_stage_mapping",
    "validate_stage_mapping_normalization",
    "get_stage_mapping_data",
    
    # Registry management
    "register_scoring_start",
    "register_scoring_completion", 
    "register_scoring_failure",
    
    # Configuration
    "init_env",
    "get_config",
    "validate_config",
    
    # Data validation and debugging
    "validate_data_normalization",
    "debug_snapshot_data",
]