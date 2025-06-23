# src/hubspot_pipeline/hubspot_ingest/__init__.py

# Ingest pipeline package with data normalization support
from .main import main as ingest_main
from .config_loader import init_env, load_schema, validate_config, get_config
from .fetcher import fetch_object, get_client
from .store import store_to_bigquery, upsert_to_bigquery
from .registry import (
    register_snapshot_start, 
    register_snapshot_ingest_complete, 
    register_snapshot_failure,
    update_snapshot_status,
    get_latest_snapshot
)
from .events import (
    publish_snapshot_completed_event,
    publish_snapshot_failed_event,
    publish_custom_event
)
from .normalization import (
    normalize_field_value,
    normalize_email,
    normalize_enum_field,
    normalize_url,
    validate_normalization,
    get_fields_requiring_normalization
)

__all__ = [
    # Main entry point
    "ingest_main",
    
    # Configuration
    "init_env",
    "load_schema", 
    "validate_config",
    "get_config",
    
    # Data fetching
    "fetch_object",
    "get_client",
    
    # Data storage
    "store_to_bigquery",
    "upsert_to_bigquery",
    
    # Registry management
    "register_snapshot_start",
    "register_snapshot_ingest_complete",
    "register_snapshot_failure",
    "update_snapshot_status",
    "get_latest_snapshot",
    
    # Event publishing
    "publish_snapshot_completed_event",
    "publish_snapshot_failed_event",
    "publish_custom_event",
    
    # Data normalization
    "normalize_field_value",
    "normalize_email",
    "normalize_enum_field",
    "normalize_url",
    "validate_normalization",
    "get_fields_requiring_normalization",
]