# src/hubspot_pipeline/hubspot_scoring/views/__init__.py

"""
BigQuery Views for Pipeline Analytics and Reporting

This module creates and manages BigQuery views for:
- Current pipeline scoring by sales rep
- Period-over-period pipeline comparison  
- Pipeline change tracking (new/deleted/changed companies)
- Historical pipeline trends by snapshot

Views are updated after scoring completion and can be refreshed on-demand.
"""

from .manager import ViewManager, refresh_all_views, refresh_view
from .definitions import (
    VIEW_CURRENT_PIPELINE_BY_OWNER,
    VIEW_PIPELINE_COMPARISON, 
    VIEW_PIPELINE_CHANGES,
    VIEW_PIPELINE_HISTORY_BY_SNAPSHOT,
    get_all_view_definitions
)

__all__ = [
    "ViewManager",
    "refresh_all_views",
    "refresh_view",
    "VIEW_CURRENT_PIPELINE_BY_OWNER",
    "VIEW_PIPELINE_COMPARISON",
    "VIEW_PIPELINE_CHANGES", 
    "VIEW_PIPELINE_HISTORY_BY_SNAPSHOT",
    "get_all_view_definitions",
]