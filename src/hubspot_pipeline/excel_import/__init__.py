# src/hubspot_pipeline/excel_import/__init__.py
"""
Excel import module for local data migration to BigQuery.
This module is designed for local use only and should not be deployed to GCP.
"""

from .excel_processor import ExcelProcessor, SnapshotProcessor
from .data_mapper import map_excel_to_schema
from .bigquery_loader import load_to_bigquery, load_multiple_snapshots

__all__ = [
    "ExcelProcessor", 
    "SnapshotProcessor",
    "map_excel_to_schema", 
    "load_to_bigquery",
    "load_multiple_snapshots"
]