# src/hubspot_pipeline/hubspot_scoring/views/manager.py

"""
BigQuery Views Manager for Pipeline Analytics

Handles creation, updating, and management of BigQuery views
for pipeline scoring and analytics.
"""

import logging
import os
from typing import Dict, List, Any
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

from .definitions import get_all_view_definitions

class ViewManager:
    """Manages BigQuery views for pipeline analytics"""
    
    def __init__(self, project_id: str = None, dataset_id: str = None):
        """
        Initialize ViewManager
        
        Args:
            project_id: BigQuery project ID (uses env var if not provided)
            dataset_id: BigQuery dataset ID (uses env var if not provided)
        """
        self.logger = logging.getLogger('hubspot.scoring.views')
        self.client = bigquery.Client()
        
        self.project_id = project_id or os.getenv('BIGQUERY_PROJECT_ID')
        self.dataset_id = dataset_id or os.getenv('BIGQUERY_DATASET_ID')
        
        if not self.project_id or not self.dataset_id:
            raise ValueError("project_id and dataset_id must be provided or set in environment")
        
        self.logger.debug(f"ViewManager initialized for {self.project_id}.{self.dataset_id}")
    
    def create_or_update_view(self, view_name: str, view_definition: Dict[str, Any]) -> bool:
        """
        Create or update a single BigQuery view
        
        Args:
            view_name: Name of the view
            view_definition: View definition containing name, description, and sql
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Format SQL with project and dataset
            formatted_sql = view_definition["sql"].format(
                project=self.project_id,
                dataset=self.dataset_id
            )
            
            view_ref = f"{self.project_id}.{self.dataset_id}.{view_name}"
            
            # Check if view exists
            try:
                existing_view = self.client.get_table(view_ref)
                action = "Updating"
                self.logger.debug(f"View {view_ref} exists, will update")
            except NotFound:
                action = "Creating"
                self.logger.debug(f"View {view_ref} does not exist, will create")
            
            # Create or update the view
            view = bigquery.Table(view_ref)
            view.view_query = formatted_sql
            view.description = view_definition.get("description", f"Pipeline analytics view: {view_name}")
            
            if action == "Creating":
                view = self.client.create_table(view)
                self.logger.info(f"✅ Created view {view_ref}")
            else:
                view = self.client.update_table(view, ["view_query", "description"])
                self.logger.info(f"✅ Updated view {view_ref}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to {action.lower()} view {view_name}: {e}")
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"SQL that failed:\n{formatted_sql}")
            return False
    
    def create_or_update_all_views(self) -> Dict[str, bool]:
        """
        Create or update all pipeline analytics views
        
        Returns:
            Dict[str, bool]: Results for each view (view_name -> success)
        """
        self.logger.info("🔄 Creating/updating all pipeline analytics views")
        
        all_views = get_all_view_definitions()
        results = {}
        
        for view_name, view_def in all_views.items():
            self.logger.info(f"📊 Processing view: {view_name}")
            success = self.create_or_update_view(view_name, view_def)
            results[view_name] = success
        
        # Summary
        successful = sum(1 for success in results.values() if success)
        total = len(results)
        
        if successful == total:
            self.logger.info(f"✅ All {total} views created/updated successfully")
        else:
            failed = total - successful
            self.logger.warning(f"⚠️ {successful}/{total} views successful, {failed} failed")
        
        return results
    
    def delete_view(self, view_name: str) -> bool:
        """
        Delete a BigQuery view
        
        Args:
            view_name: Name of the view to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            view_ref = f"{self.project_id}.{self.dataset_id}.{view_name}"
            self.client.delete_table(view_ref, not_found_ok=True)
            self.logger.info(f"🗑️ Deleted view {view_ref}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to delete view {view_name}: {e}")
            return False
    
    def list_views(self) -> List[str]:
        """
        List all views in the dataset
        
        Returns:
            List[str]: Names of all views in the dataset
        """
        try:
            dataset_ref = f"{self.project_id}.{self.dataset_id}"
            tables = self.client.list_tables(dataset_ref)
            
            views = []
            for table in tables:
                # Check if it's a view
                table_info = self.client.get_table(table)
                if table_info.table_type == "VIEW":
                    views.append(table.table_id)
            
            self.logger.debug(f"Found {len(views)} views in {dataset_ref}")
            return views
            
        except Exception as e:
            self.logger.error(f"❌ Failed to list views: {e}")
            return []
    
    def validate_view_sql(self, view_definition: Dict[str, Any]) -> bool:
        """
        Validate view SQL by doing a dry run
        
        Args:
            view_definition: View definition containing SQL
            
        Returns:
            bool: True if SQL is valid, False otherwise
        """
        try:
            formatted_sql = view_definition["sql"].format(
                project=self.project_id,
                dataset=self.dataset_id
            )
            
            # Create a dry run job to validate SQL
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            
            self.client.query(formatted_sql, job_config=job_config)
            self.logger.debug(f"✅ SQL validation passed for {view_definition.get('name', 'unnamed view')}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ SQL validation failed: {e}")
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.debug(f"Invalid SQL:\n{formatted_sql}")
            return False


# Convenience functions for use in other modules
def refresh_all_views(project_id: str = None, dataset_id: str = None) -> Dict[str, bool]:
    """
    Refresh all pipeline analytics views
    
    Args:
        project_id: BigQuery project ID (uses env var if not provided)
        dataset_id: BigQuery dataset ID (uses env var if not provided)
        
    Returns:
        Dict[str, bool]: Results for each view
    """
    manager = ViewManager(project_id, dataset_id)
    return manager.create_or_update_all_views()


def refresh_view(view_name: str, project_id: str = None, dataset_id: str = None) -> bool:
    """
    Refresh a single pipeline analytics view
    
    Args:
        view_name: Name of the view to refresh
        project_id: BigQuery project ID (uses env var if not provided)
        dataset_id: BigQuery dataset ID (uses env var if not provided)
        
    Returns:
        bool: True if successful
    """
    manager = ViewManager(project_id, dataset_id)
    
    all_views = get_all_view_definitions()
    if view_name not in all_views:
        manager.logger.error(f"❌ Unknown view: {view_name}")
        manager.logger.info(f"Available views: {list(all_views.keys())}")
        return False
    
    return manager.create_or_update_view(view_name, all_views[view_name])


def validate_all_view_sql(project_id: str = None, dataset_id: str = None) -> Dict[str, bool]:
    """
    Validate SQL for all views without creating them
    
    Args:
        project_id: BigQuery project ID (uses env var if not provided)
        dataset_id: BigQuery dataset ID (uses env var if not provided)
        
    Returns:
        Dict[str, bool]: Validation results for each view
    """
    manager = ViewManager(project_id, dataset_id)
    all_views = get_all_view_definitions()
    
    results = {}
    for view_name, view_def in all_views.items():
        results[view_name] = manager.validate_view_sql(view_def)
    
    return results