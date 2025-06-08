# ===============================================================================
# src/tests/fixtures/test_session.py
# Test session management for cleanup and isolation  
# ===============================================================================

import logging
import os
from datetime import datetime
from typing import List, Dict, Any, Optional

class TestSession:
    """
    Manages test session lifecycle, resource tracking, and cleanup.
    Ensures test isolation and proper cleanup even when tests fail.
    """
    
    def __init__(self, environment: str = 'development'):
        self.session_id = f"pytest_{int(datetime.utcnow().timestamp())}"
        self.environment = environment
        self.cleanup_registry: List[Dict[str, Any]] = []
        self.start_time = datetime.utcnow()
        self.logger = logging.getLogger('hubspot.test.session')
        
        self.logger.info(f"Test session started: {self.session_id} (env: {environment})")
        
    def register_for_cleanup(self, resource_type: str, resource_info: Dict[str, Any]) -> None:
        """
        Register a resource for cleanup at session end.
        
        Args:
            resource_type: Type of resource ('bigquery_table', 'pubsub_message', etc.)
            resource_info: Information needed to clean up the resource
        """
        cleanup_entry = {
            'type': resource_type,
            'info': resource_info,
            'created_at': datetime.utcnow(),
            'session_id': self.session_id
        }
        
        self.cleanup_registry.append(cleanup_entry)
        self.logger.debug(f"Registered for cleanup: {resource_type} - {resource_info}")
        
    def cleanup_all(self) -> Dict[str, Any]:
        """
        Clean up all registered resources.
        
        Returns:
            Dictionary with cleanup results
        """
        if not self.cleanup_registry:
            return {'cleaned': 0, 'failed': 0, 'errors': []}
            
        self.logger.info(f"Starting cleanup of {len(self.cleanup_registry)} resources")
        
        cleanup_results = {
            'cleaned': 0, 
            'failed': 0, 
            'errors': [],
            'session_id': self.session_id
        }
        
        # Clean up in reverse order (LIFO - Last In, First Out)
        for resource in reversed(self.cleanup_registry):
            try:
                self._cleanup_resource(resource)
                cleanup_results['cleaned'] += 1
                self.logger.debug(f"Cleaned up: {resource['type']}")
                
            except Exception as e:
                cleanup_results['failed'] += 1
                error_msg = f"{resource['type']}: {str(e)}"
                cleanup_results['errors'].append(error_msg)
                self.logger.error(f"Cleanup failed: {error_msg}")
        
        total_time = (datetime.utcnow() - self.start_time).total_seconds()
        self.logger.info(f"Session cleanup completed in {total_time:.2f}s: "
                        f"{cleanup_results['cleaned']} cleaned, {cleanup_results['failed']} failed")
        
        return cleanup_results
        
    def _cleanup_resource(self, resource: Dict[str, Any]) -> None:
        """Clean up a specific resource based on its type"""
        
        resource_type = resource['type']
        resource_info = resource['info']
        
        if resource_type == 'bigquery_table':
            self._cleanup_bigquery_table(resource_info)
        elif resource_type == 'pubsub_message':
            self._cleanup_pubsub_message(resource_info)
        elif resource_type == 'test_data':
            self._cleanup_test_data(resource_info)
        else:
            raise ValueError(f"Unknown resource type for cleanup: {resource_type}")
            
    def _cleanup_bigquery_table(self, table_info: Dict[str, Any]) -> None:
        """Clean up a BigQuery table"""
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound
        
        client = bigquery.Client()
        table_id = table_info.get('table_id') or table_info.get('full_table_id')
        
        if not table_id:
            raise ValueError("No table_id provided for BigQuery table cleanup")
        
        try:
            client.delete_table(table_id, not_found_ok=True)
            self.logger.debug(f"Deleted BigQuery table: {table_id}")
        except Exception as e:
            self.logger.warning(f"Failed to delete BigQuery table {table_id}: {e}")
            # Don't raise - just log the warning
            
    def _cleanup_pubsub_message(self, message_info: Dict[str, Any]) -> None:
        """Clean up Pub/Sub messages (acknowledge if needed)"""
        # For now, Pub/Sub messages are transient, so no cleanup needed
        # In the future, we might track subscription acknowledgments here
        pass
        
    def _cleanup_test_data(self, data_info: Dict[str, Any]) -> None:
        """Clean up test data from tables"""
        from google.cloud import bigquery
        
        table_id = data_info.get('table_id')
        test_filter = data_info.get('filter')  # e.g., "snapshot_id = 'test_123'"
        
        if not table_id or not test_filter:
            raise ValueError("Both table_id and filter required for test data cleanup")
        
        client = bigquery.Client()
        delete_query = f"DELETE FROM `{table_id}` WHERE {test_filter}"
        
        job = client.query(delete_query)
        job.result()  # Wait for completion
        
        self.logger.debug(f"Deleted test data from {table_id} with filter: {test_filter}")