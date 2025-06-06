# src/hubspot_pipeline/hubspot_ingest/events.py

import logging
import os
import json
from datetime import datetime
from typing import Dict, Optional
from google.cloud import bigquery

# Lazy import for Pub/Sub to avoid import errors in local testing
def _get_pubsub_client():
    """Lazy import of Pub/Sub client to handle missing dependency gracefully"""
    try:
        from google.cloud import pubsub_v1
        return pubsub_v1.PublisherClient()
    except ImportError as e:
        logging.error("❌ google-cloud-pubsub not installed. Run: pip install google-cloud-pubsub")
        raise ImportError("Missing dependency: google-cloud-pubsub") from e


def publish_snapshot_completed_event(snapshot_id: str, data_counts: Dict[str, int], 
                                   reference_counts: Dict[str, int]) -> Optional[str]:
    """
    Publish snapshot completion event to Pub/Sub for scoring pipeline.
    
    Args:
        snapshot_id: The snapshot identifier
        data_counts: Dict of table_name -> record_count for snapshot data
        reference_counts: Dict of table_name -> record_count for reference data
        
    Returns:
        Message ID if successful, None if failed, "local_mode" if running locally
    """
    logger = logging.getLogger('hubspot.events')
    
    try:
        client = bigquery.Client()
        project_id = client.project
        
        # Try to get Pub/Sub client
        try:
            publisher = _get_pubsub_client()
        except ImportError:
            logger.warning("⚠️ Pub/Sub not available - running in local mode")
            logger.info("📤 Would have published snapshot.completed event (local mode)")
            return "local_mode_no_pubsub"
        
        topic_path = publisher.topic_path(project_id, "hubspot-events")
        
        # Build event data
        event_data = {
            'snapshot_id': snapshot_id,
            'timestamp': datetime.utcnow().isoformat() + "Z",
            'data_tables': data_counts,
            'reference_tables': reference_counts,
            'metadata': {
                'triggered_by': 'ingest_function',
                'environment': os.getenv('ENVIRONMENT', 'development'),
                'total_data_records': sum(data_counts.values()),
                'total_reference_records': sum(reference_counts.values())
            }
        }
        
        # Build full event envelope
        event = {
            "type": "hubspot.snapshot.completed",
            "version": "1.0",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": "hubspot-ingest",
            "data": event_data
        }
        
        # Publish event
        message_json = json.dumps(event)
        future = publisher.publish(topic_path, message_json.encode('utf-8'))
        message_id = future.result()
        
        logger.info(f"📤 Published snapshot.completed event (message ID: {message_id})")
        logger.debug(f"Event data: snapshot_id={snapshot_id}, tables={list(data_counts.keys())}")
        
        return message_id
        
    except Exception as e:
        # Check if it's a permission error (common in local testing)
        error_str = str(e).lower()
        if "403" in error_str or "not authorized" in error_str or "permission" in error_str:
            logger.warning(f"⚠️ Pub/Sub permission error (expected for local testing): {e}")
            logger.info("📤 Would have published event (no permissions for local testing)")
            return "local_mode_no_permissions"
        else:
            logger.error(f"❌ Failed to publish event: {e}")
            # Don't fail the whole ingest if event publishing fails
            return None


def publish_snapshot_failed_event(snapshot_id: str, error_message: str) -> Optional[str]:
    """
    Publish snapshot failure event to Pub/Sub.
    
    Args:
        snapshot_id: The snapshot identifier
        error_message: Description of the error
        
    Returns:
        Message ID if successful, None if failed, "local_mode" if running locally
    """
    logger = logging.getLogger('hubspot.events')
    
    try:
        client = bigquery.Client()
        project_id = client.project
        
        # Try to get Pub/Sub client
        try:
            publisher = _get_pubsub_client()
        except ImportError:
            logger.warning("⚠️ Pub/Sub not available - running in local mode")
            return "local_mode_no_pubsub"
        
        topic_path = publisher.topic_path(project_id, "hubspot-events")
        
        # Build event data
        event_data = {
            'snapshot_id': snapshot_id,
            'timestamp': datetime.utcnow().isoformat() + "Z",
            'error_message': error_message,
            'metadata': {
                'triggered_by': 'ingest_function',
                'environment': os.getenv('ENVIRONMENT', 'development')
            }
        }
        
        # Build full event envelope
        event = {
            "type": "hubspot.snapshot.failed",
            "version": "1.0",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": "hubspot-ingest",
            "data": event_data
        }
        
        # Publish event
        message_json = json.dumps(event)
        future = publisher.publish(topic_path, message_json.encode('utf-8'))
        message_id = future.result()
        
        logger.info(f"📤 Published snapshot.failed event (message ID: {message_id})")
        
        return message_id
        
    except Exception as e:
        # Handle permission errors gracefully
        error_str = str(e).lower()
        if "403" in error_str or "not authorized" in error_str or "permission" in error_str:
            logger.warning(f"⚠️ Pub/Sub permission error (expected for local testing): {e}")
            return "local_mode_no_permissions"
        else:
            logger.error(f"❌ Failed to publish failure event: {e}")
            return None


def publish_custom_event(event_type: str, event_data: Dict, source: str = "hubspot-ingest") -> Optional[str]:
    """
    Publish a custom event to Pub/Sub.
    
    Args:
        event_type: Type of event (e.g., "hubspot.reference.updated")
        event_data: Event-specific data
        source: Source system identifier
        
    Returns:
        Message ID if successful, None if failed, "local_mode" if running locally
    """
    logger = logging.getLogger('hubspot.events')
    
    try:
        client = bigquery.Client()
        project_id = client.project
        
        # Try to get Pub/Sub client
        try:
            publisher = _get_pubsub_client()
        except ImportError:
            logger.warning("⚠️ Pub/Sub not available - running in local mode")
            return "local_mode_no_pubsub"
        
        topic_path = publisher.topic_path(project_id, "hubspot-events")
        
        # Build full event envelope
        event = {
            "type": event_type,
            "version": "1.0",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": source,
            "data": event_data
        }
        
        # Publish event
        message_json = json.dumps(event)
        future = publisher.publish(topic_path, message_json.encode('utf-8'))
        message_id = future.result()
        
        logger.info(f"📤 Published {event_type} event (message ID: {message_id})")
        
        return message_id
        
    except Exception as e:
        # Handle permission errors gracefully
        error_str = str(e).lower()
        if "403" in error_str or "not authorized" in error_str or "permission" in error_str:
            logger.warning(f"⚠️ Pub/Sub permission error (expected for local testing): {e}")
            return "local_mode_no_permissions"
        else:
            logger.error(f"❌ Failed to publish custom event: {e}")
            return None