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

def get_environment():
    """Get current environment from various sources"""
    # Check Cloud Function/Cloud Run environment variables
    function_name = os.getenv('K_SERVICE', '')  # Cloud Run service name
    if 'prod' in function_name:
        return 'production'
    elif 'staging' in function_name:
        return 'staging'
    elif 'dev' in function_name:
        return 'development'
    
    # Check explicit environment variable
    env = os.getenv('ENVIRONMENT', '').lower()
    if env in ['production', 'prod']:
        return 'production'
    elif env in ['staging', 'stage']:
        return 'staging'
    elif env in ['development', 'dev']:
        return 'development'
    
    # Default to development
    return 'development'

def get_pubsub_topic_name():
    """Get environment-specific topic name"""
    env = get_environment()
    
    topic_mapping = {
        'development': 'hubspot-events-dev',
        'staging': 'hubspot-events-staging',
        'production': 'hubspot-events-prod'
    }
    
    topic_name = topic_mapping.get(env, 'hubspot-events-dev')
    
    logger = logging.getLogger('hubspot.events')
    logger.debug(f"Environment: {env} -> Topic: {topic_name}")
    
    return topic_name

def is_running_in_gcp():
    """Enhanced detection for Google Cloud environment including Cloud Functions"""
    logger = logging.getLogger('hubspot.events')
    
    # Check for Cloud Function specific environment variables
    if os.getenv('K_SERVICE'):  # Cloud Run/Cloud Functions 2nd gen
        logger.debug("Detected Cloud Functions 2nd gen environment (K_SERVICE)")
        return True
    
    if os.getenv('FUNCTION_NAME'):  # Cloud Functions 1st gen
        logger.debug("Detected Cloud Functions 1st gen environment (FUNCTION_NAME)")
        return True
    
    if os.getenv('GAE_ENV'):  # App Engine
        logger.debug("Detected App Engine environment")
        return True
    
    # Check for general GCP environment variables
    if os.getenv('GOOGLE_CLOUD_PROJECT') and not os.getenv('LOCAL_DEV'):
        logger.debug("Detected GCP environment via GOOGLE_CLOUD_PROJECT")
        return True
    
    # Fallback to metadata server check
    try:
        import requests
        response = requests.get(
            "http://metadata.google.internal/computeMetadata/v1/project/project-id",
            headers={"Metadata-Flavor": "Google"},
            timeout=2
        )
        if response.status_code == 200:
            logger.debug("Detected GCP environment via metadata server")
            return True
    except Exception as e:
        logger.debug(f"Metadata server check failed: {e}")
    
    logger.debug("Not running in GCP environment")
    return False

def publish_snapshot_completed_event(snapshot_id: str, data_counts: Dict[str, int], 
                                   reference_counts: Dict[str, int]) -> Optional[str]:
    """
    Publish snapshot completion event to environment-specific Pub/Sub topic for scoring pipeline.
    
    Args:
        snapshot_id: The snapshot identifier
        data_counts: Dict of table_name -> record_count for snapshot data
        reference_counts: Dict of table_name -> record_count for reference data
        
    Returns:
        Message ID if successful, None if failed, "local_mode" if running locally
    """
    logger = logging.getLogger('hubspot.events')
    
    # Check if running in GCP
    if not is_running_in_gcp():
        logger.info("📤 Local development mode - skipping Pub/Sub event publishing")
        logger.debug(f"Would have published event for snapshot {snapshot_id}")
        return "local_mode_development"
    
    try:
        # Get project ID from environment or BigQuery client
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('BIGQUERY_PROJECT_ID')
        if not project_id:
            client = bigquery.Client()
            project_id = client.project
        
        logger.debug(f"Publishing to project: {project_id}")
        
        # Get Pub/Sub client
        try:
            publisher = _get_pubsub_client()
        except ImportError:
            logger.error("⚠️ Pub/Sub client not available - missing google-cloud-pubsub dependency")
            return "missing_dependency"
        
        # Get environment-specific topic
        topic_name = get_pubsub_topic_name()
        topic_path = publisher.topic_path(project_id, topic_name)
        
        logger.info(f"📤 Publishing to environment-specific topic: {topic_name}")
        logger.debug(f"Full topic path: {topic_path}")
        
        # Build event data
        current_env = get_environment()
        event_data = {
            'snapshot_id': snapshot_id,
            'timestamp': datetime.utcnow().isoformat() + "Z",
            'data_tables': data_counts,
            'reference_tables': reference_counts,
            'metadata': {
                'triggered_by': 'ingest_function',
                'environment': current_env,
                'target_environment': current_env,  # Explicit target for clarity
                'total_data_records': sum(data_counts.values()),
                'total_reference_records': sum(reference_counts.values()),
                'function_name': os.getenv('K_SERVICE', os.getenv('FUNCTION_NAME', 'unknown'))
            }
        }
        
        # Build full event envelope
        event = {
            "type": "hubspot.snapshot.completed",
            "version": "1.0",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": f"hubspot-ingest-{current_env}",
            "environment": current_env,
            "data": event_data
        }
        
        # Publish event
        message_json = json.dumps(event)
        logger.debug(f"Publishing message: {len(message_json)} bytes to {topic_name}")
        
        future = publisher.publish(topic_path, message_json.encode('utf-8'))
        message_id = future.result()
        
        logger.info(f"📤 Published snapshot.completed event to {topic_name} (message ID: {message_id})")
        logger.debug(f"Event data: snapshot_id={snapshot_id}, tables={list(data_counts.keys())}")
        
        return message_id
        
    except Exception as e:
        error_str = str(e).lower()
        
        # Handle specific error types
        if "403" in error_str or "permission" in error_str:
            logger.error(f"❌ Pub/Sub permission denied: {e}")
            logger.error("💡 Fix: Grant pubsub.publisher role to the service account")
            logger.error(f"💡 Command: gcloud pubsub topics add-iam-policy-binding {get_pubsub_topic_name()} --member='serviceAccount:{os.getenv('SERVICE_ACCOUNT', 'YOUR_SERVICE_ACCOUNT')}' --role='roles/pubsub.publisher'")
            return "permission_denied"
            
        elif "404" in error_str or "not found" in error_str:
            topic_name = get_pubsub_topic_name()
            logger.error(f"❌ Pub/Sub topic not found: {topic_name}")
            logger.error("💡 Fix: Create the environment-specific topic")
            logger.error(f"💡 Command: gcloud pubsub topics create {topic_name}")
            return "topic_not_found"
            
        else:
            logger.error(f"❌ Failed to publish event: {e}")
            logger.debug(f"Error type: {type(e).__name__}")
            return "publish_failed"


def publish_snapshot_failed_event(snapshot_id: str, error_message: str) -> Optional[str]:
    """
    Publish snapshot failure event to environment-specific Pub/Sub topic.
    
    Args:
        snapshot_id: The snapshot identifier
        error_message: Description of the error
        
    Returns:
        Message ID if successful, None if failed, "local_mode" if running locally
    """
    logger = logging.getLogger('hubspot.events')
    
    # Check if running in GCP
    if not is_running_in_gcp():
        logger.info("📤 Local development mode - skipping failure event publishing")
        return "local_mode_development"
    
    try:
        # Get project ID
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('BIGQUERY_PROJECT_ID')
        if not project_id:
            client = bigquery.Client()
            project_id = client.project
        
        # Get Pub/Sub client
        try:
            publisher = _get_pubsub_client()
        except ImportError:
            logger.error("⚠️ Pub/Sub client not available")
            return "missing_dependency"
        
        # Get environment-specific topic
        topic_name = get_pubsub_topic_name()
        topic_path = publisher.topic_path(project_id, topic_name)
        
        # Build event data
        current_env = get_environment()
        event_data = {
            'snapshot_id': snapshot_id,
            'timestamp': datetime.utcnow().isoformat() + "Z",
            'error_message': error_message,
            'metadata': {
                'triggered_by': 'ingest_function',
                'environment': current_env,
                'function_name': os.getenv('K_SERVICE', os.getenv('FUNCTION_NAME', 'unknown'))
            }
        }
        
        # Build full event envelope
        event = {
            "type": "hubspot.snapshot.failed",
            "version": "1.0",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": f"hubspot-ingest-{current_env}",
            "environment": current_env,
            "data": event_data
        }
        
        # Publish event
        message_json = json.dumps(event)
        future = publisher.publish(topic_path, message_json.encode('utf-8'))
        message_id = future.result()
        
        logger.info(f"📤 Published snapshot.failed event to {topic_name} (message ID: {message_id})")
        
        return message_id
        
    except Exception as e:
        logger.error(f"❌ Failed to publish failure event: {e}")
        return "publish_failed"


def publish_custom_event(event_type: str, event_data: Dict, source: str = "hubspot-ingest") -> Optional[str]:
    """
    Publish a custom event to environment-specific Pub/Sub topic.
    
    Args:
        event_type: Type of event (e.g., "hubspot.reference.updated")
        event_data: Event-specific data
        source: Source system identifier
        
    Returns:
        Message ID if successful, None if failed, "local_mode" if running locally
    """
    logger = logging.getLogger('hubspot.events')
    
    # Check if running in GCP
    if not is_running_in_gcp():
        logger.info(f"📤 Local development mode - skipping custom event: {event_type}")
        return "local_mode_development"
    
    try:
        # Get project ID
        project_id = os.getenv('GOOGLE_CLOUD_PROJECT') or os.getenv('BIGQUERY_PROJECT_ID')
        if not project_id:
            client = bigquery.Client()
            project_id = client.project
        
        # Get Pub/Sub client
        try:
            publisher = _get_pubsub_client()
        except ImportError:
            logger.error("⚠️ Pub/Sub client not available")
            return "missing_dependency"
        
        # Get environment-specific topic
        topic_name = get_pubsub_topic_name()
        topic_path = publisher.topic_path(project_id, topic_name)
        
        # Build full event envelope
        current_env = get_environment()
        event = {
            "type": event_type,
            "version": "1.0",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "source": f"{source}-{current_env}",
            "environment": current_env,
            "data": event_data
        }
        
        # Publish event
        message_json = json.dumps(event)
        future = publisher.publish(topic_path, message_json.encode('utf-8'))
        message_id = future.result()
        
        logger.info(f"📤 Published {event_type} event to {topic_name} (message ID: {message_id})")
        
        return message_id
        
    except Exception as e:
        logger.error(f"❌ Failed to publish custom event: {e}")
        return "publish_failed"