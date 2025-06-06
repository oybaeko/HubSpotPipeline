# src/hubspot_pipeline/hubspot_ingest/reference/fetchers.py

import logging
import os
import requests
from datetime import datetime
from typing import List, Dict, Any

def fetch_owners() -> List[Dict[str, Any]]:
    """
    Fetch all owners from HubSpot using the owners API endpoint.
    
    Returns:
        List of owner dictionaries ready for BigQuery insertion
    """
    logger = logging.getLogger('hubspot.reference')
    
    api_key = os.getenv('HUBSPOT_API_KEY')
    if not api_key:
        logger.error("HUBSPOT_API_KEY not found in environment")
        raise RuntimeError("HUBSPOT_API_KEY not found in environment")
    
    url = "https://api.hubapi.com/crm/v3/owners"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    logger.info("📊 Fetching owners from HubSpot...")
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        raw_owners = data.get("results", [])
        
        logger.info(f"✅ Retrieved {len(raw_owners)} owners from HubSpot")
        
        # Transform to BigQuery schema format
        owners_rows = []
        for owner in raw_owners:
            row = {
                "owner_id": str(owner.get("id")),  # Ensure string
                "email": owner.get("email"),
                "first_name": owner.get("firstName"),
                "last_name": owner.get("lastName"),
                "user_id": str(owner.get("userId")) if owner.get("userId") else None,
                "active": bool(owner.get("active", False)),
                "timestamp": owner.get("updatedAt") or owner.get("createdAt"),
            }
            owners_rows.append(row)
        
        logger.info(f"✅ Transformed {len(owners_rows)} owner records for BigQuery")
        return owners_rows
        
    except requests.RequestException as e:
        logger.error(f"❌ Failed to fetch owners from HubSpot: {e}")
        raise RuntimeError(f"HubSpot API error: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error fetching owners: {e}")
        raise


def fetch_deal_stages() -> List[Dict[str, Any]]:
    """
    Fetch all deal stages from HubSpot pipelines API.
    
    Returns:
        List of deal stage dictionaries ready for BigQuery insertion
    """
    logger = logging.getLogger('hubspot.reference')
    
    api_key = os.getenv('HUBSPOT_API_KEY')
    if not api_key:
        logger.error("HUBSPOT_API_KEY not found in environment")
        raise RuntimeError("HUBSPOT_API_KEY not found in environment")
    
    url = "https://api.hubapi.com/crm/v3/pipelines/deals"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    logger.info("📊 Fetching deal stages from HubSpot...")
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        pipelines = data.get("results", [])
        
        # Transform pipelines to stage records
        stage_records = []
        for pipeline in pipelines:
            pipeline_id = str(pipeline.get("id"))
            pipeline_label = pipeline.get("label")
            
            for stage in pipeline.get("stages", []):
                # Handle boolean conversion properly
                is_closed_raw = stage.get("metadata", {}).get("isClosed", False)
                if isinstance(is_closed_raw, str):
                    is_closed = is_closed_raw.lower() == "true"
                else:
                    is_closed = bool(is_closed_raw)
                
                record = {
                    "pipeline_id": pipeline_id,
                    "pipeline_label": pipeline_label,
                    "stage_id": str(stage.get("id")),
                    "stage_label": stage.get("label"),
                    "is_closed": is_closed,
                    "probability": float(stage.get("metadata", {}).get("probability", 0)),
                    "display_order": int(stage.get("displayOrder", 0))
                }
                stage_records.append(record)
        
        logger.info(f"✅ Fetched {len(stage_records)} deal stages from {len(pipelines)} pipelines")
        return stage_records
        
    except requests.RequestException as e:
        logger.error(f"❌ Failed to fetch deal stages from HubSpot: {e}")
        raise RuntimeError(f"HubSpot API error: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error fetching deal stages: {e}")
        raise