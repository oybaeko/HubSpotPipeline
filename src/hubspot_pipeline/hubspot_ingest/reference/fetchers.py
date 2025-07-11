# src/hubspot_pipeline/hubspot_ingest/reference/fetchers.py

import logging
import os
import requests
from datetime import datetime
from typing import List, Dict, Any
from hubspot_pipeline.hubspot_ingest.normalization import normalize_field_value

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
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        raw_owners = data.get("results", [])
        
        logger.info(f"✅ Retrieved {len(raw_owners)} owners from HubSpot")
        
        # Transform to BigQuery schema format with consistent record_timestamp and normalization
        owners_rows = []
        normalization_count = 0
        
        for owner in raw_owners:
            # Apply email normalization
            original_email = owner.get("email")
            normalized_email = normalize_field_value('email', original_email, 'hs_owners')
            
            # Track normalization activity
            if original_email != normalized_email and original_email is not None:
                normalization_count += 1
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Normalized owner email: '{original_email}' -> '{normalized_email}'")
            
            row = {
                "owner_id": str(owner.get("id")),  # Ensure string
                "email": normalized_email,
                "first_name": owner.get("firstName"),
                "last_name": owner.get("lastName"),
                "user_id": str(owner.get("userId")) if owner.get("userId") else None,
                "active": bool(owner.get("active", False)),
                "record_timestamp": owner.get("updatedAt") or owner.get("createdAt"),
            }
            owners_rows.append(row)
        
        # Log normalization activity
        if normalization_count > 0:
            logger.info(f"🔧 Normalized {normalization_count} owner email addresses")
        else:
            logger.debug("🔧 No owner emails required normalization")
        
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
    
    # Fixed: Use the same API key retrieval as fetch_owners()
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
    logger.debug(f"Making request to: {url}")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        
        logger.info(f"📡 Pipelines API response: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"❌ Failed to fetch pipelines: {response.status_code}")
            logger.error(f"❌ Response headers: {dict(response.headers)}")
            logger.error(f"❌ Response body: {response.text[:500]}...")
            response.raise_for_status()  # This will raise the appropriate exception
        
        data = response.json()
        pipelines = data.get("results", [])
        
        logger.info(f"📊 Received {len(pipelines)} pipelines from API")
        logger.debug(f"📋 Pipeline IDs: {[p.get('id') for p in pipelines]}")
        
        # Transform pipelines to stage records (no timestamp field for reference data)
        stage_records = []
        for pipeline in pipelines:
            pipeline_id = str(pipeline.get("id"))
            pipeline_label = pipeline.get("label")
            stages = pipeline.get("stages", [])
            
            logger.debug(f"Pipeline '{pipeline_label}' ({pipeline_id}): {len(stages)} stages")
            
            for stage in stages:
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
                    "display_order": int(stage.get("displayOrder", 0)),
                    "record_timestamp": datetime.utcnow().isoformat() + "Z"
                }
                stage_records.append(record)
                
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"  Stage: {record['stage_label']} ({record['stage_id']})")
        
        logger.info(f"✅ Fetched {len(stage_records)} deal stages from {len(pipelines)} pipelines")
        logger.debug("🔧 Deal stages do not require normalization (reference data)")
        return stage_records
        
    except requests.RequestException as e:
        logger.error(f"❌ Failed to fetch deal stages from HubSpot: {e}")
        raise RuntimeError(f"HubSpot API error: {e}")
    except Exception as e:
        logger.error(f"❌ Unexpected error fetching deal stages: {e}")
        raise