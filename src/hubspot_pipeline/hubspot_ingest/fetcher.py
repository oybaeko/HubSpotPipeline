# src/hubspot_pipeline/hubspot_ingest/fetcher.py

import logging
import os
from hubspot import HubSpot
from datetime import datetime
from hubspot_pipeline.hubspot_ingest.store import store_to_bigquery, upsert_to_bigquery
from hubspot_pipeline.hubspot_ingest.events import publish_snapshot_completed_event
from hubspot_pipeline.hubspot_ingest.registry import register_snapshot_ingest_complete

def get_client():
    """Get HubSpot client with API key from environment"""
    logger = logging.getLogger('hubspot.fetch')
    api_key = os.environ.get('HUBSPOT_API_KEY')
    
    if not api_key:
        logger.error("HUBSPOT_API_KEY not found in environment")
        raise RuntimeError("HUBSPOT_API_KEY not found in environment")
    
    logger.debug("Creating HubSpot client")
    return HubSpot(access_token=api_key)

def fetch_object(object_type, config, snapshot_id, limit=100):
    """
    Fetch objects from HubSpot API with comprehensive logging and proper limit handling
    
    Args:
        object_type: Type of object to fetch (e.g., 'company', 'deal')
        config: Configuration for this object type from schema
        snapshot_id: Unique identifier for this data snapshot
        limit: Maximum number of records to fetch (None or 0 for no limit)
    
    Returns:
        List of processed records ready for BigQuery
    """
    logger = logging.getLogger('hubspot.fetch')
    
    # Handle no-limit cases: None or 0 means unlimited
    unlimited = limit is None or limit == 0
    effective_limit = None if unlimited else limit
    
    # Includes support for associations defined in schema
    client = get_client()
    
    # Extract configuration
    api_object = config.get("api_object", object_type)
    fields = config.get("fields", {})
    associations_config = config.get("associations", {})
    
    logger.info(f"🔗 Connecting to HubSpot API for '{object_type}' (API object: {api_object})")
    if unlimited:
        logger.info("📊 Fetch limit: UNLIMITED (all records)")
    else:
        logger.info(f"📊 Fetch limit: {effective_limit}")
    
    try:
        api = getattr(client.crm, api_object).basic_api
        logger.debug(f"Successfully connected to {api_object} API")
    except AttributeError as e:
        logger.error(f"Invalid API object '{api_object}': {e}")
        raise RuntimeError(f"Invalid API object '{api_object}': {e}")
    
    # Prepare properties to fetch
    props = [p for p in fields if p != "id"]
    logger.debug(f"Properties to fetch: {props}")
    
    # Prepare associations
    assoc_keys = list(associations_config.keys()) if associations_config else []
    if assoc_keys:
        logger.debug(f"Associations to fetch: {assoc_keys}")
    
    out = []
    after = None
    page_count = 0
    api_calls = 0
    start_time = datetime.utcnow()
    
    logger.info(f"🚀 Starting data fetch for {object_type}")
    
    while True:
        page_count += 1
        page_start_time = datetime.utcnow()
        
        try:
            # Calculate page size: min of (remaining records needed, HubSpot max 100)
            records_so_far = len(out)
            
            if unlimited:
                # No limit - use HubSpot's max page size
                page_limit = 100
            else:
                # Limited - only fetch what we need, up to HubSpot's max
                remaining_needed = effective_limit - records_so_far
                page_limit = min(remaining_needed, 100)
                
                if page_limit <= 0:
                    logger.info(f"✅ Reached exact limit of {effective_limit} records")
                    break
            
            logger.debug(f"📄 Fetching page {page_count} (after: {after}, page_limit: {page_limit}, total_so_far: {records_so_far})")
            
            # Make API call
            page = api.get_page(
                limit=page_limit, 
                properties=props, 
                after=after, 
                associations=assoc_keys if assoc_keys else None
            )
            api_calls += 1
            
            page_time = (datetime.utcnow() - page_start_time).total_seconds()
            page_size = len(page.results)
            
            logger.info(f"📄 Page {page_count}: {page_size} records in {page_time:.2f}s")
            
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"API call {api_calls} completed - Rate: {page_size/page_time:.1f} records/second")
            
            # Process each object in the page
            for i, obj in enumerate(page.results):
                # Check if we've hit our limit before processing this record
                if not unlimited and len(out) >= effective_limit:
                    logger.info(f"✅ Reached exact limit of {effective_limit} records (stopping mid-page)")
                    break
                
                try:
                    # Build the record
                    row = {
                        fields.get("id", config["id_field"]): obj.id,
                        "snapshot_id": snapshot_id
                    }
                    
                    # Add properties
                    for hs_key, bq_key in fields.items():
                        if hs_key != "id":
                            value = obj.properties.get(hs_key)
                            row[bq_key] = value
                            
                            if logger.isEnabledFor(logging.DEBUG) and i == 0:  # Log first record details
                                logger.debug(f"Mapped {hs_key} -> {bq_key}: {type(value).__name__}")
                    
                    # Fetch associations if defined
                    if associations_config and hasattr(obj, "associations"):
                        for assoc_type, assoc_cfg in associations_config.items():
                            assoc_data = obj.associations.get(assoc_type, None) if obj.associations else None
                            
                            if assoc_data and assoc_data.results:
                                ids = [r.id for r in assoc_data.results]
                                if assoc_cfg.get("association_type", "single") == "single":
                                    row[assoc_cfg["field_name"]] = ids[0] if ids else None
                                    if logger.isEnabledFor(logging.DEBUG) and i == 0:
                                        logger.debug(f"Associated {assoc_type} (single): {ids[0] if ids else None}")
                                else:
                                    row[assoc_cfg["field_name"]] = ids
                                    if logger.isEnabledFor(logging.DEBUG) and i == 0:
                                        logger.debug(f"Associated {assoc_type} (multiple): {len(ids)} items")
                            else:
                                row[assoc_cfg["field_name"]] = None
                    
                    out.append(row)
                    
                except Exception as e:
                    logger.warning(f"Error processing record {obj.id}: {e}")
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Problematic record data: {obj}")
                    continue  # Skip this record but continue processing
            
            # Check if we should continue to next page
            records_so_far = len(out)
            
            # Stop if we've hit our limit
            if not unlimited and records_so_far >= effective_limit:
                logger.info(f"✅ Reached limit of {effective_limit} records")
                break
                
            # Stop if no more pages available
            if not page.paging or not page.paging.next:
                if unlimited:
                    logger.info(f"✅ No more pages available (unlimited mode)")
                else:
                    logger.info(f"✅ No more pages available (got {records_so_far}/{effective_limit} records)")
                break
                
            after = page.paging.next.after
            logger.debug(f"Next page token: {after}")
            
        except Exception as e:
            logger.error(f"Error fetching page {page_count}: {e}")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"API call details - after: {after}, page_limit: {page_limit}")
            raise RuntimeError(f"Failed to fetch {object_type} data: {e}")
    
    # Final truncation to ensure exact limit compliance
    if not unlimited and len(out) > effective_limit:
        logger.warning(f"⚠️ Truncating {len(out)} records to exact limit of {effective_limit}")
        out = out[:effective_limit]
    
    # Final summary
    total_time = (datetime.utcnow() - start_time).total_seconds()
    final_count = len(out)
    
    logger.info(f"✅ Fetch completed for {object_type}")
    logger.info(f"📊 Total records: {final_count}")
    if not unlimited:
        logger.info(f"📊 Limit compliance: {final_count}/{effective_limit} ({100*final_count/effective_limit:.1f}%)")
    logger.info(f"📄 Pages fetched: {page_count}")
    logger.info(f"🔗 API calls made: {api_calls}")
    logger.info(f"⏱️ Total time: {total_time:.2f}s")
    
    if total_time > 0:
        logger.info(f"📈 Average rate: {final_count/total_time:.1f} records/second")
    
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Memory usage: {final_count} records in memory")
        if out:
            sample_keys = list(out[0].keys())
            logger.debug(f"Record structure: {sample_keys}")
    
    return out

def fetch_and_process_reference_data(snapshot_id):
    """
    Fetch and upsert reference data (owners and deal stages)
    Uses HubSpot API directly without external dependencies
    
    Returns:
        dict: Reference data counts
    """
    logger = logging.getLogger('hubspot.reference')
    
    logger.info("🔄 Fetching reference data (owners and deal stages)")
    
    reference_counts = {}
    
    # ═══ FETCH OWNERS (using HubSpot API directly) ═══
    try:
        logger.info("📊 Fetching owners from HubSpot...")
        
        client = get_client()
        owners_api = client.crm.owners.basic_api
        
        # Fetch all owners (no pagination limit needed for owners)
        owners_response = owners_api.get_page(limit=100)
        
        if owners_response.results:
            # Transform to BigQuery schema format
            owners_rows = []
            for owner in owners_response.results:
                row = {
                    "owner_id": owner.id,
                    "email": owner.email,
                    "first_name": owner.first_name,
                    "last_name": owner.last_name,
                    "user_id": getattr(owner, 'user_id', None),
                    "active": getattr(owner, 'active', True),
                    "timestamp": getattr(owner, 'updated_at', None) or getattr(owner, 'created_at', None),
                }
                owners_rows.append(row)
            
            # Upsert to BigQuery
            owners_count = upsert_to_bigquery(owners_rows, "hs_owners", "owner_id")
            reference_counts['hs_owners'] = owners_count
            logger.info(f"✅ Upserted {owners_count} owner records")
        else:
            logger.warning("⚠️ No owners data received from HubSpot")
            reference_counts['hs_owners'] = 0
            
    except Exception as e:
        logger.error(f"❌ Failed to fetch/upsert owners: {e}")
        if logger.isEnabledFor(logging.DEBUG):
            import traceback
            logger.debug(f"Owners fetch error details: {traceback.format_exc()}")
        reference_counts['hs_owners'] = 0
    
    # ═══ FETCH DEAL STAGES (using direct API call) ═══
    try:
        logger.info("📊 Fetching deal stages from HubSpot...")
        
        # Use direct requests since pipelines API might not be in the SDK
        import requests
        
        api_key = os.getenv('HUBSPOT_API_KEY')
        url = "https://api.hubapi.com/crm/v3/pipelines/deals"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            logger.error(f"❌ Failed to fetch pipelines: {response.status_code} - {response.text}")
            reference_counts['hs_deal_stage_reference'] = 0
        else:
            pipelines = response.json().get("results", [])
            
            # Transform to stage records
            records = []
            for pipeline in pipelines:
                pipeline_id = pipeline.get("id")
                pipeline_label = pipeline.get("label")
                for stage in pipeline.get("stages", []):
                    records.append({
                        "pipeline_id": pipeline_id,
                        "pipeline_label": pipeline_label,
                        "stage_id": stage.get("id"),
                        "stage_label": stage.get("label"),
                        "is_closed": stage.get("metadata", {}).get("isClosed", False),
                        "probability": float(stage.get("metadata", {}).get("probability", 0)),
                        "display_order": stage.get("displayOrder", 0)
                    })
            
            logger.info(f"📊 Fetched {len(records)} deal stages from {len(pipelines)} pipelines")
            
            # Upsert to BigQuery
            if records:
                stages_count = upsert_to_bigquery(records, "hs_deal_stage_reference", "stage_id")
                reference_counts['hs_deal_stage_reference'] = stages_count
                logger.info(f"✅ Upserted {stages_count} deal stage records")
            else:
                logger.warning("⚠️ No deal stage records to upsert")
                reference_counts['hs_deal_stage_reference'] = 0
                
    except Exception as e:
        logger.error(f"❌ Failed to fetch/upsert deal stages: {e}")
        if logger.isEnabledFor(logging.DEBUG):
            import traceback
            logger.debug(f"Deal stages fetch error details: {traceback.format_exc()}")
        reference_counts['hs_deal_stage_reference'] = 0
    
    logger.info(f"✅ Reference data processing complete: {reference_counts}")
    return reference_counts