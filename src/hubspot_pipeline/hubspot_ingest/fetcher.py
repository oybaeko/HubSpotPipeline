# src/hubspot_pipeline/hubspot_ingest/fetcher.py

import logging
import os
from hubspot import HubSpot
from datetime import datetime

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
    Fetch objects from HubSpot API with comprehensive logging
    
    Args:
        object_type: Type of object to fetch (e.g., 'company', 'deal')
        config: Configuration for this object type from schema
        snapshot_id: Unique identifier for this data snapshot
        limit: Maximum number of records to fetch (None for no limit)
    
    Returns:
        List of processed records ready for BigQuery
    """
    logger = logging.getLogger('hubspot.fetch')
    
    # Includes support for associations defined in schema
    client = get_client()
    
    # Extract configuration
    api_object = config.get("api_object", object_type)
    fields = config.get("fields", {})
    associations_config = config.get("associations", {})
    
    logger.info(f"🔗 Connecting to HubSpot API for '{object_type}' (API object: {api_object})")
    if limit:
        logger.info(f"📊 Fetch limit: {limit}")
    else:
        logger.info("📊 Fetch limit: None (all records)")
    
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
            # Determine page size (HubSpot max is 100)
            page_limit = min(limit or 100, 100)
            
            logger.debug(f"📄 Fetching page {page_count} (after: {after}, limit: {page_limit})")
            
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
            
            # Check if we should continue
            records_so_far = len(out)
            
            if limit is not None and records_so_far >= limit:
                logger.info(f"✅ Reached limit of {limit} records")
                break
                
            if not page.paging or not page.paging.next:
                logger.info(f"✅ No more pages available")
                break
                
            after = page.paging.next.after
            logger.debug(f"Next page token: {after}")
            
        except Exception as e:
            logger.error(f"Error fetching page {page_count}: {e}")
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"API call details - after: {after}, limit: {page_limit}")
            raise RuntimeError(f"Failed to fetch {object_type} data: {e}")
    
    # Final summary
    total_time = (datetime.utcnow() - start_time).total_seconds()
    final_count = len(out)
    
    logger.info(f"✅ Fetch completed for {object_type}")
    logger.info(f"📊 Total records: {final_count}")
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