# --- fetcher.py ---
# Fetch HubSpot data using the official SDK and schema config
import logging
import os
from hubspot import HubSpot
from datetime import datetime

def get_client():
    return HubSpot(access_token=os.environ['HUBSPOT_API_KEY'])

def fetch_object(object_type, config, snapshot_id, limit=100):
    # Includes support for associations defined in schema
    client = get_client()
    logging.info(f"Connecting to HubSpot API for '{object_type}' with limit={limit}")
    api_object = config.get("api_object", object_type)
    api = getattr(client.crm, api_object).basic_api

    fields = config.get("fields", {})
    props = [p for p in fields if p != "id"]

    out = []
    after = None
    while True:
        assoc_keys = list(config.get("associations", {}).keys())
        page = api.get_page(limit=limit, properties=props, after=after, associations=assoc_keys if assoc_keys else None)
        for obj in page.results:
            row = {
                fields.get("id", config["id_field"]): obj.id,
                "snapshot_id": snapshot_id
            }
            for hs_key, bq_key in fields.items():
                if hs_key != "id":
                    row[bq_key] = obj.properties.get(hs_key)
            # Fetch associations if defined
            if "associations" in config and hasattr(obj, "associations"):
                for assoc_type, assoc_cfg in config["associations"].items():
                    assoc_data = obj.associations.get(assoc_type, None)
                    if assoc_data and assoc_data.results:
                        ids = [r.id for r in assoc_data.results]
                        if assoc_cfg.get("association_type", "single") == "single":
                            row[assoc_cfg["field_name"]] = ids[0] if ids else None
                        else:
                            row[assoc_cfg["field_name"]] = ids
                    else:
                        row[assoc_cfg["field_name"]] = None

            out.append(row)
            if limit is not None and len(out) >= limit:
                logging.info(f"Total {len(out)} records fetched for '{object_type}'")
                return out

        if not page.paging or not page.paging.next:
            break
        after = page.paging.next.after

    logging.info(f"Total {len(out)} records fetched for '{object_type}'")
    return out