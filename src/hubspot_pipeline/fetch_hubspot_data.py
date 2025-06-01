# src/hubspot_pipeline/fetch_hubspot_data.py

import os
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests

# Pull the HubSpot API key from your central config
from hubspot_pipeline.config.config import HUBSPOT_API_KEY

# Field‐to‐HubSpot‐API mappings (schema maps)
from hubspot_pipeline.schema import (
    HUBSPOT_COMPANY_FIELD_MAP,
    HUBSPOT_DEAL_FIELD_MAP,
    HUBSPOT_CONTACT_FIELD_MAP,
)

# ------------------------------------------------------------------------------
# Sanity check: make sure the API key is set
# ------------------------------------------------------------------------------
if not HUBSPOT_API_KEY:
    raise RuntimeError("Missing HUBSPOT_API_KEY in environment or config.")

# ------------------------------------------------------------------------------
# Define endpoints and field‐maps in one place
# ------------------------------------------------------------------------------
ENDPOINTS = {
    "companies": {
        "search": "https://api.hubapi.com/crm/v3/objects/companies/search",
        "list":   "https://api.hubapi.com/crm/v3/objects/companies",
    },
    "deals": {
        "search": "https://api.hubapi.com/crm/v3/objects/deals/search",
        "list":   "https://api.hubapi.com/crm/v3/objects/deals",
    },
    "contacts": {
        "search": "https://api.hubapi.com/crm/v3/objects/contacts/search",
        "list":   "https://api.hubapi.com/crm/v3/objects/contacts",
    },
}

FIELD_MAPS = {
    "companies": HUBSPOT_COMPANY_FIELD_MAP,
    "deals":     HUBSPOT_DEAL_FIELD_MAP,
    "contacts":  HUBSPOT_CONTACT_FIELD_MAP,
}

HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_API_KEY}",
    "Content-Type": "application/json",
}

# ------------------------------------------------------------------------------
# Generic HubSpot fetcher (search vs. list)
# ------------------------------------------------------------------------------
def fetch_hs_objects(
    object_type: str,
    filters:   Optional[List[Dict[str, Any]]] = None,
    limit:     Optional[int]                   = None,
    properties: Optional[List[str]]           = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Generic HubSpot‐API fetcher for 'companies', 'deals', or 'contacts'.
    - If `filters` is provided, uses the /search endpoint (POST).
    - Otherwise uses the /list endpoint (GET).
    - Automatically paginates, and returns (results, api_call_count).

    Args:
      object_type:  One of "companies", "deals", "contacts".
      filters:      If non‐None, a list of HubSpot filter dicts → use /search.
      limit:        Maximum records to return (or None for “all”).
      properties:   List of HubSpot‐API property names. If None, defaults to
                    all values in FIELD_MAPS[object_type] (i.e. the map’s values).

    Returns:
      (results, call_count)
    """
    if object_type not in ENDPOINTS:
        raise ValueError(f"Unknown object_type: {object_type!r}. Must be one of {list(ENDPOINTS)}")

    # If no explicit properties passed, derive them from our FIELD_MAP:
    if properties is None:
        properties = list(FIELD_MAPS[object_type].values())

    url_search = ENDPOINTS[object_type]["search"]
    url_list   = ENDPOINTS[object_type]["list"]

    all_results: List[Dict[str, Any]] = []
    after: Optional[str] = None
    call_count = 0

    # 1) If filters is a non‐empty list, use /search (POST)
    if filters:
        logging.info("Using HubSpot ‘search’ endpoint for %s", object_type)

        while True:
            payload = {
                "limit": min(limit or 100, 100),
                "properties": properties,
                "filterGroups": [{"filters": filters}],
            }
            if after:
                payload["after"] = after

            resp = requests.post(url_search, headers=HEADERS, json=payload)
            call_count += 1

            if resp.status_code != 200:
                logging.error(
                    "❌ HubSpot %s search error %d: %s",
                    object_type, resp.status_code, resp.text
                )
                break

            data = resp.json()
            all_results.extend(data.get("results", []))

            after = data.get("paging", {}).get("next", {}).get("after")
            if not after or (limit and len(all_results) >= limit):
                break

    # 2) Otherwise, use /list (GET) with no filters
    else:
        logging.info("Using HubSpot ‘list’ endpoint for %s", object_type)

        params = {
            "limit": min(limit or 100, 100),
            "properties": ",".join(properties)
        }

        while True:
            if after:
                params["after"] = after

            resp = requests.get(url_list, headers=HEADERS, params=params)
            call_count += 1

            if resp.status_code != 200:
                logging.error(
                    "❌ HubSpot %s list error %d: %s",
                    object_type, resp.status_code, resp.text
                )
                break

            data = resp.json()
            all_results.extend(data.get("results", []))

            after = data.get("paging", {}).get("next", {}).get("after")
            if not after or (limit and len(all_results) >= limit):
                break

    logging.info("✅ Retrieved %d %s in %d API calls.", len(all_results), object_type, call_count)
    return (all_results[:limit] if limit else all_results, call_count)


# ------------------------------------------------------------------------------
# fetch_companies → returns (companies_list, api_call_count)
# Autopicks properties from HUBSPOT_COMPANY_FIELD_MAP
# ------------------------------------------------------------------------------
def fetch_companies(
    filters:    Optional[List[Dict[str, Any]]] = None,
    limit:      Optional[int]                   = None,
    properties: Optional[List[str]]             = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Fetch companies from HubSpot. Returns (companies, call_count).

    - If you omit `properties`, it requests exactly all values from
      HUBSPOT_COMPANY_FIELD_MAP (i.e. whatever you defined in your schema).
    """
    return fetch_hs_objects(
        object_type="companies",
        filters=filters,
        limit=limit,
        properties=properties,
    )


# ------------------------------------------------------------------------------
# fetch_deals → returns (deals_list, api_call_count)
# Autopicks properties from HUBSPOT_DEAL_FIELD_MAP
# ------------------------------------------------------------------------------
def fetch_deals(
    filters:    Optional[List[Dict[str, Any]]] = None,
    limit:      Optional[int]                   = None,
    properties: Optional[List[str]]             = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Fetch deals from HubSpot. Returns (deals, call_count).

    - If you omit `properties`, it requests exactly all values from
      HUBSPOT_DEAL_FIELD_MAP.
    """
    return fetch_hs_objects(
        object_type="deals",
        filters=filters,
        limit=limit,
        properties=properties,
    )


# ------------------------------------------------------------------------------
# fetch_all_deals_with_company → special wrapper that always “list” deals
# and uses the full deal‐field map
# ------------------------------------------------------------------------------
def fetch_all_deals_with_company(
    limit: Optional[int] = None
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Fetch all deals (no filters) plus their associated companies.

    Internally, this calls fetch_hs_objects(..., object_type="deals", filters=None,
    properties=list(HUBSPOT_DEAL_FIELD_MAP.values())) so that *every* field in your
    deal‐schema is requested.
    """
    properties = list(HUBSPOT_DEAL_FIELD_MAP.values())
    return fetch_hs_objects(
        object_type="deals",
        filters=None,
        limit=limit,
        properties=properties,
    )


# ------------------------------------------------------------------------------
# fetch_contacts → returns (contacts_list, api_call_count)
# Autopicks properties from HUBSPOT_CONTACT_FIELD_MAP
# ------------------------------------------------------------------------------
def fetch_contacts(
    filters:    Optional[List[Dict[str, Any]]] = None,
    limit:      Optional[int]                   = None,
    properties: Optional[List[str]]             = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Fetch contacts from HubSpot. Returns (contacts, call_count).

    - If you omit `properties`, it requests exactly all values from
      HUBSPOT_CONTACT_FIELD_MAP (whatever you defined in your schema).
    """
    return fetch_hs_objects(
        object_type="contacts",
        filters=filters,
        limit=limit,
        properties=properties,
    )


# ------------------------------------------------------------------------------
# fetch_owners → single GET call (no pagination)
# ------------------------------------------------------------------------------
def fetch_owners() -> List[Dict[str, Any]]:
    """
    Fetch all owners from HubSpot. Returns a list of owner objects (no call_count).

    This endpoint does not follow the same pagination scheme, so we do one GET.
    """
    url = "https://api.hubapi.com/crm/v3/owners"
    resp = requests.get(url, headers=HEADERS)

    if resp.status_code != 200:
        logging.error("❌ HubSpot Owner API Error: %s", resp.text)
        return []

    data = resp.json()
    owners = data.get("results", [])
    logging.info("✅ Retrieved %d owners from HubSpot", len(owners))
    return owners


# ------------------------------------------------------------------------------
# When run directly, do a quick sanity‐check
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.INFO)

    # 1) Fetch 5 companies (auto‐picked properties from the schema‐map)
    companies, comp_calls = fetch_companies(limit=5)
    logging.info("Sample Companies (first 2): %s", json.dumps(companies[:2], indent=2))
    logging.info("API calls for companies: %d", comp_calls)

    # 2) Fetch 5 deals (auto‐picked properties from the schema‐map)
    deals, deal_calls = fetch_deals(limit=5)
    logging.info("Sample Deals (first 2): %s", json.dumps(deals[:2], indent=2))
    logging.info("API calls for deals: %d", deal_calls)

    # 3) Fetch 5 deals with associated companies (uses full deal‐field map)
    all_deals, all_deal_calls = fetch_all_deals_with_company(limit=5)
    logging.info("Sample Deals w/ associations (first 2): %s", json.dumps(all_deals[:2], indent=2))
    logging.info("API calls for deals-with-company: %d", all_deal_calls)

    # 4) Fetch all owners
    owners = fetch_owners()
    logging.info("Sample Owners (first 2): %s", json.dumps(owners[:2], indent=2))
