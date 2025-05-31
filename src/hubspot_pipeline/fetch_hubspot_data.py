import logging, os
import requests
from .config.config import HUBSPOT_API_KEY

# your field-maps live here (make sure you’ve defined CONTACT too)
from hubspot_pipeline.schema import (
    HUBSPOT_COMPANY_FIELD_MAP,
    HUBSPOT_DEAL_FIELD_MAP,
    HUBSPOT_CONTACT_FIELD_MAP,
)

API_KEY = os.getenv("HUBSPOT_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing HUBSPOT_API_KEY environment variable")

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
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}


def fetch_companies(filters=None, limit=None):
    """
    Fetches companies from the HubSpot API with optional filtering and result limiting.

    Args:
        filters (list, optional): A list of filter dictionaries to apply to the search query. Defaults to None.
        limit (int, optional): The maximum number of companies to retrieve. If None, retrieves all available companies. Defaults to None.

    Returns:
        list: A list of company objects retrieved from the HubSpot API. If an error occurs, returns an empty list.

    Notes:
        - Requires the global variable `HUBSPOT_API_KEY` to be set with a valid HubSpot API key.
        - logging.infos the number of companies retrieved and the number of API calls made.
        - Handles pagination automatically.
    """
    url = "https://api.hubapi.com/crm/v3/objects/companies/search"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    all_results = []
    after = None
    call_count = 0

    while True:
        payload = {
            "limit": 100,
            "properties": [
                "name",
                "lifecyclestage",
                "hs_lead_status",
                "hubspot_owner_id",
                "type"
            ],
            "filterGroups": [{"filters": filters}] if filters else [{}]
        }
        if after:
            payload["after"] = after

        response = requests.post(url, headers=headers, json=payload)
        call_count += 1

        if response.status_code != 200:
            logging.info(f"❌ HubSpot API Error (companies): {response.text}")
            return []

        data = response.json()
        all_results.extend(data.get("results", []))

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after or (limit and len(all_results) >= limit):
            break

    logging.info(f"✅ Retrieved {len(all_results)} companies in {call_count} API calls.")
    return all_results[:limit] if limit else all_results


def fetch_deals(filters=None, limit=None):
    url = "https://api.hubapi.com/crm/v3/objects/deals/search"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }

    all_results = []
    after = None
    call_count = 0

    while True:
        payload = {
            "limit": 100,
            "properties": [
                "dealname",
                "dealstage",
                "amount",
                "dealtype",
                "hubspot_owner_id"
            ],
            "associations": ["companies"],
            "filterGroups": [{"filters": filters}] if filters else [{}]
        }
        if after:
            payload["after"] = after

        response = requests.post(url, headers=headers, json=payload)
        call_count += 1

        if response.status_code != 200:
            logging.info(f"❌ HubSpot API Error (deals): {response.text}")
            return []

        data = response.json()
        all_results.extend(data.get("results", []))

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after or (limit and len(all_results) >= limit):
            break

    logging.info(f"✅ Retrieved {len(all_results)} deals in {call_count} API calls.")
    return all_results[:limit] if limit else all_results

def fetch_all_deals_with_company(limit=None):
    """
    Fetch all deals from HubSpot including associations (companies),
    and essential properties. Returns raw API results for downstream processing.

    Args:
        limit (int, optional): Max number of deals to return. If None, fetches all.

    Returns:
        list: Raw deal records from HubSpot, including associations and properties.
    """
    url = "https://api.hubapi.com/crm/v3/objects/deals"
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}"
    }
    params = {
        "limit": 100,
        "associations": "companies",
        "properties": [
            "dealname",
            "dealstage",
            "amount",
            "dealtype",
            "hubspot_owner_id"
        ]
    }

    all_results = []
    after = None
    call_count = 0

    while True:
        if after:
            params["after"] = after

        response = requests.get(url, headers=headers, params=params)
        call_count += 1

        if response.status_code != 200:
            logging.info(f"❌ HubSpot API Error: {response.text}")
            break

        data = response.json()
        results = data.get("results", [])
        all_results.extend(results)

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after or (limit and len(all_results) >= limit):
            break

    logging.info(f"✅ Retrieved {len(all_results)} deals in {call_count} API calls.")
    return all_results[:limit] if limit else all_results


def fetch_owners():
    url = "https://api.hubapi.com/crm/v3/owners"
    headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        logging.info(f"❌ HubSpot Owner API Error: {response.text}")
        return []

    data = response.json()
    owners = data.get("results", [])
    logging.info(f"✅ Retrieved {len(owners)} owners from HubSpot")
    return owners

# src/hubspot_pipeline/fetch_hubspot_data.py

import os
import logging
import requests
from typing import Any, Dict, List, Optional, Tuple

# your field-maps live here (make sure you’ve defined CONTACT too)
from hubspot_pipeline.schema import (
    HUBSPOT_COMPANY_FIELD_MAP,
    HUBSPOT_DEAL_FIELD_MAP,
    HUBSPOT_CONTACT_FIELD_MAP,
)

API_KEY = os.getenv("HUBSPOT_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing HUBSPOT_API_KEY environment variable")

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
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}


def fetch_hs_objects(
    object_type: str,
    filters: Optional[List[Dict[str, Any]]] = None,
    limit:   Optional[int] = None,
    properties: Optional[List[str]] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Generic fetcher for companies, deals or contacts.
    Returns (results, call_count).
    """
    if object_type not in ENDPOINTS:
        raise ValueError(f"Unknown object_type: {object_type!r}")

    # default to all mapped properties
    if properties is None:
        properties = list(FIELD_MAPS[object_type].values())

    url_search = ENDPOINTS[object_type]["search"]
    url_list   = ENDPOINTS[object_type]["list"]

    all_results: List[Dict[str, Any]] = []
    after: Optional[str] = None
    call_count = 0

    # choose search vs list
    if filters:
        logging.info("Using search endpoint for %s", object_type)
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
                    "HubSpot %s search error %d: %s",
                    object_type, resp.status_code, resp.text
                )
                break

            data = resp.json()
            all_results.extend(data.get("results", []))
            after = data.get("paging", {}).get("next", {}).get("after")

            if not after or (limit and len(all_results) >= limit):
                break

    else:
        logging.info("Using list endpoint for %s", object_type)
        params = {"limit": min(limit or 100, 100), "properties": ",".join(properties)}
        while True:
            if after:
                params["after"] = after

            resp = requests.get(url_list, headers=HEADERS, params=params)
            call_count += 1

            if resp.status_code != 200:
                logging.error(
                    "HubSpot %s list error %d: %s",
                    object_type, resp.status_code, resp.text
                )
                break

            data = resp.json()
            all_results.extend(data.get("results", []))
            after = data.get("paging", {}).get("next", {}).get("after")

            if not after or (limit and len(all_results) >= limit):
                break

    logging.info(
        "✅ Retrieved %d %s in %d API calls",
        len(all_results), object_type, call_count
    )
    # enforce limit if set
    return (all_results[:limit] if limit else all_results, call_count)


# ✅ Test / Demo: Pull 10 companies and deals
if __name__ == "__main__":
    import json

    # Test fetching 10 deals with full properties and company associations
    deals = fetch_all_deals_with_company()

    logging.info(f"✅ Fetched {len(deals)} deals.\n")

    for i, deal in enumerate(deals):
        logging.info(f"--- Deal #{i+1} ---")
        logging.info("Properties:")
        logging.info(json.dumps(deal.get("properties", {}), indent=2))

        companies = deal.get("associations", {}).get("companies", {}).get("results", [])
        if companies:
            logging.info("Associated company ID(s):", [c["id"] for c in companies])
        else:
            logging.info("No associated company.")
        logging.info()