import requests
from config.config import HUBSPOT_API_KEY

def fetch_companies(filters=None, limit=None):
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
            print(f"❌ HubSpot API Error (companies): {response.text}")
            return []

        data = response.json()
        all_results.extend(data.get("results", []))

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after or (limit and len(all_results) >= limit):
            break

    print(f"✅ Retrieved {len(all_results)} companies in {call_count} API calls.")
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
            print(f"❌ HubSpot API Error (deals): {response.text}")
            return []

        data = response.json()
        all_results.extend(data.get("results", []))

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after or (limit and len(all_results) >= limit):
            break

    print(f"✅ Retrieved {len(all_results)} deals in {call_count} API calls.")
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
            print(f"❌ HubSpot API Error: {response.text}")
            break

        data = response.json()
        results = data.get("results", [])
        all_results.extend(results)

        after = data.get("paging", {}).get("next", {}).get("after")
        if not after or (limit and len(all_results) >= limit):
            break

    print(f"✅ Retrieved {len(all_results)} deals in {call_count} API calls.")
    return all_results[:limit] if limit else all_results


def fetch_owners():
    url = "https://api.hubapi.com/crm/v3/owners"
    headers = {"Authorization": f"Bearer {HUBSPOT_API_KEY}"}

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"❌ HubSpot Owner API Error: {response.text}")
        return []

    data = response.json()
    owners = data.get("results", [])
    print(f"✅ Retrieved {len(owners)} owners from HubSpot")
    return owners


# ✅ Test / Demo: Pull 10 companies and deals
if __name__ == "__main__":
    import json

    # Test fetching 10 deals with full properties and company associations
    deals = fetch_all_deals_with_company()

    print(f"✅ Fetched {len(deals)} deals.\n")

    for i, deal in enumerate(deals):
        print(f"--- Deal #{i+1} ---")
        print("Properties:")
        print(json.dumps(deal.get("properties", {}), indent=2))

        companies = deal.get("associations", {}).get("companies", {}).get("results", [])
        if companies:
            print("Associated company ID(s):", [c["id"] for c in companies])
        else:
            print("No associated company.")
        print()