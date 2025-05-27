from src.fetch_hubspot_data import fetch_companies, fetch_owners, fetch_deals, fetch_all_deals_with_company
from src.bigquery_utils import (
    insert_companies_into_bigquery,
    overwrite_owners_into_bigquery,
    insert_deals_into_bigquery
)
# test
def main():
    companies = None
    owners = None
    deals = None
    # --- Companies ---
    companies = fetch_companies(limit=10)
    if companies:
        insert_companies_into_bigquery(companies, snapshot_id="manual-test")
    else:
        print("⚠️ No company data received from HubSpot.")

    # --- Owners ---
    owners = fetch_owners()
    if owners:
        overwrite_owners_into_bigquery(owners)
    else:
        print("⚠️ No owner data received from HubSpot.")

    # --- Deals ---
    deal_filters = [
        {"propertyName": "hubspot_owner_id", "operator": "IN", "values": [
            "677066168", "1596892909", "1596892910", "1596892911"
        ]},
        {"propertyName": "dealtype", "operator": "IN", "values": [
            "newbusiness", "existingbusiness"
        ]},
        {"propertyName": "dealstage", "operator": "IN", "values": [
            "qualifiedtobuy", "presentationscheduled", "decisionmakerboughtin",
            "contractsent", "contractsigned", "appointmentscheduled",
            "closedlost", "closedwon"
        ]}
    ]
    deals = fetch_deals(filters=deal_filters, limit=10)
    if deals:
        insert_deals_into_bigquery(deals, snapshot_id="manual-test")
    else:
        print("⚠️ No deal data received from HubSpot.")

    # --- Deals ---
    deals = fetch_all_deals_with_company(limit=10)

    if deals:
        insert_deals_into_bigquery(deals, "test")
    else:
        print("⚠️ No deal data received from HubSpot.")


if __name__ == "__main__":
    main()
