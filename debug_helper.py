#!/usr/bin/env python3
"""
debug_helper.py

Interactive menu for testing HubSpotPipeline components; correctly unpacks
fetch_* return values that might be (results, call_count) tuples.

Usage:
    cd /path/to/hubspot_pipeline
    python debug_helper.py
"""

import os
import sys
import logging
import json
import time
from dotenv import load_dotenv

# Ensure that `src/` is on the import path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

# ──────────────────────────────────────────────────────────────────────────────
#  Configure logging
# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)-8s %(message)s"
)

# ──────────────────────────────────────────────────────────────────────────────
#  Load environment variables from .env, if present
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()

# ──────────────────────────────────────────────────────────────────────────────
#  Import the pieces we want to test
# ──────────────────────────────────────────────────────────────────────────────
from hubspot_pipeline.fetch_hubspot_data import (
    fetch_companies,
    fetch_all_deals_with_company,
    fetch_owners,
)
from hubspot_pipeline.bigquery_utils import (
    insert_companies_into_bigquery,
    insert_deals_into_bigquery,
)
from hubspot_pipeline.snapshot_runner import run_snapshot_and_process
from hubspot_pipeline.config.config import (
    BIGQUERY_PROJECT_ID,
    DATASET_ID,
    BQ_COMPANY_TABLE,
    BQ_DEALS_TABLE,
    BQ_PIPELINE_UNITS_TABLE,
    BQ_PIPELINE_SCORE_HISTORY_TABLE,
    BQ_SNAPSHOT_REGISTRY_TABLE,
)


def _unwrap_fetch_result(maybe_tuple):
    """
    If maybe_tuple is a (results, call_count) pair, return results.
    Otherwise, return maybe_tuple itself.
    """
    if isinstance(maybe_tuple, tuple) and len(maybe_tuple) == 2 and isinstance(maybe_tuple[1], int):
        return maybe_tuple[0]
    return maybe_tuple


def inspect_companies(limit=5):
    """
    Fetch a small number of companies and pretty‐print the first company dict.
    Unpacks (results, call_count) if necessary.
    """
    logging.info(f"⏳ Calling fetch_companies(limit={limit}) …")
    raw = fetch_companies(limit=limit)
    companies = _unwrap_fetch_result(raw)
    count = len(companies) if isinstance(companies, list) else 0
    logging.info(f"✅ fetch_companies returned {count} company items.")
    if companies and isinstance(companies, list):
        logging.info("🔍 Inspecting the first company object:")
        print(json.dumps(companies[0], indent=2))
    else:
        logging.warning("⚠️ fetch_companies() returned no list of company dicts.")


def inspect_deals(limit=5):
    """
    Fetch a small number of deals and pretty‐print the first deal dict.
    Unpacks (results, call_count) if necessary.
    """
    logging.info(f"⏳ Calling fetch_all_deals_with_company(limit={limit}) …")
    raw = fetch_all_deals_with_company(limit=limit)
    deals = _unwrap_fetch_result(raw)
    if not isinstance(deals, list):
        logging.warning(f"⚠️ Expected a list from fetch_all_deals_with_company, got {type(deals)}")
        return
    logging.info(f"✅ fetch_all_deals_with_company returned {len(deals)} deals.")
    if deals:
        logging.info("🔍 Inspecting the first deal object:")
        print(json.dumps(deals[0], indent=2))
    else:
        logging.warning("⚠️ fetch_all_deals_with_company() returned an empty list.")


def inspect_owners():
    """
    Fetch and print the first owner to verify raw structure.
    """
    logging.info("⏳ Calling fetch_owners() …")
    owners = fetch_owners()
    if not isinstance(owners, list):
        logging.warning(f"⚠️ Expected a list from fetch_owners, got {type(owners)}")
        return
    logging.info(f"✅ fetch_owners returned {len(owners)} owners.")
    if owners:
        logging.info("🔍 Inspecting the first owner object:")
        print(json.dumps(owners[0], indent=2))
    else:
        logging.warning("⚠️ fetch_owners() returned an empty list.")


def try_insert_companies(limit=3):
    """
    Fetch a few companies and pass them to insert_companies_into_bigquery()
    using snapshot_id="DEBUG_SNAPSHOT". Unpacks the fetch result properly.
    """
    logging.info(f"⏳ Fetching {limit} companies for insert test…")
    raw = fetch_companies(limit=limit)
    companies = _unwrap_fetch_result(raw)
    if not isinstance(companies, list):
        logging.error(f"❌ Expected a list of companies, got {type(companies)}. Aborting.")
        return

    logging.info(f"✅ Retrieved {len(companies)} companies.")
    if not companies:
        logging.warning("⚠️ No companies to insert; skipping insert test.")
        return

    first = companies[0]
    logging.debug(f"Type of first company: {type(first)}. Keys: {list(first.keys())}")
    snapshot_id = "DEBUG_SNAPSHOT"
    logging.info(f"⏳ Attempting insert_companies_into_bigquery() for snapshot {snapshot_id} …")
    try:
        insert_companies_into_bigquery(companies, snapshot_id)
        logging.info("✅ insert_companies_into_bigquery() succeeded.")
    except Exception as e:
        logging.error(f"❌ insert_companies_into_bigquery() raised an exception: {e}", exc_info=True)


def try_insert_deals(limit=3):
    """
    Fetch a few deals and pass them to insert_deals_into_bigquery(),
    using snapshot_id="DEBUG_SNAPSHOT". Unpacks the fetch result properly.
    """
    logging.info(f"⏳ Fetching {limit} deals for insert test…")
    raw = fetch_all_deals_with_company(limit=limit)
    deals = _unwrap_fetch_result(raw)
    if not isinstance(deals, list):
        logging.error(f"❌ Expected a list of deals, got {type(deals)}. Aborting.")
        return

    logging.info(f"✅ Retrieved {len(deals)} deals.")
    if not deals:
        logging.warning("⚠️ No deals to insert; skipping insert test.")
        return

    first = deals[0]
    logging.debug(f"Type of first deal: {type(first)}. Keys: {list(first.keys())}")
    snapshot_id = "DEBUG_SNAPSHOT"
    logging.info(f"⏳ Attempting insert_deals_into_bigquery() for snapshot {snapshot_id} …")
    try:
        insert_deals_into_bigquery(deals, snapshot_id)
        logging.info("✅ insert_deals_into_bigquery() succeeded.")
    except Exception as e:
        logging.error(f"❌ insert_deals_into_bigquery() raised an exception: {e}", exc_info=True)


def dry_run_snapshot(limit=5):
    """
    Run a small snapshot locally in dry‐run mode (no BigQuery writes).
    """
    logging.info(f"⏳ Starting dry‐run snapshot with limit={limit} …")
    try:
        run_snapshot_and_process(limit=limit, dry_run=True)
        logging.info("✅ Dry‐run snapshot completed without errors.")
    except Exception as e:
        logging.error(f"❌ Dry‐run snapshot raised an exception: {e}", exc_info=True)


def real_run_snapshot(limit=5):
    """
    Run a real snapshot against your dev tables. WARNING: this writes data!
    """
    logging.info(f"⏳ Starting real snapshot with limit={limit} …")
    try:
        run_snapshot_and_process(limit=limit, dry_run=False)
        logging.info("✅ Real snapshot completed without errors.")
    except Exception as e:
        logging.error(f"❌ Real snapshot raised an exception: {e}", exc_info=True)


def menu():
    """
    Display an interactive menu so you can choose which helper to run.
    """
    options = {
        "1": ("Inspect a few companies", inspect_companies),
        "2": ("Inspect a few deals", inspect_deals),
        "3": ("Inspect owners", inspect_owners),
        "4": ("Test inserting companies into BigQuery", try_insert_companies),
        "5": ("Test inserting deals into BigQuery", try_insert_deals),
        "6": ("Dry‐run a small snapshot (no writes)", dry_run_snapshot),
        "7": ("Real‐run a small snapshot (writes to dev tables)", real_run_snapshot),
        "8": ("Exit", None),
    }

    while True:
        print("\n───────────────────────────────────────────────────────────")
        print(" 🐞  DEBUG HELPER MENU – HUBSPOT PIPELINE")
        print("───────────────────────────────────────────────────────────")
        for key, (desc, _) in options.items():
            print(f"  {key}) {desc}")
        choice = input("\nEnter choice [1–8]: ").strip()

        if choice not in options:
            print("Invalid choice. Please enter a number between 1 and 8.\n")
            continue

        if choice == "8":
            print("Exiting debug helper. Goodbye!\n")
            break

        desc, func = options[choice]
        print(f"\n🔹 You chose: {desc}\n")

        # Prompt for a limit argument if needed
        if func in (inspect_companies, inspect_deals):
            lim = input("Enter limit (default 5): ").strip()
            try:
                lim_val = int(lim) if lim else 5
            except ValueError:
                print("Invalid number; using default = 5.")
                lim_val = 5
            func(limit=lim_val)

        elif func in (try_insert_companies, try_insert_deals):
            lim = input("Enter limit (default 3): ").strip()
            try:
                lim_val = int(lim) if lim else 3
            except ValueError:
                print("Invalid number; using default = 3.")
                lim_val = 3
            func(limit=lim_val)

        elif func in (dry_run_snapshot, real_run_snapshot):
            lim = input("Enter limit (default 5): ").strip()
            try:
                lim_val = int(lim) if lim else 5
            except ValueError:
                print("Invalid number; using default = 5.")
                lim_val = 5
            func(limit=lim_val)

        else:
            func()

        time.sleep(0.5)  # small pause before re-displaying the menu


if __name__ == "__main__":
    menu()
