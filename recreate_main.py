#!/usr/bin/env python

"""
Interactive wrapper for recreate_all_snapshots(), with progress logging and a numbered table‐selection menu.

Usage:
  python recreate_main.py

Steps:
  1) Prints which BigQuery project/dataset and credentials are in use.
  2) Presents a menu of operations.
  3) If you choose to operate on a single table, shows a numbered list of TABLE_SCHEMAS keys and prompts for the number.
  4) Logs each major step and then calls recreate_all_snapshots() under the hood.
"""

import os
import sys
import logging

try:
    from hubspot_pipeline.recreate import recreate_all_snapshots, TABLE_SCHEMAS
    from hubspot_pipeline.config.config import BIGQUERY_PROJECT_ID, DATASET_ID as BIGQUERY_DATASET_ID
except ImportError:
    # If running directly from source (without pip‐install), uncomment and adjust the next two lines:
    # sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
    # from hubspot_pipeline.recreate import recreate_all_snapshots, TABLE_SCHEMAS
    raise

MENU = """
Please choose an operation by entering the corresponding number:

  1) Recreate a single table
  2) Recreate + populate a single table
  3) Recreate ALL tables
  4) Recreate ALL tables + populate reference tables
  5) Populate reference tables only
  6) Exit
"""


def display_env_info():
    """
    Print out which BigQuery project/dataset will be used, and which credentials are active.
    """
    print("────────────────────────────────────────────────────────────────")
    print("🚀  HUBSPOT PIPELINE RECREATE – ENVIRONMENT INFO")
    print("────────────────────────────────────────────────────────────────")
    print(f"• BigQuery Project ID:       {BIGQUERY_PROJECT_ID}")
    print(f"• BigQuery Dataset ID:       {BIGQUERY_DATASET_ID}")

    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path:
        print(f"• Using service account key: {cred_path}")
    else:
        print("• Using Application Default Credentials (e.g. `gcloud auth login`)")

    print("────────────────────────────────────────────────────────────────\n")


def prompt_menu() -> int:
    """
    Display the menu and return the integer choice (1–6).
    """
    while True:
        print(MENU)
        choice = input("Enter choice [1-6]: ").strip()
        if choice in {"1", "2", "3", "4", "5", "6"}:
            return int(choice)
        print("Invalid input. Please enter a number between 1 and 6.\n")


def prompt_single_table(needs_populate: bool) -> list[str]:
    """
    Show a numbered list of available tables (the keys of TABLE_SCHEMAS), prompt the user to choose one,
    and return [table_name] or [table_name, "populate"] depending on needs_populate.
    """
    tables = list(TABLE_SCHEMAS.keys())
    print("\nAvailable tables:")
    for idx, tbl in enumerate(tables, start=1):
        print(f"  {idx}) {tbl}")
    print()

    while True:
        sel = input(f"Enter the number of the table you want to {'recreate+populate' if needs_populate else 'recreate'}: ").strip()
        if sel.isdigit():
            num = int(sel)
            if 1 <= num <= len(tables):
                chosen = tables[num - 1]
                logging.info(f"✅ Selected table: {chosen}")
                if needs_populate:
                    return [chosen, "populate"]
                else:
                    return [chosen]
        print(f"Invalid input. Please enter a number between 1 and {len(tables)}.\n")


def main():
    # 1) Configure root logger so that INFO messages show up on the console
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s"
    )

    logging.info("🔹 Starting recreate_main.py")

    # 2) Show project/dataset and credential info
    display_env_info()

    # 3) Show menu and wait for user's choice
    choice = prompt_menu()

    # We'll build sys.argv so that recreate_all_snapshots() can parse it exactly as before
    script_name = sys.argv[0]

    if choice == 1:
        # Recreate a single table (no populate)
        logging.info("🔹 User chose: Recreate a single table")
        args = prompt_single_table(needs_populate=False)
        sys.argv = [script_name] + args
        logging.info(f"🔹 Invoking recreate_all_snapshots() with args: {sys.argv[1:]}")

    elif choice == 2:
        # Recreate + populate a single table
        logging.info("🔹 User chose: Recreate + populate a single table")
        args = prompt_single_table(needs_populate=True)
        sys.argv = [script_name] + args
        logging.info(f"🔹 Invoking recreate_all_snapshots() with args: {sys.argv[1:]}")

    elif choice == 3:
        # Recreate ALL tables (recreate.py will ask for YES confirmation)
        logging.info("🔹 User chose: Recreate ALL tables")
        sys.argv = [script_name, "all"]
        logging.info("🔹 Invoking recreate_all_snapshots() with args: ['all']")

    elif choice == 4:
        # Recreate ALL tables + populate reference tables
        logging.info("🔹 User chose: Recreate ALL tables + populate reference tables")
        sys.argv = [script_name, "all", "populate"]
        logging.info("🔹 Invoking recreate_all_snapshots() with args: ['all', 'populate']")

    elif choice == 5:
        # Populate reference tables only (no recreation)
        logging.info("🔹 User chose: Populate reference tables only")
        sys.argv = [script_name, "populate"]
        logging.info("🔹 Invoking recreate_all_snapshots() with args: ['populate']")

    else:  # choice == 6: Exit
        logging.info("🔹 User chose: Exit. Goodbye!")
        print("Goodbye!")
        return

    # 4) Finally, call the existing recreate_all_snapshots()
    recreate_all_snapshots()
    logging.info("🔹 Finished recreate_all_snapshots()")


if __name__ == "__main__":
    main()
