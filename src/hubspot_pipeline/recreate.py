# src/hubspot_pipeline/recreate.py

import sys
import logging

from .schema import (
    SCHEMA_COMPANIES,
    SCHEMA_DEALS,
    SCHEMA_OWNERS,
    SCHEMA_STAGE_MAPPING,
    SCHEMA_DEAL_STAGE_REFERENCE,
    SCHEMA_PIPELINE_UNITS_SNAPSHOT,
    SCHEMA_SNAPSHOT_REGISTRY,
    SCHEMA_PIPELINE_SCORE_HISTORY
)
from .bigquery_utils import recreate_table, delete_all_tables_in_dataset
from .populate_stage_mapping import populate_stage_mapping
from .populate_deal_stage_reference import populate_deal_stage_reference
from .fetch_hubspot_data import fetch_owners
from .populate_owners import overwrite_owners_into_bigquery
from .process_snapshot import reprocess_all_score_summaries

# ────────────────────────────────────────────────────────────────────────
#   TABLE_SCHEMAS maps dataset‐table names to their schema definitions
# ────────────────────────────────────────────────────────────────────────
TABLE_SCHEMAS = {
    "hs_companies":               SCHEMA_COMPANIES,
    "hs_deals":                   SCHEMA_DEALS,
    "hs_owners":                  SCHEMA_OWNERS,
    "hs_stage_mapping":           SCHEMA_STAGE_MAPPING,
    "hs_deal_stage_reference":    SCHEMA_DEAL_STAGE_REFERENCE,
    "hs_pipeline_units_snapshot": SCHEMA_PIPELINE_UNITS_SNAPSHOT,
    "hs_snapshot_registry":       SCHEMA_SNAPSHOT_REGISTRY,
    "hs_pipeline_score_history":  SCHEMA_PIPELINE_SCORE_HISTORY
}

# ────────────────────────────────────────────────────────────────────────
#   REFERENCE_POPULATORS maps a subset of tables to their “populate” functions
# ────────────────────────────────────────────────────────────────────────
REFERENCE_POPULATORS = {
    "hs_owners":                 lambda: overwrite_owners_into_bigquery(fetch_owners() or []),
    "hs_stage_mapping":          populate_stage_mapping,
    "hs_deal_stage_reference":   populate_deal_stage_reference,
    "hs_pipeline_score_history": reprocess_all_score_summaries
}


def populate_table(table_name: str):
    """
    Populate a single reference table (if defined). Otherwise log a warning.
    """
    if table_name in REFERENCE_POPULATORS:
        logging.info(f"🚀 Starting population for table `{table_name}` …")
        try:
            REFERENCE_POPULATORS[table_name]()
            logging.info(f"✅ Finished population for `{table_name}`.")
        except Exception as e:
            logging.error(f"❌ Error populating `{table_name}`: {e}", exc_info=True)
    else:
        logging.info(f"⚠️ No populate logic defined for `{table_name}` — skipping.")


def recreate_all_snapshots():
    """
    Main entrypoint for “recreate & optionally populate”. Examines sys.argv:
      - <no args>          → prints usage
      - ["all"]            → delete all tables, recreate all
      - ["all", "populate"]→ delete all, recreate all, then populate reference tables
      - ["populate"]       → only populate all reference tables
      - ["<table>"]        → recreate only that table
      - ["<table>", "populate"] → recreate that table and then populate if it’s in REFERENCE_POPULATORS
    """
    logging.info("────────────────────────────────────────────────────────────")
    logging.info(f"🔹 Called recreate_all_snapshots() with args: {sys.argv[1:]}")
    logging.info("────────────────────────────────────────────────────────────")

    if len(sys.argv) < 2:
        logging.info("Usage:")
        logging.info("  python recreate.py <table_name>")
        logging.info("  python recreate.py <table_name> populate")
        logging.info("  python recreate.py all")
        logging.info("  python recreate.py all populate")
        logging.info("  python recreate.py populate")
        return

    table_name = sys.argv[1]

    # ─────────────────────────────────────────────────────────────────
    # Case 1: Recreate ALL tables (and optionally populate)
    # ─────────────────────────────────────────────────────────────────
    if table_name == "all":
        logging.info("🔹 User requested: Recreate ALL tables.")
        confirm = input("⚠️ This will DELETE and RECREATE ALL tables. Type YES to continue: ")
        if confirm.strip() != "YES":
            logging.info("❌ User aborted 'all' operation.")
            return

        logging.info("🗑️  Deleting all tables in dataset …")
        deleted = False
        try:
            deleted = delete_all_tables_in_dataset()
            logging.info("✅ Delete‐all_tables returned: %s", deleted)
        except Exception as e:
            logging.error(f"❌ Error during delete_all_tables_in_dataset(): {e}", exc_info=True)
            return

        if deleted:
            # Recreate each table, one by one
            for name, schema in TABLE_SCHEMAS.items():
                logging.info(f"🔄 Recreating table `{name}` …")
                try:
                    recreate_table(name, schema)
                    logging.info(f"✅ Successfully recreated `{name}`.")
                except Exception as e:
                    logging.error(f"❌ Error recreating `{name}`: {e}", exc_info=True)

            # If “populate” was also requested, populate reference tables
            if len(sys.argv) > 2 and sys.argv[2] == "populate":
                logging.info("🔹 Now populating reference tables …")
                for ref_name in REFERENCE_POPULATORS.keys():
                    populate_table(ref_name)
                logging.info("✅ Finished populating all reference tables.")

            logging.info("✅ Completed 'all' operation.")
        else:
            logging.error("❌ delete_all_tables_in_dataset() returned False; aborting.")
        return

    # ─────────────────────────────────────────────────────────────────
    # Case 2: “populate” (populate all reference tables only)
    # ─────────────────────────────────────────────────────────────────
    elif table_name == "populate":
        logging.info("🔹 User requested: Populate ALL reference tables (no recreation).")
        for ref_name in REFERENCE_POPULATORS.keys():
            populate_table(ref_name)
        logging.info("✅ Finished populating reference tables.")
        return

    # ─────────────────────────────────────────────────────────────────
    # Case 3: Recreate a single table AND populate it (if second arg is “populate”)
    # ─────────────────────────────────────────────────────────────────
    elif len(sys.argv) == 3 and sys.argv[2] == "populate":
        if table_name in TABLE_SCHEMAS:
            logging.info(f"🔹 User requested: Recreate and populate `{table_name}`.")
            try:
                recreate_table(table_name, TABLE_SCHEMAS[table_name])
                logging.info(f"✅ Successfully recreated `{table_name}`.")
            except Exception as e:
                logging.error(f"❌ Error recreating `{table_name}`: {e}", exc_info=True)
                return

            logging.info(f"🔹 Now populating `{table_name}` …")
            populate_table(table_name)
        else:
            logging.info(f"❌ Unknown table `{table_name}`; cannot recreate/populate.")
        return

    # ─────────────────────────────────────────────────────────────────
    # Case 4: Recreate a single table (no populate)
    # ─────────────────────────────────────────────────────────────────
    elif table_name in TABLE_SCHEMAS:
        logging.info(f"🔹 User requested: Recreate only `{table_name}`.")
        try:
            recreate_table(table_name, TABLE_SCHEMAS[table_name])
            logging.info(f"✅ Successfully recreated `{table_name}`.")
        except Exception as e:
            logging.error(f"❌ Error recreating `{table_name}`: {e}", exc_info=True)
        return

    # ─────────────────────────────────────────────────────────────────
    # Case 5: Unknown command/table
    # ─────────────────────────────────────────────────────────────────
    else:
        logging.info(f"❌ Unknown command or table: `{table_name}`")
        return

