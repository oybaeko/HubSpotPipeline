import sys
from src.schema import (
    SCHEMA_COMPANIES,
    SCHEMA_DEALS,
    SCHEMA_OWNERS,
    SCHEMA_STAGE_MAPPING,
    SCHEMA_DEAL_STAGE_REFERENCE,
    SCHEMA_PIPELINE_UNITS_SNAPSHOT,
    SCHEMA_SNAPSHOT_REGISTRY,
    SCHEMA_PIPELINE_SCORE_HISTORY
)
from src.bigquery_utils import recreate_table, delete_all_tables_in_dataset
from src.populate_stage_mapping import populate_stage_mapping
from src.populate_deal_stage_reference import populate_deal_stage_reference
from src.fetch_hubspot_data import fetch_owners
from src.bigquery_utils import overwrite_owners_into_bigquery
from src.process_snapshot import reprocess_all_score_summaries

TABLE_SCHEMAS = {
    "hs_companies": SCHEMA_COMPANIES,
    "hs_deals": SCHEMA_DEALS,
    "hs_owners": SCHEMA_OWNERS,
    "hs_stage_mapping": SCHEMA_STAGE_MAPPING,
    "hs_deal_stage_reference": SCHEMA_DEAL_STAGE_REFERENCE,
    "hs_pipeline_units_snapshot": SCHEMA_PIPELINE_UNITS_SNAPSHOT,
    "hs_snapshot_registry": SCHEMA_SNAPSHOT_REGISTRY,
    "hs_pipeline_score_history": SCHEMA_PIPELINE_SCORE_HISTORY

}

REFERENCE_POPULATORS = {
    "hs_owners": lambda: overwrite_owners_into_bigquery(fetch_owners() or []),
    "hs_stage_mapping": populate_stage_mapping,
    "hs_deal_stage_reference": populate_deal_stage_reference,
    "hs_pipeline_score_history": reprocess_all_score_summaries
}

def populate_table(table_name):
    if table_name in REFERENCE_POPULATORS:
        print(f"🚀 Populating {table_name} ...")
        REFERENCE_POPULATORS[table_name]()
        print(f"✅ Populated {table_name}")
    else:
        print(f"⚠️ No populate logic defined for {table_name} — skipping.")

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python recreate.py <table_name>")
        print("  python recreate.py <table_name> populate")
        print("  python recreate.py all")
        print("  python recreate.py all populate")
        print("  python recreate.py populate")
        return

    table_name = sys.argv[1]

    # Recreate ALL (and optionally populate)
    if table_name == "all":
        confirm = input("⚠️ This will DELETE and RECREATE ALL tables. Type YES to continue: ")
        if confirm.strip() != "YES":
            print("Aborted.")
            return

        if delete_all_tables_in_dataset():
            for name, schema in TABLE_SCHEMAS.items():
                print(f"🔄 Recreating {name} ...")
                recreate_table(name, schema)

            if len(sys.argv) > 2 and sys.argv[2] == "populate":
                for name in REFERENCE_POPULATORS:
                    populate_table(name)

            print("✅ All operations completed.")
        return

    # Populate all reference tables
    elif table_name == "populate":
        for name in REFERENCE_POPULATORS:
            populate_table(name)
        print("✅ All reference tables populated.")
        return

    # Recreate and populate specific table
    elif len(sys.argv) == 3 and sys.argv[2] == "populate":
        if table_name in TABLE_SCHEMAS:
            recreate_table(table_name, TABLE_SCHEMAS[table_name])
            populate_table(table_name)
        else:
            print(f"❌ Unknown table: {table_name}")
        return

    # Recreate specific table only
    elif table_name in TABLE_SCHEMAS:
        recreate_table(table_name, TABLE_SCHEMAS[table_name])
        return

    print(f"❌ Unknown table: {table_name}")

if __name__ == "__main__":
    main()
