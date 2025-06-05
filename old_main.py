# main.py

import logging
from flask import Flask, request, jsonify
from dotenv import load_dotenv

# Make sure your `src/` directory is on the path
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from hubspot_pipeline.snapshot_runner import run_snapshot_and_process

# ────────────────────────────────────────────────────────────────────────────────
#  Configure Flask/Functions-Framework app
# ────────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)
load_dotenv()  # load local .env in dev

# ────────────────────────────────────────────────────────────────────────────────
#  Entry point for Cloud Function
# ────────────────────────────────────────────────────────────────────────────────
@app.route("/", methods=["GET", "POST"])
def hubspot_pipeline_handler():
    """
    HTTP trigger for the HubSpotPipeline Cloud Function.

    Query params (all optional):
      - full=true      → fetch unlimited records
      - limit=<n>      → fetch up to n records (ignored if full=true; default=100)
      - dry_run=true   → skip any BigQuery writes
      - test_mode=true → use test tables instead of production tables
      - triggered_by   → free-text tag for who/what triggered this run
    """
    logging.info("▶️ Function triggered. Parsing flags…")

    # Parse query parameters
    args = request.args or {}
    full_flag    = args.get("full", "").lower() == "true"
    dry_run      = args.get("dry_run", "").lower() == "true"
    test_mode    = args.get("test_mode", "").lower() == "true"
    triggered_by = args.get("triggered_by", "http_request")

    # Determine limit
    if full_flag:
        limit = None  # unlimited fetch
        logging.info("‣ full flag present; will fetch unlimited records.")
    else:
        try:
            limit = int(args.get("limit", "100"))
            logging.info(f"‣ full flag not present; using limit={limit}.")
        except ValueError:
            logging.warning("‣ Invalid `limit` param; defaulting to 100.")
            limit = 100

    # (Optional) Build filters for companies/deals if you support them via query params
    filters_company = None
    filters_deal    = None
    # Example: if you wanted ?company_filter=value, you’d parse it here
    # filters_company = [...]

    # Now call the runner
    try:
        run_snapshot_and_process(
            filters_company=filters_company,
            filters_deal=filters_deal,
            triggered_by=triggered_by,
            limit=limit,
            dry_run=dry_run,
            test_mode=test_mode,
        )
        return jsonify({"status": "success"}), 200

    except Exception as e:
        logging.error(f"❌ Pipeline failed: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


# ────────────────────────────────────────────────────────────────────────────────
#  If you run `python main.py` locally, start Flask for testing
# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    # When you run locally, you can do e.g. http://localhost:8080?limit=50&dry_run=true
    # http://localhost:8080?full=true&triggered_by=local_test
    app.run(debug=True, host="0.0.0.0", port=8080)
