#!/usr/bin/env python3
import os
import sys

# ───────────────────────────────────────────────────────────────────────────────
# 1) Add "src/" to sys.path so Python can find the hubspot_pipeline package
# ───────────────────────────────────────────────────────────────────────────────
# Determine the directory containing main.py
_APP_DIR = os.path.dirname(__file__)
# Prepend "src" (relative to that directory) onto sys.path
sys.path.insert(0, os.path.join(_APP_DIR, "src"))

# ───────────────────────────────────────────────────────────────────────────────
# 2) Configure logging immediately
# ───────────────────────────────────────────────────────────────────────────────
import logging
from flask import Request, jsonify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

# ───────────────────────────────────────────────────────────────────────────────
# 3) Now that src/ is on sys.path, these imports will work
# ───────────────────────────────────────────────────────────────────────────────
from hubspot_pipeline.snapshot_runner import run_snapshot_and_process

# ───────────────────────────────────────────────────────────────────────────────
# 4) Cloud Function entrypoint
# ───────────────────────────────────────────────────────────────────────────────
def hubspot_pipeline_handler(request: Request):
    logging.info("🏁 HubSpot Pipeline Function triggered.")
    try:
        run_snapshot_and_process()
        logging.info("✅ Pipeline completed successfully.")
        return jsonify({"status": "success"}), 200
    except Exception as e:
        logging.exception("❌ Pipeline failed with exception:")
        return jsonify({"status": "error", "message": str(e)}), 500

# ───────────────────────────────────────────────────────────────────────────────
# 5) Optional local invocation
# ───────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.info("Running pipeline locally (no HTTP).")
    run_snapshot_and_process()
    logging.info("Done.")
