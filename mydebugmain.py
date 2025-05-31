#!/usr/bin/env python3
import logging
import sys

# -----------------------------------------------------
# 1) Force‐reset any existing handlers, then configure
# -----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,    # stderr is fine in a terminal or Cloud run
    force=True            # Python 3.8+ only; forces reconfiguration
)


from hubspot_pipeline.snapshot_runner import run_snapshot_and_process

if __name__ == "__main__":
    logging.info("Starting snapshot pipeline…")
    run_snapshot_and_process(limit=10)
    logging.info("Snapshot pipeline finished.")