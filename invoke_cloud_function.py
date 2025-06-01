#!/usr/bin/env python3
"""
invoke_cloud_function.py

A helper that “hits” your locally‐running Function Framework (port 8080) 
with the same URL params you’d pass in production. By default it will
trigger a “full run” (i.e. unlimited fetch). You can also pass
`--dry-run` or `--test-mode` if you want to skip writes or point at
test tables.

Usage (from your project root):
  1) Start your Functions Framework in another terminal:
       cd /path/to/hubspot_pipeline
       source myenv/bin/activate
       functions-framework --target=hubspot_pipeline_handler --debug --port=8080

  2) In this terminal, run:
       python invoke_cloud_function.py           # full run (no limit)
       python invoke_cloud_function.py --limit 50 # run but limit=50
       python invoke_cloud_function.py --dry-run   # fetch only, no writes
       python invoke_cloud_function.py --test-mode # use test tables
       python invoke_cloud_function.py --help      # show options

Note: this script assumes your CF handler reads flags from request.args:
      - `full=true` means unlimited fetch
      - `limit=<n>` to override default limit
      - `dry_run=true` to skip BigQuery writes
      - `test_mode=true` to point at dev/staging tables instead of prod
"""

import argparse
import requests
import sys

# Default URL for local Functions Framework
LOCAL_FF_URL = "http://127.0.0.1:8080/"

def build_query_params(full: bool, limit: int | None, dry_run: bool, test_mode: bool) -> dict:
    params = {}
    if full:
        params["full"] = "true"
    elif limit is not None:
        params["limit"] = str(limit)
    if dry_run:
        params["dry_run"] = "true"
    if test_mode:
        params["test_mode"] = "true"
    return params

def main():
    parser = argparse.ArgumentParser(
        description="Invoke the locally‐running Cloud Function (port 8080) as if triggered remotely."
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Pass `?full=true` so that the function fetches unlimited records (no limit)."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Pass `?limit=<n>` to restrict fetch to <n> records. Ignored if `--full` is used."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Pass `?dry_run=true` so the function does not write to BigQuery."
    )
    parser.add_argument(
        "--test-mode",
        action="store_true",
        help="Pass `?test_mode=true` so the function writes only to test tables."
    )
    parser.add_argument(
        "--url",
        type=str,
        default=LOCAL_FF_URL,
        help="Base URL of the locally‐running Functions Framework (default: %(default)s)."
    )

    args = parser.parse_args()

    # If the user didn’t specify either --full or --limit, assume default limit=100
    if not args.full and args.limit is None:
        # We explicitly set ?limit=100 (same as prod default)
        args.limit = 100

    params = build_query_params(
        full=args.full,
        limit=args.limit,
        dry_run=args.dry_run,
        test_mode=args.test_mode,
    )

    url = args.url.rstrip("/")  # ensure no trailing slash
    print(f"→ Invoking local function at {url} with params {params} …")
    try:
        resp = requests.get(url, params=params, timeout=120)
    except requests.exceptions.RequestException as e:
        print(f"⚠️  HTTP request failed: {e}")
        sys.exit(1)

    print(f"\n← Status code: {resp.status_code}\n")
    print("← Response body:\n")
    print(resp.text)


if __name__ == "__main__":
    main()
