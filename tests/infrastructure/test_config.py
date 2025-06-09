# ===============================================================================
# tests/infrastructure/test_config.py
# Shared configuration and utilities for GCP infrastructure tests
# ===============================================================================

import pytest
import subprocess
import json
import os
import sys
from pathlib import Path

# Configuration
PROJECT_ID = "hubspot-452402"
REGION = "europe-west1"

ENVIRONMENTS = {
    'dev': {
        'dataset': 'Hubspot_dev_ob',
        'topic': 'hubspot-events-dev',
        'service_account': 'hubspot-dev-ob@hubspot-452402.iam.gserviceaccount.com',
        'function_name': 'hubspot-ingest-dev'
    },
    'staging': {
        'dataset': 'Hubspot_staging',
        'topic': 'hubspot-events-staging', 
        'service_account': 'hubspot-staging@hubspot-452402.iam.gserviceaccount.com',
        'function_name': 'hubspot-ingest-staging'
    },
    'prod': {
        'dataset': 'Hubspot_prod',
        'topic': 'hubspot-events-prod',
        'service_account': 'hubspot-prod@hubspot-452402.iam.gserviceaccount.com',
        'function_name': 'hubspot-ingest-prod'
    }
}

def run_gcloud_command(cmd, capture_output=True, check=True):
    """Run gcloud command and return result"""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=capture_output, 
            text=True, 
            check=check
        )
        return result
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {cmd}")
        print(f"Exit code: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise

def get_current_project():
    """Get current gcloud project"""
    result = run_gcloud_command("gcloud config get-value project")
    return result.stdout.strip()

def get_current_account():
    """Get current gcloud account"""
    result = run_gcloud_command("gcloud config get-value account")
    return result.stdout.strip()