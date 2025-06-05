import sys
import argparse
from flask import Request
from werkzeug.test import EnvironBuilder
from werkzeug.wrappers import Request as WerkzeugRequest
from src.main import main as cloud_main

def run_as_flask():
    builder = EnvironBuilder(method='POST', json={"local": True})
    env = builder.get_environ()
    request = Request(WerkzeugRequest(env))
    response = cloud_main(request)
    print("📤 Response:")
    print(response[0])

def run_as_cli():
    from src.main import init_env
    print("🚀 Running in CLI mode")
    init_env()
    print("✅ Environment initialized from Secret Manager")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["flask", "cli"], default="flask", help="Execution mode: flask (default) or cli")
    args = parser.parse_args()
""" 
    if args.mode == "flask":
        run_as_flask()
    else:
        run_as_cli()
"""
run_as_cli()

