# HubSpotPipeline

A Google Cloud Functions–based ETL pipeline that:

* Fetches data from the HubSpot API
* Writes raw snapshots to BigQuery weekly
* Processes snapshots into scored metrics and summary tables

---

## 📁 Directory Structure

```plaintext
hubspot-pipeline/
├── config/                      # Typed configuration loader
│   └── config.py                # Environment and secret definitions
├── src/                         # Application code (Python package)
│   ├── hubspot_pipeline/        # Main package
│   │   ├── __init__.py
│   │   ├── bigquery_utils.py    # BigQuery helper functions
│   │   ├── fetch_hubspot_data.py# HubSpot API client
│   │   ├── populate_deal_stage_reference.py
│   │   ├── populate_stage_mapping.py
│   │   ├── process_snapshot.py  # Snapshot scoring logic
│   │   ├── recreate.py          # Reprocess all snapshots
│   │   ├── schema.py            # BigQuery schema definitions
│   │   └── snapshot_runner.py   # Weekly processing orchestrator
│   └── main.py                  # Cloud Function HTTP handler
├── tests/                       # Unit & integration tests
├── .env                         # Local secrets (gitignored)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 🔧 Prerequisites

* Python 3.10+
* `gcloud` CLI configured with access to your GCP project
* A BigQuery dataset for raw snapshots and processed tables
* HubSpot API credentials with read permissions

---

## 🚀 Installation

1. Clone the repo:

   ```bash
   git clone https://github.com/your-org/hubspot-pipeline.git
   cd hubspot-pipeline
   ```
2. Install dependencies (recommended in a virtualenv):

   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. Install package in editable mode:

   ```bash
   pip install -e src/
   ```

---

## 🔑 Configuration & Secrets

1. Copy and edit `.env`:

   ```env
   HUBSPOT_API_KEY=your-dev-key
   ENV=local
   BQ_PROJECT=your-gcp-project-id
   BQ_DATASET=hubspot_snapshots
   ```
2. Local loader in `config/config.py` will validate presence and types of these vars.
3. **Do not** commit `.env` to git. Use Google Secret Manager for production.

---

## 🛠️ Local Development

* Run the Functions Framework to emulate Cloud Functions locally:

  ```bash
  pip install functions-framework
  export FLASK_ENV=development
  functions-framework --target=hubspot_pipeline_handler --source=src/main.py
  ```
* Invoke via HTTP:

  ```bash
  curl localhost:8080
  ```
* Use `limit` or `test=True` flags in handlers to fetch a small sample (e.g., `limit=10`).
* Debug by setting `DEBUG=True` in your environment.

---

## 🧪 Testing

* Tests live in `tests/` alongside `src/`.
* Run all tests:

  ```bash
  pytest --cov=hubspot_pipeline
  ```
* Unit tests should mock HubSpot and BigQuery clients (e.g., via `pytest-mock`).
* Integration tests can point at a local BigQuery emulator or a dedicated test dataset.

---

## ☁️ Deployment (Cloud Functions)

1. Ensure you’re in project directory:

   ```bash
   cd hubspot-pipeline
   ```
2. Deploy:

   ```bash
   gcloud functions deploy hubspot_pipeline_handler \
     --runtime python310 \
     --trigger-http \
     --source src/ \
     --entry-point hubspot_pipeline_handler \
     --set-env-vars HUBSPOT_API_KEY=$HUBSPOT_API_KEY,ENV=prod,BQ_PROJECT=$BQ_PROJECT,BQ_DATASET=$BQ_DATASET
   ```
3. Verify correct dataset: use `ENV` to switch between test and prod datasets.

---

## 🔄 CI/CD (GitHub Actions)

* **On push to `main`**: lint, tests, coverage report
* **On tag/release**: deploy to production function

```yaml
# .github/workflows/ci.yml
name: CI
on:
  push:
    branches: [main]
  pull_request: {}
jobs:
  build-and-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with: python-version: '3.10'
      - run: pip install -r requirements.txt
      - run: pip install -e src/
      - run: pytest --cov=hubspot_pipeline
```

---

## ⚙️ Reliability & Observability

* **Retry logic**: wrap BigQuery reads in retries with exponential back-off (e.g., 3 attempts at 2s, 4s, 8s).
* **Structured logging**: use `logging` module with JSON output for easy querying in Cloud Logging.
* **Alerts**: configure an uptime check on your HTTP endpoint and alert on errors or latency.

---

## 📚 Contributing

1. Fork the repo and create a feature branch.
2. Write tests for new features or bug fixes.
3. Open a PR with clear description and link to any relevant issue.
4. Ensure CI passes before merging.

---

## ⚖️ License

This project is MIT‑licensed. See `LICENSE` for details.
