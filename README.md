# HubSpot Pipeline

A Google Cloud Functions-based ETL pipeline that fetches data from HubSpot API, processes it through BigQuery, and calculates pipeline scores with complete environment isolation.

## 🏗️ Architecture

### **Two-Pipeline System:**
- **Ingest Pipeline**: Fetches HubSpot data → BigQuery snapshots → Pub/Sub events
- **Scoring Pipeline**: Processes snapshots → Pipeline scores → Score history

### **Event-Driven Design:**
- Ingest completion triggers scoring via Pub/Sub
- Environment-specific topics ensure isolation
- Registry system tracks all processing states

---

## 📁 Directory Structure

```plaintext
hubspot-pipeline/
├── src/
│   ├── hubspot_pipeline/
│   │   ├── hubspot_ingest/           # Ingest pipeline (HTTP triggered)
│   │   │   ├── config_loader.py      # Environment & secret management
│   │   │   ├── events.py             # Pub/Sub event publishing
│   │   │   ├── fetcher.py            # HubSpot API client
│   │   │   ├── main.py               # Ingest function entry point
│   │   │   ├── reference/            # Reference data (owners, stages)
│   │   │   ├── registry.py           # Snapshot tracking
│   │   │   ├── schema.yaml           # Data schema configuration
│   │   │   ├── store.py              # BigQuery operations
│   │   │   └── table_checker.py      # Pre-flight validation
│   │   ├── hubspot_scoring/          # Scoring pipeline (Pub/Sub triggered)
│   │   │   ├── config.py             # Scoring configuration
│   │   │   ├── main.py               # Scoring function entry point
│   │   │   ├── processor.py          # Score calculation logic
│   │   │   ├── registry.py           # Scoring tracking
│   │   │   └── stage_mapping.py      # Stage → score mapping
│   │   ├── excel_import/             # Excel data import utility
│   │   ├── bigquery_utils.py         # Smart retry & utilities
│   │   └── schema.py                 # BigQuery schema definitions
│   ├── ingest_main.py                # Ingest Cloud Function entry
│   ├── scoring_main.py               # Scoring Cloud Function entry
│   ├── main.py                       # Local testing & debugging
│   └── requirements.txt
├── deploy-ingest.sh                  # Interactive deployment script
├── test-ingest.sh                    # Enhanced testing script
└── README.md
```

---

## 🚀 Quick Start

### **Prerequisites**
- Python 3.12+
- `gcloud` CLI configured with project access
- `jq` (for enhanced test result parsing)

### **Installation**
```bash
git clone <repository-url>
cd hubspot-pipeline
python -m venv myenv
source myenv/bin/activate  # On Windows: myenv\Scripts\activate
pip install -r src/requirements.txt
```

### **Local Environment Setup**
```bash
# Copy and configure environment
cp .env.example .env
# Edit .env with your credentials:
# HUBSPOT_API_KEY=your-hubspot-key
# BIGQUERY_PROJECT_ID=your-project-id
# BIGQUERY_DATASET_ID=Hubspot_dev_ob
```

---

## 🛠️ Development & Testing

### **Local Testing & Debugging**
```bash
# Interactive testing and debugging tool
python src/main.py

# Menu-driven interface provides:
# - Dry run tests (no BigQuery writes)
# - Real tests with various data volumes
# - BigQuery table inspection
# - Dataset cleanup tools
# - Environment-aware safety checks
```

### **Cloud Function Testing**
```bash
# Enhanced testing script with result parsing
./test-ingest.sh

# Features:
# - Environment selection (dev/staging/prod)
# - Test type selection (ping, dry runs, real runs)
# - Dynamic log level control
# - Result parsing and scoring monitoring
# - BigQuery table status checking
# - Dataset cleanup capabilities
```

---

## ☁️ Deployment

### **Interactive Deployment**
```bash
# Deploy with interactive menus and safety checks
./deploy-ingest.sh

# Options:
# - Function selection (ingest/scoring/both)
# - Environment selection (dev/staging/prod)
# - Automatic prerequisite checking
# - Environment-specific confirmations
# - Post-deployment verification
```

### **Direct Deployment**
```bash
# Deploy specific function to specific environment
./deploy-ingest.sh ingest dev        # Deploy ingest to dev
./deploy-ingest.sh scoring staging   # Deploy scoring to staging
./deploy-ingest.sh both prod         # Deploy both to production
```

### **Environment Configuration**

| Environment | Ingest Function | Scoring Function | Dataset | Topic |
|-------------|----------------|------------------|---------|-------|
| **Dev** | `hubspot-ingest-dev` | `hubspot-scoring-dev` | `Hubspot_dev_ob` | `hubspot-events-dev` |
| **Staging** | `hubspot-ingest-staging` | `hubspot-scoring-staging` | `Hubspot_staging` | `hubspot-events-staging` |
| **Prod** | `hubspot-ingest-prod` | `hubspot-scoring-prod` | `Hubspot_prod` | `hubspot-events-prod` |

---

## 🔄 Pipeline Flow

### **Ingest Pipeline (HTTP Triggered)**
1. **HTTP Request** → `hubspot-ingest-{env}`
2. **Fetch Data** from HubSpot API (companies, deals)
3. **Update Reference Data** (owners, deal stages)
4. **Store Snapshots** in BigQuery
5. **Publish Event** to `hubspot-events-{env}`
6. **Register Completion** in snapshot registry

### **Scoring Pipeline (Pub/Sub Triggered)**
1. **Pub/Sub Event** → `hubspot-scoring-{env}`
2. **Populate Stage Mapping** (lifecycle → scores)
3. **Calculate Pipeline Units** (company/deal scores)
4. **Aggregate Score History** (by owner/stage)
5. **Register Completion** in snapshot registry

---

## 🧪 Testing Examples

### **Basic Testing**
```bash
# Health check
curl -X POST https://europe-west1-hubspot-452402.cloudfunctions.net/hubspot-ingest-dev \
  -H "Content-Type: application/json" \
  -d '{"limit": 5, "dry_run": true}'

# Small live test
curl -X POST https://europe-west1-hubspot-452402.cloudfunctions.net/hubspot-ingest-dev \
  -H "Content-Type: application/json" \
  -d '{"limit": 10, "dry_run": false}'
```

### **Using Test Script**
```bash
# Interactive testing with enhanced result parsing
./test-ingest.sh dev dry-small     # Safe dry run test
./test-ingest.sh dev real-tiny     # Small live test
./test-ingest.sh staging real-medium  # Medium staging test
```

---

## 🔧 Configuration & Secrets

### **Environment Variables**
- `HUBSPOT_API_KEY`: HubSpot private app token
- `BIGQUERY_PROJECT_ID`: GCP project ID
- `BIGQUERY_DATASET_ID`: BigQuery dataset (auto-detected by environment)
- `ENVIRONMENT`: Environment override (auto-detected from function name)
- `LOG_LEVEL`: Logging verbosity (DEBUG/INFO/WARN/ERROR)

### **Secret Management**
- **Local Development**: Uses `.env` file
- **Cloud Functions**: Uses Google Secret Manager
- **Auto-Detection**: Automatically detects GCP vs local environment

---

## 📊 Monitoring & Observability

### **Logging**
- **Environment-Aware**: Different log levels per environment
- **Structured Logging**: JSON format for Cloud Logging
- **Performance Metrics**: Timing, record counts, API rates
- **Error Context**: Detailed error information with troubleshooting hints

### **Registry Tracking**
All snapshots tracked in `hs_snapshot_registry` with:
- Snapshot timestamps and IDs
- Processing states (started → ingest_completed → scoring_completed)
- Record counts and processing notes
- Error tracking and failure states

### **Monitoring Commands**
```bash
# Monitor function logs
gcloud logging read 'resource.labels.function_name="hubspot-ingest-dev"' --limit=10

# Check recent snapshots
# Use main.py interactive tool → "View Recent Snapshots"

# Check BigQuery tables
./test-ingest.sh dev check-tables
```

---

## 🛡️ Security & Permissions

### **Service Account Configuration**
Each environment uses dedicated service accounts with minimal required permissions:

- **BigQuery**: `jobUser` + `dataEditor` roles
- **Pub/Sub**: `publisher` role on environment-specific topics
- **Cloud Run**: `invoker` role for function execution

### **Environment Isolation**
- **Separate Topics**: Each environment publishes to its own Pub/Sub topic
- **Dedicated Datasets**: Each environment writes to separate BigQuery datasets  
- **Service Account Separation**: Each environment uses its own service account
- **No Cross-Environment Triggers**: Dev events don't trigger staging/prod functions

---

## 🔄 Data Flow & Schema

### **Core Tables**
- `hs_companies`: Company snapshots with lifecycle stages
- `hs_deals`: Deal snapshots with stages and amounts
- `hs_owners`: Sales rep reference data
- `hs_deal_stage_reference`: Pipeline stage definitions
- `hs_snapshot_registry`: Processing state tracking

### **Scoring Tables**
- `hs_stage_mapping`: Lifecycle stage → score mapping
- `hs_pipeline_units_snapshot`: Individual company/deal scores
- `hs_pipeline_score_history`: Aggregated scores by owner/stage

---

## 🚨 Troubleshooting

### **Common Issues**

#### **403 Authentication Errors**
```bash
# Check service account permissions
gcloud projects get-iam-policy hubspot-452402 --flatten="bindings[].members" --filter="bindings.members:hubspot-dev-ob@hubspot-452402.iam.gserviceaccount.com"

# Grant missing permissions
gcloud projects add-iam-policy-binding hubspot-452402 --member="serviceAccount:SERVICE_ACCOUNT" --role="ROLE"
```

#### **Missing Tables**
```bash
# Check table status
./test-ingest.sh dev check-tables

# Tables are auto-created during first run
# Pre-flight checks validate table readiness
```

#### **Cross-Environment Issues**
```bash
# Verify environment-specific topics
gcloud eventarc triggers list --location=europe-west1

# Should show separate topics per environment
```

### **Getting Help**
- **Interactive Testing**: Use `python src/main.py` for guided debugging
- **Enhanced Test Script**: Use `./test-ingest.sh` for comprehensive testing
- **Log Analysis**: Check Cloud Logging for detailed error context

---

## 📚 Development Guidelines

### **Code Structure**
- **Modular Design**: Separate concerns (fetch, store, score, events)
- **Environment Awareness**: Auto-detect and adapt to deployment context
- **Error Handling**: Comprehensive error handling with actionable messages
- **Testing First**: Include dry run modes and extensive testing utilities

### **Deployment Best Practices**
- **Environment Isolation**: Always use environment-specific resources
- **Safety Checks**: Multiple confirmation levels for production
- **Incremental Testing**: Start with dry runs, progress to small live tests
- **Monitoring**: Verify each step of deployment and testing

---

## ⚖️ License

This project is MIT-licensed. See `LICENSE` for details.