# HubSpot Pipeline - Ingest & Scoring System

A production-ready Cloud Functions pipeline for ingesting HubSpot data and running scoring calculations on Google Cloud Platform.

## 🏗️ Architecture Overview

```
HubSpot API → Ingest Function → BigQuery → Pub/Sub → Scoring Function → BigQuery
```

### Components
- **Ingest Function**: HTTP-triggered Cloud Function that fetches data from HubSpot API
- **Scoring Function**: Pub/Sub-triggered Cloud Function that processes snapshots and calculates scores
- **BigQuery**: Data warehouse for storing companies, deals, owners, and scoring results
- **Pub/Sub**: Event system for triggering scoring pipeline after ingest completion

## 📁 Project Structure

```
├── deploy.sh                     # Deployment script for all environments
├── test-ingest.sh                # Cloud Function testing script
├── main.py                       # Local testing and development interface
├── src/
│   ├── ingest_main.py            # Ingest Cloud Function entry point
│   ├── scoring_main.py           # Scoring Cloud Function entry point
│   ├── requirements.txt          # Python dependencies
│   ├── hubspot_pipeline/         # Core production code
│   │   ├── hubspot_ingest/       # Ingest pipeline modules
│   │   │   ├── config_loader.py  # Environment & configuration management
│   │   │   ├── main.py           # Main ingest orchestration
│   │   │   ├── fetcher.py        # HubSpot API data fetching
│   │   │   ├── store.py          # BigQuery data storage
│   │   │   ├── events.py         # Pub/Sub event publishing
│   │   │   ├── registry.py       # Snapshot tracking and registry
│   │   │   ├── schema.yaml       # Data schema configuration
│   │   │   └── reference/        # Reference data management
│   │   ├── hubspot_scoring/      # Scoring pipeline modules
│   │   │   ├── config.py         # Scoring configuration management
│   │   │   ├── main.py           # Main scoring orchestration
│   │   │   ├── processor.py      # Score calculation engine
│   │   │   ├── stage_mapping.py  # Lifecycle stage scoring rules
│   │   │   └── registry.py       # Scoring completion tracking
│   │   ├── bigquery_utils.py     # Smart retry BigQuery utilities
│   │   └── schema.py             # Database schema definitions
│   ├── excel_import/             # Local Excel data import (optional)
│   └── tests/                    # 🆕 Pytest-based testing framework
│       ├── conftest.py           # Pytest configuration & fixtures
│       ├── pytest.ini           # Pytest settings
│       ├── test_infrastructure.py # Infrastructure connectivity tests
│       ├── test_database_ops.py  # Database operation tests
│       ├── test_events.py        # Event system tests
│       ├── test_logging.py       # Logging system tests
│       ├── fixtures/             # Test fixtures and utilities
│       │   ├── test_session.py   # Test session management
│       │   ├── bigquery_fixtures.py # BigQuery test fixtures
│       │   └── pubsub_fixtures.py   # Pub/Sub test fixtures
│       └── markers/              # Custom pytest markers
│           └── production_safe.py # Production-safe test markers
└── .github/workflows/            # 🆕 CI/CD automation (planned)
    └── test-and-deploy.yml       # Automated testing and deployment
```

## 🚀 Deployment

### Prerequisites
- Google Cloud SDK installed and authenticated
- Project ID: `hubspot-452402`
- Required APIs enabled: Cloud Functions, BigQuery, Pub/Sub, Secret Manager

### Deploy Functions
```bash
# Deploy ingest function
./deploy.sh ingest [dev|staging|prod]

# Deploy scoring function  
./deploy.sh scoring [dev|staging|prod]

# Deploy both functions
./deploy.sh both [dev|staging|prod]

# Interactive mode
./deploy.sh
```

### Environment Configuration
- **Development**: `hubspot-ingest-dev`, dataset: `Hubspot_dev_ob`
- **Staging**: `hubspot-ingest-staging`, dataset: `Hubspot_staging`  
- **Production**: `hubspot-ingest-prod`, dataset: `Hubspot_prod`

## 🧪 Testing

### Cloud Function Testing
```bash
# Test deployed functions with comprehensive test suite
./test-ingest.sh [dev|staging|prod]

# Available test types:
# - ping: Health check
# - dry-*: Tests without BigQuery writes
# - real-*: Tests with BigQuery writes
# - custom: Custom JSON payload
```

### 🆕 Production-Safe Testing (NEW)

#### Built-in Function Testing
Both Cloud Functions now include pytest-based testing capabilities:

```bash
# Test infrastructure (production-safe)
curl -X POST https://...hubspot-ingest-prod \
  -H "Content-Type: application/json" \
  -d '{"mode": "test", "test_type": "infrastructure"}'

# Test database operations (safe)  
curl -X POST https://...hubspot-ingest-prod \
  -d '{"mode": "test", "test_type": "database"}'

# Test event system
curl -X POST https://...hubspot-ingest-prod \
  -d '{"mode": "test", "test_type": "events"}'
```

#### Local Development Testing
```bash
cd src/
# Run all production-safe tests
pytest tests/ -m "production_safe"

# Run infrastructure tests only
pytest tests/ -m "infrastructure"

# Run with specific environment context
pytest tests/ --environment=dev -m "database"

# Generate JSON report for CI/CD
pytest tests/ --json-report --json-report-file=results.json
```

#### Test Categories & Markers
- `@pytest.mark.infrastructure` - Connectivity & permissions tests
- `@pytest.mark.database` - Database operation tests  
- `@pytest.mark.events` - Event system tests
- `@pytest.mark.logging` - Logging system tests
- `@pytest.mark.production_safe` - Safe to run in production
- `@pytest.mark.production_only` - Should only run in production
- `@pytest.mark.slow` - Tests taking >30 seconds

## 🔄 Data Flow

### Ingest Pipeline
1. **HTTP Trigger** → Ingest Cloud Function
2. **Fetch Data** → HubSpot API (companies, deals, owners, deal stages)
3. **Store Data** → BigQuery tables with snapshot_id
4. **Update Registry** → Track snapshot completion
5. **Publish Event** → Pub/Sub topic for scoring pipeline

### Scoring Pipeline  
1. **Pub/Sub Event** → Scoring Cloud Function
2. **Stage Mapping** → Update scoring configuration
3. **Process Units** → Calculate individual record scores
4. **Aggregate History** → Summarize scores by owner/stage
5. **Update Registry** → Track scoring completion

### BigQuery Tables
- `hs_companies` - Company data snapshots
- `hs_deals` - Deal data snapshots  
- `hs_owners` - Owner/user reference data
- `hs_deal_stage_reference` - Deal pipeline reference data
- `hs_snapshot_registry` - Snapshot tracking and status
- `hs_stage_mapping` - Scoring configuration
- `hs_pipeline_units_snapshot` - Individual unit scores
- `hs_pipeline_score_history` - Aggregated score history

## 🛠️ Development

### Local Development Setup
```bash
# Install dependencies
pip install -r src/requirements.txt

# Set up environment variables
cp .env.example .env
# Edit .env with your credentials

# Run local tests
cd src/
python main.py
```

### 🆕 Future CI/CD Integration (Planned)

#### Automated Testing Pipeline
- **Pull Request Tests**: Run all non-production tests
- **Dev Deploy**: Auto-deploy to dev on dev branch
- **Staging Deploy**: Auto-deploy to staging on staging branch  
- **Production Deploy**: Deploy to production on main branch after validation
- **Health Checks**: Scheduled production infrastructure tests

#### Test Strategy by Environment
- **Development**: Full pipeline tests with real data
- **Staging**: Performance and integration tests
- **Production**: Infrastructure and connectivity tests only

## 📊 Monitoring & Logging

### Logging Levels
- **DEBUG**: Detailed field mappings, API details, performance metrics
- **INFO**: Standard operations, counts, timing (default for staging)
- **WARN**: Warnings and errors only (default for production)  
- **ERROR**: Errors only

### Smart Retry System
BigQuery operations use intelligent retry logic that:
- Expects first-attempt failures for new tables (normal BigQuery behavior)
- Uses appropriate log levels (INFO for expected retries, WARNING for unexpected)
- Handles streaming buffer timing issues automatically

## 🔐 Security & Permissions

### Service Accounts
- **Ingest**: `hubspot-{env}@hubspot-452402.iam.gserviceaccount.com`
- **Scoring**: `hubspot-scoring-{env}@hubspot-452402.iam.gserviceaccount.com`

### Required Permissions
- BigQuery Data Editor
- Pub/Sub Publisher (ingest) / Subscriber (scoring)
- Secret Manager Secret Accessor
- Cloud Functions Invoker (for Eventarc)

## 🚧 Upcoming Features

### 🆕 Pytest Testing Framework (In Progress)
- Production-safe infrastructure testing
- Database operation validation  
- Event system testing
- Automated CI/CD integration
- Performance monitoring and benchmarking

### Excel Import Enhancement
- Multi-snapshot Excel processing
- Historical data backfill capabilities
- Data quality validation and reporting

### Advanced Monitoring
- Performance benchmarking and trends
- Automated alerting for failures
- Cost monitoring and optimization
- Data freshness monitoring

## 📞 Usage Examples

### Trigger Ingest
```bash
# Development test (dry run)
curl -X POST https://europe-west1-hubspot-452402.cloudfunctions.net/hubspot-ingest-dev \
  -H "Content-Type: application/json" \
  -d '{"limit": 10, "dry_run": true}'

# Production sync (all data)
curl -X POST https://europe-west1-hubspot-452402.cloudfunctions.net/hubspot-ingest-prod \
  -H "Content-Type: application/json" \
  -d '{"no_limit": true, "dry_run": false}'
```

### 🆕 Test Production Readiness
```bash
# Test infrastructure readiness
curl -X POST https://europe-west1-hubspot-452402.cloudfunctions.net/hubspot-ingest-prod \
  -H "Content-Type: application/json" \
  -d '{"mode": "test", "test_type": "infrastructure"}'

# Test database operations (safe)
curl -X POST https://europe-west1-hubspot-452402.cloudfunctions.net/hubspot-ingest-prod \
  -H "Content-Type: application/json" \
  -d '{"mode": "test", "test_type": "database"}'
```

### Query Results
```sql
-- Get latest snapshot data
SELECT * FROM `hubspot-452402.Hubspot_prod.hs_companies` 
WHERE snapshot_id = (
  SELECT snapshot_id FROM `hubspot-452402.Hubspot_prod.hs_snapshot_registry` 
  WHERE status LIKE '%completed%' 
  ORDER BY snapshot_timestamp DESC LIMIT 1
);

-- Get scoring results by owner
SELECT 
  owner_id,
  combined_stage,
  num_companies,
  total_score
FROM `hubspot-452402.Hubspot_prod.hs_pipeline_score_history`
WHERE snapshot_id = 'latest_snapshot_id'
ORDER BY total_score DESC;
```

## 🆘 Troubleshooting

### Common Issues
1. **BigQuery "Table not ready"**: Normal on first deployment - retry logic handles this
2. **Pub/Sub permission denied**: Check service account IAM bindings
3. **Secret Manager access failed**: Verify Secret Manager API enabled and IAM roles
4. **Function timeout**: Increase timeout or reduce data limit for testing

### 🆕 Production Testing Troubleshooting
1. **Test mode not working**: Verify `{"mode": "test"}` in request body
2. **Infrastructure tests failing**: Check service account permissions
3. **Database tests failing**: Verify BigQuery dataset exists and is accessible
4. **Test cleanup issues**: Check BigQuery delete permissions

### Debug Commands
```bash
# Check function logs
gcloud functions logs read hubspot-ingest-prod --region=europe-west1

# Check BigQuery tables  
bq ls hubspot-452402:Hubspot_prod

# Test connectivity
gcloud pubsub topics list | grep hubspot
```

---

## 📋 Development Status

### ✅ Completed
- Ingest pipeline with HubSpot API integration
- Scoring pipeline with stage mapping
- Smart retry BigQuery utilities  
- Event-driven architecture with Pub/Sub
- Multi-environment deployment system
- Comprehensive Cloud Function testing tools
- Registry tracking for data lineage

### 🔄 In Progress
- **Pytest testing framework** - Production-safe testing with pytest
- **CI/CD automation** - GitHub Actions integration
- **Performance monitoring** - Automated benchmarking

### 📋 Planned
- Advanced monitoring and alerting
- Cost optimization features
- Data quality validation
- Historical analysis capabilities

---

**Last Updated**: December 2024
**Version**: 2.0 (Testing Framework Integration)