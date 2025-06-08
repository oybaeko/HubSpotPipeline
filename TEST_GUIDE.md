# Pytest Framework Deployment & Testing Guide

## 🧪 Testing the Framework Locally

### 1. Validate Framework Concept
```bash
cd src/
chmod +x ../test-framework.sh
../test-framework.sh
```

This will test:
- ✅ Direct pytest execution
- ✅ Framework import/export functionality  
- ✅ Entry point integration
- ✅ Test type filtering
- ✅ Production-safe markers

### 2. Manual Framework Testing
```bash
cd src/
python tests/test_framework_validation.py
```

## 🚀 Deploy and Test in Cloud Functions

### 1. Deploy to Development
```bash
./deploy.sh ingest dev
./deploy.sh scoring dev
```

### 2. Test Infrastructure (Production-Safe)
```bash
# Test ingest function
curl -X POST https://europe-west1-hubspot-452402.cloudfunctions.net/hubspot-ingest-dev \
  -H "Content-Type: application/json" \
  -d '{"mode": "test", "test_type": "infrastructure"}'

# Expected Response:
{
  "test_mode": true,
  "function_type": "ingest", 
  "test_type": "infrastructure",
  "status": "success",
  "summary": {
    "total": 6,
    "passed": 6,
    "failed": 0,
    "skipped": 0
  },
  "environment": "development"
}
```

### 3. Test Database Operations (Safe)
```bash
curl -X POST https://europe-west1-hubspot-452402.cloudfunctions.net/hubspot-ingest-dev \
  -H "Content-Type: application/json" \
  -d '{"mode": "test", "test_type": "database"}'
```

### 4. Test Event System
```bash
curl -X POST https://europe-west1-hubspot-452402.cloudfunctions.net/hubspot-ingest-dev \
  -H "Content-Type: application/json" \
  -d '{"mode": "test", "test_type": "events"}'
```

### 5. Test Scoring Function (via Pub/Sub)
```bash
# Publish test message to Pub/Sub topic
gcloud pubsub topics publish hubspot-events-dev \
  --message='{"type":"hubspot.test.request","data":{"test_type":"infrastructure"}}'

# Check scoring function logs
gcloud functions logs read hubspot-scoring-dev --region=europe-west1
```

## 🔍 Verify Results

### Success Indicators:
- ✅ HTTP Status: 200
- ✅ `"test_mode": true` in response
- ✅ `"status": "success"` or `"status": "partial_success"`
- ✅ `"summary"` shows test counts
- ✅ `"passed"` > 0

### Troubleshooting:
```bash
# If tests fail, check details
curl -X POST https://...hubspot-ingest-dev \
  -d '{"mode": "test", "test_type": "infrastructure"}' | jq '.details.failed_tests'

# Check function logs
gcloud functions logs read hubspot-ingest-dev --region=europe-west1 --limit=50
```

## 📋 Test Types Available

| Test Type | Description | Production Safe |
|-----------|-------------|-----------------|
| `infrastructure` | Connectivity, permissions, secrets | ✅ Yes |
| `database` | Safe BigQuery operations | ✅ Yes |
| `events` | Pub/Sub permissions and topics | ✅ Yes |
| `logging` | Logging levels and format | ✅ Yes |
| `all_safe` | All production-safe tests | ✅ Yes |

## 🎯 Next Steps After Validation

1. **Deploy to Staging**: Test with staging data
2. **Deploy to Production**: Run infrastructure tests only
3. **Add More Tests**: Create specific tests for your use cases
4. **Set up CI/CD**: Integrate with GitHub Actions
5. **Monitor**: Set up automated health checks

## 🚧 Adding New Tests

### Create Test File:
```python
# src/tests/test_my_feature.py
import pytest

@pytest.mark.infrastructure
@pytest.mark.production_safe
def test_my_feature(test_logger):
    test_logger.info("Testing my feature")
    assert True
```

### Test Locally:
```bash
cd src/
python -m pytest tests/test_my_feature.py -v --function-type=test --environment=development
```

### Test in Cloud Function:
```bash
curl -d '{"mode": "test", "test_type": "infrastructure"}' https://...your-function
```

## ✅ Framework Validation Checklist

- [ ] Local framework testing passes
- [ ] Deployment successful  
- [ ] Infrastructure tests pass in dev
- [ ] Database tests pass in dev
- [ ] Event tests pass in dev
- [ ] Logging tests pass in dev
- [ ] Production deployment ready
- [ ] CI/CD pipeline planned

**Once all checkboxes are complete, the pytest framework is ready for production use!**