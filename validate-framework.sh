#!/bin/bash

# Final Framework Validation Script  
# Validates the complete pytest framework is ready for deployment

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

function print_header() {
    echo -e "${BOLD}${BLUE}================================================================${NC}"
    echo -e "${BOLD}${BLUE}           Final Pytest Framework Validation${NC}"
    echo -e "${BOLD}${BLUE}================================================================${NC}"
    echo -e "${CYAN}🔍 Comprehensive validation before deployment${NC}"
    echo -e "${BLUE}================================================================${NC}"
    echo ""
}

function check_file_structure() {
    echo -e "${BLUE}📁 Checking file structure...${NC}"
    
    local required_files=(
        "requirements.txt"
        "ingest_main.py"
        "scoring_main.py"
        "tests/__init__.py"
        "tests/conftest.py"
        "tests/pytest.ini"
        "tests/test_framework_validation.py"
        "tests/fixtures/__init__.py"
        "tests/fixtures/test_session.py"
        "tests/markers/__init__.py"
    )
    
    local missing_files=()
    
    for file in "${required_files[@]}"; do
        if [ -f "$file" ]; then
            echo -e "${GREEN}  ✅ $file${NC}"
        else
            echo -e "${RED}  ❌ $file${NC}"
            missing_files+=("$file")
        fi
    done
    
    if [ ${#missing_files[@]} -eq 0 ]; then
        echo -e "${GREEN}✅ All required files present${NC}"
        return 0
    else
        echo -e "${RED}❌ Missing ${#missing_files[@]} required files${NC}"
        return 1
    fi
}

function check_dependencies() {
    echo -e "\n${BLUE}📦 Checking dependencies...${NC}"
    
    # Check if Python can import required modules
    python3 -c "
import sys
missing = []

try:
    import pytest
    print('✅ pytest available')
except ImportError:
    missing.append('pytest')
    print('❌ pytest missing')

try:
    import json
    print('✅ json available')
except ImportError:
    missing.append('json')
    print('❌ json missing')

try:
    from google.cloud import bigquery
    print('✅ google-cloud-bigquery available')
except ImportError:
    print('⚠️ google-cloud-bigquery missing (may be OK for local testing)')

try:
    from google.cloud import pubsub_v1
    print('✅ google-cloud-pubsub available')
except ImportError:
    print('⚠️ google-cloud-pubsub missing (may be OK for local testing)')

if missing:
    print(f'❌ Critical dependencies missing: {missing}')
    sys.exit(1)
else:
    print('✅ Core dependencies available')
"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Dependencies check passed${NC}"
        return 0
    else
        echo -e "${RED}❌ Dependencies check failed${NC}"
        return 1
    fi
}

function check_imports() {
    echo -e "\n${BLUE}🔗 Checking imports...${NC}"
    
    # Test framework imports
    python3 -c "
try:
    from tests import run_production_tests
    print('✅ Framework main import works')
except ImportError as e:
    print(f'❌ Framework import failed: {e}')
    exit(1)

try:
    from tests.fixtures.test_session import TestSession
    print('✅ TestSession import works')
except ImportError as e:
    print(f'❌ TestSession import failed: {e}')
    exit(1)

try:
    import tests.conftest
    print('✅ conftest import works')
except ImportError as e:
    print(f'❌ conftest import failed: {e}')
    exit(1)

print('✅ All imports successful')
"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Import check passed${NC}"
        return 0
    else
        echo -e "${RED}❌ Import check failed${NC}"
        return 1
    fi
}

function test_pytest_execution() {
    echo -e "\n${BLUE}🧪 Testing pytest execution...${NC}"
    
    # Run a simple pytest test
    python3 -m pytest tests/test_framework_validation.py::test_framework_basic_functionality \
        -v \
        --tb=short \
        --function-type=validation \
        --environment=development \
        -q
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Pytest execution successful${NC}"
        return 0
    else
        echo -e "${RED}❌ Pytest execution failed${NC}"
        return 1
    fi
}

function test_framework_integration() {
    echo -e "\n${BLUE}🔗 Testing framework integration...${NC}"
    
    # Test the main framework entry point
    python3 -c "
from tests import run_production_tests

try:
    result = run_production_tests(
        test_type='infrastructure',
        function_type='validation',
        environment='development'
    )
    
    print(f'Status: {result[\"status\"]}')
    print(f'Tests: {result[\"summary\"][\"total\"]} total')
    
    if result['status'] in ['success', 'partial_success']:
        print('✅ Framework integration successful')
        exit(0)
    else:
        print(f'❌ Framework integration failed: {result.get(\"error\", \"Unknown\")}')
        exit(1)
        
except Exception as e:
    print(f'❌ Framework integration error: {e}')
    exit(1)
"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Framework integration successful${NC}"
        return 0
    else
        echo -e "${RED}❌ Framework integration failed${NC}"
        return 1
    fi
}

function test_entry_point_integration() {
    echo -e "\n${BLUE}🌐 Testing entry point integration...${NC}"
    
    # Test ingest entry point with test mode
    python3 -c "
class MockRequest:
    def __init__(self, json_data):
        self._json = json_data
    
    def get_json(self, silent=True):
        return self._json

try:
    from ingest_main import main as ingest_main
    
    test_request = MockRequest({
        'mode': 'test',
        'test_type': 'infrastructure'
    })
    
    result = ingest_main(test_request)
    
    if isinstance(result, tuple):
        response_data, status_code = result
        print(f'Status Code: {status_code}')
        print(f'Test Mode: {response_data.get(\"test_mode\", False)}')
        print(f'Status: {response_data.get(\"status\", \"unknown\")}')
        
        if status_code in [200, 206] and response_data.get('test_mode'):
            print('✅ Entry point integration successful')
            exit(0)
        else:
            print('❌ Entry point integration failed')
            print(f'Response: {response_data}')
            exit(1)
    else:
        print(f'❌ Unexpected response format: {type(result)}')
        exit(1)
        
except Exception as e:
    print(f'❌ Entry point test failed: {e}')
    import traceback
    traceback.print_exc()
    exit(1)
"
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✅ Entry point integration successful${NC}"
        return 0
    else
        echo -e "${RED}❌ Entry point integration failed${NC}"
        return 1
    fi
}

function test_marker_system() {
    echo -e "\n${BLUE}🏷️  Testing marker system...${NC}"
    
    # Test different marker combinations
    local markers=("infrastructure" "database" "events" "logging" "production_safe")
    
    for marker in "${markers[@]}"; do
        echo -e "${YELLOW}  Testing marker: $marker${NC}"
        
        python3 -m pytest tests/ \
            -m "$marker and production_safe" \
            --collect-only \
            --quiet \
            --function-type=validation \
            --environment=development > /dev/null 2>&1
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}    ✅ $marker marker works${NC}"
        else
            echo -e "${RED}    ❌ $marker marker failed${NC}"
            return 1
        fi
    done
    
    echo -e "${GREEN}✅ Marker system working${NC}"
    return 0
}

function run_comprehensive_test() {
    echo -e "\n${BLUE}🎯 Running comprehensive test suite...${NC}"
    
    # Run all production-safe tests
    python3 -m pytest tests/ \
        -m "production_safe" \
        -v \
        --tb=short \
        --function-type=validation \
        --environment=development \
        --json-report \
        --json-report-file=comprehensive-test-results.json
    
    local exit_code=$?
    
    if [ -f "comprehensive-test-results.json" ]; then
        echo -e "\n${CYAN}📊 Comprehensive Test Results:${NC}"
        python3 -c "
import json
try:
    with open('comprehensive-test-results.json', 'r') as f:
        data = json.load(f)
    
    summary = data['report']['summary']
    print(f'  Total Tests: {summary['total']}')
    print(f'  Passed: {summary['passed']}')
    print(f'  Failed: {summary['failed']}')
    print(f'  Skipped: {summary['skipped']}')
    print(f'  Duration: {data['report']['duration']:.2f}s')
    
    if summary['failed'] == 0:
        print('✅ All tests passed!')
    else:
        print(f'❌ {summary['failed']} tests failed')
        
except Exception as e:
    print(f'Could not parse results: {e}')
"
        rm -f comprehensive-test-results.json
    fi
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✅ Comprehensive test suite passed${NC}"
        return 0
    else
        echo -e "${RED}❌ Comprehensive test suite failed${NC}"
        return 1
    fi
}

function show_deployment_instructions() {
    echo -e "\n${BOLD}${CYAN}🚀 DEPLOYMENT READY!${NC}"
    echo -e "${BLUE}================================${NC}"
    echo -e "${GREEN}✅ Framework validation completed successfully${NC}"
    echo -e "${GREEN}✅ All components working correctly${NC}"
    echo -e "${GREEN}✅ Ready for deployment and testing${NC}"
    echo ""
    echo -e "${CYAN}Next steps:${NC}"
    echo -e "  1. ${BLUE}Deploy to development:${NC}"
    echo -e "     ./deploy.sh ingest dev"
    echo -e "     ./deploy.sh scoring dev"
    echo ""
    echo -e "  2. ${BLUE}Test deployed functions:${NC}"
    echo -e "     curl -d '{\"mode\": \"test\", \"test_type\": \"infrastructure\"}' \\"
    echo -e "       https://europe-west1-hubspot-452402.cloudfunctions.net/hubspot-ingest-dev"
    echo ""
    echo -e "  3. ${BLUE}Test different test types:${NC}"
    echo -e "     curl -d '{\"mode\": \"test\", \"test_type\": \"database\"}' https://..."
    echo -e "     curl -d '{\"mode\": \"test\", \"test_type\": \"events\"}' https://..."
    echo ""
    echo -e "  4. ${BLUE}Deploy to production when ready:${NC}"
    echo -e "     ./deploy.sh ingest prod"
    echo -e "     ./deploy.sh scoring prod"
    echo ""
    echo -e "${YELLOW}Framework includes:${NC}"
    echo -e "  • Production-safe infrastructure testing"
    echo -e "  • Database operation validation" 
    echo -e "  • Event system testing"
    echo -e "  • Logging verification"
    echo -e "  • Automatic cleanup"
    echo -e "  • Comprehensive reporting"
}

function main() {
    print_header
    
    # Must be in src directory
    if [ ! -f "requirements.txt" ] || [ ! -d "tests" ]; then
        echo -e "${RED}❌ Must be run from src/ directory${NC}"
        echo -e "${YELLOW}💡 cd src && ../validate-framework.sh${NC}"
        exit 1
    fi
    
    # Run all validation tests
    local all_passed=true
    
    check_file_structure || all_passed=false
    check_dependencies || all_passed=false  
    check_imports || all_passed=false
    test_pytest_execution || all_passed=false
    test_framework_integration || all_passed=false
    test_entry_point_integration || all_passed=false
    test_marker_system || all_passed=false
    run_comprehensive_test || all_passed=false
    
    if [ "$all_passed" = true ]; then
        show_deployment_instructions
        exit 0
    else
        echo -e "\n${RED}❌ Validation failed - framework not ready for deployment${NC}"
        echo -e "${YELLOW}Please fix the issues above and try again${NC}"
        exit 1
    fi
}

# Run if executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi