#!/bin/bash

# HubSpot Cloud Function Deployment Testing Script - Simplified
# 
# WHAT THIS SCRIPT TESTS:
# ✅ Two-Tier Testing Framework (deployment + runtime validation)
# ✅ Basic functional validation (ping + integration test)
# ✅ Dataset cleanup utilities
# ❌ Does NOT test complex functional scenarios (use integration tests)
#
# Usage: ./test-GCF-deployment.sh [dev|staging|prod]

# Colors and formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
PURPLE='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ID="hubspot-452402"
REGION="europe-west1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/test-results"

# Create results directory
mkdir -p "$RESULTS_DIR"

function get_function_url() {
    local env=$1
    echo "https://$REGION-$PROJECT_ID.cloudfunctions.net/hubspot-ingest-$env"
}

function get_default_log_level() {
    local env=$1
    case $env in
        dev) echo "DEBUG" ;;
        staging) echo "INFO" ;;
        prod) echo "WARN" ;;
        *) echo "INFO" ;;
    esac
}

function print_header() {
    echo -e "${BOLD}${BLUE}================================================================${NC}"
    echo -e "${BOLD}${BLUE}           HubSpot Cloud Function Deployment Testing${NC}"
    echo -e "${BOLD}${BLUE}================================================================${NC}"
    echo -e "${CYAN}🎯 SCOPE: Two-Tier Testing Framework + Basic Functional Tests${NC}"
    echo -e "${CYAN}📊 FEATURES: Environment validation, runtime checks, cleanup tools${NC}"
    echo -e "${CYAN}🚀 TARGET: $PROJECT_ID Cloud Functions in $REGION${NC}"
    echo -e "${BLUE}================================================================${NC}"
    echo ""
}

function get_dataset_for_env() {
    local env=$1
    case $env in
        dev) echo "Hubspot_dev_ob" ;;
        staging) echo "Hubspot_staging" ;;
        prod) echo "Hubspot_prod" ;;
        *) echo "Hubspot_dev_ob" ;;
    esac
}

function check_prerequisites() {
    echo -e "${BLUE}🔍 Checking prerequisites...${NC}"
    
    # Check if gcloud is available
    if ! command -v gcloud &> /dev/null; then
        echo -e "${RED}❌ gcloud CLI not found${NC}"
        echo -e "${YELLOW}💡 Install: https://cloud.google.com/sdk/docs/install${NC}"
        return 1
    fi
    
    # Check if jq is available for JSON parsing
    if ! command -v jq &> /dev/null; then
        echo -e "${YELLOW}⚠️  jq not found - installing for better result parsing...${NC}"
        # Try to install jq
        if command -v brew &> /dev/null; then
            brew install jq >/dev/null 2>&1
        elif command -v apt-get &> /dev/null; then
            sudo apt-get install -y jq >/dev/null 2>&1
        else
            echo -e "${YELLOW}📝 Please install jq manually for enhanced result parsing${NC}"
        fi
    fi
    
    # Check authentication
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
        echo -e "${RED}❌ Not authenticated with gcloud${NC}"
        echo -e "${YELLOW}💡 Run: gcloud auth login${NC}"
        return 1
    fi
    
    # Set project
    current_project=$(gcloud config get-value project 2>/dev/null)
    if [ "$current_project" != "$PROJECT_ID" ]; then
        echo -e "${YELLOW}🔧 Setting project to $PROJECT_ID...${NC}"
        gcloud config set project $PROJECT_ID
    fi
    
    echo -e "${GREEN}✅ Prerequisites check passed${NC}"
    return 0
}

function parse_test_result() {
    local response_file=$1
    local env=$2
    
    echo -e "\n${BOLD}${BLUE}📊 TEST RESULTS${NC}"
    echo -e "${BLUE}===============${NC}"
    
    if command -v jq &> /dev/null && [ -f "$response_file" ]; then
        # Check if this is a test mode response
        local test_mode=$(jq -r '.test_mode // false' "$response_file" 2>/dev/null)
        
        if [ "$test_mode" = "true" ]; then
            # Parse testing framework results
            parse_framework_result "$response_file"
        else
            # Parse functional test results
            parse_functional_result "$response_file"
        fi
    else
        echo -e "${YELLOW}⚠️  Raw response (jq not available):${NC}"
        cat "$response_file" 2>/dev/null | head -20
    fi
}

function parse_framework_result() {
    local response_file=$1
    
    local status=$(jq -r '.status // "unknown"' "$response_file" 2>/dev/null)
    local validation_tier=$(jq -r '.validation_tier // "unknown"' "$response_file" 2>/dev/null)
    local function_type=$(jq -r '.function_type // "unknown"' "$response_file" 2>/dev/null)
    local environment=$(jq -r '.environment // "unknown"' "$response_file" 2>/dev/null)
    
    # Display test framework header
    echo -e "${PURPLE}🧪 TWO-TIER TESTING FRAMEWORK${NC}"
    
    # Display status
    case $status in
        "success")
            echo -e "${GREEN}✅ Status: ALL TESTS PASSED${NC}"
            ;;
        "partial_success")
            echo -e "${YELLOW}⚠️  Status: PARTIAL SUCCESS${NC}"
            ;;
        "failed")
            echo -e "${RED}❌ Status: TESTS FAILED${NC}"
            ;;
        "error")
            echo -e "${RED}💥 Status: FRAMEWORK ERROR${NC}"
            local error_msg=$(jq -r '.error // "Unknown error"' "$response_file" 2>/dev/null)
            echo -e "${RED}Error: $error_msg${NC}"
            ;;
        *)
            echo -e "${YELLOW}⚠️  Status: $status${NC}"
            ;;
    esac
    
    # Display test details
    echo -e "${CYAN}🎯 Validation Tier: $validation_tier${NC}"
    echo -e "${CYAN}⚙️  Function Type: $function_type${NC}"
    echo -e "${CYAN}🌍 Environment: $environment${NC}"
    
    # Display test summary
    local total=$(jq -r '.summary.total // 0' "$response_file" 2>/dev/null)
    local passed=$(jq -r '.summary.passed // 0' "$response_file" 2>/dev/null)
    local failed=$(jq -r '.summary.failed // 0' "$response_file" 2>/dev/null)
    local skipped=$(jq -r '.summary.skipped // 0' "$response_file" 2>/dev/null)
    local duration=$(jq -r '.summary.duration // 0' "$response_file" 2>/dev/null)
    
    echo -e "\n${CYAN}📊 Test Summary:${NC}"
    echo -e "${GREEN}  ✅ Passed: $passed${NC}"
    echo -e "${RED}  ❌ Failed: $failed${NC}"
    echo -e "${YELLOW}  ⏭️  Skipped: $skipped${NC}"
    echo -e "${BLUE}  📋 Total: $total${NC}"
    echo -e "${BLUE}  ⏱️  Duration: ${duration}s${NC}"
    
    # Show failed tests if any
    if [ "$failed" -gt 0 ]; then
        echo -e "\n${RED}❌ Failed Tests:${NC}"
        jq -r '.details.failed_tests[]? | "• \(.name): \(.error)"' "$response_file" 2>/dev/null | head -5 | while read line; do
            echo -e "${RED}  $line${NC}"
        done
        if [ "$failed" -gt 5 ]; then
            echo -e "${RED}  ... and $((failed - 5)) more${NC}"
        fi
    fi
}

function parse_functional_result() {
    local response_file=$1
    
    local status=$(jq -r '.status // "unknown"' "$response_file" 2>/dev/null)
    
    echo -e "${BLUE}📊 FUNCTIONAL TEST${NC}"
    
    # Display status
    case $status in
        "success")
            echo -e "${GREEN}✅ Status: SUCCESS${NC}"
            ;;
        "error")
            echo -e "${RED}❌ Status: ERROR${NC}"
            local error_msg=$(jq -r '.error // "Unknown error"' "$response_file" 2>/dev/null)
            echo -e "${RED}💥 Error: $error_msg${NC}"
            ;;
        *)
            echo -e "${YELLOW}⚠️  Status: $status${NC}"
            ;;
    esac
    
    # Show key metrics if available
    local snapshot_id=$(jq -r '.snapshot_id // "N/A"' "$response_file" 2>/dev/null)
    local total_records=$(jq -r '.total_records // 0' "$response_file" 2>/dev/null)
    
    if [ "$snapshot_id" != "N/A" ]; then
        echo -e "${CYAN}📸 Snapshot ID: $snapshot_id${NC}"
        echo -e "${CYAN}📊 Total Records: $total_records${NC}"
    fi
}

function run_test() {
    local env=$1
    local test_type=$2
    local url=$(get_function_url $env)
    
    # Use session log level or environment default
    local log_level=${SESSION_LOG_LEVEL:-$(get_default_log_level $env)}
    
    # Create result file
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local result_file="$RESULTS_DIR/${env}_${test_type}_${timestamp}.json"
    local curl_output="$RESULTS_DIR/${env}_${test_type}_${timestamp}_curl.txt"
    
    echo -e "\n${YELLOW}🚀 Running $test_type test on $env environment...${NC}"
    echo -e "${CYAN}URL: $url${NC}"
    echo -e "${CYAN}Results: $result_file${NC}"
    
    case $test_type in
        # Two-Tier Testing Framework Tests
        test-deployment)
            echo -e "${PURPLE}🧪 Deployment Validation (Tier 1)${NC}"
            echo -e "${CYAN}ℹ️  Tests environment-specific configuration and connectivity${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"mode\": \"test\", \"test_type\": \"deployment\"}" \
                -w "\n⏱️  Total time: %{time_total}s | HTTP: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        test-runtime)
            echo -e "${PURPLE}🧪 Runtime Validation (Tier 2)${NC}"
            echo -e "${CYAN}ℹ️  Tests basic Python runtime and mechanism functionality${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"mode\": \"test\", \"test_type\": \"runtime\"}" \
                -w "\n⏱️  Total time: %{time_total}s | HTTP: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        
        # Basic Functional Tests
        ping)
            echo -e "${BLUE}📡 Health Check${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"log_level\": \"$log_level\"}" \
                -w "\n⏱️  Total time: %{time_total}s | HTTP: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        integration)
            echo -e "${GREEN}🔄 Integration Test${NC}"
            local record_limit=${E2E_RECORD_LIMIT:-5}
            echo -e "${CYAN}ℹ️  End-to-end test with $record_limit records (set E2E_RECORD_LIMIT to change)${NC}"
            
            if [ "$env" = "prod" ]; then
                confirm_production_integration_test "$record_limit"
            fi
            
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"mode\": \"test\", \"test_type\": \"integration\", \"record_limit\": $record_limit}" \
                -w "\n⏱️  Total time: %{time_total}s | HTTP: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
    esac
    
    # Show curl metrics
    echo -e "\n${BLUE}📡 Request Details:${NC}"
    cat "$curl_output"
    
    # Parse and display results
    parse_test_result "$result_file" "$env"
    
    echo -e "\n${GREEN}💾 Results saved to: $result_file${NC}"
}

function run_integration_test() {
    local env=$1
    local test_size=$2
    local record_limit=$3
    local url=$(get_function_url $env)
    
    # Create result file
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local result_file="$RESULTS_DIR/${env}_integration_${test_size}_${timestamp}.json"
    local curl_output="$RESULTS_DIR/${env}_integration_${test_size}_${timestamp}_curl.txt"
    
    echo -e "\n${YELLOW}🚀 Running integration-$test_size test on $env environment...${NC}"
    echo -e "${CYAN}URL: $url${NC}"
    echo -e "${CYAN}Results: $result_file${NC}"
    
    case $test_size in
        small)
            echo -e "${GREEN}🔄 Integration Test - Small (3 records)${NC}"
            echo -e "${CYAN}ℹ️  Quick validation test with minimal data${NC}"
            ;;
        paging)
            echo -e "${YELLOW}🔄 Integration Test - Pagination (150 records)${NC}"
            echo -e "${CYAN}ℹ️  Tests HubSpot API pagination (100+ records)${NC}"
            
            if [ "$env" = "staging" ]; then
                confirm_paging_integration_test
            fi
            ;;
        full)
            echo -e "${RED}🔄 Integration Test - Full Sync (ALL records)${NC}"
            echo -e "${CYAN}ℹ️  Complete data sync - may take several minutes${NC}"
            
            confirm_full_integration_test "$env"
            ;;
    esac
    
    # Build request payload
    local payload
    if [ "$record_limit" -eq 0 ]; then
        # Full sync - no limit
        payload="{\"mode\": \"test\", \"test_type\": \"integration\", \"no_limit\": true}"
    else
        # Limited records
        payload="{\"mode\": \"test\", \"test_type\": \"integration\", \"record_limit\": $record_limit}"
    fi
    
    echo -e "${BLUE}📤 Payload: $payload${NC}"
    
    curl -X POST "$url" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        -w "\n⏱️  Total time: %{time_total}s | HTTP: %{http_code}\n" \
        -o "$result_file" \
        2>"$curl_output"
    
    # Show curl metrics
    echo -e "\n${BLUE}📡 Request Details:${NC}"
    cat "$curl_output"
    
    # Parse and display results
    parse_test_result "$result_file" "$env"
    
    echo -e "\n${GREEN}💾 Results saved to: $result_file${NC}"
}

function check_bigquery_tables() {
    local env=$1
    local dataset=$(get_dataset_for_env "$env")
    
    echo -e "\n${BLUE}🗄️  Checking BigQuery tables in $dataset...${NC}"
    
    local required_tables=("hs_companies" "hs_deals" "hs_owners" "hs_deal_stage_reference" "hs_snapshot_registry" "hs_stage_mapping" "hs_pipeline_units_snapshot" "hs_pipeline_score_history")
    local missing_tables=()
    local existing_tables=()
    
    for table in "${required_tables[@]}"; do
        if bq show "$PROJECT_ID:$dataset.$table" &>/dev/null; then
            existing_tables+=("$table")
        else
            missing_tables+=("$table")
        fi
    done
    
    echo -e "${GREEN}✅ Existing tables (${#existing_tables[@]}/${#required_tables[@]}):${NC}"
    for table in "${existing_tables[@]}"; do
        echo -e "${GREEN}  • $table${NC}"
    done
    
    if [ ${#missing_tables[@]} -gt 0 ]; then
        echo -e "${RED}❌ Missing tables (${#missing_tables[@]}):${NC}"
        for table in "${missing_tables[@]}"; do
            echo -e "${RED}  • $table${NC}"
        done
        echo -e "${YELLOW}💡 These tables should be created automatically during ingest${NC}"
        return 1
    else
        echo -e "${GREEN}🎉 All required tables exist!${NC}"
        return 0
    fi
}

function show_dataset_cleanup_menu() {
    local env=$1
    local dataset=$(get_dataset_for_env "$env")
    
    echo -e "\n${YELLOW}🗑️  Dataset Cleanup for $env environment${NC}"
    echo -e "${YELLOW}Dataset: $dataset${NC}"
    echo ""
    echo -e "  ${RED}1) Delete ALL tables${NC} (Complete cleanup)"
    echo -e "  ${YELLOW}2) Delete data tables only${NC} (Keep reference tables)"
    echo -e "  ${YELLOW}3) Delete scoring tables only${NC} (Keep ingest tables)"
    echo -e "  ${GREEN}4) List tables with row counts${NC} (Show what exists)"
    echo -e "  ${BLUE}5) Back to main menu${NC}"
    echo ""
    
    while true; do
        read -p "$(echo -e ${GREEN}Choose cleanup option [1-5]: ${NC})" choice
        
        case $choice in
            1) confirm_and_cleanup_dataset "$env" "all" ;;
            2) confirm_and_cleanup_dataset "$env" "data" ;;
            3) confirm_and_cleanup_dataset "$env" "scoring" ;;
            4) list_dataset_tables "$env" ;;
            5) return ;;
            *)
                echo -e "${RED}❌ Invalid choice. Please select 1-5.${NC}"
                ;;
        esac
        
        echo -e "\n${CYAN}Press Enter to continue...${NC}"
        read
        show_dataset_cleanup_menu "$env"
        break
    done
}

function confirm_and_cleanup_dataset() {
    local env=$1
    local cleanup_type=$2
    local dataset=$(get_dataset_for_env "$env")
    
    case $cleanup_type in
        "all")
            local tables=("hs_companies" "hs_deals" "hs_owners" "hs_deal_stage_reference" "hs_snapshot_registry" "hs_stage_mapping" "hs_pipeline_units_snapshot" "hs_pipeline_score_history")
            local description="ALL TABLES"
            ;;
        "data")
            local tables=("hs_companies" "hs_deals" "hs_snapshot_registry")
            local description="DATA TABLES (companies, deals, registry)"
            ;;
        "scoring")
            local tables=("hs_stage_mapping" "hs_pipeline_units_snapshot" "hs_pipeline_score_history")
            local description="SCORING TABLES (stage mapping, units, history)"
            ;;
    esac
    
    echo -e "\n${RED}⚠️  DATASET CLEANUP WARNING${NC}"
    echo -e "${RED}Environment: $env${NC}"
    echo -e "${RED}Dataset: $dataset${NC}"
    echo -e "${RED}Action: Delete $description${NC}"
    echo -e "${RED}Tables to delete: ${#tables[@]}${NC}"
    for table in "${tables[@]}"; do
        echo -e "${RED}  • $table${NC}"
    done
    echo ""
    
    if [ "$env" = "prod" ]; then
        echo -e "${RED}🚨 THIS IS PRODUCTION DATA! 🚨${NC}"
        read -p "$(echo -e ${RED}Type \"DELETE PRODUCTION ${cleanup_type}\" to continue: ${NC})" confirm
        if [ "$confirm" != "DELETE PRODUCTION ${cleanup_type}" ]; then
            echo -e "${YELLOW}❌ Cleanup cancelled${NC}"
            return
        fi
    else
        read -p "$(echo -e ${YELLOW}Type \"DELETE ${cleanup_type}\" to continue: ${NC})" confirm
        if [ "$confirm" != "DELETE ${cleanup_type}" ]; then
            echo -e "${YELLOW}❌ Cleanup cancelled${NC}"
            return
        fi
    fi
    
    echo -e "\n${RED}🗑️  Deleting tables...${NC}"
    
    local deleted=0
    local failed=0
    
    for table in "${tables[@]}"; do
        echo -e "${YELLOW}Deleting $table...${NC}"
        if bq rm -f "$PROJECT_ID:$dataset.$table" &>/dev/null; then
            echo -e "${GREEN}✅ Deleted $table${NC}"
            ((deleted++))
        else
            echo -e "${RED}❌ Failed to delete $table (may not exist)${NC}"
            ((failed++))
        fi
    done
    
    echo -e "\n${GREEN}🎉 Cleanup completed${NC}"
    echo -e "${GREEN}✅ Deleted: $deleted tables${NC}"
    if [ $failed -gt 0 ]; then
        echo -e "${YELLOW}⚠️  Failed: $failed tables${NC}"
    fi
}

function list_dataset_tables() {
    local env=$1
    local dataset=$(get_dataset_for_env "$env")
    
    echo -e "\n${BLUE}📋 Tables in $dataset:${NC}"
    
    local tables=$(bq ls "$PROJECT_ID:$dataset" --format="value(tableId)" 2>/dev/null)
    
    if [ -n "$tables" ]; then
        echo "$tables" | while read table; do
            if [ -n "$table" ]; then
                # Get row count
                local count=$(bq query --use_legacy_sql=false --format=csv --quiet "SELECT COUNT(*) FROM \`$PROJECT_ID.$dataset.$table\`" 2>/dev/null | tail -1)
                echo -e "${CYAN}  • $table ${YELLOW}($count rows)${NC}"
            fi
        done
    else
        echo -e "${YELLOW}No tables found in dataset${NC}"
    fi
}

function show_log_level_menu() {
    local env=$1
    local current_level=$2
    
    echo -e "\n${CYAN}📊 Select log level for this session:${NC}"
    echo -e "  ${GREEN}1) DEBUG    ${NC}(Detailed - field mappings, API details)"
    echo -e "  ${YELLOW}2) INFO     ${NC}(Standard - operations, counts, timing)"  
    echo -e "  ${YELLOW}3) WARN     ${NC}(Minimal - warnings and errors only)"
    echo -e "  ${RED}4) ERROR    ${NC}(Errors only)"
    echo -e "  ${BLUE}5) default  ${NC}(Use environment default: $(get_default_log_level $env))"
    echo ""
    echo -e "${CYAN}Current: ${current_level} | Environment default: $(get_default_log_level $env)${NC}"
    
    while true; do
        read -p "$(echo -e ${GREEN}Choose log level [1-5, default=5]: ${NC})" choice
        
        if [ -z "$choice" ]; then
            choice="5"
        fi
        
        case $choice in
            1|DEBUG) SESSION_LOG_LEVEL="DEBUG"; echo -e "${GREEN}✅ Log level: DEBUG${NC}"; break ;;
            2|INFO) SESSION_LOG_LEVEL="INFO"; echo -e "${YELLOW}✅ Log level: INFO${NC}"; break ;;
            3|WARN) SESSION_LOG_LEVEL="WARN"; echo -e "${YELLOW}✅ Log level: WARN${NC}"; break ;;
            4|ERROR) SESSION_LOG_LEVEL="ERROR"; echo -e "${RED}✅ Log level: ERROR${NC}"; break ;;
            5|default) SESSION_LOG_LEVEL=$(get_default_log_level $env); echo -e "${BLUE}✅ Using default: ${SESSION_LOG_LEVEL}${NC}"; break ;;
            *) echo -e "${RED}❌ Invalid choice. Please select 1-5.${NC}" ;;
        esac
    done
}

function show_environment_menu() {
    echo -e "\n${BLUE}🎯 Select environment to test:${NC}"
    echo -e "  ${GREEN}1) dev      ${NC}(Development - safe for all tests)"
    echo -e "  ${YELLOW}2) staging  ${NC}(Staging - safe for validation tests)"
    echo -e "  ${RED}3) prod     ${NC}(Production - framework tests only)"
    echo -e "  ${BLUE}4) quit     ${NC}(Exit)"
    echo ""
    
    while true; do
        read -p "$(echo -e ${GREEN}Choose environment [1-4, default=1]: ${NC})" choice
        
        if [ -z "$choice" ]; then choice="1"; fi
        
        case $choice in
            1|dev) ENVIRONMENT="dev"; echo -e "${GREEN}✅ Selected: Development${NC}"; break ;;
            2|staging) ENVIRONMENT="staging"; echo -e "${YELLOW}⚠️  Selected: Staging${NC}"; break ;;
            3|prod) ENVIRONMENT="prod"; echo -e "${RED}🚨 Selected: Production${NC}"; break ;;
            4|quit|q) echo -e "${BLUE}👋 Testing cancelled${NC}"; exit 0 ;;
            *) echo -e "${RED}❌ Invalid choice. Please select 1-4.${NC}" ;;
        esac
    done
}

function show_test_menu() {
    local env=$1
    local url=$(get_function_url $env)
    
    # Initialize session log level if not set
    if [ -z "$SESSION_LOG_LEVEL" ]; then
        SESSION_LOG_LEVEL=$(get_default_log_level $env)
        echo -e "\n${BLUE}📊 Log level: ${SESSION_LOG_LEVEL}${NC}"
    fi
    
    echo -e "\n${CYAN}🧪 Select test for $env environment:${NC}"
    echo -e "\n${PURPLE}📋 Two-Tier Testing Framework:${NC}"
    echo -e "  ${PURPLE}1) test-deployment ${NC}(Environment validation - API, BigQuery, Pub/Sub)"
    echo -e "  ${PURPLE}2) test-runtime    ${NC}(Runtime validation - Python mechanisms)"
    
    echo -e "\n${BLUE}📊 Basic Functional Tests:${NC}"
    echo -e "  ${GREEN}3) ping            ${NC}(Health check)"
    
    if [ "$env" != "prod" ]; then
        echo -e "  ${GREEN}4) integration-small    ${NC}(E2E: 3 records)"
        echo -e "  ${GREEN}5) integration-paging   ${NC}(E2E: 150 records - tests pagination)"
        echo -e "  ${RED}6) integration-full     ${NC}(E2E: ALL records - full sync)"
    fi
    
    echo -e "\n${BLUE}🛠️  Utilities:${NC}"
    echo -e "  ${CYAN}7) log-level       ${NC}(Change log level - current: ${SESSION_LOG_LEVEL})"
    echo -e "  ${PURPLE}8) check-tables    ${NC}(Check BigQuery table status)"
    echo -e "  ${YELLOW}9) pytest-infra    ${NC}(Run infrastructure pytest tests)"
    echo -e "  ${RED}10) cleanup        ${NC}(Dataset cleanup)"
    echo -e "  ${BLUE}11) back           ${NC}(Back to environment selection)"
    echo -e "  ${BLUE}12) quit           ${NC}(Exit)"
    echo ""
    echo -e "${CYAN}Target: ${url}${NC}"
    
    while true; do
        read -p "$(echo -e ${GREEN}Choose test [1-12]: ${NC})" choice
        
        case $choice in
            1) run_test "$env" "test-deployment" ;;
            2) run_test "$env" "test-runtime" ;;
            3) run_test "$env" "ping" ;;
            4) 
                if [ "$env" != "prod" ]; then
                    run_integration_test "$env" "small" 3
                else
                    echo -e "${RED}❌ Integration tests not available in production${NC}"
                fi
                ;;
            5) 
                if [ "$env" != "prod" ]; then
                    run_integration_test "$env" "paging" 150
                else
                    echo -e "${RED}❌ Integration tests not available in production${NC}"
                fi
                ;;
            6) 
                if [ "$env" != "prod" ]; then
                    run_integration_test "$env" "full" 0
                else
                    echo -e "${RED}❌ Integration tests not available in production${NC}"
                fi
                ;;
            7) show_log_level_menu "$env" "$SESSION_LOG_LEVEL"; show_test_menu "$env"; return ;;
            8) check_bigquery_tables "$env" ;;
            9) run_pytest_infrastructure_tests ;;
            10) show_dataset_cleanup_menu "$env"; show_test_menu "$env"; return ;;
            11) return ;;
            12|quit|q) echo -e "${BLUE}👋 Testing cancelled${NC}"; exit 0 ;;
            *) echo -e "${RED}❌ Invalid choice. Please select 1-12.${NC}" ;;
        esac
        
        echo -e "\n${CYAN}Press Enter to continue...${NC}"
        read
        show_test_menu "$env"
        break
    done
}

function run_pytest_infrastructure_tests() {
    echo -e "\n${YELLOW}🧪 Running Infrastructure Pytest Tests...${NC}"
    echo -e "${CYAN}ℹ️  Running: pytest tests/infrastructure/test_gcp_infrastructure_part*.py -v${NC}"
    echo -e "${CYAN}ℹ️  This will test GCP infrastructure setup and permissions${NC}"
    
    # Check if we're in the right directory
    if [ ! -d "tests/infrastructure" ]; then
        echo -e "${RED}❌ tests/infrastructure directory not found${NC}"
        echo -e "${YELLOW}💡 Make sure you're running this from the project root directory${NC}"
        echo -e "${YELLOW}💡 Current directory: $(pwd)${NC}"
        return 1
    fi
    
    # Check if test files exist
    local test_files=$(find tests/infrastructure -name "test_gcp_infrastructure_part*.py" 2>/dev/null)
    if [ -z "$test_files" ]; then
        echo -e "${RED}❌ No infrastructure test files found${NC}"
        echo -e "${YELLOW}💡 Expected files: tests/infrastructure/test_gcp_infrastructure_part*.py${NC}"
        echo -e "${YELLOW}💡 Make sure you've created the test files as shown in the previous steps${NC}"
        return 1
    fi
    
    echo -e "${GREEN}✅ Found test files:${NC}"
    echo "$test_files" | while read file; do
        echo -e "${GREEN}  • $file${NC}"
    done
    
    # Run the tests
    echo -e "\n${BLUE}🚀 Executing pytest...${NC}"
    
    if command -v pytest &> /dev/null; then
        # Create results file
        local timestamp=$(date +"%Y%m%d_%H%M%S")
        local result_file="$RESULTS_DIR/pytest_infrastructure_${timestamp}.txt"
        
        echo -e "${CYAN}Results will be saved to: $result_file${NC}"
        
        # Run pytest with verbose output and save results
        pytest tests/infrastructure/test_gcp_infrastructure_part*.py -v 2>&1 | tee "$result_file"
        
        local exit_code=${PIPESTATUS[0]}
        
        echo -e "\n${BLUE}📊 Pytest Results Summary:${NC}"
        if [ $exit_code -eq 0 ]; then
            echo -e "${GREEN}✅ All tests passed!${NC}"
        elif [ $exit_code -eq 1 ]; then
            echo -e "${RED}❌ Some tests failed${NC}"
            echo -e "${YELLOW}💡 Check the output above for details${NC}"
        else
            echo -e "${RED}❌ Pytest encountered an error${NC}"
            echo -e "${YELLOW}💡 Exit code: $exit_code${NC}"
        fi
        
        echo -e "${GREEN}💾 Full results saved to: $result_file${NC}"
        
    else
        echo -e "${RED}❌ pytest not found${NC}"
        echo -e "${YELLOW}💡 Install pytest: pip install pytest${NC}"
        echo -e "${YELLOW}💡 Or run manually: python -m pytest tests/infrastructure/test_gcp_infrastructure_part*.py -v${NC}"
    fi
}

function confirm_paging_integration_test() {
    echo -e "\n${YELLOW}⚠️  PAGINATION INTEGRATION TEST${NC}"
    echo -e "${YELLOW}This will test HubSpot pagination with 150 records.${NC}"
    echo -e "${CYAN}ℹ️  Important for validating API pagination logic.${NC}"
    read -p "$(echo -e ${YELLOW}Type 'yes' to continue: ${NC})" confirm
    if [ "$confirm" != "yes" ]; then
        echo -e "${YELLOW}❌ Pagination test cancelled${NC}"
        exit 0
    fi
}

function confirm_full_integration_test() {
    local env=$1
    echo -e "\n${RED}⚠️  FULL INTEGRATION TEST WARNING${NC}"
    echo -e "${RED}This will sync ALL HubSpot data to $env environment!${NC}"
    echo -e "${RED}This may take 5-30 minutes depending on data size.${NC}"
    echo -e "${RED}This will create a complete snapshot in BigQuery.${NC}"
    
    if [ "$env" = "staging" ]; then
        read -p "$(echo -e ${RED}Type 'FULL STAGING SYNC' to continue: ${NC})" confirm
        if [ "$confirm" != "FULL STAGING SYNC" ]; then
            echo -e "${YELLOW}❌ Full sync cancelled${NC}"
            exit 0
        fi
    else
        read -p "$(echo -e ${RED}Type 'FULL DEV SYNC' to continue: ${NC})" confirm
        if [ "$confirm" != "FULL DEV SYNC" ]; then
            echo -e "${YELLOW}❌ Full sync cancelled${NC}"
            exit 0
        fi
    fi
}

function main() {
    print_header
    
    # Check prerequisites first
    if ! check_prerequisites; then
        echo -e "${RED}❌ Prerequisites check failed${NC}"
        exit 1
    fi
    
    # Reset session log level on script start
    unset SESSION_LOG_LEVEL
    
    # Parse command line arguments
    if [ $# -ge 1 ]; then
        ENVIRONMENT=$1
        if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
            echo -e "${RED}❌ Invalid environment: $ENVIRONMENT${NC}"
            echo -e "${YELLOW}Usage: $0 [dev|staging|prod]${NC}"
            exit 1
        fi
        echo -e "${GREEN}✅ Using argument: $ENVIRONMENT${NC}"
    else
        show_environment_menu
    fi
    
    show_test_menu "$ENVIRONMENT"
}

# Show help if requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "HubSpot Cloud Function Deployment Testing Script - Simplified"
    echo ""
    echo "SCOPE:"
    echo "  ✅ Two-Tier Testing Framework (deployment + runtime validation)"
    echo "  ✅ Basic functional validation (ping + integration test)"
    echo "  ✅ Dataset cleanup utilities"
    echo "  ❌ Complex functional scenarios (use pytest integration tests directly)"
    echo ""
    echo "Usage: $0 [ENVIRONMENT]"
    echo ""
    echo "Environments:"
    echo "  dev      Development (all tests available)"
    echo "  staging  Staging (all tests available)"
    echo "  prod     Production (framework tests only)"
    echo ""
    echo "Test Types:"
    echo "  Two-Tier Framework:"
    echo "    test-deployment  Environment validation (API, BigQuery, Pub/Sub)"
    echo "    test-runtime     Runtime validation (Python mechanisms)"
    echo ""
    echo "  Basic Functional:"
    echo "    ping            Health check"
    echo "    integration     End-to-end pipeline (dev/staging only)"
    echo ""
    echo "  Utilities:"
    echo "    log-level       Change logging level"
    echo "    check-tables    Verify BigQuery tables"
    echo "    cleanup         Delete dataset tables"
    echo ""
    echo "Environment Variables:"
    echo "  E2E_RECORD_LIMIT   Number of records for integration test (default: 5)"
    echo ""
    echo "Results saved to: ./test-results/"
    exit 0
fi

# Run main function
main "$@"