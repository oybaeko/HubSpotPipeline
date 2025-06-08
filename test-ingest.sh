#!/bin/bash

# HubSpot Cloud Function Testing Script - Enhanced Version
# 
# WHAT THIS SCRIPT TESTS:
# ✅ Cloud Functions (HTTP-triggered ingest function)
# ✅ BigQuery table creation and data insertion  
# ✅ Pub/Sub event publishing (triggers scoring function)
# ❌ Does NOT test local Python execution
# ❌ Does NOT directly test scoring function (triggered via Pub/Sub)
#
# Usage: ./test-ingest.sh [dev|staging|prod] [test_type]

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

function get_scoring_function_name() {
    local env=$1
    echo "hubspot-scoring-$env"
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
    echo -e "${BOLD}${BLUE}           HubSpot Cloud Function Testing Suite${NC}"
    echo -e "${BOLD}${BLUE}================================================================${NC}"
    echo -e "${CYAN}🎯 SCOPE: Tests Cloud Functions (HTTP ingest + Pub/Sub scoring)${NC}"
    echo -e "${CYAN}📊 FEATURES: Result parsing, scoring monitoring, cleanup tools${NC}"
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
    
    echo -e "\n${BOLD}${BLUE}📊 PARSED RESULTS${NC}"
    echo -e "${BLUE}=================${NC}"
    
    if command -v jq &> /dev/null && [ -f "$response_file" ]; then
        # Extract key metrics using jq
        local status=$(jq -r '.status // "unknown"' "$response_file" 2>/dev/null)
        local snapshot_id=$(jq -r '.snapshot_id // "unknown"' "$response_file" 2>/dev/null)
        local total_records=$(jq -r '.total_records // 0' "$response_file" 2>/dev/null)
        local processing_time=$(jq -r '.processing_time_seconds // 0' "$response_file" 2>/dev/null)
        local dry_run=$(jq -r '.dry_run // false' "$response_file" 2>/dev/null)
        
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
        
        # Display metrics
        echo -e "${CYAN}📸 Snapshot ID: $snapshot_id${NC}"
        echo -e "${CYAN}📊 Total Records: $total_records${NC}"
        echo -e "${CYAN}⏱️  Processing Time: ${processing_time}s${NC}"
        
        # Display run mode
        if [ "$dry_run" = "true" ]; then
            echo -e "${BLUE}🛑 Mode: DRY RUN (no BigQuery writes)${NC}"
            echo -e "${BLUE}📤 Scoring: NOT triggered (dry run)${NC}"
        else
            echo -e "${GREEN}💾 Mode: LIVE RUN (BigQuery writes)${NC}"
            echo -e "${GREEN}📤 Scoring: Should be triggered via Pub/Sub${NC}"
        fi
        
        # Display data breakdown
        local results=$(jq -r '.results // {}' "$response_file" 2>/dev/null)
        if [ "$results" != "{}" ] && [ "$results" != "null" ]; then
            echo -e "\n${CYAN}📋 Data Breakdown:${NC}"
            jq -r '.results | to_entries[] | "  \(.key): \(.value) records"' "$response_file" 2>/dev/null | while read line; do
                echo -e "${CYAN}$line${NC}"
            done
        fi
        
        # Display reference data
        local ref_counts=$(jq -r '.reference_counts // {}' "$response_file" 2>/dev/null)
        if [ "$ref_counts" != "{}" ] && [ "$ref_counts" != "null" ]; then
            echo -e "\n${CYAN}📚 Reference Data:${NC}"
            jq -r '.reference_counts | to_entries[] | "  \(.key): \(.value) records"' "$response_file" 2>/dev/null | while read line; do
                echo -e "${CYAN}$line${NC}"
            done
        fi
        
        # Check for scoring trigger
        if [ "$dry_run" = "false" ] && [ "$status" = "success" ]; then
            check_scoring_function_trigger "$env" "$snapshot_id"
        fi
        
    else
        echo -e "${YELLOW}⚠️  Raw response (jq not available):${NC}"
        cat "$response_file" 2>/dev/null | head -20
    fi
}

function check_scoring_function_trigger() {
    local env=$1
    local snapshot_id=$2
    
    echo -e "\n${PURPLE}🔍 Checking scoring function trigger...${NC}"
    
    local scoring_function=$(get_scoring_function_name "$env")
    
    # Check if scoring function exists
    if gcloud functions describe "$scoring_function" --region="$REGION" &>/dev/null; then
        echo -e "${GREEN}✅ Scoring function exists: $scoring_function${NC}"
        
        # Check recent logs for this snapshot
        echo -e "${BLUE}📋 Checking recent scoring logs...${NC}"
        
        local log_filter="resource.type=\"cloud_function\" resource.labels.function_name=\"$scoring_function\" \"$snapshot_id\""
        local recent_logs=$(gcloud logging read "$log_filter" --limit=5 --format="value(timestamp,severity,textPayload)" --freshness=10m 2>/dev/null)
        
        if [ -n "$recent_logs" ]; then
            echo -e "${GREEN}✅ Found scoring activity for snapshot $snapshot_id${NC}"
            echo -e "${CYAN}Recent scoring logs:${NC}"
            echo "$recent_logs" | head -3 | while read line; do
                echo -e "${CYAN}  $line${NC}"
            done
        else
            echo -e "${YELLOW}⚠️  No recent scoring logs found (may take a few minutes)${NC}"
            echo -e "${BLUE}💡 Check manually: gcloud logging read \"$log_filter\" --limit=10${NC}"
        fi
    else
        echo -e "${YELLOW}⚠️  Scoring function not found: $scoring_function${NC}"
        echo -e "${BLUE}💡 Deploy scoring function or check function name${NC}"
    fi
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
    echo -e "  ${GREEN}4) List tables${NC} (Show what exists)"
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
        read -p "$(echo -e ${RED}Type 'DELETE PRODUCTION $cleanup_type' to continue: ${NC})" confirm
        if [ "$confirm" != "DELETE PRODUCTION $cleanup_type" ]; then
            echo -e "${YELLOW}❌ Cleanup cancelled${NC}"
            return
        fi
    else
        read -p "$(echo -e ${YELLOW}Type 'DELETE $cleanup_type' to continue: ${NC})" confirm
        if [ "$confirm" != "DELETE $cleanup_type" ]; then
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
    echo -e "${CYAN}Log Level: $log_level${NC}"
    echo -e "${CYAN}Results: $result_file${NC}"
    
    # Pre-test: Check BigQuery tables
    if [[ "$test_type" == "real-"* ]] || [[ "$test_type" == "dry-"* ]]; then
        check_bigquery_tables "$env"
    fi
    
    case $test_type in
        ping)
            echo -e "${BLUE}📡 Sending ping (empty POST with log level)${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"log_level\": \"$log_level\", \"trigger_source\": \"test-script-ping\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        dry-tiny)
            echo -e "${BLUE}🧪 Dry run - 2 records${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 2, \"dry_run\": true, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-dry-tiny\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        dry-small)
            echo -e "${BLUE}🧪 Dry run - 10 records${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 10, \"dry_run\": true, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-dry-small\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        dry-medium)
            echo -e "${BLUE}🧪 Dry run - 50 records${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 50, \"dry_run\": true, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-dry-medium\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        dry-paging)
            echo -e "${BLUE}🧪 Dry run - 150 records (tests HubSpot paging)${NC}"
            echo -e "${CYAN}ℹ️  This tests pagination since HubSpot returns max 100 per API call${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 150, \"dry_run\": true, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-dry-paging\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        dry-nolimit)
            echo -e "${BLUE}🧪 Dry run - ALL records (no limit, no BigQuery writes)${NC}"
            echo -e "${CYAN}ℹ️  This fetches all data from HubSpot but doesn't write to BigQuery${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"no_limit\": true, \"dry_run\": true, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-dry-nolimit\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        real-tiny)
            if [ "$env" = "prod" ]; then
                confirm_production_test "2 records"
            fi
            echo -e "${GREEN}💾 Real run - 2 records (writes to BigQuery)${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 2, \"dry_run\": false, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-real-tiny\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        real-small)
            if [ "$env" = "prod" ]; then
                confirm_production_test "10 records"
            fi
            echo -e "${GREEN}💾 Real run - 10 records (writes to BigQuery)${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 10, \"dry_run\": false, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-real-small\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        real-medium)
            if [ "$env" = "prod" ]; then
                confirm_production_test "50 records"
            fi
            echo -e "${YELLOW}💾 Real run - 50 records (writes to BigQuery)${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 50, \"dry_run\": false, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-real-medium\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        real-paging)
            if [ "$env" = "staging" ] || [ "$env" = "prod" ]; then
                confirm_paging_test "$env"
            fi
            echo -e "${YELLOW}💾 Real run - 150 records (tests paging + writes to BigQuery)${NC}"
            echo -e "${CYAN}ℹ️  This tests pagination since HubSpot returns max 100 per API call${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 150, \"dry_run\": false, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-real-paging\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        real-large)
            if [ "$env" = "staging" ] || [ "$env" = "prod" ]; then
                confirm_large_test "$env"
            fi
            echo -e "${RED}💾 Real run - 500 records (writes to BigQuery)${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 500, \"dry_run\": false, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-real-large\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
        real-full)
            confirm_production_full_test
            echo -e "${RED}🚨 FULL SYNC - ALL RECORDS${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"no_limit\": true, \"dry_run\": false, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-real-full\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
                -o "$result_file" \
                2>"$curl_output"
            ;;
    esac
    
    # Show curl metrics
    echo -e "\n${BLUE}📡 Curl Metrics:${NC}"
    cat "$curl_output"
    
    # Parse and display results
    parse_test_result "$result_file" "$env"
    
    # Post-test: Check tables again if it was a real run
    if [[ "$test_type" == "real-"* ]]; then
        echo -e "\n${BLUE}🔍 Post-test table check:${NC}"
        check_bigquery_tables "$env"
    fi
    
    echo -e "\n${GREEN}💾 Results saved to: $result_file${NC}"
}

function show_log_level_menu() {
    local env=$1
    local current_level=$2
    
    echo -e "\n${CYAN}📊 Select log level for this session:${NC}"
    echo -e "  ${GREEN}1) DEBUG    ${NC}(Detailed - field mappings, API details, performance)"
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
            1|DEBUG|debug)
                SESSION_LOG_LEVEL="DEBUG"
                echo -e "${GREEN}✅ Log level set to: DEBUG${NC}"
                break
                ;;
            2|INFO|info)
                SESSION_LOG_LEVEL="INFO"
                echo -e "${YELLOW}✅ Log level set to: INFO${NC}"
                break
                ;;
            3|WARN|warn)
                SESSION_LOG_LEVEL="WARN"
                echo -e "${YELLOW}✅ Log level set to: WARN${NC}"
                break
                ;;
            4|ERROR|error)
                SESSION_LOG_LEVEL="ERROR"
                echo -e "${RED}✅ Log level set to: ERROR${NC}"
                break
                ;;
            5|default)
                SESSION_LOG_LEVEL=$(get_default_log_level $env)
                echo -e "${BLUE}✅ Using environment default: ${SESSION_LOG_LEVEL}${NC}"
                break
                ;;
            *)
                echo -e "${RED}❌ Invalid choice. Please select 1-5.${NC}"
                ;;
        esac
    done
}

function show_environment_menu() {
    echo -e "\n${BLUE}🎯 Select environment to test:${NC}"
    echo -e "  ${GREEN}1) dev      ${NC}(Development - safe for all tests)"
    echo -e "  ${YELLOW}2) staging  ${NC}(Staging - safe for small tests)"
    echo -e "  ${RED}3) prod     ${NC}(Production - be careful!)"
    echo -e "  ${BLUE}4) quit     ${NC}(Exit without testing)"
    echo ""
    
    while true; do
        read -p "$(echo -e ${GREEN}Choose environment [1-4, default=1]: ${NC})" choice
        
        # Default to dev if Enter is pressed
        if [ -z "$choice" ]; then
            choice="1"
        fi
        
        case $choice in
            1|dev)
                ENVIRONMENT="dev"
                echo -e "${GREEN}✅ Selected: Development${NC}"
                break
                ;;
            2|staging)
                ENVIRONMENT="staging"
                echo -e "${YELLOW}⚠️  Selected: Staging${NC}"
                break
                ;;
            3|prod)
                ENVIRONMENT="prod"
                echo -e "${RED}🚨 Selected: Production${NC}"
                break
                ;;
            4|quit|q)
                echo -e "${BLUE}👋 Testing cancelled${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}❌ Invalid choice. Please select 1-4.${NC}"
                ;;
        esac
    done
}

function show_test_menu() {
    local env=$1
    local url=$(get_function_url $env)
    
    # Initialize session log level if not set
    if [ -z "$SESSION_LOG_LEVEL" ]; then
        SESSION_LOG_LEVEL=$(get_default_log_level $env)
        echo -e "\n${BLUE}📊 Log level initialized to environment default: ${SESSION_LOG_LEVEL}${NC}"
    fi
    
    echo -e "\n${CYAN}🧪 Select test type for $env environment:${NC}"
    echo -e "  ${GREEN}1) ping         ${NC}(Simple health check - no data)"
    echo -e "  ${GREEN}2) dry-tiny     ${NC}(Dry run - 2 records, no BigQuery writes)"
    echo -e "  ${GREEN}3) dry-small    ${NC}(Dry run - 10 records, no BigQuery writes)"
    echo -e "  ${GREEN}4) dry-medium   ${NC}(Dry run - 50 records, no BigQuery writes)"
    echo -e "  ${GREEN}5) dry-paging   ${NC}(Dry run - 150 records, tests HubSpot paging)"
    echo -e "  ${GREEN}6) dry-nolimit  ${NC}(Dry run - ALL records, no BigQuery writes)"
    
    if [ "$env" = "dev" ]; then
        echo -e "  ${YELLOW}7) real-tiny    ${NC}(Real run - 2 records, writes to BigQuery)"
        echo -e "  ${YELLOW}8) real-small   ${NC}(Real run - 10 records, writes to BigQuery)"
        echo -e "  ${YELLOW}9) real-medium  ${NC}(Real run - 50 records, writes to BigQuery)"
        echo -e "  ${YELLOW}10) real-paging ${NC}(Real run - 150 records, tests paging + writes)"
        echo -e "  ${RED}11) real-large  ${NC}(Real run - 500 records, writes to BigQuery)"
        echo -e "  ${RED}12) real-full   ${NC}(Real run - ALL records, full sync)"
    elif [ "$env" = "staging" ]; then
        echo -e "  ${YELLOW}7) real-tiny    ${NC}(Real run - 2 records, writes to BigQuery)"
        echo -e "  ${YELLOW}8) real-small   ${NC}(Real run - 10 records, writes to BigQuery)"
        echo -e "  ${YELLOW}9) real-medium  ${NC}(Real run - 50 records, writes to BigQuery)"
        echo -e "  ${RED}10) real-paging ${NC}(Real run - 150 records, REQUIRES CONFIRMATION)"
        echo -e "  ${RED}11) real-large  ${NC}(Real run - 500 records, REQUIRES CONFIRMATION)"
        echo -e "  ${RED}12) real-full   ${NC}(Real run - ALL records, REQUIRES CONFIRMATION)"
    else
        echo -e "  ${RED}7) real-tiny    ${NC}(Real run - 2 records, PRODUCTION DATA!)"
        echo -e "  ${RED}8) real-small   ${NC}(Real run - 10 records, PRODUCTION DATA!)"
        echo -e "  ${RED}9) real-medium  ${NC}(Real run - 50 records, PRODUCTION DATA!)"
        echo -e "  ${RED}10) real-paging ${NC}(Real run - 150 records, PRODUCTION!)"
        echo -e "  ${RED}11) real-large  ${NC}(Real run - 500 records, PRODUCTION!)"
        echo -e "  ${RED}12) real-full   ${NC}(Full sync - ALL RECORDS, PRODUCTION!)"
    fi
    
    echo -e "  ${BLUE}13) custom      ${NC}(Enter custom JSON payload)"
    echo -e "  ${CYAN}14) log-level   ${NC}(Change log level - current: ${SESSION_LOG_LEVEL})"
    echo -e "  ${PURPLE}15) check-tables${NC}(Check BigQuery table status)"
    echo -e "  ${RED}16) cleanup     ${NC}(Dataset cleanup/delete tables)"
    echo -e "  ${BLUE}17) back        ${NC}(Back to environment selection)"
    echo -e "  ${BLUE}18) quit        ${NC}(Exit)"
    echo ""
    echo -e "${CYAN}Target URL: ${url}${NC}"
    echo -e "${CYAN}Current Log Level: ${SESSION_LOG_LEVEL}${NC}"
    echo -e "${CYAN}Results Directory: ${RESULTS_DIR}${NC}"
    
    while true; do
        read -p "$(echo -e ${GREEN}Choose test [1-18]: ${NC})" choice
        
        case $choice in
            1) run_test "$env" "ping" ;;
            2) run_test "$env" "dry-tiny" ;;
            3) run_test "$env" "dry-small" ;;
            4) run_test "$env" "dry-medium" ;;
            5) run_test "$env" "dry-paging" ;;
            6) run_test "$env" "dry-nolimit" ;;
            7) run_test "$env" "real-tiny" ;;
            8) run_test "$env" "real-small" ;;
            9) run_test "$env" "real-medium" ;;
            10) run_test "$env" "real-paging" ;;
            11) run_test "$env" "real-large" ;;
            12) run_test "$env" "real-full" ;;
            13) run_custom_test "$env" ;;
            14) 
                show_log_level_menu "$env" "$SESSION_LOG_LEVEL"
                show_test_menu "$env"  # Return to test menu
                return
                ;;
            15) 
                check_bigquery_tables "$env"
                ;;
            16) 
                show_dataset_cleanup_menu "$env"
                show_test_menu "$env"  # Return to test menu
                return
                ;;
            17) return ;;
            18|quit|q)
                echo -e "${BLUE}👋 Testing cancelled${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}❌ Invalid choice. Please select 1-18.${NC}"
                ;;
        esac
        
        echo -e "\n${CYAN}Press Enter to continue...${NC}"
        read
        show_test_menu "$env"
        break
    done
}

function run_custom_test() {
    local env=$1
    local url=$(get_function_url $env)
    
    echo -e "\n${CYAN}🛠️  Custom test for $env environment${NC}"
    echo -e "${YELLOW}Examples:${NC}"
    echo -e "  {}                                           # Default (10 records, dry run)"
    echo -e "  {\"limit\": 5}                                # 5 records, dry run"
    echo -e "  {\"limit\": 20, \"dry_run\": false}            # 20 records, real run"
    echo -e "  {\"no_limit\": true, \"dry_run\": true}        # All records, dry run"
    echo -e "  {\"limit\": 10, \"log_level\": \"DEBUG\"}       # 10 records with debug logging"
    echo -e "  {\"limit\": 5, \"dry_run\": true, \"log_level\": \"INFO\"} # Custom with info logging"
    echo ""
    echo -e "${BLUE}Log Levels: DEBUG, INFO, WARN, ERROR${NC}"
    echo ""
    
    read -p "$(echo -e ${GREEN}Enter JSON payload: ${NC})" payload
    
    if [ -z "$payload" ]; then
        payload="{}"
    fi
    
    # Create result file for custom test
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local result_file="$RESULTS_DIR/${env}_custom_${timestamp}.json"
    local curl_output="$RESULTS_DIR/${env}_custom_${timestamp}_curl.txt"
    
    echo -e "\n${YELLOW}🚀 Running custom test...${NC}"
    echo -e "${CYAN}URL: $url${NC}"
    echo -e "${CYAN}Payload: $payload${NC}"
    echo -e "${CYAN}Results: $result_file${NC}"
    
    curl -X POST "$url" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n" \
        -o "$result_file" \
        2>"$curl_output"
    
    # Show curl metrics
    echo -e "\n${BLUE}📡 Curl Metrics:${NC}"
    cat "$curl_output"
    
    # Parse and display results
    parse_test_result "$result_file" "$env"
    
    echo -e "\n${GREEN}💾 Results saved to: $result_file${NC}"
}

function confirm_paging_test() {
    local env=$1
    echo -e "\n${YELLOW}⚠️  PAGING TEST WARNING${NC}"
    echo -e "${YELLOW}This will test HubSpot pagination (150 records) and write to $env BigQuery.${NC}"
    echo -e "${CYAN}ℹ️  This is important to test since HubSpot API paginates at 100 records.${NC}"
    read -p "$(echo -e ${YELLOW}Type 'yes' to continue: ${NC})" confirm
    if [ "$confirm" != "yes" ]; then
        echo -e "${YELLOW}❌ Paging test cancelled${NC}"
        exit 0
    fi
}

function confirm_large_test() {
    local env=$1
    if [ "$env" = "prod" ]; then
        echo -e "\n${RED}🚨 PRODUCTION LARGE TEST WARNING${NC}"
        echo -e "${RED}This will write 500 records to PRODUCTION BigQuery!${NC}"
        read -p "$(echo -e ${RED}Type 'PRODUCTION LARGE' to continue: ${NC})" confirm
        if [ "$confirm" != "PRODUCTION LARGE" ]; then
            echo -e "${YELLOW}❌ Production large test cancelled${NC}"
            exit 0
        fi
    else
        echo -e "\n${YELLOW}⚠️  LARGE TEST WARNING${NC}"
        echo -e "${YELLOW}This will write 500 records to $env BigQuery.${NC}"
        read -p "$(echo -e ${YELLOW}Type 'yes' to continue: ${NC})" confirm
        if [ "$confirm" != "yes" ]; then
            echo -e "${YELLOW}❌ Large test cancelled${NC}"
            exit 0
        fi
    fi
}

function confirm_production_test() {
    local description=$1
    echo -e "\n${RED}🚨 PRODUCTION TEST WARNING${NC}"
    echo -e "${RED}This will write $description to PRODUCTION BigQuery!${NC}"
    echo -e "${RED}This affects live business data.${NC}"
    read -p "$(echo -e ${RED}Type 'PRODUCTION TEST' to continue: ${NC})" confirm
    if [ "$confirm" != "PRODUCTION TEST" ]; then
        echo -e "${YELLOW}❌ Production test cancelled${NC}"
        exit 0
    fi
}

function confirm_production_full_test() {
    echo -e "\n${RED}🚨🚨 PRODUCTION FULL SYNC WARNING 🚨🚨${NC}"
    echo -e "${RED}This will sync ALL HubSpot data to PRODUCTION!${NC}"
    echo -e "${RED}This is a complete data refresh and will take significant time.${NC}"
    echo -e "${RED}This affects live business operations.${NC}"
    
    read -p "$(echo -e ${RED}Type 'FULL PRODUCTION SYNC' to continue: ${NC})" confirm
    if [ "$confirm" != "FULL PRODUCTION SYNC" ]; then
        echo -e "${YELLOW}❌ Full sync cancelled${NC}"
        exit 0
    fi
    
    echo -e "\n${RED}Final confirmation - this cannot be undone easily.${NC}"
    read -p "$(echo -e ${RED}Type 'YES' to proceed: ${NC})" final_confirm
    if [ "$final_confirm" != "YES" ]; then
        echo -e "${YELLOW}❌ Full sync cancelled${NC}"
        exit 0
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
    
    # If environment provided as argument, use it
    if [ $# -ge 1 ]; then
        ENVIRONMENT=$1
        if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
            echo -e "${RED}❌ Invalid environment: $ENVIRONMENT${NC}"
            echo -e "${YELLOW}Usage: $0 [dev|staging|prod]${NC}"
            exit 1
        fi
        echo -e "${GREEN}✅ Using command line argument: $ENVIRONMENT${NC}"
    else
        show_environment_menu
    fi
    
    show_test_menu "$ENVIRONMENT"
}

# Show help if requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "HubSpot Cloud Function Testing Script - Enhanced Version"
    echo ""
    echo "SCOPE:"
    echo "  ✅ Tests Cloud Functions (HTTP-triggered ingest function)"
    echo "  ✅ Tests BigQuery table creation and data insertion"
    echo "  ✅ Tests Pub/Sub event publishing (triggers scoring function)"
    echo "  ❌ Does NOT test local Python execution"
    echo "  ❌ Does NOT directly test scoring function execution"
    echo ""
    echo "FEATURES:"
    echo "  📊 Enhanced result parsing and display"
    echo "  🔍 BigQuery table status checking"
    echo "  📤 Scoring function trigger monitoring"
    echo "  🗑️ Dataset cleanup tools"
    echo "  💾 Result history and logging"
    echo ""
    echo "Usage: $0 [ENVIRONMENT]"
    echo ""
    echo "Interactive Mode:"
    echo "  $0           # Shows environment menu, then test menu"
    echo ""
    echo "Direct Mode:"
    echo "  $0 dev       # Test dev environment"
    echo "  $0 staging   # Test staging environment"
    echo "  $0 prod      # Test production environment"
    echo ""
    echo "Environments:"
    echo "  dev      Development (safe for all tests, DEBUG logging default)"
    echo "  staging  Staging (safe for small-medium tests, INFO logging default)"
    echo "  prod     Production (requires confirmation for real tests, WARN logging default)"
    echo ""
    echo "Test Types:"
    echo "  ping         Health check (no data processing)"
    echo "  dry-*        Dry runs (no BigQuery writes, no scoring trigger)"
    echo "  real-*       Real runs (writes to BigQuery, triggers scoring)"
    echo "  custom       Enter your own JSON payload"
    echo "  check-tables Verify BigQuery table status"
    echo "  cleanup      Delete dataset tables"
    echo ""
    echo "Results are saved to: ./test-results/"
    exit 0
fi

# Run main function
main "$@"