#!/bin/bash

# HubSpot Ingest Function Testing Script
# Usage: ./test-ingest.sh [dev|staging|prod] [test_type]

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ID="hubspot-452402"
REGION="europe-west1"

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
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}      HubSpot Ingest Function Tester${NC}"
    echo -e "${BLUE}================================================${NC}"
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
    echo -e "  ${BLUE}15) back        ${NC}(Back to environment selection)"
    echo -e "  ${BLUE}16) quit        ${NC}(Exit)"
    echo ""
    echo -e "${CYAN}Target URL: ${url}${NC}"
    echo -e "${CYAN}Current Log Level: ${SESSION_LOG_LEVEL}${NC}"
    
    while true; do
        read -p "$(echo -e ${GREEN}Choose test [1-16]: ${NC})" choice
        
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
            15) return ;;
            16|quit|q)
                echo -e "${BLUE}👋 Testing cancelled${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}❌ Invalid choice. Please select 1-16.${NC}"
                ;;
        esac
        
        echo -e "\n${CYAN}Press Enter to continue...${NC}"
        read
        show_test_menu "$env"
        break
    done
}

function run_test() {
    local env=$1
    local test_type=$2
    local url=$(get_function_url $env)
    
    # Use session log level or environment default
    local log_level=${SESSION_LOG_LEVEL:-$(get_default_log_level $env)}
    
    echo -e "\n${YELLOW}🚀 Running $test_type test on $env environment...${NC}"
    echo -e "${CYAN}URL: $url${NC}"
    echo -e "${CYAN}Log Level: $log_level${NC}"
    
    case $test_type in
        ping)
            echo -e "${BLUE}📡 Sending ping (empty POST with log level)${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"log_level\": \"$log_level\", \"trigger_source\": \"test-script-ping\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
            ;;
        dry-tiny)
            echo -e "${BLUE}🧪 Dry run - 2 records${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 2, \"dry_run\": true, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-dry-tiny\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
            ;;
        dry-small)
            echo -e "${BLUE}🧪 Dry run - 10 records${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 10, \"dry_run\": true, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-dry-small\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
            ;;
        dry-medium)
            echo -e "${BLUE}🧪 Dry run - 50 records${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 50, \"dry_run\": true, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-dry-medium\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
            ;;
        dry-paging)
            echo -e "${BLUE}🧪 Dry run - 150 records (tests HubSpot paging)${NC}"
            echo -e "${CYAN}ℹ️  This tests pagination since HubSpot returns max 100 per API call${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 150, \"dry_run\": true, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-dry-paging\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
            ;;
        dry-nolimit)
            echo -e "${BLUE}🧪 Dry run - ALL records (no limit, no BigQuery writes)${NC}"
            echo -e "${CYAN}ℹ️  This fetches all data from HubSpot but doesn't write to BigQuery${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"no_limit\": true, \"dry_run\": true, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-dry-nolimit\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
            ;;
        real-tiny)
            if [ "$env" = "prod" ]; then
                confirm_production_test "2 records"
            fi
            echo -e "${GREEN}💾 Real run - 2 records (writes to BigQuery)${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 2, \"dry_run\": false, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-real-tiny\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
            ;;
        real-small)
            if [ "$env" = "prod" ]; then
                confirm_production_test "10 records"
            fi
            echo -e "${GREEN}💾 Real run - 10 records (writes to BigQuery)${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 10, \"dry_run\": false, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-real-small\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
            ;;
        real-medium)
            if [ "$env" = "prod" ]; then
                confirm_production_test "50 records"
            fi
            echo -e "${YELLOW}💾 Real run - 50 records (writes to BigQuery)${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 50, \"dry_run\": false, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-real-medium\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
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
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
            ;;
        real-large)
            if [ "$env" = "staging" ] || [ "$env" = "prod" ]; then
                confirm_large_test "$env"
            fi
            echo -e "${RED}💾 Real run - 500 records (writes to BigQuery)${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"limit\": 500, \"dry_run\": false, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-real-large\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
            ;;
        real-full)
            confirm_production_full_test
            echo -e "${RED}🚨 FULL SYNC - ALL RECORDS${NC}"
            curl -X POST "$url" \
                -H "Content-Type: application/json" \
                -d "{\"no_limit\": true, \"dry_run\": false, \"log_level\": \"$log_level\", \"trigger_source\": \"test-script-real-full\"}" \
                -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
            ;;
    esac
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
    
    echo -e "\n${YELLOW}🚀 Running custom test...${NC}"
    echo -e "${CYAN}URL: $url${NC}"
    echo -e "${CYAN}Payload: $payload${NC}"
    
    curl -X POST "$url" \
        -H "Content-Type: application/json" \
        -d "$payload" \
        -w "\n\n⏱️  Total time: %{time_total}s\n📊 HTTP status: %{http_code}\n"
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
    echo "HubSpot Ingest Function Testing Script"
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
    echo "  dry-*        Dry runs (no BigQuery writes)"
    echo "  real-*       Real runs (writes to BigQuery)"
    echo "  custom       Enter your own JSON payload"
    echo "  log-level    Change logging verbosity (DEBUG/INFO/WARN/ERROR)"
    exit 0
fi

# Run main function
main "$@"