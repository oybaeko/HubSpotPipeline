#!/bin/bash

# HubSpot Ingest Function Deployment Script
# Usage: ./deploy.sh [dev|staging|prod] or interactive menu

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration functions - simpler than associative arrays
function get_function_name() {
    case $1 in
        dev) echo "hubspot-ingest-dev" ;;
        staging) echo "hubspot-ingest-staging" ;;
        prod) echo "hubspot-ingest-prod" ;;
    esac
}

function get_service_account() {
    case $1 in
        dev) echo "hubspot-dev-ob@hubspot-452402.iam.gserviceaccount.com" ;;
        staging) echo "hubspot-staging@hubspot-452402.iam.gserviceaccount.com" ;;
        prod) echo "hubspot-prod@hubspot-452402.iam.gserviceaccount.com" ;;
    esac
}

function get_dataset() {
    case $1 in
        dev) echo "Hubspot_dev_ob" ;;
        staging) echo "Hubspot_staging" ;;
        prod) echo "Hubspot_prod" ;;
    esac
}

# Common settings
PROJECT_ID="hubspot-452402"
REGION="europe-west1"
RUNTIME="python312"
SOURCE_DIR="src"
ENTRY_POINT="main"
TIMEOUT="540s"
MEMORY="512MB"

function print_header() {
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}    HubSpot Ingest Function Deployment${NC}"
    echo -e "${BLUE}================================================${NC}"
}

function show_environment_menu() {
    echo -e "\n${BLUE}🎯 Select deployment environment:${NC}"
    echo -e "  ${GREEN}1) dev      ${NC}(Development - safe for testing)"
    echo -e "  ${YELLOW}2) staging  ${NC}(Staging - requires confirmation)"
    echo -e "  ${RED}3) prod     ${NC}(Production - requires double confirmation)"
    echo -e "  ${BLUE}4) quit     ${NC}(Exit without deploying)"
    echo ""
    
    while true; do
        read -p "$(echo -e ${GREEN}Choose environment [1-4, default=1]: ${NC})" choice
        
        # Default to dev if Enter is pressed (empty input)
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
                echo -e "${BLUE}👋 Deployment cancelled${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}❌ Invalid choice. Please select 1-4.${NC}"
                ;;
        esac
    done
}

function validate_environment() {
    if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
        echo -e "${RED}❌ Invalid environment: $ENVIRONMENT${NC}"
        echo -e "${YELLOW}Usage: $0 [dev|staging|prod]${NC}"
        exit 1
    fi
}

function confirm_deployment() {
    local env=$1
    local warning_level=$2
    
    echo -e "\n${BLUE}📋 Deployment Summary:${NC}"
    echo -e "  Environment: ${YELLOW}$env${NC}"
    echo -e "  Function: $(get_function_name $env)"
    echo -e "  Service Account: $(get_service_account $env)"
    echo -e "  Dataset: $(get_dataset $env)"
    echo -e "  Region: $REGION"
    echo -e "  Runtime: $RUNTIME"
    
    if [ "$warning_level" = "high" ]; then
        echo -e "\n${RED}⚠️  PRODUCTION DEPLOYMENT WARNING ⚠️${NC}"
        echo -e "${RED}This will deploy to the PRODUCTION environment!${NC}"
        echo -e "${RED}This affects live data and real business operations.${NC}"
        
        read -p "$(echo -e ${RED}Type 'DEPLOY TO PRODUCTION' to continue: ${NC})" confirm
        if [ "$confirm" != "DEPLOY TO PRODUCTION" ]; then
            echo -e "${YELLOW}❌ Production deployment cancelled${NC}"
            exit 1
        fi
        
        echo -e "\n${RED}Final confirmation - are you absolutely sure?${NC}"
        read -p "$(echo -e ${RED}Type 'YES' to proceed: ${NC})" final_confirm
        if [ "$final_confirm" != "YES" ]; then
            echo -e "${YELLOW}❌ Production deployment cancelled${NC}"
            exit 1
        fi
        
    elif [ "$warning_level" = "medium" ]; then
        echo -e "\n${YELLOW}⚠️  STAGING DEPLOYMENT WARNING ⚠️${NC}"
        echo -e "${YELLOW}This will deploy to the STAGING environment.${NC}"
        echo -e "${YELLOW}This affects staging data used for testing.${NC}"
        
        read -p "$(echo -e ${YELLOW}Type 'yes' to continue: ${NC})" confirm
        if [ "$confirm" != "yes" ]; then
            echo -e "${YELLOW}❌ Staging deployment cancelled${NC}"
            exit 1
        fi
        
    else
        echo -e "\n${GREEN}✅ Development deployment${NC}"
        read -p "$(echo -e ${GREEN}Press Enter to continue or Ctrl+C to cancel${NC})"
    fi
}

function check_prerequisites() {
    echo -e "\n${BLUE}🔍 Checking prerequisites...${NC}"
    
    # Check if gcloud is installed and authenticated
    if ! command -v gcloud &> /dev/null; then
        echo -e "${RED}❌ gcloud CLI not found. Please install Google Cloud SDK.${NC}"
        exit 1
    fi
    
    # Check if authenticated
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
        echo -e "${RED}❌ Not authenticated with gcloud. Run: gcloud auth login${NC}"
        exit 1
    fi
    
    # Check if correct project is set
    current_project=$(gcloud config get-value project 2>/dev/null)
    if [ "$current_project" != "$PROJECT_ID" ]; then
        echo -e "${YELLOW}⚠️  Setting project to $PROJECT_ID${NC}"
        gcloud config set project $PROJECT_ID
    fi
    
    # Check if source directory exists
    if [ ! -d "$SOURCE_DIR" ]; then
        echo -e "${RED}❌ Source directory '$SOURCE_DIR' not found${NC}"
        exit 1
    fi
    
    # Check if main.py exists
    if [ ! -f "$SOURCE_DIR/main.py" ]; then
        echo -e "${RED}❌ Entry point '$SOURCE_DIR/main.py' not found${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✅ Prerequisites check passed${NC}"
}

function deploy_function() {
    local env=$1
    local function_name=$(get_function_name $env)
    local service_account=$(get_service_account $env)
    local dataset=$(get_dataset $env)
    
    echo -e "\n${BLUE}🚀 Deploying $function_name...${NC}"
    
    gcloud functions deploy $function_name \
        --runtime $RUNTIME \
        --trigger-http \
        --allow-unauthenticated \
        --source $SOURCE_DIR \
        --entry-point $ENTRY_POINT \
        --timeout $TIMEOUT \
        --memory $MEMORY \
        --region $REGION \
        --service-account $service_account \
        --set-env-vars BIGQUERY_DATASET_ID=$dataset \
        --quiet
    
    if [ $? -eq 0 ]; then
        echo -e "\n${GREEN}✅ Deployment successful!${NC}"
        echo -e "${GREEN}Function URL: https://$REGION-$PROJECT_ID.cloudfunctions.net/$function_name${NC}"
        
        # Show test commands
        echo -e "\n${BLUE}🧪 Test Commands:${NC}"
        echo -e "${YELLOW}# Safe test (dry run):${NC}"
        echo "curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/$function_name \\"
        echo "  -H 'Content-Type: application/json' \\"
        echo "  -d '{\"limit\": 5, \"dry_run\": true}'"
        
        if [ "$env" != "prod" ]; then
            echo -e "\n${YELLOW}# Real test (small):${NC}"
            echo "curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/$function_name \\"
            echo "  -H 'Content-Type: application/json' \\"
            echo "  -d '{\"limit\": 10, \"dry_run\": false}'"
        fi
        
    else
        echo -e "\n${RED}❌ Deployment failed${NC}"
        exit 1
    fi
}

function main() {
    print_header
    
    # If no argument provided, show interactive menu
    if [ $# -eq 0 ]; then
        show_environment_menu
    else
        # Use command line argument
        ENVIRONMENT=$1
        validate_environment
        echo -e "${GREEN}✅ Using command line argument: $ENVIRONMENT${NC}"
    fi
    
    check_prerequisites
    
    case $ENVIRONMENT in
        dev)
            confirm_deployment "dev" "low"
            deploy_function "dev"
            ;;
        staging)
            confirm_deployment "staging" "medium"
            deploy_function "staging"
            ;;
        prod)
            confirm_deployment "prod" "high"
            deploy_function "prod"
            ;;
    esac
    
    echo -e "\n${GREEN}🎉 Deployment completed successfully!${NC}"
}

# Show help if requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "HubSpot Ingest Function Deployment Script"
    echo ""
    echo "Usage: $0 [ENVIRONMENT]"
    echo ""
    echo "Interactive Mode:"
    echo "  $0           # Shows menu, dev is default (just press Enter)"
    echo ""
    echo "Direct Mode:"
    echo "  $0 dev       # Deploy to dev directly"
    echo "  $0 staging   # Deploy to staging directly"
    echo "  $0 prod      # Deploy to production directly"
    echo ""
    echo "Environments:"
    echo "  dev      Development (minimal guards)"
    echo "  staging  Staging (medium guards)"  
    echo "  prod     Production (maximum guards)"
    exit 0
fi

# Run main function with all arguments
main "$@"