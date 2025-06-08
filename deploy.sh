#!/bin/bash

# HubSpot Pipeline Functions Deployment Script - Updated for Two-Tier Testing
# Usage: ./deploy.sh [ingest|scoring] [dev|staging|prod] or interactive menu

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration functions
function get_function_name() {
    local func_type=$1
    local env=$2
    case $func_type in
        ingest)
            case $env in
                dev) echo "hubspot-ingest-dev" ;;
                staging) echo "hubspot-ingest-staging" ;;
                prod) echo "hubspot-ingest-prod" ;;
            esac
            ;;
        scoring)
            case $env in
                dev) echo "hubspot-scoring-dev" ;;
                staging) echo "hubspot-scoring-staging" ;;
                prod) echo "hubspot-scoring-prod" ;;
            esac
            ;;
    esac
}

function get_entry_point() {
    local func_type=$1
    case $func_type in
        ingest) echo "main" ;;
        scoring) echo "main" ;;
    esac
}

function get_source_file() {
    local func_type=$1
    case $func_type in
        ingest) echo "ingest_main.py" ;;
        scoring) echo "scoring_main.py" ;;
    esac
}

function get_trigger_type() {
    local func_type=$1
    local env=$2
    case $func_type in
        ingest) echo "--trigger-http --allow-unauthenticated" ;;
        scoring) 
            # Check if we want HTTP or Pub/Sub trigger for scoring
            if [ "${SCORING_HTTP_MODE:-false}" = "true" ]; then
                echo "--trigger-http --allow-unauthenticated"
            else
                local topic=$(get_pubsub_topic $env)
                echo "--trigger-topic=$topic"
            fi
            ;;
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

function get_pubsub_topic() {
    local env=$1
    case $env in
        dev) echo "hubspot-events-dev" ;;
        staging) echo "hubspot-events-staging" ;;
        prod) echo "hubspot-events-prod" ;;
        *) echo "hubspot-events-dev" ;;
    esac
}

# Common settings
PROJECT_ID="hubspot-452402"
REGION="europe-west1"
RUNTIME="python312"
SOURCE_DIR="src"
TIMEOUT="540s"
MEMORY="512MB"

function print_header() {
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}    HubSpot Pipeline Functions Deployment${NC}"
    echo -e "${BLUE}       (with Two-Tier Testing Framework)${NC}"
    echo -e "${BLUE}================================================${NC}"
}

function check_prerequisites() {
    echo -e "${BLUE}🔍 Checking prerequisites...${NC}"
    
    # Check if gcloud is installed and authenticated
    if ! command -v gcloud &> /dev/null; then
        echo -e "${RED}❌ gcloud CLI not found. Please install Google Cloud SDK.${NC}"
        return 1
    fi
    
    # Check if authenticated
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
        echo -e "${RED}❌ Not authenticated with gcloud. Run: gcloud auth login${NC}"
        return 1
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
        return 1
    fi
    
    # Check function-specific files based on what we're deploying
    if [ "$FUNCTION_TYPE" = "ingest" ] || [ "$FUNCTION_TYPE" = "both" ]; then
        if [ ! -f "$SOURCE_DIR/ingest_main.py" ]; then
            echo -e "${RED}❌ Ingest entry point '$SOURCE_DIR/ingest_main.py' not found${NC}"
            return 1
        fi
    fi
    
    if [ "$FUNCTION_TYPE" = "scoring" ] || [ "$FUNCTION_TYPE" = "both" ]; then
        if [ ! -f "$SOURCE_DIR/scoring_main.py" ]; then
            echo -e "${RED}❌ Scoring entry point '$SOURCE_DIR/scoring_main.py' not found${NC}"
            return 1
        fi
    fi
    
    # Check if requirements.txt exists
    if [ ! -f "$SOURCE_DIR/requirements.txt" ]; then
        echo -e "${RED}❌ Requirements file '$SOURCE_DIR/requirements.txt' not found${NC}"
        return 1
    fi
    
    echo -e "${GREEN}✅ Prerequisites check passed${NC}"
    return 0
}

function validate_inputs() {
    if [[ ! "$FUNCTION_TYPE" =~ ^(ingest|scoring|both)$ ]]; then
        echo -e "${RED}❌ Invalid function type: $FUNCTION_TYPE${NC}"
        echo -e "${YELLOW}Usage: $0 [ingest|scoring|both] [dev|staging|prod]${NC}"
        exit 1
    fi
    
    if [[ ! "$ENVIRONMENT" =~ ^(dev|staging|prod)$ ]]; then
        echo -e "${RED}❌ Invalid environment: $ENVIRONMENT${NC}"
        echo -e "${YELLOW}Usage: $0 [ingest|scoring|both] [dev|staging|prod]${NC}"
        exit 1
    fi
}

function show_function_menu() {
    echo -e "\n${BLUE}🔧 Select function to deploy:${NC}"
    echo -e "  ${GREEN}1) ingest   ${NC}(Ingest function - HTTP trigger)"
    echo -e "  ${YELLOW}2) scoring  ${NC}(Scoring function - Pub/Sub trigger)"
    echo -e "  ${BLUE}3) both     ${NC}(Deploy both functions)"
    echo -e "  ${BLUE}4) quit     ${NC}(Exit without deploying)"
    echo ""
    
    while true; do
        read -p "$(echo -e ${GREEN}Choose function [1-4, default=1]: ${NC})" choice
        
        # Default to ingest if Enter is pressed
        if [ -z "$choice" ]; then
            choice="1"
        fi
        
        case $choice in
            1|ingest)
                FUNCTION_TYPE="ingest"
                echo -e "${GREEN}✅ Selected: Ingest Function${NC}"
                break
                ;;
            2|scoring)
                FUNCTION_TYPE="scoring"
                echo -e "${YELLOW}✅ Selected: Scoring Function${NC}"
                break
                ;;
            3|both)
                FUNCTION_TYPE="both"
                echo -e "${BLUE}✅ Selected: Both Functions${NC}"
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

function show_environment_menu() {
    echo -e "\n${BLUE}🎯 Select deployment environment:${NC}"
    echo -e "  ${GREEN}1) dev      ${NC}(Development - safe for testing)"
    echo -e "  ${YELLOW}2) staging  ${NC}(Staging - requires confirmation)"
    echo -e "  ${RED}3) prod     ${NC}(Production - requires double confirmation)"
    echo -e "  ${BLUE}4) quit     ${NC}(Exit without deploying)"
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
                echo -e "${BLUE}👋 Deployment cancelled${NC}"
                exit 0
                ;;
            *)
                echo -e "${RED}❌ Invalid choice. Please select 1-4.${NC}"
                ;;
        esac
    done
}

function confirm_deployment() {
    local func_type=$1
    local env=$2
    local warning_level=$3
    
    echo -e "\n${BLUE}📋 Deployment Summary:${NC}"
    echo -e "  Function Type: ${YELLOW}$func_type${NC}"
    echo -e "  Environment: ${YELLOW}$env${NC}"
    
    if [ "$func_type" = "both" ]; then
        echo -e "  Functions:"
        echo -e "    - $(get_function_name ingest $env) (HTTP trigger)"
        echo -e "    - $(get_function_name scoring $env) (Pub/Sub trigger)"
    else
        echo -e "  Function: $(get_function_name $func_type $env)"
        echo -e "  Entry Point: $(get_entry_point $func_type)"
        echo -e "  Trigger: $(get_trigger_type $func_type $env)"
    fi
    
    echo -e "  Service Account: $(get_service_account $env)"
    echo -e "  Dataset: $(get_dataset $env)"
    echo -e "  Region: $REGION"
    echo -e "  Runtime: $RUNTIME"
    echo -e "  Two-Tier Testing: ${GREEN}✅ Included${NC}"
    
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

function create_gcloudignore() {
    local source_dir=$1
    
    echo -e "${BLUE}📁 Creating .gcloudignore for two-tier testing framework...${NC}"
    
    # Create .gcloudignore that includes tests but excludes unnecessary files
    cat > "$source_dir/.gcloudignore" << 'EOF'
# Python bytecode
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
env/
venv/
ENV/
env.bak/
venv.bak/
.venv/

# IDE files
.vscode/
.idea/
*.swp
*.swo
*~

# OS files
.DS_Store
Thumbs.db

# Git
.git/
.gitignore

# Documentation
*.md
docs/

# Local development files
.env
.env.local
.env.development
.env.test
.env.production

# Test results and coverage
htmlcov/
.coverage
.coverage.*
coverage.xml
*.cover
.hypothesis/
.pytest_cache/
test-results/
comprehensive-test-results.json

# Jupyter Notebooks
.ipynb_checkpoints

# Excel import (not needed in Cloud Functions)
excel_import/

# Development scripts (KEEP tests/ directory for two-tier testing)
invoke_cloud_function.py
test-ingest.sh
validate-framework.sh

# OLD test files to exclude (if they still exist)
tests/test_framework_validation.py
tests/test_infrastructure.py
tests/test_database_ops.py
tests/test_events.py
tests/test_logging.py
tests/fixtures/bigquery_fixtures.py
tests/fixtures/pubsub_fixtures.py
tests/markers/production_safe.py
EOF

    echo -e "${GREEN}✅ Created .gcloudignore - two-tier tests included, old tests excluded${NC}"
}

function prepare_function_source() {
    local func_type=$1
    local source_file=$(get_source_file $func_type)
    
    echo -e "${BLUE}📁 Preparing source for $func_type function...${NC}"
    echo -e "${YELLOW}ℹ️  Using enhanced entry point with two-tier testing framework${NC}"
    
    # Create .gcloudignore to ensure tests are included
    create_gcloudignore "$SOURCE_DIR"
    
    # Copy the appropriate entry point to main.py
    if [ "$func_type" = "scoring" ]; then
        cp "$SOURCE_DIR/scoring_main.py" "$SOURCE_DIR/main.py"
        echo -e "${GREEN}✅ Using enhanced scoring_main.py (with two-tier testing)${NC}"
    else
        cp "$SOURCE_DIR/ingest_main.py" "$SOURCE_DIR/main.py"
        echo -e "${GREEN}✅ Using enhanced ingest_main.py (with two-tier testing)${NC}"
    fi
    
    # Verify two-tier testing framework files exist
    echo -e "${BLUE}🔍 Verifying two-tier testing framework files...${NC}"
    local required_test_files=(
        "tests/__init__.py"
        "tests/conftest.py"
        "tests/deployment_validation.py"
        "tests/runtime_validation.py"
        "tests/fixtures/__init__.py"
        "tests/fixtures/test_session.py"
        "tests/pytest.ini"
    )
    
    local missing_files=()
    for file in "${required_test_files[@]}"; do
        if [ -f "$SOURCE_DIR/$file" ]; then
            echo -e "${GREEN}  ✅ $file${NC}"
        else
            echo -e "${RED}  ❌ $file${NC}"
            missing_files+=("$file")
        fi
    done
    
    if [ ${#missing_files[@]} -gt 0 ]; then
        echo -e "${RED}❌ Missing two-tier testing framework files. Please ensure all test files are present.${NC}"
        return 1
    fi
    
    echo -e "${GREEN}✅ Two-tier testing framework files verified${NC}"
}

function check_requirements() {
    echo -e "${BLUE}📦 Checking requirements.txt for testing dependencies...${NC}"
    
    local req_file="$SOURCE_DIR/requirements.txt"
    
    if grep -q "pytest" "$req_file"; then
        echo -e "${GREEN}✅ pytest found in requirements.txt${NC}"
    else
        echo -e "${RED}❌ pytest missing from requirements.txt${NC}"
        echo -e "${YELLOW}💡 Adding pytest dependencies...${NC}"
        
        # Add pytest dependencies if missing
        echo "" >> "$req_file"
        echo "# Testing dependencies (required for two-tier testing in Cloud Functions)" >> "$req_file"
        echo "pytest>=7.0.0" >> "$req_file"
        echo "pytest-json-report>=1.5.0" >> "$req_file"
        
        echo -e "${GREEN}✅ Added pytest dependencies to requirements.txt${NC}"
    fi
    
    if grep -q "pytest-json-report" "$req_file"; then
        echo -e "${GREEN}✅ pytest-json-report found in requirements.txt${NC}"
    else
        echo -e "${YELLOW}⚠️ pytest-json-report missing - JSON reports may not be available${NC}"
    fi
}

# Fix for the deploy_single_function - only change the deployment part
function deploy_single_function() {
    local func_type=$1
    local env=$2
    local function_name=$(get_function_name $func_type $env)
    local entry_point=$(get_entry_point $func_type)
    local trigger=$(get_trigger_type $func_type $env)
    local service_account=$(get_service_account $env)
    local dataset=$(get_dataset $env)
    
    echo -e "\n${BLUE}🚀 Deploying $function_name with two-tier testing framework...${NC}"
    echo -e "${BLUE}   Entry Point: $entry_point${NC}"
    echo -e "${BLUE}   Trigger: $trigger${NC}"
    
    # Check requirements
    check_requirements
    
    # For scoring functions, ensure the topic exists first
    if [ "$func_type" = "scoring" ]; then
        local topic=$(get_pubsub_topic $env)
        echo -e "${BLUE}   Ensuring Pub/Sub topic exists: $topic${NC}"
        
        if ! gcloud pubsub topics describe $topic --project=$PROJECT_ID &>/dev/null; then
            echo -e "${YELLOW}   Creating topic: $topic${NC}"
            gcloud pubsub topics create $topic --project=$PROJECT_ID
        else
            echo -e "${GREEN}   Topic exists: $topic${NC}"
        fi
    fi
    
    # Prepare the main.py file for this function type
    prepare_function_source $func_type
    if [ $? -ne 0 ]; then
        echo -e "${RED}❌ Failed to prepare function source${NC}"
        return 1
    fi
    
    # Build the deployment command
    deploy_cmd="gcloud functions deploy $function_name \
        --runtime $RUNTIME \
        $trigger \
        --source $SOURCE_DIR \
        --entry-point $entry_point \
        --timeout $TIMEOUT \
        --memory $MEMORY \
        --region $REGION \
        --service-account $service_account \
        --set-env-vars BIGQUERY_DATASET_ID=$dataset,BIGQUERY_PROJECT_ID=$PROJECT_ID,ENVIRONMENT=$env \
        --quiet"
    
    echo -e "${BLUE}⏳ Executing deployment...${NC}"
    echo -e "${BLUE}📝 Deployment will use main.py created from ${func_type}_main.py${NC}"
    
    # Execute deployment and capture result BEFORE cleanup
    eval $deploy_cmd
    deployment_result=$?
    
    # Only clean up AFTER deployment completes (success or failure)
    echo -e "${BLUE}🧹 Cleaning up generated main.py after deployment...${NC}"
    if [ -f "$SOURCE_DIR/main.py" ]; then
        rm "$SOURCE_DIR/main.py"
        echo -e "${BLUE}✅ Cleaned up generated main.py${NC}"
    fi
    
    if [ $deployment_result -eq 0 ]; then
        echo -e "\n${GREEN}✅ $function_name deployed successfully with two-tier testing framework!${NC}"
        
        # Apply secure permissions for scoring function
        if [ "$func_type" = "scoring" ]; then
            echo -e "${BLUE}🔒 Applying secure permissions for scoring function...${NC}"
            
            # Get project number for Eventarc service account
            project_number=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
            eventarc_sa="service-${project_number}@gcp-sa-eventarc.iam.gserviceaccount.com"
            
            # Grant Eventarc permission to invoke the function
            gcloud run services add-iam-policy-binding $function_name \
                --region=$REGION \
                --member="serviceAccount:$eventarc_sa" \
                --role="roles/run.invoker" \
                --quiet
            
            local topic=$(get_pubsub_topic $env)
            echo -e "${GREEN}✅ Secure permissions applied to $function_name${NC}"
            echo -e "${GREEN}   Topic: $topic${NC}"
            echo -e "${GREEN}   Eventarc SA: $eventarc_sa${NC}"
        fi
        
        # Show two-tier test command examples
        echo -e "\n${CYAN}🧪 Two-Tier Testing Framework Usage:${NC}"
        
        if [ "$func_type" = "ingest" ]; then
            echo -e "Function URL: https://$REGION-$PROJECT_ID.cloudfunctions.net/$function_name"
            
            echo -e "\n${YELLOW}# Tier 1: Environment-specific deployment validation:${NC}"
            echo "curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/$function_name \\"
            echo "  -H 'Content-Type: application/json' \\"
            echo "  -d '{\"mode\": \"test\", \"test_type\": \"deployment\"}'"
            
            echo -e "\n${YELLOW}# Tier 2: Basic runtime mechanism validation:${NC}"
            echo "curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/$function_name \\"
            echo "  -H 'Content-Type: application/json' \\"
            echo "  -d '{\"mode\": \"test\", \"test_type\": \"runtime\"}'"
            
            echo -e "\n${YELLOW}# Legacy compatibility (maps to appropriate tier):${NC}"
            echo "curl -X POST https://$REGION-$PROJECT_ID.cloudfunctions.net/$function_name \\"
            echo "  -H 'Content-Type: application/json' \\"
            echo "  -d '{\"mode\": \"test\", \"test_type\": \"infrastructure\"}'"
            
            local topic=$(get_pubsub_topic $env)
            echo -e "\nWill publish to topic: $topic"
            
        else
            local topic=$(get_pubsub_topic $env)
            echo -e "${GREEN}Scoring function deployed with Pub/Sub trigger on: $topic${NC}"
            
            echo -e "\n${YELLOW}# Tier 1: Test scoring function deployment validation via Pub/Sub:${NC}"
            echo "gcloud pubsub topics publish $topic \\"
            echo "  --message='{\"type\":\"hubspot.test.request\",\"data\":{\"test_type\":\"deployment\"}}'"
            
            echo -e "\n${YELLOW}# Tier 2: Test scoring function runtime validation via Pub/Sub:${NC}"
            echo "gcloud pubsub topics publish $topic \\"
            echo "  --message='{\"type\":\"hubspot.test.request\",\"data\":{\"test_type\":\"runtime\"}}'"
            
            echo -e "\n${BLUE}🧪 Test by triggering the $env ingest function${NC}"
        fi
        
    else
        echo -e "\n${RED}❌ Deployment of $function_name failed${NC}"
        echo -e "${RED}Check the Cloud Build logs for more details${NC}"
        return 1
    fi
}

function deploy_functions() {
    local func_type=$1
    local env=$2
    
    if [ "$func_type" = "both" ]; then
        echo -e "\n${BLUE}📦 Deploying both functions...${NC}"
        
        # Deploy ingest first
        deploy_single_function "ingest" "$env"
        ingest_result=$?
        
        # Deploy scoring second
        deploy_single_function "scoring" "$env"  
        scoring_result=$?
        
        if [ $ingest_result -eq 0 ] && [ $scoring_result -eq 0 ]; then
            echo -e "\n${GREEN}🎉 Both functions deployed successfully!${NC}"
        else
            echo -e "\n${RED}❌ One or more deployments failed${NC}"
            exit 1
        fi
    else
        deploy_single_function "$func_type" "$env"
        if [ $? -ne 0 ]; then
            exit 1
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
    
    # Parse command line arguments
    if [ $# -eq 0 ]; then
        # Interactive mode
        show_function_menu
        show_environment_menu
    elif [ $# -eq 1 ]; then
        # Only function type provided
        FUNCTION_TYPE=$1
        validate_inputs
        show_environment_menu
    elif [ $# -eq 2 ]; then
        # Both function type and environment provided
        FUNCTION_TYPE=$1
        ENVIRONMENT=$2
        validate_inputs
        echo -e "${GREEN}✅ Using command line arguments: $FUNCTION_TYPE -> $ENVIRONMENT${NC}"
    else
        echo -e "${RED}❌ Too many arguments${NC}"
        echo -e "${YELLOW}Usage: $0 [ingest|scoring|both] [dev|staging|prod]${NC}"
        exit 1
    fi
    
    check_prerequisites
    
    case $ENVIRONMENT in
        dev)
            confirm_deployment "$FUNCTION_TYPE" "dev" "low"
            deploy_functions "$FUNCTION_TYPE" "dev"
            ;;
        staging)
            confirm_deployment "$FUNCTION_TYPE" "staging" "medium"
            deploy_functions "$FUNCTION_TYPE" "staging"
            ;;
        prod)
            confirm_deployment "$FUNCTION_TYPE" "prod" "high"
            deploy_functions "$FUNCTION_TYPE" "prod"
            ;;
    esac
    
    echo -e "\n${GREEN}🎉 Deployment completed successfully with two-tier testing framework!${NC}"
}

# Show help if requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    echo "HubSpot Pipeline Functions Deployment Script - Enhanced with Two-Tier Testing Framework"
    echo ""
    echo "Usage: $0 [FUNCTION_TYPE] [ENVIRONMENT]"
    echo ""
    echo "NEW: Two-tier testing framework automatically included in deployments"
    echo "✅ Tier 1: Environment-specific deployment validation"
    echo "✅ Tier 2: Basic runtime mechanism validation"
    echo "✅ Production-safe testing with automatic cleanup"
    echo "✅ Ready for deployment confidence validation"
    echo ""
    echo "Interactive Mode:"
    echo "  $0                    # Shows menus for both function and environment"
    echo "  $0 ingest            # Shows environment menu for ingest function"
    echo ""
    echo "Direct Mode:"
    echo "  $0 ingest dev        # Deploy ingest to dev"
    echo "  $0 scoring staging   # Deploy scoring to staging"
    echo "  $0 both prod         # Deploy both to production"
    echo ""
    echo "Function Types:"
    echo "  ingest    HTTP-triggered function for data ingestion"
    echo "  scoring   Pub/Sub-triggered function for data scoring"
    echo "  both      Deploy both functions"
    echo ""
    echo "Environments:"
    echo "  dev       Development (minimal guards)"
    echo "  staging   Staging (medium guards)"  
    echo "  prod      Production (maximum guards)"
    echo ""
    echo "Two-Tier Testing Framework:"
    echo "  After deployment, test with:"
    echo ""
    echo "  # Tier 1: Environment-specific validation"
    echo "  curl -d '{\"mode\": \"test\", \"test_type\": \"deployment\"}' [function-url]"
    echo ""
    echo "  # Tier 2: Runtime mechanism validation"
    echo "  curl -d '{\"mode\": \"test\", \"test_type\": \"runtime\"}' [function-url]"
    echo ""
    echo "  # Legacy compatibility (automatically maps to appropriate tier)"
    echo "  curl -d '{\"mode\": \"test\", \"test_type\": \"infrastructure\"}' [function-url]"
    exit 0
fi

# Run main function with all arguments
main "$@"