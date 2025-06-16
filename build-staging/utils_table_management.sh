#!/bin/bash

# HubSpot Staging Table Management Module
# Handles BigQuery table operations for staging environment

# Colors and formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
PURPLE='\033[0;35m'
BOLD='\033[1m'
NC='\033[0m'

# Configuration
PROJECT_ID="hubspot-452402"
STAGING_DATASET="Hubspot_staging"

# Table definitions with schemas (matching production schema.py)
declare -A TABLE_SCHEMAS=(
    ["hs_companies"]="company_id:STRING,company_name:STRING,lifecycle_stage:STRING,lead_status:STRING,hubspot_owner_id:STRING,company_type:STRING,development_category:STRING,hiring_developers:STRING,inhouse_developers:STRING,proff_likviditetsgrad:STRING,proff_link:STRING,proff_lonnsomhet:STRING,proff_soliditet:STRING,snapshot_id:STRING,timestamp:TIMESTAMP"
    ["hs_deals"]="deal_id:STRING,deal_name:STRING,deal_stage:STRING,deal_type:STRING,amount:FLOAT,owner_id:STRING,associated_company_id:STRING,timestamp:TIMESTAMP,snapshot_id:STRING"
    ["hs_owners"]="owner_id:STRING,email:STRING,first_name:STRING,last_name:STRING,user_id:STRING,active:BOOLEAN,timestamp:TIMESTAMP"
    ["hs_contacts"]="contact_id:STRING,email:STRING,first_name:STRING,last_name:STRING,hubspot_owner_id:STRING,phone:STRING,job_title:STRING,lifecycle_stage:STRING,snapshot_id:STRING,timestamp:TIMESTAMP"
    ["hs_deal_stage_reference"]="pipeline_id:STRING,pipeline_label:STRING,stage_id:STRING,stage_label:STRING,is_closed:BOOLEAN,probability:FLOAT,display_order:INTEGER"
    ["hs_snapshot_registry"]="snapshot_id:STRING,snapshot_timestamp:TIMESTAMP,triggered_by:STRING,status:STRING,notes:STRING"
    ["hs_stage_mapping"]="lifecycle_stage:STRING,lead_status:STRING,deal_stage:STRING,combined_stage:STRING,stage_level:INTEGER,adjusted_score:FLOAT"
    ["hs_pipeline_units_snapshot"]="snapshot_id:STRING,snapshot_timestamp:TIMESTAMP,company_id:STRING,deal_id:STRING,owner_id:STRING,lifecycle_stage:STRING,lead_status:STRING,deal_stage:STRING,combined_stage:STRING,stage_level:INTEGER,adjusted_score:FLOAT,stage_source:STRING"
    ["hs_pipeline_score_history"]="snapshot_id:STRING,owner_id:STRING,combined_stage:STRING,num_companies:INTEGER,total_score:FLOAT,snapshot_timestamp:TIMESTAMP"
)

# Core tables that must exist for basic functionality (matching production)
CORE_TABLES=("hs_companies" "hs_deals" "hs_owners" "hs_contacts")

# All tables in dependency order (matching production schema)
ALL_TABLES=("hs_owners" "hs_deal_stage_reference" "hs_stage_mapping" "hs_companies" "hs_contacts" "hs_deals" "hs_snapshot_registry" "hs_pipeline_units_snapshot" "hs_pipeline_score_history")

function print_header() {
    echo -e "${BOLD}${BLUE}================================================================${NC}"
    echo -e "${BOLD}${BLUE}        HubSpot Staging Table Management${NC}"
    echo -e "${BOLD}${BLUE}================================================================${NC}"
    echo -e "${CYAN}📊 Project: $PROJECT_ID${NC}"
    echo -e "${CYAN}🗄️  Dataset: $STAGING_DATASET${NC}"
    echo -e "${CYAN}📋 Tables: ${#TABLE_SCHEMAS[@]} defined${NC}"
    echo -e "${BLUE}================================================================${NC}"
    echo ""
}

function check_prerequisites() {
    echo -e "${BLUE}🔍 Checking prerequisites...${NC}"
    
    # Check if bq CLI is available
    if ! command -v bq &> /dev/null; then
        echo -e "${RED}❌ BigQuery CLI (bq) not found${NC}"
        echo -e "${YELLOW}💡 Install: gcloud components install bq${NC}"
        return 1
    fi
    
    # Check authentication
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
        echo -e "${RED}❌ Not authenticated with gcloud${NC}"
        echo -e "${YELLOW}💡 Run: gcloud auth login${NC}"
        return 1
    fi
    
    # Set project
    local current_project=$(gcloud config get-value project 2>/dev/null)
    if [ "$current_project" != "$PROJECT_ID" ]; then
        echo -e "${YELLOW}🔧 Setting project to $PROJECT_ID...${NC}"
        gcloud config set project $PROJECT_ID
    fi
    
    # Check if dataset exists, create if not
    if ! bq ls "$PROJECT_ID:$STAGING_DATASET" >/dev/null 2>&1; then
        echo -e "${YELLOW}📊 Creating staging dataset...${NC}"
        bq mk --dataset "$PROJECT_ID:$STAGING_DATASET"
        echo -e "${GREEN}✅ Created dataset: $STAGING_DATASET${NC}"
    fi
    
    echo -e "${GREEN}✅ Prerequisites check passed${NC}"
    return 0
}

function get_table_status() {
    local table_name=$1
    local table_id="$PROJECT_ID:$STAGING_DATASET.$table_name"
    
    if bq show "$table_id" >/dev/null 2>&1; then
        local row_count=$(bq query --use_legacy_sql=false --format=csv --quiet \
            "SELECT COUNT(*) FROM \`$table_id\`" 2>/dev/null | tail -1)
        
        if [ -n "$row_count" ] && [ "$row_count" != "0" ]; then
            echo "EXISTS_WITH_DATA:$row_count"
        else
            echo "EXISTS_EMPTY"
        fi
    else
        echo "NOT_EXISTS"
    fi
}

function show_all_table_status() {
    echo -e "${BLUE}📋 Current Table Status:${NC}"
    echo -e "${BLUE}═════════════════════════${NC}"
    
    local total_tables=${#ALL_TABLES[@]}
    local existing_tables=0
    local tables_with_data=0
    local total_rows=0
    
    for table in "${ALL_TABLES[@]}"; do
        local status=$(get_table_status "$table")
        local status_type=$(echo "$status" | cut -d':' -f1)
        local row_count=$(echo "$status" | cut -d':' -f2)
        
        case $status_type in
            "EXISTS_WITH_DATA")
                echo -e "${GREEN}✅ $table${NC} - ${GREEN}$row_count rows${NC}"
                ((existing_tables++))
                ((tables_with_data++))
                total_rows=$((total_rows + row_count))
                ;;
            "EXISTS_EMPTY")
                echo -e "${YELLOW}📝 $table${NC} - ${YELLOW}Empty table${NC}"
                ((existing_tables++))
                ;;
            "NOT_EXISTS")
                echo -e "${RED}❌ $table${NC} - ${RED}Not created${NC}"
                ;;
        esac
    done
    
    echo -e "${BLUE}═════════════════════════${NC}"
    echo -e "${CYAN}📊 Summary:${NC}"
    echo -e "${CYAN}  • Total tables: $total_tables${NC}"
    echo -e "${CYAN}  • Existing: $existing_tables${NC}"
    echo -e "${CYAN}  • With data: $tables_with_data${NC}"
    echo -e "${CYAN}  • Total rows: $total_rows${NC}"
    echo ""
}

function create_table() {
    local table_name=$1
    local force_recreate=${2:-false}
    
    echo -e "${CYAN}📝 Creating table: $table_name${NC}"
    
    local table_id="$PROJECT_ID:$STAGING_DATASET.$table_name"
    local schema="${TABLE_SCHEMAS[$table_name]}"
    
    if [ -z "$schema" ]; then
        echo -e "${RED}❌ No schema defined for table: $table_name${NC}"
        return 1
    fi
    
    # Check if table already exists
    if bq show "$table_id" >/dev/null 2>&1; then
        if [ "$force_recreate" = "true" ]; then
            echo -e "${YELLOW}🗑️  Dropping existing table...${NC}"
            bq rm -f "$table_id"
        else
            echo -e "${YELLOW}⚠️  Table already exists: $table_name${NC}"
            return 0
        fi
    fi
    
    # Create the table with schema
    echo -e "${BLUE}🔨 Creating table with schema...${NC}"
    if bq mk --table "$table_id" "$schema"; then
        echo -e "${GREEN}✅ Successfully created: $table_name${NC}"
        return 0
    else
        echo -e "${RED}❌ Failed to create: $table_name${NC}"
        return 1
    fi
}

function create_all_tables() {
    local force_recreate=${1:-false}
    
    echo -e "${BLUE}🏗️  Creating all tables...${NC}"
    
    if [ "$force_recreate" = "true" ]; then
        echo -e "${YELLOW}⚠️  Force recreate mode - existing tables will be dropped${NC}"
        read -p "$(echo -e ${YELLOW}Type 'yes' to continue: ${NC})" confirm
        if [ "$confirm" != "yes" ]; then
            echo -e "${YELLOW}❌ Operation cancelled${NC}"
            return 0
        fi
    fi
    
    local created_count=0
    local failed_count=0
    
    # Create tables in dependency order
    for table in "${ALL_TABLES[@]}"; do
        if create_table "$table" "$force_recreate"; then
            ((created_count++))
        else
            ((failed_count++))
        fi
        echo ""
    done
    
    echo -e "${BOLD}${BLUE}📊 CREATE TABLES SUMMARY${NC}"
    echo -e "${BLUE}═══════════════════════════${NC}"
    echo -e "${GREEN}✅ Created: $created_count tables${NC}"
    echo -e "${RED}❌ Failed: $failed_count tables${NC}"
    
    if [ $failed_count -eq 0 ]; then
        echo -e "${GREEN}🎉 All tables created successfully!${NC}"
    else
        echo -e "${RED}⚠️  Some table creation failed${NC}"
        return 1
    fi
    
    return 0
}

function drop_table() {
    local table_name=$1
    local table_id="$PROJECT_ID:$STAGING_DATASET.$table_name"
    
    echo -e "${CYAN}🗑️  Dropping table: $table_name${NC}"
    
    if ! bq show "$table_id" >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  Table doesn't exist: $table_name${NC}"
        return 0
    fi
    
    # Check if table has data
    local row_count=$(bq query --use_legacy_sql=false --format=csv --quiet \
        "SELECT COUNT(*) FROM \`$table_id\`" 2>/dev/null | tail -1)
    
    if [ -n "$row_count" ] && [ "$row_count" != "0" ]; then
        echo -e "${YELLOW}⚠️  Table contains $row_count rows${NC}"
        read -p "$(echo -e ${YELLOW}Are you sure you want to drop it? [y/N]: ${NC})" confirm
        if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
            echo -e "${YELLOW}❌ Operation cancelled${NC}"
            return 0
        fi
    fi
    
    if bq rm -f "$table_id"; then
        echo -e "${GREEN}✅ Successfully dropped: $table_name${NC}"
        return 0
    else
        echo -e "${RED}❌ Failed to drop: $table_name${NC}"
        return 1
    fi
}

function drop_all_tables() {
    echo -e "${RED}🗑️  Dropping all tables...${NC}"
    echo -e "${RED}⚠️  This will delete all data in staging tables!${NC}"
    
    read -p "$(echo -e ${RED}Type 'DROP ALL STAGING TABLES' to continue: ${NC})" confirm
    if [ "$confirm" != "DROP ALL STAGING TABLES" ]; then
        echo -e "${YELLOW}❌ Operation cancelled${NC}"
        return 0
    fi
    
    local dropped_count=0
    local failed_count=0
    
    # Drop tables in reverse dependency order
    for ((i=${#ALL_TABLES[@]}-1; i>=0; i--)); do
        local table="${ALL_TABLES[i]}"
        if drop_table "$table"; then
            ((dropped_count++))
        else
            ((failed_count++))
        fi
    done
    
    echo -e "${BOLD}${BLUE}📊 DROP TABLES SUMMARY${NC}"
    echo -e "${BLUE}═══════════════════════════${NC}"
    echo -e "${GREEN}✅ Dropped: $dropped_count tables${NC}"
    echo -e "${RED}❌ Failed: $failed_count tables${NC}"
    
    return 0
}

function clear_table_data() {
    local table_name=$1
    local table_id="$PROJECT_ID:$STAGING_DATASET.$table_name"
    
    echo -e "${CYAN}🧹 Clearing data from: $table_name${NC}"
    
    if ! bq show "$table_id" >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  Table doesn't exist: $table_name${NC}"
        return 0
    fi
    
    # Check current row count
    local row_count=$(bq query --use_legacy_sql=false --format=csv --quiet \
        "SELECT COUNT(*) FROM \`$table_id\`" 2>/dev/null | tail -1)
    
    if [ -z "$row_count" ] || [ "$row_count" = "0" ]; then
        echo -e "${BLUE}ℹ️  Table is already empty: $table_name${NC}"
        return 0
    fi
    
    echo -e "${YELLOW}📊 Current rows: $row_count${NC}"
    
    # Delete all data
    if bq query --use_legacy_sql=false "DELETE FROM \`$table_id\` WHERE TRUE"; then
        echo -e "${GREEN}✅ Successfully cleared: $table_name${NC}"
        return 0
    else
        echo -e "${RED}❌ Failed to clear: $table_name${NC}"
        return 1
    fi
}

function clear_all_data() {
    echo -e "${YELLOW}🧹 Clearing all data from staging tables...${NC}"
    echo -e "${YELLOW}⚠️  This will remove all data but keep table structures${NC}"
    
    read -p "$(echo -e ${YELLOW}Type 'yes' to continue: ${NC})" confirm
    if [ "$confirm" != "yes" ]; then
        echo -e "${YELLOW}❌ Operation cancelled${NC}"
        return 0
    fi
    
    local cleared_count=0
    local failed_count=0
    
    # Clear data in reverse dependency order
    for ((i=${#ALL_TABLES[@]}-1; i>=0; i--)); do
        local table="${ALL_TABLES[i]}"
        if clear_table_data "$table"; then
            ((cleared_count++))
        else
            ((failed_count++))
        fi
    done
    
    echo -e "${BOLD}${BLUE}📊 CLEAR DATA SUMMARY${NC}"
    echo -e "${BLUE}═══════════════════════════${NC}"
    echo -e "${GREEN}✅ Cleared: $cleared_count tables${NC}"
    echo -e "${RED}❌ Failed: $failed_count tables${NC}"
    
    return 0
}

function verify_schemas() {
    echo -e "${BLUE}🔍 Verifying table schemas...${NC}"
    
    local verified_count=0
    local failed_count=0
    
    for table in "${ALL_TABLES[@]}"; do
        local table_id="$PROJECT_ID:$STAGING_DATASET.$table"
        
        if bq show "$table_id" >/dev/null 2>&1; then
            echo -e "${GREEN}✅ $table${NC} - Schema verified"
            ((verified_count++))
        else
            echo -e "${RED}❌ $table${NC} - Table missing"
            ((failed_count++))
        fi
    done
    
    echo ""
    echo -e "${CYAN}📊 Schema verification:${NC}"
    echo -e "${GREEN}  ✅ Verified: $verified_count tables${NC}"
    echo -e "${RED}  ❌ Missing: $failed_count tables${NC}"
    
    return $([ $failed_count -eq 0 ] && echo 0 || echo 1)
}

function show_help() {
    echo "HubSpot Staging Table Management"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  status                Show current table status"
    echo "  create [TABLE]        Create table(s)"
    echo "  drop [TABLE]          Drop table(s)"  
    echo "  recreate [TABLE]      Drop and recreate table(s)"
    echo "  clear [TABLE]         Clear data from table(s)"
    echo "  verify                Verify table schemas"
    echo "  help                  Show this help"
    echo ""
    echo "Options:"
    echo "  --all                 Apply to all tables"
    echo "  --core                Apply to core tables only"
    echo "  --force               Skip confirmations"
    echo ""
    echo "Examples:"
    echo "  $0 status                      # Show all table status"
    echo "  $0 create --all                # Create all tables"
    echo "  $0 recreate hs_companies       # Recreate companies table"
    echo "  $0 clear --all                 # Clear all data"
    echo "  $0 drop hs_deals              # Drop deals table"
}

# Main command processing
case "${1:-}" in
    "status")
        print_header
        check_prerequisites && show_all_table_status
        ;;
    "create")
        print_header
        if ! check_prerequisites; then exit 1; fi
        
        if [ "$2" = "--all" ]; then
            create_all_tables false
        elif [ "$2" = "--core" ]; then
            for table in "${CORE_TABLES[@]}"; do
                create_table "$table" false
            done
        elif [ -n "$2" ]; then
            create_table "$2" false
        else
            echo -e "${RED}❌ Specify table name, --all, or --core${NC}"
            exit 1
        fi
        ;;
    "recreate")
        print_header
        if ! check_prerequisites; then exit 1; fi
        
        if [ "$2" = "--all" ]; then
            create_all_tables true
        elif [ "$2" = "--core" ]; then
            for table in "${CORE_TABLES[@]}"; do
                create_table "$table" true
            done
        elif [ -n "$2" ]; then
            create_table "$2" true
        else
            echo -e "${RED}❌ Specify table name, --all, or --core${NC}"
            exit 1
        fi
        ;;
    "drop")
        print_header
        if ! check_prerequisites; then exit 1; fi
        
        if [ "$2" = "--all" ]; then
            drop_all_tables
        elif [ -n "$2" ]; then
            drop_table "$2"
        else
            echo -e "${RED}❌ Specify table name or --all${NC}"
            exit 1
        fi
        ;;
    "clear")
        print_header
        if ! check_prerequisites; then exit 1; fi
        
        if [ "$2" = "--all" ]; then
            clear_all_data
        elif [ -n "$2" ]; then
            clear_table_data "$2"
        else
            echo -e "${RED}❌ Specify table name or --all${NC}"
            exit 1
        fi
        ;;
    "verify")
        print_header
        check_prerequisites && verify_schemas
        ;;
    "help"|"--help"|"-h")
        show_help
        ;;
    *)
        echo -e "${RED}❌ Unknown command: ${1:-}${NC}"
        echo ""
        show_help
        exit 1
        ;;
esac