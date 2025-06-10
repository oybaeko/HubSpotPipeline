#!/bin/bash

# Enhanced HubSpot Excel Import Script with CRM File Validation

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
DEFAULT_EXCEL_FILE="pipeline-import.xlsx"
DEFAULT_DOWNLOAD_DIR="$HOME/Downloads"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMPORT_SCRIPT="$SCRIPT_DIR/import_excel.py"

get_dataset_for_env() {
    local env=$1
    case $env in
        dev) echo "Hubspot_dev_ob" ;;
        staging) echo "Hubspot_staging" ;;
        prod) echo "Hubspot_prod" ;;
        *) echo "Hubspot_dev_ob" ;;
    esac
}

print_header() {
    echo -e "${BOLD}${BLUE}================================================================${NC}"
    echo -e "${BOLD}${BLUE}     Enhanced HubSpot Excel Import with CRM File Validation${NC}"
    echo -e "${BOLD}${BLUE}================================================================${NC}"
    echo -e "${CYAN}📊 Import historical HubSpot data from Excel to BigQuery${NC}"
    echo -e "${CYAN}✅ Validate against original CRM export files${NC}"
    echo -e "${CYAN}⏰ Use actual CRM file download timestamps as snapshot_id${NC}"
    echo -e "${CYAN}🎯 Target: $PROJECT_ID datasets${NC}"
    echo -e "${BLUE}================================================================${NC}"
    echo ""
}

check_prerequisites() {
    echo -e "${BLUE}🔍 Checking prerequisites...${NC}"
    
    if [ ! -f "$IMPORT_SCRIPT" ]; then
        echo -e "${RED}❌ Python import script not found: $IMPORT_SCRIPT${NC}"
        return 1
    fi
    
    if ! command -v python3 &> /dev/null && ! command -v python &> /dev/null; then
        echo -e "${RED}❌ Python not found${NC}"
        return 1
    fi
    
    if command -v python3 &> /dev/null; then
        PYTHON_CMD="python3"
    else
        PYTHON_CMD="python"
    fi
    
    if ! command -v gcloud &> /dev/null; then
        echo -e "${RED}❌ gcloud CLI not found${NC}"
        return 1
    fi
    
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
        echo -e "${RED}❌ Not authenticated with gcloud${NC}"
        return 1
    fi
    
    local current_project=$(gcloud config get-value project 2>/dev/null)
    if [ "$current_project" != "$PROJECT_ID" ]; then
        echo -e "${YELLOW}🔧 Setting project to $PROJECT_ID...${NC}"
        gcloud config set project $PROJECT_ID
    fi
    
    echo -e "${GREEN}✅ Prerequisites check passed${NC}"
    return 0
}

check_excel_file() {
    local excel_file=$1
    
    if [ ! -f "$excel_file" ]; then
        echo -e "${RED}❌ Excel file not found: $excel_file${NC}"
        return 1
    fi
    
    if [[ ! "$excel_file" =~ \.(xlsx|xls)$ ]]; then
        echo -e "${RED}❌ File must be Excel format (.xlsx or .xls): $excel_file${NC}"
        return 1
    fi
    
    echo -e "${GREEN}✅ Excel file found: $excel_file${NC}"
    return 0
}

check_download_directory() {
    local download_dir=$1
    
    if [ ! -d "$download_dir" ]; then
        echo -e "${RED}❌ Download directory not found: $download_dir${NC}"
        return 1
    fi
    
    local crm_files=$(find "$download_dir" -name "hubspot-crm-exports-weekly-status-*" 2>/dev/null | wc -l)
    
    echo -e "${GREEN}✅ Download directory found: $download_dir${NC}"
    echo -e "${CYAN}📁 Found $crm_files CRM export files${NC}"
    
    return 0
}

get_file_download_timestamp() {
    local file_path=$1
    
    if [ ! -f "$file_path" ]; then
        echo ""
        return
    fi
    
    if command -v stat >/dev/null 2>&1; then
        # Try macOS format first
        if stat -f "%Sm" -t "%Y-%m-%dT%H:%M:%S%z" "$file_path" 2>/dev/null; then
            return
        # Try Linux format
        elif stat -c "%y" "$file_path" 2>/dev/null | sed 's/ /T/' | sed 's/\([0-9][0-9]\)$/:\1/'; then
            return
        fi
    fi
    
    echo ""
}

find_crm_export_files() {
    local download_dir=$1
    local snapshot_date=$2
    
    # Look for files matching the pattern - try exact date first
    local company_pattern="hubspot-crm-exports-weekly-status-company-${snapshot_date}*"
    local deals_pattern="hubspot-crm-exports-weekly-status-deals-${snapshot_date}*"
    
    local company_files=($(find "$download_dir" -name "$company_pattern" 2>/dev/null))
    local deals_files=($(find "$download_dir" -name "$deals_pattern" 2>/dev/null))
    
    # If exact date not found, try nearby dates (±3 days)
    if [ ${#company_files[@]} -eq 0 ] || [ ${#deals_files[@]} -eq 0 ]; then
        if command -v date >/dev/null 2>&1; then
            local base_date
            # Try macOS date format first, then Linux
            if base_date=$(date -j -f "%Y-%m-%d" "$snapshot_date" "+%s" 2>/dev/null); then
                :
            elif base_date=$(date -d "$snapshot_date" "+%s" 2>/dev/null); then
                :
            else
                echo "${company_files[@]} ${deals_files[@]}"
                return
            fi
            
            for days_offset in -3 -2 -1 1 2 3; do
                local search_timestamp=$((base_date + days_offset * 86400))
                local search_date
                
                # Try macOS date format first, then Linux
                if search_date=$(date -r "$search_timestamp" "+%Y-%m-%d" 2>/dev/null); then
                    :
                elif search_date=$(date -d "@$search_timestamp" "+%Y-%m-%d" 2>/dev/null); then
                    :
                else
                    continue
                fi
                
                if [ ${#company_files[@]} -eq 0 ]; then
                    local alt_company_pattern="hubspot-crm-exports-weekly-status-company-${search_date}*"
                    local alt_company_files=($(find "$download_dir" -name "$alt_company_pattern" 2>/dev/null))
                    if [ ${#alt_company_files[@]} -gt 0 ]; then
                        company_files=("${alt_company_files[@]}")
                    fi
                fi
                
                if [ ${#deals_files[@]} -eq 0 ]; then
                    local alt_deals_pattern="hubspot-crm-exports-weekly-status-deals-${search_date}*"
                    local alt_deals_files=($(find "$download_dir" -name "$alt_deals_pattern" 2>/dev/null))
                    if [ ${#alt_deals_files[@]} -gt 0 ]; then
                        deals_files=("${alt_deals_files[@]}")
                    fi
                fi
                
                if [ ${#company_files[@]} -gt 0 ] && [ ${#deals_files[@]} -gt 0 ]; then
                    break
                fi
            done
        fi
    fi
    
    echo "${company_files[@]} ${deals_files[@]}"
}

validate_crm_files() {
    local excel_file=$1
    local download_dir=$2
    local preflight_only=$3
    
    echo -e "${BLUE}🔍 Validating CRM export files...${NC}"
    echo -e "${CYAN}Excel file: $excel_file${NC}"
    echo -e "${CYAN}Download directory: $download_dir${NC}"
    echo ""
    
    echo -e "${BLUE}📋 Running Excel validation to extract snapshot dates...${NC}"
    local validation_output=$($PYTHON_CMD "$IMPORT_SCRIPT" "$excel_file" --mode validate 2>&1)
    
    # Extract snapshot dates from validation output
    local snapshot_dates=($(echo "$validation_output" | grep "📸 Configured snapshots" -A 20 | grep -oE "20[0-9]{2}-[0-9]{2}-[0-9]{2}" | sort -u))
    
    if [ ${#snapshot_dates[@]} -eq 0 ]; then
        snapshot_dates=($(echo "$validation_output" | grep -oE "20[0-9]{2}-[0-9]{2}-[0-9]{2}" | sort -u))
    fi
    
    if [ ${#snapshot_dates[@]} -eq 0 ]; then
        echo -e "${YELLOW}⚠️  Could not extract snapshot dates from validation output${NC}"
        echo -e "${BLUE}💡 Using configured snapshot dates from schema...${NC}"
        snapshot_dates=("2025-03-21" "2025-03-23" "2025-04-04" "2025-04-06" "2025-04-14" "2025-04-27" "2025-05-11" "2025-05-18" "2025-05-25" "2025-06-01")
        echo -e "${CYAN}Using configured dates: ${snapshot_dates[*]}${NC}"
    fi
    
    if [ ${#snapshot_dates[@]} -eq 0 ]; then
        echo -e "${RED}❌ Could not determine snapshot dates${NC}"
        return 1
    fi
    
    echo -e "${GREEN}📸 Found ${#snapshot_dates[@]} snapshot dates in Excel:${NC}"
    for date in "${snapshot_dates[@]}"; do
        echo -e "${GREEN}  • $date${NC}"
    done
    echo ""
    
    local validation_results=()
    local total_found=0
    local total_missing=0
    local validation_passed=true
    
    echo -e "${BLUE}🔍 Searching for corresponding CRM export files...${NC}"
    echo ""
    
    for snapshot_date in "${snapshot_dates[@]}"; do
        echo -e "${CYAN}📅 Checking snapshot: $snapshot_date${NC}"
        
        local crm_files=($(find_crm_export_files "$download_dir" "$snapshot_date"))
        
        local company_file=""
        local deals_file=""
        local company_timestamp=""
        local deals_timestamp=""
        
        for file in "${crm_files[@]}"; do
            if [[ "$file" == *"company"* ]]; then
                company_file="$file"
                company_timestamp=$(get_file_download_timestamp "$file")
            elif [[ "$file" == *"deals"* ]]; then
                deals_file="$file"
                deals_timestamp=$(get_file_download_timestamp "$file")
            fi
        done
        
        if [ -n "$company_file" ]; then
            echo -e "${GREEN}  ✅ Company file: $(basename "$company_file")${NC}"
            echo -e "${BLUE}     📅 Downloaded: ${company_timestamp:-"unknown"}${NC}"
            local file_date=$(basename "$company_file" | grep -oE "20[0-9]{2}-[0-9]{2}-[0-9]{2}")
            if [ "$file_date" != "$snapshot_date" ]; then
                echo -e "${YELLOW}     ⚠️  File date ($file_date) differs from snapshot date ($snapshot_date)${NC}"
            fi
            ((total_found++))
        else
            echo -e "${RED}  ❌ Company file: NOT FOUND${NC}"
            echo -e "${YELLOW}     🔍 Looking for: hubspot-crm-exports-weekly-status-company-${snapshot_date}* (±3 days)${NC}"
            ((total_missing++))
            validation_passed=false
        fi
        
        if [ -n "$deals_file" ]; then
            echo -e "${GREEN}  ✅ Deals file: $(basename "$deals_file")${NC}"
            echo -e "${BLUE}     📅 Downloaded: ${deals_timestamp:-"unknown"}${NC}"
            local file_date=$(basename "$deals_file" | grep -oE "20[0-9]{2}-[0-9]{2}-[0-9]{2}")
            if [ "$file_date" != "$snapshot_date" ]; then
                echo -e "${YELLOW}     ⚠️  File date ($file_date) differs from snapshot date ($snapshot_date)${NC}"
            fi
            ((total_found++))
        else
            echo -e "${RED}  ❌ Deals file: NOT FOUND${NC}"
            echo -e "${YELLOW}     🔍 Looking for: hubspot-crm-exports-weekly-status-deals-${snapshot_date}* (±3 days)${NC}"
            ((total_missing++))
            validation_passed=false
        fi
        
        validation_results+=("$snapshot_date|$company_file|$deals_file|$company_timestamp|$deals_timestamp")
        echo ""
    done
    
    echo -e "${BOLD}${BLUE}📊 CRM FILE VALIDATION SUMMARY${NC}"
    echo -e "${BLUE}═══════════════════════════════════${NC}"
    echo -e "${GREEN}✅ Files found: $total_found${NC}"
    echo -e "${RED}❌ Files missing: $total_missing${NC}"
    echo -e "${BLUE}📸 Snapshots checked: ${#snapshot_dates[@]}${NC}"
    
    if [ "$validation_passed" = true ]; then
        echo -e "${GREEN}🎉 ALL CRM FILES FOUND - VALIDATION PASSED${NC}"
    else
        echo -e "${RED}❌ MISSING CRM FILES - VALIDATION FAILED${NC}"
        echo -e "${YELLOW}💡 Make sure all CRM export files are in: $download_dir${NC}"
    fi
    
    echo ""
    
    if [ "$preflight_only" = "true" ]; then
        return $([ "$validation_passed" = true ] && echo 0 || echo 1)
    fi
    
    export CRM_VALIDATION_RESULTS="${validation_results[@]}"
    return $([ "$validation_passed" = true ] && echo 0 || echo 1)
}

create_crm_metadata_file() {
    local excel_file=$1
    local download_dir=$2
    local output_file=$3
    
    echo -e "${BLUE}📋 Creating CRM metadata file...${NC}"
    
    local validation_output=$($PYTHON_CMD "$IMPORT_SCRIPT" "$excel_file" --mode validate 2>&1)
    local snapshot_dates=($(echo "$validation_output" | grep "📸 Configured snapshots" -A 20 | grep -oE "20[0-9]{2}-[0-9]{2}-[0-9]{2}" | sort -u))
    
    if [ ${#snapshot_dates[@]} -eq 0 ]; then
        snapshot_dates=($(echo "$validation_output" | grep -oE "20[0-9]{2}-[0-9]{2}-[0-9]{2}" | sort -u))
    fi
    
    if [ ${#snapshot_dates[@]} -eq 0 ]; then
        echo -e "${RED}❌ Could not extract snapshot dates from Excel${NC}"
        return 1
    fi
    
    echo "{" > "$output_file"
    echo '  "crm_file_metadata": {' >> "$output_file"
    
    local first_entry=true
    local successful_mappings=0
    
    for snapshot_date in "${snapshot_dates[@]}"; do
        echo -e "${CYAN}  🔍 Processing snapshot: $snapshot_date${NC}"
        
        local crm_files=($(find_crm_export_files "$download_dir" "$snapshot_date"))
        
        local company_file=""
        local deals_file=""
        local company_timestamp=""
        local deals_timestamp=""
        
        for file in "${crm_files[@]}"; do
            if [[ "$file" == *"company"* ]]; then
                company_file="$file"
                company_timestamp=$(get_file_download_timestamp "$file")
            elif [[ "$file" == *"deals"* ]]; then
                deals_file="$file"
                deals_timestamp=$(get_file_download_timestamp "$file")
            fi
        done
        
        if [ -n "$company_file" ] && [ -n "$deals_file" ] && [ -n "$company_timestamp" ] && [ -n "$deals_timestamp" ]; then
            
            local snapshot_timestamp="$company_timestamp"
            if [[ "$deals_timestamp" < "$company_timestamp" ]]; then
                snapshot_timestamp="$deals_timestamp"
            fi
            
            if [ "$first_entry" = false ]; then
                echo "," >> "$output_file"
            fi
            first_entry=false
            
            echo "    \"$snapshot_date\": {" >> "$output_file"
            echo "      \"snapshot_id\": \"$snapshot_timestamp\"," >> "$output_file"
            echo "      \"company_file\": \"$company_file\"," >> "$output_file"
            echo "      \"deals_file\": \"$deals_file\"," >> "$output_file"
            echo "      \"company_timestamp\": \"$company_timestamp\"," >> "$output_file"
            echo "      \"deals_timestamp\": \"$deals_timestamp\"" >> "$output_file"
            echo -n "    }" >> "$output_file"
            
            echo -e "${GREEN}    ✅ Mapped to CRM timestamp: $snapshot_timestamp${NC}"
            ((successful_mappings++))
        else
            echo -e "${YELLOW}    ⚠️  Skipping - missing CRM files or timestamps${NC}"
        fi
    done
    
    echo "" >> "$output_file"
    echo "  }" >> "$output_file"
    echo "}" >> "$output_file"
    
    echo -e "${GREEN}📊 Successfully mapped $successful_mappings snapshots to CRM timestamps${NC}"
    
    if [ "$successful_mappings" -eq 0 ]; then
        echo -e "${RED}❌ No snapshots could be mapped to CRM files${NC}"
        return 1
    fi
    
    return 0
}

show_preflight_check() {
    local env=$1
    local excel_file=$2
    local download_dir=$3
    local skip_crm_validation=$4
    
    echo -e "\n${PURPLE}🚀 PREFLIGHT CHECK${NC}"
    echo -e "${PURPLE}══════════════════${NC}"
    echo -e "${CYAN}Environment: $env${NC}"
    echo -e "${CYAN}Dataset: $(get_dataset_for_env "$env")${NC}"
    echo -e "${CYAN}Excel file: $excel_file${NC}"
    if [ "$skip_crm_validation" != "true" ]; then
        echo -e "${CYAN}Download directory: $download_dir${NC}"
    fi
    echo ""
    
    echo -e "${BLUE}1️⃣ Validating Excel file structure...${NC}"
    echo -e "${CYAN}Running: $PYTHON_CMD \"$IMPORT_SCRIPT\" \"$excel_file\" --mode validate --log-level WARN${NC}"
    
    # Run validation and capture output for debugging
    local validation_result
    local validation_output
    validation_output=$($PYTHON_CMD "$IMPORT_SCRIPT" "$excel_file" --mode validate --log-level WARN 2>&1)
    validation_result=$?
    
    if [ $validation_result -eq 0 ]; then
        echo -e "${GREEN}   ✅ Excel structure is valid${NC}"
    else
        echo -e "${RED}   ❌ Excel validation failed${NC}"
        echo -e "${YELLOW}   📋 Validation output:${NC}"
        echo "$validation_output" | head -10
        echo ""
        return 1
    fi
    
    if [ "$skip_crm_validation" != "true" ]; then
        echo -e "${BLUE}2️⃣ Validating CRM export files...${NC}"
        if validate_crm_files "$excel_file" "$download_dir" "true"; then
            echo -e "${GREEN}   ✅ All CRM files found and validated${NC}"
        else
            echo -e "${YELLOW}   ⚠️  CRM file validation failed${NC}"
            echo -e "${YELLOW}   💡 Use --skip-crm-validation to import without CRM validation${NC}"
            return 1
        fi
    else
        echo -e "${BLUE}2️⃣ Skipping CRM file validation...${NC}"
        echo -e "${YELLOW}   ⚠️  CRM validation skipped - importing Excel data only${NC}"
    fi
    
    echo -e "${BLUE}3️⃣ Checking BigQuery access...${NC}"
    local dataset=$(get_dataset_for_env "$env")
    if bq ls "$PROJECT_ID:$dataset" >/dev/null 2>&1; then
        echo -e "${GREEN}   ✅ BigQuery dataset accessible${NC}"
    else
        echo -e "${RED}   ❌ Cannot access BigQuery dataset: $dataset${NC}"
        return 1
    fi
    
    echo -e "\n${GREEN}🎉 ALL PREFLIGHT CHECKS PASSED${NC}"
    echo -e "${GREEN}✅ Ready for import${NC}"
    return 0
}

setup_environment() {
    local env=$1
    local dataset=$(get_dataset_for_env "$env")
    
    echo -e "${BLUE}🔧 Setting up environment for $env...${NC}"
    
    local env_file="$SCRIPT_DIR/.env"
    if [ ! -f "$env_file" ]; then
        echo -e "${YELLOW}⚠️  .env file not found, creating one...${NC}"
        
        cat > "$env_file" << 'EOF'
# BigQuery Configuration for Excel Import
BIGQUERY_PROJECT_ID=hubspot-452402
BIGQUERY_DATASET_ID=placeholder

# Google Cloud Authentication
# GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json
EOF
        echo -e "${GREEN}✅ Created .env file: $env_file${NC}"
    else
        echo -e "${GREEN}✅ Found .env file${NC}"
    fi
    
    if command -v sed &> /dev/null; then
        sed -i.bak "s/^BIGQUERY_DATASET_ID=.*/BIGQUERY_DATASET_ID=$dataset/" "$env_file"
        echo -e "${GREEN}✅ Updated dataset to: $dataset${NC}"
    else
        echo -e "${YELLOW}⚠️  Please manually update BIGQUERY_DATASET_ID=$dataset in .env${NC}"
    fi
    
    echo -e "${CYAN}📊 Target dataset: $dataset${NC}"
}

confirm_import() {
    local env=$1
    local excel_file=$2
    local dry_run=$3
    
    echo -e "\n${YELLOW}⚠️  IMPORT CONFIRMATION${NC}"
    
    if [ "$dry_run" = "true" ]; then
        echo -e "${BLUE}This is a DRY RUN - no data will be written to BigQuery${NC}"
        read -p "$(echo -e ${GREEN}Press Enter to continue with dry run...${NC})"
        return 0
    fi
    
    echo -e "${YELLOW}This will import data to $env environment${NC}"
    echo -e "${YELLOW}Excel file: $excel_file${NC}"
    echo -e "${YELLOW}Target dataset: $(get_dataset_for_env "$env")${NC}"
    echo -e "${YELLOW}Using CRM file timestamps as snapshot_id${NC}"
    
    if [ "$env" = "prod" ]; then
        echo -e "\n${RED}🚨 THIS IS PRODUCTION! 🚨${NC}"
        echo -e "${RED}Data will be written to production BigQuery tables${NC}"
        read -p "$(echo -e ${RED}Type 'IMPORT TO PRODUCTION' to continue: ${NC})" confirm
        if [ "$confirm" != "IMPORT TO PRODUCTION" ]; then
            echo -e "${YELLOW}❌ Import cancelled${NC}"
            exit 0
        fi
    else
        read -p "$(echo -e ${YELLOW}Type 'yes' to continue: ${NC})" confirm
        if [ "$confirm" != "yes" ]; then
            echo -e "${YELLOW}❌ Import cancelled${NC}"
            exit 0
        fi
    fi
}

run_import_with_crm_metadata() {
    local env=$1
    local excel_file=$2
    local mode=$3
    local dry_run_flag=$4
    local extra_args="$5"
    local download_dir="$6"
    local skip_crm_validation="$7"
    
    echo -e "\n${YELLOW}🚀 Starting Excel import...${NC}"
    
    if [ "$skip_crm_validation" = "true" ]; then
        echo -e "${YELLOW}⚠️  Skipping CRM metadata - using standard import${NC}"
        
        local cmd="$PYTHON_CMD \"$IMPORT_SCRIPT\" \"$excel_file\" --mode $mode $dry_run_flag $extra_args"
        
        echo -e "${CYAN}Command: $cmd${NC}"
        echo ""
        
        if eval $cmd; then
            echo -e "\n${GREEN}🎉 EXCEL IMPORT COMPLETED SUCCESSFULLY${NC}"
            
            if [ "$dry_run_flag" != "--dry-run" ]; then
                echo -e "${GREEN}✅ Data imported to $(get_dataset_for_env "$env")${NC}"
                echo -e "${BLUE}📊 Using standard snapshot IDs (Excel sheet dates)${NC}"
            else
                echo -e "${BLUE}🛑 This was a dry run - no data was actually imported${NC}"
            fi
        else
            echo -e "\n${RED}❌ EXCEL IMPORT FAILED${NC}"
            return 1
        fi
        
        return 0
    fi
    
    local crm_metadata_file="/tmp/crm_metadata_$$.json"
    
    echo -e "${BLUE}📋 Extracting CRM file metadata...${NC}"
    if ! create_crm_metadata_file "$excel_file" "$download_dir" "$crm_metadata_file"; then
        echo -e "${RED}❌ Failed to extract CRM metadata${NC}"
        return 1
    fi
    
    echo -e "${CYAN}📄 CRM metadata file created: $crm_metadata_file${NC}"
    
    echo -e "${BLUE}📋 CRM Metadata Preview:${NC}"
    if command -v jq &> /dev/null; then
        jq '.crm_file_metadata | to_entries | .[0:3] | map({snapshot_date: .key, snapshot_id: .value.snapshot_id})' "$crm_metadata_file" 2>/dev/null || echo "Preview not available"
    else
        head -20 "$crm_metadata_file"
    fi
    echo ""
    
    extra_args="$extra_args --crm-metadata \"$crm_metadata_file\""
    
    local cmd="$PYTHON_CMD \"$IMPORT_SCRIPT\" \"$excel_file\" --mode $mode $dry_run_flag $extra_args"
    
    echo -e "${CYAN}Command: $cmd${NC}"
    echo ""
    
    if eval $cmd; then
        echo -e "\n${GREEN}🎉 EXCEL IMPORT WITH CRM METADATA COMPLETED SUCCESSFULLY${NC}"
        
        if [ "$dry_run_flag" != "--dry-run" ]; then
            echo -e "${GREEN}✅ Data imported to $(get_dataset_for_env "$env") with CRM timestamps${NC}"
            echo -e "${GREEN}✅ Snapshot IDs based on actual CRM file download times${NC}"
            echo -e "${CYAN}💡 Query your data using the CRM-based snapshot_id timestamps${NC}"
        else
            echo -e "${BLUE}🛑 This was a dry run - no data was actually imported${NC}"
            echo -e "${CYAN}💡 Remove --dry-run to perform the actual import${NC}"
        fi
        
        rm -f "$crm_metadata_file"
        
    else
        echo -e "\n${RED}❌ EXCEL IMPORT WITH CRM METADATA FAILED${NC}"
        echo -e "${YELLOW}💡 Check the error messages above for details${NC}"
        echo -e "${YELLOW}🔍 CRM metadata file saved for debugging: $crm_metadata_file${NC}"
        return 1
    fi
}

show_help() {
    echo "Enhanced HubSpot Excel Import Script with CRM File Validation"
    echo ""
    echo "Usage: $0 [ENVIRONMENT] [EXCEL_FILE] [OPTIONS]"
    echo ""
    echo "Arguments:"
    echo "  ENVIRONMENT    Target environment (dev|staging|prod)"
    echo "  EXCEL_FILE     Excel file to import (default: $DEFAULT_EXCEL_FILE)"
    echo ""
    echo "Options:"
    echo "  --mode MODE           Import mode (snapshots|auto|validate) [default: snapshots]"
    echo "  --dry-run            Preview import without writing to BigQuery"
    echo "  --download-dir DIR   Directory containing CRM export files [default: ~/Downloads]"
    echo "  --preflight-only     Run preflight checks only, don't import"
    echo "  --skip-crm-validation Skip CRM file validation (import Excel only)"
    echo "  --snapshot-id ID     Custom snapshot ID for auto mode"
    echo "  --log-level LEVEL    Set logging level (DEBUG|INFO|WARN|ERROR)"
    echo "  --force              Skip confirmation prompts"
    echo "  --help, -h           Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 dev --preflight-only                     # Just run checks"
    echo "  $0 dev --skip-crm-validation --dry-run      # Import Excel only"
    echo "  $0 staging my-data.xlsx --dry-run           # Custom file with validation"
}

# Parse help flag first
for arg in "$@"; do
    if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
        show_help
        exit 0
    fi
done

print_header

if ! check_prerequisites; then
    echo -e "${RED}❌ Prerequisites check failed${NC}"
    exit 1
fi

# Parse arguments
environment=""
excel_file=""
mode="snapshots"
download_dir="$DEFAULT_DOWNLOAD_DIR"
preflight_only="false"
skip_crm_validation="false"
snapshot_id=""
log_level="INFO"
force="false"

# Parse all arguments
pos_args=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --mode)
            mode="$2"
            shift 2
            ;;
        --mode=*)
            mode="${1#*=}"
            shift
            ;;
        --dry-run)
            dry_run="true"
            shift
            ;;
        --download-dir)
            download_dir="$2"
            shift 2
            ;;
        --download-dir=*)
            download_dir="${1#*=}"
            shift
            ;;
        --preflight-only)
            preflight_only="true"
            shift
            ;;
        --skip-crm-validation)
            skip_crm_validation="true"
            shift
            ;;
        --snapshot-id)
            snapshot_id="$2"
            shift 2
            ;;
        --snapshot-id=*)
            snapshot_id="${1#*=}"
            shift
            ;;
        --log-level)
            log_level="$2"
            shift 2
            ;;
        --log-level=*)
            log_level="${1#*=}"
            shift
            ;;
        --force)
            force="true"
            shift
            ;;
        --*)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            exit 1
            ;;
        *)
            pos_args+=("$1")
            shift
            ;;
    esac
done

# Assign positional arguments
environment="${pos_args[0]:-}"
excel_file="${pos_args[1]:-$DEFAULT_EXCEL_FILE}"

# Expand tilde in download_dir
download_dir="${download_dir/#\~/$HOME}"

# Validate environment
if [ -z "$environment" ]; then
    echo -e "${RED}❌ Environment required${NC}"
    echo -e "${YELLOW}Usage: $0 <dev|staging|prod> [excel_file] [options]${NC}"
    exit 1
fi

if [[ ! "$environment" =~ ^(dev|staging|prod)$ ]]; then
    echo -e "${RED}❌ Invalid environment: $environment${NC}"
    echo -e "${YELLOW}Valid environments: dev, staging, prod${NC}"
    exit 1
fi

# Validate mode
if [[ ! "$mode" =~ ^(snapshots|auto|validate)$ ]]; then
    echo -e "${RED}❌ Invalid mode: $mode${NC}"
    echo -e "${YELLOW}Valid modes: snapshots, auto, validate${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Target environment: $environment${NC}"
echo -e "${GREEN}✅ Excel file: $excel_file${NC}"
echo -e "${GREEN}✅ Import mode: $mode${NC}"
echo -e "${GREEN}✅ Download directory: $download_dir${NC}"

# Check Excel file
if ! check_excel_file "$excel_file"; then
    exit 1
fi

# Check download directory
if ! check_download_directory "$download_dir"; then
    exit 1
fi

# Run preflight checks
echo -e "\n${PURPLE}🚀 Running preflight checks...${NC}"
if ! show_preflight_check "$environment" "$excel_file" "$download_dir" "$skip_crm_validation"; then
    echo -e "${RED}❌ Preflight checks failed${NC}"
    exit 1
fi

# If preflight only, stop here
if [ "$preflight_only" = "true" ]; then
    echo -e "\n${GREEN}✅ Preflight checks completed successfully${NC}"
    echo -e "${CYAN}💡 Remove --preflight-only to proceed with actual import${NC}"
    exit 0
fi

# Setup environment
setup_environment "$environment"

# Build extra arguments
extra_args=""
if [ -n "$snapshot_id" ]; then
    extra_args="$extra_args --snapshot-id \"$snapshot_id\""
fi
extra_args="$extra_args --log-level $log_level"

# Set dry run flag
dry_run_flag=""
if [ "$dry_run" = "true" ]; then
    dry_run_flag="--dry-run"
fi

# Confirm import (unless force flag or validate mode)
if [ "$force" != "true" ] && [ "$mode" != "validate" ]; then
    confirm_import "$environment" "$excel_file" "$dry_run"
fi

# Run the import
if run_import_with_crm_metadata "$environment" "$excel_file" "$mode" "$dry_run_flag" "$extra_args" "$download_dir" "$skip_crm_validation"; then
    echo -e "\n${GREEN}🎉 Excel import process completed successfully!${NC}"
    if [ "$skip_crm_validation" != "true" ]; then
        echo -e "${GREEN}✅ Snapshot IDs based on actual CRM file download timestamps${NC}"
    fi
else
    echo -e "\n${RED}❌ Excel import process failed${NC}"
    exit 1
fi