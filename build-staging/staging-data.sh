#!/bin/bash
# File: build-staging/staging-data.sh
# Path: build-staging/staging-data.sh

# staging-data.sh - CLI wrapper for staging data operations
# This script provides a simple bash interface to the Python staging data module

# Colors and formatting
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/staging_data.py"
CONFIG_DIR="$SCRIPT_DIR/config"
VENV_DIR="$SCRIPT_DIR/venv"

function print_header() {
    echo -e "${BLUE}================================================================${NC}"
    echo -e "${BLUE}           Staging Data Operations - CLI Wrapper${NC}"
    echo -e "${BLUE}================================================================${NC}"
    echo -e "${CYAN}🏗️  Manages staging data import, validation, and operations${NC}"
    echo -e "${CYAN}🐍 Python backend with interactive menu for VSCode debugging${NC}"
    echo -e "${BLUE}================================================================${NC}"
    echo ""
}

function check_prerequisites() {
    echo -e "${BLUE}🔍 Checking prerequisites...${NC}"
    
    # Check if Python script exists
    if [ ! -f "$PYTHON_SCRIPT" ]; then
        echo -e "${RED}❌ Python script not found: $PYTHON_SCRIPT${NC}"
        return 1
    fi
    
    # Check Python availability
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}❌ Python 3 not found${NC}"
        echo -e "${YELLOW}💡 Please install Python 3${NC}"
        return 1
    fi
    
    # Check if we're in the right directory (project root)
    if [ ! -d "src" ]; then
        echo -e "${YELLOW}⚠️  Warning: 'src' directory not found${NC}"
        echo -e "${YELLOW}💡 Make sure you're running from the project root${NC}"
        echo -e "${YELLOW}💡 Current directory: $(pwd)${NC}"
    fi
    
    echo -e "${GREEN}✅ Prerequisites check passed${NC}"
    return 0
}

function setup_environment() {
    echo -e "${BLUE}🔧 Setting up environment...${NC}"
    
    # Create config directory if it doesn't exist
    if [ ! -d "$CONFIG_DIR" ]; then
        echo -e "${YELLOW}📁 Creating config directory: $CONFIG_DIR${NC}"
        mkdir -p "$CONFIG_DIR"
    fi
    
    # Create default config if it doesn't exist
    local config_file="$CONFIG_DIR/staging-config.conf"
    if [ ! -f "$config_file" ]; then
        echo -e "${YELLOW}📄 Creating default config: $config_file${NC}"
        cat > "$config_file" << 'EOF'
{
    "environment": "staging",
    "excel_directory": "~/Downloads",
    "default_excel_file": "pipeline-import.xlsx",
    "bigquery_project": "hubspot-452402",
    "bigquery_dataset": "Hubspot_staging",
    "backup_directory": "build-staging/backups",
    "log_level": "INFO",
    "max_import_records": 1000,
    "crm_validation_enabled": true
}
EOF
        echo -e "${GREEN}✅ Created default config${NC}"
    fi
    
    # Create backup directory
    local backup_dir="$SCRIPT_DIR/backups"
    if [ ! -d "$backup_dir" ]; then
        mkdir -p "$backup_dir"
        echo -e "${GREEN}✅ Created backup directory${NC}"
    fi
    
    echo -e "${GREEN}✅ Environment setup complete${NC}"
}

function run_python_interactive() {
    echo -e "${BLUE}🐍 Starting interactive staging data operations...${NC}"
    echo -e "${CYAN}💡 This will open the Python interactive menu${NC}"
    echo -e "${CYAN}💡 Perfect for VSCode debugging - set breakpoints in staging_data.py${NC}"
    echo ""
    
    # Run the Python script
    python3 "$PYTHON_SCRIPT"
}

function run_python_cli() {
    local operation=$1
    local excel_file=$2
    local extra_args="${@:3}"
    
    echo -e "${BLUE}🐍 Running staging data operation: $operation${NC}"
    
    # Build command
    local cmd="python3 \"$PYTHON_SCRIPT\""
    
    if [ -n "$operation" ]; then
        cmd="$cmd --operation $operation"
    fi
    
    if [ -n "$excel_file" ]; then
        cmd="$cmd --excel-file \"$excel_file\""
    fi
    
    if [ -n "$extra_args" ]; then
        cmd="$cmd $extra_args"
    fi
    
    echo -e "${CYAN}Command: $cmd${NC}"
    eval $cmd
}

function show_help() {
    echo "Staging Data Operations - CLI Wrapper"
    echo ""
    echo "Usage:"
    echo "  $0                          # Interactive menu (recommended for development)"
    echo "  $0 [operation] [options]    # Direct CLI operation"
    echo ""
    echo "Operations:"
    echo "  import <excel-file>         # Import Excel data"
    echo "  samples                     # Show data samples"
    echo "  stats                       # Show data statistics" 
    echo "  clear                       # Clear data (TRUNCATE tables)"
    echo "  recreate                    # Recreate dataset (DROP + CREATE tables)"
    echo "  export                      # Export data"
    echo ""
    echo "Options:"
    echo "  --environment <env>         # Target environment: dev, staging, prod"
    echo "  --dry-run                   # Preview import without writing"
    echo "  --config <file>             # Use custom config file"
    echo ""
    echo "Examples:"
    echo "  $0                          # Start interactive menu"
    echo "  $0 --environment dev        # Start with dev environment selected"
    echo "  $0 import my-data.xlsx      # Import to default environment"
    echo "  $0 import data.xlsx --environment staging --dry-run  # Import to staging (preview)"
    echo "  $0 clear --environment dev  # Clear dev environment data"
    echo "  $0 recreate --environment dev  # Recreate dev dataset"
    echo ""
    echo "Environment Safety:"
    echo "  🟢 dev      - Safe for all operations"
    echo "  🟡 staging  - Safe for validation and testing"  
    echo "  🔴 prod     - READ ONLY (write operations blocked)"
    echo ""
    echo "VSCode Debugging:"
    echo "  1. Open staging_data.py in VSCode"
    echo "  2. Set breakpoints in the main() or show_data_menu() functions"
    echo "  3. Run this script without arguments to enter interactive mode"
    echo "  4. The debugger will hit your breakpoints"
    echo ""
    echo "Files:"
    echo "  Python backend: $PYTHON_SCRIPT"
    echo "  Configuration: $CONFIG_DIR/staging-config.conf"
    echo "  Backups: $SCRIPT_DIR/backups/"
}

function main() {
    print_header
    
    # Check help flag
    if [[ "$1" == "-h" || "$1" == "--help" ]]; then
        show_help
        exit 0
    fi
    
    # Check prerequisites
    if ! check_prerequisites; then
        echo -e "${RED}❌ Prerequisites check failed${NC}"
        exit 1
    fi
    
    # Setup environment
    setup_environment
    
    # Determine how to run
    if [ $# -eq 0 ]; then
        # No arguments - run interactive mode
        run_python_interactive
    else
        # Arguments provided - run CLI mode
        operation=$1
        excel_file=$2
        extra_args="${@:3}"
        
        run_python_cli "$operation" "$excel_file" "$extra_args"
    fi
}

# Run main function
main "$@"