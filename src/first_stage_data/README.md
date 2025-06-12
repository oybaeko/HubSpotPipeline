# Staging Data Operations

**File**: `build-staging/README.md`  
**Path**: `build-staging/README.md`

A Python-based staging data migration system designed for VSCode debugging and CLI usage.

## 🎯 Purpose

This system provides simplified data operations for staging environments, including:
- ✅ Import Excel data with CRM validation
- ✅ Clear data while preserving tables  
- ✅ Show data samples and statistics
- ✅ Data export and backup functionality
- ✅ Import status tracking and resume capability

## 🚀 Quick Start

### Interactive Mode (Recommended for Development)
```bash
# Start interactive menu
./staging-data.sh

# This opens the Python interactive menu - perfect for VSCode debugging
```

### CLI Mode
```bash
# Import Excel file
./staging-data.sh import ~/Downloads/pipeline-import.xlsx

# Preview import (dry run)
./staging-data.sh import my-data.xlsx --dry-run

# Show data statistics
./staging-data.sh stats

# Show help
./staging-data.sh --help
```

## 🐍 VSCode Debugging Setup

1. **Open Files**: Open `staging_data.py` in VSCode
2. **Set Breakpoints**: Add breakpoints in the `main()` or `show_data_menu()` functions
3. **Run Interactive**: Execute `./staging-data.sh` (no arguments)
4. **Debug**: The debugger will hit your breakpoints when you use the menu

### Key Debug Points
- `main()` - Entry point
- `show_data_menu()` - Main menu loop
- `_import_excel_data()` - Import operations
- `_execute_import()` - Core import logic

## 📁 File Structure

```
build-staging/
├── staging_data.py           # Main Python module (ROOT function: main())
├── staging-data.sh           # CLI wrapper script
├── config/
│   └── staging-config.conf   # Configuration file
├── backups/                  # Data backups
├── logs/                     # Operation logs
└── temp/                     # Temporary files
```

## ⚙️ Configuration

Edit `config/staging-config.conf` to customize:

```json
{
    "environment": "staging",
    "bigquery_dataset": "Hubspot_staging",
    "excel_directory": "~/Downloads",
    "max_import_records": 1000,
    "crm_validation_enabled": true
}
```

## 🔧 Operations Menu

When you run in interactive mode, you'll see:

```
📊 DATA OPERATIONS
  1) 📥 Import Excel Data
  2) 👀 Show Data Samples
  3) 📈 Show Data Statistics
  4) 🗑️  Clear Staging Data
  5) 📤 Export Data
  6) 📋 Check Import Status
  7) ⚙️  Configure Settings
  8) 📜 Operation History
  0) ❌ Exit
```

## 🛠️ Development Notes

### Dependencies
- Uses existing `src/first_stage_data/excel_import/` modules
- May need path adjustments after folder reorganization
- Requires Python 3.6+ and access to BigQuery

### Design Principles
- **Root Function**: `main()` in `staging_data.py` for easy VSCode debugging
- **CLI Wrapper**: Simple bash script for command-line usage
- **Modular**: Separate functions for each operation
- **Safe**: Confirmation prompts for destructive operations

### Import Modes
1. **snapshots** - Import configured snapshots from Excel
2. **auto** - Auto-detect HubSpot sheets and import as single snapshot  
3. **validate** - Validate file structure without importing

### Safety Features
- Dry run mode by default
- Confirmation prompts for destructive operations
- Operation history tracking
- Configurable safety limits

## 🔗 Integration

This system integrates with your existing:
- Excel import modules (`src/first_stage_data/excel_import/`)
- BigQuery schemas and table structures
- CRM validation system

## 📝 Next Steps

This is the **Data Operations Module** (Priority #1). Next modules to build:
1. **Validation Suite** (`staging-validate.sh`)
2. **Utilities Module** (`staging-utils.sh`) 
3. **Configuration Files** (validation rules, table dependencies)
4. **Migration Script** (`migrate-staging-to-prod.sh`)

## 🚨 Important Notes

- **VSCode Debugging**: The `main()` function is the ROOT function for debugging
- **Path Adjustments**: May need import path fixes after folder moves
- **Configuration**: Customize settings in `config/staging-config.conf`
- **Safety First**: Always test with dry run mode initially

## 📞 Usage Examples

```python
# In VSCode debugger, call directly:
from staging_data import main
main()  # Sets breakpoint here for debugging

# Or create manager directly:
from staging_data import StagingDataManager
manager = StagingDataManager()
manager.show_data_menu()  # Interactive menu
```