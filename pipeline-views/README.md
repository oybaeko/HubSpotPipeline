# BigQuery Views Deployment Tool

Interactive deployment tool for HubSpot Pipeline analytics views. Provides safe, environment-aware deployment of BigQuery views with protection against accidental production overwrites.

## 🎯 Purpose

This tool allows you to deploy and manage BigQuery views for Looker Studio reports without redeploying your Cloud Functions. It's designed to work alongside the HubSpot Pipeline system and respects your environment setup (development, staging, production).

## 📁 Folder Structure

```
pipeline-views/
├── sql/
│   ├── vw_pipeline_history_enhanced.sql    # Enhanced view for Looker reports
│   └── (additional view files...)          # Add more views here
├── deploy_views.py                         # Main deployment script
├── requirements.txt                        # Python dependencies
└── README.md                              # This file
```

## 🛡️ Environment Safety Features

- **Production Protection**: Requires typing "PRODUCTION" + "YES" for confirmation
- **Staging Protection**: Requires typing "YES" for confirmation  
- **Development**: Simple y/n confirmation
- **Auto-detection**: Uses same environment config as your main pipeline

## 🚀 Quick Start

### 1. Setup

```bash
# Create the folder structure
mkdir pipeline-views
cd pipeline-views
mkdir sql

# Install dependencies
pip install -r requirements.txt

# Add your SQL files to the sql/ folder
```

### 2. Run the Tool

```bash
# From your main hubspot-pipeline project directory
python pipeline-views/deploy_views.py
```

### 3. Interactive Menu

The tool will show:
- Current environment (dev/staging/prod)
- Available SQL files
- Existing views status
- Deployment options

## 📊 Menu Options

```
Available SQL Files:
==================================================
 1. vw_pipeline_history_enhanced.sql    🔵 new
 2. vw_current_pipeline_detailed.sql    🟢 exists

 3. Deploy ALL files
 4. Validate SQL only (dry run)
 5. Show current schema info
 6. Exit
```

## 🔍 Features

### ✅ SQL Validation
- **Dry run validation**: Test SQL syntax without deploying
- **Schema compatibility**: Validates references to known tables
- **Placeholder replacement**: Supports `${PROJECT_ID}` and `${DATASET_ID}`

### ✅ Environment Awareness
- **Auto-detection**: Reads from same config as main pipeline
- **Environment warnings**: Clear indicators for prod/staging
- **Smart confirmations**: Different levels based on environment risk

### ✅ Schema Integration
- **Compatible tables**: Works with your existing HubSpot schema
- **View status**: Shows which views exist vs. new ones
- **Reference validation**: Checks that SQL references known tables

## 📝 Writing SQL Files

### Placeholder Support

Use these placeholders in your SQL files:

```sql
-- Both formats supported
CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.my_view` AS
CREATE OR REPLACE VIEW `{project}.{dataset}.my_view` AS
```

### Schema Compatibility

Reference your existing tables:

```sql
-- Core data tables
FROM `${PROJECT_ID}.${DATASET_ID}.hs_companies` c
FROM `${PROJECT_ID}.${DATASET_ID}.hs_deals` d
FROM `${PROJECT_ID}.${DATASET_ID}.hs_owners` o

-- Pipeline tables
FROM `${PROJECT_ID}.${DATASET_ID}.hs_pipeline_units_snapshot` pus
FROM `${PROJECT_ID}.${DATASET_ID}.hs_stage_mapping` sm

-- Existing views
FROM `${PROJECT_ID}.${DATASET_ID}.vw_pipeline_history_by_snapshot` ph
```

## 🎨 Example: Enhanced View for Looker Studio

The included `vw_pipeline_history_enhanced.sql` demonstrates:

- **Filtering**: Excludes disqualified and closed stages
- **Joins**: Adds company_type and deal_type for filtering
- **Aggregation**: Provides stage counts for stacked area charts
- **Time series**: Maintains historical accuracy with snapshot matching

Perfect for creating stacked area charts showing pipeline progression over time.

## 🔧 Advanced Usage

### Command Line Mode
```bash
# Skip interactive menu (uses environment detection)
python deploy_views.py --auto

# Deploy specific file
python deploy_views.py --file vw_pipeline_history_enhanced.sql
```

### Environment Variables
The tool reads from your existing pipeline configuration:
- `BIGQUERY_PROJECT_ID`
- `BIGQUERY_DATASET_ID` 
- `ENVIRONMENT`

### Custom SQL Structure
```sql
-- Template for new views
CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.vw_your_view_name` AS
WITH base_data AS (
  SELECT *
  FROM `${PROJECT_ID}.${DATASET_ID}.hs_pipeline_units_snapshot`
  WHERE stage_level NOT IN (-1, 9)  -- Exclude disqualified/closed
)
SELECT * FROM base_data;
```

## 📈 Integration with Looker Studio

### Data Source Setup
1. Connect to BigQuery
2. Select your enhanced view (e.g., `vw_pipeline_history_enhanced`)
3. Configure chart:
   - **X-axis**: `snapshot_id` (time dimension)
   - **Y-axis**: Stage counts (`nurturing_count`, `lead_count`, etc.)
   - **Filters**: `owner_name`, `company_type`, `deal_type`

### Recommended Chart Types
- **Stacked Area**: Pipeline progression over time
- **Bar Chart**: Current pipeline by owner
- **Line Chart**: Score trends by owner
- **Table**: Detailed pipeline breakdown

## 🛠️ Troubleshooting

### Common Issues

**"Failed to get environment info"**
```bash
# Ensure you're running from the correct directory
cd /path/to/your/hubspot-pipeline-project
python pipeline-views/deploy_views.py
```

**"No known tables referenced"**
- Check your SQL syntax
- Ensure table names match your schema
- Use the validation option to test

**"Permission denied"**
- Verify BigQuery permissions
- Check Google Cloud authentication
- Ensure correct project/dataset access

### Debug Mode
```python
# Add to deploy_views.py for debugging
import logging
logging.getLogger('google.cloud.bigquery').setLevel(logging.DEBUG)
```

## 🔐 Security Best Practices

- **Never commit credentials** to the SQL files
- **Use placeholders** for all project/dataset references
- **Test in development first** before staging/production
- **Review confirmations carefully** in production environment

## 🤝 Contributing

### Adding New Views
1. Create SQL file in `sql/` folder
2. Use standard placeholders (`${PROJECT_ID}`, `${DATASET_ID}`)
3. Reference existing schema tables
4. Test with dry run validation

### Naming Convention
- Use descriptive prefixes: `vw_` for views
- Include purpose: `_enhanced`, `_detailed`, `_summary`
- Follow existing patterns: `vw_pipeline_history_enhanced.sql`

## 📚 Related Documentation

- [HubSpot Pipeline Schema](../src/hubspot_pipeline/schema.py)
- [Existing Views](../src/hubspot_pipeline/hubspot_scoring/views/)
- [Main Testing Tool](../main.py)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Looker Studio Guide](https://support.google.com/looker-studio/)

## 📄 License

Same license as the main HubSpot Pipeline project.

---

**⚠️ Remember**: Always test in development first, and be extra careful in staging/production environments!