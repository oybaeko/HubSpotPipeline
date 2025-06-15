#!/usr/bin/env python3
"""
Schema comparison and validation for migration
"""

import logging
import json
import subprocess
from typing import Dict, List, Optional
from .config import PROJECT_ID, ENVIRONMENTS

class SchemaAnalyzer:
    """Analyzes and compares schemas across environments"""
    
    def __init__(self):
        self.project_id = PROJECT_ID
        self.environments = ENVIRONMENTS
        self.logger = logging.getLogger('migration.schema')
    
    def get_table_schema(self, dataset: str, table: str) -> Optional[List]:
        """Get table schema using bq CLI"""
        try:
            cmd = f"bq show --schema --format=json {self.project_id}:{dataset}.{table}"
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=False)
            
            if result.returncode == 0:
                schema_data = json.loads(result.stdout)
                return [{"name": field["name"], "type": field["type"], "mode": field.get("mode", "NULLABLE")} 
                       for field in schema_data]
            else:
                if "Not found" in result.stderr or "does not exist" in result.stderr:
                    self.logger.debug(f"Table {table} not found in {dataset}")
                    return None
                else:
                    self.logger.error(f"Error getting schema for {table} in {dataset}: {result.stderr}")
                    return None
        except Exception as e:
            self.logger.error(f"Error getting schema for {table}: {e}")
            return None
    
    def compare_schemas(self, table: str) -> Dict:
        """Compare table schemas across environments"""
        self.logger.info(f"🔍 Comparing schemas for {table}")
        
        schemas = {}
        for env, dataset in self.environments.items():
            schema = self.get_table_schema(dataset, table)
            schemas[env] = schema
        
        # Analyze differences
        comparison = {
            'table': table,
            'schemas': schemas,
            'compatible': True,
            'differences': []
        }
        
        # Compare prod vs staging (our migration path)
        prod_schema = schemas.get('prod')
        staging_schema = schemas.get('staging')
        
        if not prod_schema:
            comparison['differences'].append(f"Table {table} does not exist in prod")
            comparison['compatible'] = False
        elif not staging_schema:
            comparison['differences'].append(f"Table {table} does not exist in staging")
            comparison['compatible'] = False
        else:
            # Compare field by field
            prod_fields = {f['name']: f for f in prod_schema}
            staging_fields = {f['name']: f for f in staging_schema}
            
            # Check for missing fields in staging
            for field_name, field_info in prod_fields.items():
                if field_name not in staging_fields:
                    comparison['differences'].append(f"Field '{field_name}' missing in staging")
                    comparison['compatible'] = False
                elif staging_fields[field_name]['type'] != field_info['type']:
                    comparison['differences'].append(
                        f"Field '{field_name}' type mismatch: "
                        f"prod={field_info['type']}, staging={staging_fields[field_name]['type']}"
                    )
                    comparison['compatible'] = False
            
            # Check for extra fields in staging (usually OK)
            for field_name in staging_fields:
                if field_name not in prod_fields:
                    comparison['differences'].append(f"Extra field '{field_name}' in staging (OK)")
        
        return comparison
    
    def check_prod_snapshot_format(self) -> Dict:
        """Check snapshot_id format in production data with proper error handling"""
        self.logger.info("🔍 Checking production snapshot_id formats")
        
        format_analysis = {}
        
        # Check companies table
        try:
            from google.cloud import bigquery
            from google.api_core.exceptions import NotFound
            
            client = bigquery.Client(project=self.project_id)
            table_ref = f"{self.project_id}.Hubspot_prod.hs_companies"
            
            # First check if we have access to the table
            try:
                client.get_table(table_ref)
            except NotFound:
                self.logger.warning("Production companies table not found")
                format_analysis['hs_companies'] = "Table not found"
                return format_analysis
            except Exception as access_error:
                if "Access Denied" in str(access_error) or "permission" in str(access_error).lower():
                    self.logger.warning("No access to production companies table")
                    format_analysis['hs_companies'] = "No access"
                    format_analysis['hs_deals'] = "No access" 
                    return format_analysis
                else:
                    raise
            
            query = f"""
            SELECT 
                snapshot_id,
                COUNT(*) as record_count
            FROM `{table_ref}`
            GROUP BY snapshot_id
            ORDER BY record_count DESC
            LIMIT 5
            """
            
            result = client.query(query).result()
            if result:
                format_analysis['hs_companies'] = []
                for row in result:
                    snapshot_format = self._analyze_snapshot_format(row.snapshot_id)
                    format_analysis['hs_companies'].append({
                        'snapshot_id': row.snapshot_id,
                        'count': row.record_count,
                        'format': snapshot_format
                    })
                    
        except Exception as e:
            self.logger.warning(f"Could not analyze companies snapshot format: {str(e)[:100]}")
            format_analysis['hs_companies'] = f"Error: {str(e)[:50]}"
        
        # Check deals table  
        try:
            table_ref = f"{self.project_id}.Hubspot_prod.hs_deals"
            
            query = f"""
            SELECT 
                snapshot_id,
                COUNT(*) as record_count
            FROM `{table_ref}`
            GROUP BY snapshot_id
            ORDER BY record_count DESC
            LIMIT 5
            """
            
            result = client.query(query).result()
            if result:
                format_analysis['hs_deals'] = []
                for row in result:
                    snapshot_format = self._analyze_snapshot_format(row.snapshot_id)
                    format_analysis['hs_deals'].append({
                        'snapshot_id': row.snapshot_id,
                        'count': row.record_count,
                        'format': snapshot_format
                    })
                    
        except Exception as e:
            self.logger.warning(f"Could not analyze deals snapshot format: {str(e)[:100]}")
            format_analysis['hs_deals'] = f"Error: {str(e)[:50]}"
        
        return format_analysis
    
    def _analyze_snapshot_format(self, snapshot_id: str) -> Dict:
        """Analyze the format of a snapshot_id"""
        analysis = {
            'has_microseconds': '.000000' in snapshot_id or '.' in snapshot_id,
            'has_timezone': snapshot_id.endswith('Z') or '+' in snapshot_id or snapshot_id.endswith('UTC'),
            'length': len(snapshot_id),
            'format_type': 'unknown'
        }
        
        if len(snapshot_id) == 10:  # YYYY-MM-DD
            analysis['format_type'] = 'date_only'
        elif len(snapshot_id) == 19:  # YYYY-MM-DDTHH:MM:SS
            analysis['format_type'] = 'datetime_no_tz'
        elif len(snapshot_id) == 20 and snapshot_id.endswith('Z'):  # YYYY-MM-DDTHH:MM:SSZ
            analysis['format_type'] = 'datetime_utc'
        elif len(snapshot_id) == 27 and snapshot_id.endswith('Z'):  # YYYY-MM-DDTHH:MM:SS.FFFFFFZ
            analysis['format_type'] = 'datetime_microseconds_utc'
        else:
            analysis['format_type'] = 'custom'
        
        return analysis
    
    def validate_migration_compatibility(self, tables: List[str]) -> bool:
        """Validate that migration can proceed for given tables"""
        self.logger.info("🔍 Validating migration compatibility")
        
        all_compatible = True
        
        for table in tables:
            comparison = self.compare_schemas(table)
            
            if not comparison['compatible']:
                self.logger.error(f"❌ {table} schema incompatible:")
                for diff in comparison['differences']:
                    if 'type mismatch' in diff:
                        self.logger.error(f"  🔄 {diff}")
                    else:
                        self.logger.warning(f"  ⚠️ {diff}")
                all_compatible = False
            else:
                self.logger.info(f"✅ {table} schema compatible")
        
        return all_compatible