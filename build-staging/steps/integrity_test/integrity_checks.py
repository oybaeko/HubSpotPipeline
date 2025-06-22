#!/usr/bin/env python3
"""
Core integrity checking logic - UPDATED VERSION with lowercase normalization
"""

import logging
from typing import List, Dict
from google.cloud import bigquery

from models import IntegrityIssue
from config import (
    REFERENCE_RELATIONSHIPS, REQUIRED_FIELDS, FORMAT_VALIDATIONS,
    TABLES_TO_CHECK, UNIQUE_CONSTRAINTS, EMAIL_TABLES, DATA_TABLES,
    LOWERCASE_NORMALIZATION_FIELDS
)

class IntegrityChecker:
    """Core integrity checking functionality"""
    
    def __init__(self, project_id: str, dataset: str):
        self.project_id = project_id
        self.dataset = dataset
        self.logger = logging.getLogger('integrity_checker')
    
    def get_table_record_counts(self, client: bigquery.Client) -> Dict[str, int]:
        """Get record counts for all tables"""
        self.logger.info("📊 Getting table record counts...")
        
        counts = {}
        
        for table in TABLES_TO_CHECK:
            try:
                table_ref = f"{self.project_id}.{self.dataset}.{table}"
                count_query = f"SELECT COUNT(*) as count FROM `{table_ref}`"
                result = client.query(count_query).result()
                
                for row in result:
                    counts[table] = row.count
                    self.logger.info(f"  📋 {table}: {row.count:,} records")
                    break
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Could not count {table}: {e}")
                counts[table] = 0
        
        return counts
    
    def check_blank_reference_fields(self, client: bigquery.Client) -> List[IntegrityIssue]:
        """Check for blank/empty strings in reference fields that should be NULL"""
        self.logger.info("🔍 Checking for blank reference fields...")
        issues = []
        
        for table, relationships in REFERENCE_RELATIONSHIPS.items():
            try:
                table_ref = f"{self.project_id}.{self.dataset}.{table}"
                
                # Check if table exists first
                try:
                    client.get_table(table_ref)
                except Exception as e:
                    if "not found" in str(e).lower():
                        self.logger.info(f"ℹ️ Table {table} not found, skipping")
                        continue
                    else:
                        raise
                
                for field, (ref_table, ref_field, description) in relationships.items():
                    # Check for empty strings
                    empty_query = f"""
                    SELECT COUNT(*) as count 
                    FROM `{table_ref}` 
                    WHERE {field} = ''
                    """
                    
                    result = client.query(empty_query).result()
                    for row in result:
                        if row.count > 0:
                            issues.append(IntegrityIssue(
                                table=table,
                                field=field,
                                issue_type='blank_reference_field',
                                count=row.count,
                                severity='warning',
                                description=f"Empty string values in {field} should be NULL",
                                sample_values=["''"]
                            ))
                            self.logger.warning(f"⚠️ {table}.{field}: {row.count} empty strings")
                        break
                        
            except Exception as e:
                self.logger.warning(f"⚠️ Could not check blank fields in {table}: {e}")
        
        return issues
    
    def check_referential_integrity(self, client: bigquery.Client) -> List[IntegrityIssue]:
        """Check referential integrity (foreign key relationships)"""
        self.logger.info("🔗 Checking referential integrity...")
        issues = []
        
        for table, relationships in REFERENCE_RELATIONSHIPS.items():
            try:
                table_ref = f"{self.project_id}.{self.dataset}.{table}"
                
                # Check if table exists
                try:
                    client.get_table(table_ref)
                except Exception as e:
                    if "not found" in str(e).lower():
                        self.logger.info(f"ℹ️ Table {table} not found, skipping")
                        continue
                    else:
                        raise
                
                for field, (ref_table, ref_field, description) in relationships.items():
                    ref_table_ref = f"{self.project_id}.{self.dataset}.{ref_table}"
                    
                    # Check if reference table exists
                    try:
                        client.get_table(ref_table_ref)
                    except Exception as e:
                        if "not found" in str(e).lower():
                            self.logger.info(f"ℹ️ Reference table {ref_table} not found, skipping {table}.{field}")
                            continue
                        else:
                            raise
                    
                    # Find orphaned records (references that don't exist in target table)
                    orphan_query = f"""
                    SELECT COUNT(*) as count
                    FROM `{table_ref}` t
                    WHERE t.{field} IS NOT NULL 
                      AND t.{field} != ''
                      AND NOT EXISTS (
                          SELECT 1 FROM `{ref_table_ref}` r 
                          WHERE r.{ref_field} = t.{field}
                      )
                    """
                    
                    result = client.query(orphan_query).result()
                    for row in result:
                        if row.count > 0:
                            # Get sample orphaned values
                            sample_query = f"""
                            SELECT DISTINCT t.{field}
                            FROM `{table_ref}` t
                            WHERE t.{field} IS NOT NULL 
                              AND t.{field} != ''
                              AND NOT EXISTS (
                                  SELECT 1 FROM `{ref_table_ref}` r 
                                  WHERE r.{ref_field} = t.{field}
                              )
                            LIMIT 5
                            """
                            
                            sample_result = client.query(sample_query).result()
                            sample_values = [getattr(sample_row, field) for sample_row in sample_result]
                            
                            issues.append(IntegrityIssue(
                                table=table,
                                field=field,
                                issue_type='orphaned_reference',
                                count=row.count,
                                severity='critical',
                                description=f"Orphaned {description} - {field} values don't exist in {ref_table}.{ref_field}",
                                sample_values=sample_values
                            ))
                            self.logger.error(f"❌ {table}.{field}: {row.count} orphaned references")
                        break
                        
            except Exception as e:
                self.logger.warning(f"⚠️ Could not check referential integrity for {table}: {e}")
        
        return issues
    
    def check_required_fields(self, client: bigquery.Client) -> List[IntegrityIssue]:
        """Check for NULL/empty values in required fields - FIXED timestamp handling"""
        self.logger.info("✅ Checking required fields...")
        issues = []
        
        for table, required_fields in REQUIRED_FIELDS.items():
            try:
                table_ref = f"{self.project_id}.{self.dataset}.{table}"
                
                # Check if table exists
                try:
                    client.get_table(table_ref)
                except Exception as e:
                    if "not found" in str(e).lower():
                        self.logger.info(f"ℹ️ Table {table} not found, skipping")
                        continue
                    else:
                        raise
                
                for field in required_fields:
                    # FIXED: Handle timestamp fields differently to avoid casting errors
                    if table == 'hs_snapshot_registry' and field == 'snapshot_id':
                        # For snapshot_id in registry, we check for NULL but not empty string (it's a timestamp)
                        null_query = f"""
                        SELECT COUNT(*) as count 
                        FROM `{table_ref}` 
                        WHERE {field} IS NULL
                        """
                    else:
                        # For other fields, check both NULL and empty strings
                        null_query = f"""
                        SELECT COUNT(*) as count 
                        FROM `{table_ref}` 
                        WHERE {field} IS NULL OR {field} = ''
                        """
                    
                    result = client.query(null_query).result()
                    for row in result:
                        if row.count > 0:
                            issues.append(IntegrityIssue(
                                table=table,
                                field=field,
                                issue_type='missing_required_field',
                                count=row.count,
                                severity='critical',
                                description=f"Required field {field} has NULL or empty values",
                                sample_values=["NULL", "''"] if table != 'hs_snapshot_registry' else ["NULL"]
                            ))
                            self.logger.error(f"❌ {table}.{field}: {row.count} missing required values")
                        break
                        
            except Exception as e:
                self.logger.warning(f"⚠️ Could not check required fields for {table}: {e}")
        
        return issues
    
    def check_format_validations(self, client: bigquery.Client) -> List[IntegrityIssue]:
        """Check field format validations (email, phone, etc.)"""
        self.logger.info("📧 Checking format validations...")
        issues = []
        
        for table, field in EMAIL_TABLES:
            try:
                table_ref = f"{self.project_id}.{self.dataset}.{table}"
                
                # Check if table exists
                try:
                    client.get_table(table_ref)
                except Exception as e:
                    if "not found" in str(e).lower():
                        self.logger.info(f"ℹ️ Table {table} not found, skipping")
                        continue
                    else:
                        raise
                
                # Check for invalid email formats
                invalid_email_query = f"""
                SELECT COUNT(*) as count
                FROM `{table_ref}`
                WHERE {field} IS NOT NULL 
                  AND {field} != ''
                  AND NOT REGEXP_CONTAINS({field}, r'{FORMAT_VALIDATIONS["email"]}')
                """
                
                result = client.query(invalid_email_query).result()
                for row in result:
                    if row.count > 0:
                        # Get sample invalid emails
                        sample_query = f"""
                        SELECT DISTINCT {field}
                        FROM `{table_ref}`
                        WHERE {field} IS NOT NULL 
                          AND {field} != ''
                          AND NOT REGEXP_CONTAINS({field}, r'{FORMAT_VALIDATIONS["email"]}')
                        LIMIT 5
                        """
                        
                        sample_result = client.query(sample_query).result()
                        sample_values = [getattr(sample_row, field) for sample_row in sample_result]
                        
                        issues.append(IntegrityIssue(
                            table=table,
                            field=field,
                            issue_type='invalid_format',
                            count=row.count,
                            severity='warning',
                            description=f"Invalid email format in {field}",
                            sample_values=sample_values
                        ))
                        self.logger.warning(f"⚠️ {table}.{field}: {row.count} invalid email formats")
                    break
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Could not check email format for {table}.{field}: {e}")
        
        return issues
    
    def check_lowercase_normalization(self, client: bigquery.Client) -> List[IntegrityIssue]:
        """NEW: Check for fields that should be lowercase normalized"""
        self.logger.info("🔤 Checking lowercase normalization...")
        issues = []
        
        for table, fields in LOWERCASE_NORMALIZATION_FIELDS.items():
            try:
                table_ref = f"{self.project_id}.{self.dataset}.{table}"
                
                # Check if table exists
                try:
                    client.get_table(table_ref)
                except Exception as e:
                    if "not found" in str(e).lower():
                        self.logger.info(f"ℹ️ Table {table} not found, skipping lowercase normalization")
                        continue
                    else:
                        raise
                
                for field, description in fields.items():
                    # Check for values that are not lowercase
                    mixed_case_query = f"""
                    SELECT COUNT(*) as count
                    FROM `{table_ref}`
                    WHERE {field} IS NOT NULL 
                      AND {field} != ''
                      AND {field} != LOWER({field})
                    """
                    
                    result = client.query(mixed_case_query).result()
                    for row in result:
                        if row.count > 0:
                            # Get sample mixed case values
                            sample_query = f"""
                            SELECT DISTINCT {field}
                            FROM `{table_ref}`
                            WHERE {field} IS NOT NULL 
                              AND {field} != ''
                              AND {field} != LOWER({field})
                            LIMIT 5
                            """
                            
                            sample_result = client.query(sample_query).result()
                            sample_values = [getattr(sample_row, field) for sample_row in sample_result]
                            
                            issues.append(IntegrityIssue(
                                table=table,
                                field=field,
                                issue_type='case_normalization',
                                count=row.count,
                                severity='warning',
                                description=description,
                                sample_values=sample_values
                            ))
                            self.logger.warning(f"⚠️ {table}.{field}: {row.count} records not lowercase normalized")
                        break
                        
            except Exception as e:
                self.logger.warning(f"⚠️ Could not check lowercase normalization for {table}: {e}")
        
        return issues
    
    def check_snapshot_consistency(self, client: bigquery.Client) -> List[IntegrityIssue]:
        """Check snapshot consistency across tables"""
        self.logger.info("📸 Checking snapshot consistency...")
        issues = []
        
        try:
            # Check if registry table exists
            registry_ref = f"{self.project_id}.{self.dataset}.hs_snapshot_registry"
            try:
                client.get_table(registry_ref)
            except Exception as e:
                if "not found" in str(e).lower():
                    self.logger.info("ℹ️ Snapshot registry not found, skipping consistency check")
                    return issues
                else:
                    raise
            
            # Get snapshots from registry - FIXED: Convert to string to handle different types
            registry_snapshots_query = f"""
            SELECT DISTINCT CAST(snapshot_id AS STRING) as snapshot_id
            FROM `{registry_ref}`
            ORDER BY snapshot_id
            """
            
            registry_result = client.query(registry_snapshots_query).result()
            registry_snapshots = {row.snapshot_id for row in registry_result}
            
            if not registry_snapshots:
                issues.append(IntegrityIssue(
                    table='hs_snapshot_registry',
                    field='snapshot_id',
                    issue_type='missing_snapshots',
                    count=0,
                    severity='critical',
                    description="No snapshots found in registry",
                    sample_values=[]
                ))
                return issues
            
            # Check each data table for snapshot consistency
            for table in DATA_TABLES:
                table_ref = f"{self.project_id}.{self.dataset}.{table}"
                
                # Check if table exists
                try:
                    client.get_table(table_ref)
                except Exception as e:
                    if "not found" in str(e).lower():
                        self.logger.info(f"ℹ️ Table {table} not found, skipping")
                        continue
                    else:
                        raise
                
                # Get snapshots in this table - FIXED: Convert to string consistently
                table_snapshots_query = f"""
                SELECT CAST(snapshot_id AS STRING) as snapshot_id, COUNT(*) as record_count
                FROM `{table_ref}`
                GROUP BY snapshot_id
                ORDER BY snapshot_id
                """
                
                table_result = client.query(table_snapshots_query).result()
                table_snapshots = {row.snapshot_id: row.record_count for row in table_result}
                
                # Check for snapshots in table but not in registry
                unregistered_snapshots = set(table_snapshots.keys()) - registry_snapshots
                if unregistered_snapshots:
                    issues.append(IntegrityIssue(
                        table=table,
                        field='snapshot_id',
                        issue_type='unregistered_snapshot',
                        count=len(unregistered_snapshots),
                        severity='warning',
                        description=f"Snapshots in {table} not registered in registry",
                        sample_values=list(unregistered_snapshots)[:5]
                    ))
                    self.logger.warning(f"⚠️ {table}: {len(unregistered_snapshots)} unregistered snapshots")
                
                # Check for snapshots in registry but not in table
                missing_snapshots = registry_snapshots - set(table_snapshots.keys())
                if missing_snapshots:
                    issues.append(IntegrityIssue(
                        table=table,
                        field='snapshot_id',
                        issue_type='missing_snapshot_data',
                        count=len(missing_snapshots),
                        severity='info',
                        description=f"Snapshots registered but no data in {table}",
                        sample_values=list(missing_snapshots)[:5]
                    ))
                    self.logger.info(f"ℹ️ {table}: {len(missing_snapshots)} registered snapshots with no data")
                    
        except Exception as e:
            self.logger.warning(f"⚠️ Could not check snapshot consistency: {e}")
        
        return issues
    
    def check_duplicate_records(self, client: bigquery.Client) -> List[IntegrityIssue]:
        """Check for duplicate records in tables with unique constraints"""
        self.logger.info("🔍 Checking for duplicate records...")
        issues = []
        
        for table, unique_fields in UNIQUE_CONSTRAINTS.items():
            try:
                table_ref = f"{self.project_id}.{self.dataset}.{table}"
                
                # Check if table exists
                try:
                    client.get_table(table_ref)
                except Exception as e:
                    if "not found" in str(e).lower():
                        self.logger.info(f"ℹ️ Table {table} not found, skipping")
                        continue
                    else:
                        raise
                
                # Build group by clause
                group_by_fields = ', '.join(unique_fields)
                
                # Check for duplicates
                duplicate_query = f"""
                SELECT COUNT(*) as duplicate_groups
                FROM (
                    SELECT {group_by_fields}, COUNT(*) as count
                    FROM `{table_ref}`
                    GROUP BY {group_by_fields}
                    HAVING COUNT(*) > 1
                )
                """
                
                result = client.query(duplicate_query).result()
                for row in result:
                    if row.duplicate_groups > 0:
                        # Get total duplicate records
                        total_duplicates_query = f"""
                        SELECT SUM(count - 1) as total_duplicates
                        FROM (
                            SELECT {group_by_fields}, COUNT(*) as count
                            FROM `{table_ref}`
                            GROUP BY {group_by_fields}
                            HAVING COUNT(*) > 1
                        )
                        """
                        
                        total_result = client.query(total_duplicates_query).result()
                        for total_row in total_result:
                            issues.append(IntegrityIssue(
                                table=table,
                                field=f"({', '.join(unique_fields)})",
                                issue_type='duplicate_records',
                                count=total_row.total_duplicates,
                                severity='critical',
                                description=f"Duplicate records found - {row.duplicate_groups} groups with duplicates",
                                sample_values=[f"{row.duplicate_groups} duplicate groups"]
                            ))
                            self.logger.error(f"❌ {table}: {total_row.total_duplicates} duplicate records in {row.duplicate_groups} groups")
                            break
                    break
                    
            except Exception as e:
                self.logger.warning(f"⚠️ Could not check duplicates for {table}: {e}")
        
        return issues
    
    def check_data_distribution(self, client: bigquery.Client) -> List[IntegrityIssue]:
        """Check data distribution and potential anomalies"""
        self.logger.info("📊 Checking data distribution...")
        issues = []
        
        try:
            # Check if data tables exist
            existing_tables = []
            for table in DATA_TABLES:
                table_ref = f"{self.project_id}.{self.dataset}.{table}"
                try:
                    client.get_table(table_ref)
                    existing_tables.append(table)
                except Exception as e:
                    if "not found" in str(e).lower():
                        self.logger.info(f"ℹ️ Table {table} not found, skipping distribution check")
                        continue
                    else:
                        raise
            
            if not existing_tables:
                self.logger.info("ℹ️ No data tables found for distribution analysis")
                return issues
            
            # Build union query for existing tables only - FIXED: Cast snapshot_id consistently
            union_parts = []
            for table in existing_tables:
                if table == 'hs_companies':
                    union_parts.append(f"SELECT CAST(snapshot_id AS STRING) as snapshot_id, company_id, NULL as deal_id FROM `{self.project_id}.{self.dataset}.{table}`")
                elif table == 'hs_deals':
                    union_parts.append(f"SELECT CAST(snapshot_id AS STRING) as snapshot_id, NULL as company_id, deal_id FROM `{self.project_id}.{self.dataset}.{table}`")
            
            if not union_parts:
                return issues
            
            # Check snapshot distribution
            snapshot_dist_query = f"""
            SELECT 
                snapshot_id,
                COUNT(DISTINCT company_id) as companies,
                COUNT(DISTINCT deal_id) as deals
            FROM (
                {' UNION ALL '.join(union_parts)}
            )
            GROUP BY snapshot_id
            ORDER BY snapshot_id
            """
            
            result = client.query(snapshot_dist_query).result()
            snapshot_stats = []
            
            for row in result:
                snapshot_stats.append({
                    'snapshot_id': str(row.snapshot_id),  # Convert to string to handle different formats
                    'companies': row.companies,
                    'deals': row.deals,
                    'total': row.companies + row.deals
                })
            
            if snapshot_stats and len(snapshot_stats) > 1:  # Need at least 2 snapshots for comparison
                # Calculate averages
                avg_companies = sum(s['companies'] for s in snapshot_stats) / len(snapshot_stats)
                avg_deals = sum(s['deals'] for s in snapshot_stats) / len(snapshot_stats)
                
                # Check for anomalous snapshots (significantly different from average)
                for stats in snapshot_stats:
                    companies_ratio = stats['companies'] / avg_companies if avg_companies > 0 else 0
                    deals_ratio = stats['deals'] / avg_deals if avg_deals > 0 else 0
                    
                    # Flag if snapshot has less than 50% or more than 200% of average
                    if companies_ratio < 0.5 or companies_ratio > 2.0:
                        issues.append(IntegrityIssue(
                            table='hs_companies',
                            field='snapshot_id',
                            issue_type='anomalous_distribution',
                            count=stats['companies'],
                            severity='warning',
                            description=f"Snapshot {stats['snapshot_id']} has unusual company count: {stats['companies']} (avg: {avg_companies:.0f})",
                            sample_values=[stats['snapshot_id']]
                        ))
                    
                    if deals_ratio < 0.5 or deals_ratio > 2.0:
                        issues.append(IntegrityIssue(
                            table='hs_deals',
                            field='snapshot_id',
                            issue_type='anomalous_distribution',
                            count=stats['deals'],
                            severity='warning',
                            description=f"Snapshot {stats['snapshot_id']} has unusual deal count: {stats['deals']} (avg: {avg_deals:.0f})",
                            sample_values=[stats['snapshot_id']]
                        ))
                        
        except Exception as e:
            self.logger.warning(f"⚠️ Could not check data distribution: {e}")
        
        return issues