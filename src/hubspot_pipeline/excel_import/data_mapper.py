# src/hubspot_pipeline/excel_import/data_mapper.py
import pandas as pd
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from .schema import (
    EXCEL_COMPANY_FIELD_MAP, 
    EXCEL_DEAL_FIELD_MAP, 
    SNAPSHOTS_TO_IMPORT
)

def map_excel_to_schema(sheet_data: Dict[str, pd.DataFrame], snapshot_id: str) -> Dict[str, List[Dict]]:
    """
    Map Excel sheet data to BigQuery schema format for companies and deals only
    
    Args:
        sheet_data: Dictionary of sheet_name -> DataFrame
        snapshot_id: Unique identifier for this import batch
        
    Returns:
        Dictionary of table_name -> list of records ready for BigQuery
    """
    logger = logging.getLogger('hubspot.excel_import')
    mapped_data = {}
    
    for sheet_name, df in sheet_data.items():
        logger.info(f"🔄 Mapping sheet: {sheet_name} ({len(df)} rows)")
        
        if _is_company_sheet(df, sheet_name):
            mapped_data['companies'] = _map_company_data(df, snapshot_id)
            logger.info(f"✅ Mapped {len(mapped_data['companies'])} company records")
            
        elif _is_deal_sheet(df, sheet_name):
            mapped_data['deals'] = _map_deal_data(df, snapshot_id)
            logger.info(f"✅ Mapped {len(mapped_data['deals'])} deal records")
            
        else:
            logger.warning(f"⚠️ Unknown sheet format: {sheet_name}")
            logger.debug(f"Columns: {list(df.columns)}")
    
    return mapped_data

def get_snapshot_configurations() -> List[Dict]:
    """Return the hardcoded list of snapshots to import"""
    return SNAPSHOTS_TO_IMPORT

def _is_company_sheet(df: pd.DataFrame, sheet_name: str) -> bool:
    """Check if this is a company export sheet"""
    # Check sheet name pattern
    if 'company' in sheet_name.lower():
        return True
    
    # Check for company-specific columns
    expected_cols = ['Record ID', 'Company name', 'Company owner', 'Lifecycle Stage']
    return all(col in df.columns for col in expected_cols)

def _is_deal_sheet(df: pd.DataFrame, sheet_name: str) -> bool:
    """Check if this is a deal export sheet"""
    # Check sheet name pattern
    if 'deal' in sheet_name.lower():
        return True
    
    # Check for deal-specific columns (flexible matching)
    deal_indicators = ['deal name', 'dealname', 'deal stage', 'dealstage', 'amount']
    df_cols_lower = [str(col).lower() for col in df.columns]
    
    matches = sum(1 for indicator in deal_indicators if any(indicator in col for col in df_cols_lower))
    return matches >= 2

def _map_company_data(df: pd.DataFrame, snapshot_id: str) -> List[Dict]:
    """
    Map company data to hs_companies schema
    
    Excel columns -> BigQuery columns based on EXCEL_COMPANY_FIELD_MAP
    """
    logger = logging.getLogger('hubspot.excel_import')
    
    mapped_records = []
    
    for idx, row in df.iterrows():
        try:
            # Build record following hs_companies schema
            record = {
                'snapshot_id': snapshot_id,
            }
            
            # Map Excel columns to BigQuery columns
            for excel_col, bq_col in EXCEL_COMPANY_FIELD_MAP.items():
                if bq_col is None:  # Skip metadata fields
                    continue
                    
                if excel_col in df.columns:
                    value = row[excel_col]
                    
                    if bq_col == 'timestamp':
                        # Skip timestamp if not in table schema
                        continue
                    elif bq_col == 'lifecycle_stage':
                        record[bq_col] = _normalize_lifecycle_stage(value)
                    elif bq_col == 'lead_status':
                        record[bq_col] = _normalize_lead_status(value)
                    elif bq_col == 'company_id':
                        record[bq_col] = _safe_string_id(value)
                    elif bq_col == 'hubspot_owner_id':
                        # Convert owner name to ID using lookup table
                        record[bq_col] = _convert_owner_name_to_id(value)
                    else:
                        record[bq_col] = _safe_string(value)
                else:
                    # Column not found in Excel, set to None
                    record[bq_col] = None
            
            # Set fields not available in Excel to None
            excel_fields = set(EXCEL_COMPANY_FIELD_MAP.values())
            schema_fields = [
                'development_category', 'hiring_developers', 'inhouse_developers',
                'proff_likviditetsgrad', 'proff_link', 'proff_lonnsomhet', 'proff_soliditet'
            ]
            
            for field in schema_fields:
                if field not in excel_fields:
                    record[field] = None
            
            mapped_records.append(record)
            
        except Exception as e:
            logger.warning(f"⚠️ Error mapping company row {idx}: {e}")
            logger.debug(f"Problematic row: {dict(row)}")
            continue
    
    logger.info(f"✅ Successfully mapped {len(mapped_records)}/{len(df)} company records")
    if mapped_records:
        logger.debug(f"Sample company record: {_safe_record_preview(mapped_records[0])}")
    
    return mapped_records

def _map_deal_data(df: pd.DataFrame, snapshot_id: str) -> List[Dict]:
    """
    Map deal data to hs_deals schema
    
    Excel columns -> BigQuery columns based on EXCEL_DEAL_FIELD_MAP
    """
    logger = logging.getLogger('hubspot.excel_import')
    
    mapped_records = []
    
    for idx, row in df.iterrows():
        try:
            record = {
                'snapshot_id': snapshot_id,
            }
            
            # Map Excel columns to BigQuery columns
            for excel_col, bq_col in EXCEL_DEAL_FIELD_MAP.items():
                if bq_col is None:  # Skip if no mapping
                    continue
                    
                if excel_col in df.columns:
                    value = row[excel_col]
                    
                    if bq_col == 'timestamp':
                        record[bq_col] = _parse_timestamp(value)
                    elif bq_col == 'amount':
                        record[bq_col] = _parse_amount(value)
                    elif bq_col in ['deal_id', 'associated_company_id']:
                        record[bq_col] = _safe_string_id(value)
                    elif bq_col == 'owner_id':
                        # Convert owner name to ID using lookup table
                        record[bq_col] = _convert_owner_name_to_id(value)
                    else:
                        record[bq_col] = _safe_string(value)
                else:
                    # Column not found, try to find similar column
                    similar_col = _find_similar_column(excel_col, df.columns)
                    if similar_col:
                        value = row[similar_col]
                        logger.debug(f"Using similar column '{similar_col}' for '{excel_col}'")
                        
                        if bq_col == 'timestamp':
                            # Skip timestamp if not in table schema
                            continue
                        elif bq_col == 'amount':
                            record[bq_col] = _parse_amount(value)
                        elif bq_col in ['deal_id', 'associated_company_id']:
                            record[bq_col] = _safe_string_id(value)
                        elif bq_col == 'owner_id':
                            # Convert owner name to ID using lookup table
                            record[bq_col] = _convert_owner_name_to_id(value)
                        else:
                            record[bq_col] = _safe_string(value)
                    else:
                        record[bq_col] = None
            
            mapped_records.append(record)
            
        except Exception as e:
            logger.warning(f"⚠️ Error mapping deal row {idx}: {e}")
            logger.debug(f"Problematic row: {dict(row)}")
            continue
    
    logger.info(f"✅ Successfully mapped {len(mapped_records)}/{len(df)} deal records")
    if mapped_records:
        logger.debug(f"Sample deal record: {_safe_record_preview(mapped_records[0])}")
    
    return mapped_records

def _find_similar_column(target: str, available_columns: List[str]) -> Optional[str]:
    """Find a similar column name (case-insensitive, flexible matching)"""
    target_lower = target.lower().replace(' ', '').replace('_', '')
    
    for col in available_columns:
        col_lower = str(col).lower().replace(' ', '').replace('_', '')
        if target_lower == col_lower or target_lower in col_lower or col_lower in target_lower:
            return col
    
    return None

def _safe_string(value) -> Optional[str]:
    """Safely convert value to string, handling NaN/None"""
    if pd.isna(value) or value is None or value == '':
        return None
    return str(value).strip() if str(value).strip() else None

def _safe_string_id(value) -> Optional[str]:
    """Safely convert ID value to string"""
    if pd.isna(value) or value is None or value == '':
        return None
    
    # Handle numeric IDs
    if isinstance(value, (int, float)):
        return str(int(value))
    
    return str(value).strip() if str(value).strip() else None

def _parse_amount(value) -> Optional[float]:
    """Parse amount/currency value to float"""
    if pd.isna(value) or value is None or value == '':
        return None
    
    try:
        # Remove currency symbols and commas
        if isinstance(value, str):
            cleaned = value.replace('$', '').replace(',', '').replace('€', '').replace('kr', '').strip()
            return float(cleaned) if cleaned else None
        else:
            return float(value)
    except (ValueError, TypeError):
        return None

def _normalize_lifecycle_stage(value) -> Optional[str]:
    """Normalize lifecycle stage to match your schema expectations"""
    if pd.isna(value):
        return None
    
    # Convert to lowercase for consistency
    normalized = str(value).lower().strip()
    
    # Map common variations (keep consistent with your existing data)
    stage_mapping = {
        'lead': 'lead',
        'sales qualified lead': 'salesqualifiedlead', 
        'opportunity': 'opportunity',
        'customer': 'customer',
        'closed-won': 'closed-won',
        'disqualified': 'disqualified'
    }
    
    return stage_mapping.get(normalized, normalized)

def _normalize_lead_status(value) -> Optional[str]:
    """Normalize lead status to match your schema expectations"""
    if pd.isna(value):
        return None
    
    # Convert to lowercase and replace spaces with underscores for consistency
    normalized = str(value).lower().strip().replace(' ', '_')
    
    # Map to match your stage_mapping table
    status_mapping = {
        'new': 'new',
        'restart': 'restart', 
        'attempted_to_contact': 'attempted_to_contact',
        'connected': 'connected',
        'nurturing': 'nurturing',
        'opportunity': 'opportunity'
    }
    
    return status_mapping.get(normalized, normalized)

def _parse_timestamp(value) -> Optional[str]:
    """Parse timestamp from Excel date/datetime to ISO format"""
    if pd.isna(value):
        return None
    
    try:
        # Handle different timestamp formats
        if isinstance(value, str):
            # Try parsing ISO format first
            if 'T' in value:
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
            else:
                dt = pd.to_datetime(value)
        else:
            # pandas datetime or other datetime object
            dt = pd.to_datetime(value)
        
        # Convert to UTC ISO format for BigQuery
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
    except Exception as e:
        logger = logging.getLogger('hubspot.excel_import')
        logger.warning(f"⚠️ Could not parse timestamp '{value}': {e}")
        return None

def _safe_record_preview(record: Dict) -> Dict:
    """Create a safe preview of record for logging (truncate long values)"""
    preview = {}
    for key, value in record.items():
        if value is None:
            preview[key] = None
        elif isinstance(value, str) and len(value) > 50:
            preview[key] = value[:50] + "..."
        else:
            preview[key] = value
    return preview

def _convert_owner_name_to_id(owner_name: str) -> Optional[str]:
    """Convert owner name to owner ID using lookup table"""
    if pd.isna(owner_name) or owner_name is None:
        return None
    
    # Import here to avoid circular imports
    from .schema import OWNERS_LOOKUP
    
    logger = logging.getLogger('hubspot.excel_import')
    
    # Normalize the name for lookup
    name_lower = str(owner_name).lower().strip()
    
    # Try exact match first
    if name_lower in OWNERS_LOOKUP:
        owner_id = OWNERS_LOOKUP[name_lower]
        logger.debug(f"Owner lookup: '{owner_name}' -> {owner_id}")
        return owner_id
    
    # Try partial matches (first name only)
    for lookup_name, owner_id in OWNERS_LOOKUP.items():
        if name_lower in lookup_name or lookup_name in name_lower:
            logger.debug(f"Owner partial match: '{owner_name}' -> '{lookup_name}' -> {owner_id}")
            return owner_id
    
    # No match found - log warning and return the original name
    logger.warning(f"⚠️ Owner name '{owner_name}' not found in lookup table")
    return owner_name  # Return original name as fallback