# build-staging/steps/excel_import/excel_processor.py
# UPDATED VERSION - Uses Excel-specific configuration only

import pandas as pd
import logging
from typing import Dict, List, Any, Tuple
from pathlib import Path

# Import Excel-specific configuration
from .schema import SNAPSHOTS_TO_IMPORT

class ExcelProcessor:
    """Process Excel files containing HubSpot export data for multiple snapshots"""
    
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.logger = logging.getLogger('hubspot.excel_import')
        
        if not self.file_path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.file_path}")
        
        if not self.file_path.suffix.lower() in ['.xlsx', '.xls']:
            raise ValueError(f"File must be Excel format (.xlsx or .xls): {self.file_path}")
        
    def extract_all_snapshots(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Extract all configured snapshots from Excel file
        
        Returns:
            Dictionary of snapshot_date -> {companies: DataFrame, deals: DataFrame}
        """
        self.logger.info(f"📂 Processing Excel file: {self.file_path}")
        
        try:
            # Read all sheets at once
            all_sheets = pd.read_excel(self.file_path, sheet_name=None, engine='openpyxl')
            self.logger.info(f"Found {len(all_sheets)} total sheets in Excel file")
            
        except Exception as e:
            raise RuntimeError(f"Failed to read Excel file: {e}")
        
        # Extract snapshots based on configuration
        snapshots_data = {}
        
        for snapshot_config in SNAPSHOTS_TO_IMPORT:
            snapshot_date = snapshot_config["date"]
            company_sheet_name = snapshot_config["company_sheet"]
            deal_sheet_name = snapshot_config["deal_sheet"]
            
            self.logger.info(f"🔍 Processing snapshot: {snapshot_date}")
            
            snapshot_data = {}
            
            # Extract company data
            if company_sheet_name in all_sheets:
                company_df = self._clean_dataframe(all_sheets[company_sheet_name], company_sheet_name)
                if len(company_df) > 0:
                    snapshot_data['companies'] = company_df
                    self.logger.info(f"  ✅ Companies: {len(company_df)} records from '{company_sheet_name}'")
                else:
                    self.logger.warning(f"  ⚠️ Company sheet '{company_sheet_name}' has no valid data")
            else:
                self.logger.warning(f"  ❌ Company sheet '{company_sheet_name}' not found")
            
            # Extract deal data
            if deal_sheet_name in all_sheets:
                deal_df = self._clean_dataframe(all_sheets[deal_sheet_name], deal_sheet_name)
                if len(deal_df) > 0:
                    snapshot_data['deals'] = deal_df
                    self.logger.info(f"  ✅ Deals: {len(deal_df)} records from '{deal_sheet_name}'")
                else:
                    self.logger.warning(f"  ⚠️ Deal sheet '{deal_sheet_name}' has no valid data")
            else:
                self.logger.warning(f"  ❌ Deal sheet '{deal_sheet_name}' not found")
            
            # Only include snapshot if we have at least some data
            if snapshot_data:
                snapshots_data[snapshot_date] = snapshot_data
                self.logger.info(f"  📸 Snapshot {snapshot_date}: {len(snapshot_data)} data types")
            else:
                self.logger.warning(f"  ❌ Skipping snapshot {snapshot_date}: no valid data found")
        
        self.logger.info(f"✅ Extracted {len(snapshots_data)} snapshots with data")
        return snapshots_data
    
    def extract_hubspot_sheets(self) -> Dict[str, pd.DataFrame]:
        """
        Legacy method for backward compatibility
        Extract all HubSpot-related sheets (auto-detection mode)
        """
        self.logger.info(f"📂 Auto-detecting HubSpot sheets in: {self.file_path}")
        
        try:
            all_sheets = pd.read_excel(self.file_path, sheet_name=None, engine='openpyxl')
            self.logger.debug(f"Found {len(all_sheets)} total sheets")
            
        except Exception as e:
            raise RuntimeError(f"Failed to read Excel file: {e}")
        
        # Filter for HubSpot export sheets using auto-detection
        hubspot_sheets = {}
        
        for sheet_name, df in all_sheets.items():
            self.logger.debug(f"Examining sheet: {sheet_name} ({len(df)} rows, {len(df.columns)} cols)")
            
            if self._is_hubspot_export_sheet(sheet_name, df):
                cleaned_df = self._clean_dataframe(df, sheet_name)
                if len(cleaned_df) > 0:
                    hubspot_sheets[sheet_name] = cleaned_df
                    self.logger.info(f"✅ Auto-detected HubSpot sheet: {sheet_name} ({len(cleaned_df)} data rows)")
                else:
                    self.logger.warning(f"⚠️ Sheet {sheet_name} appears to be HubSpot data but has no valid rows")
        
        if not hubspot_sheets:
            self.logger.warning("⚠️ No HubSpot export sheets found with auto-detection")
            self._suggest_sheet_patterns(all_sheets)
        
        return hubspot_sheets
    
    def get_available_sheets(self) -> List[str]:
        """Get list of all sheet names in the Excel file"""
        try:
            all_sheets = pd.read_excel(self.file_path, sheet_name=None, engine='openpyxl')
            return list(all_sheets.keys())
        except Exception as e:
            self.logger.error(f"Failed to read sheet names: {e}")
            return []
    
    def validate_snapshot_sheets(self) -> Tuple[List[str], List[str]]:
        """
        Validate that all configured snapshot sheets exist in the Excel file
        
        Returns:
            Tuple of (found_sheets, missing_sheets)
        """
        available_sheets = self.get_available_sheets()
        
        expected_sheets = []
        for snapshot in SNAPSHOTS_TO_IMPORT:
            expected_sheets.extend([snapshot["company_sheet"], snapshot["deal_sheet"]])
        
        found_sheets = [sheet for sheet in expected_sheets if sheet in available_sheets]
        missing_sheets = [sheet for sheet in expected_sheets if sheet not in available_sheets]
        
        return found_sheets, missing_sheets
    
    def _is_hubspot_export_sheet(self, sheet_name: str, df: pd.DataFrame) -> bool:
        """Determine if sheet contains HubSpot export data (auto-detection)"""
        # Skip empty sheets
        if df.empty or len(df) < 2:
            return False
        
        name_lower = sheet_name.lower()
        
        # Check sheet name patterns
        hubspot_keywords = ['hubspot', 'crm-export', 'weekly-stat', 'weekly-status', 'company', 'deal']
        if any(keyword in name_lower for keyword in hubspot_keywords):
            self.logger.debug(f"Sheet {sheet_name} matches name pattern")
            return True
            
        # Check column patterns for company data
        if self._has_company_columns(df):
            self.logger.debug(f"Sheet {sheet_name} matches company column pattern")
            return True
            
        # Check column patterns for deal data  
        if self._has_deal_columns(df):
            self.logger.debug(f"Sheet {sheet_name} matches deal column pattern")
            return True
            
        return False
    
    def _has_company_columns(self, df: pd.DataFrame) -> bool:
        """Check if DataFrame has typical company export columns"""
        expected_cols = ['record id', 'company name', 'company owner', 'lifecycle stage']
        df_cols_lower = [str(col).lower() for col in df.columns]
        matches = sum(1 for col in expected_cols if col in df_cols_lower)
        return matches >= 3
    
    def _has_deal_columns(self, df: pd.DataFrame) -> bool:
        """Check if DataFrame has typical deal export columns"""
        deal_indicators = ['deal name', 'dealname', 'deal stage', 'dealstage', 'amount']
        df_cols_lower = [str(col).lower() for col in df.columns]
        matches = sum(1 for indicator in deal_indicators if any(indicator in col for col in df_cols_lower))
        return matches >= 2
    
    def _clean_dataframe(self, df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
        """Clean and prepare DataFrame for processing"""
        original_rows = len(df)
        
        # Remove completely empty rows
        df_cleaned = df.dropna(how='all')
        
        # Remove rows where the first column (usually Record ID) is empty
        if len(df_cleaned) > 0:
            first_col = df_cleaned.columns[0]
            df_cleaned = df_cleaned.dropna(subset=[first_col])
        
        # Reset index
        df_cleaned = df_cleaned.reset_index(drop=True)
        
        removed_rows = original_rows - len(df_cleaned)
        if removed_rows > 0:
            self.logger.debug(f"Cleaned {sheet_name}: removed {removed_rows} empty rows")
        
        return df_cleaned
    
    def _suggest_sheet_patterns(self, all_sheets: Dict[str, pd.DataFrame]):
        """Suggest which sheets might contain data based on analysis"""
        self.logger.info("💡 Analyzing sheets for potential HubSpot data:")
        
        for sheet_name, df in all_sheets.items():
            if df.empty or len(df) < 2:
                continue
                
            analysis = []
            
            # Check for ID columns
            if any('id' in str(col).lower() for col in df.columns):
                analysis.append("has ID columns")
            
            # Check for date columns
            if any('date' in str(col).lower() for col in df.columns):
                analysis.append("has date columns")
            
            # Check for company indicators
            company_indicators = ['company', 'owner', 'lifecycle', 'lead']
            if any(indicator in str(col).lower() for col in df.columns for indicator in company_indicators):
                analysis.append("company-like columns")
            
            # Check for deal indicators
            deal_indicators = ['deal', 'stage', 'amount']
            if any(indicator in str(col).lower() for col in df.columns for indicator in deal_indicators):
                analysis.append("deal-like columns")
            
            if analysis:
                self.logger.info(f"  📋 {sheet_name}: {', '.join(analysis)} ({len(df)} rows)")
            else:
                self.logger.debug(f"  📋 {sheet_name}: no obvious patterns ({len(df)} rows)")
        
        self.logger.info("Expected sheet name patterns:")
        self.logger.info("  • Company sheets: contain 'company' in name")
        self.logger.info("  • Deal sheets: contain 'deal' in name")
        self.logger.info("  • HubSpot exports: contain 'hubspot', 'crm', 'weekly-status'")

class SnapshotProcessor:
    """Process multiple snapshots from Excel file"""
    
    def __init__(self, excel_processor: ExcelProcessor):
        self.excel_processor = excel_processor
        self.logger = logging.getLogger('hubspot.excel_import')
    
    def process_all_snapshots(self) -> Dict[str, Dict[str, Any]]:
        """
        Process all configured snapshots and return summary
        
        Returns:
            Dictionary with processing summary for each snapshot
        """
        self.logger.info("🚀 Starting multi-snapshot processing")
        
        # Validate sheets exist first
        found_sheets, missing_sheets = self.excel_processor.validate_snapshot_sheets()
        
        if missing_sheets:
            self.logger.warning(f"⚠️ Missing {len(missing_sheets)} expected sheets:")
            for sheet in missing_sheets[:5]:  # Show first 5
                self.logger.warning(f"  • {sheet}")
            if len(missing_sheets) > 5:
                self.logger.warning(f"  • ... and {len(missing_sheets) - 5} more")
        
        if not found_sheets:
            raise RuntimeError("No expected snapshot sheets found in Excel file")
        
        self.logger.info(f"✅ Found {len(found_sheets)} expected sheets")
        
        # Extract all snapshots
        snapshots_data = self.excel_processor.extract_all_snapshots()
        
        if not snapshots_data:
            raise RuntimeError("No valid snapshot data extracted from Excel file")
        
        # Process summary
        summary = {}
        total_companies = 0
        total_deals = 0
        
        for snapshot_date, data in snapshots_data.items():
            companies_count = len(data.get('companies', []))
            deals_count = len(data.get('deals', []))
            
            summary[snapshot_date] = {
                'companies': companies_count,
                'deals': deals_count,
                'total': companies_count + deals_count
            }
            
            total_companies += companies_count
            total_deals += deals_count
        
        self.logger.info(f"📊 Processing summary:")
        self.logger.info(f"  • {len(snapshots_data)} snapshots")
        self.logger.info(f"  • {total_companies} total company records")
        self.logger.info(f"  • {total_deals} total deal records")
        self.logger.info(f"  • {total_companies + total_deals} total records")
        
        return {
            'snapshots': snapshots_data,
            'summary': summary,
            'totals': {
                'snapshots': len(snapshots_data),
                'companies': total_companies,
                'deals': total_deals,
                'total_records': total_companies + total_deals
            }
        }
    
    def process_all_snapshots_with_crm_metadata(self, crm_metadata: Dict[str, Dict]) -> Dict[str, Dict[str, Any]]:
        """
        Process all configured snapshots with CRM metadata and return summary
        
        Args:
            crm_metadata: Dictionary of snapshot_date -> CRM file metadata
        
        Returns:
            Dictionary with processing summary for each snapshot with CRM timestamps
        """
        self.logger.info("🚀 Starting multi-snapshot processing with CRM metadata")
        
        # Validate sheets exist first
        found_sheets, missing_sheets = self.excel_processor.validate_snapshot_sheets()
        
        if missing_sheets:
            self.logger.warning(f"⚠️ Missing {len(missing_sheets)} expected sheets:")
            for sheet in missing_sheets[:5]:  # Show first 5
                self.logger.warning(f"  • {sheet}")
            if len(missing_sheets) > 5:
                self.logger.warning(f"  • ... and {len(missing_sheets) - 5} more")
        
        if not found_sheets:
            raise RuntimeError("No expected snapshot sheets found in Excel file")
        
        self.logger.info(f"✅ Found {len(found_sheets)} expected sheets")
        
        # Extract all snapshots
        excel_snapshots_data = self.excel_processor.extract_all_snapshots()
        
        if not excel_snapshots_data:
            raise RuntimeError("No valid snapshot data extracted from Excel file")
        
        # Process snapshots with CRM metadata
        snapshots_data_with_crm = {}
        summary = {}
        total_companies = 0
        total_deals = 0
        matched_snapshots = 0
        
        for snapshot_date, excel_data in excel_snapshots_data.items():
            companies_count = len(excel_data.get('companies', []))
            deals_count = len(excel_data.get('deals', []))
            
            # Check if we have CRM metadata for this snapshot
            if snapshot_date in crm_metadata:
                crm_info = crm_metadata[snapshot_date]
                crm_snapshot_id = crm_info['snapshot_id']
                
                self.logger.info(f"📸 Snapshot {snapshot_date}: Using CRM timestamp {crm_snapshot_id}")
                
                # Use CRM timestamp as the key for snapshots_data
                snapshots_data_with_crm[crm_snapshot_id] = excel_data
                
                summary[snapshot_date] = {
                    'companies': companies_count,
                    'deals': deals_count,
                    'total': companies_count + deals_count,
                    'crm_snapshot_id': crm_snapshot_id,
                    'company_file': crm_info.get('company_file', ''),
                    'deals_file': crm_info.get('deals_file', ''),
                    'company_timestamp': crm_info.get('company_timestamp', ''),
                    'deals_timestamp': crm_info.get('deals_timestamp', '')
                }
                
                matched_snapshots += 1
            else:
                self.logger.warning(f"⚠️ No CRM metadata found for snapshot {snapshot_date}, skipping")
                continue
            
            total_companies += companies_count
            total_deals += deals_count
        
        self.logger.info(f"📊 Processing summary with CRM metadata:")
        self.logger.info(f"  • {len(excel_snapshots_data)} Excel snapshots")
        self.logger.info(f"  • {matched_snapshots} matched with CRM metadata")
        self.logger.info(f"  • {total_companies} total company records")
        self.logger.info(f"  • {total_deals} total deal records")
        self.logger.info(f"  • {total_companies + total_deals} total records")
        
        if matched_snapshots == 0:
            raise RuntimeError("No snapshots could be matched with CRM metadata")
        
        return {
            'snapshots': snapshots_data_with_crm,  # Now keyed by CRM timestamps
            'summary': summary,
            'totals': {
                'snapshots': matched_snapshots,
                'companies': total_companies,
                'deals': total_deals,
                'total_records': total_companies + total_deals
            }
        }