# src/hubspot_pipeline/hubspot_ingest/normalization.py

import logging
import re
from typing import Optional, Set
from urllib.parse import urlparse

def get_fields_requiring_normalization() -> dict:
    """
    Get all fields that require lowercase normalization by table.
    
    Returns:
        Dictionary mapping table names to sets of field names that need normalization
    """
    return {
        'hs_companies': {
            'lifecycle_stage', 'lead_status', 'company_type',
            'development_category', 'hiring_developers', 'inhouse_developers',
            'proff_likviditetsgrad', 'proff_lonnsomhet', 'proff_soliditet'
        },
        'hs_deals': {
            'deal_stage', 'deal_type'
        },
        'hs_owners': {
            'email'
        }
    }

def get_url_fields() -> dict:
    """
    Get fields that contain URLs and need domain normalization.
    
    Returns:
        Dictionary mapping table names to sets of URL field names
    """
    return {
        'hs_companies': {'proff_link'}
    }

def should_normalize_field(field_name: str, table_name: str = None) -> bool:
    """
    Determine if a field should be normalized to lowercase.
    
    Args:
        field_name: Name of the field
        table_name: Optional table name for more specific checking
        
    Returns:
        True if field should be normalized
    """
    normalization_fields = get_fields_requiring_normalization()
    
    if table_name and table_name in normalization_fields:
        return field_name in normalization_fields[table_name]
    
    # Check across all tables if no specific table provided
    for table_fields in normalization_fields.values():
        if field_name in table_fields:
            return True
    
    return False

def should_normalize_url(field_name: str, table_name: str = None) -> bool:
    """
    Determine if a field contains URLs that need domain normalization.
    
    Args:
        field_name: Name of the field
        table_name: Optional table name for more specific checking
        
    Returns:
        True if field contains URLs that should be normalized
    """
    url_fields = get_url_fields()
    
    if table_name and table_name in url_fields:
        return field_name in url_fields[table_name]
    
    # Check across all tables if no specific table provided
    for table_fields in url_fields.values():
        if field_name in table_fields:
            return True
    
    return False

def normalize_email(email: Optional[str]) -> Optional[str]:
    """
    Normalize email address to lowercase.
    
    Args:
        email: Email address to normalize
        
    Returns:
        Lowercase email address or None if input is None/empty
    """
    if not email or not isinstance(email, str):
        return email
    
    email = email.strip()
    if not email:
        return email
    
    # Basic email validation pattern
    if '@' not in email:
        # Log warning but don't fail - let validation happen elsewhere
        logging.getLogger('hubspot.normalization').debug(f"Invalid email format: {email}")
        return email.lower()
    
    return email.lower()

def normalize_enum_field(value: Optional[str]) -> Optional[str]:
    """
    Normalize enum/status field to lowercase.
    
    Args:
        value: Field value to normalize
        
    Returns:
        Lowercase field value or None if input is None/empty
    """
    if not value or not isinstance(value, str):
        return value
    
    value = value.strip()
    if not value:
        return value
    
    return value.lower()

def normalize_url(url: Optional[str]) -> Optional[str]:
    """
    Normalize URL by making the domain lowercase while preserving path case.
    
    Args:
        url: URL to normalize
        
    Returns:
        URL with lowercase domain or None if input is None/empty
    """
    if not url or not isinstance(url, str):
        return url
    
    url = url.strip()
    if not url:
        return url
    
    try:
        # Parse the URL
        parsed = urlparse(url)
        
        # If no scheme, assume it's a domain-only URL
        if not parsed.scheme and not parsed.netloc:
            # Treat the whole thing as a domain
            return url.lower()
        
        # Normalize the domain part (netloc) to lowercase
        normalized_netloc = parsed.netloc.lower()
        
        # Reconstruct the URL with lowercase domain
        normalized_url = parsed._replace(netloc=normalized_netloc).geturl()
        
        return normalized_url
        
    except Exception as e:
        # If URL parsing fails, log and return original
        logging.getLogger('hubspot.normalization').warning(f"Failed to normalize URL '{url}': {e}")
        return url

def normalize_field_value(field_name: str, value: Optional[str], table_name: str = None) -> Optional[str]:
    """
    Normalize a field value based on its type and name.
    
    Args:
        field_name: Name of the field
        value: Value to normalize
        table_name: Optional table name for context
        
    Returns:
        Normalized value
    """
    if value is None:
        return None
    
    # URL fields get special treatment
    if should_normalize_url(field_name, table_name):
        return normalize_url(value)
    
    # Email fields
    if field_name == 'email' or field_name.endswith('_email'):
        return normalize_email(value)
    
    # Enum/status fields
    if should_normalize_field(field_name, table_name):
        return normalize_enum_field(value)
    
    # No normalization needed
    return value

def validate_normalization(data: dict, table_name: str) -> list:
    """
    Validate that all fields requiring normalization are properly normalized.
    
    Args:
        data: Record data to validate
        table_name: Name of the table this data belongs to
        
    Returns:
        List of validation errors (empty if all good)
    """
    logger = logging.getLogger('hubspot.normalization')
    errors = []
    
    normalization_fields = get_fields_requiring_normalization().get(table_name, set())
    url_fields = get_url_fields().get(table_name, set())
    
    for field_name, value in data.items():
        if value is None or not isinstance(value, str):
            continue
        
        original_value = value
        
        # Check enum/status fields
        if field_name in normalization_fields:
            if value != value.lower():
                errors.append(f"Field '{field_name}' not normalized: '{value}' should be '{value.lower()}'")
        
        # Check URL fields
        if field_name in url_fields:
            normalized = normalize_url(value)
            if normalized != value:
                errors.append(f"URL field '{field_name}' not normalized: domain should be lowercase")
        
        # Check email fields
        if field_name == 'email' or field_name.endswith('_email'):
            if '@' in value and value != value.lower():
                errors.append(f"Email field '{field_name}' not normalized: '{value}' should be '{value.lower()}'")
    
    if errors and logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"Normalization validation errors for {table_name}: {errors}")
    
    return errors