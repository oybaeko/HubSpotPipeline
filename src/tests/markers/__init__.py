# ===============================================================================
# Updated markers/__init__.py - Simplified markers
# ===============================================================================

# NEW SIMPLIFIED markers/__init__.py content:
"""
# Custom pytest markers for two-tier HubSpot pipeline testing

# Tier 1: Environment-specific deployment validation
DEPLOYMENT_MARKER = "deployment"

# Tier 2: Basic runtime mechanism validation  
RUNTIME_MARKER = "runtime"

# Safety markers
PRODUCTION_SAFE_MARKER = "production_safe"
PRODUCTION_ONLY_MARKER = "production_only"

__all__ = [
    'DEPLOYMENT_MARKER',
    'RUNTIME_MARKER',
    'PRODUCTION_SAFE_MARKER', 
    'PRODUCTION_ONLY_MARKER'
]
"""