#!/usr/bin/env python3
"""
Data models for integrity testing
"""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional

@dataclass
class IntegrityIssue:
    """Represents a data integrity issue"""
    table: str
    field: str
    issue_type: str
    count: int
    severity: str  # 'critical', 'warning', 'info'
    description: str
    sample_values: Optional[List[str]] = None
    
    def __post_init__(self):
        """Ensure sample_values is never None"""
        if self.sample_values is None:
            self.sample_values = []

@dataclass
class IntegrityReport:
    """Complete integrity verification report"""
    timestamp: str
    environment: str
    dataset: str
    total_tables_checked: int
    total_issues_found: int
    critical_issues: int
    warning_issues: int
    info_issues: int
    issues: List[IntegrityIssue]
    summary: Dict[str, Any]
    
    @property
    def integrity_score(self) -> float:
        """Get integrity score from summary"""
        return self.summary.get('integrity_score', 0.0)
    
    @property
    def has_critical_issues(self) -> bool:
        """Check if report has critical issues"""
        return self.critical_issues > 0
    
    @property
    def total_records(self) -> int:
        """Get total records from summary"""
        return self.summary.get('total_records', 0)
    
    def get_issues_by_severity(self, severity: str) -> List[IntegrityIssue]:
        """Get issues filtered by severity"""
        return [issue for issue in self.issues if issue.severity == severity]
    
    def get_issues_by_table(self, table: str) -> List[IntegrityIssue]:
        """Get issues filtered by table"""
        return [issue for issue in self.issues if issue.table == table]