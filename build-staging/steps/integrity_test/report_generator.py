#!/usr/bin/env python3
"""
Report generation for integrity testing - UPDATED with case normalization
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any

from models import IntegrityIssue, IntegrityReport

class ReportGenerator:
    """Generates and formats integrity reports"""
    
    def __init__(self):
        self.logger = logging.getLogger('report_generator')
    
    def generate_integrity_report(self, all_issues: List[IntegrityIssue], table_counts: Dict[str, int], 
                                 environment: str, dataset: str) -> IntegrityReport:
        """Generate comprehensive integrity report"""
        self.logger.info("📋 Generating integrity report...")
        
        # Count issues by severity
        critical_count = sum(1 for issue in all_issues if issue.severity == 'critical')
        warning_count = sum(1 for issue in all_issues if issue.severity == 'warning')
        info_count = sum(1 for issue in all_issues if issue.severity == 'info')
        
        # Generate summary statistics
        total_records = sum(table_counts.values())
        issue_by_table = {}
        issue_by_type = {}
        
        for issue in all_issues:
            # Count by table
            if issue.table not in issue_by_table:
                issue_by_table[issue.table] = 0
            issue_by_table[issue.table] += 1
            
            # Count by type
            if issue.issue_type not in issue_by_type:
                issue_by_type[issue.issue_type] = 0
            issue_by_type[issue.issue_type] += 1
        
        # Create summary
        summary = {
            'total_records': total_records,
            'table_counts': table_counts,
            'issues_by_table': issue_by_table,
            'issues_by_type': issue_by_type,
            'integrity_score': self._calculate_integrity_score(all_issues, total_records),
            'recommendations': self._generate_recommendations(all_issues)
        }
        
        # Create report
        report = IntegrityReport(
            timestamp=datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            environment=environment,
            dataset=dataset,
            total_tables_checked=len(table_counts),
            total_issues_found=len(all_issues),
            critical_issues=critical_count,
            warning_issues=warning_count,
            info_issues=info_count,
            issues=all_issues,
            summary=summary
        )
        
        return report
    
    def _calculate_integrity_score(self, issues: List[IntegrityIssue], total_records: int) -> float:
        """Calculate overall data integrity score (0-100)"""
        if total_records == 0:
            return 0.0
        
        # Weight issues by severity
        penalty_weights = {'critical': 3, 'warning': 1, 'info': 0.1}
        
        total_penalty = 0
        for issue in issues:
            weight = penalty_weights.get(issue.severity, 1)
            # Penalty is proportional to the number of affected records
            penalty = (issue.count / total_records) * weight * 100
            total_penalty += penalty
        
        # Score is 100 minus total penalty, minimum 0
        score = max(0, 100 - total_penalty)
        return round(score, 2)
    
    def _generate_recommendations(self, issues: List[IntegrityIssue]) -> List[str]:
        """Generate recommendations based on found issues"""
        recommendations = []
        
        issue_types = {issue.issue_type for issue in issues}
        
        if 'blank_reference_field' in issue_types:
            recommendations.append("Implement normalization to convert empty strings to NULL in reference fields")
        
        if 'orphaned_reference' in issue_types:
            recommendations.append("Fix referential integrity by either adding missing records or removing orphaned references")
        
        if 'missing_required_field' in issue_types:
            recommendations.append("Address missing required field values - investigate data source quality")
        
        if 'duplicate_records' in issue_types:
            recommendations.append("Remove duplicate records and implement unique constraints")
        
        if 'invalid_format' in issue_types:
            recommendations.append("Implement data validation at ingestion time to ensure proper formats")
        
        if 'case_normalization' in issue_types:
            recommendations.append("Implement case normalization at ingestion time for consistent filtering and enum handling")
        
        if 'anomalous_distribution' in issue_types:
            recommendations.append("Investigate snapshots with unusual data distribution - possible data quality issues")
        
        if not recommendations:
            recommendations.append("Data integrity is good - continue monitoring with regular checks")
        
        return recommendations
    
    def print_integrity_report(self, report: IntegrityReport):
        """Print comprehensive integrity report to console"""
        print(f"\n{'='*80}")
        print(f"📋 DATA INTEGRITY VERIFICATION REPORT")
        print(f"{'='*80}")
        print(f"🕐 Timestamp: {report.timestamp}")
        print(f"🌍 Environment: {report.environment}")
        print(f"📂 Dataset: {report.dataset}")
        print(f"📊 Tables Checked: {report.total_tables_checked}")
        print(f"🎯 Integrity Score: {report.summary['integrity_score']}/100")
        print(f"{'='*80}")
        
        # Issue summary
        print(f"\n🔍 ISSUE SUMMARY")
        print(f"{'─'*40}")
        print(f"🔴 Critical Issues: {report.critical_issues}")
        print(f"🟡 Warning Issues: {report.warning_issues}")
        print(f"🔵 Info Issues: {report.info_issues}")
        print(f"📊 Total Issues: {report.total_issues_found}")
        
        # Table counts
        print(f"\n📋 TABLE RECORD COUNTS")
        print(f"{'─'*40}")
        for table, count in report.summary['table_counts'].items():
            print(f"  {table:<30} {count:>10,} records")
        print(f"  {'TOTAL':<30} {report.summary['total_records']:>10,} records")
        
        # Issues by table
        if report.summary['issues_by_table']:
            print(f"\n⚠️  ISSUES BY TABLE")
            print(f"{'─'*40}")
            for table, count in report.summary['issues_by_table'].items():
                print(f"  {table:<30} {count:>10} issues")
        
        # Issues by type
        if report.summary['issues_by_type']:
            print(f"\n🔍 ISSUES BY TYPE")
            print(f"{'─'*40}")
            for issue_type, count in report.summary['issues_by_type'].items():
                print(f"  {issue_type:<30} {count:>10} issues")
        
        # Detailed issues
        if report.issues:
            print(f"\n📝 DETAILED ISSUES")
            print(f"{'─'*80}")
            
            # Group by severity
            critical_issues = [i for i in report.issues if i.severity == 'critical']
            warning_issues = [i for i in report.issues if i.severity == 'warning']
            info_issues = [i for i in report.issues if i.severity == 'info']
            
            for severity, issues in [('CRITICAL', critical_issues), ('WARNING', warning_issues), ('INFO', info_issues)]:
                if issues:
                    severity_colors = {'CRITICAL': '🔴', 'WARNING': '🟡', 'INFO': '🔵'}
                    print(f"\n{severity_colors[severity]} {severity} ISSUES:")
                    
                    for i, issue in enumerate(issues, 1):
                        print(f"\n  {i}. {issue.table}.{issue.field}")
                        print(f"     Type: {issue.issue_type}")
                        print(f"     Count: {issue.count:,}")
                        print(f"     Description: {issue.description}")
                        if issue.sample_values:
                            sample_str = ', '.join(str(v) for v in issue.sample_values[:3])
                            if len(issue.sample_values) > 3:
                                sample_str += f" ... (+{len(issue.sample_values)-3} more)"
                            print(f"     Samples: {sample_str}")
        
        # Recommendations
        if report.summary['recommendations']:
            print(f"\n💡 RECOMMENDATIONS")
            print(f"{'─'*80}")
            for i, rec in enumerate(report.summary['recommendations'], 1):
                print(f"  {i}. {rec}")
        
        # Final assessment
        print(f"\n🎯 FINAL ASSESSMENT")
        print(f"{'─'*40}")
        
        if report.critical_issues == 0 and report.warning_issues == 0:
            print(f"✅ Excellent data integrity - no issues found")
        elif report.critical_issues == 0:
            print(f"🟡 Good data integrity - only minor warnings")
        elif report.critical_issues <= 3:
            print(f"🟠 Moderate data integrity issues - requires attention")
        else:
            print(f"🔴 Significant data integrity issues - immediate action required")
        
        print(f"{'='*80}")
    
    def save_integrity_report(self, report: IntegrityReport, format: str = 'json', output_dir: str = None):
        """Save integrity report to file"""
        try:
            # Create reports directory
            if output_dir:
                reports_dir = Path(output_dir)
            else:
                reports_dir = Path(__file__).parent / "reports"
            
            reports_dir.mkdir(exist_ok=True)
            
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            if format == 'json':
                report_file = reports_dir / f"integrity_report_{timestamp_str}.json"
                
                # Convert report to JSON-serializable format
                report_data = {
                    'timestamp': report.timestamp,
                    'environment': report.environment,
                    'dataset': report.dataset,
                    'total_tables_checked': report.total_tables_checked,
                    'total_issues_found': report.total_issues_found,
                    'critical_issues': report.critical_issues,
                    'warning_issues': report.warning_issues,
                    'info_issues': report.info_issues,
                    'summary': report.summary,
                    'issues': [
                        {
                            'table': issue.table,
                            'field': issue.field,
                            'issue_type': issue.issue_type,
                            'count': issue.count,
                            'severity': issue.severity,
                            'description': issue.description,
                            'sample_values': issue.sample_values or []
                        }
                        for issue in report.issues
                    ]
                }
                
                with open(report_file, 'w') as f:
                    json.dump(report_data, f, indent=2, default=str)
                
                self.logger.info(f"📄 Report saved: {report_file}")
                return str(report_file)
            
            elif format == 'text':
                report_file = reports_dir / f"integrity_report_{timestamp_str}.txt"
                
                # Capture console output to file
                import io
                import sys
                
                old_stdout = sys.stdout
                sys.stdout = buffer = io.StringIO()
                
                try:
                    self.print_integrity_report(report)
                    content = buffer.getvalue()
                finally:
                    sys.stdout = old_stdout
                
                with open(report_file, 'w') as f:
                    f.write(content)
                
                self.logger.info(f"📄 Report saved: {report_file}")
                return str(report_file)
            
        except Exception as e:
            self.logger.warning(f"⚠️ Could not save report: {e}")
            return None