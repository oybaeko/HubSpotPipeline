#!/usr/bin/env python3
"""
Logging Configuration Overview Script
Provides a comprehensive overview of all logging configurations in your codebase

Usage:
    python logging_overview.py [src/]
    python logging_overview.py --detailed [src/]
    python logging_overview.py --export overview.json [src/]
"""

import os
import re
import ast
import json
from pathlib import Path
import argparse
from typing import List, Dict, Optional, Set
from collections import defaultdict

class LoggingConfigurationAnalyzer:
    """Analyzes logging configurations across the codebase"""
    
    def __init__(self):
        self.logger_names = set()
        self.logging_imports = {}
        self.logger_definitions = {}
        self.logging_calls = defaultdict(list)
        self.basicconfig_calls = []
        self.print_statements = defaultdict(list)
        
    def analyze_file(self, file_path: Path) -> Dict:
        """Analyze logging configuration in a single file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Parse the AST
            tree = ast.parse(content)
            lines = content.split('\n')
            
            file_analysis = {
                'file': str(file_path),
                'imports': [],
                'logger_definitions': [],
                'basicconfig_calls': [],
                'logging_calls': [],
                'print_statements': [],
                'log_levels_used': set(),
                'logger_names_used': set()
            }
            
            # Analyze AST nodes
            for node in ast.walk(tree):
                self._analyze_node(node, lines, file_analysis)
            
            # Convert sets to lists for JSON serialization
            file_analysis['log_levels_used'] = list(file_analysis['log_levels_used'])
            file_analysis['logger_names_used'] = list(file_analysis['logger_names_used'])
            
            return file_analysis
            
        except Exception as e:
            return {
                'file': str(file_path),
                'error': str(e),
                'imports': [],
                'logger_definitions': [],
                'basicconfig_calls': [],
                'logging_calls': [],
                'print_statements': []
            }
    
    def _analyze_node(self, node: ast.AST, lines: List[str], file_analysis: Dict):
        """Analyze a single AST node for logging-related patterns"""
        
        # Import statements
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == 'logging':
                    import_info = {
                        'line': node.lineno,
                        'type': 'import',
                        'module': 'logging',
                        'alias': alias.asname,
                        'code': self._get_line(lines, node.lineno)
                    }
                    file_analysis['imports'].append(import_info)
        
        elif isinstance(node, ast.ImportFrom):
            if node.module == 'logging':
                import_info = {
                    'line': node.lineno,
                    'type': 'from_import',
                    'module': 'logging',
                    'names': [alias.name for alias in node.names],
                    'code': self._get_line(lines, node.lineno)
                }
                file_analysis['imports'].append(import_info)
        
        # Logger definitions
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and 'logger' in target.id.lower():
                    logger_def = {
                        'line': node.lineno,
                        'variable_name': target.id,
                        'code': self._get_line(lines, node.lineno)
                    }
                    
                    # Try to extract logger name from getLogger() call
                    if isinstance(node.value, ast.Call):
                        logger_name = self._extract_logger_name(node.value)
                        if logger_name:
                            logger_def['logger_name'] = logger_name
                            file_analysis['logger_names_used'].add(logger_name)
                    
                    file_analysis['logger_definitions'].append(logger_def)
        
        # Function calls
        elif isinstance(node, ast.Call):
            # logging.basicConfig() calls
            if self._is_basicconfig_call(node):
                basicconfig_info = {
                    'line': node.lineno,
                    'code': self._get_line(lines, node.lineno),
                    'parameters': self._extract_basicconfig_params(node)
                }
                file_analysis['basicconfig_calls'].append(basicconfig_info)
            
            # Logger method calls (logger.info, logger.error, etc.)
            elif self._is_logger_call(node):
                log_call_info = {
                    'line': node.lineno,
                    'level': self._get_log_level(node),
                    'logger_variable': self._get_logger_variable(node),
                    'message_preview': self._get_message_preview(node, lines),
                    'code': self._get_line(lines, node.lineno)
                }
                
                if log_call_info['level']:
                    file_analysis['log_levels_used'].add(log_call_info['level'])
                
                file_analysis['logging_calls'].append(log_call_info)
            
            # Print statements
            elif isinstance(node.func, ast.Name) and node.func.id == 'print':
                print_info = {
                    'line': node.lineno,
                    'code': self._get_line(lines, node.lineno),
                    'is_problematic': self._is_problematic_print(lines, node.lineno),
                    'category': self._categorize_print(lines, node.lineno)
                }
                file_analysis['print_statements'].append(print_info)
    
    def _get_line(self, lines: List[str], line_no: int) -> str:
        """Get a line from the source code"""
        if 1 <= line_no <= len(lines):
            return lines[line_no - 1].strip()
        return ""
    
    def _extract_logger_name(self, call_node: ast.Call) -> Optional[str]:
        """Extract logger name from getLogger() call"""
        if (isinstance(call_node.func, ast.Attribute) and 
            call_node.func.attr == 'getLogger' and
            len(call_node.args) > 0):
            
            arg = call_node.args[0]
            if isinstance(arg, ast.Constant):
                return arg.value
            elif isinstance(arg, ast.Str):  # Python < 3.8 compatibility
                return arg.s
            elif isinstance(arg, ast.Name) and arg.id == '__name__':
                return '__name__'
        
        return None
    
    def _is_basicconfig_call(self, node: ast.Call) -> bool:
        """Check if this is a logging.basicConfig() call"""
        return (isinstance(node.func, ast.Attribute) and 
                node.func.attr == 'basicConfig' and
                isinstance(node.func.value, ast.Name) and
                node.func.value.id == 'logging')
    
    def _extract_basicconfig_params(self, node: ast.Call) -> Dict:
        """Extract parameters from basicConfig() call"""
        params = {}
        
        for keyword in node.keywords:
            if keyword.arg:
                if isinstance(keyword.value, ast.Constant):
                    params[keyword.arg] = keyword.value.value
                elif isinstance(keyword.value, ast.Str):
                    params[keyword.arg] = keyword.value.s
                elif isinstance(keyword.value, ast.Attribute):
                    # Handle things like sys.stderr
                    params[keyword.arg] = f"{keyword.value.value.id}.{keyword.value.attr}"
                else:
                    params[keyword.arg] = f"<{type(keyword.value).__name__}>"
        
        return params
    
    def _is_logger_call(self, node: ast.Call) -> bool:
        """Check if this is a logger method call"""
        if isinstance(node.func, ast.Attribute):
            method_name = node.func.attr
            return method_name in ['debug', 'info', 'warning', 'error', 'critical', 'exception']
        return False
    
    def _get_log_level(self, node: ast.Call) -> Optional[str]:
        """Get the log level from a logger call"""
        if isinstance(node.func, ast.Attribute):
            return node.func.attr
        return None
    
    def _get_logger_variable(self, node: ast.Call) -> Optional[str]:
        """Get the logger variable name from a logger call"""
        if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name):
            return node.func.value.id
        return None
    
    def _get_message_preview(self, node: ast.Call, lines: List[str]) -> str:
        """Get a preview of the log message"""
        if node.args:
            line = self._get_line(lines, node.lineno)
            # Extract content within the parentheses
            match = re.search(r'\((.*)\)', line)
            if match:
                content = match.group(1)
                # Truncate if too long
                if len(content) > 60:
                    return content[:57] + "..."
                return content
        return ""
    
    def _is_problematic_print(self, lines: List[str], line_no: int) -> bool:
        """Check if a print statement should probably be logging"""
        line = self._get_line(lines, line_no)
        
        # Patterns that suggest it should be logging
        problematic_patterns = [
            r'["\'].*[Ss]tarting.*["\']',
            r'["\'].*[Cc]hecking.*["\']',
            r'["\'].*[Tt]esting.*["\']',
            r'["\'].*[Ee]rror.*["\']',
            r'["\'].*✅.*["\']',
            r'["\'].*❌.*["\']',
            r'["\'].*🧪.*["\']',
        ]
        
        # Patterns that suggest it should stay as print
        valid_patterns = [
            r'json\.dumps',
            r'response',
            r'local.*development',
            r'debug.*mode'
        ]
        
        # Check if it should stay as print
        for pattern in valid_patterns:
            if re.search(pattern, line, re.IGNORECASE):
                return False
        
        # Check if it should be logging
        for pattern in problematic_patterns:
            if re.search(pattern, line, re.IGNORECASE):
                return True
        
        return False
    
    def _categorize_print(self, lines: List[str], line_no: int) -> str:
        """Categorize the type of print statement"""
        line = self._get_line(lines, line_no)
        
        if 'json.dumps' in line or 'jsonify' in line:
            return 'http_response'
        elif any(word in line.lower() for word in ['local', 'development', 'debug']):
            return 'local_debug'
        elif any(word in line.lower() for word in ['test', 'check', 'start', 'error', 'success']):
            return 'should_be_logging'
        elif re.search(r'["\'].*[✅❌🧪🔍📋🚀⚠️].*["\']', line):
            return 'should_be_logging'
        else:
            return 'data_output'

def generate_overview_report(files_analysis: List[Dict], detailed: bool = False) -> str:
    """Generate a comprehensive overview report"""
    
    # Aggregate statistics
    total_files = len([f for f in files_analysis if 'error' not in f])
    files_with_logging_imports = 0
    files_with_logger_definitions = 0
    files_with_basicconfig = 0
    files_with_logging_calls = 0
    files_with_problematic_prints = 0
    
    all_logger_names = set()
    all_log_levels = set()
    all_basicconfig_params = set()
    total_logging_calls = 0
    total_print_statements = 0
    total_problematic_prints = 0
    
    print_categories = defaultdict(int)
    
    for file_analysis in files_analysis:
        if 'error' in file_analysis:
            continue
            
        if file_analysis['imports']:
            files_with_logging_imports += 1
        if file_analysis['logger_definitions']:
            files_with_logger_definitions += 1
        if file_analysis['basicconfig_calls']:
            files_with_basicconfig += 1
        if file_analysis['logging_calls']:
            files_with_logging_calls += 1
        
        # Collect logger names
        all_logger_names.update(file_analysis['logger_names_used'])
        
        # Collect log levels
        all_log_levels.update(file_analysis['log_levels_used'])
        
        # Count logging calls
        total_logging_calls += len(file_analysis['logging_calls'])
        
        # Analyze prints
        total_print_statements += len(file_analysis['print_statements'])
        problematic_in_file = sum(1 for p in file_analysis['print_statements'] if p['is_problematic'])
        total_problematic_prints += problematic_in_file
        
        if problematic_in_file > 0:
            files_with_problematic_prints += 1
        
        # Categorize prints
        for print_stmt in file_analysis['print_statements']:
            print_categories[print_stmt['category']] += 1
        
        # Collect basicConfig parameters
        for bc in file_analysis['basicconfig_calls']:
            all_basicconfig_params.update(bc['parameters'].keys())
    
    # Generate report
    report = []
    report.append("🔍 LOGGING CONFIGURATION OVERVIEW")
    report.append("=" * 60)
    report.append("")
    
    # Summary statistics
    report.append("📊 SUMMARY STATISTICS:")
    report.append(f"   📄 Total Python files analyzed: {total_files}")
    report.append(f"   📦 Files with logging imports: {files_with_logging_imports}")
    report.append(f"   🔧 Files with logger definitions: {files_with_logger_definitions}")
    report.append(f"   ⚙️  Files with basicConfig calls: {files_with_basicconfig}")
    report.append(f"   📝 Files with logging calls: {files_with_logging_calls}")
    report.append(f"   🖨️  Files with problematic prints: {files_with_problematic_prints}")
    report.append("")
    
    # Logger names overview
    if all_logger_names:
        report.append("📛 LOGGER NAMES USED:")
        for logger_name in sorted(all_logger_names):
            report.append(f"   • {logger_name}")
        report.append("")
    
    # Log levels overview
    if all_log_levels:
        report.append("📊 LOG LEVELS USED:")
        level_order = ['debug', 'info', 'warning', 'error', 'critical', 'exception']
        for level in level_order:
            if level in all_log_levels:
                report.append(f"   • {level.upper()}")
        report.append("")
    
    # Print statement analysis
    report.append("🖨️  PRINT STATEMENT ANALYSIS:")
    report.append(f"   📈 Total print statements: {total_print_statements}")
    report.append(f"   ⚠️  Problematic prints (should be logging): {total_problematic_prints}")
    report.append("   📋 Print categories:")
    for category, count in sorted(print_categories.items()):
        category_names = {
            'http_response': 'HTTP Responses (keep as print)',
            'local_debug': 'Local Debug (keep as print)',
            'should_be_logging': 'Should be Logging (fix needed)',
            'data_output': 'Data Output (review needed)'
        }
        display_name = category_names.get(category, category)
        report.append(f"     • {display_name}: {count}")
    report.append("")
    
    # BasicConfig parameters
    if all_basicconfig_params:
        report.append("⚙️  BASICCONFIG PARAMETERS USED:")
        for param in sorted(all_basicconfig_params):
            report.append(f"   • {param}")
        report.append("")
    
    # Detailed file analysis
    if detailed:
        report.append("📄 DETAILED FILE ANALYSIS:")
        report.append("-" * 60)
        
        for file_analysis in files_analysis:
            if 'error' in file_analysis:
                report.append(f"❌ {file_analysis['file']}: ERROR - {file_analysis['error']}")
                continue
            
            try:
                file_path = Path(file_analysis['file'])
                display_path = file_path.relative_to(Path.cwd())
            except (ValueError, OSError):
                display_path = Path(file_analysis['file'])
            
            report.append(f"\n📄 {display_path}")
            
            # Imports
            if file_analysis['imports']:
                report.append("   📦 Imports:")
                for imp in file_analysis['imports']:
                    if imp['type'] == 'import':
                        alias_text = f" as {imp['alias']}" if imp['alias'] else ""
                        report.append(f"     Line {imp['line']}: import {imp['module']}{alias_text}")
                    else:
                        names = ', '.join(imp['names'])
                        report.append(f"     Line {imp['line']}: from {imp['module']} import {names}")
            
            # Logger definitions
            if file_analysis['logger_definitions']:
                report.append("   🔧 Logger definitions:")
                for logger_def in file_analysis['logger_definitions']:
                    logger_name = logger_def.get('logger_name', 'unknown')
                    report.append(f"     Line {logger_def['line']}: {logger_def['variable_name']} → '{logger_name}'")
            
            # BasicConfig calls
            if file_analysis['basicconfig_calls']:
                report.append("   ⚙️  BasicConfig calls:")
                for bc in file_analysis['basicconfig_calls']:
                    params = ', '.join(f"{k}={v}" for k, v in bc['parameters'].items())
                    report.append(f"     Line {bc['line']}: basicConfig({params})")
            
            # Logging calls summary
            if file_analysis['logging_calls']:
                level_counts = defaultdict(int)
                for call in file_analysis['logging_calls']:
                    level_counts[call['level']] += 1
                
                report.append("   📝 Logging calls:")
                for level, count in sorted(level_counts.items()):
                    report.append(f"     • {level}: {count} calls")
            
            # Print statements
            if file_analysis['print_statements']:
                problematic = [p for p in file_analysis['print_statements'] if p['is_problematic']]
                valid = [p for p in file_analysis['print_statements'] if not p['is_problematic']]
                
                if problematic:
                    report.append(f"   🖨️  Print statements - {len(problematic)} problematic:")
                    for print_stmt in problematic[:3]:  # Show first 3
                        preview = print_stmt['code'][:50] + "..." if len(print_stmt['code']) > 50 else print_stmt['code']
                        report.append(f"     Line {print_stmt['line']}: {preview}")
                    if len(problematic) > 3:
                        report.append(f"     ... and {len(problematic) - 3} more")
                
                if valid:
                    report.append(f"   ✅ Valid prints: {len(valid)}")
    
    # Recommendations
    report.append("")
    report.append("💡 RECOMMENDATIONS:")
    
    if total_problematic_prints > 0:
        report.append(f"   1. Fix {total_problematic_prints} problematic print statements")
        report.append("      Run: python fix_logging.py --dry-run")
    
    if files_with_logging_imports < files_with_problematic_prints:
        report.append("   2. Add logging imports to files that need them")
    
    if files_with_logger_definitions < files_with_problematic_prints:
        report.append("   3. Add logger definitions to files using logging")
    
    if len(all_logger_names) > 5:
        report.append("   4. Consider standardizing logger names for consistency")
    
    if files_with_basicconfig > 1:
        report.append("   5. Consider centralizing logging configuration")
    
    if total_problematic_prints == 0 and files_with_logging_calls > 0:
        report.append("   🎉 Your logging configuration looks good!")
    
    return "\n".join(report)

def main():
    parser = argparse.ArgumentParser(description='Analyze logging configurations across codebase')
    parser.add_argument('path', nargs='?', default='src/', help='Path to analyze')
    parser.add_argument('--detailed', '-d', action='store_true', help='Show detailed file-by-file analysis')
    parser.add_argument('--export', '-e', type=str, help='Export detailed data to JSON file')
    parser.add_argument('--filter', choices=['issues', 'good', 'all'], default='all', 
                       help='Filter files: issues=files with problems, good=files without issues, all=all files')
    
    args = parser.parse_args()
    
    path = Path(args.path)
    if not path.exists():
        print(f"❌ Path {path} does not exist")
        return 1
    
    # Find Python files
    if path.is_file():
        python_files = [path]
    else:
        python_files = list(path.rglob('*.py'))
        # Filter out hidden directories and common exclusions
        python_files = [f for f in python_files if not any(part.startswith('.') for part in f.parts)]
        python_files = [f for f in python_files if '__pycache__' not in str(f)]
    
    if not python_files:
        print("❌ No Python files found")
        return 1
    
    print(f"🔍 Analyzing {len(python_files)} Python files...")
    
    # Analyze files
    analyzer = LoggingConfigurationAnalyzer()
    files_analysis = []
    
    for file_path in python_files:
        analysis = analyzer.analyze_file(file_path)
        files_analysis.append(analysis)
    
    # Filter results if requested
    if args.filter == 'issues':
        files_analysis = [f for f in files_analysis if 
                         (len(f.get('print_statements', [])) > 0 and 
                          any(p['is_problematic'] for p in f['print_statements'])) or
                         'error' in f]
    elif args.filter == 'good':
        files_analysis = [f for f in files_analysis if 
                         'error' not in f and
                         not any(p['is_problematic'] for p in f.get('print_statements', []))]
    
    # Generate and display report
    report = generate_overview_report(files_analysis, detailed=args.detailed)
    print(report)
    
    # Export to JSON if requested
    if args.export:
        export_data = {
            'analysis_timestamp': str(Path.cwd()),
            'total_files_analyzed': len(python_files),
            'files_analysis': files_analysis
        }
        
        with open(args.export, 'w') as f:
            json.dump(export_data, f, indent=2)
        
        print(f"\n💾 Detailed analysis exported to: {args.export}")
    
    return 0

if __name__ == "__main__":
    exit(main())