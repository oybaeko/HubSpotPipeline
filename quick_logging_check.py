#!/usr/bin/env python3
"""
Quick Logging Check Script
Fast analysis of logging issues without making changes

Usage:
    python quick_logging_check.py [src/]
"""

import os
import re
from pathlib import Path
import argparse

def analyze_file_quick(file_path: Path) -> dict:
    """Quick analysis of a Python file for logging issues"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        lines = content.split('\n')
        
        # Check for imports
        has_logging_import = 'import logging' in content or 'from logging' in content
        has_logger = any('logger' in line.lower() and '=' in line for line in lines)
        
        # Find print statements
        print_lines = []
        problematic_prints = []
        
        for i, line in enumerate(lines, 1):
            if 'print(' in line:
                print_lines.append({'line': i, 'content': line.strip()})
                
                # Quick check if it's problematic
                if any(word in line.lower() for word in [
                    'test', 'check', 'start', 'end', 'error', 'success', 'fail',
                    '✅', '❌', '🧪', '🔍', '📋', '🚀', '⚠️'
                ]) and not any(pattern in line.lower() for pattern in [
                    'json.dumps', 'response', 'local', 'debug'
                ]):
                    problematic_prints.append({'line': i, 'content': line.strip()})
        
        return {
            'file': file_path,
            'has_logging_import': has_logging_import,
            'has_logger': has_logger,
            'total_prints': len(print_lines),
            'problematic_prints': problematic_prints,
            'all_prints': print_lines
        }
        
    except Exception as e:
        return {'file': file_path, 'error': str(e)}

def main():
    parser = argparse.ArgumentParser(description='Quick logging analysis')
    parser.add_argument('path', nargs='?', default='src/', help='Path to analyze')
    parser.add_argument('--show-all-prints', action='store_true', help='Show all print statements')
    
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
    
    print("🔍 QUICK LOGGING CHECK")
    print("=" * 40)
    print(f"📁 Path: {path}")
    print(f"📄 Files: {len(python_files)}")
    print()
    
    total_issues = 0
    total_problematic_prints = 0
    
    for file_path in python_files:
        result = analyze_file_quick(file_path)
        
        if 'error' in result:
            print(f"❌ {file_path}: {result['error']}")
            continue
        
        issues = []
        if not result['has_logging_import'] and result['problematic_prints']:
            issues.append("No logging import")
        if not result['has_logger'] and result['problematic_prints']:
            issues.append("No logger definition")
        if result['problematic_prints']:
            issues.append(f"{len(result['problematic_prints'])} problematic prints")
        
        if issues:
            total_issues += 1
            total_problematic_prints += len(result['problematic_prints'])
            
            # Handle relative path safely
            try:
                display_path = file_path.relative_to(Path.cwd())
            except ValueError:
                display_path = file_path
            print(f"⚠️  {display_path}")
            for issue in issues:
                print(f"   • {issue}")
            
            if args.show_all_prints or len(result['problematic_prints']) <= 5:
                for p in result['problematic_prints']:
                    print(f"   Line {p['line']}: {p['content'][:70]}...")
            elif len(result['problematic_prints']) > 5:
                for p in result['problematic_prints'][:3]:
                    print(f"   Line {p['line']}: {p['content'][:70]}...")
                print(f"   ... and {len(result['problematic_prints']) - 3} more")
            
            print()
    
    print("=" * 40)
    print("📊 SUMMARY:")
    print(f"Files with issues: {total_issues}")
    print(f"Problematic prints: {total_problematic_prints}")
    
    if total_issues > 0:
        print(f"\n💡 Run the full fix script:")
        print(f"   python fix_logging.py --dry-run {args.path}")
    else:
        print("🎉 No logging issues found!")
    
    return 0

if __name__ == "__main__":
    exit(main())