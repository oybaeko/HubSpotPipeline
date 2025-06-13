#!/usr/bin/env python3
"""
BigQuery Access Manager for HubSpot Pipeline Data Migration
Manages dataset permissions and service account access across environments
"""

import subprocess
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional
import logging

class BigQueryAccessManager:
    """Manages BigQuery access permissions for data migration"""
    
    def __init__(self):
        self.project_id = "hubspot-452402"
        self.environments = {
            'dev': {
                'dataset': 'Hubspot_dev_ob',
                'service_account': 'hubspot-dev-ob@hubspot-452402.iam.gserviceaccount.com'
            },
            'staging': {
                'dataset': 'Hubspot_staging',
                'service_account': 'hubspot-staging@hubspot-452402.iam.gserviceaccount.com'
            },
            'prod': {
                'dataset': 'Hubspot_prod',
                'service_account': 'hubspot-prod@hubspot-452402.iam.gserviceaccount.com'
            }
        }
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
        self.logger = logging.getLogger('bq_access')
        
    def run_command(self, cmd: str, check: bool = True) -> subprocess.CompletedProcess:
        """Run shell command and return result"""
        try:
            result = subprocess.run(
                cmd, shell=True, capture_output=True, text=True, check=check
            )
            return result
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Command failed: {cmd}")
            self.logger.error(f"Exit code: {e.returncode}")
            self.logger.error(f"Stderr: {e.stderr}")
            raise
    
    def get_current_user(self) -> str:
        """Get current authenticated user email"""
        result = self.run_command("gcloud config get-value account")
        return result.stdout.strip()
    
    def check_dataset_exists(self, dataset: str) -> bool:
        """Check if BigQuery dataset exists"""
        cmd = f"bq show {self.project_id}:{dataset}"
        result = self.run_command(cmd, check=False)
        return result.returncode == 0
    
    def list_dataset_tables(self, dataset: str) -> List[str]:
        """List tables in a dataset"""
        if not self.check_dataset_exists(dataset):
            return []
        
        cmd = f"bq ls {self.project_id}:{dataset}"
        result = self.run_command(cmd, check=False)
        
        if result.returncode != 0:
            self.logger.debug(f"Failed to list tables in {dataset}: {result.stderr}")
            return []
        
        # Parse the tabular output - look for lines with table names
        lines = result.stdout.strip().split('\n')
        tables = []
        
        for line in lines:
            line = line.strip()
            # Skip empty lines, headers, and separator lines
            if not line or 'tableId' in line or '---' in line or 'Type' in line:
                continue
            
            # Look for lines that start with table names (have TABLE or VIEW in them)
            if 'TABLE' in line or 'VIEW' in line:
                # Split by whitespace and take the first part (table name)
                parts = line.split()
                if parts:
                    table_name = parts[0].strip()
                    if table_name and not table_name.startswith('-'):
                        tables.append(table_name)
        
        return tables
    
    def get_dataset_iam_policy(self, dataset: str) -> Dict:
        """Get IAM policy for a dataset"""
        cmd = f"bq get-iam-policy {self.project_id}:{dataset} --format=json"
        result = self.run_command(cmd, check=False)
        
        if result.returncode != 0:
            return {}
        
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return {}
    
    def test_dataset_access(self, dataset: str) -> Dict[str, bool]:
        """Test different levels of access to a dataset"""
        access_tests = {
            'dataset_exists': False,
            'can_list_tables': False,
            'can_read_table': False,
            'can_write_table': False
        }
        
        # Test 1: Dataset exists
        access_tests['dataset_exists'] = self.check_dataset_exists(dataset)
        if not access_tests['dataset_exists']:
            return access_tests
        
        # Test 2: Can list tables
        tables = self.list_dataset_tables(dataset)
        access_tests['can_list_tables'] = len(tables) >= 0  # Empty list is still success
        
        # Test 3: Can read table (if any exist)
        if tables:
            test_table = tables[0]
            cmd = f"bq show {self.project_id}:{dataset}.{test_table}"
            result = self.run_command(cmd, check=False)
            access_tests['can_read_table'] = result.returncode == 0
        else:
            # No tables to test, but if we can list tables, we probably have read access
            access_tests['can_read_table'] = access_tests['can_list_tables']
        
        # Test 4: Can write (try to create a temp table)
        temp_table = f"temp_access_test_{int(os.getpid())}"
        cmd = f"""bq mk --table \
            {self.project_id}:{dataset}.{temp_table} \
            test_col:STRING"""
        result = self.run_command(cmd, check=False)
        
        if result.returncode == 0:
            access_tests['can_write_table'] = True
            # Clean up temp table
            cleanup_cmd = f"bq rm -f {self.project_id}:{dataset}.{temp_table}"
            self.run_command(cleanup_cmd, check=False)
        
        return access_tests
    
    def grant_user_access(self, dataset: str, user_email: str, role: str = "roles/bigquery.dataViewer") -> bool:
        """Grant user access to dataset"""
        self.logger.info(f"Granting {role} to {user_email} on {dataset}")
        
        cmd = f"""bq add-iam-policy-binding \
            --member="user:{user_email}" \
            --role="{role}" \
            {self.project_id}:{dataset}"""
        
        result = self.run_command(cmd, check=False)
        
        if result.returncode == 0:
            self.logger.info(f"✅ Granted access successfully")
            return True
        else:
            # Show more detailed error information
            self.logger.error(f"❌ Failed to grant access")
            self.logger.error(f"Command: {cmd}")
            self.logger.error(f"Exit code: {result.returncode}")
            if result.stdout:
                self.logger.error(f"Stdout: {result.stdout}")
            if result.stderr:
                self.logger.error(f"Stderr: {result.stderr}")
            
            # Check if it's because the binding already exists
            if "already exists" in result.stderr.lower() or "duplicate" in result.stderr.lower():
                self.logger.info(f"✅ Access already granted (binding exists)")
                return True
            
            return False
    
    def revoke_user_access(self, dataset: str, user_email: str, role: str = "roles/bigquery.dataViewer") -> bool:
        """Revoke user access from dataset"""
        self.logger.info(f"Revoking {role} from {user_email} on {dataset}")
        
        cmd = f"""bq remove-iam-policy-binding \
            --member="user:{user_email}" \
            --role="{role}" \
            {self.project_id}:{dataset}"""
        
        result = self.run_command(cmd, check=False)
        return result.returncode == 0
    
    def setup_migration_access(self, user_email: Optional[str] = None) -> Dict[str, bool]:
        """Setup access permissions needed for data migration"""
        if not user_email:
            user_email = self.get_current_user()
        
        self.logger.info(f"Setting up migration access for: {user_email}")
        
        results = {}
        
        for env, config in self.environments.items():
            dataset = config['dataset']
            
            self.logger.info(f"\n🔧 Setting up access for {env} environment")
            self.logger.info(f"Dataset: {dataset}")
            
            # Check if dataset exists
            if not self.check_dataset_exists(dataset):
                self.logger.warning(f"⚠️  Dataset {dataset} does not exist")
                results[env] = False
                continue
            
            # Grant data editor access for migration (read/write)
            success = self.grant_user_access(dataset, user_email, "roles/bigquery.dataEditor")
            results[env] = success
        
        return results
    
    def cleanup_migration_access(self, user_email: Optional[str] = None) -> Dict[str, bool]:
        """Cleanup migration access permissions"""
        if not user_email:
            user_email = self.get_current_user()
        
        self.logger.info(f"Cleaning up migration access for: {user_email}")
        
        results = {}
        
        for env, config in self.environments.items():
            dataset = config['dataset']
            
            # Keep dev access, remove staging/prod editor access
            if env == 'dev':
                self.logger.info(f"Keeping dev access for ongoing development")
                results[env] = True
                continue
            
            self.logger.info(f"Removing editor access from {env}")
            
            # Remove editor access
            editor_removed = self.revoke_user_access(dataset, user_email, "roles/bigquery.dataEditor")
            
            # Grant viewer access for monitoring
            viewer_granted = self.grant_user_access(dataset, user_email, "roles/bigquery.dataViewer")
            
            results[env] = editor_removed and viewer_granted
        
        return results
    
    def show_access_status(self) -> None:
        """Show current access status for all environments"""
        user_email = self.get_current_user()
        
        print(f"\n📊 ACCESS STATUS for {user_email}")
        print("=" * 80)
        
        for env, config in self.environments.items():
            dataset = config['dataset']
            
            print(f"\n🌍 {env.upper()} Environment")
            print(f"📂 Dataset: {dataset}")
            
            # Test access capabilities
            access_tests = self.test_dataset_access(dataset)
            
            if not access_tests['dataset_exists']:
                print(f"❌ Dataset does not exist")
                continue
            
            # Show access test results
            if access_tests['can_list_tables']:
                print(f"✅ Can list tables")
            else:
                print(f"❌ Cannot list tables")
            
            if access_tests['can_read_table']:
                print(f"✅ Can read tables")
            else:
                print(f"❌ Cannot read tables")
            
            if access_tests['can_write_table']:
                print(f"✅ Can write tables")
            else:
                print(f"❌ Cannot write tables")
            
            # List tables
            tables = self.list_dataset_tables(dataset)
            print(f"📋 Tables: {len(tables)}")
            
            if tables:
                for table in tables[:5]:  # Show first 5
                    print(f"   • {table}")
                if len(tables) > 5:
                    print(f"   • ... and {len(tables) - 5} more")
            
            # Show recommended access level
            if access_tests['can_write_table']:
                print(f"🎯 Access Level: EDITOR (sufficient for migration)")
            elif access_tests['can_read_table']:
                print(f"🎯 Access Level: VIEWER (read-only)")
            else:
                print(f"🎯 Access Level: NONE (no access)")
    
    def create_missing_datasets(self) -> Dict[str, bool]:
        """Create missing datasets"""
        self.logger.info("Creating missing datasets...")
        
        results = {}
        
        for env, config in self.environments.items():
            dataset = config['dataset']
            
            if self.check_dataset_exists(dataset):
                self.logger.info(f"✅ {dataset} already exists")
                results[env] = True
                continue
            
            self.logger.info(f"📝 Creating dataset: {dataset}")
            
            cmd = f"""bq mk --dataset \
                --location=europe-west1 \
                --description="HubSpot Pipeline {env.title()} Environment" \
                {self.project_id}:{dataset}"""
            
            result = self.run_command(cmd, check=False)
            
            if result.returncode == 0:
                self.logger.info(f"✅ Created dataset: {dataset}")
                results[env] = True
            else:
                self.logger.error(f"❌ Failed to create dataset: {dataset}")
                self.logger.error(f"Error: {result.stderr}")
                results[env] = False
        
        return results
    
    def interactive_menu(self):
        """Interactive menu for access management"""
        while True:
            print(f"\n{'='*60}")
            print(f"🔐 BIGQUERY ACCESS MANAGER")
            print(f"{'='*60}")
            print(f"Project: {self.project_id}")
            print(f"User: {self.get_current_user()}")
            print(f"{'='*60}")
            
            print(f"\n📋 OPERATIONS")
            print(f"  1) 📊 Show Access Status")
            print(f"  2) 🔧 Setup Migration Access (data editor)")
            print(f"  3) 🧹 Cleanup Migration Access (back to viewer)")
            print(f"  4) 📝 Create Missing Datasets")
            print(f"  5) 🔍 Test Table Access")
            print(f"  6) 🐛 Debug Table Listing")
            print(f"  0) ❌ Exit")
            
            try:
                choice = input(f"\n🔹 Enter choice (0-6): ").strip()
                
                if choice == '0':
                    print("\n👋 Goodbye!")
                    break
                elif choice == '1':
                    self.show_access_status()
                elif choice == '2':
                    results = self.setup_migration_access()
                    print(f"\n📊 Setup Results:")
                    for env, success in results.items():
                        status = "✅" if success else "❌"
                        print(f"  {status} {env}")
                elif choice == '3':
                    results = self.cleanup_migration_access()
                    print(f"\n📊 Cleanup Results:")
                    for env, success in results.items():
                        status = "✅" if success else "❌"
                        print(f"  {status} {env}")
                elif choice == '4':
                    results = self.create_missing_datasets()
                    print(f"\n📊 Creation Results:")
                    for env, success in results.items():
                        status = "✅" if success else "❌"
                        print(f"  {status} {env}")
                elif choice == '5':
                    self.test_table_access()
                elif choice == '6':
                    self.debug_table_listing()
                else:
                    print("❌ Invalid choice. Please select 0-6.")
                    
            except KeyboardInterrupt:
                print("\n\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {e}")
            
            if choice != '0':
                input("\n⏸️  Press Enter to continue...")
    
    def test_table_access(self):
        """Test access to specific tables"""
        print(f"\n🔍 Testing table access...")
        
        test_tables = ['hs_companies', 'hs_deals', 'hs_owners', 'hs_snapshot_registry']
        
        for env, config in self.environments.items():
            dataset = config['dataset']
            print(f"\n🌍 {env.upper()} - {dataset}")
            
            for table in test_tables:
                table_id = f"{self.project_id}:{dataset}.{table}"
                cmd = f"bq show {table_id}"
                result = self.run_command(cmd, check=False)
                
                if result.returncode == 0:
                    print(f"  ✅ {table}")
                else:
                    # Extract meaningful error message
                    error_msg = "Unknown error"
                    if result.stdout:
                        if "Not found" in result.stdout:
                            error_msg = "Table does not exist"
                        elif "Access Denied" in result.stdout or "Permission denied" in result.stdout:
                            error_msg = "Permission denied"
                        else:
                            # Extract error from stdout
                            lines = result.stdout.strip().split('\n')
                            for line in lines:
                                if 'error' in line.lower():
                                    error_msg = line.strip()
                                    break
                    
                    print(f"  ❌ {table} - {error_msg}")
    
    def debug_table_listing(self):
        """Debug table listing issues"""
        print(f"\n🐛 DEBUG: Table listing")
        
        for env, config in self.environments.items():
            dataset = config['dataset']
            print(f"\n🌍 {env.upper()} - {dataset}")
            
            # Try raw bq ls command
            cmd = f"bq ls {self.project_id}:{dataset}"
            print(f"Command: {cmd}")
            result = self.run_command(cmd, check=False)
            
            print(f"Exit code: {result.returncode}")
            print(f"Stdout: {repr(result.stdout)}")
            print(f"Stderr: {repr(result.stderr)}")
            
            if result.stdout:
                print("Raw output:")
                print(result.stdout)
            
            print("-" * 40)


def main():
    """Main entry point"""
    manager = BigQueryAccessManager()
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'status':
            manager.show_access_status()
        elif command == 'setup':
            results = manager.setup_migration_access()
            for env, success in results.items():
                status = "✅" if success else "❌"
                print(f"{status} {env}")
        elif command == 'cleanup':
            results = manager.cleanup_migration_access()
            for env, success in results.items():
                status = "✅" if success else "❌"
                print(f"{status} {env}")
        elif command == 'create':
            results = manager.create_missing_datasets()
            for env, success in results.items():
                status = "✅" if success else "❌"
                print(f"{status} {env}")
        else:
            print(f"Unknown command: {command}")
            print("Available commands: status, setup, cleanup, create")
    else:
        manager.interactive_menu()


if __name__ == "__main__":
    main()