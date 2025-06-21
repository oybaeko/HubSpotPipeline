#!/usr/bin/env python3
"""
Simple Working Views Deployment
Uses the exact authentication pattern that worked in the test
"""

import os
from pathlib import Path

# Clear service account credentials FIRST (this made the test work)
if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
    del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    print("🔑 Using user authentication (cleared service account)")

from google.cloud import bigquery

def main():
    print("📊 Simple Working Views Deployment")
    print("Using exact pattern from successful test")
    
    # Fixed values (no config loading to avoid issues)
    project_id = "hubspot-452402"
    
    # Environment selection
    print("\n🌍 Select Environment:")
    print("1. Development (Hubspot_dev_ob)")
    print("2. Staging (Hubspot_staging)")
    print("3. Production (Hubspot_prod)")
    
    choice = input("Choose (1-3): ").strip()
    
    if choice == "1":
        dataset_id = "Hubspot_dev_ob"
        env_name = "development"
    elif choice == "2":
        dataset_id = "Hubspot_staging"
        env_name = "staging"
    elif choice == "3":
        dataset_id = "Hubspot_prod"
        env_name = "production"
    else:
        print("❌ Invalid choice")
        return
    
    print(f"\n✅ Selected: {env_name} ({dataset_id})")
    
    # Create BigQuery client (using cleared credentials)
    client = bigquery.Client(project=project_id)
    
    # Test access first
    print(f"\n🔍 Testing access to {project_id}.{dataset_id}")
    try:
        # Test simple query
        query = "SELECT 1 as test"
        job = client.query(query)
        job.result()
        print("✅ Can run simple query")
        
        # Test dataset access  
        dataset = client.get_dataset(f"{project_id}.{dataset_id}")
        print("✅ Can access dataset")
        
        # Test table query
        query = f"SELECT COUNT(*) as count FROM `{project_id}.{dataset_id}.hs_companies` LIMIT 1"
        job = client.query(query)
        result = job.result()
        for row in result:
            print(f"✅ Can query tables: {row.count} companies")
        
    except Exception as e:
        print(f"❌ Access test failed: {e}")
        return
    
    # Find SQL files
    script_dir = Path(__file__).parent
    sql_folder = script_dir / "sql"
    
    if not sql_folder.exists():
        print(f"❌ SQL folder not found: {sql_folder}")
        return
    
    sql_files = list(sql_folder.glob("*.sql"))
    if not sql_files:
        print("❌ No SQL files found")
        return
    
    print(f"\n📊 Found {len(sql_files)} SQL files:")
    for i, sql_file in enumerate(sql_files, 1):
        print(f"  {i}. {sql_file.name}")
    
    # Deploy choice
    print(f"\nOptions:")
    print(f"1. Deploy all files")
    for i, sql_file in enumerate(sql_files, 2):
        print(f"{i}. Deploy {sql_file.name}")
    
    choice = input(f"\nChoose (1-{len(sql_files)+1}): ").strip()
    
    if choice == "1":
        files_to_deploy = sql_files
    else:
        try:
            idx = int(choice) - 2
            if 0 <= idx < len(sql_files):
                files_to_deploy = [sql_files[idx]]
            else:
                print("❌ Invalid choice")
                return
        except ValueError:
            print("❌ Invalid choice")
            return
    
    # Confirm deployment
    if env_name == "production":
        print(f"\n🚨 PRODUCTION DEPLOYMENT")
        confirm = input("Type 'DEPLOY TO PRODUCTION' to confirm: ").strip()
        if confirm != 'DEPLOY TO PRODUCTION':
            print("❌ Cancelled")
            return
    elif env_name == "staging":
        print(f"\n⚠️ STAGING DEPLOYMENT")
        confirm = input("Type 'YES' to confirm: ").strip().upper()
        if confirm != 'YES':
            print("❌ Cancelled")
            return
    else:
        confirm = input(f"Deploy to {env_name}? (y/n): ").strip().lower()
        if confirm != 'y':
            print("❌ Cancelled")
            return
    
    # Deploy files
    success_count = 0
    for sql_file in files_to_deploy:
        print(f"\n🚀 Deploying {sql_file.name}...")
        
        try:
            # Read SQL
            with open(sql_file, 'r') as f:
                sql = f.read()
            
            # Replace placeholders
            sql = sql.replace('${PROJECT_ID}', project_id)
            sql = sql.replace('${DATASET_ID}', dataset_id)
            sql = sql.replace('{project}', project_id)
            sql = sql.replace('{dataset}', dataset_id)
            
            # Execute
            job = client.query(sql)
            job.result()  # Wait for completion
            
            print(f"✅ {sql_file.name} deployed successfully!")
            success_count += 1
            
        except Exception as e:
            print(f"❌ Failed to deploy {sql_file.name}: {e}")
    
    print(f"\n📊 Deployment complete: {success_count}/{len(files_to_deploy)} successful")

if __name__ == "__main__":
    main()