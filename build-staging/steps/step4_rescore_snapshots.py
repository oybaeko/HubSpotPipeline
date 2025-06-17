#!/usr/bin/env python3
"""
Step 4: Rescore All Snapshots
Triggers the scoring Cloud Function to process all snapshots in staging
"""

import sys
import os
import logging
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any

from google.cloud import bigquery

class RescoreSnapshotsStep:
    """
    Step 4: Trigger rescoring of all snapshots via Cloud Function
    
    This step:
    1. Counts existing snapshots in staging
    2. Estimates processing time
    3. Triggers scoring Cloud Function via Pub/Sub
    4. Provides monitoring guidance
    5. Creates registry entry for the rescore operation
    """
    
    def __init__(self, project_id: str = "hubspot-452402", dataset: str = "Hubspot_staging"):
        self.project_id = project_id
        self.staging_dataset = dataset
        self.pubsub_topic = "hubspot-events-staging"
        
        # Setup logging
        self.logger = logging.getLogger('rescore_snapshots_step')
        
        # Setup environment
        self._setup_environment()
        
        # Track results
        self.results = {}
        self.completed = False
        
    def _setup_environment(self):
        """Setup environment and clear service account credentials"""
        # Clear service account credentials to use user auth
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
            self.logger.debug("Cleared GOOGLE_APPLICATION_CREDENTIALS to use user auth")
    
    def validate_prerequisites(self) -> bool:
        """Check if gcloud CLI and staging data are available"""
        try:
            # Check gcloud CLI availability
            result = subprocess.run("gcloud --version", shell=True, capture_output=True, text=True, check=False)
            if result.returncode != 0:
                self.logger.error("❌ gcloud CLI not available")
                self.logger.error("💡 Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install")
                return False
            
            self.logger.info("✅ gcloud CLI available")
            
            # Check BigQuery access
            client = bigquery.Client(project=self.project_id)
            
            # Check staging dataset access
            staging_dataset_ref = client.dataset(self.staging_dataset)
            list(client.list_tables(staging_dataset_ref, max_results=1))
            self.logger.info("✅ Staging dataset access confirmed")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Prerequisites check failed: {e}")
            return False
    
    def get_snapshot_count(self, client: bigquery.Client) -> Dict[str, Any]:
        """Get snapshot count and statistics from staging data"""
        try:
            # Count unique snapshots across companies table
            snapshot_query = f"""
            SELECT 
                COUNT(DISTINCT snapshot_id) as total_snapshots,
                COUNT(*) as total_records,
                MIN(snapshot_id) as earliest_snapshot,
                MAX(snapshot_id) as latest_snapshot
            FROM `{self.project_id}.{self.staging_dataset}.hs_companies`
            """
            
            result = client.query(snapshot_query).result()
            
            for row in result:
                snapshot_stats = {
                    'total_snapshots': row.total_snapshots,
                    'total_records': row.total_records,
                    'earliest_snapshot': row.earliest_snapshot,
                    'latest_snapshot': row.latest_snapshot
                }
                
                self.logger.info(f"📊 Staging snapshot statistics:")
                self.logger.info(f"  • Total snapshots: {snapshot_stats['total_snapshots']}")
                self.logger.info(f"  • Total records: {snapshot_stats['total_records']:,}")
                self.logger.info(f"  • Date range: {snapshot_stats['earliest_snapshot']} to {snapshot_stats['latest_snapshot']}")
                
                return snapshot_stats
            
            return {'total_snapshots': 0, 'total_records': 0}
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get snapshot statistics: {e}")
            return {'total_snapshots': 0, 'total_records': 0}
    
    def get_snapshot_details(self, client: bigquery.Client) -> List[Dict]:
        """Get detailed breakdown of snapshots by source"""
        try:
            # Get snapshot details with source information from registry
            details_query = f"""
            SELECT 
                r.snapshot_id,
                r.triggered_by,
                r.status,
                r.record_timestamp,
                COUNT(c.company_id) as company_count,
                COUNT(d.deal_id) as deal_count
            FROM `{self.project_id}.{self.staging_dataset}.hs_snapshot_registry` r
            LEFT JOIN `{self.project_id}.{self.staging_dataset}.hs_companies` c 
                ON r.snapshot_id = c.snapshot_id
            LEFT JOIN `{self.project_id}.{self.staging_dataset}.hs_deals` d 
                ON r.snapshot_id = d.snapshot_id
            GROUP BY r.snapshot_id, r.triggered_by, r.status, r.record_timestamp
            ORDER BY r.record_timestamp DESC
            """
            
            result = client.query(details_query).result()
            
            snapshot_details = []
            excel_count = 0
            production_count = 0
            
            for row in result:
                detail = {
                    'snapshot_id': row.snapshot_id,
                    'triggered_by': row.triggered_by,
                    'status': row.status,
                    'record_timestamp': row.record_timestamp,
                    'company_count': row.company_count,
                    'deal_count': row.deal_count,
                    'total_count': row.company_count + row.deal_count
                }
                
                snapshot_details.append(detail)
                
                # Count by source
                if row.triggered_by == 'excel_import_crm':
                    excel_count += 1
                elif row.triggered_by == 'production_migration_step3':
                    production_count += 1
            
            self.logger.info(f"📸 Snapshot breakdown:")
            self.logger.info(f"  • Excel imports: {excel_count} snapshots")
            self.logger.info(f"  • Production migration: {production_count} snapshots")
            self.logger.info(f"  • Total to score: {len(snapshot_details)} snapshots")
            
            return snapshot_details
            
        except Exception as e:
            self.logger.error(f"❌ Failed to get snapshot details: {e}")
            return []
    
    def estimate_processing_time(self, snapshot_count: int) -> Dict[str, int]:
        """Estimate processing time based on snapshot count"""
        # Based on orchestrator: 35 seconds per snapshot
        seconds_per_snapshot = 35
        total_seconds = snapshot_count * seconds_per_snapshot
        
        minutes = total_seconds // 60
        seconds = total_seconds % 60
        
        return {
            'total_seconds': total_seconds,
            'minutes': minutes,
            'seconds': seconds,
            'seconds_per_snapshot': seconds_per_snapshot
        }
    
    def trigger_rescore_via_pubsub(self) -> Dict[str, Any]:
        """Trigger rescore-all via Pub/Sub message"""
        try:
            pubsub_cmd = [
                "gcloud", "pubsub", "topics", "publish", self.pubsub_topic,
                '--message={"type":"hubspot.rescore.all","data":{}}',
                "--project", self.project_id
            ]
            
            self.logger.info(f"📤 Publishing rescore-all message to {self.pubsub_topic} topic...")
            result = subprocess.run(pubsub_cmd, capture_output=True, text=True, check=True)
            
            if result.returncode == 0:
                # Extract message ID from output
                output = result.stdout.strip()
                message_id = None
                
                if "messageIds:" in output:
                    # Extract message ID
                    lines = output.split('\n')
                    for line in lines:
                        if line.strip().startswith('- '):
                            message_id = line.strip()[2:].strip("'\"")
                            break
                
                return {
                    'success': True,
                    'message_id': message_id,
                    'output': output
                }
            else:
                return {
                    'success': False,
                    'error': f"Exit code: {result.returncode}",
                    'stderr': result.stderr
                }
                
        except subprocess.CalledProcessError as e:
            return {
                'success': False,
                'error': f"Command failed: {e}",
                'stderr': getattr(e, 'stderr', ''),
                'stdout': getattr(e, 'stdout', '')
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Unexpected error: {e}"
            }
    
    def create_rescore_registry_entry(self, client: bigquery.Client, snapshot_count: int, message_id: str = None):
        """Create registry entry for the rescore operation"""
        try:
            current_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
            # Create a unique "rescore operation" ID
            rescore_operation_id = f"rescore_operation_{current_time}"
            
            notes = f"Triggered rescore-all for {snapshot_count} snapshots"
            if message_id:
                notes += f" | Pub/Sub Message ID: {message_id}"
            
            registry_query = f"""
            INSERT INTO `{self.project_id}.{self.staging_dataset}.hs_snapshot_registry` 
            (snapshot_id, record_timestamp, triggered_by, status, notes)
            VALUES (
                '{rescore_operation_id}',
                CURRENT_TIMESTAMP(),
                'rescore_step4',
                'started',
                '{notes}'
            )
            """
            
            client.query(registry_query).result()
            self.logger.info(f"📝 Created rescore registry entry: {rescore_operation_id}")
            
            return rescore_operation_id
            
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to create rescore registry entry: {e}")
            return None
    
    def show_monitoring_instructions(self, snapshot_count: int, estimated_time: Dict):
        """Show monitoring instructions for tracking progress"""
        self.logger.info("📊 MONITORING INSTRUCTIONS:")
        self.logger.info("=" * 50)
        self.logger.info("🔄 The scoring function is now processing all snapshots")
        self.logger.info(f"⏱️ Estimated duration: {estimated_time['minutes']}m {estimated_time['seconds']}s")
        self.logger.info("")
        self.logger.info("📊 Monitor progress in Google Cloud Console:")
        self.logger.info("   • Go to: Logging > Logs Explorer")
        self.logger.info("   • Query: resource.type=\"cloud_run_revision\" AND resource.labels.service_name=\"hubspot-scoring-staging\"")
        self.logger.info("")
        self.logger.info("🔍 Alternative monitoring:")
        self.logger.info("   • Check Cloud Functions logs")
        self.logger.info("   • Monitor Pub/Sub topic activity")
        self.logger.info("   • Query staging dataset for scoring updates")
        self.logger.info("=" * 50)
    
    def execute(self, dry_run: bool = True) -> bool:
        """Execute rescore operation"""
        self.logger.info(f"🔄 STEP 4: Rescoring all snapshots (dry_run={dry_run})")
        
        try:
            # Validate prerequisites
            if not self.validate_prerequisites():
                return False
            
            # Initialize BigQuery client
            client = bigquery.Client(project=self.project_id)
            
            # Get snapshot statistics
            snapshot_stats = self.get_snapshot_count(client)
            if snapshot_stats['total_snapshots'] == 0:
                self.logger.warning("⚠️ No snapshots found in staging - nothing to score")
                return False
            
            # Get detailed snapshot breakdown
            snapshot_details = self.get_snapshot_details(client)
            
            # Estimate processing time
            estimated_time = self.estimate_processing_time(snapshot_stats['total_snapshots'])
            
            self.logger.info(f"⏱️ Estimated processing time: {estimated_time['minutes']}m {estimated_time['seconds']}s")
            self.logger.info(f"📊 Processing rate: ~{estimated_time['seconds_per_snapshot']}s per snapshot")
            
            if dry_run:
                self.logger.info("🧪 DRY RUN: Would trigger rescore-all operation")
                self.logger.info(f"📤 Would publish to topic: {self.pubsub_topic}")
                self.logger.info(f"🔄 Would process {snapshot_stats['total_snapshots']} snapshots")
                
                # Store dry run results
                self.results = {
                    'dry_run': True,
                    'snapshot_stats': snapshot_stats,
                    'snapshot_details': snapshot_details,
                    'estimated_time': estimated_time,
                    'pubsub_topic': self.pubsub_topic
                }
                
                self.completed = True
                return True
            
            # Execute actual rescore trigger
            self.logger.info("🚀 Triggering rescore-all operation...")
            pubsub_result = self.trigger_rescore_via_pubsub()
            
            if pubsub_result['success']:
                self.logger.info("✅ Pub/Sub message published successfully")
                if pubsub_result.get('message_id'):
                    self.logger.info(f"📨 Message ID: {pubsub_result['message_id']}")
                
                # Create registry entry
                registry_id = self.create_rescore_registry_entry(
                    client, 
                    snapshot_stats['total_snapshots'], 
                    pubsub_result.get('message_id')
                )
                
                # Show monitoring instructions
                self.show_monitoring_instructions(snapshot_stats['total_snapshots'], estimated_time)
                
                # Store results
                self.results = {
                    'dry_run': False,
                    'snapshot_stats': snapshot_stats,
                    'snapshot_details': snapshot_details,
                    'estimated_time': estimated_time,
                    'pubsub_result': pubsub_result,
                    'registry_id': registry_id,
                    'pubsub_topic': self.pubsub_topic
                }
                
                self.completed = True
                self.logger.info("🎉 Rescore operation initiated successfully!")
                return True
                
            else:
                self.logger.error("❌ Failed to publish Pub/Sub message")
                self.logger.error(f"Error: {pubsub_result.get('error', 'Unknown error')}")
                if pubsub_result.get('stderr'):
                    self.logger.error(f"Details: {pubsub_result['stderr']}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Rescore operation failed: {e}")
            import traceback
            self.logger.debug(f"Traceback: {traceback.format_exc()}")
            return False
    
    def get_results(self) -> Dict[str, Any]:
        """Get step execution results"""
        return {
            'completed': self.completed,
            'results': self.results
        }
    
    def show_status(self):
        """Show current status and results"""
        print(f"\n🔄 RESCORE OPERATION STATUS")
        print("=" * 50)
        print(f"Project: {self.project_id}")
        print(f"Dataset: {self.staging_dataset}")
        print(f"Pub/Sub Topic: {self.pubsub_topic}")
        print(f"Completed: {'✅' if self.completed else '❌'}")
        
        if self.results:
            print(f"\n📈 RESULTS:")
            
            snapshot_stats = self.results.get('snapshot_stats', {})
            if snapshot_stats:
                print(f"  • Total Snapshots: {snapshot_stats.get('total_snapshots', 0)}")
                print(f"  • Total Records: {snapshot_stats.get('total_records', 0):,}")
                
            estimated_time = self.results.get('estimated_time', {})
            if estimated_time:
                print(f"  • Estimated Time: {estimated_time.get('minutes', 0)}m {estimated_time.get('seconds', 0)}s")
            
            print(f"  • Dry Run: {self.results.get('dry_run', 'Unknown')}")
            
            if not self.results.get('dry_run', True):
                pubsub_result = self.results.get('pubsub_result', {})
                if pubsub_result.get('success'):
                    print(f"  • Pub/Sub: ✅ Message sent")
                    if pubsub_result.get('message_id'):
                        print(f"  • Message ID: {pubsub_result['message_id']}")
                else:
                    print(f"  • Pub/Sub: ❌ Failed")
                
                registry_id = self.results.get('registry_id')
                if registry_id:
                    print(f"  • Registry Entry: {registry_id}")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description="Rescore Snapshots Step")
    parser.add_argument('--project', default='hubspot-452402', help='BigQuery project')
    parser.add_argument('--dataset', default='Hubspot_staging', help='Staging dataset')
    parser.add_argument('--topic', default='hubspot-events-staging', help='Pub/Sub topic for triggering rescore')
    parser.add_argument('--dry-run', action='store_true', help='Preview rescore operation only')
    parser.add_argument('--verbose', action='store_true', help='Verbose logging')
    parser.add_argument('--check-prereqs', action='store_true', help='Check prerequisites only')
    parser.add_argument('--show-snapshots', action='store_true', help='Show snapshot details only')
    
    args = parser.parse_args()
    
    # Setup logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    
    # Create rescore step
    step = RescoreSnapshotsStep(args.project, args.dataset)
    if args.topic != 'hubspot-events-staging':
        step.pubsub_topic = args.topic
    
    print(f"🔄 Rescore Snapshots Step")
    print(f"Project: {args.project}")
    print(f"Dataset: {args.dataset}")
    print(f"Pub/Sub Topic: {step.pubsub_topic}")
    print(f"Dry run: {args.dry_run}")
    
    # Check prerequisites only
    if args.check_prereqs:
        print("\n🔍 CHECKING PREREQUISITES...")
        if step.validate_prerequisites():
            print("✅ All prerequisites satisfied")
            return 0
        else:
            print("❌ Prerequisites not met")
            return 1
    
    # Show snapshots only
    if args.show_snapshots:
        print("\n📸 SHOWING SNAPSHOT DETAILS...")
        try:
            from google.cloud import bigquery
            client = bigquery.Client(project=args.project)
            snapshot_details = step.get_snapshot_details(client)
            if snapshot_details:
                print("✅ Snapshot details retrieved")
                return 0
            else:
                print("❌ No snapshots found")
                return 1
        except Exception as e:
            print(f"❌ Error: {e}")
            return 1
    
    # Execute rescore operation
    try:
        success = step.execute(dry_run=args.dry_run)
        
        if success:
            print(f"\n✅ Rescore operation completed!")
            step.show_status()
            return 0
        else:
            print(f"\n❌ Rescore operation failed!")
            return 1
            
    except KeyboardInterrupt:
        print(f"\n⚠️ Operation cancelled by user")
        return 1
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())