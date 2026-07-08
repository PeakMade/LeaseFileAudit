"""
Test script to verify that load_run() can now read from AuditRuns2 and CSVs
"""
import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path('.') / '.env')

from storage.service import StorageService
from config import config
from activity_logging.sharepoint import _get_app_only_token

print("="*80)
print("Testing StorageService.load_run() with AuditRuns2 and CSV fallback")
print("="*80)

# Initialize storage service
access_token = _get_app_only_token()
storage = StorageService(
    base_dir=config.storage.base_dir,
    use_sharepoint=config.storage.is_sharepoint_configured(),
    sharepoint_site_url=config.auth.sharepoint_site_url,
    library_name=config.storage.sharepoint_library_name,
    access_token=access_token,
    audit_results_list_name=config.auth.audit_results_list_name,
)

print("\n1. Listing available runs from RunDisplaySnapshots...")
runs = storage.list_runs(limit=5)
if not runs:
    print("   ❌ No runs found in RunDisplaySnapshots")
    print("   Please run an audit first or check SharePoint connection")
    exit(1)

print(f"   ✅ Found {len(runs)} runs")
for i, run in enumerate(runs, 1):
    print(f"      {i}. {run['run_id']} - {run.get('timestamp', 'Unknown')}")

# Test loading the most recent run
test_run_id = runs[0]['run_id']
print(f"\n2. Testing load_run() for: {test_run_id}")
print("   This should now read from AuditRuns2 (preferred) or CSVs (fallback)...")

try:
    run_data = storage.load_run(test_run_id)
    
    print(f"\n   ✅ SUCCESS! Run loaded successfully:")
    print(f"      - Bucket results: {len(run_data['bucket_results'])} rows")
    print(f"      - Findings: {len(run_data['findings'])} rows")
    print(f"      - Expected detail: {len(run_data['expected_detail'])} rows")
    print(f"      - Actual detail: {len(run_data['actual_detail'])} rows")
    print(f"      - Variance detail: {len(run_data.get('variance_detail', []))} rows")
    
    # Check where data was loaded from
    bucket_source = run_data['bucket_results'].attrs.get('read_source', 'unknown')
    bucket_reason = run_data['bucket_results'].attrs.get('read_reason', 'unknown')
    print(f"\n      📊 Data source info:")
    print(f"         - Bucket results read from: {bucket_source}")
    print(f"         - Read reason: {bucket_reason}")
    
    if bucket_source == 'sharepoint_list':
        print(f"\n      🎉 PERFECT! Data is being read from AuditRuns2 SharePoint list!")
    elif bucket_source == 'snapshots':
        print(f"\n      ⚠️  Data is being read from RunDisplaySnapshots (AuditRuns2 unavailable)")
    elif bucket_source == 'csv':
        print(f"\n      ⚠️  Data is being read from CSV files (SharePoint lists unavailable)")
    
    print("\n   🎯 Complete lease data is now accessible in the app!")
    
except Exception as e:
    print(f"\n   ❌ ERROR loading run: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
print("Test complete!")
print("="*80)
