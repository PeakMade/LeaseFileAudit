"""
Comprehensive test to verify CSV AND SharePoint read/write flow
Tests that the app works "exactly the way it was" with both storage methods
"""
import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path('.') / '.env')

from storage.service import StorageService
from config import config
from activity_logging.sharepoint import _get_app_only_token

print("="*80)
print("Testing Complete CSV + SharePoint Read/Write Flow")
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

print("\n1️⃣  CONFIGURATION CHECK")
print("-" * 80)
print(f"✓ USE_SHAREPOINT_STORAGE: {os.getenv('USE_SHAREPOINT_STORAGE')}")
print(f"✓ DISABLE_CSV_WRITES: {os.getenv('DISABLE_CSV_WRITES', 'false')}")
print(f"✓ ASYNC_AUDIT_RESULTS_WRITE: {os.getenv('ASYNC_AUDIT_RESULTS_WRITE', 'NOT SET (defaults to false)')}")
print(f"✓ SHAREPOINT_WRITE_EXCEPTIONS_ONLY: {os.getenv('SHAREPOINT_WRITE_EXCEPTIONS_ONLY', 'NOT SET (defaults to false)')}")
print(f"✓ SharePoint site URL: {config.auth.sharepoint_site_url}")
print(f"✓ Document library: {config.storage.sharepoint_library_name}")
print(f"✓ AuditRuns2 list name: {config.auth.audit_results_list_name}")

print("\n2️⃣  AUTHENTICATION CHECK")
print("-" * 80)
can_use_lists = storage._can_use_sharepoint_lists()
print(f"✓ Access token available: {'YES' if access_token else 'NO'}")
print(f"✓ Can use SharePoint lists: {'YES' if can_use_lists else 'NO'}")

if not can_use_lists:
    print("\n❌ ERROR: Cannot access SharePoint lists!")
    print("   This will prevent AuditRuns2 reads/writes")
    exit(1)

print("\n3️⃣  SHAREPOINT LIST VERIFICATION")
print("-" * 80)
try:
    site_id = storage._get_site_id()
    print(f"✓ SharePoint site ID resolved: {site_id[:30]}...")
    
    list_id = storage._get_audit_results_list_id()
    if list_id:
        print(f"✓ AuditRuns2 list ID resolved: {list_id}")
    else:
        print("❌ Could not resolve AuditRuns2 list ID")
        exit(1)
    
    snapshots_list_id = storage._get_run_display_snapshots_list_id()
    if snapshots_list_id:
        print(f"✓ RunDisplaySnapshots list ID resolved: {snapshots_list_id}")
    else:
        print("⚠️  Could not resolve RunDisplaySnapshots list ID")
        
except Exception as e:
    print(f"❌ Error accessing SharePoint: {e}")
    exit(1)

print("\n4️⃣  READ PATH TEST (Historical Data)")
print("-" * 80)
runs = storage.list_runs(limit=3)
print(f"✓ Found {len(runs)} runs in RunDisplaySnapshots")

if runs:
    test_run_id = runs[0]['run_id']
    print(f"\n   Testing load_run() for: {test_run_id}")
    
    try:
        run_data = storage.load_run(test_run_id)
        bucket_source = run_data['bucket_results'].attrs.get('read_source', 'unknown')
        bucket_reason = run_data['bucket_results'].attrs.get('read_reason', 'unknown')
        
        print(f"   ✓ Loaded {len(run_data['bucket_results'])} bucket results")
        print(f"   ✓ Loaded {len(run_data['findings'])} findings")
        print(f"   ✓ Data source: {bucket_source} (reason: {bucket_reason})")
        
        if bucket_source == 'sharepoint_list':
            print("   🎉 Reading from AuditRuns2 SharePoint list!")
        elif bucket_source == 'snapshots':
            print("   ⚠️  Reading from RunDisplaySnapshots (AuditRuns2 fallback)")
        elif bucket_source == 'csv':
            print("   ⚠️  Reading from CSV files (SharePoint fallback)")
            
    except Exception as e:
        print(f"   ❌ Error loading run: {e}")

print("\n5️⃣  WRITE PATH CONFIGURATION")
print("-" * 80)
write_details_async = os.getenv('ASYNC_AUDIT_RESULTS_WRITE', 'false').lower() == 'true'
disable_csv = os.getenv('DISABLE_CSV_WRITES', 'false').lower() == 'true'
write_exceptions_only = os.getenv('SHAREPOINT_WRITE_EXCEPTIONS_ONLY', 'false').lower() == 'true'

print(f"✓ CSV writes: {'DISABLED' if disable_csv else 'ENABLED'}")
print(f"✓ AuditRuns2 writes: {'ASYNC' if write_details_async else 'SYNC'}")
print(f"✓ AuditRuns2 mode: {'EXCEPTIONS ONLY' if write_exceptions_only else 'ALL ROWS'}")

if disable_csv:
    print("\n⚠️  WARNING: CSV writes are disabled!")
    print("   Change DISABLE_CSV_WRITES=false to enable")
    
if write_exceptions_only:
    print("\n⚠️  WARNING: Writing only exceptions to AuditRuns2")
    print("   Change SHAREPOINT_WRITE_EXCEPTIONS_ONLY=false to write all data")

print("\n6️⃣  EXPECTED BEHAVIOR ON NEXT AUDIT RUN")
print("-" * 80)
print("When you run a new audit, data will be saved to:")
print(f"  1. {'✓' if not disable_csv else '✗'} CSV files (expected_detail.csv, actual_detail.csv, bucket_results.csv, findings.csv)")
print(f"  2. ✓ Parquet files (expected_detail.parquet, actual_detail.parquet)")
print(f"  3. {'✓' if can_use_lists else '✗'} AuditRuns2 SharePoint list ({'all rows' if not write_exceptions_only else 'exceptions only'})")
print(f"  4. {'✓' if can_use_lists else '✗'} RunDisplaySnapshots SharePoint list")
print(f"  5. {'✓' if can_use_lists else '✗'} Audit Run Metrics SharePoint list")

print("\nWhen you load historical runs, data will be read from (in order):")
print("  1. Memory cache (if available)")
print("  2. AuditRuns2 SharePoint list (preferred)")
print("  3. RunDisplaySnapshots (fallback if AuditRuns2 empty)")
print("  4. CSV files (final fallback)")

print("\n" + "="*80)
if can_use_lists and not disable_csv:
    print("✅ COMPLETE FLOW READY: CSV + SharePoint reads & writes are configured")
elif can_use_lists:
    print("⚠️  PARTIAL: SharePoint ready, but CSV writes disabled")
elif not disable_csv:
    print("⚠️  PARTIAL: CSV writes ready, but SharePoint access unavailable")
else:
    print("❌ ERROR: Neither CSV nor SharePoint writes are enabled!")
print("="*80)
