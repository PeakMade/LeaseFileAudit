"""
Quick test audit to capture full storage logs
"""
import os
import sys

# Set required env vars
os.environ['REQUIRE_AUTH'] = 'false'

# Load .env
from dotenv import load_dotenv
from pathlib import Path
env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)

from storage.service import StorageService
from activity_logging.sharepoint import _get_app_only_token
import json
import pandas as pd

print("=" * 80)
print("TEST AUDIT - Capturing full storage logs")
print("=" * 80)

access_token = _get_app_only_token()
print(f"\n[OK] Access token obtained: {access_token[:50] if access_token else 'NONE'}...")

storage = StorageService(
    base_dir="instance/runs",
    use_sharepoint=True,
    sharepoint_site_url="https://peakcampus.sharepoint.com/sites/BaseCampApps",
    library_name="LeaseFileAudit Runs",
    access_token=access_token,
    audit_results_list_name="AuditRuns2"
)

print(f"[OK] StorageService initialized")
print(f"  - use_sharepoint: {storage.use_sharepoint}")
print(f"  - audit_results_list_name: {storage.audit_results_list_name}")
print(f"  - access_token: {'SET' if storage.access_token else 'MISSING'}")
print(f"  - _can_use_sharepoint_lists(): {storage._can_use_sharepoint_lists()}")

# Create minimal run data as DataFrames
run_id = "test_run_20260625_999999"

expected_detail = pd.DataFrame([{
    'resident_name': 'TEST RESIDENT',
    'charge_amount': 1000.00,
    'charge_date': '2026-01-01'
}])

actual_detail = pd.DataFrame([{
    'resident_name': 'TEST RESIDENT',
    'charge_amount': 1000.00,
    'charge_date': '2026-01-01'
}])

bucket_results = pd.DataFrame([{
    'property_id': 1122966,
    'property_name': 'TEST PROPERTY',
    'bucket_name': 'RENT',
    'status': 'MATCHED',
    'severity': 'info',
    'entrata_charge_code': '154771',
    'entrata_resident_id': '12345',
    'entrata_charge_amount': 1000.00,
    'scheduled_charge_amount': 1000.00,
    'variance_amount': 0.00
}])

findings = pd.DataFrame([{
    'property_id': 1122966,
    'property_name': 'TEST PROPERTY',
    'bucket_name': 'RENT',
    'status': 'MATCHED',
    'severity': 'info',
    'message': 'Test finding'
}])

metadata = {
    'run_id': run_id,
    'property_id': 1122966,
    'property_name': 'TEST PROPERTY',
    'run_date': '2026-06-25',
    'total_buckets': 1,
    'matched': 1,
    'exceptions': 0
}

print(f"\n\nCalling save_run()...")
print("=" * 80)

# Save the run
storage.save_run(
    run_id=run_id,
    expected_detail=expected_detail,
    actual_detail=actual_detail,
    bucket_results=bucket_results,
    findings=findings,
    metadata=metadata,
    write_display_snapshots=False  # Skip snapshots for speed
)

print("\n" + "=" * 80)
print("save_run() call completed - check logs above for:")
print("  1. [SAVE_RUN DEBUG] Environment variable values")
print("  2. [STEP 7 DEBUG] Execution path decision")
print("  3. [AUDITRUNS2_ASYNC] Background write start/completion")
print("=" * 80)

# Wait for async write to complete
import time
print("\nWaiting 5 seconds for async write to complete...")
time.sleep(5)

print("\n[OK] Test complete - check output above")
