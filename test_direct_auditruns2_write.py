"""
Direct test of AuditRuns2 async write
"""
import os
import sys
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)

from storage.service import StorageService
from activity_logging.sharepoint import _get_app_only_token
import pandas as pd
import time

print("=" * 80)
print("DIRECT TEST OF AUDITRUNS2 ASYNC WRITE")
print("=" * 80)

access_token = _get_app_only_token()
print(f"\n[OK] Access token obtained: {access_token[:50]}...")

storage = StorageService(
    base_dir="instance/runs",
    use_sharepoint=True,
    sharepoint_site_url="https://peakcampus.sharepoint.com/sites/BaseCampApps",
    library_name="LeaseFileAudit Runs",
    access_token=access_token,
    audit_results_list_name="AuditRuns2"
)

print(f"[OK] StorageService initialized")
print(f"  - _can_use_sharepoint_lists(): {storage._can_use_sharepoint_lists()}")

# Create minimal test data
run_id = "direct_test_20260625_888888"

bucket_results = pd.DataFrame([{
    'property_id': 1122966,
    'property_name': 'DIRECT TEST PROPERTY',
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
    'property_name': 'DIRECT TEST PROPERTY',
    'bucket_name': 'RENT',
    'status': 'MATCHED',
    'severity': 'info',
    'message': 'Direct test finding'
}])

print(f"\n[DIRECT TEST] Calling _write_results_to_sharepoint_list_async()...")
print(f"  - run_id: {run_id}")
print(f"  - bucket_results: {len(bucket_results)} rows")
print(f"  - findings: {len(findings)} rows")
print("=" * 80)

# Call the async write method directly
storage._write_results_to_sharepoint_list_async(
    run_id=run_id,
    bucket_results=bucket_results,
    findings=findings
)

print("\n[DIRECT TEST] Async write dispatched, waiting 10 seconds for completion...")
time.sleep(10)

print("\n" + "=" * 80)
print("[DIRECT TEST] Complete - check logs above for:")
print("  1. [AUDITRUNS2_ASYNC] Starting background write")
print("  2. [AUDITRUNS2_ASYNC] Background write completed/failed")
print("  3. Any error messages")
print("=" * 80)

# Now query SharePoint to verify the data was written
print("\n" + "=" * 80)
print("[VERIFY] Querying SharePoint for test entry...")
print("=" * 80)

import requests
token = access_token
site_url = 'peakcampus.sharepoint.com:/sites/BaseCampApps'
list_id = 'd8166180-5dcb-41a9-84c0-0ab104b77c27'

# Get site ID
site_resp = requests.get(f'https://graph.microsoft.com/v1.0/sites/{site_url}', headers={'Authorization': f'Bearer {token}'})
site_id = site_resp.json()['id']

# Query for our test entry by RunID field
list_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items'
filter_query = f"fields/field_1 eq '{run_id}'"  # field_1 is RunId
query_headers = {
    'Authorization': f'Bearer {token}',
    'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
}
resp = requests.get(f'{list_url}?$top=10&$expand=fields&$filter={filter_query}', headers=query_headers)

print(f"Query URL: {resp.url}")
print(f"Response status: {resp.status_code}")

if resp.status_code == 200:
    items = resp.json().get('value', [])
    if items:
        print(f"\n[SUCCESS] Found {len(items)} item(s) in AuditRuns2!")
        for item in items:
            fields = item.get('fields', {})
            print(f"  - Title: {fields.get('Title', 'N/A')}")
            print(f"  - RunId (field_1): {fields.get('field_1', 'N/A')}")
            print(f"  - ResultType (field_2): {fields.get('field_2', 'N/A')}")
            print(f"  - Created: {item.get('createdDateTime', 'N/A')}")
    else:
        print(f"\n[FAIL] NO items found with RunID={run_id}")
        print("Items were reported as created (201) but don't appear in queries!")
else:
    print(f"\n[ERROR] Query failed: {resp.text}")
