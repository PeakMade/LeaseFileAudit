"""
Check if AuditRuns2 has data for the most recent run
"""
import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path('.') / '.env')

from storage.service import StorageService
from config import config
from activity_logging.sharepoint import _get_app_only_token
import requests

print("="*80)
print("Checking AuditRuns2 for recent run data")
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

# Get the most recent run
runs = storage.list_runs(limit=1)
if not runs:
    print("❌ No runs found")
    exit(1)

test_run_id = runs[0]['run_id']
print(f"\nMost recent run: {test_run_id}")

# Try to get AuditRuns2 list ID
site_id = storage._get_site_id()
if not site_id:
    print("❌ Could not get site ID")
    exit(1)

list_id = storage._get_audit_results_list_id()
if not list_id:
    print("❌ Could not get AuditRuns2 list ID")
    exit(1)

print(f"AuditRuns2 list ID: {list_id}")

# Query for items with this run_id
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
    'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
}

items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
params = {
    '$expand': 'fields',
    '$filter': f"fields/RunId eq '{test_run_id}'",
    '$top': 10,
    '$select': 'id,fields'
}

print(f"\nQuerying AuditRuns2 for run: {test_run_id}")
response = requests.get(items_url, headers=headers, params=params, timeout=30)

if response.status_code != 200:
    print(f"❌ Query failed: {response.status_code}")
    print(f"   Response: {response.text[:500]}")
    exit(1)

items = response.json().get('value', [])
print(f"\n✅ Query successful!")
print(f"   Found {len(items)} items in AuditRuns2 for run {test_run_id}")

if items:
    print("\n   Sample item fields:")
    sample = items[0].get('fields', {})
    for key in ['RunId', 'PropertyId', 'ResultType', 'Status', 'ExpectedTotal', 'ActualTotal']:
        print(f"      {key}: {sample.get(key, 'N/A')}")
else:
    print("\n   ⚠️  No data found in AuditRuns2 for this run")
    print("   This run may have been created before AuditRuns2 writes were enabled")
    print("   or the write may have failed")

print("\n" + "="*80)
