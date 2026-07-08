"""Check what scope types exist in RunDisplaySnapshots for the most recent run."""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

from audit_engine.api_ingest import _get_app_only_token, _resolve_sharepoint_site_id, _resolve_sharepoint_list_id

sharepoint_site_url = os.getenv("SHAREPOINT_SITE_URL")
token = _get_app_only_token()
site_id = _resolve_sharepoint_site_id(token, sharepoint_site_url)
list_id = _resolve_sharepoint_list_id(token, site_id, "RunDisplaySnapshots")

# Get the most recent run ID first
endpoint = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
params = {
    "$expand": "fields($select=RunId,ScopeType)",
    "$filter": "fields/ScopeType eq 'portfolio'",
    "$top": "1",
}

response = requests.get(endpoint, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=60)
if response.status_code != 200:
    print(f"ERROR: {response.status_code} - {response.text}")
    exit(1)

items = response.json().get("value", [])
if not items:
    print("No runs found in RunDisplaySnapshots")
    exit(0)

run_id = items[0].get("fields", {}).get("RunId")
print(f"Most recent run: {run_id}")
print()

# Now get all scope types for this run
params2 = {
    "$expand": "fields($select=ScopeType)",
    "$filter": f"fields/RunId eq '{run_id}'",
    "$top": "5000",
}

response2 = requests.get(endpoint, headers={"Authorization": f"Bearer {token}"}, params=params2, timeout=60)
if response2.status_code != 200:
    print(f"ERROR: {response2.status_code} - {response2.text}")
    exit(1)

items2 = response2.json().get("value", [])
scope_counts = {}
for item in items2:
    scope_type = item.get("fields", {}).get("ScopeType", "unknown")
    scope_counts[scope_type] = scope_counts.get(scope_type, 0) + 1

print(f"Snapshot breakdown for run {run_id}:")
for scope_type, count in sorted(scope_counts.items()):
    print(f"  {scope_type}: {count} rows")
print()
print(f"Total: {len(items2)} snapshots")
