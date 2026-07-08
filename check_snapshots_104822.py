"""Check RunDisplaySnapshots for run_20260701_104822"""
import requests
import os
from msal import ConfidentialClientApplication

# Get auth token
client_id = os.getenv('AZURE_CLIENT_ID')
client_secret = os.getenv('AZURE_CLIENT_SECRET')
tenant_id = os.getenv('AZURE_TENANT_ID')

app = ConfidentialClientApplication(
    client_id,
    authority=f"https://login.microsoftonline.com/{tenant_id}",
    client_credential=client_secret
)

token_response = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
access_token = token_response.get('access_token')

# Query RunDisplaySnapshots
site_url = "https://peakcampus.sharepoint.com/sites/BaseCampApps"
site_id = "peakcampus.sharepoint.com,f83f37af-e64c-4dbc-9457-68c9484ee93b,f65502be-a283-4eae-a759-a23b23603fbb"

headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json'
}

# Get RunDisplaySnapshots list ID
lists_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
lists_response = requests.get(lists_url, headers=headers, params={'$filter': "displayName eq 'RunDisplaySnapshots'"})
lists_data = lists_response.json()
list_id = lists_data['value'][0]['id'] if lists_data.get('value') else None

if not list_id:
    print("❌ RunDisplaySnapshots list not found")
    exit(1)

print(f"✓ Found RunDisplaySnapshots list: {list_id}")

# Query for run_20260701_104822
run_id = "run_20260701_104822"
items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
params = {
    '$filter': f"fields/RunId eq '{run_id}'",
    '$top': 999
}

response = requests.get(items_url, headers=headers, params=params)
data = response.json()

print(f"\n=== RunDisplaySnapshots for {run_id} ===")
print(f"Status: {response.status_code}")
print(f"Total snapshots found: {len(data.get('value', []))}")

if data.get('value'):
    scope_counts = {}
    for item in data['value']:
        fields = item.get('fields', {})
        scope = fields.get('ScopeType', 'unknown')
        scope_counts[scope] = scope_counts.get(scope, 0) + 1
    
    print(f"\nBreakdown by scope:")
    for scope, count in sorted(scope_counts.items()):
        print(f"  {scope}: {count}")
    
    print(f"\nFirst 3 snapshots:")
    for item in data['value'][:3]:
        fields = item.get('fields', {})
        print(f"  - ScopeType: {fields.get('ScopeType')}, PropertyId: {fields.get('PropertyId')}, "
              f"LeaseIntervalId: {fields.get('LeaseIntervalId')}, ExceptionCount: {fields.get('ExceptionCountStatic')}")
else:
    print("\n❌ NO SNAPSHOTS FOUND - Audit either hasn't finished or write failed")
    print("\nCheck app logs for:")
    print("  - [STORAGE] Step 6/7: Writing display snapshots")
    print("  - [STORAGE] ✓ Display snapshots written successfully")
