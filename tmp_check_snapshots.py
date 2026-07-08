"""Check RunDisplaySnapshots for recent runs."""
import requests
import os
from dotenv import load_dotenv

load_dotenv()

# Auth
client_id = os.getenv('SHAREPOINT_CLIENT_ID')
tenant_id = os.getenv('SHAREPOINT_TENANT_ID')
client_secret = os.getenv('MICROSOFT_PROVIDER_AUTHENTICATION_SECRET')

token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
token_data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'scope': 'https://graph.microsoft.com/.default'
}
token_response = requests.post(token_url, data=token_data)
access_token = token_response.json()['access_token']

# Get site
site_url = os.getenv('SHAREPOINT_SITE_URL', 'https://peakcampus.sharepoint.com/sites/BaseCampApps')
site_path = site_url.split('.com')[-1]
site_response = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/peakcampus.sharepoint.com:{site_path}',
    headers={'Authorization': f'Bearer {access_token}'}
)
site_id = site_response.json()['id']

# Get list
list_response = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists',
    headers={'Authorization': f'Bearer {access_token}'}
)
lists = list_response.json().get('value', [])
snapshot_list = next((l for l in lists if l['name'] == 'RunDisplaySnapshots'), None)
if not snapshot_list:
    print("RunDisplaySnapshots list not found")
    exit(1)

list_id = snapshot_list['id']

# Query ALL items (paginate)
items = []
next_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields&$top=5000'
while next_url:
    items_response = requests.get(
        next_url,
        headers={'Authorization': f'Bearer {access_token}'}
    )
    batch = items_response.json().get('value', [])
    items.extend(batch)
    next_url = items_response.json().get('@odata.nextLink')
print(f"\nFound {len(items)} items in RunDisplaySnapshots")

# Group by RunId and ScopeType
from collections import defaultdict
by_run_and_scope = defaultdict(lambda: defaultdict(int))

for item in items:
    fields = item.get('fields', {})
    run_id = fields.get('RunId', 'unknown')
    scope_type = fields.get('ScopeType', 'unknown')
    by_run_and_scope[run_id][scope_type] += 1

print("\nBreakdown by run and scope:")
for run_id, scopes in sorted(by_run_and_scope.items()):
    print(f"\n{run_id}:")
    for scope_type, count in sorted(scopes.items()):
        print(f"  {scope_type}: {count} rows")
    total = sum(scopes.values())
    print(f"  TOTAL: {total} rows")
