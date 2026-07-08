"""Test what the lease query returns for property 1122966."""
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

# Test the exact query that property_view uses
run_id = 'run_20260624_104140'
property_id = 1122966
scope_type = 'lease'

print(f"Testing query:")
print(f"  RunId: {run_id}")
print(f"  PropertyId: {property_id}")
print(f"  ScopeType: {scope_type}")
print()

items_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items'
params = {
    '$expand': 'fields',
    '$filter': (
        f"fields/RunId eq '{run_id}' and "
        f"fields/ScopeType eq '{scope_type}' and "
        f"fields/PropertyId eq {int(property_id)}"
    ),
    '$top': 5000
}

print(f"Filter: {params['$filter']}")
print()

response = requests.get(
    items_url, 
    headers={
        'Authorization': f'Bearer {access_token}',
        'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
    }, 
    params=params, 
    timeout=60
)

if response.status_code != 200:
    print(f"ERROR {response.status_code}: {response.text}")
else:
    items = response.json().get('value', [])
    print(f"Found {len(items)} items")
    
    if len(items) > 0:
        print("\nFirst 5 items:")
        for item in items[:5]:
            fields = item.get('fields', {})
            lease_id = fields.get('LeaseIntervalId')
            exceptions = fields.get('ExceptionCountStatic') or fields.get('ExceptionCountStatistic')
            matched = fields.get('MatchedBucketsStatic')
            print(f"  LeaseIntervalId={lease_id} Exceptions={exceptions} Matched={matched}")
