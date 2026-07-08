import requests, json
from activity_logging.sharepoint import _get_app_only_token
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path('.') / '.env')
token = _get_app_only_token()

site_url = 'peakcampus.sharepoint.com:/sites/BaseCampApps'
list_id = 'd8166180-5dcb-41a9-84c0-0ab104b77c27'

# Get site ID
site_resp = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_url}',
    headers={'Authorization': f'Bearer {token}'}
)
site_id = site_resp.json()['id']

print("Checking AuditRuns2 list configuration...")
print("=" * 80)

# Get list details
print("\n1. LIST DETAILS:")
list_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}'
resp = requests.get(list_url, headers={'Authorization': f'Bearer {token}'})
if resp.status_code == 200:
    list_data = resp.json()
    print(f'   Name: {list_data.get("name")}')
    print(f'   Display Name: {list_data.get("displayName")}')
    print(f'   Template: {list_data.get("list", {}).get("template")}')
    print(f'   Content Types Enabled: {list_data.get("list", {}).get("contentTypesEnabled")}')
    print(f'   Hidden: {list_data.get("list", {}).get("hidden")}')
else:
    print(f'   Error: {resp.status_code}')

# Get list views
print("\n2. LIST VIEWS:")
views_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/views'
resp = requests.get(views_url, headers={'Authorization': f'Bearer {token}'})
if resp.status_code == 200:
    views = resp.json().get('value', [])
    print(f'   Found {len(views)} views:\n')
    for view in views:
        print(f'   View: {view.get("name")}')
        print(f'      Display Name: {view.get("displayName")}')
        print(f'      ID: {view.get("id")}')
        print(f'      Is Default: {view.get("isDefault", False)}')
        print(f'      Hidden: {view.get("hidden", False)}')
        print(f'      View Type: {view.get("viewType", "N/A")}')
        
        # Check if there's a filter/query
        view_query = view.get('viewQuery', '')
        if view_query:
            print(f'      *** VIEW HAS FILTER/QUERY: {view_query[:200]}')
        else:
            print(f'      No filter/query')
        print()
else:
    print(f'   Error: {resp.status_code}')

# Get content types
print("\n3. CONTENT TYPES:")
ct_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/contentTypes'
resp = requests.get(ct_url, headers={'Authorization': f'Bearer {token}'})
if resp.status_code == 200:
    cts = resp.json().get('value', [])
    print(f'   Found {len(cts)} content types:')
    for ct in cts:
        print(f'      {ct.get("name")} (ID: {ct.get("id")})')
else:
    print(f'   Error: {resp.status_code}')

print("\n" + "=" * 80)
