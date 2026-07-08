"""Check what columns exist in RunDisplaySnapshots list."""
import os
import requests
from dotenv import load_dotenv
load_dotenv(override=True)

from auth import AzureTokenManager

# Get access token
token_mgr = AzureTokenManager(
    client_id=os.getenv('SHAREPOINT_CLIENT_ID'),
    tenant_id=os.getenv('SHAREPOINT_TENANT_ID'),
    client_secret=os.getenv('MICROSOFT_PROVIDER_AUTHENTICATION_SECRET')
)
token = token_mgr.get_token()

# Get site ID
site_url = 'peakcampus.sharepoint.com,f83f37af-e64c-4dbc-9457-68c9484ee93b,f65502be-a283-4eae-a759-a23b23603fbb'

# Get list
headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
list_resp = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_url}/lists',
    headers=headers
)

if list_resp.status_code == 200:
    lists = list_resp.json().get('value', [])
    snapshot_list = next((l for l in lists if l['displayName'] == 'RunDisplaySnapshots'), None)
    
    if snapshot_list:
        list_id = snapshot_list['id']
        print(f"Found RunDisplaySnapshots list: {list_id}")
        print()
        
        # Get columns
        columns_resp = requests.get(
            f'https://graph.microsoft.com/v1.0/sites/{site_url}/lists/{list_id}/columns',
            headers=headers
        )
        
        if columns_resp.status_code == 200:
            columns = columns_resp.json().get('value', [])
            print(f"Total columns: {len(columns)}")
            print()
            print("Column names:")
            for col in sorted(columns, key=lambda c: c.get('displayName', '')):
                name = col.get('name', '')
                display_name = col.get('displayName', '')
                col_type = col.get('columnGroup', 'Custom')
                if col_type != 'Custom' and not display_name.startswith('_'):
                    # Skip system columns
                    continue
                print(f"  - {display_name} (internal: {name})")
        else:
            print(f"Failed to get columns: {columns_resp.status_code}")
            print(columns_resp.text)
    else:
        print("RunDisplaySnapshots list not found")
else:
    print(f"Failed to get lists: {list_resp.status_code}")
    print(list_resp.text)
