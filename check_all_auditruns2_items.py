import requests
from activity_logging.sharepoint import _get_app_only_token
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path('.') / '.env')

token = _get_app_only_token()
site_url = 'peakcampus.sharepoint.com:/sites/BaseCampApps'
list_id = 'd8166180-5dcb-41a9-84c0-0ab104b77c27'

# Get site ID
site_resp = requests.get(f'https://graph.microsoft.com/v1.0/sites/{site_url}', headers={'Authorization': f'Bearer {token}'})
site_id = site_resp.json()['id']

# Get ALL items (no filter) sorted by created date
list_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items'
query_headers = {'Authorization': f'Bearer {token}'}
resp = requests.get(f'{list_url}?$top=10&$expand=fields', headers=query_headers)

print(f'Status: {resp.status_code}')
if resp.status_code == 200:
    items = resp.json().get('value', [])
    print(f'Total items returned: {len(items)}')
    if items:
        print('\nMost recent 3 items:')
        for i, item in enumerate(items[:3]):
            print(f'\n  Item {i+1}:')
            print(f'    ID: {item["id"]}')
            print(f'    Created: {item.get("createdDateTime", "N/A")}')
            fields = item.get('fields', {})
            print(f'    Title: {fields.get("Title", "N/A")}')
            print(f'    RunId (field_1): {fields.get("field_1", "N/A")}')
            print(f'    ResultType (field_2): {fields.get("field_2", "N/A")}')
