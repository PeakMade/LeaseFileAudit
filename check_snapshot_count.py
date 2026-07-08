import requests
from activity_logging.sharepoint import _get_app_only_token
import os
from dotenv import load_dotenv

load_dotenv()
token = _get_app_only_token()
site_url = os.getenv("SHAREPOINT_SITE_URL")

# Get site ID
site_resp = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_url.replace("https://", "").replace("/", ",")}',
    headers={'Authorization': f'Bearer {token}'}
)
site_id = site_resp.json()['id']

# Get list ID
list_resp = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists',
    params={'$filter': "displayName eq 'RunDisplaySnapshots'"},
    headers={'Authorization': f'Bearer {token}'}
)
list_data = list_resp.json()
if not list_data.get('value'):
    print("List not found")
    exit(1)
list_id = list_data['value'][0]['id']

# Get item count
count_resp = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items',
    params={'$top': '1', '$count': 'true'},
    headers={'Authorization': f'Bearer {token}', 'ConsistencyLevel': 'eventual'}
)
count = count_resp.json().get('@odata.count', 'Unknown')
print(f"Items remaining in RunDisplaySnapshots: {count}")
