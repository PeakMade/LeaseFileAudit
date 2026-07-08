import requests
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

print("=" * 80)
print("CHECKING FOR RECENT AUDIT RUNS IN AUDITRUNS2")
print("=" * 80)

# Try multiple approaches to find recent items

# 1. Get all items sorted by created date descending
print("\n1. Top 20 items sorted by creation date (newest first):")
list_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields&$orderby=createdDateTime desc&$top=20'
resp = requests.get(list_url, headers={'Authorization': f'Bearer {token}'})
data = resp.json()
print(f'   Status: {resp.status_code}')
print(f'   Items found: {len(data.get("value", []))}')
for item in data.get('value', [])[:10]:
    fields = item.get('fields', {})
    print(f'   Item {item["id"]}: RunId={fields.get("field_1", "N/A")[:50]}, Created={item.get("createdDateTime", "N/A")}')

# 2. Look for today's date specifically
print("\n2. Filtering for June 25, 2026 items:")
list_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields&$filter=createdDateTime ge 2026-06-25T00:00:00Z'
resp = requests.get(list_url, headers={'Authorization': f'Bearer {token}'})
data = resp.json()
print(f'   Status: {resp.status_code}')
print(f'   Items found: {len(data.get("value", []))}')
for item in data.get('value', [])[:10]:
    fields = item.get('fields', {})
    print(f'   Item {item["id"]}: RunId={fields.get("field_1", "N/A")[:50]}, Created={item.get("createdDateTime", "N/A")}')

# 3. Count total items in list
print("\n3. Total item count:")
list_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$count=true&$top=1'
resp = requests.get(list_url, headers={'Authorization': f'Bearer {token}'})
data = resp.json()
print(f'   Total items in list: {data.get("@odata.count", "N/A")}')

print("\n" + "=" * 80)
