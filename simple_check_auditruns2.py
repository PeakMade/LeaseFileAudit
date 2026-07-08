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

print("Querying AuditRuns2 for recent items...")

# Simple query - no filters, no ordering
list_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields&$top=50'
resp = requests.get(list_url, headers={'Authorization': f'Bearer {token}'})

print(f'Status: {resp.status_code}')

if resp.status_code == 200:
    data = resp.json()
    items = data.get('value', [])
    print(f'Total items returned: {len(items)}')
    print('\nMost recent 15 items:')
    
    # Sort by createdDateTime in Python
    items_sorted = sorted(items, key=lambda x: x.get('createdDateTime', ''), reverse=True)
    
    for item in items_sorted[:15]:
        fields = item.get('fields', {})
        run_id = fields.get('field_1', 'N/A')
        created = item.get('createdDateTime', 'N/A')
        item_id = item.get('id', 'N/A')
        print(f'  Item {item_id}: {created} - RunId: {run_id[:60]}')
    
    # Check specifically for June 25 items
    june25_items = [item for item in items if '2026-06-25' in item.get('createdDateTime', '')]
    print(f'\n\nJune 25, 2026 items found: {len(june25_items)}')
    for item in june25_items[:10]:
        fields = item.get('fields', {})
        print(f'  Item {item["id"]}: RunId={fields.get("field_1", "N/A")}')
else:
    print(f'Error: {resp.text[:500]}')
