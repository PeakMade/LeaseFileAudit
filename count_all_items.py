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

print("Counting ALL items in AuditRuns2...")
print("=" * 80)

all_items = []
list_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$select=id,createdDateTime&$top=500'

page_count = 0
while list_url and page_count < 50:  # Allow up to 50 pages (25,000 items)
    page_count += 1
    resp = requests.get(list_url, headers={'Authorization': f'Bearer {token}'})
    
    if resp.status_code != 200:
        print(f'Error on page {page_count}: {resp.status_code} - {resp.text[:200]}')
        break
    
    data = resp.json()
    page_items = data.get('value', [])
    all_items.extend(page_items)
    
    if page_count % 5 == 0 or not data.get('@odata.nextLink'):
        print(f'Page {page_count}: {len(page_items)} items (total so far: {len(all_items)})')
    
    # Get next page
    list_url = data.get('@odata.nextLink')
    if not list_url:
        print(f'\nReached end of list at page {page_count}.')
        break

print(f'\n*** TOTAL ITEMS IN LIST: {len(all_items)} ***')

# Check dates
june25_count = sum(1 for item in all_items if '2026-06-25' in item.get('createdDateTime', ''))
may27_count = sum(1 for item in all_items if '2026-05-27' in item.get('createdDateTime', ''))

print(f'\nMay 27 items: {may27_count}')
print(f'June 25 items: {june25_count}')

if len(all_items) > 5000:
    print(f'\n⚠️ LIST EXCEEDS 5000 ITEM THRESHOLD!')
    print(f'   SharePoint list view threshold may be preventing queries from working correctly.')
    print(f'   Solution: Index the field_1 (RunId) column in SharePoint list settings.')

# Check if item 2106086 is in the full result set
item_ids = [item.get('id') for item in all_items]
if '2106086' in item_ids:
    print(f'\n✓ Item 2106086 IS in the full list')
else:
    print(f'\n✗ Item 2106086 NOT in the full list')

print("\n" + "=" * 80)
