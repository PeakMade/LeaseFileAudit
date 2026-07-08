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

print("Searching all pages for item 2106086...")

all_items = []
list_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$expand=fields&$top=200'

page_count = 0
while list_url and page_count < 20:  # Max 20 pages (4000 items)
    page_count += 1
    resp = requests.get(list_url, headers={'Authorization': f'Bearer {token}'})
    
    if resp.status_code != 200:
        print(f'Error on page {page_count}: {resp.status_code}')
        break
    
    data = resp.json()
    page_items = data.get('value', [])
    all_items.extend(page_items)
    
    print(f'Page {page_count}: {len(page_items)} items (total so far: {len(all_items)})')
    
    # Check if item 2106086 is in this page
    for item in page_items:
        if item.get('id') == '2106086':
            print(f'\n*** FOUND ITEM 2106086 in page {page_count}! ***')
            print(f'    Created: {item.get("createdDateTime")}')
            print(f'    RunId: {item.get("fields", {}).get("field_1")}')
            break
    
    # Get next page
    list_url = data.get('@odata.nextLink')
    if not list_url:
        print(f'\nNo more pages. Total items retrieved: {len(all_items)}')
        break

# Final check
item_ids = [item.get('id') for item in all_items]
if '2106086' in item_ids:
    print('\n✓ Item 2106086 IS in the list query results')
else:
    print('\n✗ Item 2106086 NOT FOUND in any page of list query results')
    print(f'   (but we know it exists because direct query by ID worked)')

# Show date range of items
dates = [item.get('createdDateTime', '')[:10] for item in all_items if item.get('createdDateTime')]
unique_dates = sorted(set(dates))
print(f'\nDate range of items in list: {unique_dates[0] if unique_dates else "N/A"} to {unique_dates[-1] if unique_dates else "N/A"}')
print(f'Unique dates present: {", ".join(unique_dates)}')
