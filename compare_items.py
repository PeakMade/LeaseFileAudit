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

print("Comparing old (visible) vs new (invisible) items...")
print("=" * 80)

# Get an old visible item (363515 from May 27)
print("\n1. OLD VISIBLE ITEM (363515 from May 27):")
old_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/363515?$expand=fields'
resp = requests.get(old_url, headers={'Authorization': f'Bearer {token}'})
if resp.status_code == 200:
    old_item = resp.json()
    print(f'   Status: {resp.status_code}')
    print(f'   ID: {old_item.get("id")}')
    print(f'   Created: {old_item.get("createdDateTime")}')
    print(f'   Content Type: {old_item.get("contentType", {})}')
    print(f'   Fields keys: {list(old_item.get("fields", {}).keys())[:15]}')
    old_content_type_id = old_item.get('contentType', {}).get('id', 'N/A')
    print(f'   Content Type ID: {old_content_type_id}')
else:
    print(f'   Error: {resp.status_code}')
    old_item = None

# Get the new invisible item (2106086 from June 25)
print("\n2. NEW INVISIBLE ITEM (2106086 from June 25):")
new_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/2106086?$expand=fields'
resp = requests.get(new_url, headers={'Authorization': f'Bearer {token}'})
if resp.status_code == 200:
    new_item = resp.json()
    print(f'   Status: {resp.status_code}')
    print(f'   ID: {new_item.get("id")}')
    print(f'   Created: {new_item.get("createdDateTime")}')
    print(f'   Content Type: {new_item.get("contentType", {})}')
    print(f'   Fields keys: {list(new_item.get("fields", {}).keys())[:15]}')
    new_content_type_id = new_item.get('contentType', {}).get('id', 'N/A')
    print(f'   Content Type ID: {new_content_type_id}')
else:
    print(f'   Error: {resp.status_code}')
    new_item = None

# Compare
print("\n" + "=" * 80)
print("COMPARISON:")
print("=" * 80)
if old_item and new_item:
    if old_content_type_id == new_content_type_id:
        print("✓ Content Type IDs MATCH - not the issue")
    else:
        print(f"✗ CONTENT TYPE MISMATCH!")
        print(f"   Old: {old_content_type_id}")
        print(f"   New: {new_content_type_id}")
    
    # Check for other differences
    old_keys = set(old_item.get('fields', {}).keys())
    new_keys = set(new_item.get('fields', {}).keys())
    
    missing_in_new = old_keys - new_keys
    extra_in_new = new_keys - old_keys
    
    if missing_in_new:
        print(f"\n   Fields in old but missing in new: {missing_in_new}")
    if extra_in_new:
        print(f"\n   Extra fields in new: {extra_in_new}")
    
    # Check specific important fields
    print(f"\n   Old Title: {old_item.get('fields', {}).get('Title', 'N/A')}")
    print(f"   New Title: {new_item.get('fields', {}).get('Title', 'N/A')}")
    
    print(f"\n   Old RunId (field_1): {old_item.get('fields', {}).get('field_1', 'N/A')[:50]}")
    print(f"   New RunId (field_1): {new_item.get('fields', {}).get('field_1', 'N/A')[:50]}")

print("\n" + "=" * 80)
print("\nDUMP OF NEW ITEM FULL STRUCTURE:")
if new_item:
    print(json.dumps(new_item, indent=2)[:2000])
