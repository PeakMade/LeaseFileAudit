"""Quick script to check AuditRuns2 data in SharePoint."""
import requests
from activity_logging.sharepoint import _get_app_only_token

# Get auth token
token = _get_app_only_token()
if not token:
    print("❌ Failed to get auth token")
    exit(1)

headers = {
    'Authorization': f'Bearer {token}',
    'Accept': 'application/json'
}

# Get site ID
site_url = "https://peakcampus.sharepoint.com/sites/BaseCampApps"
site_response = requests.get(
    f"https://graph.microsoft.com/v1.0/sites/{site_url.replace('https://', '')}",
    headers=headers
)
if site_response.status_code != 200:
    print(f"❌ Failed to get site: {site_response.status_code}")
    exit(1)

site_id = site_response.json()['id']
print(f"✅ Site ID: {site_id}")

# Get AuditRuns2 list
list_id = "d8166180-5dcb-41a9-84c0-0ab104b77c27"
list_response = requests.get(
    f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}",
    headers=headers
)
if list_response.status_code != 200:
    print(f"❌ Failed to get list: {list_response.status_code}")
    exit(1)

list_data = list_response.json()
print(f"\n📋 AuditRuns2 List:")
print(f"   Name: {list_data['displayName']}")
print(f"   Created: {list_data.get('createdDateTime', 'N/A')}")
print(f"   Last Modified: {list_data.get('lastModifiedDateTime', 'N/A')}")

# Get item count
items_response = requests.get(
    f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$top=1&$count=true",
    headers=headers
)
if items_response.status_code != 200:
    print(f"❌ Failed to get items: {items_response.status_code}")
    print(f"Response: {items_response.text}")
    exit(1)

items_data = items_response.json()
item_count = items_data.get('@odata.count', 0)
print(f"\n📊 Total Items: {item_count}")

# Get sample recent items
if item_count > 0:
    recent_response = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$top=5&$expand=fields&$orderby=fields/CreatedAt desc",
        headers=headers
    )
    if recent_response.status_code == 200:
        recent_items = recent_response.json().get('value', [])
        print(f"\n🔍 Recent {len(recent_items)} Items:")
        for item in recent_items:
            fields = item.get('fields', {})
            print(f"   - RunId: {fields.get('RunId', fields.get('field_1', 'N/A'))}")
            print(f"     ResultType: {fields.get('ResultType', fields.get('field_2', 'N/A'))}")
            print(f"     PropertyId: {fields.get('PropertyId', fields.get('field_3', 'N/A'))}")
            print(f"     CreatedAt: {fields.get('CreatedAt', fields.get('field_21', 'N/A'))}")
            print()
    else:
        print(f"⚠️  Failed to get recent items: {recent_response.status_code}")

print("\n✅ Check complete!")
