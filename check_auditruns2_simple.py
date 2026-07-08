"""Quick check of AuditRuns2 data using app infrastructure."""
from storage.service import StorageService
from config import config
from activity_logging.sharepoint import _get_app_only_token

# Get token
access_token = _get_app_only_token()
if not access_token:
    print("❌ Failed to get token")
    exit(1)

# Initialize storage service
storage = StorageService(
    base_dir=config.storage.base_dir,
    use_sharepoint=config.storage.is_sharepoint_configured(),
    sharepoint_site_url=config.auth.sharepoint_site_url,
    library_name=config.storage.sharepoint_library_name,
    access_token=access_token,
    audit_results_list_name=config.auth.audit_results_list_name,
)

# Check if SharePoint is available
if not storage._can_use_sharepoint_lists():
    print("❌ SharePoint lists not available")
    exit(1)

print("✅ SharePoint available")

# Get list ID
list_id = storage._get_audit_results_list_id()
print(f"📋 AuditRuns2 List ID: {list_id}")

# Query for any items
site_id = storage._get_site_id()
import requests

headers = {
    'Authorization': f'Bearer {storage.access_token}',
    'Accept': 'application/json'
}

# Get total count
items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
params = {'$top': '1', '$count': 'true'}
response = requests.get(items_url, headers=headers, params=params, timeout=30)

if response.status_code == 200:
    data = response.json()
    count = data.get('@odata.count', 0)
    print(f"📊 Total Items in AuditRuns2: {count}")
    
    # Get recent CLEMSON EDGE items
    params2 = {
        '$filter': "fields/field_1 eq 'run_20260625_105039'",
        '$top': '5',
        '$expand': 'fields'
    }
    response2 = requests.get(items_url, headers=headers, params=params2, timeout=30)
    
    if response2.status_code == 200:
        clemson_items = response2.json().get('value', [])
        print(f"\n🔍 CLEMSON EDGE items (run_20260625_105039): {len(clemson_items)}")
        for item in clemson_items[:3]:
            fields = item.get('fields', {})
            print(f"   - ResultType: {fields.get('field_2', 'N/A')}")
            print(f"     PropertyId: {fields.get('field_3', 'N/A')}")
    else:
        print(f"⚠️  Failed to query CLEMSON items: {response2.status_code}")
        print(f"    Response: {response2.text[:200]}")
else:
    print(f"❌ Failed to query list: {response.status_code}")
    print(f"Response: {response.text[:200]}")

print("\n✅ Check complete!")
