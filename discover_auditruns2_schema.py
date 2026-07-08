"""
Discover the actual column names in AuditRuns2
"""
import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path('.') / '.env')

from activity_logging.sharepoint import _get_app_only_token
import requests

print("="*80)
print("Discovering AuditRuns2 schema")
print("="*80)

access_token = _get_app_only_token()
site_url = 'peakcampus.sharepoint.com:/sites/BaseCampApps'
list_id = 'd8166180-5dcb-41a9-84c0-0ab104b77c27'

# Get site ID
site_resp = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_url}',
    headers={'Authorization': f'Bearer {access_token}'}
)
site_id = site_resp.json()['id']
print(f"\nSite ID: {site_id}")

# Get list columns
columns_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns'
resp = requests.get(columns_url, headers={'Authorization': f'Bearer {access_token}'})

if resp.status_code != 200:
    print(f"❌ Failed to get columns: {resp.status_code}")
    print(resp.text[:500])
    exit(1)

columns = resp.json().get('value', [])
print(f"\n✅ Found {len(columns)} columns in AuditRuns2:")
print("\nRelevant columns:")
for col in columns:
    name = col.get('name', 'N/A')
    display_name = col.get('displayName', 'N/A')
    col_type = col.get('type', 'N/A')
    
    # Filter for relevant columns
    if any(keyword in name.lower() or keyword in display_name.lower() 
           for keyword in ['run', 'property', 'lease', 'status', 'expected', 'actual', 'result']):
        print(f"   - {display_name}")
        print(f"     Internal name: {name}")
        print(f"     Type: {col_type}")
        print()

# Get a few items to see actual field names
print("\n" + "="*80)
print("Sample data from AuditRuns2 (first 3 items):")
print("="*80)

items_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items'
params = {'$expand': 'fields', '$top': 3}
resp = requests.get(items_url, headers={'Authorization': f'Bearer {access_token}'}, params=params)

if resp.status_code != 200:
    print(f"❌ Failed to get items: {resp.status_code}")
    exit(1)

items = resp.json().get('value', [])
if items:
    print(f"\nFound {len(items)} sample items")
    for i, item in enumerate(items, 1):
        fields = item.get('fields', {})
        print(f"\nItem {i} field names:")
        for key in sorted(fields.keys())[:20]:  # Show first 20 fields
            print(f"   {key}: {str(fields.get(key))[:50]}")
else:
    print("\n⚠️  No items found in AuditRuns2")

print("\n" + "="*80)
