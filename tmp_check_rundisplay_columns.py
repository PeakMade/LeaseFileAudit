"""Check RunDisplaySnapshots columns."""
import os
import requests
from pathlib import Path
from storage.service import StorageService

storage = StorageService(base_dir=Path("instance/runs"))

# Get site and list IDs
site_id = storage._get_site_id()
list_id = storage._get_run_display_snapshots_list_id()

print(f"Site ID: {site_id}")
print(f"List ID: {list_id}")

# Get columns
headers = {'Authorization': f'Bearer {storage.access_token}'}
columns_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns"
response = requests.get(columns_url, headers=headers, timeout=30)

if response.status_code == 200:
    columns = response.json().get('value', [])
    print(f"\n✅ Found {len(columns)} columns:")
    for col in sorted(columns, key=lambda x: x.get('name', '')):
        name = col.get('name')
        col_type = col.get('text') and 'text' or col.get('number') and 'number' or col.get('dateTime') and 'dateTime' or 'other'
        print(f"  - {name} ({col_type})")
        
    # Check specifically for ArCodeId and AuditMonth
    column_names = [c.get('name') for c in columns]
    print(f"\n🔍 ArCodeId exists: {'ArCodeId' in column_names}")
    print(f"🔍 AuditMonth exists: {'AuditMonth' in column_names}")
else:
    print(f"❌ Failed to get columns: {response.status_code} - {response.text}")
