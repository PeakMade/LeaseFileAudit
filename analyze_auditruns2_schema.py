#!/usr/bin/env python3
"""
Discover AuditRuns2 list schema by examining existing items and field properties.
"""

import sys
import os
from pathlib import Path
from typing import Optional, Dict, Any

sys.path.insert(0, str(Path(__file__).parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from activity_logging.sharepoint import _get_app_only_token
import requests
import json


SHAREPOINT_SITE_URL = "https://peakcampus.sharepoint.com/sites/BaseCampApps"
AUDITRUNS2_LIST_NAME = "AuditRuns2"


def get_site_id(token: str, site_url: str) -> str:
    parts = site_url.replace('https://', '').split('/')
    hostname = parts[0]
    site_path = '/'.join(parts[1:])
    url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{site_path}"
    
    headers = {'Authorization': f'Bearer {token}'}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()["id"]


def find_list_by_name(token: str, site_id: str, list_name: str) -> Optional[Dict]:
    headers = {'Authorization': f'Bearer {token}'}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    params = {"$filter": f"displayName eq '{list_name}'"}
    
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    
    items = r.json().get('value', [])
    return items[0] if items else None


def get_list_columns_detailed(token: str, site_id: str, list_id: str) -> Dict[str, Any]:
    """Get detailed column information including internal names and types."""
    headers = {'Authorization': f'Bearer {token}'}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns"
    
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    
    columns = r.json().get('value', [])
    schema = {}
    for col in columns:
        schema[col['name']] = {
            'displayName': col.get('displayName', ''),
            'description': col.get('description', ''),
            'type': col.get('columnTypes', [None])[0] if col.get('columnTypes') else None,
        }
    return schema


def get_list_items(token: str, site_id: str, list_id: str, limit: int = 5) -> list:
    """Get sample items from the list."""
    headers = {'Authorization': f'Bearer {token}'}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    params = {
        "$top": limit,
        "$expand": "fields"
    }
    
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    
    return r.json().get('value', [])


def main():
    print("🔍 Analyzing AuditRuns2 Schema...\n")
    
    # Get token
    print("📝 Acquiring access token...")
    token = _get_app_only_token()
    if not token:
        print("❌ Failed to acquire access token.")
        sys.exit(1)
    print("✅ Token acquired\n")
    
    # Get site ID
    print(f"🔍 Resolving site ID...")
    site_id = get_site_id(token, SHAREPOINT_SITE_URL)
    print(f"✅ Site ID: {site_id}\n")
    
    # Find list
    print(f"🔍 Finding list '{AUDITRUNS2_LIST_NAME}'...")
    list_info = find_list_by_name(token, site_id, AUDITRUNS2_LIST_NAME)
    if not list_info:
        print(f"❌ List not found")
        sys.exit(1)
    
    list_id = list_info['id']
    print(f"✅ List found: {list_id}\n")
    
    # Get schema
    print("📋 Getting column details...")
    schema = get_list_columns_detailed(token, site_id, list_id)
    
    print("\n" + "="*80)
    print("COLUMN SCHEMA")
    print("="*80)
    for col_name in sorted(schema.keys()):
        col_info = schema[col_name]
        print(f"\nField Name: {col_name}")
        print(f"  Display Name: {col_info['displayName']}")
        print(f"  Description: {col_info['description']}")
        print(f"  Type: {col_info['type']}")
    
    # Get sample items to see what data might be there
    print("\n" + "="*80)
    print("SAMPLE ITEMS")
    print("="*80)
    
    items = get_list_items(token, site_id, list_id, limit=5)
    print(f"\nFound {len(items)} items in list\n")
    
    for i, item in enumerate(items, 1):
        print(f"\n--- Item {i} (ID: {item.get('id')}) ---")
        fields = item.get('fields', {})
        for key in sorted(fields.keys()):
            value = fields[key]
            if value is not None and value != '':
                val_str = str(value)
                if len(val_str) > 100:
                    val_str = val_str[:97] + "..."
                print(f"  {key}: {val_str}")
    
    # Generate template for writing
    print("\n" + "="*80)
    print("WRITE TEMPLATE")
    print("="*80)
    
    # Extract editable field names (exclude system fields)
    system_fields = {'ID', 'Created', 'Modified', 'Author', 'Editor', 'ContentType', 
                     '_UIVersionString', '_ComplianceFlags', '_ComplianceTag', 
                     '_ComplianceTagUserId', '_ComplianceTagWrittenTime', '_IsRecord',
                     'Attachments', 'AppAuthor', 'AppEditor', 'ComplianceAssetId',
                     'DocIcon', 'Edit', 'FolderChildCount', 'ItemChildCount', 
                     'LinkTitle', 'LinkTitleNoMenu', '_ColorTag'}
    
    editable_fields = [f for f in schema.keys() if f not in system_fields]
    
    print(f"\nEditable fields ({len(editable_fields)}):")
    for field in sorted(editable_fields):
        col_info = schema[field]
        display = col_info.get('displayName', field) or field
        print(f"  - {field:20} ({display})")
    
    print("\n" + "-"*80)
    print("Python code to write a new item:")
    print("-"*80)
    
    template_fields = {field: "" for field in editable_fields[:5]}
    template_fields['Title'] = 'Your Audit Title'
    
    print("\nfrom write_auditruns2 import create_item, get_site_id, find_list_by_name, SHAREPOINT_SITE_URL, AUDITRUNS2_LIST_NAME")
    print("from activity_logging.sharepoint import _get_app_only_token")
    print("\ntoken = _get_app_only_token()")
    print("site_id = get_site_id(token, SHAREPOINT_SITE_URL)")
    print("list_info = find_list_by_name(token, site_id, AUDITRUNS2_LIST_NAME)")
    print("list_id = list_info['id']")
    print("\naudit_data = {")
    for field, value in template_fields.items():
        print(f"    \"{field}\": {json.dumps(value)},")
    print("}")
    print("\nresult = create_item(token, site_id, list_id, audit_data)")
    print("print(f'✅ Created item: {result[\"id\"]}')")


if __name__ == "__main__":
    main()
