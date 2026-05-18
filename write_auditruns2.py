#!/usr/bin/env python3
"""
Write audit run data to AuditRuns2 SharePoint list at:
https://peakcampus.sharepoint.com/sites/BaseCampApps/Lists/AuditRuns2/AllItems.aspx
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from activity_logging.sharepoint import _get_app_only_token
import requests


# Your specific SharePoint configuration
SHAREPOINT_SITE_URL = "https://peakcampus.sharepoint.com/sites/BaseCampApps"
AUDITRUNS2_LIST_NAME = "AuditRuns2"


def get_site_id(token: str, site_url: str) -> str:
    """Resolve SharePoint site ID from site URL."""
    parts = site_url.replace('https://', '').split('/')
    hostname = parts[0]
    site_path = '/'.join(parts[1:])
    url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{site_path}"
    
    headers = {'Authorization': f'Bearer {token}'}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()["id"]


def find_list_by_name(token: str, site_id: str, list_name: str) -> Optional[Dict]:
    """Find SharePoint list by display name."""
    headers = {'Authorization': f'Bearer {token}'}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    params = {"$filter": f"displayName eq '{list_name}'"}
    
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    
    items = r.json().get('value', [])
    return items[0] if items else None


def get_list_schema(token: str, site_id: str, list_id: str) -> Dict[str, Any]:
    """Get list columns/schema."""
    headers = {'Authorization': f'Bearer {token}'}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns"
    
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    
    columns = r.json().get('value', [])
    schema = {}
    for col in columns:
        schema[col['name']] = {
            'displayName': col.get('displayName', ''),
            'type': col.get('columnTypes', [None])[0] if col.get('columnTypes') else None,
            'required': col.get('required', False)
        }
    return schema


def create_item(token: str, site_id: str, list_id: str, fields: Dict[str, Any]) -> Dict:
    """Create a new item in AuditRuns2."""
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    
    payload = {"fields": fields}
    
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    
    return r.json()


def write_audit_run(token: str, site_id: str, list_id: str, audit_data: Dict[str, Any]) -> str:
    """Write an audit run to AuditRuns2 and return item ID."""
    try:
        result = create_item(token, site_id, list_id, audit_data)
        return result.get('id', '')
    except requests.exceptions.HTTPError as e:
        print(f"❌ Error writing to list: {e.response.status_code} - {e.response.text}")
        raise


def main():
    print("🔌 Connecting to AuditRuns2 on BaseCampApps...\n")
    
    # Get token
    print("📝 Acquiring access token...")
    token = _get_app_only_token()
    if not token:
        print("❌ Failed to acquire access token.")
        print("   Make sure these are set in .env:")
        print("   - SHAREPOINT_TENANT_ID")
        print("   - SHAREPOINT_CLIENT_ID")
        print("   - MICROSOFT_PROVIDER_AUTHENTICATION_SECRET")
        sys.exit(1)
    print("✅ Token acquired\n")
    
    # Get site ID
    print(f"🔍 Resolving site ID...")
    try:
        site_id = get_site_id(token, SHAREPOINT_SITE_URL)
        print(f"✅ Site ID: {site_id}\n")
    except Exception as e:
        print(f"❌ Failed to resolve site: {e}")
        sys.exit(1)
    
    # Find list
    print(f"🔍 Finding list '{AUDITRUNS2_LIST_NAME}'...")
    try:
        list_info = find_list_by_name(token, site_id, AUDITRUNS2_LIST_NAME)
        if not list_info:
            print(f"❌ List '{AUDITRUNS2_LIST_NAME}' not found")
            sys.exit(1)
        
        list_id = list_info['id']
        print(f"✅ List found: {list_id}\n")
    except Exception as e:
        print(f"❌ Failed to find list: {e}")
        sys.exit(1)
    
    # Get schema
    print("📋 Fetching list schema...")
    try:
        schema = get_list_schema(token, site_id, list_id)
        print("✅ Available columns:")
        for col_name, col_info in sorted(schema.items()):
            required = " (required)" if col_info['required'] else ""
            print(f"   - {col_name}: {col_info['type']}{required}")
        print()
    except Exception as e:
        print(f"⚠️  Could not fetch schema: {e}\n")
        schema = {}
    
    # Example 1: Basic audit run with common fields
    print("="*70)
    print("EXAMPLE 1: Writing a basic audit run")
    print("="*70)
    
    basic_audit = {
        "Title": f"Audit Run {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "Status": "In Progress",
        "RunDate": datetime.now().isoformat(),
        "PropertyCount": 5,
        "FindingCount": 12,
    }
    
    print(f"\nData to write:")
    for key, value in basic_audit.items():
        print(f"  {key}: {value}")
    
    try:
        print("\n📤 Writing to list...")
        item_id = write_audit_run(token, site_id, list_id, basic_audit)
        print(f"✅ Successfully created item: {item_id}\n")
    except Exception as e:
        print(f"❌ Failed to write: {e}\n")
    
    # Example 2: More detailed audit run
    print("="*70)
    print("EXAMPLE 2: Writing a detailed audit run")
    print("="*70)
    
    detailed_audit = {
        "Title": f"Detailed Audit {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "Status": "Completed",
        "RunDate": datetime.now().isoformat(),
        "PropertyCount": 10,
        "FindingCount": 25,
        "ExceptionCount": 3,
        "Notes": "Full audit completed successfully",
    }
    
    print(f"\nData to write:")
    for key, value in detailed_audit.items():
        print(f"  {key}: {value}")
    
    try:
        print("\n📤 Writing to list...")
        item_id = write_audit_run(token, site_id, list_id, detailed_audit)
        print(f"✅ Successfully created item: {item_id}\n")
    except Exception as e:
        print(f"❌ Failed to write: {e}\n")
    
    print("="*70)
    print("✅ Write examples completed!")
    print("="*70)
    print("""
NEXT STEPS:

1. Review the examples above to understand the field structure

2. Use this in your code to write audit results programmatically:

   from query_auditruns2 import (
       get_site_id, find_list_by_name, create_item,
       SHAREPOINT_SITE_URL, AUDITRUNS2_LIST_NAME
   )
   from activity_logging.sharepoint import _get_app_only_token
   
   token = _get_app_only_token()
   site_id = get_site_id(token, SHAREPOINT_SITE_URL)
   list_info = find_list_by_name(token, site_id, AUDITRUNS2_LIST_NAME)
   list_id = list_info['id']
   
   audit_data = {
       "Title": "My Audit Run",
       "Status": "Completed",
       "PropertyCount": 42,
       # ... other fields
   }
   
   result = create_item(token, site_id, list_id, audit_data)
   print(f"Created item: {result['id']}")

3. View your list in SharePoint:
   https://peakcampus.sharepoint.com/sites/BaseCampApps/Lists/AuditRuns2/AllItems.aspx
""")


if __name__ == "__main__":
    main()
