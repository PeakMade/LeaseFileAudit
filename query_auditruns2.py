#!/usr/bin/env python3
"""
Connect to and query AuditRuns2 SharePoint list at:
https://peakcampus.sharepoint.com/sites/BaseCampApps/Lists/AuditRuns2/AllItems.aspx
"""

import sys
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

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


def get_list_items(token: str, site_id: str, list_id: str, limit: int = 100) -> tuple[List[Dict], int]:
    """Get items from AuditRuns2 list."""
    headers = {'Authorization': f'Bearer {token}'}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    params = {
        "$top": min(limit, 100),
        "$expand": "fields"
    }
    r = requests.get(url, headers=headers, params=params, timeout=30)
    if r.status_code == 400:
        # Some list schemas reject $expand=fields. Retry with a plain item request.
        params.pop("$expand", None)
        r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get('value', []), data.get('@odata.count', len(data.get('value', [])))


def get_list_column_maps(token: str, site_id: str, list_id: str) -> tuple[Dict[str, str], set[str]]:
    """Return display-name->internal-name map and internal-name set for a list."""
    headers = {'Authorization': f'Bearer {token}'}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns"
    params = {"$select": "name,displayName", "$top": 200}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()

    display_to_internal: Dict[str, str] = {}
    internal_names: set[str] = set()
    for column in r.json().get("value", []):
        internal_name = column.get("name")
        display_name = column.get("displayName")
        if internal_name:
            internal_names.add(internal_name)
        if internal_name and display_name:
            display_to_internal[display_name] = internal_name

    return display_to_internal, internal_names


def map_fields_to_internal_names(fields: Dict[str, Any], display_to_internal: Dict[str, str], internal_names: set[str]) -> Dict[str, Any]:
    """Map logical/display field names to the list's internal field names."""
    mapped: Dict[str, Any] = {}
    for field_name, value in fields.items():
        if field_name in internal_names:
            mapped[field_name] = value
            continue
        internal_name = display_to_internal.get(field_name)
        if internal_name:
            mapped[internal_name] = value
    return mapped


def query_list_items(token: str, site_id: str, list_id: str, filter_query: Optional[str] = None) -> List[Dict]:
    """Query AuditRuns2 with optional filter."""
    headers = {'Authorization': f'Bearer {token}'}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    params = {
        "$expand": "fields",
        "$top": 100
    }
    
    if filter_query:
        params["$filter"] = filter_query
    
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    
    return r.json().get('value', [])


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


def main():
    print("🔌 Connecting to AuditRuns2 on BaseCampApps...")
    print(f"   URL: {SHAREPOINT_SITE_URL}\n")
    
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
    print(f"🔍 Resolving site ID for {SHAREPOINT_SITE_URL}...")
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
        print(f"✅ List found!")
        print(f"   ID: {list_id}")
        print(f"   Name: {list_info.get('displayName', 'N/A')}\n")
    except Exception as e:
        print(f"❌ Failed to find list: {e}")
        sys.exit(1)
    
    # Load schema so writes can use internal field names
    print(f"📐 Resolving {AUDITRUNS2_LIST_NAME} schema...")
    try:
        display_to_internal, internal_names = get_list_column_maps(token, site_id, list_id)
        print(f"✅ Loaded {len(internal_names)} columns")
    except Exception as e:
        print(f"❌ Failed to load list schema: {e}")
        sys.exit(1)

    # Direct write test: create a new item in AuditRuns2
    print(f"📝 Attempting to create a test item in {AUDITRUNS2_LIST_NAME}...")
    test_fields = {
        "Title": "Copilot Write Test",
        "RunId": f"copilot-test-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "ResultType": "test",
        "Status": "Test",
        "AuditMonth": "2026-05",
        "Variance": "0",
        "ExpectedTotal": "0",
        "ActualTotal": "0"
    }
    mapped_fields = map_fields_to_internal_names(test_fields, display_to_internal, internal_names)
    if "Title" not in mapped_fields:
        mapped_fields["Title"] = test_fields["Title"]

    try:
        result = create_item(token, site_id, list_id, mapped_fields)
        print("✅ Successfully wrote test item to AuditRuns2!")
        print(f"   Item ID: {result.get('id')}")
        print(f"   Sent fields: {sorted(mapped_fields.keys())}")

        items, _ = get_list_items(token, site_id, list_id, limit=5)
        print(f"✅ Read test succeeded: retrieved {len(items)} recent item(s)")
    except Exception as e:
        print(f"❌ Failed to write test item: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
