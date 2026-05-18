#!/usr/bin/env python3
"""
Test connection to AuditRuns2 SharePoint list.
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import config as cfg
from activity_logging.sharepoint import _get_app_only_token
import requests


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


def find_list_by_name(token: str, site_id: str, list_name: str) -> dict:
    """Find SharePoint list by display name."""
    headers = {'Authorization': f'Bearer {token}'}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    params = {"$filter": f"displayName eq '{list_name}'"}
    
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    
    items = r.json().get('value', [])
    if items:
        return items[0]
    return None


def get_list_items(token: str, site_id: str, list_id: str, limit: int = 5) -> list:
    """Get items from AuditRuns2 list."""
    headers = {'Authorization': f'Bearer {token}'}
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    params = {"$top": limit}
    
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    
    return r.json().get('value', [])


def main():
    app_config = cfg.config
    site_url = app_config.auth.sharepoint_site_url
    list_name = app_config.auth.audit_results_list_name
    
    if not site_url:
        print("❌ ERROR: sharepoint_site_url not configured in .env")
        sys.exit(1)
    
    if not list_name:
        print("❌ ERROR: audit_results_list_name not configured in .env")
        sys.exit(1)
    
    print(f"🔌 Connecting to SharePoint...")
    print(f"   Site URL: {site_url}")
    print(f"   List Name: {list_name}\n")
    
    # Get token
    print("📝 Acquiring access token...")
    token = _get_app_only_token()
    if not token:
        print("❌ Failed to acquire access token. Check credentials in .env")
        sys.exit(1)
    print("✅ Token acquired\n")
    
    # Get site ID
    print("🔍 Resolving site ID...")
    try:
        site_id = get_site_id(token, site_url)
        print(f"✅ Site ID: {site_id}\n")
    except Exception as e:
        print(f"❌ Failed to resolve site: {e}")
        sys.exit(1)
    
    # Find list
    print(f"🔍 Finding list '{list_name}'...")
    try:
        list_info = find_list_by_name(token, site_id, list_name)
        if not list_info:
            print(f"❌ List '{list_name}' not found")
            sys.exit(1)
        
        list_id = list_info['id']
        print(f"✅ List found!")
        print(f"   ID: {list_id}")
        print(f"   Name: {list_info.get('displayName', 'N/A')}\n")
    except Exception as e:
        print(f"❌ Failed to find list: {e}")
        sys.exit(1)
    
    # Get sample items
    print(f"📊 Fetching sample items from {list_name}...")
    try:
        items = get_list_items(token, site_id, list_id, limit=5)
        print(f"✅ Retrieved {len(items)} items\n")
        
        if items:
            print("📋 Sample items:")
            for i, item in enumerate(items, 1):
                print(f"\n   Item {i}:")
                fields = item.get('fields', {})
                for key, value in fields.items():
                    if value is not None and value != '':
                        print(f"      {key}: {value}")
        else:
            print("   (No items in list yet)")
    except Exception as e:
        print(f"❌ Failed to fetch items: {e}")
        sys.exit(1)
    
    print("\n✅ Connection successful!")


if __name__ == "__main__":
    main()
