#!/usr/bin/env python3
"""
Write sample audit findings to AuditRuns2 using the correct field mappings.
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import uuid

sys.path.insert(0, str(Path(__file__).parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from activity_logging.sharepoint import _get_app_only_token
import requests


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
    print("🔌 Writing to AuditRuns2...\n")
    
    # Get token
    print("📝 Acquiring access token...")
    token = _get_app_only_token()
    if not token:
        print("❌ Failed to acquire access token.")
        sys.exit(1)
    print("✅ Token acquired\n")
    
    # Get site ID
    print(f"🔍 Resolving site...")
    site_id = get_site_id(token, SHAREPOINT_SITE_URL)
    
    # Find list
    print(f"🔍 Finding list...")
    list_info = find_list_by_name(token, site_id, AUDITRUNS2_LIST_NAME)
    if not list_info:
        print(f"❌ List not found")
        sys.exit(1)
    
    list_id = list_info['id']
    print(f"✅ Connected\n")
    
    # Example audit findings
    findings = [
        {
            "Title": "Rent Charge Discrepancy - Unit 101",
            "field_1": str(uuid.uuid4()),  # RunId
            "field_2": "AMOUNT_MISMATCH",  # ResultType
            "field_3": "PROP001",  # PropertyId
            "field_6": "2026-05-15",  # AuditMonth
            "field_7": "OPEN",  # Status
            "field_8": "HIGH",  # Severity
            "field_9": "Rent amount discrepancy",  # FindingTitle
            "field_11": "2500.00",  # ExpectedTotal
            "field_12": "2400.00",  # ActualTotal
            "field_13": "100.00",  # ImpactAmount
            "field_17": "Billing",  # Category
            "field_18": "Scheduled rent charge does not match billed amount",  # Description
            "field_21": "Property Name: Summit Apartments",  # PropertyName
            "field_22": "Unit 101 - John Smith",  # ResidentName
        },
        {
            "Title": "Late Fee Not Applied - Unit 204",
            "field_1": str(uuid.uuid4()),  # RunId
            "field_2": "MISSING_BILLINGS",  # ResultType
            "field_3": "PROP001",  # PropertyId
            "field_6": "2026-05-15",  # AuditMonth
            "field_7": "OPEN",  # Status
            "field_8": "MEDIUM",  # Severity
            "field_9": "Missing late fee charge",  # FindingTitle
            "field_11": "125.00",  # ExpectedTotal
            "field_12": "0.00",  # ActualTotal
            "field_13": "125.00",  # ImpactAmount
            "field_17": "Charges",  # Category
            "field_18": "Late fee charge was scheduled but not billed to resident",  # Description
            "field_21": "Property Name: Summit Apartments",  # PropertyName
            "field_22": "Unit 204 - Jane Doe",  # ResidentName
        },
        {
            "Title": "Utility Charge Reconciliation Complete",
            "field_1": str(uuid.uuid4()),  # RunId
            "field_2": "MATCHED",  # ResultType
            "field_3": "PROP002",  # PropertyId
            "field_6": "2026-05-15",  # AuditMonth
            "field_7": "CLOSED",  # Status
            "field_8": "INFO",  # Severity
            "field_9": "Utility charges match",  # FindingTitle
            "field_11": "350.00",  # ExpectedTotal
            "field_12": "350.00",  # ActualTotal
            "field_13": "0.00",  # ImpactAmount
            "field_17": "Utilities",  # Category
            "field_18": "All utility charges have been properly reconciled",  # Description
            "field_21": "Property Name: Meadowbrook Complex",  # PropertyName
            "field_22": "Unit 50 - Robert Jones",  # ResidentName
        },
    ]
    
    print("📤 Writing findings to AuditRuns2...\n")
    created_items = []
    
    for i, finding in enumerate(findings, 1):
        try:
            print(f"  [{i}/{len(findings)}] {finding['Title']}...")
            result = create_item(token, site_id, list_id, finding)
            item_id = result.get('id')
            created_items.append(item_id)
            print(f"      ✅ Created (ID: {item_id})")
        except Exception as e:
            print(f"      ❌ Failed: {e}")
    
    print(f"\n{'='*70}")
    print(f"✅ Successfully wrote {len(created_items)} findings to AuditRuns2")
    print(f"{'='*70}")
    print(f"\nItem IDs: {', '.join(created_items)}")
    print(f"\nView in SharePoint:")
    print(f"https://peakcampus.sharepoint.com/sites/BaseCampApps/Lists/AuditRuns2/AllItems.aspx")


if __name__ == "__main__":
    main()
