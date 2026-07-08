"""
Quick check: Were snapshots written for the latest run?
"""
import os
import sys
import requests
from datetime import datetime

# Azure AD token acquisition
TENANT_ID = os.getenv('SHAREPOINT_TENANT_ID')
CLIENT_ID = os.getenv('SHAREPOINT_CLIENT_ID')
CLIENT_SECRET = os.getenv('MICROSOFT_PROVIDER_AUTHENTICATION_SECRET')
SITE_URL = 'https://peakcampus.sharepoint.com/sites/BaseCampApps'

def get_access_token():
    token_url = f'https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token'
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'scope': 'https://graph.microsoft.com/.default'
    }
    response = requests.post(token_url, data=token_data)
    response.raise_for_status()
    return response.json()['access_token']

def get_site_id(access_token):
    """Get the SharePoint site ID."""
    headers = {'Authorization': f'Bearer {access_token}'}
    site_parts = SITE_URL.replace('https://', '').split('/')
    hostname = site_parts[0]
    site_path = '/' + '/'.join(site_parts[1:])
    url = f'https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()['id']

def get_list_id(access_token, site_id, list_name):
    """Get the SharePoint list ID."""
    headers = {'Authorization': f'Bearer {access_token}'}
    url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists'
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    lists = response.json().get('value', [])
    for lst in lists:
        if lst['displayName'] == list_name:
            return lst['id']
    raise ValueError(f"List '{list_name}' not found")

def query_snapshots(access_token, site_id, list_id, run_id):
    """Query RunDisplaySnapshots for a specific run."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'
    }
    
    # Query for all snapshots for this run
    url = (
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items'
        f'?$filter=fields/RunId eq \'{run_id}\''
        f'&$select=id,fields&$expand=fields($select=RunId,ScopeType,PropertyId,LeaseIntervalId)'
        f'&$top=5000'
    )
    
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    items = response.json().get('value', [])
    
    # Group by scope type
    by_scope = {}
    for item in items:
        fields = item.get('fields', {})
        scope_type = fields.get('ScopeType', 'unknown')
        by_scope[scope_type] = by_scope.get(scope_type, 0) + 1
    
    return items, by_scope

if __name__ == '__main__':
    run_id = 'run_20260624_111417'  # Latest run from terminal
    
    print(f"\n🔍 Checking snapshots for run: {run_id}")
    print("=" * 70)
    
    try:
        token = get_access_token()
        print("✅ Got access token")
        
        site_id = get_site_id(token)
        print(f"✅ Got site ID: {site_id}")
        
        list_id = get_list_id(token, site_id, 'RunDisplaySnapshots')
        print(f"✅ Got list ID: {list_id}")
        
        items, by_scope = query_snapshots(token, site_id, list_id, run_id)
        
        print(f"\n📊 RESULTS:")
        print(f"   Total snapshots: {len(items)}")
        print(f"\n   By scope type:")
        for scope_type, count in sorted(by_scope.items()):
            print(f"      {scope_type}: {count}")
        
        if not items:
            print("\n❌ NO SNAPSHOTS FOUND!")
            print("   This explains why the property page shows 'No leases found'")
            print("   Background async write may still be in progress...")
        else:
            print(f"\n✅ Snapshots exist for this run")
            
            # Show a few lease examples
            lease_items = [item for item in items if item.get('fields', {}).get('ScopeType') == 'lease']
            if lease_items:
                print(f"\n   Sample lease IDs (first 5):")
                for item in lease_items[:5]:
                    fields = item.get('fields', {})
                    print(f"      LeaseIntervalId: {fields.get('LeaseIntervalId')}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
