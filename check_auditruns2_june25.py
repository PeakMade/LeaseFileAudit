"""
Check AuditRuns2 SharePoint list for any entries from June 25, 2026
"""
import os
import sys
import requests
from datetime import datetime
from pathlib import Path

# Load .env file
from dotenv import load_dotenv
env_path = Path(__file__).parent / '.env'
load_dotenv(env_path)

def get_app_only_token():
    """Get Microsoft Graph token"""
    tenant_id = os.getenv('SHAREPOINT_TENANT_ID')
    client_id = os.getenv('SHAREPOINT_CLIENT_ID')
    client_secret = os.getenv('MICROSOFT_PROVIDER_AUTHENTICATION_SECRET')
    
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scope': 'https://graph.microsoft.com/.default'
    }
    
    response = requests.post(token_url, data=token_data)
    response.raise_for_status()
    return response.json()['access_token']

def query_auditruns2():
    """Query AuditRuns2 for June 25 entries"""
    token = get_app_only_token()
    
    site_url = "peakcampus.sharepoint.com:/sites/BaseCampApps"
    list_id = "d8166180-5dcb-41a9-84c0-0ab104b77c27"  # AuditRuns2
    
    # Get site ID
    site_response = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_url}",
        headers={'Authorization': f'Bearer {token}'}
    )
    site_response.raise_for_status()
    site_id = site_response.json()['id']
    
    print(f"✓ Site ID: {site_id}")
    
    # Query list items with filter for June 25
    list_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    
    # First get all items to see what we have
    response = requests.get(
        f"{list_url}?$top=100&$expand=fields",
        headers={'Authorization': f'Bearer {token}'}
    )
    response.raise_for_status()
    data = response.json()
    
    print(f"\n✓ Total items returned: {len(data.get('value', []))}")
    
    # Check dates
    june_25_items = []
    for item in data.get('value', []):
        created = item.get('createdDateTime', '')
        fields = item.get('fields', {})
        run_id = fields.get('RunID', 'NO_RUN_ID')
        
        if created.startswith('2026-06-25'):
            june_25_items.append({
                'run_id': run_id,
                'created': created,
                'item_id': item['id']
            })
            print(f"  📌 FOUND June 25 item: {run_id} (created {created})")
    
    if not june_25_items:
        print("\n❌ NO June 25, 2026 entries found in AuditRuns2")
        print("\nMost recent entries:")
        for item in data.get('value', [])[:5]:
            created = item.get('createdDateTime', '')
            fields = item.get('fields', {})
            run_id = fields.get('RunID', 'NO_RUN_ID')
            print(f"  - {run_id} (created {created})")
    else:
        print(f"\n✅ Found {len(june_25_items)} June 25 entries!")
    
    return june_25_items

if __name__ == '__main__':
    try:
        june_25_items = query_auditruns2()
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
