"""Check what fields exist in RunDisplaySnapshots SharePoint list."""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

# SharePoint configuration
SHAREPOINT_SITE_URL = os.getenv('SHAREPOINT_SITE_URL')
SHAREPOINT_CLIENT_ID = os.getenv('SHAREPOINT_CLIENT_ID')
SHAREPOINT_TENANT_ID = os.getenv('SHAREPOINT_TENANT_ID')
SHAREPOINT_CLIENT_SECRET = os.getenv('MICROSOFT_PROVIDER_AUTHENTICATION_SECRET')

def get_access_token():
    """Get SharePoint access token."""
    token_url = f"https://login.microsoftonline.com/{SHAREPOINT_TENANT_ID}/oauth2/v2.0/token"
    token_data = {
        'grant_type': 'client_credentials',
        'client_id': SHAREPOINT_CLIENT_ID,
        'client_secret': SHAREPOINT_CLIENT_SECRET,
        'scope': 'https://graph.microsoft.com/.default'
    }
    response = requests.post(token_url, data=token_data, timeout=30)
    response.raise_for_status()
    return response.json()['access_token']

def get_site_id(token, site_url):
    """Get SharePoint site ID."""
    # Extract hostname and site path
    # e.g., https://tenant.sharepoint.com/sites/sitename
    parts = site_url.replace('https://', '').split('/')
    hostname = parts[0]
    site_path = '/'.join(parts[1:])
    
    url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{site_path}"
    headers = {'Authorization': f'Bearer {token}'}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()['id']

def get_list_id(token, site_id, list_name):
    """Get SharePoint list ID by name."""
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
    headers = {'Authorization': f'Bearer {token}'}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    
    lists = resp.json().get('value', [])
    for lst in lists:
        if lst.get('displayName') == list_name or lst.get('name') == list_name:
            return lst['id']
    return None

def main():
    print("🔍 CHECKING RUNDISPLAYSNAPSHOTS SCHEMA")
    print("=" * 80)
    
    # Get access token
    print("\n1. Getting access token...")
    token = get_access_token()
    print("   ✓ Token acquired")
    
    # Get site ID
    print(f"\n2. Resolving site ID for: {SHAREPOINT_SITE_URL}")
    site_id = get_site_id(token, SHAREPOINT_SITE_URL)
    print(f"   ✓ Site ID: {site_id}")
    
    # Get list ID
    print("\n3. Finding RunDisplaySnapshots list...")
    list_id = get_list_id(token, site_id, 'RunDisplaySnapshots')
    if not list_id:
        print("   ❌ RunDisplaySnapshots list not found!")
        return
    print(f"   ✓ List ID: {list_id}")
    
    # Get list columns
    print("\n4. Fetching list columns...")
    url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns'
    headers = {'Authorization': f'Bearer {token}'}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    
    columns = resp.json().get('value', [])
    
    # Filter to relevant columns
    system_fields = {
        'ContentType', 'Modified', 'Created', 'Author', 'Editor', '_UIVersionString',
        'Attachments', 'Edit', 'LinkTitleNoMenu', 'LinkTitle', 'DocIcon', 'ItemChildCount',
        'FolderChildCount', '_ComplianceFlags', '_ComplianceTag', '_ComplianceTagWrittenTime',
        '_ComplianceTagUserId', 'AppAuthor', 'AppEditor', '_UIVersion', 'FileLeafRef',
        'FileDirRef', 'Last_x0020_Modified', 'Created_x0020_Date', 'FSObjType',
        'PermMask', 'PrincipalCount', 'CheckedOutUserId', 'IsCheckedoutToLocal',
        'owshiddenversion', 'WorkflowVersion', '_Level', '_IsCurrentVersion',
        'GUID', 'FileSizeDisplay', 'SelectTitle', 'SelectFilename', 'Edit', 'Type',
        'Compliance Asset Id'
    }
    
    custom_cols = [c for c in columns if c.get('name') not in system_fields and not c.get('hidden', False)]
    
    print(f"\n📋 RunDisplaySnapshots has {len(custom_cols)} custom columns:\n")
    print(f"{'Field Name':<35} {'Internal Name':<35} {'Type':<15}")
    print("-" * 85)
    
    for col in sorted(custom_cols, key=lambda x: x.get('displayName', '')):
        name = col.get('name', '')
        display_name = col.get('displayName', '')
        
        # Determine type
        if 'text' in col:
            field_type = 'Text'
        elif 'number' in col:
            field_type = 'Number'
        elif 'dateTime' in col:
            field_type = 'DateTime'
        elif 'choice' in col:
            choices = col.get('choice', {}).get('choices', [])
            field_type = f'Choice({len(choices)})'
        elif 'boolean' in col:
            field_type = 'Boolean'
        elif 'lookup' in col:
            field_type = 'Lookup'
        else:
            field_type = 'Other'
        
        print(f"{display_name:<35} {name:<35} {field_type:<15}")
    
    print("\n" + "=" * 80)
    print("✓ Schema check complete")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
