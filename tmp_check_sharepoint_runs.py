"""Quick check: list recent run folders in SharePoint Drive."""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

tenant_id = os.getenv('SHAREPOINT_TENANT_ID')
client_id = os.getenv('SHAREPOINT_CLIENT_ID')
client_secret = os.getenv('MICROSOFT_PROVIDER_AUTHENTICATION_SECRET')
site_url = os.getenv('SHAREPOINT_SITE_URL', 'https://peakcampus.sharepoint.com/sites/BaseCampApps')

# Get token
token_url = f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token'
resp = requests.post(token_url, data={
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'scope': 'https://graph.microsoft.com/.default'
})
resp.raise_for_status()
token = resp.json().get('access_token')
print('Token obtained:', bool(token))

headers = {'Authorization': f'Bearer {token}'}

# Get site
site_resp = requests.get(
    'https://graph.microsoft.com/v1.0/sites/peakcampus.sharepoint.com:/sites/BaseCampApps',
    headers=headers
)
site = site_resp.json()
site_id = site['id']
print('Site ID:', site_id[:30] + '...')

# List drives
drives_resp = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/drives',
    headers=headers
)
drives = drives_resp.json().get('value', [])
print(f'Found {len(drives)} drives:')
for d in drives:
    print(f'  {d["name"]} ({d["id"][:20]}...)')

# Find the Audit Results or Documents drive
audit_drive = None
for d in drives:
    if 'audit' in d['name'].lower() or 'document' in d['name'].lower():
        audit_drive = d
        print(f'Using drive: {d["name"]}')
        break

if not audit_drive and drives:
    audit_drive = drives[0]
    print(f'Using first drive: {audit_drive["name"]}')

drive_id = audit_drive['id']

# List root children to find audit run folder
root_resp = requests.get(
    f'https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children',
    headers=headers
)
root_items = root_resp.json().get('value', [])
print(f'\nRoot items ({len(root_items)}):')
for item in root_items[:20]:
    print(f'  {item["name"]}')

# Look for a "runs" or "audit" subfolder
for item in root_items:
    if 'run' in item['name'].lower() or 'audit' in item['name'].lower():
        print(f'\nChecking subfolder: {item["name"]}')
        sub_resp = requests.get(
            f'https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item["id"]}/children?$orderby=name desc&$top=10',
            headers=headers
        )
        sub_items = sub_resp.json().get('value', [])
        print(f'  Recent items ({len(sub_items)}):')
        for si in sub_items:
            print(f'    {si["name"]} (modified: {si.get("lastModifiedDateTime", "?")})')
