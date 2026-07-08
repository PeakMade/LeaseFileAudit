"""
Verify AuditRuns2 connection by checking for test data
"""
import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv(Path('.') / '.env')

import requests
from activity_logging.sharepoint import _get_app_only_token

token = _get_app_only_token()
site_url = 'peakcampus.sharepoint.com:/sites/BaseCampApps'
list_id = 'd8166180-5dcb-41a9-84c0-0ab104b77c27'

# Get site ID
site_resp = requests.get(f'https://graph.microsoft.com/v1.0/sites/{site_url}', headers={'Authorization': f'Bearer {token}'})
site_id = site_resp.json()['id']

# Query for our test entry
list_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items'
resp = requests.get(f'{list_url}?$top=10&$expand=fields&$filter=fields/RunID eq \'direct_test_20260625_888888\'', headers={'Authorization': f'Bearer {token}'})

items = resp.json().get('value', [])
if items:
    print(f'\n[OK] AuditRuns2 IS CONNECTED!')
    print(f'Found {len(items)} test entry(ies) from our direct write test:\n')
    for item in items:
        created = item.get('createdDateTime', '')
        fields = item.get('fields', {})
        print(f'  RunID: {fields.get("RunID", "N/A")}')
        print(f'  PropertyName: {fields.get("PropertyName", "N/A")}')
        print(f'  BucketName: {fields.get("BucketName", "N/A")}')
        print(f'  Status: {fields.get("Status", "N/A")}')
        print(f'  Created: {created}')
        print()
    print('[SUCCESS] Writes to AuditRuns2 are working!')
else:
    print('\n[ERROR] Test entry NOT found in AuditRuns2')
    print('This suggests writes are still failing')
