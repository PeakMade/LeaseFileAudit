import os, sys, requests
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, '.')

from config import config
from storage.service import StorageService
from activity_logging.sharepoint import _get_app_only_token

access_token = _get_app_only_token()

svc = StorageService(
    base_dir=config.storage.base_dir,
    use_sharepoint=True,
    sharepoint_site_url=config.auth.sharepoint_site_url,
    library_name=config.storage.sharepoint_library_name,
    access_token=access_token,
    audit_results_list_name=config.auth.audit_results_list_name,
)
if not svc.access_token:
    print('ERROR: No access token')
    sys.exit(1)

site_id = svc._get_site_id()
print(f'site_id: {site_id}')

list_id = svc._get_sharepoint_list_id('AuditRuns2')
print(f'AuditRuns2 list_id: {list_id}')

if not list_id:
    print('ERROR: AuditRuns2 not found in SharePoint')
    sys.exit(1)

headers = {'Authorization': f'Bearer {svc.access_token}'}

# Get columns
col_resp = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns',
    headers=headers,
    params={'$select': 'name,displayName', '$top': 50}
)
print(f'Column status: {col_resp.status_code}')
cols = col_resp.json().get('value', [])
print('Columns (internal -> display):')
for c in cols:
    print(f'  {c["name"]} -> {c["displayName"]}')

required = {'RunId', 'ResultType', 'PropertyId', 'LeaseIntervalId', 'ArCodeId', 'AuditMonth', 'Status', 'Variance', 'ExpectedTotal', 'ActualTotal'}
internal_names = {c['name'] for c in cols}
display_to_internal = {c['displayName']: c['name'] for c in cols}
missing = []
for r in required:
    if r in internal_names:
        print(f'  FOUND (internal): {r}')
    elif r in display_to_internal:
        print(f'  FOUND (display->internal): {r} -> {display_to_internal[r]}')
    else:
        print(f'  MISSING: {r}')
        missing.append(r)

if missing:
    print(f'\nMISSING REQUIRED COLUMNS: {missing}')
else:
    print('\nAll required columns present')

# Get item count
items_resp = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items',
    headers=headers,
    params={'$top': 5, '$expand': 'fields'}
)
items = items_resp.json().get('value', [])
print(f'\nItem sample (top {len(items)}):')
for item in items[:3]:
    f = item['fields']
    print(f'  RunId={f.get("RunId","")} ResultType={f.get("ResultType","")} PropertyId={f.get("PropertyId","")}')
