import os, sys, requests
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, '.')
from activity_logging.sharepoint import _get_app_only_token

access_token = _get_app_only_token()
headers = {'Authorization': f'Bearer {access_token}'}
site_id = 'peakcampus.sharepoint.com,f83f37af-e64c-4dbc-9457-68c9484ee93b,f65502be-a283-4eae-a759-a23b23603fbb'

# Check AuditRuns
r = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists',
    headers=headers,
    params={'$filter': "displayName eq 'AuditRuns'"}
)
runs = r.json().get('value', [])
if runs:
    audit_runs_id = runs[0]['id']
    print(f'AuditRuns id: {audit_runs_id}')
    r2 = requests.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{audit_runs_id}/items',
        headers=headers,
        params={'$top': 2, '$expand': 'fields'}
    )
    for item in r2.json().get('value', []):
        f = item['fields']
        print(f"  RunId={f.get('RunId','')} ResultType={f.get('ResultType','')} PropertyId={f.get('PropertyId','')}")
else:
    print('AuditRuns not found')

# Also check AuditRuns2 with field_ names
audit_runs2_id = 'd8166180-5dcb-41a9-84c0-0ab104b77c27'
r3 = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{audit_runs2_id}/items',
    headers=headers,
    params={'$top': 3, '$expand': 'fields'}
)
print(f'\nAuditRuns2 items:')
for item in r3.json().get('value', []):
    f = item['fields']
    # field_1=RunId, field_2=ResultType, field_3=PropertyId
    print(f"  field_1(RunId)={f.get('field_1','')} field_2(ResultType)={f.get('field_2','')} created={item.get('createdDateTime','')}")
