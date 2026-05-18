"""Test writing a single item to AuditRuns2 with field data."""
import os, sys, requests, json
from dotenv import load_dotenv
load_dotenv()
sys.path.insert(0, '.')
from activity_logging.sharepoint import _get_app_only_token

access_token = _get_app_only_token()
headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json',
}
site_id = 'peakcampus.sharepoint.com,f83f37af-e64c-4dbc-9457-68c9484ee93b,f65502be-a283-4eae-a759-a23b23603fbb'
list_id = 'd8166180-5dcb-41a9-84c0-0ab104b77c27'

# Try writing with field_* names (internal names)
payload = {
    'fields': {
        'Title': 'TEST_ITEM',
        'field_1': 'run_TEST_DEBUG',
        'field_2': 'bucket_result',
        'field_3': '9999999',
        'field_4': '12345',
        'field_5': '154771',
        'field_6': '2026-01',
        'field_7': 'OK',
        'field_10': '0.0',
        'field_11': '100.0',
        'field_12': '100.0',
    }
}

url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items'
resp = requests.post(url, headers=headers, json=payload)
print(f'Status: {resp.status_code}')
print(f'Response: {resp.text[:500]}')

if resp.status_code in [200, 201]:
    created = resp.json()
    fields = created.get('fields', {})
    print(f'\nCreated item:')
    print(f'  field_1 (RunId): {fields.get("field_1", "")}')
    print(f'  field_2 (ResultType): {fields.get("field_2", "")}')
    print(f'  Title: {fields.get("Title", "")}')
    item_id = created.get('id')
    
    # Clean up - delete the test item
    del_resp = requests.delete(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}',
        headers=headers
    )
    print(f'\nCleanup (delete test item): {del_resp.status_code}')
