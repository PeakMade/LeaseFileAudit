"""
Sends one test row to AuditRuns2 and prints the full request payload + response body.
This reveals exactly why 500 generalException errors occur.
"""
import json
import requests
from dotenv import load_dotenv

load_dotenv()

import config as _cfg
from activity_logging.sharepoint import _get_app_only_token


def main() -> None:
    site = _cfg.config.auth.sharepoint_site_url
    token = _get_app_only_token()

    host = site.split('/')[2]
    path = '/'.join(site.split('/')[3:])

    s = requests.Session()
    s.headers.update({'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'})

    site_resp = s.get(f'https://graph.microsoft.com/v1.0/sites/{host}:/{path}', timeout=30)
    site_id = site_resp.json()['id']

    lists_resp = s.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists',
        params={'$filter': "displayName eq 'AuditRuns2'"},
        timeout=30,
    )
    list_id = lists_resp.json()['value'][0]['id']

    # Fetch column internal → display mapping
    cols_resp = s.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns',
        params={'$select': 'name,displayName', '$top': 200},
        timeout=30,
    )
    columns = cols_resp.json().get('value', [])
    print("=== AuditRuns2 columns (internal_name -> displayName) ===")
    display_to_internal = {}
    for col in columns:
        iname = col.get('name', '')
        dname = col.get('displayName', '')
        print(f"  {iname:30s} -> {dname}")
        if iname and dname:
            display_to_internal[dname] = iname

    # Build mapping using the SAME logic as service.py:
    # prefer internal_name == logical_name, then fall back to display_name lookup
    column_names = {col.get('name') for col in columns if col.get('name')}
    logical_to_internal = {}
    for logical in ['Title', 'RunId', 'ResultType', 'PropertyId', 'LeaseIntervalId',
                    'ArCodeId', 'AuditMonth', 'Status', 'Severity', 'FindingTitle',
                    'Variance', 'ExpectedTotal', 'ActualTotal', 'ImpactAmount',
                    'MatchRule', 'FindingId', 'Category', 'Description',
                    'ExpectedValue', 'ActualValue', 'CreatedAt', 'PropertyName', 'ResidentName']:
        if logical in column_names:
            logical_to_internal[logical] = logical  # internal name matches logical name
        elif logical in display_to_internal:
            logical_to_internal[logical] = display_to_internal[logical]

    print("\n=== Logical -> Internal mapping resolved ===")

    for k, v in logical_to_internal.items():
        print(f"  {k:20s} -> {v}")

    # Build payload with internal names
    fields_payload = {}
    for logical, value in {
        'Title': 'TEST:0',
        'RunId': 'run_TEST_DEBUG',
        'ResultType': 'bucket_result',
        'PropertyId': '12345',
        'LeaseIntervalId': '99999',
        'ArCodeId': 'RENT',
        'AuditMonth': '2026-01',
        'Status': 'ok',
        'Severity': 'low',
        'FindingTitle': 'Test Finding',
        'Variance': '0.0',
        'ExpectedTotal': '100.0',
        'ActualTotal': '100.0',
        'ImpactAmount': '0.0',
        'MatchRule': 'exact',
        'FindingId': 'f1',
        'Category': 'rent',
        'Description': 'debug test row',
        'ExpectedValue': '100',
        'ActualValue': '100',
        'CreatedAt': '2026-05-12T00:00:00',
    }.items():
        internal = logical_to_internal.get(logical)
        if internal:
            fields_payload[internal] = value

    body = {'fields': fields_payload}
    print("\n=== Sending payload ===")
    print(json.dumps(body, indent=2))

    items_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items'
    resp = s.post(items_url, json=body, timeout=30)
    print(f"\n=== Response: {resp.status_code} ===")
    try:
        print(json.dumps(resp.json(), indent=2))
    except Exception:
        print(resp.text)


if __name__ == '__main__':
    main()
