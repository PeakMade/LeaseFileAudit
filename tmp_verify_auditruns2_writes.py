import requests
from dotenv import load_dotenv

load_dotenv()

import config as _cfg
from activity_logging.sharepoint import _get_app_only_token

TARGET_RUN = "run_20260512_131836"


def main() -> None:
    site = _cfg.config.auth.sharepoint_site_url
    token = _get_app_only_token()

    host = site.split('/')[2]
    path = '/'.join(site.split('/')[3:])

    s = requests.Session()
    s.headers.update({'Authorization': f'Bearer {token}'})

    site_id = s.get(f'https://graph.microsoft.com/v1.0/sites/{host}:/{path}', timeout=30).json()['id']
    list_id = s.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists',
        params={'$filter': "displayName eq 'AuditRuns2'"},
        timeout=30,
    ).json()['value'][0]['id']

    resp = s.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items',
        params={'$top': 200, '$expand': 'fields'},
        timeout=30,
    )
    items = resp.json().get('value', [])

    run_rows = []
    target_rows = []
    for item in items:
        f = item.get('fields', {})
        run_val = f.get('field_1') or f.get('RunId') or ''
        result_type = f.get('field_2') or f.get('ResultType') or ''
        if run_val:
            run_rows.append((item.get('id'), run_val, result_type, f.get('Created', '')))
        if run_val == TARGET_RUN:
            target_rows.append((item.get('id'), run_val, result_type, f.get('Created', '')))

    print(f"total_items_scanned={len(items)}")
    print(f"items_with_nonempty_runid={len(run_rows)}")
    print(f"items_for_target_run={len(target_rows)}")
    if run_rows:
        print("sample_nonempty_run_rows:")
        for row in run_rows[:10]:
            print(row)


if __name__ == '__main__':
    main()
