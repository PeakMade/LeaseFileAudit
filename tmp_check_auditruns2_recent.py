import requests
from dotenv import load_dotenv

load_dotenv()

import config as _cfg
from activity_logging.sharepoint import _get_app_only_token


def main() -> None:
    site = _cfg.config.auth.sharepoint_site_url
    token = _get_app_only_token()
    if not site or not token:
        print("Missing site or token")
        return

    host = site.split('/')[2]
    path = '/'.join(site.split('/')[3:])

    s = requests.Session()
    s.headers.update({'Authorization': f'Bearer {token}'})

    site_id = s.get(f'https://graph.microsoft.com/v1.0/sites/{host}:/{path}', timeout=30).json()['id']
    list_items = s.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists',
        params={'$filter': "displayName eq 'AuditRuns2'"},
        timeout=30,
    ).json().get('value', [])
    if not list_items:
        print('AuditRuns2 not found')
        return

    list_id = list_items[0]['id']
    resp = s.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items',
        params={
            '$top': 20,
            '$expand': 'fields($select=Title,field_1,field_2,field_6,Created)',
        },
        timeout=30,
    )
    data = resp.json().get('value', [])
    print(f'rows_returned={len(data)}')

    for item in data[:20]:
        f = item.get('fields', {})
        print(
            f"{f.get('Created','')} | run={f.get('field_1','')} | type={f.get('field_2','')} | month={f.get('field_6','')} | title={f.get('Title','')}"
        )


if __name__ == '__main__':
    main()
