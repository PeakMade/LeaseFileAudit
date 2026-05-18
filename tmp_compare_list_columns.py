import requests
import sys

sys.path.insert(0, '.')
from dotenv import load_dotenv

load_dotenv()

import config as _cfg
from activity_logging.sharepoint import _get_app_only_token

CORE = {
    'RunId', 'ResultType', 'PropertyId', 'LeaseIntervalId', 'ArCodeId',
    'AuditMonth', 'Status', 'Variance', 'ExpectedTotal', 'ActualTotal',
    'PropertyName', 'ResidentName',
}


def get_list_id(session, site_id, name):
    r = session.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists',
        params={'$filter': f"displayName eq '{name}'"},
        timeout=30,
    )
    r.raise_for_status()
    vals = r.json().get('value', [])
    if not vals:
        raise RuntimeError(f'List not found: {name}')
    return vals[0]['id']


def get_columns(session, site_id, list_id):
    r = session.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns',
        params={'$select': 'name,displayName', '$top': 300},
        timeout=30,
    )
    r.raise_for_status()
    return [(c.get('name'), c.get('displayName')) for c in r.json().get('value', []) if c.get('name')]


def main():
    site = _cfg.config.auth.sharepoint_site_url
    token = _get_app_only_token()

    host = site.split('/')[2]
    path = '/'.join(site.split('/')[3:])

    s = requests.Session()
    s.headers.update({'Authorization': f'Bearer {token}'})

    site_id = s.get(f'https://graph.microsoft.com/v1.0/sites/{host}:/{path}', timeout=30).json()['id']

    for list_name in ['AuditRuns', 'AuditRuns2']:
        list_id = get_list_id(s, site_id, list_name)
        columns = get_columns(s, site_id, list_id)
        print(f'\n{list_name} id={list_id} total_columns={len(columns)}')
        core_rows = [
            (internal, display)
            for internal, display in columns
            if internal in CORE or display in CORE
        ]
        for internal, display in sorted(core_rows):
            print(f'  {internal} -> {display}')


if __name__ == '__main__':
    main()
