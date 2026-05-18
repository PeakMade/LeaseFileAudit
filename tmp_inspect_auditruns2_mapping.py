import requests
from dotenv import load_dotenv

load_dotenv()

import config as _cfg
from activity_logging.sharepoint import _get_app_only_token

TARGET = {
    "RunId", "ResultType", "PropertyId", "LeaseIntervalId", "ArCodeId", "AuditMonth",
    "Status", "Severity", "FindingTitle", "Variance", "ExpectedTotal", "ActualTotal",
    "ImpactAmount", "MatchRule", "FindingId", "Category", "Description", "ExpectedValue",
    "ActualValue", "CreatedAt", "PropertyName", "ResidentName"
}


def main() -> None:
    site = _cfg.config.auth.sharepoint_site_url
    token = _get_app_only_token()
    host = site.split('/')[2]
    path = '/'.join(site.split('/')[3:])

    sess = requests.Session()
    sess.headers.update({'Authorization': f'Bearer {token}'})

    site_id = sess.get(f'https://graph.microsoft.com/v1.0/sites/{host}:/{path}', timeout=30).json()['id']
    lists = sess.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists',
        params={'$filter': "displayName eq 'AuditRuns2'"},
        timeout=30,
    ).json()['value']
    list_id = lists[0]['id']

    cols = sess.get(
        f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns',
        params={'$select': 'name,displayName', '$top': 200},
        timeout=30,
    ).json().get('value', [])

    print(f'AuditRuns2 id={list_id}')
    for c in cols:
        name = c.get('name')
        display = c.get('displayName')
        if display in TARGET or name in TARGET:
            print(f"internal={name} | display={display}")


if __name__ == '__main__':
    main()
