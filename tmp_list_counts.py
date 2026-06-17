import sys; sys.path.insert(0, '.'); sys.stdout.reconfigure(line_buffering=True)
from dotenv import load_dotenv; load_dotenv()
import config as _cfg
from activity_logging.sharepoint import _get_app_only_token
import requests

token = _get_app_only_token()
site_url = _cfg.config.auth.sharepoint_site_url
hostname = site_url.split('/')[2]
path = '/'.join(site_url.split('/')[3:])
h = {'Authorization': 'Bearer ' + token}

r = requests.get('https://graph.microsoft.com/v1.0/sites/' + hostname + ':/' + path, headers=h, timeout=30)
site_id = r.json()['id']

LISTS = [
    'AuditRuns2', 'RunDisplaySnapshots', 'LeaseTermSet', 'LeaseTerms',
    'LeaseTermEvidence', 'ExceptionMonths', 'Audit Run Metrics',
    'LeaseFileAudit Runs', 'AR Sessions', 'Innovation Use Log'
]

r2 = requests.get('https://graph.microsoft.com/v1.0/sites/' + site_id + '/lists', headers=h, timeout=30)
list_map = {item['displayName']: item['id'] for item in r2.json().get('value', [])}

for name in LISTS:
    lid = list_map.get(name)
    if not lid:
        print(f'  {name}: NOT FOUND')
        continue
    url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{lid}/items'
    count = 0
    next_url = url
    params = {'$top': '999', '$select': 'id'}
    while next_url:
        if next_url == url:
            r3 = requests.get(next_url, headers=h, params=params, timeout=60)
        else:
            r3 = requests.get(next_url, headers=h, timeout=60)
        if r3.status_code != 200:
            print(f'  {name}: error {r3.status_code}')
            break
        data = r3.json()
        count += len(data.get('value', []))
        next_url = data.get('@odata.nextLink')
    else:
        print(f'  {name}: {count} items')
