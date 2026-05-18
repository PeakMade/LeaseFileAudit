import requests
from dotenv import load_dotenv
load_dotenv()
import config as _cfg
from activity_logging.sharepoint import _get_app_only_token

site = _cfg.config.auth.sharepoint_site_url
token = _get_app_only_token()
host = site.split('/')[2]
path = '/'.join(site.split('/')[3:])
s = requests.Session()
s.headers.update({'Authorization': f'Bearer {token}'})
site_id = s.get(f'https://graph.microsoft.com/v1.0/sites/{host}:/{path}').json()['id']
list_id = s.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists',
    params={'$filter': "displayName eq 'AuditRuns2'"}
).json()['value'][0]['id']
items = s.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items',
    params={'$top': 200}
).json().get('value', [])
deleted = 0
for item in items:
    r = s.delete(f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item["id"]}')
    if r.status_code == 204:
        deleted += 1
print(f'Deleted {deleted} rows from AuditRuns2')
