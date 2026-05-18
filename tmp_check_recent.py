import requests
from dotenv import load_dotenv
load_dotenv()
import sys; sys.path.insert(0,'.')
from activity_logging.sharepoint import _get_app_only_token
access_token = _get_app_only_token()
headers = {'Authorization': f'Bearer {access_token}'}
site_id = 'peakcampus.sharepoint.com,f83f37af-e64c-4dbc-9457-68c9484ee93b,f65502be-a283-4eae-a759-a23b23603fbb'
list_id = 'd8166180-5dcb-41a9-84c0-0ab104b77c27'
r = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items',
    headers=headers,
    params={'$orderby': 'Created desc', '$top': 5, '$expand': 'fields'}
)
for item in r.json().get('value', []):
    f = item['fields']
    print(f'field_1={f.get("field_1","")!r} field_2={f.get("field_2","")!r} created={item.get("createdDateTime","")}')
