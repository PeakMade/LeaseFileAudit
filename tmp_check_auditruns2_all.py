import requests, sys
sys.path.insert(0,'.')
from dotenv import load_dotenv; load_dotenv()
from activity_logging.sharepoint import _get_app_only_token
t = _get_app_only_token()
h = {'Authorization': f'Bearer {t}', 'Prefer': 'HonorNonIndexedQueriesWarningMayFailRandomly'}
sid = 'peakcampus.sharepoint.com,f83f37af-e64c-4dbc-9457-68c9484ee93b,f65502be-a283-4eae-a759-a23b23603fbb'
lid = 'd8166180-5dcb-41a9-84c0-0ab104b77c27'

all_items = []
url = f'https://graph.microsoft.com/v1.0/sites/{sid}/lists/{lid}/items'
params = {'$top': '200', '$expand': 'fields'}
while url:
    r = requests.get(url, headers=h, params=params)
    data = r.json()
    all_items.extend(data.get('value', []))
    url = data.get('@odata.nextLink')
    params = {}

print(f'Total items: {len(all_items)}')
populated = [x for x in all_items if x.get('fields', {}).get('field_1')]
print(f'Items with field_1 populated: {len(populated)}')
for x in populated[:5]:
    f = x.get('fields', {})
    print(f'  {x.get("createdDateTime")} field_1={f.get("field_1")} field_2={f.get("field_2")}')

# Show most recent items
recent = sorted(all_items, key=lambda x: x.get('createdDateTime', ''), reverse=True)[:5]
print(f'\nMost recent 5:')
for x in recent:
    f = x.get('fields', {})
    print(f'  {x.get("createdDateTime")} field_1={f.get("field_1", "EMPTY")!r}')
