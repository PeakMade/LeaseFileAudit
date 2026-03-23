import os
import json
from urllib.parse import urlparse
import requests
from activity_logging.sharepoint import _get_app_only_token

site_url = os.getenv('SHAREPOINT_SITE_URL')
list_name = os.getenv('LEASE_API_PROPERTIES_SHAREPOINT_LIST') or 'Properties_0'
out_file = 'properties_reportable_legacy_entrata_ids.md'

if not site_url:
    raise RuntimeError('SHAREPOINT_SITE_URL is not set')

token = _get_app_only_token()
if not token:
    raise RuntimeError('Unable to acquire app-only token')

headers = {
    'Authorization': f'Bearer {token}',
    'Accept': 'application/json',
}

parsed = urlparse(site_url)
hostname = parsed.hostname
site_path = parsed.path
if not hostname or not site_path:
    raise RuntimeError(f'Invalid SHAREPOINT_SITE_URL: {site_url}')

site_resp = requests.get(f'https://graph.microsoft.com/v1.0/sites/{hostname}:{site_path}', headers=headers, timeout=30)
site_resp.raise_for_status()
site_id = site_resp.json().get('id')
if not site_id:
    raise RuntimeError('Could not resolve SharePoint site id')

lists_resp = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists',
    headers=headers,
    params={'$filter': f"displayName eq '{list_name}'", '$select': 'id,displayName'},
    timeout=30,
)
lists_resp.raise_for_status()
list_rows = lists_resp.json().get('value', [])
if not list_rows:
    raise RuntimeError(f"SharePoint list '{list_name}' not found")

list_id = list_rows[0]['id']

cols_resp = requests.get(
    f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/columns',
    headers=headers,
    params={'$select': 'name,displayName'},
    timeout=30,
)
cols_resp.raise_for_status()
columns = cols_resp.json().get('value', [])
col_names = [str(c.get('name') or '') for c in columns]

name_candidates = ['PROPERTY_NAME', 'PropertyName', 'property_name', 'Title']
legacy_id_candidates = ['LEGACY_ENTRATA_ID', 'LegacyEntrataId', 'legacy_entrata_id']


def _normalize_field_token(value: str) -> str:
    token = str(value or '').lower().replace('_x005f_', '_')
    return ''.join(ch for ch in token if ch.isalnum())


def pick_field(candidates):
    lower_map = {c.lower(): c for c in col_names if c}
    for cand in candidates:
        if cand in col_names:
            return cand
        if cand.lower() in lower_map:
            return lower_map[cand.lower()]

    normalized_column_map = {_normalize_field_token(c): c for c in col_names if c}
    for cand in candidates:
        norm = _normalize_field_token(cand)
        if norm in normalized_column_map:
            return normalized_column_map[norm]
    return None


name_field = pick_field(name_candidates)
legacy_field = pick_field(legacy_id_candidates)
reportable_field = None

if not name_field:
    raise RuntimeError('Could not find property name field')
if not legacy_field:
    raise RuntimeError('Could not find legacy entrata id field')
select_fields = sorted({name_field, legacy_field})

items_url = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items'
params = {
    '$expand': f"fields($select={','.join(select_fields)})",
    '$top': '5000',
}

rows = []
next_url = items_url
next_params = params
while next_url:
    resp = requests.get(next_url, headers=headers, params=next_params, timeout=60)
    resp.raise_for_status()
    payload = resp.json()

    for item in payload.get('value', []):
        fields = item.get('fields') if isinstance(item.get('fields'), dict) else {}
        property_name = str(fields.get(name_field) or '').strip()
        legacy_id = str(fields.get(legacy_field) or '').strip()
        if not property_name or not legacy_id:
            continue

        rows.append((property_name, legacy_id))

    next_url = payload.get('@odata.nextLink')
    next_params = None

seen = set()
deduped = []
for name, legacy_id in rows:
    key = (name, legacy_id)
    if key in seen:
        continue
    seen.add(key)
    deduped.append(key)

deduped.sort(key=lambda t: (t[0].lower(), t[1]))

lines = []
lines.append('# Properties and Legacy Entrata IDs')
lines.append('')
lines.append(f'- Source list: {list_name}')
lines.append(f'- Total properties with legacy IDs: {len(deduped)}')
lines.append('')
lines.append('| Property Name | Legacy Entrata ID |')
lines.append('|---|---|')
for name, legacy_id in deduped:
    safe_name = name.replace('|', '\\|')
    safe_id = legacy_id.replace('|', '\\|')
    lines.append(f'| {safe_name} | {safe_id} |')

with open(out_file, 'w', encoding='utf-8') as f:
    f.write('\n'.join(lines) + '\n')

print(json.dumps({
    'output_file': out_file,
    'list_name': list_name,
    'field_name': name_field,
    'field_legacy_id': legacy_field,
    'count': len(deduped),
}, indent=2))
