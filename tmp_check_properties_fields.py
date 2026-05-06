"""One-off script to inspect Properties_0 PM/RM lookup values."""
import os
from dotenv import load_dotenv
load_dotenv()
from audit_engine.api_ingest import _get_app_only_token, _resolve_sharepoint_site_id, _resolve_sharepoint_list_id, _graph_get_json

token = _get_app_only_token()
site_url = os.getenv('SHAREPOINT_SITE_URL')
site_id = _resolve_sharepoint_site_id(token, site_url)
list_id = _resolve_sharepoint_list_id(token, site_id, 'Properties_0')

# Request PM and RM as lookup fields (without LookupId suffix to get the display value)
endpoint = f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items'
result = _graph_get_json(endpoint, token, params={
    '$expand': 'fields($select=PROPERTY_NAME,LEGACY_ENTRATA_ID,PM,RM)',
    '$top': '5'
})
items = result.get('value', [])
print("=== PM/RM lookup expansion test ===")
for item in items:
    fields = item.get('fields', {})
    print(f"  Property: {fields.get('PROPERTY_NAME')} | PM: {fields.get('PM')} | RM: {fields.get('RM')}")
    print(f"    All keys: {[k for k in fields if 'PM' in k or 'RM' in k or 'pm' in k.lower() or 'rm' in k.lower()]}")
