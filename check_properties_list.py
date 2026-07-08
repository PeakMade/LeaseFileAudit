"""Check what properties are in the Properties_0 SharePoint list."""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

# Get token
from audit_engine.api_ingest import _get_app_only_token, _resolve_sharepoint_site_id, _resolve_sharepoint_list_id

sharepoint_site_url = os.getenv("SHAREPOINT_SITE_URL")
list_name = os.getenv("LEASE_API_PROPERTIES_SHAREPOINT_LIST", "Properties_0")

print(f"SharePoint Site: {sharepoint_site_url}")
print(f"List Name: {list_name}")
print()

token = _get_app_only_token()
site_id = _resolve_sharepoint_site_id(token, sharepoint_site_url)
list_id = _resolve_sharepoint_list_id(token, site_id, list_name)

print(f"Fetching properties from {list_name}...")
print()

endpoint = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
params = {
    "$expand": "fields($select=PROPERTY_NAME,PropertyName,LEGACY_ENTRATA_ID,LegacyEntrataId)",
    "$top": "100",
}

response = requests.get(endpoint, headers={"Authorization": f"Bearer {token}"}, params=params, timeout=60)
if response.status_code != 200:
    print(f"ERROR: {response.status_code} - {response.text}")
else:
    items = response.json().get("value", [])
    print(f"Total properties found: {len(items)}")
    print()
    
    for item in items:
        fields = item.get("fields", {})
        property_name = fields.get("PROPERTY_NAME") or fields.get("PropertyName") or "(no name)"
        property_id = fields.get("LEGACY_ENTRATA_ID") or fields.get("LegacyEntrataId") or "(no ID)"
        print(f"  - {property_name}: {property_id}")
