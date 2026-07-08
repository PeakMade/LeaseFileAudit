"""
Check item counts in all LeaseFileAudit SharePoint lists.
"""
import requests
from activity_logging.sharepoint import _get_app_only_token
from dotenv import load_dotenv
import os

load_dotenv()

def get_site_id(token, site_url):
    """Resolve site ID from URL."""
    hostname = site_url.split("/")[2]
    path = "/".join(site_url.split("/")[3:])
    url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{path}"
    resp = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=30)
    resp.raise_for_status()
    return resp.json()["id"]

def get_list_count(token, site_id, list_name):
    """Get item count for a SharePoint list."""
    try:
        # Find list by name (try multiple fallback names)
        fallbacks = {
            "AuditRuns2": ["AuditRuns2", "AuditRuns 2"],
            "RunDisplaySnapshots": ["RunDisplaySnapshots", "Run Display Snapshots"],
            "LeaseTermSet": ["LeaseTermSet", "Lease Term Set"],
            "LeaseTerms": ["LeaseTerms", "Lease Terms"],
            "LeaseTermEvidence": ["LeaseTermEvidence", "Lease Term Evidence"],
            "ExceptionMonths": ["ExceptionMonths"],
            "Audit Run Metrics": ["Audit Run Metrics"],
        }
        
        names_to_try = fallbacks.get(list_name, [list_name])
        list_id = None
        
        for name in names_to_try:
            list_resp = requests.get(
                f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists',
                params={'$filter': f"displayName eq '{name}'"},
                headers={'Authorization': f'Bearer {token}'},
                timeout=30
            )
            
            if list_resp.status_code == 200:
                lists = list_resp.json().get('value', [])
                if lists:
                    list_id = lists[0]['id']
                    break
        
        if not list_id:
            return None, "List not found"
        
        # Get item count
        count_resp = requests.get(
            f'https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items',
            params={'$top': '1', '$count': 'true'},
            headers={'Authorization': f'Bearer {token}', 'ConsistencyLevel': 'eventual'},
            timeout=30
        )
        
        if count_resp.status_code != 200:
            return None, f"Error: {count_resp.status_code} - {count_resp.text[:100]}"
        
        count = count_resp.json().get('@odata.count', 0)
        return count, None
    except Exception as e:
        return None, str(e)

# Main
token = _get_app_only_token()
if not token:
    print("❌ Failed to get authentication token")
    exit(1)

site_url = os.getenv("SHAREPOINT_SITE_URL")
print(f"Site: {site_url}\n")

# Resolve site ID dynamically
site_id = get_site_id(token, site_url)

lists_to_check = [
    "AuditRuns2",
    "RunDisplaySnapshots",
    "LeaseTermSet",
    "LeaseTerms",
    "LeaseTermEvidence",
    "ExceptionMonths",
    "Audit Run Metrics",
]

print("SharePoint List Item Counts:")
print("=" * 60)

for list_name in lists_to_check:
    count, error = get_list_count(token, site_id, list_name)
    if error:
        print(f"❌ {list_name:30} | {error}")
    else:
        status = "✅" if count == 0 else "📊"
        print(f"{status} {list_name:30} | {count:,} items")

print("=" * 60)
