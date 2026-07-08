"""Quick check of remaining items in SharePoint lists."""
import requests
from activity_logging.sharepoint import _get_app_only_token

token = _get_app_only_token()
site_url = "https://peakcampus.sharepoint.com/sites/BaseCampApps"

# Get site ID
site_resp = requests.get(
    f"https://graph.microsoft.com/v1.0/sites/{site_url.replace('https://', '').replace('/', ',')}",
    headers={"Authorization": f"Bearer {token}"}
)
site_id = site_resp.json()["id"]

# Lists to check
lists_to_check = [
    "RunDisplaySnapshots",
    "LeaseTermSet",
    "LeaseTerms",
    "LeaseTermEvidence",
    "ExceptionMonths",
    "Audit Run Metrics",
]

print("\nSharePoint List Item Counts:")
print("=" * 50)

for list_name in lists_to_check:
    # Get list ID
    list_resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists?$filter=displayName eq '{list_name}'",
        headers={"Authorization": f"Bearer {token}"}
    )
    
    if not list_resp.json().get("value"):
        print(f"{list_name:<30} NOT FOUND")
        continue
    
    list_id = list_resp.json()["value"][0]["id"]
    
    # Get item count
    items_resp = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$top=1&$count=true",
        headers={"Authorization": f"Bearer {token}", "ConsistencyLevel": "eventual"}
    )
    
    count = items_resp.json().get("@odata.count", "Unknown")
    print(f"{list_name:<30} {count:>10} items")

print("=" * 50)
