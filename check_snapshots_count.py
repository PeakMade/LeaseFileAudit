"""Quick check of RunDisplaySnapshots item count"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from activity_logging.sharepoint import _get_app_only_token
import requests

token = _get_app_only_token()
if not token:
    print("Could not get token")
    sys.exit(1)

# Get site ID
site_url = "https://peakcampus.sharepoint.com/sites/BaseCampApps"
hostname = site_url.split("/")[2]
path = "/".join(site_url.split("/")[3:])
url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{path}"
r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
site_id = r.json()["id"]

# Get list ID
url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
lists = r.json().get("value", [])
list_id = None
for lst in lists:
    if lst.get("displayName") in ["RunDisplaySnapshots", "Run Display Snapshots"]:
        list_id = lst["id"]
        break

if not list_id:
    print("Could not find RunDisplaySnapshots list")
    sys.exit(1)

# Get item count
url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$count=true&$top=1"
r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
count = r.json().get("@odata.count", "unknown")

print(f"\nRunDisplaySnapshots currently has {count} items\n")
