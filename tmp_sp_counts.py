import sys, os
sys.path.insert(0, '.')
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import config as _cfg
from activity_logging.sharepoint import _get_app_only_token
import requests

LISTS = [
    "AuditRuns",
    "AuditRuns2",
    "RunDisplaySnapshots",
    "LeaseTermSet",
    "LeaseTerms",
    "LeaseTermEvidence",
    "ExceptionMonths",
    "Audit Run Metrics",
]

token = _get_app_only_token()
site_url = _cfg.config.auth.sharepoint_site_url
hostname = site_url.split("/")[2]
path = "/".join(site_url.split("/")[3:])
headers = {"Authorization": f"Bearer {token}"}

r = requests.get(f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{path}", headers=headers, timeout=30)
site_id = r.json()["id"]
print(f"Site: {site_url}\n")

for name in LISTS:
    r = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists",
        headers=headers,
        params={"$filter": f"displayName eq '{name}'"},
        timeout=30,
    )
    items = r.json().get("value", [])
    if not items:
        print(f"  {name}: NOT FOUND")
        continue
    list_id = items[0]["id"]
    count_headers = dict(headers)
    count_headers["ConsistencyLevel"] = "eventual"
    r4 = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items?$count=true",
        headers=count_headers,
        params={"$top": 1, "$select": "id"},
        timeout=60,
    )
    if r4.status_code == 200:
        count = r4.json().get("@odata.count", "n/a")
        print(f"  {name}: {count} items")
    else:
        print(f"  {name}: error ({r4.status_code})")
