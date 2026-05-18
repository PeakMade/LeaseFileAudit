#!/usr/bin/env python3
"""List all SharePoint lists on the configured site."""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import config as _cfg
from activity_logging.sharepoint import _get_app_only_token
import requests

cfg = _cfg.config
site_url = cfg.auth.sharepoint_site_url

if not site_url:
    print("ERROR: sharepoint_site_url not configured.")
    sys.exit(1)

print(f"SharePoint site: {site_url}")

token = _get_app_only_token()
if not token:
    print("ERROR: Could not acquire access token.")
    sys.exit(1)

# Get site ID
hostname = site_url.split("/")[2]
path = "/".join(site_url.split("/")[3:])
site_id_url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{path}"
r = requests.get(site_id_url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
r.raise_for_status()
site_id = r.json()["id"]
print(f"Site ID: {site_id}\n")

# List all lists
lists_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
r = requests.get(lists_url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
r.raise_for_status()

lists_data = r.json().get("value", [])
print(f"Found {len(lists_data)} list(s):\n")

for lst in sorted(lists_data, key=lambda x: x["displayName"]):
    print(f"  - {lst['displayName']}")
    print(f"    ID: {lst['id']}")
    print()
