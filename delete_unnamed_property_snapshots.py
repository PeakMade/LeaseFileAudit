"""
Delete RunDisplaySnapshots rows for properties that have no name stored
(i.e. sandbox/test runs showing as "Property 100162547" etc.).

Targets: property-scoped snapshots where PropertyNameStatic is blank/missing.

Run from the project root:
    python delete_unnamed_property_snapshots.py
"""

import sys
import os
import requests

sys.path.insert(0, os.path.dirname(__file__))
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import config as _cfg
from activity_logging.sharepoint import _get_app_only_token


def get_site_id(session, site_url: str) -> str:
    hostname = site_url.split("/")[2]
    path = "/".join(site_url.split("/")[3:])
    resp = session.get(
        f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{path}",
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["id"]


def get_list_id(session, site_id: str, list_name: str) -> str:
    resp = session.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists",
        params={"$select": "id,name,displayName"},
        timeout=30,
    )
    resp.raise_for_status()
    for lst in resp.json().get("value", []):
        if lst.get("name") == list_name or lst.get("displayName") == list_name:
            return lst["id"]
    raise RuntimeError(f"List '{list_name}' not found on site.")


def fetch_unnamed_property_snapshot_ids(session, site_id: str, list_id: str) -> list[tuple[str, str]]:
    """Return list of (item_id, property_id_str) for property snapshots with no PropertyNameStatic."""
    items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    params = {
        "$expand": "fields($select=PropertyId,PropertyNameStatic,ScopeType)",
        "$filter": "fields/ScopeType eq 'property'",
        "$top": 1000,
        "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly",
    }

    results = []
    page_count = 0
    url = items_url

    while url:
        resp = session.get(url, params=params if page_count == 0 else None, timeout=60)
        if resp.status_code != 200:
            print(f"ERROR fetching items: {resp.status_code} - {resp.text}")
            break
        data = resp.json()
        for item in data.get("value", []):
            fields = item.get("fields", {})
            name = (fields.get("PropertyNameStatic") or "").strip()
            if not name:
                results.append((item["id"], str(fields.get("PropertyId", "?"))))
        url = data.get("@odata.nextLink")
        page_count += 1

    return results


def delete_items(session, site_id: str, list_id: str, item_ids: list[tuple[str, str]]) -> None:
    items_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    deleted = 0
    failed = 0
    for item_id, prop_id in item_ids:
        resp = session.delete(f"{items_url}/{item_id}", timeout=30)
        if resp.status_code in (200, 204):
            deleted += 1
            print(f"  Deleted item {item_id} (PropertyId={prop_id})")
        else:
            failed += 1
            print(f"  FAILED item {item_id} (PropertyId={prop_id}): {resp.status_code} - {resp.text}")
    print(f"\nDone. Deleted: {deleted}  Failed: {failed}")


def main():
    token = _get_app_only_token()
    if not token:
        print("ERROR: Could not acquire access token.")
        sys.exit(1)

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly",
    })

    sharepoint_url = os.getenv("SHAREPOINT_SITE_URL") or getattr(_cfg, "SHAREPOINT_SITE_URL", None)
    if not sharepoint_url:
        print("ERROR: SHAREPOINT_SITE_URL not configured.")
        sys.exit(1)

    print(f"Site: {sharepoint_url}")
    site_id = get_site_id(session, sharepoint_url)
    print(f"Site ID: {site_id}")

    list_id = get_list_id(session, site_id, "RunDisplaySnapshots")
    print(f"List ID: {list_id}")

    print("\nFetching property snapshots with no name...")
    targets = fetch_unnamed_property_snapshot_ids(session, site_id, list_id)

    if not targets:
        print("No unnamed property snapshots found. Nothing to delete.")
        return

    print(f"\nFound {len(targets)} unnamed property snapshot(s):")
    for item_id, prop_id in targets:
        print(f"  Item {item_id}  PropertyId={prop_id}")

    answer = input(f"\nPermanently delete these {len(targets)} item(s)? [yes/no]: ").strip().lower()
    if answer != "yes":
        print("Aborted.")
        return

    print()
    delete_items(session, site_id, list_id, targets)


if __name__ == "__main__":
    main()
