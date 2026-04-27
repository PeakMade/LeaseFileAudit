"""
Wipe all items from all active SharePoint lists used by LeaseFileAudit.

Lists cleared:
  - AuditRuns
  - RunDisplaySnapshots
  - LeaseTermSet
  - LeaseTerms
  - LeaseTermEvidence
  - ExceptionMonths
  - Audit Run Metrics

Run from the project root:
    python clean_sharepoint_lists.py

You will be prompted to confirm before anything is deleted.
"""

import sys
import os
import time
import threading
import requests
from requests.adapters import HTTPAdapter
from concurrent.futures import ThreadPoolExecutor, as_completed

# Bootstrap app config so we can reuse the same auth/config as the app
sys.path.insert(0, os.path.dirname(__file__))
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import config as _cfg
from activity_logging.sharepoint import _get_app_only_token

# ── List names to wipe ──────────────────────────────────────────────────────
LISTS_TO_CLEAN = [
    "AuditRuns",
    "RunDisplaySnapshots",
    "LeaseTermSet",
    "LeaseTerms",
    "LeaseTermEvidence",
    "ExceptionMonths",
    "Audit Run Metrics",
]

# Each entry may have a fallback name (same as service.py resolver)
LIST_FALLBACKS = {
    "AuditRuns": ["AuditRuns", "Audit Run Results"],
    "RunDisplaySnapshots": ["RunDisplaySnapshots", "Run Display Snapshots"],
    "LeaseTermSet": ["LeaseTermSet", "Lease Term Set"],
    "LeaseTerms": ["LeaseTerms", "Lease Terms"],
    "LeaseTermEvidence": ["LeaseTermEvidence", "Lease Term Evidence"],
    "ExceptionMonths": ["ExceptionMonths"],
    "Audit Run Metrics": ["Audit Run Metrics"],
}


def get_token() -> str:
    # _get_app_only_token reads directly from env vars (SHAREPOINT_TENANT_ID, SHAREPOINT_CLIENT_ID, etc.)
    token = _get_app_only_token()
    if not token:
        print("ERROR: Could not acquire access token. Check SHAREPOINT_TENANT_ID, SHAREPOINT_CLIENT_ID, MICROSOFT_PROVIDER_AUTHENTICATION_SECRET in .env")
        sys.exit(1)
    return token


# Shared session for connection reuse (avoids repeated SSL handshakes)
_SESSION: requests.Session | None = None

def get_session(token: str) -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10)
        _SESSION.mount("https://", adapter)
        _SESSION.headers.update({
            "Authorization": f"Bearer {token}",
            "Prefer": "HonorNonIndexedQueriesWarningMayFailRandomly",
        })
        # Warm up SSL connection to graph.microsoft.com
        try:
            _SESSION.get("https://graph.microsoft.com", timeout=15)
        except Exception:
            pass
    else:
        _SESSION.headers.update({"Authorization": f"Bearer {token}"})
    return _SESSION


def get_site_id(token: str, site_url: str) -> str:
    hostname = site_url.split("/")[2]
    path = "/".join(site_url.split("/")[3:])
    url = f"https://graph.microsoft.com/v1.0/sites/{hostname}:/{path}"
    r = get_session(token).get(url, timeout=30)
    r.raise_for_status()
    return r.json()["id"]


def resolve_list_id(token: str, site_id: str, names: list[str]) -> tuple[str | None, str | None]:
    sess = get_session(token)
    for name in names:
        url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists"
        r = sess.get(url, params={"$filter": f"displayName eq '{name}'"}, timeout=30)
        if r.status_code == 200:
            items = r.json().get("value", [])
            if items:
                return items[0]["id"], name
    return None, None


def fetch_all_item_ids(token: str, site_id: str, list_id: str) -> list[str]:
    sess = get_session(token)
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    params = {"$select": "id", "$top": 200}
    ids = []
    page = 0
    while url:
        page += 1
        # Show a live timer so the user can see we're still working (SharePoint is slow)
        stop = threading.Event()
        t_start = time.time()
        def _tick(stop=stop, t_start=t_start, page=page):
            while not stop.wait(2):
                elapsed = int(time.time() - t_start)
                print(f"\r    page {page}: waiting for SharePoint... ({elapsed}s)  ", end="", flush=True)
        t = threading.Thread(target=_tick, daemon=True)
        t.start()
        try:
            r = sess.get(url, params=params, timeout=120)
        finally:
            stop.set()
            t.join(timeout=1)

        elapsed = int(time.time() - t_start)
        if r.status_code != 200:
            print(f"\r    WARNING: {r.status_code}: {r.text[:200]}")
            break
        data = r.json()
        batch = data.get("value", [])
        ids.extend(item["id"] for item in batch)
        print(f"\r    page {page}: {len(batch)} item(s) in {elapsed}s{' ' * 20}")
        url = data.get("@odata.nextLink")
        params = {}
    return ids


def delete_item(token: str, site_id: str, list_id: str, item_id: str) -> bool:
    # Each thread needs its own session (Session is not thread-safe)
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items/{item_id}"
    r = requests.delete(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
    return r.status_code in (200, 204)


def wipe_list(token: str, site_id: str, list_id: str, list_name: str) -> tuple[int, int]:
    print(f"  Fetching items (this can take 30-60s, please wait)...")
    item_ids = fetch_all_item_ids(token, site_id, list_id)
    total = len(item_ids)
    print(f"  {total} item(s) found.")
    if not item_ids:
        return 0, 0

    deleted = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(delete_item, token, site_id, list_id, iid): iid for iid in item_ids}
        for i, fut in enumerate(as_completed(futures), 1):
            if fut.result():
                deleted += 1
            else:
                failed += 1
            print(f"\r  Deleting... {i}/{len(item_ids)}", end="", flush=True)
    print()
    return deleted, failed


def main():
    cfg = _cfg.config
    site_url = cfg.auth.sharepoint_site_url
    if not site_url:
        print("ERROR: sharepoint_site_url not configured.")
        sys.exit(1)

    print(f"\nSharePoint site: {site_url}")
    print("Connecting and resolving lists (may take ~30s)...")
    token = get_token()
    site_id = get_site_id(token, site_url)

    # Resolve all list IDs upfront while connection is warm
    resolved = []
    for canonical_name in LISTS_TO_CLEAN:
        names = LIST_FALLBACKS.get(canonical_name, [canonical_name])
        list_id, resolved_name = resolve_list_id(token, site_id, names)
        if list_id:
            resolved.append((list_id, resolved_name))
        else:
            print(f"  WARNING: '{canonical_name}' not found on SharePoint, will skip.")

    print(f"\nReady to wipe {len(resolved)} list(s):")
    for _, name in resolved:
        print(f"  - {name}")

    confirm = input("\nType YES to permanently delete ALL items from these lists: ").strip()
    if confirm != "YES":
        print("Aborted.")
        sys.exit(0)
    print()

    for list_id, resolved_name in resolved:
        print(f"[{resolved_name}]")
        deleted, failed = wipe_list(token, site_id, list_id, resolved_name)
        print(f"  Done: {deleted} deleted, {failed} failed.\n")

    print("All lists cleaned.")


if __name__ == "__main__":
    main()
