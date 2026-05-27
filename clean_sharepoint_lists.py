"""
Wipe all items from all active SharePoint lists used by LeaseFileAudit.

Lists cleared:
    - AuditRuns2
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
import requests
from requests.adapters import HTTPAdapter

# Bootstrap app config so we can reuse the same auth/config as the app
sys.path.insert(0, os.path.dirname(__file__))
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
import config as _cfg
from activity_logging.sharepoint import _get_app_only_token


def _banner(text: str):
    print(f"\n{text}")

# ── List names to wipe ──────────────────────────────────────────────────────
LISTS_TO_CLEAN = [
    "AuditRuns2",
    "RunDisplaySnapshots",
    "LeaseTermSet",
    "LeaseTerms",
    "LeaseTermEvidence",
    "ExceptionMonths",
    "Audit Run Metrics",
    # "AuditRuns",  # Excluded per user request
]

# Each entry may have a fallback name (same as service.py resolver)
LIST_FALLBACKS = {
    "AuditRuns2": ["AuditRuns2", "AuditRuns 2"],
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
        print("We couldn't connect to SharePoint with the current app credentials.")
        print("Please verify SHAREPOINT_TENANT_ID, SHAREPOINT_CLIENT_ID, and MICROSOFT_PROVIDER_AUTHENTICATION_SECRET in your .env file.")
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


BATCH_SIZE = 20  # Graph $batch allows up to 20 requests per call
MAX_RETRIES = 8


def _request_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs) and retry on 429/503 with Retry-After backoff."""
    for attempt in range(MAX_RETRIES):
        r = fn(*args, **kwargs)
        if r.status_code not in (429, 503):
            return r
        wait = int(r.headers.get("Retry-After", 30))
        print(
            f"\n  SharePoint asked us to slow down. Waiting {wait}s before trying again ({attempt + 1}/{MAX_RETRIES}).",
            flush=True,
        )
        time.sleep(wait)
    return r  # return last response after exhausting retries


def _batch_delete(sess: requests.Session, token: str, site_id: str, list_id: str, item_ids: list[str]) -> tuple[int, int]:
    """Send up to BATCH_SIZE DELETEs in a single Graph $batch call."""
    requests_payload = [
        {"id": str(i), "method": "DELETE",
         "url": f"/sites/{site_id}/lists/{list_id}/items/{iid}"}
        for i, iid in enumerate(item_ids)
    ]
    r = _request_with_retry(
        sess.post,
        "https://graph.microsoft.com/v1.0/$batch",
        json={"requests": requests_payload},
        headers={"Content-Type": "application/json"},
        timeout=60,
    )
    if r.status_code not in (200, 201):
        print(f"\n  We hit an issue removing a batch of items (status {r.status_code}).")
        return 0, len(item_ids)
    responses = r.json().get("responses", [])
    deleted = sum(1 for resp in responses if resp.get("status") in (200, 204))
    failed = len(responses) - deleted
    return deleted, failed


def wipe_list(token: str, site_id: str, list_id: str, list_name: str) -> tuple[int, int]:
    """Stream pages of item IDs and batch-delete as we go (no full pre-fetch)."""
    sess = get_session(token)
    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/lists/{list_id}/items"
    params = {"$select": "id", "$top": 200}
    total_deleted = 0
    total_failed = 0
    page = 0
    t_overall = time.time()

    while url:
        page += 1
        r = _request_with_retry(sess.get, url, params=params, timeout=120)
        if r.status_code != 200:
            print(f"  We couldn't read the next page for this list (status {r.status_code}).")
            break
        data = r.json()
        ids = [item["id"] for item in data.get("value", [])]
        if not ids:
            break

        # Batch-delete this page in chunks of BATCH_SIZE
        for i in range(0, len(ids), BATCH_SIZE):
            chunk = ids[i:i + BATCH_SIZE]
            d, f = _batch_delete(sess, token, site_id, list_id, chunk)
            total_deleted += d
            total_failed += f
            elapsed = time.time() - t_overall
            rate = total_deleted / elapsed if elapsed > 0 else 0
            print(
                f"\r  Working on {list_name}... page {page} | removed {total_deleted} item(s) | {rate:.0f}/sec    ",
                end="",
                flush=True,
            )

        url = data.get("@odata.nextLink")
        params = {}

    print(f"\r  Finished {list_name}: removed {total_deleted} item(s), skipped {total_failed}.{' ' * 20}")
    return total_deleted, total_failed


def main():
    cfg = _cfg.config
    site_url = cfg.auth.sharepoint_site_url
    if not site_url:
        print("SharePoint site URL is missing from your configuration.")
        sys.exit(1)

    _banner("SharePoint cleanup")
    print(f"Site: {site_url}")
    print("Connecting and checking your configured lists (this can take about 30 seconds)...")
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
            print(f"  Couldn't find '{canonical_name}' on SharePoint, so it will be skipped.")

    print(f"\nReady to clear {len(resolved)} list(s):")
    for _, name in resolved:
        print(f"  - {name}")

    confirm = input("\nType YES to permanently remove all items from these lists: ").strip()
    if confirm != "YES":
        print("Canceled. No changes were made.")
        sys.exit(0)
    print()

    for list_id, resolved_name in resolved:
        print(f"Now cleaning: {resolved_name}")
        deleted, failed = wipe_list(token, site_id, list_id, resolved_name)
        print(f"  Summary: removed {deleted}, skipped {failed}.\n")

    print("Cleanup complete. All selected lists have been processed.")


if __name__ == "__main__":
    main()
