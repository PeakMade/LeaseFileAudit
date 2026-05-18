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


BATCH_SIZE = 20  # Graph $batch allows up to 20 requests per call
MAX_RETRIES = 8


def _request_with_retry(fn, *args, **kwargs):
    """Call fn(*args, **kwargs) and retry on 429/503 with Retry-After backoff."""
    for attempt in range(MAX_RETRIES):
        r = fn(*args, **kwargs)
        if r.status_code not in (429, 503):
            return r
        wait = int(r.headers.get("Retry-After", 30))
        print(f"\n  Throttled (429) — waiting {wait}s before retry {attempt+1}/{MAX_RETRIES}...", flush=True)
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
        print(f"\n  Batch error {r.status_code}: {r.text[:200]}")
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
            print(f"  WARNING page {page}: {r.status_code} {r.text[:200]}")
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
            print(f"\r  page {page} | deleted {total_deleted} | {rate:.0f}/s    ", end="", flush=True)

        url = data.get("@odata.nextLink")
        params = {}

    print(f"\r  Done: {total_deleted} deleted, {total_failed} failed.{' ' * 30}")
    return total_deleted, total_failed


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
