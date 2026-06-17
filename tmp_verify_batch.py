import sys, os
sys.path.insert(0, '.')
os.environ.setdefault('FLASK_ENV', 'development')

import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

from config import config
from activity_logging.sharepoint import _get_app_only_token
from storage.service import StorageService

svc = StorageService(
    base_dir=Path('instance/runs'),
    use_sharepoint=config.storage.is_sharepoint_configured(),
    sharepoint_site_url=config.auth.sharepoint_site_url,
    library_name=config.storage.sharepoint_library_name,
    access_token=_get_app_only_token(),
    audit_results_list_name=config.auth.audit_results_list_name,
)

RUN_ID = 'run_20260521_153934'
PROPERTY_ID = 771903  # Clemson Edge

def find_col(df, *candidates):
    for c in candidates:
        if c in df.columns: return c
        if c.lower() in df.columns: return c.lower()
    return None

print(f"Loading all bucket results for property {PROPERTY_ID}...")
all_buckets = svc.load_bucket_results(RUN_ID, property_id=PROPERTY_ID)
print(f"  -> {len(all_buckets)} rows loaded")

print(f"Loading all snapshots for property {PROPERTY_ID}...")
snap_map = svc.load_run_display_snapshots_for_property(RUN_ID, PROPERTY_ID)
print(f"  -> {len(snap_map) if snap_map else 0} snapshots loaded\n")

var_col    = find_col(all_buckets, 'VARIANCE', 'variance')
status_col = find_col(all_buckets, 'STATUS', 'status')
lid_col    = find_col(all_buckets, 'LEASE_INTERVAL_ID', 'lease_interval_id')

all_buckets[var_col] = pd.to_numeric(all_buckets[var_col], errors='coerce').fillna(0)
all_buckets['_is_matched'] = all_buckets[status_col].str.lower() == 'matched'

results = []
for lid, group in all_buckets.groupby(lid_col):
    non_matched  = group[~group['_is_matched']]
    recomp_under = abs(non_matched[non_matched[var_col] < 0][var_col].sum())
    recomp_over  = non_matched[non_matched[var_col] > 0][var_col].sum()

    s = snap_map.get(int(lid)) or snap_map.get(str(int(lid))) if snap_map else None
    snap_under = float(s.get('undercharge', 0) or 0) if s else None
    snap_over  = float(s.get('overcharge',  0) or 0) if s else None

    under_ok = snap_under is not None and abs(snap_under - recomp_under) < 0.01
    over_ok  = snap_over  is not None and abs(snap_over  - recomp_over)  < 0.01
    results.append({
        'lid': int(lid),
        'recomp_under': recomp_under,
        'recomp_over':  recomp_over,
        'snap_under':   snap_under,
        'snap_over':    snap_over,
        'ok': under_ok and over_ok,
        'no_snap': s is None,
    })

total     = len(results)
confirmed = sum(1 for r in results if r['ok'])
no_snap   = sum(1 for r in results if r['no_snap'])
mismatches = [r for r in results if not r['ok'] and not r['no_snap']]

print(f"{'='*70}")
print(f"Total leases checked : {total}")
print(f"Verified ✓           : {confirmed}")
print(f"No snapshot found    : {no_snap}")
print(f"Mismatches ✗         : {len(mismatches)}")
print(f"{'='*70}")

if mismatches:
    print("\n--- MISMATCH DETAIL ---")
    for r in mismatches:
        print(f"\n  lease_interval_id={r['lid']}")
        print(f"    Recomputed: under=${r['recomp_under']:,.2f}  over=${r['recomp_over']:,.2f}")
        print(f"    Snapshot:   under=${r['snap_under']:,.2f}  over=${r['snap_over']:,.2f}")
elif no_snap == 0:
    print("\nAll residents verified end-to-end ✓")
else:
    print(f"\n{confirmed} verified ✓  |  {no_snap} leases had no snapshot (new leases or not yet snapshotted)")

