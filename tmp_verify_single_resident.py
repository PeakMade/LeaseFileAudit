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

RUN_ID = 'run_20260521_161236'
PROPERTY_ID = 1224149

# All residents visible in the property table screenshot
TEST_CASES = [
    (18523873, 'Tanieka Aime',       630.00,    120.00),
    (18667458, 'Rayne Roe',         1099.00,      0.00),
    (18586198, 'Haile Barrera',      840.00,      0.00),
    (18655847, 'Model 4',            840.00,      0.00),
    (18668984, 'Jordan Riehle',      840.00,      0.00),
    (18506447, 'Nevaeh Deering',     765.00,      0.00),
    (18569711, 'Grant Matthews',     765.00,      0.00),
    (18816228, 'Conrad Lightbourne', 765.00,      0.00),
    (18550744, 'Cal Sturos',         764.00,      0.00),
    (18550747, 'william baewer',     764.00,      0.00),
]

print(f"\n=== RAW BUCKET DATA: lease_interval_id={LEASE_INTERVAL_ID} ===\n")
df = svc.load_bucket_results(RUN_ID, property_id=PROPERTY_ID, lease_interval_id=LEASE_INTERVAL_ID)

if df is None or df.empty:
    print("No bucket data found.")
    sys.exit(1)

print(f"Columns found: {list(df.columns)}\n")

# Handle both uppercase and lowercase column names
def find_col(df, *candidates):
    for c in candidates:
        if c in df.columns: return c
        if c.lower() in df.columns: return c.lower()
    return None

exp_col    = find_col(df, 'EXPECTED_TOTAL', 'expected_total')
act_col    = find_col(df, 'ACTUAL_TOTAL', 'actual_total')
var_col    = find_col(df, 'VARIANCE', 'variance')
status_col = find_col(df, 'STATUS', 'status')
lid_col    = find_col(df, 'LEASE_INTERVAL_ID', 'lease_interval_id')

cols = [c for c in [lid_col, status_col, exp_col, act_col, var_col] if c]
print(df[cols].to_string(index=False))

print(f"\n=== RECOMPUTED METRICS ===")


df[exp_col] = pd.to_numeric(df[exp_col], errors='coerce').fillna(0)
df[act_col] = pd.to_numeric(df[act_col], errors='coerce').fillna(0)
df[var_col] = pd.to_numeric(df[var_col], errors='coerce').fillna(0)

print(f"Total rows: {len(df)}")
print(f"\nStatus breakdown:")
print(df[status_col].value_counts().to_string())

# Mirror _calculate_static_metrics exactly: exclude MATCHED rows before computing undercharge/overcharge
non_matched = df[df[status_col].str.lower() != 'matched']
print(f"\nNon-matched rows (same filter used by snapshot): {len(non_matched)}")

undercharge_rows = non_matched[non_matched[var_col] < 0]
overcharge_rows  = non_matched[non_matched[var_col] > 0]

undercharge = abs(undercharge_rows[var_col].sum())
overcharge  = overcharge_rows[var_col].sum()

print(f"\nRecomputed undercharge (|variance| where ACTUAL < EXPECTED): ${undercharge:,.2f}")
print(f"Recomputed overcharge  (variance where ACTUAL > EXPECTED):   ${overcharge:,.2f}")

if undercharge_rows.empty:
    print("\n  (no undercharge rows)")
else:
    print("\nUndercharge detail rows:")
    print(undercharge_rows[cols].to_string(index=False))

if overcharge_rows.empty:
    print("\n  (no overcharge rows)")
else:
    print("\nOvercharge detail rows:")
    print(overcharge_rows[cols].to_string(index=False))

print(f"\n=== SNAPSHOT STORED VALUES ===")
snap_map = svc.load_run_display_snapshots_for_property(RUN_ID, PROPERTY_ID)
if snap_map:
    s = snap_map.get(LEASE_INTERVAL_ID) or snap_map.get(str(LEASE_INTERVAL_ID))
    if s:
        snap_under = float(s.get('undercharge', 0) or 0)
        snap_over  = float(s.get('overcharge', 0) or 0)
        snap_exc   = s.get('exception_count')
        print(f"Snapshot undercharge:     ${snap_under:,.2f}")
        print(f"Snapshot overcharge:      ${snap_over:,.2f}")
        print(f"Snapshot exception_count: {snap_exc}")
        print(f"\nMatch: undercharge {'✓ CONFIRMED' if abs(snap_under - undercharge) < 0.01 else '✗ MISMATCH'}"
              f"  |  overcharge {'✓ CONFIRMED' if abs(snap_over - overcharge) < 0.01 else '✗ MISMATCH'}")
    else:
        print(f"No snapshot found for lease_interval_id={LEASE_INTERVAL_ID}")
        print(f"Available keys (first 5): {list(snap_map.keys())[:5]}")
else:
    print("No snapshots returned for this property")
