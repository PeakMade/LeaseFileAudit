"""Check data for run_20260701_104822"""
import sys
sys.path.insert(0, 'Z:\\Shared\\Technology\\AI Projects\\LeaseFileAudit')

from storage.service import StorageService
import os

base_dir = os.path.dirname(os.path.abspath(__file__))
storage = StorageService(base_dir)

run_id = "run_20260701_104822"

# Check RunDisplaySnapshots
print(f"\n=== Checking RunDisplaySnapshots for {run_id} ===")
snapshots = storage._query_snapshots_by_filter(run_id, "")
print(f"Total snapshots: {len(snapshots)}")

if snapshots:
    scope_counts = {}
    for snap in snapshots:
        scope = snap.get('ScopeType', 'unknown')
        scope_counts[scope] = scope_counts.get(scope, 0) + 1
    print(f"Breakdown by scope: {scope_counts}")
    
    # Show first few snapshots
    print(f"\nFirst 3 snapshots:")
    for snap in snapshots[:3]:
        print(f"  - Scope: {snap.get('ScopeType')}, PropertyId: {snap.get('PropertyId')}, "
              f"LeaseIntervalId: {snap.get('LeaseIntervalId')}, ARCodeId: {snap.get('ARCodeId')}")
else:
    print("No snapshots found!")

# Check AuditRuns2
print(f"\n=== Checking AuditRuns2 for {run_id} ===")
try:
    from storage.service import StorageService
    audit_runs = storage.load_bucket_results(run_id)
    print(f"AuditRuns2 rows: {len(audit_runs)}")
    if not audit_runs.empty:
        print(f"Columns: {list(audit_runs.columns)}")
        print(f"PropertyIds: {audit_runs['PropertyId'].unique()[:5]}")
except Exception as e:
    print(f"Error loading AuditRuns2: {e}")

print("\n=== Checking available runs ===")
runs = storage.list_runs()
print(f"Available runs: {len(runs)}")
for run in runs[:5]:
    print(f"  - {run.get('run_id')}: {run.get('property_count')} properties, "
          f"{run.get('total_exceptions')} exceptions, executed {run.get('executed_timestamp')}")
