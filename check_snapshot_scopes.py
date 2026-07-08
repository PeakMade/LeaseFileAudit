"""Quick script to check what snapshot scope types exist for the latest run."""
import os
import sys
from dotenv import load_dotenv
load_dotenv(override=True)

# Add parent directory to path to import app modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import create_app

# Create app to initialize storage
app = create_app()

with app.app_context():
    from web.views import get_storage_service
    storage = get_storage_service()

# Get latest run
runs = storage.list_runs(limit=1)
if not runs:
    print("No runs found")
    exit(0)

run_id = runs[0]['run_id']
print(f"Checking run: {run_id}")
print(f"Run name: {runs[0].get('display_name', 'N/A')}")
print()

# Query all snapshots for this run
filter_str = f"RunId eq '{run_id}'"
snapshots = storage._query_snapshots_by_filter(run_id, filter_str)

if not snapshots:
    print("No snapshots found for this run")
    exit(0)

# Count by scope type
from collections import Counter
scope_counts = Counter(s.get('ScopeType', 'unknown') for s in snapshots)

print(f"Total snapshots: {len(snapshots)}")
print("\nSnapshot counts by ScopeType:")
for scope_type, count in sorted(scope_counts.items()):
    print(f"  {scope_type}: {count}")

# Check if month-level snapshots exist
month_snapshots = [s for s in snapshots if s.get('ScopeType') == 'month']
print(f"\nMonth-level snapshots: {len(month_snapshots)}")

if month_snapshots:
    # Show a sample
    sample = month_snapshots[0]
    print("\nSample month snapshot fields:")
    for key in ['PropertyId', 'LeaseIntervalId', 'ArCodeId', 'AuditMonth', 'Status', 'Variance']:
        print(f"  {key}: {sample.get(key, 'N/A')}")
else:
    print("  ❌ No month-level snapshots found - lease detail view will fail!")
    print("  The audit needs to write month-level snapshots to RunDisplaySnapshots")

# Check AuditRuns2
print("\n" + "="*60)
print("Checking AuditRuns2 list...")
try:
    audit_results = storage._load_results_from_sharepoint_list(
        run_id, 'bucket_result', property_id=None, lease_interval_id=None
    )
    if audit_results is not None and len(audit_results) > 0:
        print(f"✅ AuditRuns2 has {len(audit_results)} rows")
    else:
        print("❌ AuditRuns2 is empty or not found")
except Exception as e:
    print(f"❌ Error reading AuditRuns2: {e}")

print("\n" + "="*60)
print("RECOMMENDATION:")
if not month_snapshots:
    print("The audit run completed but didn't write month-level snapshots.")
    print("Lease detail views need month-level data to show AR code breakdowns.")
    print("\nOptions:")
    print("  1. Check if async snapshot writes are still in progress")
    print("  2. Run a new audit (it should write month-level snapshots)")
    print("  3. Check storage configuration for snapshot writes")
