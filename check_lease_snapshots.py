"""Check if lease snapshots exist for the current run."""
import sys
from pathlib import Path
from web.views import get_storage_service

def main():
    storage = get_storage_service()
    
    run_id = 'run_20260702_093008'
    property_id = 1150907
    lease_interval_id = 17599602
    
    print(f"\n{'='*80}")
    print(f"CHECKING SNAPSHOTS FOR RUN: {run_id}")
    print(f"Property: {property_id}, Lease: {lease_interval_id}")
    print(f"{'='*80}\n")
    
    # Check portfolio snapshot
    print("1. Checking portfolio snapshot...")
    portfolio_snaps = storage.load_run_display_snapshots_for_run(run_id, scope_type='portfolio')
    print(f"   Found {len(portfolio_snaps)} portfolio snapshot(s)")
    
    # Check property snapshots
    print("\n2. Checking property snapshots...")
    property_snaps = storage.load_run_display_snapshots_for_run(run_id, scope_type='property')
    print(f"   Found {len(property_snaps)} property snapshot(s)")
    if property_snaps:
        for snap in property_snaps[:3]:
            print(f"      - Property {snap.get('property_id')}: {snap.get('property_name')}")
    
    # Check lease snapshots for the whole run
    print("\n3. Checking ALL lease snapshots for this run...")
    lease_snaps = storage.load_run_display_snapshots_for_run(run_id, scope_type='lease')
    print(f"   Found {len(lease_snaps)} lease snapshot(s) total")
    if lease_snaps:
        print(f"   Sample lease snapshots:")
        for snap in lease_snaps[:5]:
            print(f"      - Property {snap.get('property_id')}, Lease {snap.get('lease_interval_id')}: {snap.get('resident_name')}")
    
    # Check lease snapshots for specific property
    print(f"\n4. Checking lease snapshots for property {property_id}...")
    property_lease_snaps = storage.load_run_display_snapshots_for_property(run_id, property_id, scope_type='lease')
    print(f"   Found {len(property_lease_snaps)} lease snapshot(s) for property {property_id}")
    if property_lease_snaps:
        print(f"   Lease IDs: {list(property_lease_snaps.keys())[:10]}")
        if lease_interval_id in property_lease_snaps:
            print(f"\n   ✓ Found snapshot for lease {lease_interval_id}!")
            snap = property_lease_snaps[lease_interval_id]
            print(f"      Resident: {snap.get('resident_name')}")
            print(f"      Undercharge: ${snap.get('undercharge', 0):.2f}")
            print(f"      Overcharge: ${snap.get('overcharge', 0):.2f}")
            print(f"      Exceptions: {snap.get('exception_count', 0)}")
        else:
            print(f"\n   ✗ Lease {lease_interval_id} NOT found in snapshots!")
    
    # Check AR code and month level snapshots
    print(f"\n5. Checking AR code level snapshots...")
    ar_code_snaps = storage.load_run_display_snapshots_for_run(run_id, scope_type='ar_code')
    print(f"   Found {len(ar_code_snaps)} AR code snapshot(s)")
    
    print(f"\n6. Checking month level snapshots...")
    month_snaps = storage.load_run_display_snapshots_for_run(run_id, scope_type='month')
    print(f"   Found {len(month_snaps)} month snapshot(s)")
    
    print(f"\n{'='*80}")

if __name__ == '__main__':
    main()
