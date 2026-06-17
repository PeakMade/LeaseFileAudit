"""Check what snapshots exist in RunDisplaySnapshots list."""
import os
from storage.service import StorageService

def main():
    storage = StorageService(use_sharepoint=True)
    
    # Try to load snapshots for recent runs
    run_ids_to_check = [
        'run_20260422_125827',
        'run_20260422_124912',
        'run_20260422_123456',  # example
    ]
    
    print("\n" + "="*80)
    print("CHECKING RUNDISPLAYSNAPSHOTS CONTENT")
    print("="*80 + "\n")
    
    for run_id in run_ids_to_check:
        print(f"\n--- Run: {run_id} ---")
        
        # Check for portfolio snapshots
        portfolio_snaps = storage.load_run_display_snapshots_for_run(
            run_id=run_id,
            scope_type='portfolio'
        )
        print(f"  Portfolio snapshots: {len(portfolio_snaps)}")
        
        # Check for property snapshots
        property_snaps = storage.load_run_display_snapshots_for_run(
            run_id=run_id,
            scope_type='property'
        )
        print(f"  Property snapshots: {len(property_snaps)}")
        
        # Check for lease snapshots
        lease_snaps = storage.load_run_display_snapshots_for_run(
            run_id=run_id,
            scope_type='lease'
        )
        print(f"  Lease snapshots: {len(lease_snaps)}")
        
        if len(property_snaps) > 0:
            print(f"\n  Sample property snapshot:")
            print(f"    Property ID: {property_snaps[0].get('property_id')}")
            print(f"    Property Name: {property_snaps[0].get('property_name')}")
            print(f"    Undercharge: ${property_snaps[0].get('undercharge', 0):,.2f}")
            print(f"    Overcharge: ${property_snaps[0].get('overcharge', 0):,.2f}")
            print(f"    Exception Count: {property_snaps[0].get('exception_count', 0)}")

if __name__ == '__main__':
    main()
