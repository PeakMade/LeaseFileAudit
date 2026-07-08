"""Quick check of current run data"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from web.views import get_storage_service

# The run ID from the screenshot
run_id = "run_20260701_104822"
property_id = 771903

print(f"\nChecking data for {run_id}, Property {property_id}")
print(f"="*80)

try:
    storage = get_storage_service()
    
    # Load bucket results (this is what property view uses)
    print(f"\n1. Loading bucket results via storage.load_bucket_results()...")
    results = storage.load_bucket_results(run_id, property_id=property_id)
    print(f"   Got {len(results)} bucket result rows")
    
    if len(results) > 0:
        print(f"\n2. Columns: {list(results.columns)}")
        
        # Check AR codes
        if 'ar_code_id' in results.columns:
            print(f"\n3. AR Code distribution:")
            print(results['ar_code_id'].value_counts().head(10))
            
            # Check for base rent (154771)
            base_rent = results[results['ar_code_id'].astype(str).str.strip() == '154771']
            print(f"\n4. Base rent (154771) rows: {len(base_rent)}")
            if len(base_rent) > 0:
                print(f"   ✅ Base rent data EXISTS")
                print(f"   Sample row:")
                print(base_rent.iloc[0].to_dict())
            else:
                print(f"   ❌ No base rent data found!")
                print(f"   All AR codes present: {sorted(results['ar_code_id'].unique())}")
        
        # Check statuses
        if 'status' in results.columns:
            print(f"\n5. Status distribution:")
            print(results['status'].value_counts())
    else:
        print(f"   ❌ NO DATA RETURNED!")
        print(f"\n   Trying alternate data sources...")
        
        # Check if data exists in RunDisplaySnapshots
        print(f"\n   Checking RunDisplaySnapshots...")
        filter_str = f"fields/RunId eq '{run_id}' and fields/ScopeType eq 'month' and fields/PropertyId eq {property_id}"
        snapshots = storage._query_snapshots_by_filter(filter_str)
        print(f"   Found {len(snapshots)} month-level snapshots")
        
        if snapshots:
            print(f"   ✅ Data exists in snapshots but not being returned by load_bucket_results!")
            print(f"   Sample snapshot: {snapshots[0]}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()

print(f"\n" + "="*80)

