"""Debug snapshot data to see why charges are disappearing"""
import os
import sys
import pandas as pd

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from storage.service import StorageService
from config import config
from pathlib import Path

# Initialize storage service (simplified - no SharePoint auth needed for query)
storage = StorageService(
    base_dir=Path("instance/runs"),
    use_sharepoint=True,
    sharepoint_site_url=config.auth.sharepoint_site_url,
    library_name=config.storage.sharepoint_library_name,
    access_token=None,  # Will use app default token
    audit_results_list_name=config.auth.audit_results_list_name,
)

run_id = "run_20260701_084056"  # Latest run from terminal output
property_id = 771903  # CLEMSON EDGE

print(f"\n{'='*80}")
print(f"DEBUGGING SNAPSHOT DATA FOR {run_id} / Property {property_id}")
print(f"{'='*80}\n")

# Check what's in RunDisplaySnapshots for this property
print("1. Querying RunDisplaySnapshots for month-level data...")
filter_parts = [
    f"fields/RunId eq '{run_id}'",
    f"fields/ScopeType eq 'month'",
    f"fields/PropertyId eq {property_id}"
]
filter_str = " and ".join(filter_parts)
print(f"   Filter: {filter_str}")

try:
    results = storage._query_snapshots_by_filter(filter_str)
    print(f"   ✅ Got {len(results)} month-level snapshots for property {property_id}")
    
    if results:
        print(f"\n2. Sample snapshot fields:")
        sample = results[0]
        for key in sorted(sample.keys()):
            print(f"   - {key}: {sample.get(key)}")
        
        # Check for AR code field
        print(f"\n3. Checking AR code field names...")
        ar_code_fields = [k for k in sample.keys() if 'ar' in k.lower() or 'code' in k.lower()]
        print(f"   AR code related fields: {ar_code_fields}")
        
        # Check unique AR codes
        print(f"\n4. Checking unique AR codes in snapshots...")
        ar_codes = set()
        for snap in results:
            for field in ['ArCodeId', 'ar_code_id', 'ARCodeId', 'AR_CODE_ID']:
                if field in snap:
                    ar_codes.add(snap[field])
        print(f"   Unique AR codes found: {sorted(ar_codes)}")
        
        # Check if 154771 (base rent) is present
        print(f"\n5. Looking for base rent AR code 154771...")
        base_rent_count = sum(1 for snap in results 
                              if str(snap.get('ArCodeId', '')).strip() == '154771' or
                                 str(snap.get('ar_code_id', '')).strip() == '154771')
        print(f"   Found {base_rent_count} snapshots with AR code 154771 (base rent)")
        
        if base_rent_count == 0:
            print(f"   ❌ WARNING: No base rent (154771) snapshots found!")
            print(f"   This explains why lease detail shows 'No Charges Found'")
        
        # Convert to DataFrame to see structure
        print(f"\n6. Converting to DataFrame...")
        df = pd.DataFrame(results)
        print(f"   Shape: {df.shape}")
        print(f"   Columns: {list(df.columns)}")
        
        if 'ArCodeId' in df.columns or 'ar_code_id' in df.columns:
            ar_col = 'ArCodeId' if 'ArCodeId' in df.columns else 'ar_code_id'
            print(f"\n7. AR Code distribution:")
            print(df[ar_col].value_counts().head(10))
        
except Exception as e:
    print(f"   ❌ Error: {e}")
    import traceback
    traceback.print_exc()

# Check AuditRuns2 as well
print(f"\n{'='*80}")
print(f"8. Checking AuditRuns2 table...")
print(f"{'='*80}\n")

try:
    filter_parts = [
        f"fields/RunId eq '{run_id}'",
        f"fields/PropertyId eq {property_id}"
    ]
    filter_str = " and ".join(filter_parts)
    print(f"   Filter: {filter_str}")
    
    # Use Graph API directly
    list_id = storage._resolve_list_id("AuditRuns2")
    url = f"{storage.graph_api_base}/sites/{storage.site_id}/lists/{list_id}/items"
    params = {"$filter": filter_str, "$expand": "fields", "$top": 5000}
    
    response = storage._execute_graph_request(url, params=params)
    items = response.get('value', [])
    print(f"   ✅ Got {len(items)} items from AuditRuns2")
    
    if items:
        # Check for AR code 154771
        base_rent_items = [item for item in items 
                          if str(item.get('fields', {}).get('ArCodeId', '')).strip() == '154771']
        print(f"   Found {len(base_rent_items)} items with AR code 154771 (base rent)")
        
except Exception as e:
    print(f"   ❌ Error: {e}")
    import traceback
    traceback.print_exc()

print(f"\n{'='*80}")
print(f"DIAGNOSIS COMPLETE")
print(f"{'='*80}\n")
