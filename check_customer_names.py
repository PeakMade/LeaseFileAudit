"""
Check customer names in the most recent audit run
"""
import sys
sys.path.insert(0, '.')

from storage.service import StorageService
from audit_engine.canonical_fields import CanonicalField
from config import config
import pandas as pd

def check_customer_names():
    print("=" * 80)
    print("CUSTOMER NAME CHECK")
    print("=" * 80)
    
    # Load most recent run
    print("\n📂 Loading most recent audit run...")
    storage = StorageService(
        base_dir=config.storage.base_dir,
        use_sharepoint=False
    )
    runs = storage.list_runs(limit=1)
    
    if not runs:
        print("❌ No audit runs found")
        return
    
    run_id = runs[0]['run_id']
    print(f"✓ Found run: {run_id}")
    
    # Load data
    print("\n📊 Loading run data...")
    data = storage.load_run(run_id)
    
    bucket_results = data.get('bucket_results', pd.DataFrame())
    expected_detail = data.get('expected_detail', pd.DataFrame())
    actual_detail = data.get('actual_detail', pd.DataFrame())
    
    print(f"  • Bucket results: {len(bucket_results)} rows")
    print(f"  • Expected detail: {len(expected_detail)} rows")
    print(f"  • Actual detail: {len(actual_detail)} rows")
    
    # Check customer names in each DataFrame
    customer_col = CanonicalField.CUSTOMER_NAME.value
    
    print("\n" + "=" * 80)
    print("CUSTOMER NAMES IN BUCKET RESULTS")
    print("=" * 80)
    
    if customer_col in bucket_results.columns:
        unique_names = bucket_results[customer_col].dropna().unique()
        print(f"Found {len(unique_names)} unique customer names:")
        for name in sorted(unique_names)[:20]:  # Show first 20
            count = (bucket_results[customer_col] == name).sum()
            print(f"  • {name} ({count} rows)")
        if len(unique_names) > 20:
            print(f"  ... and {len(unique_names) - 20} more")
        
        # Check for empty/null values
        null_count = bucket_results[customer_col].isna().sum()
        empty_count = (bucket_results[customer_col].fillna('').astype(str).str.strip() == '').sum()
        print(f"\n  Null values: {null_count}")
        print(f"  Empty values: {empty_count}")
    else:
        print(f"⚠️  Column '{customer_col}' not found in bucket_results")
        print(f"Available columns: {list(bucket_results.columns)}")
    
    print("\n" + "=" * 80)
    print("CUSTOMER NAMES IN EXPECTED DETAIL")
    print("=" * 80)
    
    if customer_col in expected_detail.columns:
        unique_names = expected_detail[customer_col].dropna().unique()
        print(f"Found {len(unique_names)} unique customer names:")
        for name in sorted(unique_names)[:20]:
            count = (expected_detail[customer_col] == name).sum()
            print(f"  • {name} ({count} rows)")
        if len(unique_names) > 20:
            print(f"  ... and {len(unique_names) - 20} more")
        
        null_count = expected_detail[customer_col].isna().sum()
        empty_count = (expected_detail[customer_col].fillna('').astype(str).str.strip() == '').sum()
        print(f"\n  Null values: {null_count}")
        print(f"  Empty values: {empty_count}")
    else:
        print(f"⚠️  Column '{customer_col}' not found in expected_detail")
        print(f"Available columns: {list(expected_detail.columns)}")
    
    print("\n" + "=" * 80)
    print("CUSTOMER NAMES IN ACTUAL DETAIL")
    print("=" * 80)
    
    if customer_col in actual_detail.columns:
        unique_names = actual_detail[customer_col].dropna().unique()
        print(f"Found {len(unique_names)} unique customer names:")
        for name in sorted(unique_names)[:20]:
            count = (actual_detail[customer_col] == name).sum()
            print(f"  • {name} ({count} rows)")
        if len(unique_names) > 20:
            print(f"  ... and {len(unique_names) - 20} more")
        
        null_count = actual_detail[customer_col].isna().sum()
        empty_count = (actual_detail[customer_col].fillna('').astype(str).str.strip() == '').sum()
        print(f"\n  Null values: {null_count}")
        print(f"  Empty values: {empty_count}")
    else:
        print(f"⚠️  Column '{customer_col}' not found in actual_detail")
        print(f"Available columns: {list(actual_detail.columns)}")
    
    # Check for "Resident" as a placeholder
    print("\n" + "=" * 80)
    print("CHECKING FOR GENERIC PLACEHOLDERS")
    print("=" * 80)
    
    generic_patterns = ['Resident', 'Generic', 'Unknown', 'N/A', 'None']
    
    for df_name, df in [('bucket_results', bucket_results), ('expected_detail', expected_detail), ('actual_detail', actual_detail)]:
        if customer_col not in df.columns:
            continue
        print(f"\n{df_name}:")
        for pattern in generic_patterns:
            count = df[customer_col].fillna('').astype(str).str.contains(pattern, case=False, regex=False).sum()
            if count > 0:
                print(f"  ⚠️  Found '{pattern}' in {count} rows")
    
    print("\n" + "=" * 80)

if __name__ == '__main__':
    check_customer_names()
