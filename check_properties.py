"""
Check what properties are in the most recent audit run
"""
import sys
sys.path.insert(0, '.')

from storage.service import StorageService
from audit_engine.canonical_fields import CanonicalField
from config import config
import pandas as pd

def check_properties():
    print("=" * 80)
    print("PROPERTY CHECK")
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
    
    # Check properties in each DataFrame
    prop_id_col = CanonicalField.PROPERTY_ID.value
    prop_name_col = CanonicalField.PROPERTY_NAME.value
    
    for df_name, df in [('bucket_results', bucket_results), ('expected_detail', expected_detail), ('actual_detail', actual_detail)]:
        print("\n" + "=" * 80)
        print(f"PROPERTIES IN {df_name.upper()}")
        print("=" * 80)
        
        if prop_id_col in df.columns:
            unique_prop_ids = df[prop_id_col].dropna().unique()
            print(f"Found {len(unique_prop_ids)} unique property IDs:")
            for prop_id in sorted(unique_prop_ids):
                count = (df[prop_id_col] == prop_id).sum()
                prop_name = "N/A"
                if prop_name_col in df.columns:
                    prop_name_values = df[df[prop_id_col] == prop_id][prop_name_col].dropna().unique()
                    if len(prop_name_values) > 0:
                        prop_name = prop_name_values[0]
                print(f"  • Property ID {int(prop_id)}: {prop_name} ({count} rows)")
        else:
            print(f"⚠️  Column '{prop_id_col}' not found")
    
    print("\n" + "=" * 80)

if __name__ == '__main__':
    check_properties()
