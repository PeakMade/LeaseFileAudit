"""List all available audit runs and their data"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pathlib import Path
import pandas as pd

# Check local CSV files first
runs_dir = Path("instance/runs")
if runs_dir.exists():
    print(f"\nLocal CSV Runs in {runs_dir}:")
    print(f"="*80)
    for run_folder in sorted(runs_dir.iterdir(), reverse=True):
        if run_folder.is_dir():
            bucket_file = run_folder / "bucket_results.csv"
            if bucket_file.exists():
                df = pd.read_csv(bucket_file)
                print(f"\n{run_folder.name}:")
                print(f"   - bucket_results.csv: {len(df)} rows")
                if 'property_id' in df.columns:
                    properties = df['property_id'].unique()
                    print(f"   - Properties: {len(properties)}")
                    if 771903 in properties:
                        prop_data = df[df['property_id'] == 771903]
                        print(f"   - CLEMSON EDGE (771903): {len(prop_data)} rows")
                        if 'ar_code_id' in prop_data.columns:
                            base_rent = prop_data[prop_data['ar_code_id'].astype(str).str.strip() == '154771']
                            print(f"     * Base rent (154771): {len(base_rent)} rows")
else:
    print(f"No local runs directory found at {runs_dir}")

print(f"\n" + "="*80)
