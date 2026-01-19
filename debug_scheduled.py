import pandas as pd
import sys
sys.path.insert(0, '.')

from audit_engine.mappings import SCHEDULED_CHARGES_MAPPING, apply_source_mapping

# Load the Excel file
sheets = pd.read_excel('instance/runs/run_20260119_172255/EXPANDED_AR_SC.xlsx', sheet_name=None)

# Find SC sheet
sc_sheet_name = [name for name in sheets.keys() if 'SC_TRANS' in name.upper()][0]
df = sheets[sc_sheet_name]

print(f"Sheet found: {sc_sheet_name}")
print(f"Total rows: {len(df)}")
print(f"\n=== BEFORE MAPPING ===")
print(f"CHARGE_START_DATE dtype: {df['CHARGE_START_DATE'].dtype}")
print(f"CHARGE_END_DATE dtype: {df['CHARGE_END_DATE'].dtype}")
print(f"First 3 CHARGE_START_DATE values: {df['CHARGE_START_DATE'].head(3).tolist()}")
print(f"First 3 CHARGE_END_DATE values: {df['CHARGE_END_DATE'].head(3).tolist()}")

# Apply mapping
try:
    canonical_df = apply_source_mapping(df, SCHEDULED_CHARGES_MAPPING)
    print(f"\n=== AFTER MAPPING ===")
    print(f"Canonical rows: {len(canonical_df)}")
    print(f"Canonical columns: {canonical_df.columns.tolist()}")
    print(f"\nFirst 3 rows:")
    print(canonical_df[['SCHEDULED_CHARGES_ID', 'PROPERTY_ID', 'LEASE_INTERVAL_ID', 'PERIOD_START', 'PERIOD_END', 'expected_amount']].head(3))
    print(f"\nPERIOD_START NULL count: {canonical_df['PERIOD_START'].isna().sum()}")
    print(f"PERIOD_END NULL count: {canonical_df['PERIOD_END'].isna().sum()}")
except Exception as e:
    print(f"\n=== ERROR ===")
    print(f"{type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

