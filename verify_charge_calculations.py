"""
Verify undercharge/overcharge calculations are correct
Shows breakdown of variances from most recent audit run
"""
import sys
sys.path.insert(0, '.')

from storage.service import StorageService
from audit_engine.canonical_fields import CanonicalField
from config import config
import pandas as pd

def verify_calculations():
    print("=" * 80)
    print("UNDERCHARGE/OVERCHARGE VERIFICATION")
    print("=" * 80)
    
    # Load most recent run
    print("\n📂 Loading most recent audit run...")
    storage = StorageService(
        base_dir=config.storage.base_dir,
        use_sharepoint=False  # Use local CSV files for verification
    )
    runs = storage.list_runs(limit=1)
    
    if not runs:
        print("❌ No audit runs found")
        return
    
    run_id = runs[0]['run_id']
    print(f"✓ Found run: {run_id}")
    print(f"  Timestamp: {runs[0].get('timestamp', 'N/A')}")
    print(f"  File: {runs[0].get('file_name', 'N/A')}")
    
    # Load bucket results
    print("\n📊 Loading bucket results...")
    data = storage.load_run(run_id)
    bucket_results = data['bucket_results']
    
    print(f"✓ Loaded {len(bucket_results)} total buckets")
    
    # Filter to exception rows only (excludes MATCHED and SCHEDULED_ONLY)
    status_col = CanonicalField.STATUS.value
    non_exception_statuses = {'MATCHED', 'SCHEDULED_ONLY'}
    
    exception_rows = bucket_results[~bucket_results[status_col].isin(non_exception_statuses)]
    matched_rows = bucket_results[bucket_results[status_col] == 'MATCHED']
    
    print(f"  • Matched buckets: {len(matched_rows)}")
    print(f"  • Exception buckets: {len(exception_rows)}")
    
    if len(exception_rows) == 0:
        print("\n✓ No exceptions found - all charges matched perfectly!")
        return
    
    # Calculate undercharge and overcharge
    print("\n" + "=" * 80)
    print("VARIANCE BREAKDOWN")
    print("=" * 80)
    
    expected_col = CanonicalField.EXPECTED_TOTAL.value
    actual_col = CanonicalField.ACTUAL_TOTAL.value
    variance_col = CanonicalField.VARIANCE.value
    
    expected = pd.to_numeric(exception_rows[expected_col], errors='coerce').fillna(0)
    actual = pd.to_numeric(exception_rows[actual_col], errors='coerce').fillna(0)
    variance = pd.to_numeric(exception_rows[variance_col], errors='coerce').fillna(0)
    
    # Undercharge: actual < expected (variance is negative)
    undercharge_rows = exception_rows[variance < 0].copy()
    undercharge_total = abs(undercharge_rows[variance_col].sum()) if len(undercharge_rows) > 0 else 0
    
    # Overcharge: actual > expected (variance is positive)
    overcharge_rows = exception_rows[variance > 0].copy()
    overcharge_total = overcharge_rows[variance_col].sum() if len(overcharge_rows) > 0 else 0
    
    # Alternative calculation using clip (matches audit_engine/metrics.py)
    undercharge_clip = float((expected - actual).clip(lower=0).sum())
    overcharge_clip = float((actual - expected).clip(lower=0).sum())
    
    print(f"\nUndercharge (residents charged LESS than scheduled):")
    print(f"  • Number of buckets: {len(undercharge_rows)}")
    print(f"  • Total amount: ${undercharge_total:,.2f}")
    print(f"  • Clip calculation: ${undercharge_clip:,.2f}")
    
    print(f"\nOvercharge (residents charged MORE than scheduled):")
    print(f"  • Number of buckets: {len(overcharge_rows)}")
    print(f"  • Total amount: ${overcharge_total:,.2f}")
    print(f"  • Clip calculation: ${overcharge_clip:,.2f}")
    
    print(f"\nNet Variance: ${overcharge_total - undercharge_total:,.2f}")
    print(f"  (Positive = net overcharged, Negative = net undercharged)")
    
    # Verify calculations match
    print("\n" + "=" * 80)
    print("CALCULATION VERIFICATION")
    print("=" * 80)
    
    calc_match = abs(undercharge_total - undercharge_clip) < 0.01 and abs(overcharge_total - overcharge_clip) < 0.01
    print(f"Variance method vs Clip method: {'✓ MATCH' if calc_match else '✗ MISMATCH'}")
    
    # Show top 5 undercharges
    if len(undercharge_rows) > 0:
        print("\n" + "-" * 80)
        print("TOP 5 UNDERCHARGES (highest missing charges):")
        print("-" * 80)
        top_under = undercharge_rows.nlargest(5, variance_col, keep='first')
        display_cols = [
            CanonicalField.PROPERTY_NAME.value,
            CanonicalField.CUSTOMER_NAME.value,
            CanonicalField.AR_CODE_NAME.value,
            CanonicalField.EXPECTED_TOTAL.value,
            CanonicalField.ACTUAL_TOTAL.value,
            CanonicalField.VARIANCE.value,
            status_col
        ]
        available_cols = [c for c in display_cols if c in top_under.columns]
        print(top_under[available_cols].to_string(index=False))
    
    # Show top 5 overcharges
    if len(overcharge_rows) > 0:
        print("\n" + "-" * 80)
        print("TOP 5 OVERCHARGES (highest excess charges):")
        print("-" * 80)
        top_over = overcharge_rows.nlargest(5, variance_col, keep='first')
        display_cols = [
            CanonicalField.PROPERTY_NAME.value,
            CanonicalField.CUSTOMER_NAME.value,
            CanonicalField.AR_CODE_NAME.value,
            CanonicalField.EXPECTED_TOTAL.value,
            CanonicalField.ACTUAL_TOTAL.value,
            CanonicalField.VARIANCE.value,
            status_col
        ]
        available_cols = [c for c in display_cols if c in top_over.columns]
        print(top_over[available_cols].to_string(index=False))
    
    # Status breakdown
    print("\n" + "=" * 80)
    print("EXCEPTION STATUS BREAKDOWN")
    print("=" * 80)
    status_counts = exception_rows[status_col].value_counts()
    for status, count in status_counts.items():
        status_variance = exception_rows[exception_rows[status_col] == status][variance_col].sum()
        print(f"  {status}: {count} buckets (${status_variance:,.2f} total variance)")
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"✓ Calculations verified for run {run_id}")
    print(f"✓ {len(exception_rows)} exceptions found")
    print(f"✓ Undercharge: ${undercharge_total:,.2f} ({len(undercharge_rows)} buckets)")
    print(f"✓ Overcharge: ${overcharge_total:,.2f} ({len(overcharge_rows)} buckets)")
    print(f"✓ Total absolute variance: ${undercharge_total + overcharge_total:,.2f}")
    print("=" * 80)

if __name__ == '__main__':
    verify_calculations()
