"""
Test that customer names and property names are now included in bucket_results after reconciliation
"""
import sys
sys.path.insert(0, '.')

from audit_engine.reconcile import reconcile_buckets
from audit_engine.canonical_fields import CanonicalField
from config import config
import pandas as pd

def test_customer_names():
    print("=" * 80)
    print("TEST: Customer Names and Property Names in Bucket Results")
    print("=" * 80)
    
    # Create sample expected detail with customer names
    expected_detail = pd.DataFrame([
        {
            CanonicalField.PROPERTY_ID.value: 101,
            CanonicalField.LEASE_INTERVAL_ID.value: 1001,
            CanonicalField.AR_CODE_ID.value: 154771,
            CanonicalField.AUDIT_MONTH.value: pd.Timestamp('2026-01-01'),
            CanonicalField.EXPECTED_AMOUNT.value: 1000.0,
            CanonicalField.CUSTOMER_NAME.value: 'John Doe',
            CanonicalField.LEASE_MODE.value: 'active',
        },
        {
            CanonicalField.PROPERTY_ID.value: 101,
            CanonicalField.LEASE_INTERVAL_ID.value: 1002,
            CanonicalField.AR_CODE_ID.value: 154771,
            CanonicalField.AUDIT_MONTH.value: pd.Timestamp('2026-01-01'),
            CanonicalField.EXPECTED_AMOUNT.value: 1200.0,
            CanonicalField.CUSTOMER_NAME.value: 'Jane Smith',
            CanonicalField.LEASE_MODE.value: 'active',
        },
    ])
    
    # Create sample actual detail with customer names AND property names
    actual_detail = pd.DataFrame([
        {
            CanonicalField.PROPERTY_ID.value: 101,
            CanonicalField.LEASE_INTERVAL_ID.value: 1001,
            CanonicalField.AR_CODE_ID.value: 154771,
            CanonicalField.AUDIT_MONTH.value: pd.Timestamp('2026-01-01'),
            CanonicalField.ACTUAL_AMOUNT.value: 1000.0,
            CanonicalField.CUSTOMER_NAME.value: 'John Doe',
            CanonicalField.PROPERTY_NAME.value: 'Test Property',
        },
        {
            CanonicalField.PROPERTY_ID.value: 101,
            CanonicalField.LEASE_INTERVAL_ID.value: 1002,
            CanonicalField.AR_CODE_ID.value: 154771,
            CanonicalField.AUDIT_MONTH.value: pd.Timestamp('2026-01-01'),
            CanonicalField.ACTUAL_AMOUNT.value: 1100.0,  # Mismatch
            CanonicalField.CUSTOMER_NAME.value: 'Jane Smith',
            CanonicalField.PROPERTY_NAME.value: 'Test Property',
        },
    ])
    
    print("\n✓ Created test data:")
    print(f"  Expected detail: {len(expected_detail)} rows")
    print(f"  Actual detail: {len(actual_detail)} rows")
    print(f"  Customer names in expected: {list(expected_detail[CanonicalField.CUSTOMER_NAME.value])}")
    print(f"  Customer names in actual: {list(actual_detail[CanonicalField.CUSTOMER_NAME.value])}")
    print(f"  Property names in actual: {list(actual_detail[CanonicalField.PROPERTY_NAME.value])}")
    
    # Run reconciliation
    print("\n🔄 Running reconciliation...")
    bucket_results = reconcile_buckets(
        expected_detail=expected_detail,
        actual_detail=actual_detail,
        recon_config=config.reconciliation
    )
    
    print(f"✓ Bucket results created: {len(bucket_results)} rows")
    
    # Check if CUSTOMER_NAME is in bucket_results
    customer_col = CanonicalField.CUSTOMER_NAME.value
    property_col = CanonicalField.PROPERTY_NAME.value
    
    print("\n" + "=" * 80)
    print("VERIFICATION")
    print("=" * 80)
    
    success = True
    
    if customer_col in bucket_results.columns:
        print(f"✅ SUCCESS: {customer_col} column exists in bucket_results!")
        null_count = bucket_results[customer_col].isna().sum()
        if null_count > 0:
            print(f"⚠️  Warning: {null_count} rows have null customer names")
            success = False
        else:
            print(f"✅ All rows have customer names populated!")
    else:
        print(f"❌ FAILED: {customer_col} column NOT found in bucket_results")
        success = False
    
    if property_col in bucket_results.columns:
        print(f"✅ SUCCESS: {property_col} column exists in bucket_results!")
        null_count = bucket_results[property_col].isna().sum()
        if null_count > 0:
            print(f"⚠️  Warning: {null_count} rows have null property names")
            success = False
        else:
            print(f"✅ All rows have property names populated!")
    else:
        print(f"❌ FAILED: {property_col} column NOT found in bucket_results")
        success = False
    
    if success:
        print(f"\n🎉 ALL CHECKS PASSED!")
    
    print("\n" + "=" * 80)
    print("Bucket results summary:")
    print("=" * 80)
    for idx, row in bucket_results.iterrows():
        lease_id = row[CanonicalField.LEASE_INTERVAL_ID.value]
        customer = row.get(customer_col, 'N/A')
        property_name = row.get(property_col, 'N/A')
        status = row[CanonicalField.STATUS.value]
        print(f"  • Lease {lease_id}: {customer} @ {property_name} ({status})")
    
    print("\n" + "=" * 80)
    print("Full bucket results:")
    print("=" * 80)
    print(bucket_results.to_string())
    print("=" * 80)

if __name__ == '__main__':
    test_customer_names()
