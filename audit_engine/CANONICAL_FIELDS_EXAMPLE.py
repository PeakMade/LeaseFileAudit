"""
Example: Using Canonical Fields and Schema Validation

This demonstrates how the new canonical field system works and how to
migrate existing code to use it.
"""

# ==================== Example 1: Basic Usage ====================

# OLD WAY (scattered string literals):
# df["PROPERTY_ID"]  # What if you typo? No IDE help!
# df["expected_amount"]  # Is it expected_amount or EXPECTED_AMOUNT?

# NEW WAY (canonical fields):
from audit_engine.canonical_fields import CanonicalField

# IDE autocomplete helps you discover fields
# Type-safe - enum prevents typos
df[CanonicalField.PROPERTY_ID.value]
df[CanonicalField.EXPECTED_AMOUNT.value]

# ==================== Example 2: Normalize with Mappings ====================

"""
How normalize.py uses the new mapping system:

from audit_engine.mappings import apply_source_mapping, AR_TRANSACTIONS_MAPPING
from audit_engine.canonical_fields import CanonicalField
import pandas as pd

def normalize_ar_transactions(df_raw: pd.DataFrame) -> pd.DataFrame:
    '''
    Transform raw AR data to canonical format.
    
    The mapping handles:
    - Column renaming (raw source names -> canonical fields)
    - Row filtering (IS_POSTED=1, IS_DELETED=0)
    - Derived fields (AUDIT_MONTH from POST_MONTH_DATE)
    '''
    df_canonical = apply_source_mapping(df_raw, AR_TRANSACTIONS_MAPPING)
    
    # Now ALL columns use CanonicalField names
    # No raw source column names appear here!
    
    # Work with canonical fields
    print(f"Records: {len(df_canonical)}")
    print(f"Total amount: {df_canonical[CanonicalField.ACTUAL_AMOUNT.value].sum()}")
    
    return df_canonical
"""

# ==================== Example 3: Schema Validation ====================

"""
Validate canonical DataFrames have required fields:

from audit_engine.schemas import validate_columns, enforce_dtypes
from audit_engine.canonical_fields import REQUIRED_EXPECTED_DETAIL_FIELDS

# Ensure DataFrame has all required canonical fields
validate_columns(
    expected_df,
    REQUIRED_EXPECTED_DETAIL_FIELDS,
    "expected_detail"
)

# Enforce canonical data types
expected_df = enforce_dtypes(expected_df)

# Now expected_df is guaranteed to have:
# - All required canonical fields
# - Correct data types (dates as datetime, amounts as float, etc.)
"""

# ==================== Example 4: CanonicalDataSet Container ====================

"""
Use CanonicalDataSet for type-safe dataset management:

from audit_engine.schemas import CanonicalDataSet

# Create container with validated datasets
dataset = CanonicalDataSet(
    expected_detail=expected_df,
    actual_detail=actual_df,
    bucket_results=bucket_df
)

# Validate all datasets at once
dataset.validate()

# Add future data sources easily
dataset.add_extra("lease_terms", lease_terms_df)
dataset.add_extra("residents", residents_df)

# Get summary
print(dataset.summary())
# {'expected_detail': 1500, 'actual_detail': 1480, 'bucket_results': 350, 'lease_terms': 125}
"""

# ==================== Example 5: Adding New Data Source ====================

"""
To add a new data source (e.g., Lease Terms):

1. Add new canonical fields to canonical_fields.py if needed:
   (Already have LEASE_START_DATE, LEASE_END_DATE, RENT_AMOUNT, etc.)

2. Create mapping in mappings.py:

from audit_engine.canonical_fields import CanonicalField

class LeaseTermsSourceColumns:
    '''Raw column names from Lease Terms source.'''
    LEASE_ID = "LeaseID"
    PROPERTY_NUM = "PropertyNumber"
    START_DT = "StartDate"
    END_DT = "EndDate"
    MONTHLY_RENT = "MonthlyRentAmount"

LEASE_TERMS_MAPPING = SourceMapping(
    name="lease_terms",
    required_source_columns=[
        LeaseTermsSourceColumns.LEASE_ID,
        LeaseTermsSourceColumns.PROPERTY_NUM,
        LeaseTermsSourceColumns.START_DT,
        LeaseTermsSourceColumns.END_DT,
        LeaseTermsSourceColumns.MONTHLY_RENT,
    ],
    column_transforms=[
        ColumnTransform(LeaseTermsSourceColumns.LEASE_ID, 
                       CanonicalField.LEASE_INTERVAL_ID),
        ColumnTransform(LeaseTermsSourceColumns.PROPERTY_NUM, 
                       CanonicalField.PROPERTY_ID),
        ColumnTransform(LeaseTermsSourceColumns.MONTHLY_RENT, 
                       CanonicalField.RENT_AMOUNT),
    ],
    derived_fields={
        CanonicalField.LEASE_START_DATE: lambda df: pd.to_datetime(df[LeaseTermsSourceColumns.START_DT]),
        CanonicalField.LEASE_END_DATE: lambda df: pd.to_datetime(df[LeaseTermsSourceColumns.END_DT]),
    }
)

3. Use in normalization:

def normalize_lease_terms(df_raw: pd.DataFrame) -> pd.DataFrame:
    return apply_source_mapping(df_raw, LEASE_TERMS_MAPPING)

4. Add to CanonicalDataSet:

dataset.add_extra("lease_terms", normalize_lease_terms(raw_lease_df))

5. Create new rules that use lease_terms:

class LeaseStatusRule(Rule):
    def evaluate(self, context: RuleContext) -> List[Dict[str, Any]]:
        lease_terms = context.get_source("lease_terms")
        if lease_terms is not None:
            # Check for lease-specific issues
            # Use CanonicalField enum everywhere
            expired = lease_terms[
                lease_terms[CanonicalField.LEASE_END_DATE.value] < pd.Timestamp.now()
            ]
            # Generate findings...

That's it! No changes needed to core reconcile/metrics/storage logic.
"""

# ==================== Example 6: Field Groups ====================

"""
Use predefined field groups for common operations:

from audit_engine.canonical_fields import (
    BUCKET_KEY_FIELDS,
    AMOUNT_FIELDS,
    DATE_FIELDS,
    get_field_names
)

# Get bucket key column names for groupby
bucket_cols = get_field_names(BUCKET_KEY_FIELDS)
df.groupby(list(bucket_cols)).sum()

# Select only amount columns
amount_cols = get_field_names(AMOUNT_FIELDS)
df[list(amount_cols)].describe()

# Convert all date columns
for field in DATE_FIELDS:
    col = field.value
    if col in df.columns:
        df[col] = pd.to_datetime(df[col])
"""

# ==================== Example 7: Type Safety Benefits ====================

"""
Benefits of CanonicalField enum:

1. IDE Autocomplete:
   CanonicalField.  <-- IDE shows all available fields

2. Typo Prevention:
   CanonicalField.PROPRETY_ID  <-- IDE error before you even run!
   
3. Refactoring Safety:
   If you rename a field, IDE finds all usages
   
4. Documentation:
   Hover over CanonicalField.VARIANCE and see the docstring
   
5. Discovery:
   New team member can explore CanonicalField enum to see all available fields
   
6. Validation:
   Can't accidentally use a field that doesn't exist
"""

# ==================== Migration Guide ====================

"""
Migrating existing code:

BEFORE:
    df["PROPERTY_ID"]
    df["expected_amount"]
    bucket_keys = ["PROPERTY_ID", "LEASE_INTERVAL_ID", "AR_CODE_ID", "AUDIT_MONTH"]

AFTER:
    from audit_engine.canonical_fields import CanonicalField, BUCKET_KEY_FIELDS, get_field_names
    
    df[CanonicalField.PROPERTY_ID.value]
    df[CanonicalField.EXPECTED_AMOUNT.value]
    bucket_keys = get_field_names(BUCKET_KEY_FIELDS)

The old ARColumns, BucketColumns, etc. classes still work (backward compatible)
but should be replaced with CanonicalField enum over time.
"""
