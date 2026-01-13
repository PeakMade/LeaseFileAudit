"""
Normalization logic for data sources.
Converts raw data into canonical format.
"""
import pandas as pd
from .canonical_fields import CanonicalField


def normalize_ar_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize AR transactions to canonical format.
    
    Note: Mapping layer (mappings.py) now handles all transformations.
    This function validates and cleans the already-mapped data.
    
    Input: DataFrame with canonical field names (lowercase)
    Output: Clean DataFrame with only required canonical fields
    """
    # DEBUG: Print columns received from mapping layer
    print("\n=== AR TRANSACTIONS NORMALIZE DEBUG ===")
    print(f"Columns received: {df.columns.tolist()}")
    print(f"Shape before cleaning: {df.shape}")
    
    # Drop rows with NaT in AUDIT_MONTH (these had invalid POST_MONTH_DATE)
    before_count = len(df)
    df = df[df[CanonicalField.AUDIT_MONTH.value].notna()].copy()
    after_count = len(df)
    
    if before_count > after_count:
        print(f"[WARNING] Dropped {before_count - after_count} rows with invalid AUDIT_MONTH (NaT)")
    
    print(f"Shape after cleaning: {df.shape}")
    if len(df) > 0:
        print(f"First row sample: {df.head(1).to_dict('records')}")
    print("=" * 50 + "\n")
    
    # Validate required canonical columns exist
    required_cols = [
        CanonicalField.PROPERTY_ID.value,
        CanonicalField.LEASE_INTERVAL_ID.value,
        CanonicalField.AR_CODE_ID.value,
        CanonicalField.AUDIT_MONTH.value,
        CanonicalField.ACTUAL_AMOUNT.value,
        CanonicalField.AR_TRANSACTION_ID.value,
        CanonicalField.IS_REVERSAL.value,
        CanonicalField.POST_DATE.value
    ]
    
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required canonical columns: {missing}\n"
            f"Available columns: {df.columns.tolist()}"
        )
    
    # Return DataFrame with ONLY canonical columns (no source columns)
    return df[required_cols].copy()


def normalize_scheduled_charges(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize scheduled charges to canonical format.
    
    Note: Mapping layer handles all transformations via derived_fields.
    This function validates and cleans the already-mapped data.
    
    Input: DataFrame with canonical field names (lowercase)
    Output: Clean DataFrame with only required canonical fields
    """
    # DEBUG: Print columns received from mapping layer
    print("\n=== SCHEDULED CHARGES NORMALIZE DEBUG ===")
    print(f"Columns received: {df.columns.tolist()}")
    print(f"Shape before cleaning: {df.shape}")
    
    # Drop rows with NaT in PERIOD_START only (PERIOD_END can be NaT for one-time charges)
    before_count = len(df)
    df = df[df[CanonicalField.PERIOD_START.value].notna()].copy()
    after_count = len(df)
    
    if before_count > after_count:
        print(f"[WARNING] Dropped {before_count - after_count} rows with invalid PERIOD_START (NaT)")
    
    print(f"Shape after cleaning: {df.shape}")
    if len(df) > 0:
        print(f"First row sample: {df.head(1).to_dict('records')}")
    print("=" * 50 + "\n")
    
    # Validate required canonical columns exist
    required_cols = [
        CanonicalField.SCHEDULED_CHARGES_ID.value,
        CanonicalField.PROPERTY_ID.value,
        CanonicalField.LEASE_INTERVAL_ID.value,
        CanonicalField.AR_CODE_ID.value,
        CanonicalField.EXPECTED_AMOUNT.value,
        CanonicalField.PERIOD_START.value,
        CanonicalField.PERIOD_END.value
    ]
    
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required canonical columns: {missing}\n"
            f"Available columns: {df.columns.tolist()}"
        )
    
    # Return DataFrame with ONLY canonical columns (no source columns)
    return df[required_cols].copy()
