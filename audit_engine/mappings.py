"""
Source-to-canonical field mappings for Lease Audit Engine.

This module is the ONLY place where raw source column names should appear.
All other modules use CanonicalField enums exclusively.

Mappings define how to transform raw source data into canonical format:
1. Column name mapping (raw -> canonical)
2. Data type conversions
3. Value transformations (filters, calculations)
"""
from dataclasses import dataclass
from typing import Dict, List, Callable, Optional, Any
import pandas as pd

from .canonical_fields import CanonicalField


# ==================== Raw Source Column Names ====================
# These are the ONLY references to raw source column names in the entire codebase

class ARSourceColumns:
    """Raw column names from AR Transactions source."""
    PROPERTY_ID = "PROPERTY_ID"
    PROPERTY_NAME = "PROPERTY_NAME"
    LEASE_INTERVAL_ID = "LEASE_INTERVAL_ID"
    AR_CODE_ID = "AR_CODE_ID"
    AR_CODE_NAME = "AR_CODE_NAME"
    TRANSACTION_AMOUNT = "TRANSACTION_AMOUNT"
    POST_DATE = "POST_DATE"
    POST_MONTH_DATE = "POST_MONTH_DATE"
    IS_POSTED = "IS_POSTED"
    IS_DELETED = "IS_DELETED"
    IS_REVERSAL = "IS_REVERSAL"
    ID = "ID"
    GUARANTOR_NAME = "GUARANTOR_NAME"


class ScheduledSourceColumns:
    """Raw column names from Scheduled Charges source."""
    ID = "ID"
    PROPERTY_ID = "PROPERTY_ID"
    LEASE_INTERVAL_ID = "LEASE_INTERVAL_ID"
    AR_CODE_ID = "AR_CODE_ID"
    AR_CODE_NAME = "AR_CODE_NAME"
    CHARGE_AMOUNT = "CHARGE_AMOUNT"
    CHARGE_START_DATE = "CHARGE_START_DATE"
    CHARGE_END_DATE = "CHARGE_END_DATE"
    GUARANTOR_NAME = "GUARANTOR_NAME"


# ==================== Source Mapping Configuration ====================

@dataclass
class ColumnTransform:
    """Defines a transformation for a single column."""
    source_column: str
    canonical_field: CanonicalField
    transform_func: Optional[Callable[[pd.Series], pd.Series]] = None
    
    def apply(self, df: pd.DataFrame) -> pd.Series:
        """Apply transformation to source data."""
        if self.source_column not in df.columns:
            raise ValueError(f"Source column '{self.source_column}' not found in DataFrame")
        
        series = df[self.source_column]
        
        if self.transform_func is not None:
            return self.transform_func(series)
        
        return series


@dataclass
class SourceMapping:
    """
    Complete mapping configuration for a data source.
    
    Defines:
    - Source name and required columns
    - Column-to-canonical field mappings
    - Row filters
    - Derived field calculations
    
    Example usage in normalize.py:
        >>> mapping = AR_TRANSACTIONS_MAPPING
        >>> df_canonical = apply_source_mapping(df_raw, mapping)
        >>> # df_canonical now has CanonicalField columns only
    """
    
    name: str
    """Source name (e.g., 'ar_transactions')"""
    
    required_source_columns: List[str]
    """List of required raw source columns"""
    
    column_transforms: List[ColumnTransform]
    """List of column transformations"""
    
    row_filter: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None
    """Optional function to filter rows (e.g., IS_POSTED=1)"""
    
    derived_fields: Optional[Dict[CanonicalField, Callable[[pd.DataFrame], pd.Series]]] = None
    """Optional derived/calculated fields"""


# ==================== V1 Mappings: AR Transactions ====================

# AR codes posted through API - exclude from audit to prevent false exceptions
API_POSTED_AR_CODES = [157001, 155180, 156669, 155203]

def _ar_row_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter AR transactions: posted, not deleted, and exclude API-posted codes.
    
    API-posted codes (157001, 155180, 156669, 155203) are automatically posted
    and should not appear in scheduled charges. Excluding them prevents false
    exceptions from API-generated transactions.
    """
    return df[
        (df[ARSourceColumns.IS_POSTED] == 1) & 
        (df[ARSourceColumns.IS_DELETED] == 0) &
        (~df[ARSourceColumns.AR_CODE_ID].isin(API_POSTED_AR_CODES))
    ].copy()


def _ar_audit_month_calc(df: pd.DataFrame) -> pd.Series:
    """
    Calculate audit month from POST_MONTH_DATE (YYYYMMDD integer format).
    
    Args:
        df: SOURCE DataFrame (after row_filter, before column transforms)
        
    Returns:
        Series of datetime64[ns] values representing audit month
    """
    if ARSourceColumns.POST_MONTH_DATE not in df.columns:
        raise ValueError(
            f"POST_MONTH_DATE column not found in source data. "
            f"Available columns: {df.columns.tolist()}"
        )
    
    # POST_MONTH_DATE is in YYYYMMDD integer format (e.g., 20240101)
    # Convert to int first (in case it's float), then string, then parse as datetime
    result = pd.to_datetime(df[ARSourceColumns.POST_MONTH_DATE].astype(int).astype(str), format='%Y%m%d', errors='coerce')
    
    # Check for NaT values and warn
    nat_count = result.isna().sum()
    if nat_count > 0:
        print(f"[WARNING] Found {nat_count} invalid/missing POST_MONTH_DATE values")
        print(f"[WARNING] Sample of problematic values: {df[ARSourceColumns.POST_MONTH_DATE][result.isna()].head().tolist()}")
        print(f"[WARNING] These rows will be dropped during normalization")
    
    return result


AR_TRANSACTIONS_MAPPING = SourceMapping(
    name="ar_transactions",
    required_source_columns=[
        ARSourceColumns.PROPERTY_ID,
        ARSourceColumns.PROPERTY_NAME,
        ARSourceColumns.LEASE_INTERVAL_ID,
        ARSourceColumns.AR_CODE_ID,
        ARSourceColumns.AR_CODE_NAME,
        ARSourceColumns.TRANSACTION_AMOUNT,
        ARSourceColumns.POST_DATE,
        ARSourceColumns.POST_MONTH_DATE,
        ARSourceColumns.IS_POSTED,
        ARSourceColumns.IS_DELETED,
        ARSourceColumns.IS_REVERSAL,
        ARSourceColumns.ID,
        ARSourceColumns.GUARANTOR_NAME,
    ],
    column_transforms=[
        ColumnTransform(ARSourceColumns.PROPERTY_ID, CanonicalField.PROPERTY_ID),
        ColumnTransform(ARSourceColumns.PROPERTY_NAME, CanonicalField.PROPERTY_NAME),
        ColumnTransform(ARSourceColumns.LEASE_INTERVAL_ID, CanonicalField.LEASE_INTERVAL_ID),
        ColumnTransform(ARSourceColumns.AR_CODE_ID, CanonicalField.AR_CODE_ID),
        ColumnTransform(ARSourceColumns.AR_CODE_NAME, CanonicalField.AR_CODE_NAME),
        ColumnTransform(ARSourceColumns.TRANSACTION_AMOUNT, CanonicalField.ACTUAL_AMOUNT),
        ColumnTransform(ARSourceColumns.POST_DATE, CanonicalField.POST_DATE,
                       transform_func=lambda s: pd.to_datetime(s.astype(int).astype(str), format='%Y%m%d', errors='coerce')),
        ColumnTransform(ARSourceColumns.IS_POSTED, CanonicalField.IS_POSTED),
        ColumnTransform(ARSourceColumns.IS_DELETED, CanonicalField.IS_DELETED),
        ColumnTransform(ARSourceColumns.IS_REVERSAL, CanonicalField.IS_REVERSAL),
        ColumnTransform(ARSourceColumns.ID, CanonicalField.AR_TRANSACTION_ID),
        ColumnTransform(ARSourceColumns.GUARANTOR_NAME, CanonicalField.GUARANTOR_NAME),
    ],
    row_filter=_ar_row_filter,
    derived_fields={
        CanonicalField.AUDIT_MONTH: _ar_audit_month_calc,
    }
)


# ==================== V1 Mappings: Scheduled Charges ====================

def _scheduled_period_start_convert(df: pd.DataFrame) -> pd.Series:
    """
    Convert CHARGE_START_DATE to datetime.
    Handles both datetime objects (from Excel date columns) and YYYYMMDD integers.
    NULL values are converted to NaT.
    
    Args:
        df: SOURCE DataFrame (after row_filter, before column transforms)
    """
    if ScheduledSourceColumns.CHARGE_START_DATE not in df.columns:
        raise ValueError(
            f"CHARGE_START_DATE column not found. "
            f"Available columns: {df.columns.tolist()}"
        )
    
    series = df[ScheduledSourceColumns.CHARGE_START_DATE].copy()
    
    # Check if already datetime (pandas reads Excel dates as datetime)
    if pd.api.types.is_datetime64_any_dtype(series):
        return series
    
    # Otherwise, assume integer YYYYMMDD format
    mask = series.notna()
    result = pd.Series(pd.NaT, index=series.index)
    if mask.any():
        result.loc[mask] = pd.to_datetime(series[mask].astype(int).astype(str), format='%Y%m%d', errors='coerce')
    return result


def _scheduled_period_end_convert(df: pd.DataFrame) -> pd.Series:
    """
    Convert CHARGE_END_DATE to datetime.
    Handles both datetime objects (from Excel date columns) and YYYYMMDD integers.
    NULL/NaT values indicate one-time charges (will be handled by expand logic).
    
    Args:
        df: SOURCE DataFrame (after row_filter, before column transforms)
    """
    if ScheduledSourceColumns.CHARGE_END_DATE not in df.columns:
        raise ValueError(
            f"CHARGE_END_DATE column not found. "
            f"Available columns: {df.columns.tolist()}"
        )
    
    series = df[ScheduledSourceColumns.CHARGE_END_DATE].copy()
    
    # Check if already datetime (pandas reads Excel dates as datetime)
    if pd.api.types.is_datetime64_any_dtype(series):
        return series
    
    # Otherwise, assume integer YYYYMMDD format
    mask = series.notna()
    result = pd.Series(pd.NaT, index=series.index)
    if mask.any():
        result.loc[mask] = pd.to_datetime(series[mask].astype(int).astype(str), format='%Y%m%d', errors='coerce')
    return result


SCHEDULED_CHARGES_MAPPING = SourceMapping(
    name="scheduled_charges",
    required_source_columns=[
        ScheduledSourceColumns.ID,
        ScheduledSourceColumns.PROPERTY_ID,
        ScheduledSourceColumns.LEASE_INTERVAL_ID,
        ScheduledSourceColumns.AR_CODE_ID,
        ScheduledSourceColumns.AR_CODE_NAME,
        ScheduledSourceColumns.CHARGE_AMOUNT,
        ScheduledSourceColumns.CHARGE_START_DATE,
        ScheduledSourceColumns.CHARGE_END_DATE,
        ScheduledSourceColumns.GUARANTOR_NAME,
    ],
    column_transforms=[
        ColumnTransform(ScheduledSourceColumns.ID, 
                       CanonicalField.SCHEDULED_CHARGES_ID),
        ColumnTransform(ScheduledSourceColumns.PROPERTY_ID, 
                       CanonicalField.PROPERTY_ID),
        ColumnTransform(ScheduledSourceColumns.LEASE_INTERVAL_ID, 
                       CanonicalField.LEASE_INTERVAL_ID),
        ColumnTransform(ScheduledSourceColumns.AR_CODE_ID, 
                       CanonicalField.AR_CODE_ID),
        ColumnTransform(ScheduledSourceColumns.AR_CODE_NAME, 
                       CanonicalField.AR_CODE_NAME),
        ColumnTransform(ScheduledSourceColumns.CHARGE_AMOUNT, 
                       CanonicalField.EXPECTED_AMOUNT),
        ColumnTransform(ScheduledSourceColumns.GUARANTOR_NAME, 
                       CanonicalField.GUARANTOR_NAME),
    ],
    row_filter=None,  # No filtering for scheduled charges
    derived_fields={
        CanonicalField.PERIOD_START: _scheduled_period_start_convert,
        CanonicalField.PERIOD_END: _scheduled_period_end_convert,
    }
)


# ==================== Mapping Application Utilities ====================

def apply_source_mapping(df: pd.DataFrame, mapping: SourceMapping) -> pd.DataFrame:
    """
    Apply a source mapping to transform raw data to canonical format.
    
    This is the primary function used by normalize.py to convert source data.
    
    Process:
    1. Validate required source columns exist
    2. Apply row filter (if specified) - filters on SOURCE data
    3. Apply column transformations - transforms SOURCE columns to CANONICAL columns
    4. Apply derived field calculations - calculates new fields from SOURCE data
    
    Args:
        df: Raw source DataFrame
        mapping: SourceMapping configuration
    
    Returns:
        DataFrame with canonical field names
    
    Example:
        >>> from audit_engine.mappings import apply_source_mapping, AR_TRANSACTIONS_MAPPING
        >>> df_canonical = apply_source_mapping(df_raw, AR_TRANSACTIONS_MAPPING)
        >>> # Now use CanonicalField enums to reference columns:
        >>> df_canonical[CanonicalField.ACTUAL_AMOUNT.value]
    """
    print(f"\n[MAPPING DEBUG] Processing source: {mapping.name}")
    print(f"[MAPPING DEBUG] Input shape: {df.shape}")
    print(f"[MAPPING DEBUG] Input columns: {df.columns.tolist()}")
    
    # Validate required columns
    missing = [col for col in mapping.required_source_columns if col not in df.columns]
    if missing:
        raise ValueError(
            f"Source '{mapping.name}' is missing required columns: {missing}. \n"
            f"Available columns: {df.columns.tolist()}"
        )
    
    df = df.copy()
    
    # Apply row filter if specified
    if mapping.row_filter is not None:
        original_count = len(df)
        df = mapping.row_filter(df)
        filtered_count = len(df)
        print(f"[MAPPING DEBUG] Row filter applied: {original_count} -> {filtered_count} rows ({original_count - filtered_count} filtered out)")
    
    # Apply column transformations
    result_data = {}
    for transform in mapping.column_transforms:
        try:
            result_data[transform.canonical_field.value] = transform.apply(df)
        except Exception as e:
            raise ValueError(
                f"Error transforming column '{transform.source_column}' -> '{transform.canonical_field.value}': {e}"
            )
    
    result_df = pd.DataFrame(result_data)
    print(f"[MAPPING DEBUG] After column transforms: {result_df.shape}, columns: {result_df.columns.tolist()}")
    
    # Apply derived fields if specified
    if mapping.derived_fields is not None:
        for canonical_field, calc_func in mapping.derived_fields.items():
            try:
                result_df[canonical_field.value] = calc_func(df)
                print(f"[MAPPING DEBUG] Added derived field: '{canonical_field.value}'")
            except Exception as e:
                raise ValueError(
                    f"Error calculating derived field '{canonical_field.value}': {e}\n"
                    f"Calculator function: {calc_func.__name__}\n"
                    f"Source columns available: {df.columns.tolist()}"
                )
    
    print(f"[MAPPING DEBUG] Final output: {result_df.shape}, columns: {result_df.columns.tolist()}")
    print(f"[MAPPING DEBUG] Sample first row: {result_df.head(1).to_dict('records')}\n")
    
    return result_df


# ==================== Bucket Key Helper ====================
# Export bucket key columns for convenience
BUCKET_KEY_COLUMNS = [
    CanonicalField.PROPERTY_ID.value,
    CanonicalField.LEASE_INTERVAL_ID.value,
    CanonicalField.AR_CODE_ID.value,
    CanonicalField.AUDIT_MONTH.value
]
