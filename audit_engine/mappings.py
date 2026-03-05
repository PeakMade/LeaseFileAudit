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
from functools import lru_cache
from pathlib import Path
import json
import os
import pandas as pd

from .canonical_fields import CanonicalField


# ==================== Raw Source Column Names ====================
# These are the ONLY references to raw source column names in the entire codebase

class ARSourceColumns:
    """Raw column names from AR Transactions source."""
    PROPERTY_ID = "PROPERTY_ID"
    PROPERTY_NAME = "PROPERTY_NAME"
    LEASE_ID = "LEASE_ID"
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
    CUSTOMER_NAME = "CUSTOMER_NAME"
    CUSTOMER_ID = "CUSTOMER_ID"
    GUARANTOR_NAME = "GUARANTOR_NAME"
    FLAG_ACTIVE_LEASE_INTERVAL = "FLAG_ACTIVE_LEASE_INTERVAL"
    # Critical reconciliation linking field
    SCHEDULED_CHARGE_ID = "SCHEDULED_CHARGE_ID"


class ScheduledSourceColumns:
    """Raw column names from Scheduled Charges source."""
    ID = "ID"
    SCHEDULED_CHARGE_ID = "SCHEDULED_CHARGE_ID"
    PROPERTY_ID = "PROPERTY_ID"
    LEASE_ID = "LEASE_ID"
    LEASE_INTERVAL_ID = "LEASE_INTERVAL_ID"
    AR_CODE_ID = "AR_CODE_ID"
    AR_CODE_NAME = "AR_CODE_NAME"
    CHARGE_AMOUNT = "CHARGE_AMOUNT"
    CHARGE_START_DATE = "CHARGE_START_DATE"
    CHARGE_END_DATE = "CHARGE_END_DATE"
    GUARANTOR_NAME = "GUARANTOR_NAME"
    CUSTOMER_NAME = "CUSTOMER_NAME"
    CUSTOMER_ID = "CUSTOMER_ID"
    DELETED_ON = "DELETED_ON"
    FLAG_ACTIVE_LEASE_INTERVAL = "FLAG_ACTIVE_LEASE_INTERVAL"
    # Critical reconciliation filter fields
    IS_UNSELECTED_QUOTE = "IS_UNSELECTED_QUOTE"
    IS_CACHED_TO_LEASE = "IS_CACHED_TO_LEASE"
    POSTED_THROUGH_DATE = "POSTED_THROUGH_DATE"
    LAST_POSTED_ON = "LAST_POSTED_ON"
    # Billing frequency fields
    AR_CASCADE_ID = "AR_CASCADE_ID"
    AR_TRIGGER_ID = "AR_TRIGGER_ID"
    SCHEDULED_CHARGE_TYPE_ID = "SCHEDULED_CHARGE_TYPE_ID"


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

# AR codes posted through API or timed/external charges - exclude from audit to prevent false exceptions
# These codes should NOT appear in scheduled charges; if they do, they're flagged as "TIMED_OR_EXTERNAL_CHARGE"
API_POSTED_AR_CODES = [
    155023, 154776, 155217, 154777, 155018, 
    155099, 155022, 154785, 155049, 155040, 155015, 
    155017, 155176, 155203, 155053, 154787, 155073, 155083, 155028, 155202, 154774, 155030, 155037, 154776, 156669
]

API_POSTED_AR_CODES_SET = {int(code) for code in API_POSTED_AR_CODES}
API_POSTED_AR_CODES_TEXT_SET = {str(code) for code in API_POSTED_AR_CODES_SET}


def _normalize_ar_code_token(value: Any) -> str | None:
    """Normalize AR code identifiers to comparable string tokens."""
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    numeric = pd.to_numeric(pd.Series([value]), errors='coerce').iloc[0]
    if pd.notna(numeric):
        try:
            return str(int(float(numeric)))
        except Exception:
            pass

    return text


def _normalize_ar_code_series(series: pd.Series) -> pd.Series:
    """Vectorized AR code normalization helper."""
    normalized = series.astype(str).str.strip()
    numeric_values = pd.to_numeric(series, errors='coerce')
    numeric_mask = numeric_values.notna()

    if numeric_mask.any():
        normalized_numeric = numeric_values[numeric_mask].astype(float).astype(int).astype(str)
        normalized.loc[numeric_mask] = normalized_numeric

    normalized = normalized.replace({'': pd.NA, 'nan': pd.NA, 'None': pd.NA, '<NA>': pd.NA})
    return normalized


@lru_cache(maxsize=1)
def _load_ar_code_reference_map() -> tuple[dict[str, str], set[str], str]:
    """Load AR code -> display name reference map from project JSON."""
    repo_root = Path(__file__).resolve().parent.parent
    configured_path = os.getenv('AR_CODE_NAME_USAGE_MAP_PATH')
    map_path = Path(configured_path) if configured_path else (repo_root / 'ar_code_name_usage_map.json')

    if not map_path.exists():
        print(f"[AR CODE MAP] Reference file not found at {map_path}; skipping AR name enrichment")
        return {}, set(), str(map_path)

    try:
        payload = json.loads(map_path.read_text(encoding='utf-8-sig'))
    except Exception as exc:
        print(f"[AR CODE MAP] Failed to load {map_path}: {exc}")
        return {}, set(), str(map_path)

    mapping_section = payload.get('mapping') if isinstance(payload, dict) else None
    if not isinstance(mapping_section, dict):
        print(f"[AR CODE MAP] Invalid format in {map_path}; expected top-level 'mapping' object")
        return {}, set(), str(map_path)

    name_by_code: dict[str, str] = {}
    allowed_codes: set[str] = set()

    for raw_code, entry in mapping_section.items():
        code_key = _normalize_ar_code_token(raw_code)
        if not code_key:
            continue

        allowed_codes.add(code_key)

        name_value = ''
        if isinstance(entry, dict):
            name_value = str(entry.get('name') or '').strip()
        elif isinstance(entry, str):
            name_value = entry.strip()

        if name_value:
            name_by_code[code_key] = name_value

    print(
        f"[AR CODE MAP] Loaded {len(allowed_codes)} allowed AR codes "
        f"({len(name_by_code)} names) from {map_path}"
    )
    return name_by_code, allowed_codes, str(map_path)


def _apply_ar_code_reference_map(result_df: pd.DataFrame, mapping_name: str) -> pd.DataFrame:
    """Apply AR code name enrichment and unknown-code diagnostics to mapped canonical data."""
    ar_code_col = CanonicalField.AR_CODE_ID.value
    ar_name_col = CanonicalField.AR_CODE_NAME.value

    if ar_code_col not in result_df.columns:
        return result_df

    name_by_code, allowed_codes, map_path = _load_ar_code_reference_map()
    if not allowed_codes:
        return result_df

    normalized_codes = _normalize_ar_code_series(result_df[ar_code_col])

    if ar_name_col not in result_df.columns:
        result_df[ar_name_col] = pd.NA

    mapped_names = normalized_codes.map(name_by_code)
    mapped_mask = mapped_names.notna()
    if mapped_mask.any():
        result_df.loc[mapped_mask, ar_name_col] = mapped_names.loc[mapped_mask]
        print(f"[AR CODE MAP] Applied {int(mapped_mask.sum())} AR code names for source '{mapping_name}'")

    unknown_mask = normalized_codes.notna() & ~normalized_codes.isin(allowed_codes)
    unknown_values = sorted(set(normalized_codes[unknown_mask].dropna().tolist()))
    if unknown_values:
        sample = unknown_values[:10]
        print(
            f"[AR CODE MAP][WARNING] Found {len(unknown_values)} unknown AR code(s) in source '{mapping_name}' "
            f"not present in reference map ({map_path}). Sample: {sample}"
        )

    return result_df


def _build_api_posted_code_mask(series: pd.Series) -> pd.Series:
    """Return True where AR code is one of the API-posted codes, robust to str/float/int input."""
    numeric_values = pd.to_numeric(series, errors='coerce')
    numeric_mask = numeric_values.isin(API_POSTED_AR_CODES_SET)
    text_mask = series.astype(str).str.strip().isin(API_POSTED_AR_CODES_TEXT_SET)
    return numeric_mask | text_mask

def _ar_row_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter AR transactions: posted, exclude API-posted codes, and only active lease intervals.
    
    IMPORTANT: We now KEEP deleted/reversed transactions so they can be matched to scheduled charges.
    They will be flagged as special variance types during reconciliation (REVERSED_BILLING).
    This prevents false "SCHEDULED_NOT_BILLED" flags when a charge was billed but then reversed.
    
    API-posted codes (157001, 155180, 156669, 155203) are automatically posted
    and should not appear in scheduled charges. Excluding them prevents false
    exceptions from API-generated transactions.
    
    FLAG_ACTIVE_LEASE_INTERVAL = 1 indicates an active lease interval.
    Only active lease intervals should be audited.
    """
    # DEBUG: Check data types and values
    print(f"\n[AR FILTER DEBUG] Total AR transactions: {len(df)}")
    print(f"[AR FILTER DEBUG] IS_POSTED type: {df[ARSourceColumns.IS_POSTED].dtype}, unique: {df[ARSourceColumns.IS_POSTED].unique()}")
    print(f"[AR FILTER DEBUG] IS_DELETED type: {df[ARSourceColumns.IS_DELETED].dtype}, unique: {df[ARSourceColumns.IS_DELETED].unique()}")
    print(f"[AR FILTER DEBUG] IS_REVERSAL type: {df[ARSourceColumns.IS_REVERSAL].dtype}, unique: {df[ARSourceColumns.IS_REVERSAL].unique()}")
    
    # Count how many have each flag
    posted_count = (df[ARSourceColumns.IS_POSTED].astype(float) == 1).sum()
    deleted_count = (df[ARSourceColumns.IS_DELETED].astype(float) == 1).sum()
    reversal_count = (df[ARSourceColumns.IS_REVERSAL].astype(float) == 1).sum()
    
    print(f"[AR FILTER DEBUG] IS_POSTED == 1: {posted_count}/{len(df)}")
    print(f"[AR FILTER DEBUG] IS_DELETED == 1: {deleted_count}/{len(df)} (KEEPING for reconciliation)")
    print(f"[AR FILTER DEBUG] IS_REVERSAL == 1: {reversal_count}/{len(df)} (KEEPING for reconciliation)")
    
    # Handle potential data type mismatches (sometimes Excel reads as float or string)
    # ONLY filter by IS_POSTED - KEEP deleted/reversed for matching
    mask = (df[ARSourceColumns.IS_POSTED].astype(float) == 1)
    
    # Exclude API-posted AR codes - these are automatically posted and shouldn't be audited
    if ARSourceColumns.AR_CODE_ID in df.columns:
        api_posted_mask = _build_api_posted_code_mask(df[ARSourceColumns.AR_CODE_ID])
        filtered_api_codes = int(api_posted_mask.sum())
        if filtered_api_codes > 0:
            print(f"[FILTER] Excluding {filtered_api_codes} AR transactions with API-posted AR codes: {API_POSTED_AR_CODES}")
        mask = mask & ~api_posted_mask
    
    # Inactive lease interval filter temporarily disabled.
    if ARSourceColumns.FLAG_ACTIVE_LEASE_INTERVAL in df.columns:
        inactive_count = (pd.to_numeric(df[ARSourceColumns.FLAG_ACTIVE_LEASE_INTERVAL], errors='coerce') != 1).sum()
        if inactive_count > 0:
            print(f"[FILTER] Inactive lease interval filter disabled in AR source; retaining {inactive_count} inactive rows")
    
    result = df[mask].copy()
    print(f"[AR FILTER DEBUG] After filtering: {len(result)}/{len(df)} rows ({len(df) - len(result)} filtered out)")
    print(f"[AR FILTER DEBUG] Deleted/reversed transactions KEPT for reconciliation: {deleted_count + reversal_count}")
    
    return result


def _ar_audit_month_calc(df: pd.DataFrame) -> pd.Series:
    """
    Calculate audit month from POST_DATE (YYYYMMDD integer format).
    Normalizes to first day of the month to match with scheduled charges expansion.
    
    Args:
        df: SOURCE DataFrame (after row_filter, before column transforms)
        
    Returns:
        Series of datetime64[ns] values representing first day of audit month
    """
    if ARSourceColumns.POST_DATE not in df.columns:
        raise ValueError(
            f"POST_DATE column not found in source data. "
            f"Available columns: {df.columns.tolist()}"
        )
    
    # POST_DATE is in YYYYMMDD integer format (e.g., 20250808)
    # Convert to datetime, then normalize to first day of month
    dates = pd.to_datetime(df[ARSourceColumns.POST_DATE].astype(int).astype(str), format='%Y%m%d', errors='coerce')
    
    # Normalize to first day of month (e.g., 2025-08-08 -> 2025-08-01)
    result = dates.dt.to_period('M').dt.to_timestamp()
    
    # Check for NaT values and warn
    nat_count = result.isna().sum()
    if nat_count > 0:
        print(f"[WARNING] Found {nat_count} invalid/missing POST_DATE values")
        print(f"[WARNING] Sample of problematic values: {df[ARSourceColumns.POST_DATE][result.isna()].head().tolist()}")
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
        ARSourceColumns.CUSTOMER_NAME,
        ARSourceColumns.GUARANTOR_NAME,
        # LEASE_ID and CUSTOMER_ID are optional in AR - will fall back to scheduled charges
        # SCHEDULED_CHARGE_ID is optional - not all AR transactions link to scheduled charges
    ],
    column_transforms=[
        ColumnTransform(ARSourceColumns.PROPERTY_ID, CanonicalField.PROPERTY_ID),
        ColumnTransform(ARSourceColumns.PROPERTY_NAME, CanonicalField.PROPERTY_NAME),
        ColumnTransform(ARSourceColumns.LEASE_ID, CanonicalField.LEASE_ID),
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
        ColumnTransform(ARSourceColumns.CUSTOMER_NAME, CanonicalField.CUSTOMER_NAME),
        ColumnTransform(ARSourceColumns.CUSTOMER_ID, CanonicalField.CUSTOMER_ID),
        ColumnTransform(ARSourceColumns.GUARANTOR_NAME, CanonicalField.GUARANTOR_NAME),
        ColumnTransform(ARSourceColumns.SCHEDULED_CHARGE_ID, CanonicalField.SCHEDULED_CHARGE_ID_LINK),
        # Add LEASE_ID and CUSTOMER_ID if columns exist (optional for AR, will fall back to scheduled)
    ],
    row_filter=_ar_row_filter,
    derived_fields={
        CanonicalField.AUDIT_MONTH: _ar_audit_month_calc,
    }
)


# ==================== V1 Mappings: Scheduled Charges ====================

def _scheduled_row_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter scheduled charges to identify ACTIVE records that should generate billings.
    
    STEP 1 of Reconciliation Framework: Filter Active Records
    ---------------------------------------------------------
    Include scheduled charges WHERE:
      - IS_UNSELECTED_QUOTE != 1 (exclude unselected quotes - they should never bill)
      - DELETED_ON IS NULL (exclude deleted charges)
      - IS_CACHED_TO_LEASE = 1 (only active charges cached to lease)
      - FLAG_ACTIVE_LEASE_INTERVAL = 1 (only active lease intervals)
      - AR_CODE_ID NOT IN API_POSTED_AR_CODES (exclude API-posted charges)
    
    This ensures we only compare billings against charges that SHOULD have been billed.
    """
    mask = pd.Series(True, index=df.index)

    def _flag_is_one(series: pd.Series) -> pd.Series:
        """Robustly evaluate boolean-style numeric flags equal to 1."""
        numeric = pd.to_numeric(series, errors='coerce')
        return numeric == 1
    
    # Exclude API-posted AR codes - these are automatically posted and shouldn't be in scheduled charges
    if ScheduledSourceColumns.AR_CODE_ID in df.columns:
        api_posted_mask = _build_api_posted_code_mask(df[ScheduledSourceColumns.AR_CODE_ID])
        filtered_api_codes = int(api_posted_mask.sum())
        if filtered_api_codes > 0:
            print(f"[FILTER] Excluding {filtered_api_codes} scheduled charges with API-posted AR codes: {API_POSTED_AR_CODES}")
        mask = mask & ~api_posted_mask
    
    # CRITICAL: Exclude unselected quotes (IS_UNSELECTED_QUOTE = 1)
    # These are from quotes the tenant didn't select, so they should never appear in AR
    if ScheduledSourceColumns.IS_UNSELECTED_QUOTE in df.columns:
        selected_mask = ~_flag_is_one(df[ScheduledSourceColumns.IS_UNSELECTED_QUOTE])
        mask = mask & selected_mask
        filtered_quotes = (~selected_mask).sum()
        if filtered_quotes > 0:
            print(f"[FILTER] Excluded {filtered_quotes} unselected quote records")
    
    # Exclude deleted scheduled charges (DELETED_ON is not null)
    if ScheduledSourceColumns.DELETED_ON in df.columns:
        deleted_col = df[ScheduledSourceColumns.DELETED_ON]
        is_blank_or_null = deleted_col.isna() | (deleted_col.astype(str).str.strip() == '')
        mask = mask & is_blank_or_null
        filtered_deleted = (~is_blank_or_null).sum()
        if filtered_deleted > 0:
            print(f"[FILTER] Excluded {filtered_deleted} deleted scheduled charge records")

    # Exclude scheduled charges that were never posted.
    # Example source value: "Deleted - Never Posted".
    if ScheduledSourceColumns.POSTED_THROUGH_DATE in df.columns:
        posted_through = df[ScheduledSourceColumns.POSTED_THROUGH_DATE].fillna('').astype(str).str.strip().str.lower()
        deleted_never_posted_mask = posted_through.str.contains('deleted', na=False) & posted_through.str.contains('never posted', na=False)
        mask = mask & ~deleted_never_posted_mask
        filtered_never_posted = deleted_never_posted_mask.sum()
        if filtered_never_posted > 0:
            print(f"[FILTER] Excluded {filtered_never_posted} scheduled charges marked as deleted/never-posted")
    
    # Only include charges cached to lease (IS_CACHED_TO_LEASE = 1)
    if ScheduledSourceColumns.IS_CACHED_TO_LEASE in df.columns:
        cached_mask = _flag_is_one(df[ScheduledSourceColumns.IS_CACHED_TO_LEASE])
        mask = mask & cached_mask
        filtered_not_cached = (~cached_mask).sum()
        if filtered_not_cached > 0:
            print(f"[FILTER] Excluded {filtered_not_cached} not-cached-to-lease records")
    
    # Inactive lease interval filter temporarily disabled.
    if ScheduledSourceColumns.FLAG_ACTIVE_LEASE_INTERVAL in df.columns:
        active_mask = _flag_is_one(df[ScheduledSourceColumns.FLAG_ACTIVE_LEASE_INTERVAL])
        filtered_inactive = (~active_mask).sum()
        if filtered_inactive > 0:
            print(f"[FILTER] Inactive lease interval filter disabled in scheduled source; retaining {filtered_inactive} inactive rows")
    
    result = df[mask].copy()
    print(f"[FILTER] Scheduled charges: {len(df)} total -> {len(result)} active (filtered {len(df) - len(result)})")
    return result


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

    # Attempt 1: generic parser (handles datetime strings and datetime objects)
    result = pd.to_datetime(series, errors='coerce')

    # Attempt 2: numeric fallbacks for values not parsed above
    unresolved_mask = result.isna() & series.notna()
    if unresolved_mask.any():
        numeric_values = pd.to_numeric(series[unresolved_mask], errors='coerce')

        # 2a: YYYYMMDD integers
        yyyymmdd_mask = numeric_values.between(19000101, 21001231)
        if yyyymmdd_mask.any():
            parsed_yyyymmdd = pd.to_datetime(
                numeric_values[yyyymmdd_mask].astype('Int64').astype(str),
                format='%Y%m%d',
                errors='coerce'
            )
            result.loc[parsed_yyyymmdd.index] = parsed_yyyymmdd

        # 2b: Excel serial date numbers
        excel_mask = numeric_values.notna() & ~yyyymmdd_mask
        if excel_mask.any():
            parsed_excel = pd.to_datetime(
                numeric_values[excel_mask],
                unit='D',
                origin='1899-12-30',
                errors='coerce'
            )
            result.loc[parsed_excel.index] = parsed_excel

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

    # Attempt 1: generic parser (handles datetime strings and datetime objects)
    result = pd.to_datetime(series, errors='coerce')

    # Attempt 2: numeric fallbacks for values not parsed above
    unresolved_mask = result.isna() & series.notna()
    if unresolved_mask.any():
        numeric_values = pd.to_numeric(series[unresolved_mask], errors='coerce')

        # 2a: YYYYMMDD integers
        yyyymmdd_mask = numeric_values.between(19000101, 21001231)
        if yyyymmdd_mask.any():
            parsed_yyyymmdd = pd.to_datetime(
                numeric_values[yyyymmdd_mask].astype('Int64').astype(str),
                format='%Y%m%d',
                errors='coerce'
            )
            result.loc[parsed_yyyymmdd.index] = parsed_yyyymmdd

        # 2b: Excel serial date numbers
        excel_mask = numeric_values.notna() & ~yyyymmdd_mask
        if excel_mask.any():
            parsed_excel = pd.to_datetime(
                numeric_values[excel_mask],
                unit='D',
                origin='1899-12-30',
                errors='coerce'
            )
            result.loc[parsed_excel.index] = parsed_excel

    return result


SCHEDULED_CHARGES_MAPPING = SourceMapping(
    name="scheduled_charges",
    required_source_columns=[
        ScheduledSourceColumns.ID,
        ScheduledSourceColumns.PROPERTY_ID,
        ScheduledSourceColumns.LEASE_ID,
        ScheduledSourceColumns.LEASE_INTERVAL_ID,
        ScheduledSourceColumns.AR_CODE_ID,
        ScheduledSourceColumns.AR_CODE_NAME,
        ScheduledSourceColumns.CHARGE_AMOUNT,
        ScheduledSourceColumns.CHARGE_START_DATE,
        ScheduledSourceColumns.CHARGE_END_DATE,
        ScheduledSourceColumns.GUARANTOR_NAME,
        ScheduledSourceColumns.CUSTOMER_NAME,
        ScheduledSourceColumns.CUSTOMER_ID,
    ],
    column_transforms=[
        ColumnTransform(ScheduledSourceColumns.ID, 
                       CanonicalField.SCHEDULED_CHARGES_ID),
        ColumnTransform(ScheduledSourceColumns.SCHEDULED_CHARGE_ID,
                       CanonicalField.SCHEDULED_CHARGE_ID),
        ColumnTransform(ScheduledSourceColumns.PROPERTY_ID, 
                       CanonicalField.PROPERTY_ID),
        ColumnTransform(ScheduledSourceColumns.LEASE_ID, 
                       CanonicalField.LEASE_ID),
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
        ColumnTransform(ScheduledSourceColumns.CUSTOMER_NAME, 
                       CanonicalField.CUSTOMER_NAME),
        ColumnTransform(ScheduledSourceColumns.CUSTOMER_ID, 
                       CanonicalField.CUSTOMER_ID),
        # Reconciliation filtering and matching fields
        ColumnTransform(ScheduledSourceColumns.IS_UNSELECTED_QUOTE, 
                       CanonicalField.IS_UNSELECTED_QUOTE),
        ColumnTransform(ScheduledSourceColumns.IS_CACHED_TO_LEASE, 
                       CanonicalField.IS_CACHED_TO_LEASE),
        ColumnTransform(ScheduledSourceColumns.POSTED_THROUGH_DATE, 
                       CanonicalField.POSTED_THROUGH_DATE),
        ColumnTransform(ScheduledSourceColumns.LAST_POSTED_ON, 
                       CanonicalField.LAST_POSTED_ON),
        ColumnTransform(ScheduledSourceColumns.AR_CASCADE_ID, 
                       CanonicalField.AR_CASCADE_ID),
        ColumnTransform(ScheduledSourceColumns.AR_TRIGGER_ID, 
                       CanonicalField.AR_TRIGGER_ID),
        ColumnTransform(ScheduledSourceColumns.SCHEDULED_CHARGE_TYPE_ID, 
                       CanonicalField.SCHEDULED_CHARGE_TYPE_ID),
    ],
    row_filter=_scheduled_row_filter,  # Filter out deleted scheduled charges
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
        if transform.source_column not in df.columns:
            if transform.source_column in mapping.required_source_columns:
                raise ValueError(
                    f"Source '{mapping.name}' is missing required column during transform: {transform.source_column}"
                )

            print(
                f"[MAPPING DEBUG] Skipping optional column transform: "
                f"'{transform.source_column}' -> '{transform.canonical_field.value}' (column not present)"
            )
            continue

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

    result_df = _apply_ar_code_reference_map(result_df, mapping.name)
    
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
