"""
Schema validation and canonical dataset container for Lease Audit Engine.

Provides utilities to validate DataFrame schemas against canonical field
definitions and enforce proper data types.
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
import pandas as pd

from .canonical_fields import (
    CanonicalField,
    DATE_FIELDS,
    AMOUNT_FIELDS,
    IDENTIFIER_FIELDS,
)


def validate_columns(
    df: pd.DataFrame,
    required_fields: Set[CanonicalField],
    df_name: str = "DataFrame"
) -> None:
    """
    Validate that DataFrame contains all required canonical fields.
    
    Args:
        df: DataFrame to validate
        required_fields: Set of required CanonicalField enums
        df_name: Name of DataFrame for error messages
    
    Raises:
        ValueError: If any required fields are missing
    
    Example:
        >>> validate_columns(
        ...     expected_df,
        ...     REQUIRED_EXPECTED_DETAIL_FIELDS,
        ...     "expected_detail"
        ... )
    """
    required_names = {f.value for f in required_fields}
    available_names = set(df.columns)
    missing = required_names - available_names
    
    if missing:
        raise ValueError(
            f"{df_name} is missing required canonical fields: {sorted(missing)}. "
            f"Available columns: {sorted(available_names)}"
        )


def enforce_dtypes(
    df: pd.DataFrame,
    dtype_map: Optional[Dict[CanonicalField, str]] = None,
    coerce_errors: bool = True
) -> pd.DataFrame:
    """
    Enforce canonical data types on DataFrame columns.
    
    Args:
        df: DataFrame to process
        dtype_map: Optional mapping of fields to dtypes. If None, uses default map.
        coerce_errors: If True, coerce errors to NaT/NaN instead of raising
    
    Returns:
        DataFrame with enforced dtypes
    
    Example:
        >>> df = enforce_dtypes(raw_df, get_default_dtype_map())
    """
    df = df.copy()
    
    if dtype_map is None:
        dtype_map = get_default_dtype_map()
    
    for field, dtype in dtype_map.items():
        col_name = field.value
        
        if col_name not in df.columns:
            continue
        
        try:
            if dtype.startswith('datetime'):
                # Handle datetime conversions
                df[col_name] = pd.to_datetime(
                    df[col_name],
                    errors='coerce' if coerce_errors else 'raise'
                )
            elif dtype in ('Int64', 'int64'):
                # Handle integer conversions (nullable Int64 preferred)
                df[col_name] = pd.to_numeric(
                    df[col_name],
                    errors='coerce' if coerce_errors else 'raise'
                ).astype('Int64')
            elif dtype in ('float64', 'Float64'):
                # Handle float conversions
                df[col_name] = pd.to_numeric(
                    df[col_name],
                    errors='coerce' if coerce_errors else 'raise'
                ).astype('float64')
            elif dtype == 'string':
                # Handle string conversions
                df[col_name] = df[col_name].astype('string')
            else:
                # Generic dtype conversion
                df[col_name] = df[col_name].astype(dtype)
                
        except Exception as e:
            raise ValueError(
                f"Failed to convert column '{col_name}' to dtype '{dtype}': {e}"
            )
    
    return df


def get_default_dtype_map() -> Dict[CanonicalField, str]:
    """
    Get default canonical field dtype mappings.
    
    Returns:
        Dictionary mapping CanonicalField to pandas dtype strings
    """
    dtype_map = {}
    
    # Date fields -> datetime64[ns]
    for field in DATE_FIELDS:
        dtype_map[field] = 'datetime64[ns]'
    
    # Amount fields -> float64
    for field in AMOUNT_FIELDS:
        dtype_map[field] = 'float64'
    
    # Identifier fields -> Int64 (nullable integer)
    # Some IDs might be strings, but numeric IDs use Int64
    for field in IDENTIFIER_FIELDS:
        if field in (CanonicalField.PROPERTY_ID, 
                     CanonicalField.LEASE_INTERVAL_ID,
                     CanonicalField.AR_CODE_ID):
            # Keep as-is, can be string or int depending on source
            pass
        else:
            dtype_map[field] = 'Int64'
    
    # Specific overrides
    dtype_map[CanonicalField.SCHEDULED_CHARGES_ID] = 'Int64'
    dtype_map[CanonicalField.AR_TRANSACTION_ID] = 'Int64'
    dtype_map[CanonicalField.IS_POSTED] = 'int64'
    dtype_map[CanonicalField.IS_DELETED] = 'int64'
    dtype_map[CanonicalField.IS_REVERSAL] = 'int64'
    dtype_map[CanonicalField.IS_VOID] = 'int64'
    dtype_map[CanonicalField.STATUS] = 'string'
    dtype_map[CanonicalField.MATCH_RULE] = 'string'
    dtype_map[CanonicalField.SEVERITY] = 'string'
    dtype_map[CanonicalField.CATEGORY] = 'string'
    dtype_map[CanonicalField.FINDING_ID] = 'string'
    dtype_map[CanonicalField.RUN_ID] = 'string'
    
    return dtype_map


@dataclass
class CanonicalDataSet:
    """
    Container for canonical audit datasets.
    
    This provides a typed, validated container for all datasets used in
    the audit engine. Enforces that all datasets use canonical field names.
    
    Attributes:
        expected_detail: Expanded scheduled charges (one row per bucket)
        actual_detail: Normalized AR transactions
        bucket_results: Reconciliation results at bucket grain
        findings: Generated findings (optional)
        extras: Additional datasets for future sources (lease terms, etc.)
    
    Example:
        >>> dataset = CanonicalDataSet(
        ...     expected_detail=expected_df,
        ...     actual_detail=actual_df,
        ...     bucket_results=buckets_df
        ... )
        >>> dataset.validate()
        >>> dataset.add_extra("lease_terms", lease_terms_df)
    """
    
    expected_detail: pd.DataFrame
    actual_detail: pd.DataFrame
    bucket_results: pd.DataFrame
    findings: Optional[pd.DataFrame] = None
    extras: Dict[str, pd.DataFrame] = field(default_factory=dict)
    
    def validate(self, strict: bool = True) -> None:
        """
        Validate all datasets have required canonical fields.
        
        Args:
            strict: If True, raise on validation errors. If False, log warnings.
        
        Raises:
            ValueError: If strict=True and validation fails
        """
        from .canonical_fields import (
            REQUIRED_EXPECTED_DETAIL_FIELDS,
            REQUIRED_ACTUAL_DETAIL_FIELDS,
            REQUIRED_BUCKET_RESULTS_FIELDS,
            REQUIRED_FINDING_FIELDS,
        )
        
        errors = []
        
        try:
            validate_columns(
                self.expected_detail,
                REQUIRED_EXPECTED_DETAIL_FIELDS,
                "expected_detail"
            )
        except ValueError as e:
            errors.append(str(e))
        
        try:
            validate_columns(
                self.actual_detail,
                REQUIRED_ACTUAL_DETAIL_FIELDS,
                "actual_detail"
            )
        except ValueError as e:
            errors.append(str(e))
        
        try:
            validate_columns(
                self.bucket_results,
                REQUIRED_BUCKET_RESULTS_FIELDS,
                "bucket_results"
            )
        except ValueError as e:
            errors.append(str(e))
        
        if self.findings is not None and not self.findings.empty:
            try:
                validate_columns(
                    self.findings,
                    REQUIRED_FINDING_FIELDS,
                    "findings"
                )
            except ValueError as e:
                errors.append(str(e))
        
        if errors:
            error_msg = "\n".join(errors)
            if strict:
                raise ValueError(f"CanonicalDataSet validation failed:\n{error_msg}")
            else:
                print(f"Warning: CanonicalDataSet validation issues:\n{error_msg}")
    
    def add_extra(self, name: str, df: pd.DataFrame) -> None:
        """
        Add an additional dataset for future sources.
        
        Args:
            name: Name of the dataset (e.g., "lease_terms", "residents")
            df: DataFrame with canonical field names
        
        Example:
            >>> dataset.add_extra("lease_terms", lease_terms_df)
        """
        self.extras[name] = df
    
    def get_extra(self, name: str) -> Optional[pd.DataFrame]:
        """
        Retrieve an additional dataset.
        
        Args:
            name: Name of the dataset
        
        Returns:
            DataFrame if exists, None otherwise
        """
        return self.extras.get(name)
    
    def list_extras(self) -> List[str]:
        """List names of all extra datasets."""
        return list(self.extras.keys())
    
    def summary(self) -> Dict[str, int]:
        """
        Get summary of record counts in all datasets.
        
        Returns:
            Dictionary with dataset names and row counts
        """
        summary = {
            "expected_detail": len(self.expected_detail),
            "actual_detail": len(self.actual_detail),
            "bucket_results": len(self.bucket_results),
        }
        
        if self.findings is not None:
            summary["findings"] = len(self.findings)
        
        for name, df in self.extras.items():
            summary[name] = len(df)
        
        return summary


def create_empty_canonical_df(required_fields: Set[CanonicalField]) -> pd.DataFrame:
    """
    Create an empty DataFrame with canonical field columns.
    
    Useful for initializing empty results.
    
    Args:
        required_fields: Set of CanonicalField enums to use as columns
    
    Returns:
        Empty DataFrame with canonical columns
    
    Example:
        >>> empty_expected = create_empty_canonical_df(REQUIRED_EXPECTED_DETAIL_FIELDS)
    """
    columns = [f.value for f in required_fields]
    return pd.DataFrame(columns=columns)
