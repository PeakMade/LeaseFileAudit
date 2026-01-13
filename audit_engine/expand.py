"""
Month expansion logic for scheduled charges.
"""
import pandas as pd
from typing import List
from .canonical_fields import CanonicalField


def generate_month_range(start_date: pd.Timestamp, end_date: pd.Timestamp) -> List[pd.Timestamp]:
    """
    Generate list of month starts between start_date and end_date (inclusive).
    
    If end_date is NaT (missing), treats it as a one-time charge and returns
    only the start month.
    
    Example:
        start: 2024-01-15, end: 2024-03-20
        Returns: [2024-01-01, 2024-02-01, 2024-03-01]
        
        start: 2024-01-15, end: NaT
        Returns: [2024-01-01] (one-time charge)
    """
    # Handle missing start date - return empty list
    if pd.isna(start_date):
        return []
    
    # Handle missing end date - treat as one-time charge
    if pd.isna(end_date):
        start_month = start_date.to_period('M').to_timestamp()
        return [start_month]
    
    # Convert to month period and back to get month starts
    start_month = start_date.to_period('M').to_timestamp()
    end_month = end_date.to_period('M').to_timestamp()
    
    # Generate monthly range
    months = pd.date_range(start=start_month, end=end_month, freq='MS')
    return months.tolist()


def expand_scheduled_to_months(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand scheduled charges into one row per month.
    
    Each scheduled charge row with DATE_CHARGE_START..DATE_CHARGE_END
    becomes multiple rows, one for each month in that range.
    
    The AUDIT_MONTH column is set to the month start date.
    """
    expanded_rows = []
    
    for _, row in df.iterrows():
        months = generate_month_range(
            row[CanonicalField.PERIOD_START.value],
            row[CanonicalField.PERIOD_END.value]
        )
        
        for month in months:
            expanded_row = row.copy()
            expanded_row[CanonicalField.AUDIT_MONTH.value] = month
            expanded_rows.append(expanded_row)
    
    if not expanded_rows:
        # Return empty DataFrame with correct columns
        result = df.copy()
        result[CanonicalField.AUDIT_MONTH.value] = pd.NaT
        return result.iloc[0:0]
    
    result = pd.DataFrame(expanded_rows)
    
    # Reorder columns to put AUDIT_MONTH with bucket keys
    cols = [
        CanonicalField.SCHEDULED_CHARGES_ID.value,
        CanonicalField.PROPERTY_ID.value,
        CanonicalField.LEASE_INTERVAL_ID.value,
        CanonicalField.AR_CODE_ID.value,
        CanonicalField.AUDIT_MONTH.value,
        CanonicalField.EXPECTED_AMOUNT.value,
        CanonicalField.PERIOD_START.value,
        CanonicalField.PERIOD_END.value
    ]
    
    return result[cols].reset_index(drop=True)
