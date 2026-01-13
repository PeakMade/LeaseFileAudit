"""
Reconciliation logic - aggregate and compare expected vs actual at bucket grain.
"""
import pandas as pd
from .canonical_fields import CanonicalField, BUCKET_KEY_FIELDS, get_field_names
from config import ReconciliationConfig

BUCKET_KEY_COLUMNS = list(get_field_names(BUCKET_KEY_FIELDS))


def reconcile_buckets(
    expected_detail: pd.DataFrame,
    actual_detail: pd.DataFrame,
    recon_config: ReconciliationConfig
) -> pd.DataFrame:
    """
    Aggregate expected and actual by bucket key, calculate variance and status.
    
    Args:
        expected_detail: Expanded scheduled charges with AUDIT_MONTH
        actual_detail: Normalized AR transactions with AUDIT_MONTH
        recon_config: Reconciliation configuration
    
    Returns:
        DataFrame with bucket-level reconciliation results
    """
    # Aggregate expected totals
    expected_agg = expected_detail.groupby(BUCKET_KEY_COLUMNS)[
        CanonicalField.EXPECTED_AMOUNT.value
    ].sum().reset_index()
    expected_agg.rename(columns={CanonicalField.EXPECTED_AMOUNT.value: CanonicalField.EXPECTED_TOTAL.value}, inplace=True)
    
    # Aggregate actual totals
    actual_agg = actual_detail.groupby(BUCKET_KEY_COLUMNS)[
        CanonicalField.ACTUAL_AMOUNT.value
    ].sum().reset_index()
    actual_agg.rename(columns={CanonicalField.ACTUAL_AMOUNT.value: CanonicalField.ACTUAL_TOTAL.value}, inplace=True)
    
    # Full outer join to capture all buckets
    reconciled = pd.merge(
        expected_agg,
        actual_agg,
        on=BUCKET_KEY_COLUMNS,
        how='outer'
    )
    
    # Fill NaNs with 0 for calculation
    reconciled[CanonicalField.EXPECTED_TOTAL.value] = reconciled[CanonicalField.EXPECTED_TOTAL.value].fillna(0)
    reconciled[CanonicalField.ACTUAL_TOTAL.value] = reconciled[CanonicalField.ACTUAL_TOTAL.value].fillna(0)
    
    # Calculate variance
    reconciled[CanonicalField.VARIANCE.value] = (
        reconciled[CanonicalField.ACTUAL_TOTAL.value] - 
        reconciled[CanonicalField.EXPECTED_TOTAL.value]
    )
    
    # Classify status
    reconciled[CanonicalField.STATUS.value] = reconciled.apply(
        lambda row: _classify_status(row, recon_config),
        axis=1
    )
    
    # Set match rule (for v1, all use same rule)
    reconciled[CanonicalField.MATCH_RULE.value] = "AR_SCHEDULED_MATCH"
    
    return reconciled


def _classify_status(row: pd.Series, config: ReconciliationConfig) -> str:
    """
    Classify reconciliation status based on business rules.
    
    Rules:
    - MATCHED if abs(variance) <= tolerance
    - SCHEDULED_NOT_BILLED if expected != 0 and actual == 0
    - BILLED_NOT_SCHEDULED if expected == 0 and actual != 0
    - AMOUNT_MISMATCH otherwise
    """
    expected = row[CanonicalField.EXPECTED_TOTAL.value]
    actual = row[CanonicalField.ACTUAL_TOTAL.value]
    variance = row[CanonicalField.VARIANCE.value]
    
    # Check for match within tolerance
    if abs(variance) <= config.amount_tolerance:
        return config.status_matched
    
    # Check for scheduled not billed
    if expected != 0 and actual == 0:
        return config.status_scheduled_not_billed
    
    # Check for billed not scheduled
    if expected == 0 and actual != 0:
        return config.status_billed_not_scheduled
    
    # Amount mismatch
    return config.status_amount_mismatch
