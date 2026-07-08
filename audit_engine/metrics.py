"""
KPI and metrics calculation.
"""
import pandas as pd
from typing import Dict, Any, Optional
from .canonical_fields import CanonicalField


def calculate_kpis(
    bucket_results: pd.DataFrame,
    findings: pd.DataFrame,
    property_id: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Calculate KPIs from bucket results and findings.
    
    Args:
        bucket_results: Reconciliation results
        findings: Generated findings
        property_id: Optional property filter
    
    Returns:
        Dictionary with KPI values
    """
    # Filter by property if specified
    if property_id is not None:
        bucket_results = bucket_results[
            bucket_results[CanonicalField.PROPERTY_ID.value] == property_id
        ]
        # Only filter findings if PROPERTY_ID column exists
        if CanonicalField.PROPERTY_ID.value in findings.columns:
            findings = findings[findings[CanonicalField.PROPERTY_ID.value] == property_id]
    
    total_buckets = len(bucket_results)
    
    if total_buckets == 0:
        return {
            "total_buckets": 0,
            "matched_buckets": 0,
            "exception_buckets": 0,
            "match_rate": 0.0,
            "total_expected": 0.0,
            "total_actual": 0.0,
            "total_variance": 0.0,
            "total_undercharge": 0.0,
            "total_overcharge": 0.0,
            "total_findings": 0,
            "high_severity_count": 0,
            "medium_severity_count": 0,
            "total_impact": 0.0
        }
    
    # Status counts — SCHEDULED_ONLY is future-lease informational, not a discrepancy
    from config import config
    non_exception_statuses = {config.reconciliation.status_matched, 'SCHEDULED_ONLY'}
    non_exception_mask = bucket_results[CanonicalField.STATUS.value].isin(non_exception_statuses)
    matched_buckets = int(non_exception_mask.sum())
    exception_buckets = total_buckets - matched_buckets
    match_rate = (matched_buckets / total_buckets) * 100 if total_buckets > 0 else 0.0

    # Financial aggregates (all rows)
    expected_values = pd.to_numeric(
        bucket_results[CanonicalField.EXPECTED_TOTAL.value],
        errors='coerce'
    ).fillna(0.0)
    actual_values = pd.to_numeric(
        bucket_results[CanonicalField.ACTUAL_TOTAL.value],
        errors='coerce'
    ).fillna(0.0)
    variances = pd.to_numeric(
        bucket_results[CanonicalField.VARIANCE.value],
        errors='coerce'
    ).fillna(0.0)

    total_expected = expected_values.sum()
    total_actual = actual_values.sum()
    total_variance = variances.sum()

    # Calculate undercharge and overcharge on exception rows only
    # (excludes MATCHED and SCHEDULED_ONLY — same logic as _calculate_static_metrics)
    exception_rows = bucket_results[~non_exception_mask]
    if len(exception_rows) > 0:
        exc_expected = pd.to_numeric(exception_rows[CanonicalField.EXPECTED_TOTAL.value], errors='coerce').fillna(0.0)
        exc_actual = pd.to_numeric(exception_rows[CanonicalField.ACTUAL_TOTAL.value], errors='coerce').fillna(0.0)
        total_undercharge = float((exc_expected - exc_actual).clip(lower=0).sum())
        total_overcharge = float((exc_actual - exc_expected).clip(lower=0).sum())
    else:
        total_undercharge = 0.0
        total_overcharge = 0.0
    
    # Finding counts
    total_findings = len(findings)
    high_severity_count = (
        len(findings[findings["severity"] == "high"])
        if len(findings) > 0 and "severity" in findings.columns
        else 0
    )
    medium_severity_count = (
        len(findings[findings["severity"] == "medium"])
        if len(findings) > 0 and "severity" in findings.columns
        else 0
    )
    
    # Impact calculation
    total_impact = (
        pd.to_numeric(findings["impact_amount"], errors='coerce').fillna(0.0).sum()
        if len(findings) > 0 and "impact_amount" in findings.columns
        else 0.0
    )
    
    return {
        "total_buckets": int(total_buckets),
        "matched_buckets": int(matched_buckets),
        "exception_buckets": int(exception_buckets),
        "match_rate": float(match_rate),
        "total_expected": float(total_expected),
        "total_actual": float(total_actual),
        "total_variance": float(total_variance),
        "total_undercharge": float(total_undercharge),
        "total_overcharge": float(total_overcharge),
        "total_findings": int(total_findings),
        "high_severity_count": int(high_severity_count),
        "medium_severity_count": int(medium_severity_count),
        "total_impact": float(total_impact)
    }


def calculate_property_summary(bucket_results: pd.DataFrame, findings: pd.DataFrame, actual_detail: pd.DataFrame = None) -> pd.DataFrame:
    """
    Calculate summary KPIs by property.
    
    Args:
        bucket_results: Reconciliation results
        findings: Generated findings
        actual_detail: Actual detail data (for property names)
    
    Returns:
        DataFrame with one row per property
    """
    # Handle empty bucket_results
    if bucket_results.empty or CanonicalField.PROPERTY_ID.value not in bucket_results.columns:
        return pd.DataFrame()
    
    properties = bucket_results[CanonicalField.PROPERTY_ID.value].unique()
    
    summaries = []
    for prop_id in properties:
        kpis = calculate_kpis(bucket_results, findings, property_id=prop_id)
        kpis["property_id"] = prop_id
        
        # Get property name from actual detail if available, otherwise use hardcoded mapping
        kpis["property_name"] = None
        if actual_detail is not None and CanonicalField.PROPERTY_NAME.value in actual_detail.columns:
            prop_data = actual_detail[actual_detail[CanonicalField.PROPERTY_ID.value] == prop_id]
            if len(prop_data) > 0:
                kpis["property_name"] = prop_data[CanonicalField.PROPERTY_NAME.value].iloc[0]
        
        # Fallback to property id string if name not found in source data
        if not kpis["property_name"]:
            try:
                kpis["property_name"] = f"Property {int(float(prop_id))}"
            except Exception:
                kpis["property_name"] = f"Property {prop_id}"
        
        # Calculate total unique lease intervals for this property
        prop_buckets = bucket_results[bucket_results[CanonicalField.PROPERTY_ID.value] == prop_id]
        total_lease_intervals = prop_buckets[CanonicalField.LEASE_INTERVAL_ID.value].nunique()
        kpis["total_lease_intervals"] = total_lease_intervals

        summaries.append(kpis)
    
    return pd.DataFrame(summaries)


def calculate_future_lease_kpis(future_lease_results: pd.DataFrame) -> Dict[str, Any]:
    """
    Calculate KPIs specific to future lease audit.
    
    Args:
        future_lease_results: Future lease audit results DataFrame
    
    Returns:
        Dictionary with future lease audit KPIs
    """
    if future_lease_results.empty:
        return {
            'total_future_leases': 0,
            'pass_count': 0,
            'expected_exception_count': 0,
            'needs_review_count': 0,
            'true_discrepancy_count': 0,
            'total_potential_undercharge': 0.0,
            'total_potential_overcharge': 0.0,
            'match_rate': 0.0,
            'avg_variance': 0.0,
            'max_undercharge': 0.0,
            'max_overcharge': 0.0
        }
    
    total_leases = len(future_lease_results)
    
    # Status counts
    status_col = CanonicalField.FUTURE_LEASE_AUDIT_STATUS.value
    status_counts = future_lease_results[status_col].value_counts().to_dict()
    
    pass_count = status_counts.get('Pass', 0)
    expected_exception_count = status_counts.get('Expected Exception', 0)
    needs_review_count = status_counts.get('Needs Review', 0)
    true_discrepancy_count = status_counts.get('True Discrepancy', 0)
    
    # Variance calculations
    variance_col = CanonicalField.VARIANCE.value
    direction_col = CanonicalField.VARIANCE_DIRECTION.value
    
    variances = pd.to_numeric(future_lease_results[variance_col], errors='coerce').fillna(0.0)
    
    # Calculate undercharge/overcharge only for true discrepancies
    discrepancy_mask = future_lease_results[status_col] == 'True Discrepancy'
    discrepancy_results = future_lease_results[discrepancy_mask]
    
    if len(discrepancy_results) > 0:
        undercharge_mask = discrepancy_results[direction_col] == 'undercharge'
        overcharge_mask = discrepancy_results[direction_col] == 'overcharge'
        
        undercharge_variances = pd.to_numeric(
            discrepancy_results[undercharge_mask][variance_col], 
            errors='coerce'
        ).fillna(0.0)
        overcharge_variances = pd.to_numeric(
            discrepancy_results[overcharge_mask][variance_col], 
            errors='coerce'
        ).fillna(0.0)
        
        total_potential_undercharge = float(undercharge_variances.abs().sum())
        total_potential_overcharge = float(overcharge_variances.abs().sum())
        max_undercharge = float(undercharge_variances.abs().max()) if len(undercharge_variances) > 0 else 0.0
        max_overcharge = float(overcharge_variances.abs().max()) if len(overcharge_variances) > 0 else 0.0
    else:
        total_potential_undercharge = 0.0
        total_potential_overcharge = 0.0
        max_undercharge = 0.0
        max_overcharge = 0.0
    
    # Match rate (Pass + Expected Exception) / Total
    match_count = pass_count + expected_exception_count
    match_rate = (match_count / total_leases * 100.0) if total_leases > 0 else 0.0
    
    # Average variance (absolute value)
    avg_variance = float(variances.abs().mean()) if len(variances) > 0 else 0.0
    
    return {
        'total_future_leases': int(total_leases),
        'pass_count': int(pass_count),
        'expected_exception_count': int(expected_exception_count),
        'needs_review_count': int(needs_review_count),
        'true_discrepancy_count': int(true_discrepancy_count),
        'total_potential_undercharge': float(total_potential_undercharge),
        'total_potential_overcharge': float(total_potential_overcharge),
        'match_rate': float(match_rate),
        'avg_variance': float(avg_variance),
        'max_undercharge': float(max_undercharge),
        'max_overcharge': float(max_overcharge)
    }
