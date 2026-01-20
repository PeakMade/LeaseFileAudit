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
        findings = findings[findings["property_id"] == property_id]
    
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
            "total_findings": 0,
            "high_severity_count": 0,
            "medium_severity_count": 0,
            "total_impact": 0.0
        }
    
    # Status counts
    from config import config
    matched_buckets = len(bucket_results[
        bucket_results[CanonicalField.STATUS.value] == config.reconciliation.status_matched
    ])
    exception_buckets = total_buckets - matched_buckets
    match_rate = (matched_buckets / total_buckets) * 100 if total_buckets > 0 else 0.0
    
    # Financial aggregates
    total_expected = bucket_results[CanonicalField.EXPECTED_TOTAL.value].sum()
    total_actual = bucket_results[CanonicalField.ACTUAL_TOTAL.value].sum()
    total_variance = bucket_results[CanonicalField.VARIANCE.value].sum()
    
    # Finding counts
    total_findings = len(findings)
    high_severity_count = len(findings[findings["severity"] == "high"])
    medium_severity_count = len(findings[findings["severity"] == "medium"])
    
    # Impact calculation
    total_impact = findings["impact_amount"].sum() if len(findings) > 0 else 0.0
    
    return {
        "total_buckets": int(total_buckets),
        "matched_buckets": int(matched_buckets),
        "exception_buckets": int(exception_buckets),
        "match_rate": float(match_rate),
        "total_expected": float(total_expected),
        "total_actual": float(total_actual),
        "total_variance": float(total_variance),
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
    # Temporary hardcoded property names
    PROPERTY_NAME_MAP = {
        1122966: "48 West",
        100069944: "Bixby Kennesaw"
    }
    
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
        
        # Fallback to hardcoded names if not found
        if not kpis["property_name"]:
            kpis["property_name"] = PROPERTY_NAME_MAP.get(int(float(prop_id)))
        
        # Calculate total unique lease intervals for this property
        prop_buckets = bucket_results[bucket_results[CanonicalField.PROPERTY_ID.value] == prop_id]
        total_lease_intervals = prop_buckets[CanonicalField.LEASE_INTERVAL_ID.value].nunique()
        kpis["total_lease_intervals"] = total_lease_intervals
        
        # Calculate undercharge/overcharge for this property
        # Undercharge = expected > actual (billed less than scheduled)
        # Overcharge = actual > expected (billed more than scheduled)
        
        undercharge = prop_buckets.apply(
            lambda row: max(0, row[CanonicalField.EXPECTED_TOTAL.value] - row[CanonicalField.ACTUAL_TOTAL.value]),
            axis=1
        ).sum()
        
        overcharge = prop_buckets.apply(
            lambda row: max(0, row[CanonicalField.ACTUAL_TOTAL.value] - row[CanonicalField.EXPECTED_TOTAL.value]),
            axis=1
        ).sum()
        
        kpis['total_undercharge'] = undercharge
        kpis['total_overcharge'] = overcharge
        
        summaries.append(kpis)
    
    return pd.DataFrame(summaries)
