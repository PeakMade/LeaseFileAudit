"""
Findings generation and management.
"""
import pandas as pd
from typing import List, Dict, Any
from dataclasses import dataclass, asdict
import uuid


@dataclass
class Finding:
    """Structured finding record."""
    finding_id: str
    run_id: str
    property_id: Any
    lease_interval_id: Any
    ar_code_id: Any
    audit_month: str
    category: str
    severity: str
    title: str
    description: str
    expected_value: float
    actual_value: float
    variance: float
    impact_amount: float
    evidence: Dict[str, List]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        d = asdict(self)
        # Convert audit_month to string if it's a timestamp
        if hasattr(d['audit_month'], 'strftime'):
            d['audit_month'] = d['audit_month'].strftime('%Y-%m-%d')
        return d


def generate_findings(finding_dicts: List[Dict[str, Any]], run_id: str) -> pd.DataFrame:
    """
    Convert finding dictionaries to DataFrame with unique IDs.
    
    Args:
        finding_dicts: List of finding dicts from rule evaluation
        run_id: Current audit run ID
    
    Returns:
        DataFrame with all findings
    """
    if not finding_dicts:
        # Return empty DataFrame with correct columns
        from .canonical_fields import CanonicalField
        return pd.DataFrame(columns=[
            CanonicalField.FINDING_ID.value,
            CanonicalField.RUN_ID.value,
            CanonicalField.PROPERTY_ID.value,
            CanonicalField.LEASE_INTERVAL_ID.value,
            CanonicalField.AR_CODE_ID.value,
            CanonicalField.AUDIT_MONTH.value,
            CanonicalField.CATEGORY.value,
            CanonicalField.SEVERITY.value,
            CanonicalField.TITLE.value,
            CanonicalField.DESCRIPTION.value,
            CanonicalField.EXPECTED_VALUE.value,
            CanonicalField.ACTUAL_VALUE.value,
            CanonicalField.VARIANCE.value,
            CanonicalField.IMPACT_AMOUNT.value,
            CanonicalField.EVIDENCE.value
        ])
    
    findings = []
    for fd in finding_dicts:
        finding = Finding(
            finding_id=str(uuid.uuid4()),
            run_id=run_id,
            property_id=fd["property_id"],
            lease_interval_id=fd["lease_interval_id"],
            ar_code_id=fd["ar_code_id"],
            audit_month=fd["audit_month"],
            category=fd["category"],
            severity=fd["severity"],
            title=fd["title"],
            description=fd["description"],
            expected_value=fd["expected_value"],
            actual_value=fd["actual_value"],
            variance=fd["variance"],
            impact_amount=fd["impact_amount"],
            evidence=fd["evidence"]
        )
        findings.append(finding.to_dict())
    
    df = pd.DataFrame(findings)
    
    # Ensure audit_month is datetime
    df['audit_month'] = pd.to_datetime(df['audit_month'])
    
    return df
