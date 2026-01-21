"""
Canonical field definitions for Lease Audit Engine.

This module is the single source of truth for all field names used throughout
the audit engine, rules, metrics, and UI. Raw source column names should NEVER
be referenced outside of mappings.py.

Using Enum provides:
- Type safety and IDE autocomplete
- Easy discovery of available fields
- Runtime validation
- Clear documentation
"""
from enum import Enum
from typing import Tuple, FrozenSet


class CanonicalField(str, Enum):
    """
    Canonical field names used throughout the audit engine.
    
    Inheriting from str makes these usable as dictionary keys and
    compatible with pandas DataFrame column operations.
    """
    
    # ==================== Common Identifiers ====================
    PROPERTY_ID = "PROPERTY_ID"
    """Unique identifier for a property"""
    
    PROPERTY_NAME = "PROPERTY_NAME"
    """Name of the property"""
    
    LEASE_ID = "LEASE_ID"
    """Unique identifier for a lease (lifetime)"""
    
    LEASE_INTERVAL_ID = "LEASE_INTERVAL_ID"
    """Unique identifier for a lease interval/term"""
    
    CUSTOMER_ID = "CUSTOMER_ID"
    """Unique identifier for a customer account"""
    
    CUSTOMER_NAME = "CUSTOMER_NAME"
    """Name of the customer"""
    
    GUARANTOR_NAME = "GUARANTOR_NAME"
    """Name of the guarantor for the lease"""
    
    RESIDENT_ID = "RESIDENT_ID"
    """Unique identifier for a resident"""
    
    UNIT_ID = "UNIT_ID"
    """Unique identifier for a unit"""
    
    PET_ID = "PET_ID"
    """Unique identifier for a pet"""
    
    # ==================== Charge Coding ====================
    AR_CODE_ID = "AR_CODE_ID"
    """Unique identifier for an AR/GL code"""
    
    AR_CODE_NAME = "AR_CODE_NAME"
    """Descriptive name for the AR code"""
    
    AR_CODE_TYPE_ID = "AR_CODE_TYPE_ID"
    """Type/category of the AR code"""
    
    CHARGE_TYPE = "CHARGE_TYPE"
    """Type of charge (rent, fee, deposit, etc.)"""
    
    # ==================== Time Dimensions ====================
    AUDIT_MONTH = "AUDIT_MONTH"
    """Month bucket for audit reconciliation (YYYY-MM-01)"""
    
    PERIOD_START = "PERIOD_START"
    """Start date of a period/range"""
    
    PERIOD_END = "PERIOD_END"
    """End date of a period/range"""
    
    POST_DATE = "POST_DATE"
    """Date a transaction was posted"""
    
    TRANSACTION_DATE = "TRANSACTION_DATE"
    """Date a transaction occurred"""
    
    EFFECTIVE_DATE = "EFFECTIVE_DATE"
    """Date a record becomes effective"""
    
    EXPIRATION_DATE = "EXPIRATION_DATE"
    """Date a record expires"""
    
    # ==================== Amounts ====================
    EXPECTED_AMOUNT = "expected_amount"
    """Expected/scheduled amount for a single record"""
    
    ACTUAL_AMOUNT = "actual_amount"
    """Actual/billed amount for a single record"""
    
    EXPECTED_TOTAL = "expected_total"
    """Aggregated expected amount at bucket level"""
    
    ACTUAL_TOTAL = "actual_total"
    """Aggregated actual amount at bucket level"""
    
    VARIANCE = "variance"
    """Difference between actual and expected (actual - expected)"""
    
    AMOUNT = "AMOUNT"
    """Generic amount field"""
    
    # ==================== Source Provenance ====================
    SOURCE_SYSTEM = "SOURCE_SYSTEM"
    """Name of the source system"""
    
    SOURCE_ROW_ID = "SOURCE_ROW_ID"
    """Original row identifier from source"""
    
    SCHEDULED_CHARGES_ID = "SCHEDULED_CHARGES_ID"
    """Identifier from scheduled charges source"""
    
    AR_TRANSACTION_ID = "AR_TRANSACTION_ID"
    """Identifier from AR transactions source (maps to ID in raw data)"""
    
    INVOICE_ID = "INVOICE_ID"
    """Invoice identifier"""
    
    LINE_ID = "LINE_ID"
    """Line item identifier"""
    
    # ==================== Status & Metadata ====================
    STATUS = "status"
    """Record or reconciliation status"""
    
    MATCH_RULE = "match_rule"
    """Rule that produced the match/mismatch"""
    
    SEVERITY = "severity"
    """Severity level (info, low, medium, high, critical)"""
    
    CATEGORY = "category"
    """Category of finding or record (financial, compliance, data quality)"""
    
    FINDING_ID = "finding_id"
    """Unique identifier for a finding"""
    
    RUN_ID = "run_id"
    """Unique identifier for an audit run"""
    
    # ==================== Audit Flags ====================
    IS_POSTED = "IS_POSTED"
    """Flag indicating if transaction is posted"""
    
    IS_DELETED = "IS_DELETED"
    """Flag indicating if record is deleted"""
    
    IS_REVERSAL = "IS_REVERSAL"
    """Flag indicating if transaction is a reversal"""
    
    IS_VOID = "IS_VOID"
    """Flag indicating if record is void"""
    
    # ==================== Findings Detail ====================
    TITLE = "title"
    """Finding title/summary"""
    
    DESCRIPTION = "description"
    """Detailed finding description"""
    
    EXPECTED_VALUE = "expected_value"
    """Expected value for comparison findings"""
    
    ACTUAL_VALUE = "actual_value"
    """Actual value for comparison findings"""
    
    IMPACT_AMOUNT = "impact_amount"
    """Financial impact of a finding"""
    
    EVIDENCE = "evidence"
    """Evidence supporting a finding (JSON/dict)"""
    
    # ==================== Lease/Resident Extensions (for future use) ====================
    LEASE_START_DATE = "LEASE_START_DATE"
    """Lease start date"""
    
    LEASE_END_DATE = "LEASE_END_DATE"
    """Lease end date"""
    
    LEASE_STATUS = "LEASE_STATUS"
    """Lease status (active, expired, etc.)"""
    
    RENT_AMOUNT = "RENT_AMOUNT"
    """Monthly rent amount"""
    
    RESIDENT_NAME = "RESIDENT_NAME"
    """Resident name"""
    
    RESIDENT_TYPE = "RESIDENT_TYPE"
    """Resident type (primary, occupant, guarantor)"""
    
    UNIT_NUMBER = "UNIT_NUMBER"
    """Unit number"""
    
    UNIT_TYPE = "UNIT_TYPE"
    """Unit type (1BR, 2BR, studio, etc.)"""


# ==================== Field Groups ====================

# Bucket key fields define the reconciliation grain
BUCKET_KEY_FIELDS: Tuple[CanonicalField, ...] = (
    CanonicalField.PROPERTY_ID,
    CanonicalField.LEASE_INTERVAL_ID,
    CanonicalField.AR_CODE_ID,
    CanonicalField.AUDIT_MONTH,
)
"""Fields that define the reconciliation bucket (audit grain)"""

# Required fields for expected detail (scheduled charges)
REQUIRED_EXPECTED_DETAIL_FIELDS: FrozenSet[CanonicalField] = frozenset({
    CanonicalField.PROPERTY_ID,
    CanonicalField.LEASE_INTERVAL_ID,
    CanonicalField.AR_CODE_ID,
    CanonicalField.AUDIT_MONTH,
    CanonicalField.EXPECTED_AMOUNT,
    CanonicalField.SCHEDULED_CHARGES_ID,
})
"""Minimum required fields for expected detail records"""

# Required fields for actual detail (AR transactions)
REQUIRED_ACTUAL_DETAIL_FIELDS: FrozenSet[CanonicalField] = frozenset({
    CanonicalField.PROPERTY_ID,
    CanonicalField.LEASE_INTERVAL_ID,
    CanonicalField.AR_CODE_ID,
    CanonicalField.AUDIT_MONTH,
    CanonicalField.ACTUAL_AMOUNT,
    CanonicalField.AR_TRANSACTION_ID,
})
"""Minimum required fields for actual detail records"""

# Required fields for bucket results
REQUIRED_BUCKET_RESULTS_FIELDS: FrozenSet[CanonicalField] = frozenset({
    CanonicalField.PROPERTY_ID,
    CanonicalField.LEASE_INTERVAL_ID,
    CanonicalField.AR_CODE_ID,
    CanonicalField.AUDIT_MONTH,
    CanonicalField.EXPECTED_TOTAL,
    CanonicalField.ACTUAL_TOTAL,
    CanonicalField.VARIANCE,
    CanonicalField.STATUS,
    CanonicalField.MATCH_RULE,
})
"""Required fields for bucket-level reconciliation results"""

# Required fields for findings
REQUIRED_FINDING_FIELDS: FrozenSet[CanonicalField] = frozenset({
    CanonicalField.FINDING_ID,
    CanonicalField.RUN_ID,
    CanonicalField.PROPERTY_ID,
    CanonicalField.LEASE_INTERVAL_ID,
    CanonicalField.AR_CODE_ID,
    CanonicalField.AUDIT_MONTH,
    CanonicalField.CATEGORY,
    CanonicalField.SEVERITY,
    CanonicalField.TITLE,
    CanonicalField.DESCRIPTION,
})
"""Required fields for findings records"""

# Identifier fields (for filtering/grouping)
IDENTIFIER_FIELDS: FrozenSet[CanonicalField] = frozenset({
    CanonicalField.PROPERTY_ID,
    CanonicalField.LEASE_ID,
    CanonicalField.LEASE_INTERVAL_ID,
    CanonicalField.CUSTOMER_ID,
    CanonicalField.RESIDENT_ID,
    CanonicalField.UNIT_ID,
    CanonicalField.AR_CODE_ID,
})
"""Fields used as identifiers/dimensions"""

# Amount fields (for aggregation)
AMOUNT_FIELDS: FrozenSet[CanonicalField] = frozenset({
    CanonicalField.EXPECTED_AMOUNT,
    CanonicalField.ACTUAL_AMOUNT,
    CanonicalField.EXPECTED_TOTAL,
    CanonicalField.ACTUAL_TOTAL,
    CanonicalField.VARIANCE,
    CanonicalField.IMPACT_AMOUNT,
    CanonicalField.AMOUNT,
    CanonicalField.RENT_AMOUNT,
})
"""Fields containing monetary amounts"""

# Date fields (for time-based filtering)
DATE_FIELDS: FrozenSet[CanonicalField] = frozenset({
    CanonicalField.AUDIT_MONTH,
    CanonicalField.PERIOD_START,
    CanonicalField.PERIOD_END,
    CanonicalField.POST_DATE,
    CanonicalField.TRANSACTION_DATE,
    CanonicalField.EFFECTIVE_DATE,
    CanonicalField.EXPIRATION_DATE,
    CanonicalField.LEASE_START_DATE,
    CanonicalField.LEASE_END_DATE,
})
"""Fields containing dates"""


def get_field_names(fields: FrozenSet[CanonicalField]) -> Tuple[str, ...]:
    """
    Convert a set of CanonicalField enums to a tuple of string names.
    
    Useful for pandas operations that require string column names.
    
    Args:
        fields: Set of CanonicalField enums
    
    Returns:
        Tuple of field name strings
    
    Example:
        >>> names = get_field_names(BUCKET_KEY_FIELDS)
        >>> df[list(names)]  # Select bucket key columns
    """
    return tuple(f.value for f in fields)


def validate_field_group(fields: FrozenSet[CanonicalField], 
                         available_fields: FrozenSet[CanonicalField]) -> None:
    """
    Validate that all required fields are present in available fields.
    
    Args:
        fields: Required fields
        available_fields: Available fields
    
    Raises:
        ValueError: If any required fields are missing
    """
    missing = fields - available_fields
    if missing:
        missing_names = [f.value for f in missing]
        raise ValueError(f"Missing required fields: {missing_names}")
