"""
Audit Engine - Core audit processing modules.
"""
from .io import DataSourceLoader, ExcelSourceLoader
from .normalize import normalize_ar_transactions, normalize_scheduled_charges
from .expand import expand_scheduled_to_months
from .reconcile import reconcile_buckets
from .rules import RuleContext, Rule, RuleRegistry, ARScheduledMatchRule
from .findings import Finding, generate_findings
from .metrics import calculate_kpis, calculate_property_summary
from .canonical_fields import CanonicalField, BUCKET_KEY_FIELDS
from .schemas import CanonicalDataSet, validate_columns, enforce_dtypes

__all__ = [
    "DataSourceLoader",
    "ExcelSourceLoader",
    "normalize_ar_transactions",
    "normalize_scheduled_charges",
    "expand_scheduled_to_months",
    "reconcile_buckets",
    "RuleContext",
    "Rule",
    "RuleRegistry",
    "ARScheduledMatchRule",
    "Finding",
    "generate_findings",
    "calculate_kpis",
    "calculate_property_summary",
    "CanonicalField",
    "BUCKET_KEY_FIELDS",
    "CanonicalDataSet",
    "validate_columns",
    "enforce_dtypes",
]
