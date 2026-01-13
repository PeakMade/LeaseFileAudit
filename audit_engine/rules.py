"""
Rule framework and rule implementations.
Extensible plugin-style architecture for audit rules.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import pandas as pd


@dataclass
class RuleContext:
    """
    Context object passed to rules containing all canonical datasets.
    
    This provides a central place to access normalized data and
    allows for easy extension with additional data sources.
    """
    run_id: str
    expected_detail: pd.DataFrame  # Expanded scheduled charges
    actual_detail: pd.DataFrame    # Normalized AR transactions
    bucket_results: pd.DataFrame   # Reconciliation results
    
    # Registry for additional data sources (future extensibility)
    additional_sources: Dict[str, pd.DataFrame] = None
    
    def __post_init__(self):
        if self.additional_sources is None:
            self.additional_sources = {}
    
    def register_source(self, name: str, data: pd.DataFrame):
        """Register additional data source for rules to access."""
        self.additional_sources[name] = data
    
    def get_source(self, name: str) -> Optional[pd.DataFrame]:
        """Get additional data source by name."""
        return self.additional_sources.get(name)


class Rule(ABC):
    """
    Abstract base class for audit rules.
    
    Each rule has a unique ID, name, and evaluation logic.
    Rules are deterministic and produce Finding objects.
    """
    
    @property
    @abstractmethod
    def rule_id(self) -> str:
        """Unique identifier for this rule."""
        pass
    
    @property
    @abstractmethod
    def rule_name(self) -> str:
        """Human-readable name for this rule."""
        pass
    
    @property
    @abstractmethod
    def applies_to(self) -> List[str]:
        """List of data source names this rule applies to."""
        pass
    
    @abstractmethod
    def evaluate(self, context: RuleContext) -> List[Dict[str, Any]]:
        """
        Evaluate rule against context and return list of finding dicts.
        
        Returns:
            List of finding dictionaries with keys:
            - property_id, lease_interval_id, ar_code_id, audit_month
            - category, severity, title, description
            - expected_value, actual_value, variance, impact_amount
            - evidence (dict with detail record IDs)
        """
        pass


class ARScheduledMatchRule(Rule):
    """
    V1 Rule: Compare AR transactions against scheduled charges.
    
    Produces findings for all non-matched buckets.
    """
    
    @property
    def rule_id(self) -> str:
        return "AR_SCHEDULED_MATCH"
    
    @property
    def rule_name(self) -> str:
        return "AR vs Scheduled Charges Reconciliation"
    
    @property
    def applies_to(self) -> List[str]:
        return ["ar_transactions", "scheduled_charges"]
    
    def evaluate(self, context: RuleContext) -> List[Dict[str, Any]]:
        """Generate findings for non-matched buckets."""
        from .canonical_fields import CanonicalField
        from config import config as app_config
        
        findings = []
        
        # Filter to non-matched buckets
        exceptions = context.bucket_results[
            context.bucket_results[CanonicalField.STATUS.value] != app_config.reconciliation.status_matched
        ]
        
        for _, bucket in exceptions.iterrows():
            # Get evidence IDs
            evidence = self._get_evidence(bucket, context)
            
            finding = {
                "property_id": bucket[CanonicalField.PROPERTY_ID.value],
                "lease_interval_id": bucket[CanonicalField.LEASE_INTERVAL_ID.value],
                "ar_code_id": bucket[CanonicalField.AR_CODE_ID.value],
                "audit_month": bucket[CanonicalField.AUDIT_MONTH.value],
                "category": "financial",
                "severity": app_config.severity.get_severity(bucket[CanonicalField.STATUS.value]),
                "title": self._generate_title(bucket[CanonicalField.STATUS.value]),
                "description": self._generate_description(bucket),
                "expected_value": bucket[CanonicalField.EXPECTED_TOTAL.value],
                "actual_value": bucket[CanonicalField.ACTUAL_TOTAL.value],
                "variance": bucket[CanonicalField.VARIANCE.value],
                "impact_amount": abs(bucket[CanonicalField.VARIANCE.value]),
                "evidence": evidence
            }
            findings.append(finding)
        
        return findings
    
    def _get_evidence(self, bucket: pd.Series, context: RuleContext) -> Dict[str, List]:
        """Get evidence record IDs for this bucket."""
        from .canonical_fields import CanonicalField
        
        # Build filter for this bucket
        bucket_filter = (
            (context.expected_detail[CanonicalField.PROPERTY_ID.value] == bucket[CanonicalField.PROPERTY_ID.value]) &
            (context.expected_detail[CanonicalField.LEASE_INTERVAL_ID.value] == bucket[CanonicalField.LEASE_INTERVAL_ID.value]) &
            (context.expected_detail[CanonicalField.AR_CODE_ID.value] == bucket[CanonicalField.AR_CODE_ID.value]) &
            (context.expected_detail[CanonicalField.AUDIT_MONTH.value] == bucket[CanonicalField.AUDIT_MONTH.value])
        )
        expected_ids = context.expected_detail[bucket_filter][CanonicalField.SCHEDULED_CHARGES_ID.value].tolist()
        
        bucket_filter_actual = (
            (context.actual_detail[CanonicalField.PROPERTY_ID.value] == bucket[CanonicalField.PROPERTY_ID.value]) &
            (context.actual_detail[CanonicalField.LEASE_INTERVAL_ID.value] == bucket[CanonicalField.LEASE_INTERVAL_ID.value]) &
            (context.actual_detail[CanonicalField.AR_CODE_ID.value] == bucket[CanonicalField.AR_CODE_ID.value]) &
            (context.actual_detail[CanonicalField.AUDIT_MONTH.value] == bucket[CanonicalField.AUDIT_MONTH.value])
        )
        actual_ids = context.actual_detail[bucket_filter_actual][CanonicalField.AR_TRANSACTION_ID.value].tolist()
        
        return {
            "scheduled_charge_ids": expected_ids,
            "ar_transaction_ids": actual_ids
        }
    
    def _generate_title(self, status: str) -> str:
        """Generate finding title based on status."""
        titles = {
            "SCHEDULED_NOT_BILLED": "Scheduled Charge Not Billed",
            "BILLED_NOT_SCHEDULED": "Billed Without Schedule",
            "AMOUNT_MISMATCH": "Amount Mismatch"
        }
        return titles.get(status, "Reconciliation Exception")
    
    def _generate_description(self, bucket: pd.Series) -> str:
        """Generate detailed finding description."""
        from .canonical_fields import CanonicalField
        
        status = bucket[CanonicalField.STATUS.value]
        expected = bucket[CanonicalField.EXPECTED_TOTAL.value]
        actual = bucket[CanonicalField.ACTUAL_TOTAL.value]
        variance = bucket[CanonicalField.VARIANCE.value]
        
        descriptions = {
            "SCHEDULED_NOT_BILLED": f"Scheduled amount ${expected:.2f} was not billed.",
            "BILLED_NOT_SCHEDULED": f"Amount ${actual:.2f} was billed without a schedule.",
            "AMOUNT_MISMATCH": f"Expected ${expected:.2f}, actual ${actual:.2f}, variance ${variance:.2f}."
        }
        
        return descriptions.get(status, f"Reconciliation issue: {status}")


class RuleRegistry:
    """
    Central registry for audit rules.
    
    Adding a new rule:
    1. Create a Rule subclass
    2. Register it here
    3. No other code changes needed
    """
    
    def __init__(self):
        self._rules: List[Rule] = []
    
    def register(self, rule: Rule):
        """Register a rule."""
        self._rules.append(rule)
    
    def get_all_rules(self) -> List[Rule]:
        """Get all registered rules."""
        return self._rules.copy()
    
    def get_rule(self, rule_id: str) -> Optional[Rule]:
        """Get rule by ID."""
        for rule in self._rules:
            if rule.rule_id == rule_id:
                return rule
        return None
    
    def evaluate_all(self, context: RuleContext) -> List[Dict[str, Any]]:
        """Evaluate all registered rules and aggregate findings."""
        all_findings = []
        for rule in self._rules:
            findings = rule.evaluate(context)
            all_findings.extend(findings)
        return all_findings


# Create global registry and register v1 rules
default_registry = RuleRegistry()
default_registry.register(ARScheduledMatchRule())
