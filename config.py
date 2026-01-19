"""
Centralized configuration for Lease File Audit application.
All mappings, tolerances, and detection rules are defined here.
"""
from dataclasses import dataclass, field
from typing import Dict, List
from pathlib import Path


@dataclass
class ColumnMapping:
    """Maps required columns for a data source."""
    required_columns: List[str]
    optional_columns: List[str] = field(default_factory=list)
    
    def validate(self, columns: List[str]) -> tuple[bool, List[str]]:
        """Check if all required columns are present."""
        missing = [col for col in self.required_columns if col not in columns]
        return len(missing) == 0, missing


@dataclass
class DataSourceConfig:
    """Configuration for a data source."""
    name: str
    column_mapping: ColumnMapping
    detection_keywords: List[str]  # For sheet name detection


@dataclass
class ReconciliationConfig:
    """Configuration for reconciliation tolerances and rules."""
    amount_tolerance: float = 0.01
    status_matched: str = "MATCHED"
    status_scheduled_not_billed: str = "SCHEDULED_NOT_BILLED"
    status_billed_not_scheduled: str = "BILLED_NOT_SCHEDULED"
    status_amount_mismatch: str = "AMOUNT_MISMATCH"


@dataclass
class SeverityMapping:
    """Map status to severity level."""
    severity_by_status: Dict[str, str] = field(default_factory=lambda: {
        "MATCHED": "info",
        "SCHEDULED_NOT_BILLED": "high",
        "BILLED_NOT_SCHEDULED": "medium",
        "AMOUNT_MISMATCH": "high"
    })
    
    def get_severity(self, status: str) -> str:
        return self.severity_by_status.get(status, "medium")


@dataclass
class StorageConfig:
    """Configuration for data persistence."""
    base_dir: Path = field(default_factory=lambda: Path("instance/runs"))
    inputs_dir: str = "inputs_normalized"
    outputs_dir: str = "outputs"
    meta_file: str = "run_meta.json"
    
    def get_run_dir(self, run_id: str) -> Path:
        return self.base_dir / run_id


@dataclass
class AuditConfig:
    """Main audit configuration container."""
    # Data sources
    ar_source: DataSourceConfig = field(default_factory=lambda: DataSourceConfig(
        name="ar_transactions",
        column_mapping=ColumnMapping(
            required_columns=[
                "PROPERTY_ID", "LEASE_INTERVAL_ID", "AR_CODE_ID", "AR_CODE_NAME",
                "TRANSACTION_AMOUNT", "POST_MONTH_DATE", "POST_DATE",
                "IS_POSTED", "IS_DELETED", "IS_REVERSAL", "ID"
            ]
        ),
        detection_keywords=["ar_trans", "ar trans"]  # Matches AR_TRANS_1_EXPANDED
    ))
    
    scheduled_source: DataSourceConfig = field(default_factory=lambda: DataSourceConfig(
        name="scheduled_charges",
        column_mapping=ColumnMapping(
            required_columns=[
                "ID", "PROPERTY_ID", "LEASE_INTERVAL_ID",
                "AR_CODE_ID", "AR_CODE_NAME", "CHARGE_AMOUNT", 
                "CHARGE_START_DATE", "CHARGE_END_DATE"
            ]
        ),
        detection_keywords=["sc_trans", "sc trans"]  # Matches SC_TRANS_1 EXPANDED
    ))
    
    # Reconciliation settings
    reconciliation: ReconciliationConfig = field(default_factory=ReconciliationConfig)
    
    # Severity mapping
    severity: SeverityMapping = field(default_factory=SeverityMapping)
    
    # Storage settings
    storage: StorageConfig = field(default_factory=StorageConfig)
    
    # Bucket key columns (canonical audit grain)
    bucket_key_columns: List[str] = field(default_factory=lambda: [
        "PROPERTY_ID", "LEASE_INTERVAL_ID", "AR_CODE_ID", "AUDIT_MONTH"
    ])


# Global configuration instance
config = AuditConfig()
