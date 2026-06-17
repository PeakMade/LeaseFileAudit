"""
Tests for Future Lease Audit Module

Validates the Phase 1 future lease audit workflow:
- Scheduled charge rollup logic
- Lease contract amount comparison
- Variance calculation and classification
- Status assignment (Pass/Needs Review/True Discrepancy)
"""

import pytest
import pandas as pd
from pathlib import Path
import json
import tempfile

from audit_engine.future_lease_audit import (
    load_future_lease_config,
    identify_future_leases,
    build_charge_rollup_map,
    calculate_scheduled_charge_rollup,
    calculate_future_lease_variances,
    classify_audit_status,
    execute_future_lease_audit,
)
from audit_engine.canonical_fields import CanonicalField


# Test fixtures
@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        "enabled": True,
        "lease_selection": {
            "require_lease_start_after_today": False  # Disable for deterministic testing
        },
        "charge_rollup_rules": {
            "included_usage_categories": ["Base", "Amenity", "Parking", "Pet", "Add Ons"],
            "excluded_usage_categories": ["Lease Violation", "Maintenance", "Special"]
        },
        "variance_thresholds": {
            "tolerance_amount": 0.01
        },
        "audit_statuses": {
            "pass": "Pass",
            "needs_review": "Needs Review",
            "true_discrepancy": "True Discrepancy",
            "expected_exception": "Expected Exception"
        }
    }


@pytest.fixture
def sample_ar_code_map(tmp_path):
    """Sample AR code usage map for testing."""
    map_data = {
        "property_id": 1001,
        "total_codes": 5,
        "mapping": {
            "154771": {"code": 2, "name": "Rent", "usage": "Base"},
            "155007": {"code": 3, "name": "Amenity Premium", "usage": "Amenity"},
            "155052": {"code": 3, "name": "Parking", "usage": "Parking"},
            "155034": {"code": 3, "name": "Pet Rent", "usage": "Pet"},
            "154789": {"code": 3, "name": "Resident Fines", "usage": "Lease Violation"}
        }
    }
    
    map_path = tmp_path / "ar_code_name_usage_map.json"
    with open(map_path, 'w') as f:
        json.dump(map_data, f)
    
    return map_path


# Test 1: Perfect match (1500 = 1400 + 100)
def test_perfect_match(sample_config, sample_ar_code_map):
    """
    Test scenario where scheduled charges exactly match lease contract.
    Expected: Pass status, zero variance.
    """
    # Scheduled charges: Base Rent (1400) + Amenity (100) = 1500
    scheduled_df = pd.DataFrame([
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5001,
            CanonicalField.AR_CODE_ID.value: "154771",
            CanonicalField.EXPECTED_AMOUNT.value: 1400.00
        },
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5001,
            CanonicalField.AR_CODE_ID.value: "155007",
            CanonicalField.EXPECTED_AMOUNT.value: 100.00
        }
    ])
    
    # Build rollup map
    rollup_map = build_charge_rollup_map(sample_ar_code_map, sample_config)
    
    # Calculate rollup
    rollup_df = calculate_scheduled_charge_rollup(scheduled_df, rollup_map, sample_config)
    
    assert len(rollup_df) == 1
    assert rollup_df.iloc[0][CanonicalField.SCHEDULED_CHARGE_ROLLUP_TOTAL.value] == 1500.00
    
    # Add contract amount
    contract_amounts = {5001: 1500.00}
    variance_df = calculate_future_lease_variances(rollup_df, contract_amounts, sample_config)
    
    assert variance_df.iloc[0][CanonicalField.VARIANCE.value] == 0.00
    assert variance_df.iloc[0][CanonicalField.VARIANCE_DIRECTION.value] == 'matched'
    
    # Classify status
    final_df = classify_audit_status(variance_df, sample_config)
    
    assert final_df.iloc[0][CanonicalField.FUTURE_LEASE_AUDIT_STATUS.value] == "Pass"
    assert "match" in final_df.iloc[0][CanonicalField.EXCEPTION_REASON.value].lower()


# Test 2: Undercharge (-100)
def test_undercharge(sample_config, sample_ar_code_map):
    """
    Test scenario where scheduled charges are less than lease contract.
    Expected: True Discrepancy status, negative variance (undercharge).
    """
    # Scheduled charges: Base Rent (1400) only = 1400
    # Contract: 1500
    # Variance: 1400 - 1500 = -100 (undercharge)
    scheduled_df = pd.DataFrame([
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5002,
            CanonicalField.AR_CODE_ID.value: "154771",
            CanonicalField.EXPECTED_AMOUNT.value: 1400.00
        }
    ])
    
    rollup_map = build_charge_rollup_map(sample_ar_code_map, sample_config)
    rollup_df = calculate_scheduled_charge_rollup(scheduled_df, rollup_map, sample_config)
    
    assert rollup_df.iloc[0][CanonicalField.SCHEDULED_CHARGE_ROLLUP_TOTAL.value] == 1400.00
    
    contract_amounts = {5002: 1500.00}
    variance_df = calculate_future_lease_variances(rollup_df, contract_amounts, sample_config)
    
    assert variance_df.iloc[0][CanonicalField.VARIANCE.value] == -100.00
    assert variance_df.iloc[0][CanonicalField.VARIANCE_DIRECTION.value] == 'undercharge'
    
    final_df = classify_audit_status(variance_df, sample_config)
    
    assert final_df.iloc[0][CanonicalField.FUTURE_LEASE_AUDIT_STATUS.value] == "True Discrepancy"
    assert "under contract" in final_df.iloc[0][CanonicalField.EXCEPTION_REASON.value].lower()
    assert "100.00" in final_df.iloc[0][CanonicalField.EXCEPTION_REASON.value]


# Test 3: Overcharge (+100)
def test_overcharge(sample_config, sample_ar_code_map):
    """
    Test scenario where scheduled charges exceed lease contract.
    Expected: True Discrepancy status, positive variance (overcharge).
    """
    # Scheduled charges: Base Rent (1600) = 1600
    # Contract: 1500
    # Variance: 1600 - 1500 = +100 (overcharge)
    scheduled_df = pd.DataFrame([
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5003,
            CanonicalField.AR_CODE_ID.value: "154771",
            CanonicalField.EXPECTED_AMOUNT.value: 1600.00
        }
    ])
    
    rollup_map = build_charge_rollup_map(sample_ar_code_map, sample_config)
    rollup_df = calculate_scheduled_charge_rollup(scheduled_df, rollup_map, sample_config)
    
    assert rollup_df.iloc[0][CanonicalField.SCHEDULED_CHARGE_ROLLUP_TOTAL.value] == 1600.00
    
    contract_amounts = {5003: 1500.00}
    variance_df = calculate_future_lease_variances(rollup_df, contract_amounts, sample_config)
    
    assert variance_df.iloc[0][CanonicalField.VARIANCE.value] == 100.00
    assert variance_df.iloc[0][CanonicalField.VARIANCE_DIRECTION.value] == 'overcharge'
    
    final_df = classify_audit_status(variance_df, sample_config)
    
    assert final_df.iloc[0][CanonicalField.FUTURE_LEASE_AUDIT_STATUS.value] == "True Discrepancy"
    assert "exceed contract" in final_df.iloc[0][CanonicalField.EXCEPTION_REASON.value].lower()
    assert "100.00" in final_df.iloc[0][CanonicalField.EXCEPTION_REASON.value]


# Test 4: Missing contract amount
def test_missing_contract_amount(sample_config, sample_ar_code_map):
    """
    Test scenario where lease contract amount is not available.
    Expected: Needs Review status.
    """
    scheduled_df = pd.DataFrame([
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5004,
            CanonicalField.AR_CODE_ID.value: "154771",
            CanonicalField.EXPECTED_AMOUNT.value: 1500.00
        }
    ])
    
    rollup_map = build_charge_rollup_map(sample_ar_code_map, sample_config)
    rollup_df = calculate_scheduled_charge_rollup(scheduled_df, rollup_map, sample_config)
    
    # Contract amount is None (missing)
    contract_amounts = {5004: None}
    variance_df = calculate_future_lease_variances(rollup_df, contract_amounts, sample_config)
    
    assert pd.isna(variance_df.iloc[0][CanonicalField.LEASE_CONTRACT_AMOUNT.value])
    assert variance_df.iloc[0][CanonicalField.VARIANCE_DIRECTION.value] == 'unknown'
    
    final_df = classify_audit_status(variance_df, sample_config)
    
    assert final_df.iloc[0][CanonicalField.FUTURE_LEASE_AUDIT_STATUS.value] == "Needs Review"
    assert "missing" in final_df.iloc[0][CanonicalField.EXCEPTION_REASON.value].lower()
    assert "lease contract amount" in final_df.iloc[0][CanonicalField.EXCEPTION_REASON.value].lower()


# Test 5: Unmapped charge code
def test_unmapped_charge_code(sample_config, sample_ar_code_map):
    """
    Test scenario where scheduled charge contains unmapped AR code.
    Expected: Needs Review status.
    """
    scheduled_df = pd.DataFrame([
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5005,
            CanonicalField.AR_CODE_ID.value: "154771",
            CanonicalField.EXPECTED_AMOUNT.value: 1400.00
        },
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5005,
            CanonicalField.AR_CODE_ID.value: "999999",  # Unmapped code
            CanonicalField.EXPECTED_AMOUNT.value: 100.00
        }
    ])
    
    rollup_map = build_charge_rollup_map(sample_ar_code_map, sample_config)
    rollup_df = calculate_scheduled_charge_rollup(scheduled_df, rollup_map, sample_config)
    
    # Check that unmapped code is tracked
    assert "999999" in rollup_df.iloc[0][CanonicalField.UNMAPPED_CHARGE_CODES.value]
    
    contract_amounts = {5005: 1500.00}
    variance_df = calculate_future_lease_variances(rollup_df, contract_amounts, sample_config)
    
    final_df = classify_audit_status(variance_df, sample_config)
    
    assert final_df.iloc[0][CanonicalField.FUTURE_LEASE_AUDIT_STATUS.value] == "Needs Review"
    assert "unmapped" in final_df.iloc[0][CanonicalField.EXCEPTION_REASON.value].lower()
    assert "999999" in final_df.iloc[0][CanonicalField.EXCEPTION_REASON.value]


# Test 6: Excluded charge categories
def test_excluded_charge_categories(sample_config, sample_ar_code_map):
    """
    Test scenario where scheduled charges include excluded categories.
    Expected: Excluded charges not counted in rollup.
    """
    # Scheduled charges:
    # - Base Rent (1400) - INCLUDED
    # - Amenity (100) - INCLUDED
    # - Resident Fines (50) - EXCLUDED (Lease Violation)
    scheduled_df = pd.DataFrame([
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5006,
            CanonicalField.AR_CODE_ID.value: "154771",
            CanonicalField.EXPECTED_AMOUNT.value: 1400.00
        },
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5006,
            CanonicalField.AR_CODE_ID.value: "155007",
            CanonicalField.EXPECTED_AMOUNT.value: 100.00
        },
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5006,
            CanonicalField.AR_CODE_ID.value: "154789",  # Lease Violation - EXCLUDED
            CanonicalField.EXPECTED_AMOUNT.value: 50.00
        }
    ])
    
    rollup_map = build_charge_rollup_map(sample_ar_code_map, sample_config)
    rollup_df = calculate_scheduled_charge_rollup(scheduled_df, rollup_map, sample_config)
    
    # Rollup should only include Base + Amenity = 1500, not the fine (50)
    assert rollup_df.iloc[0][CanonicalField.SCHEDULED_CHARGE_ROLLUP_TOTAL.value] == 1500.00
    
    # Check excluded codes are tracked
    assert "154789" in rollup_df.iloc[0][CanonicalField.EXCLUDED_CHARGE_CODES.value]
    
    # Check included codes
    included_codes = rollup_df.iloc[0][CanonicalField.INCLUDED_CHARGE_CODES.value]
    assert "154771" in included_codes
    assert "155007" in included_codes
    assert "154789" not in included_codes


# Test 7: Integration test - execute_future_lease_audit
def test_execute_future_lease_audit_integration(sample_config, sample_ar_code_map, tmp_path, monkeypatch):
    """
    Integration test of complete future lease audit workflow.
    """
    # Mock the ar_code_name_usage_map.json path
    monkeypatch.setattr(
        'audit_engine.future_lease_audit.Path',
        lambda x: tmp_path if str(x).endswith('future_lease_audit.py') else Path(x)
    )
    
    # Copy AR code map to expected location
    import shutil
    shutil.copy(sample_ar_code_map, tmp_path / "ar_code_name_usage_map.json")
    
    # Create scheduled charges DataFrame with mix of scenarios
    scheduled_df = pd.DataFrame([
        # Lease 1: Perfect match
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5001,
            CanonicalField.AR_CODE_ID.value: "154771",
            CanonicalField.EXPECTED_AMOUNT.value: 1400.00,
            CanonicalField.PERIOD_START.value: pd.Timestamp('2026-08-01')
        },
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5001,
            CanonicalField.AR_CODE_ID.value: "155007",
            CanonicalField.EXPECTED_AMOUNT.value: 100.00,
            CanonicalField.PERIOD_START.value: pd.Timestamp('2026-08-01')
        },
        # Lease 2: Undercharge
        {
            CanonicalField.PROPERTY_ID.value: 1001,
            CanonicalField.LEASE_INTERVAL_ID.value: 5002,
            CanonicalField.AR_CODE_ID.value: "154771",
            CanonicalField.EXPECTED_AMOUNT.value: 1200.00,
            CanonicalField.PERIOD_START.value: pd.Timestamp('2026-09-01')
        }
    ])
    
    # Mock storage service
    class MockStorageService:
        pass
    
    storage = MockStorageService()
    
    # Execute audit (will use sample config without lease contract amounts)
    result = execute_future_lease_audit(
        scheduled_df=scheduled_df,
        run_id="test_run_001",
        config=sample_config,
        storage_service=storage
    )
    
    # Verify results structure
    assert 'future_lease_results' in result
    assert 'kpis' in result
    assert 'metadata' in result
    
    # Verify KPIs
    kpis = result['kpis']
    assert kpis['total_future_leases'] == 2
    
    # Both leases should be "Needs Review" because contract amounts are missing
    assert kpis['needs_review_count'] == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
