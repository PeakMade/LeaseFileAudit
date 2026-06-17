"""
Future Lease Audit Module - Phase 1

Validates future-status leases by comparing lease contract amounts against
scheduled recurring charge totals. Does not rely on ledger-posted AR transactions
since future leases may not have billing activity yet.

Key Differences from Standard Reconciliation:
1. Uses signed lease contract as source of truth (not AR transactions)
2. Compares against scheduled charge rollup (not posted charges)
3. Focuses on future leases only (not current/past)
4. Does not flag SCHEDULED_NOT_BILLED for future months
5. Classification: Pass/Needs Review/True Discrepancy (not MATCHED/MISMATCH)
"""

import pandas as pd
import logging
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

from .canonical_fields import CanonicalField

logger = logging.getLogger(__name__)


def load_future_lease_config() -> dict:
    """
    Load future lease audit configuration from JSON file.
    
    Returns:
        Configuration dictionary with defaults if file not found
    """
    config_path = Path(__file__).parent.parent / "future_lease_audit_config.json"
    
    if not config_path.exists():
        logger.warning(f"[FUTURE LEASE AUDIT] Config file not found at {config_path}, using defaults")
        return {
            "enabled": False,
            "charge_rollup_rules": {
                "included_usage_categories": ["Base", "Amenity", "Parking", "Pet", "Add Ons"]
            },
            "variance_thresholds": {
                "tolerance_amount": 0.01
            }
        }
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"[FUTURE LEASE AUDIT] Loaded configuration from {config_path}")
        return config
    except Exception as e:
        logger.error(f"[FUTURE LEASE AUDIT] Error loading config: {e}")
        return {"enabled": False}


def identify_future_leases(
    scheduled_df: pd.DataFrame,
    config: dict
) -> pd.DataFrame:
    """
    Filter scheduled charges to future-status leases only.
    
    Args:
        scheduled_df: Normalized scheduled charges DataFrame
        config: Future lease audit configuration
    
    Returns:
        Filtered DataFrame containing only future lease scheduled charges
    """
    if scheduled_df.empty:
        logger.info("[FUTURE LEASE AUDIT] No scheduled charges to filter")
        return scheduled_df.iloc[0:0].copy()
    
    logger.info(f"[FUTURE LEASE AUDIT] Identifying future leases from {len(scheduled_df)} scheduled charges")
    
    # Filter by lease start date if available
    lease_selection = config.get("lease_selection", {})
    require_future_start = lease_selection.get("require_lease_start_after_today", True)
    
    if require_future_start:
        today = pd.Timestamp.now().normalize()
        
        # Check if we have lease start date information
        start_col = CanonicalField.LEASE_START_DATE.value
        if start_col in scheduled_df.columns:
            start_dates = pd.to_datetime(scheduled_df[start_col], errors='coerce')
            future_mask = start_dates > today
            
            future_df = scheduled_df[future_mask].copy()
            logger.info(
                f"[FUTURE LEASE AUDIT] Filtered to {len(future_df)} scheduled charges "
                f"with lease start > {today.date()} (from {len(scheduled_df)} total)"
            )
        else:
            # Fallback: use charge start date
            charge_start_col = CanonicalField.PERIOD_START.value
            if charge_start_col in scheduled_df.columns:
                charge_dates = pd.to_datetime(scheduled_df[charge_start_col], errors='coerce')
                future_mask = charge_dates > today
                future_df = scheduled_df[future_mask].copy()
                logger.info(
                    f"[FUTURE LEASE AUDIT] Filtered to {len(future_df)} scheduled charges "
                    f"with charge start > {today.date()} (from {len(scheduled_df)} total)"
                )
            else:
                logger.warning("[FUTURE LEASE AUDIT] No date columns found, returning all scheduled charges")
                future_df = scheduled_df.copy()
    else:
        future_df = scheduled_df.copy()
    
    return future_df


def build_charge_rollup_map(
    ar_code_usage_map_path: Path,
    config: dict
) -> Dict[str, dict]:
    """
    Build mapping of AR codes to rollup inclusion rules.
    
    Args:
        ar_code_usage_map_path: Path to ar_code_name_usage_map.json
        config: Future lease audit configuration
    
    Returns:
        Dictionary mapping AR_CODE_ID -> {include: bool, usage: str, name: str}
    """
    rollup_rules = config.get("charge_rollup_rules", {})
    included_categories = set(rollup_rules.get("included_usage_categories", []))
    excluded_categories = set(rollup_rules.get("excluded_usage_categories", []))
    
    logger.info(f"[FUTURE LEASE AUDIT] Building charge rollup map")
    logger.info(f"[FUTURE LEASE AUDIT] Included categories: {included_categories}")
    logger.info(f"[FUTURE LEASE AUDIT] Excluded categories: {excluded_categories}")
    
    rollup_map = {}
    
    if not ar_code_usage_map_path.exists():
        logger.warning(f"[FUTURE LEASE AUDIT] AR code usage map not found at {ar_code_usage_map_path}")
        return rollup_map
    
    try:
        with open(ar_code_usage_map_path, 'r') as f:
            usage_data = json.load(f)
        
        mapping = usage_data.get("mapping", {})
        
        for ar_code_id, code_info in mapping.items():
            usage = code_info.get("usage", "")
            name = code_info.get("name", "")
            
            # Determine if this code should be included in rollup
            if excluded_categories and usage in excluded_categories:
                include = False
            elif included_categories and usage in included_categories:
                include = True
            elif not included_categories:
                # If no included categories specified, include everything not explicitly excluded
                include = usage not in excluded_categories
            else:
                # Not in included categories and included categories are specified
                include = False
            
            rollup_map[str(ar_code_id)] = {
                "include": include,
                "usage": usage,
                "name": name
            }
        
        included_count = sum(1 for v in rollup_map.values() if v["include"])
        logger.info(
            f"[FUTURE LEASE AUDIT] Loaded rollup map: {len(rollup_map)} codes total, "
            f"{included_count} included in rollup"
        )
        
    except Exception as e:
        logger.error(f"[FUTURE LEASE AUDIT] Error loading AR code usage map: {e}")
    
    return rollup_map


def calculate_scheduled_charge_rollup(
    scheduled_df: pd.DataFrame,
    rollup_map: Dict[str, dict],
    config: dict
) -> pd.DataFrame:
    """
    Calculate scheduled charge rollup totals per lease.
    
    Args:
        scheduled_df: Future lease scheduled charges
        rollup_map: AR code rollup inclusion rules
        config: Future lease audit configuration
    
    Returns:
        DataFrame with one row per lease containing rollup totals and code lists
    """
    if scheduled_df.empty:
        logger.info("[FUTURE LEASE AUDIT] No scheduled charges to roll up")
        return pd.DataFrame(columns=[
            CanonicalField.PROPERTY_ID.value,
            CanonicalField.LEASE_INTERVAL_ID.value,
            CanonicalField.SCHEDULED_CHARGE_ROLLUP_TOTAL.value,
            CanonicalField.INCLUDED_CHARGE_CODES.value,
            CanonicalField.EXCLUDED_CHARGE_CODES.value,
            CanonicalField.UNMAPPED_CHARGE_CODES.value
        ])
    
    logger.info(f"[FUTURE LEASE AUDIT] Calculating scheduled charge rollup for {len(scheduled_df)} charges")
    
    # Get required columns
    property_col = CanonicalField.PROPERTY_ID.value
    lease_col = CanonicalField.LEASE_INTERVAL_ID.value
    ar_code_col = CanonicalField.AR_CODE_ID.value
    amount_col = CanonicalField.EXPECTED_AMOUNT.value
    
    # Ensure we have required columns
    if not all(col in scheduled_df.columns for col in [property_col, lease_col, ar_code_col, amount_col]):
        missing = [col for col in [property_col, lease_col, ar_code_col, amount_col] if col not in scheduled_df.columns]
        logger.error(f"[FUTURE LEASE AUDIT] Missing required columns: {missing}")
        return pd.DataFrame()
    
    # Add rollup inclusion flag
    scheduled_df = scheduled_df.copy()
    scheduled_df['_ar_code_str'] = scheduled_df[ar_code_col].astype(str)
    scheduled_df['_include_in_rollup'] = scheduled_df['_ar_code_str'].apply(
        lambda x: rollup_map.get(x, {}).get('include', False)
    )
    scheduled_df['_usage_category'] = scheduled_df['_ar_code_str'].apply(
        lambda x: rollup_map.get(x, {}).get('usage', 'UNMAPPED')
    )
    
    # Group by lease
    lease_groups = scheduled_df.groupby([property_col, lease_col])
    
    rollup_results = []
    
    for (property_id, lease_interval_id), group in lease_groups:
        # Calculate rollup total (only included charges)
        included_charges = group[group['_include_in_rollup']]
        rollup_total = pd.to_numeric(included_charges[amount_col], errors='coerce').fillna(0).sum()
        
        # Collect code lists
        included_codes = included_charges['_ar_code_str'].unique().tolist()
        excluded_charges = group[~group['_include_in_rollup'] & (group['_usage_category'] != 'UNMAPPED')]
        excluded_codes = excluded_charges['_ar_code_str'].unique().tolist()
        unmapped_charges = group[group['_usage_category'] == 'UNMAPPED']
        unmapped_codes = unmapped_charges['_ar_code_str'].unique().tolist()
        
        rollup_results.append({
            property_col: property_id,
            lease_col: lease_interval_id,
            CanonicalField.SCHEDULED_CHARGE_ROLLUP_TOTAL.value: float(rollup_total),
            CanonicalField.INCLUDED_CHARGE_CODES.value: ','.join(included_codes) if included_codes else '',
            CanonicalField.EXCLUDED_CHARGE_CODES.value: ','.join(excluded_codes) if excluded_codes else '',
            CanonicalField.UNMAPPED_CHARGE_CODES.value: ','.join(unmapped_codes) if unmapped_codes else '',
            '_included_charge_count': len(included_charges),
            '_excluded_charge_count': len(excluded_charges),
            '_unmapped_charge_count': len(unmapped_charges)
        })
    
    rollup_df = pd.DataFrame(rollup_results)
    
    logger.info(f"[FUTURE LEASE AUDIT] Calculated rollup for {len(rollup_df)} leases")
    logger.info(
        f"[FUTURE LEASE AUDIT] Total scheduled charge rollup: "
        f"${rollup_df[CanonicalField.SCHEDULED_CHARGE_ROLLUP_TOTAL.value].sum():,.2f}"
    )
    
    return rollup_df


def get_lease_contract_amounts(
    lease_interval_ids: List[Any],
    storage_service
) -> Dict[Any, Optional[float]]:
    """
    Retrieve lease contract amounts from lease term extraction.
    
    Args:
        lease_interval_ids: List of lease interval IDs to look up
        storage_service: Storage service instance for SharePoint access
    
    Returns:
        Dictionary mapping lease_interval_id -> contract_amount (or None if not found)
    """
    logger.info(f"[FUTURE LEASE AUDIT] Retrieving lease contract amounts for {len(lease_interval_ids)} leases")
    
    contract_amounts = {}
    
    # TODO: Implement lease term extraction lookup from SharePoint LeaseTerms list
    # For now, return empty dict (will trigger "Needs Review" status)
    
    for lease_id in lease_interval_ids:
        # Placeholder: In production, query LeaseTerms list and extract:
        # - BASE_RENT
        # - amenity_rent
        # - parking_rent
        # - pet_rent
        # Sum these to get contract_amount
        contract_amounts[lease_id] = None
    
    found_count = sum(1 for v in contract_amounts.values() if v is not None)
    logger.info(
        f"[FUTURE LEASE AUDIT] Found contract amounts for {found_count}/{len(lease_interval_ids)} leases"
    )
    
    return contract_amounts


def calculate_future_lease_variances(
    rollup_df: pd.DataFrame,
    contract_amounts: Dict[Any, Optional[float]],
    config: dict
) -> pd.DataFrame:
    """
    Calculate variances between scheduled rollup and lease contract amounts.
    
    Args:
        rollup_df: Scheduled charge rollup DataFrame
        contract_amounts: Lease contract amounts by lease_interval_id
        config: Future lease audit configuration
    
    Returns:
        DataFrame with variance calculations and direction classification
    """
    if rollup_df.empty:
        logger.info("[FUTURE LEASE AUDIT] No rollup data to calculate variances")
        return rollup_df.copy()
    
    logger.info(f"[FUTURE LEASE AUDIT] Calculating variances for {len(rollup_df)} leases")
    
    variance_df = rollup_df.copy()
    lease_col = CanonicalField.LEASE_INTERVAL_ID.value
    
    # Add contract amounts
    variance_df[CanonicalField.LEASE_CONTRACT_AMOUNT.value] = variance_df[lease_col].map(contract_amounts)
    
    # Calculate variance (scheduled - contract)
    scheduled_total_col = CanonicalField.SCHEDULED_CHARGE_ROLLUP_TOTAL.value
    contract_col = CanonicalField.LEASE_CONTRACT_AMOUNT.value
    variance_col = CanonicalField.VARIANCE.value
    
    variance_df[variance_col] = (
        pd.to_numeric(variance_df[scheduled_total_col], errors='coerce').fillna(0) -
        pd.to_numeric(variance_df[contract_col], errors='coerce').fillna(0)
    )
    
    # Determine variance direction
    tolerance = config.get("variance_thresholds", {}).get("tolerance_amount", 0.01)
    
    def classify_direction(row):
        if pd.isna(row[contract_col]) or row[contract_col] is None:
            return 'unknown'
        
        var = row[variance_col]
        if abs(var) <= tolerance:
            return 'matched'
        elif var > 0:
            return 'overcharge'
        else:
            return 'undercharge'
    
    variance_df[CanonicalField.VARIANCE_DIRECTION.value] = variance_df.apply(classify_direction, axis=1)
    
    # Summary statistics
    matched_count = (variance_df[CanonicalField.VARIANCE_DIRECTION.value] == 'matched').sum()
    overcharge_count = (variance_df[CanonicalField.VARIANCE_DIRECTION.value] == 'overcharge').sum()
    undercharge_count = (variance_df[CanonicalField.VARIANCE_DIRECTION.value] == 'undercharge').sum()
    unknown_count = (variance_df[CanonicalField.VARIANCE_DIRECTION.value] == 'unknown').sum()
    
    logger.info(f"[FUTURE LEASE AUDIT] Variance summary:")
    logger.info(f"[FUTURE LEASE AUDIT]   Matched: {matched_count}")
    logger.info(f"[FUTURE LEASE AUDIT]   Overcharge: {overcharge_count}")
    logger.info(f"[FUTURE LEASE AUDIT]   Undercharge: {undercharge_count}")
    logger.info(f"[FUTURE LEASE AUDIT]   Unknown: {unknown_count}")
    
    return variance_df


def classify_audit_status(
    variance_df: pd.DataFrame,
    config: dict
) -> pd.DataFrame:
    """
    Classify audit status based on variance and data quality.
    
    Status Rules:
    - Pass: Variance within tolerance
    - Needs Review: Missing contract amount OR unmapped codes present
    - True Discrepancy: Variance exceeds tolerance with all data present
    - Expected Exception: Matches configured business rule (future enhancement)
    
    Args:
        variance_df: DataFrame with variance calculations
        config: Future lease audit configuration
    
    Returns:
        DataFrame with audit status, exception reason, and recommended action
    """
    if variance_df.empty:
        return variance_df.copy()
    
    logger.info(f"[FUTURE LEASE AUDIT] Classifying audit status for {len(variance_df)} leases")
    
    result_df = variance_df.copy()
    
    # Get status names from config
    statuses = config.get("audit_statuses", {})
    status_pass = statuses.get("pass", "Pass")
    status_needs_review = statuses.get("needs_review", "Needs Review")
    status_true_discrepancy = statuses.get("true_discrepancy", "True Discrepancy")
    status_expected_exception = statuses.get("expected_exception", "Expected Exception")
    
    # Initialize status columns
    status_col = CanonicalField.FUTURE_LEASE_AUDIT_STATUS.value
    reason_col = CanonicalField.EXCEPTION_REASON.value
    action_col = CanonicalField.RECOMMENDED_ACTION.value
    
    result_df[status_col] = ''
    result_df[reason_col] = ''
    result_df[action_col] = ''
    
    # Classification logic
    contract_col = CanonicalField.LEASE_CONTRACT_AMOUNT.value
    unmapped_col = CanonicalField.UNMAPPED_CHARGE_CODES.value
    direction_col = CanonicalField.VARIANCE_DIRECTION.value
    variance_col = CanonicalField.VARIANCE.value
    
    for idx, row in result_df.iterrows():
        # Rule 1: Missing contract amount
        if pd.isna(row[contract_col]) or row[contract_col] is None:
            result_df.at[idx, status_col] = status_needs_review
            result_df.at[idx, reason_col] = "Missing lease contract amount"
            result_df.at[idx, action_col] = "Extract lease terms from signed lease document"
        
        # Rule 2: Unmapped charge codes present
        elif row[unmapped_col]:
            result_df.at[idx, status_col] = status_needs_review
            result_df.at[idx, reason_col] = f"Unmapped charge codes present: {row[unmapped_col]}"
            result_df.at[idx, action_col] = "Add usage category mapping for charge codes"
        
        # Rule 3: Matched (within tolerance)
        elif row[direction_col] == 'matched':
            result_df.at[idx, status_col] = status_pass
            result_df.at[idx, reason_col] = "Scheduled charges match lease contract within tolerance"
            result_df.at[idx, action_col] = "No action required"
        
        # Rule 4: True discrepancy (overcharge or undercharge)
        elif row[direction_col] in ['overcharge', 'undercharge']:
            result_df.at[idx, status_col] = status_true_discrepancy
            if row[direction_col] == 'overcharge':
                result_df.at[idx, reason_col] = f"Scheduled charges exceed contract by ${abs(row[variance_col]):.2f}"
                result_df.at[idx, action_col] = "Review and reduce scheduled charges to match lease contract"
            else:
                result_df.at[idx, reason_col] = f"Scheduled charges under contract by ${abs(row[variance_col]):.2f}"
                result_df.at[idx, action_col] = "Review and add missing scheduled charges"
        
        # Rule 5: Unknown (fallback)
        else:
            result_df.at[idx, status_col] = status_needs_review
            result_df.at[idx, reason_col] = "Unable to determine variance direction"
            result_df.at[idx, action_col] = "Manual review required"
    
    # Summary by status
    status_counts = result_df[status_col].value_counts().to_dict()
    logger.info(f"[FUTURE LEASE AUDIT] Status classification summary:")
    for status, count in status_counts.items():
        logger.info(f"[FUTURE LEASE AUDIT]   {status}: {count}")
    
    return result_df


def execute_future_lease_audit(
    scheduled_df: pd.DataFrame,
    run_id: str,
    config: dict,
    storage_service
) -> dict:
    """
    Execute complete future lease audit workflow.
    
    Args:
        scheduled_df: Normalized scheduled charges DataFrame
        run_id: Audit run identifier
        config: Future lease audit configuration
        storage_service: Storage service for persistence
    
    Returns:
        Dictionary with audit results, KPIs, and metadata
    """
    logger.info(f"[FUTURE LEASE AUDIT] ========== STARTING FUTURE LEASE AUDIT ==========")
    logger.info(f"[FUTURE LEASE AUDIT] Run ID: {run_id}")
    logger.info(f"[FUTURE LEASE AUDIT] Input scheduled charges: {len(scheduled_df)}")
    
    start_time = datetime.now()
    
    # Step 1: Identify future leases
    future_leases_df = identify_future_leases(scheduled_df, config)
    
    if future_leases_df.empty:
        logger.info("[FUTURE LEASE AUDIT] No future leases found, skipping audit")
        return {
            "future_lease_results": pd.DataFrame(),
            "kpis": {
                "total_future_leases": 0,
                "pass_count": 0,
                "needs_review_count": 0,
                "true_discrepancy_count": 0,
                "total_potential_undercharge": 0.0,
                "total_potential_overcharge": 0.0,
                "match_rate": 0.0
            },
            "metadata": {
                "run_id": run_id,
                "timestamp": start_time.isoformat(),
                "duration_seconds": 0,
                "config": config
            }
        }
    
    # Step 2: Build charge rollup map
    ar_code_map_path = Path(__file__).parent.parent / "ar_code_name_usage_map.json"
    rollup_map = build_charge_rollup_map(ar_code_map_path, config)
    
    # Step 3: Calculate scheduled charge rollup
    rollup_df = calculate_scheduled_charge_rollup(future_leases_df, rollup_map, config)
    
    if rollup_df.empty:
        logger.warning("[FUTURE LEASE AUDIT] No rollup results calculated")
        return {
            "future_lease_results": pd.DataFrame(),
            "kpis": {},
            "metadata": {"run_id": run_id}
        }
    
    # Step 4: Get lease contract amounts
    lease_ids = rollup_df[CanonicalField.LEASE_INTERVAL_ID.value].unique().tolist()
    contract_amounts = get_lease_contract_amounts(lease_ids, storage_service)
    
    # Step 5: Calculate variances
    variance_df = calculate_future_lease_variances(rollup_df, contract_amounts, config)
    
    # Step 6: Classify audit status
    final_results = classify_audit_status(variance_df, config)
    
    # Calculate KPIs
    from .metrics import calculate_future_lease_kpis
    kpis = calculate_future_lease_kpis(final_results)
    
    # Record metadata
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    metadata = {
        "run_id": run_id,
        "timestamp": start_time.isoformat(),
        "duration_seconds": duration,
        "total_scheduled_charges_analyzed": len(future_leases_df),
        "total_leases_audited": len(final_results),
        "config": config
    }
    
    logger.info(f"[FUTURE LEASE AUDIT] ========== AUDIT COMPLETE ==========")
    logger.info(f"[FUTURE LEASE AUDIT] Duration: {duration:.2f}s")
    logger.info(f"[FUTURE LEASE AUDIT] Leases audited: {len(final_results)}")
    logger.info(f"[FUTURE LEASE AUDIT] Pass: {kpis.get('pass_count', 0)}")
    logger.info(f"[FUTURE LEASE AUDIT] True Discrepancies: {kpis.get('true_discrepancy_count', 0)}")
    
    return {
        "future_lease_results": final_results,
        "kpis": kpis,
        "metadata": metadata
    }
