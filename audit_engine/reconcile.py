"""
Reconciliation logic - aggregate and compare expected vs actual at bucket grain.

This module provides two levels of reconciliation:
1. reconcile_buckets: Aggregate bucket-level reconciliation (existing v1)
2. reconcile_detail: Row-level reconciliation with PRIMARY and SECONDARY matching (framework)
"""
import pandas as pd
import logging
from typing import Tuple, Dict, List
from .canonical_fields import CanonicalField, BUCKET_KEY_FIELDS, get_field_names
from config import ReconciliationConfig

logger = logging.getLogger(__name__)

BUCKET_KEY_COLUMNS = list(get_field_names(BUCKET_KEY_FIELDS))


def reconcile_buckets(
    expected_detail: pd.DataFrame,
    actual_detail: pd.DataFrame,
    recon_config: ReconciliationConfig
) -> pd.DataFrame:
    """
    Aggregate expected and actual by bucket key, calculate variance and status.
    
    Note: API/timed/external charges should be filtered from actual_detail BEFORE calling this function.
    
    Args:
        expected_detail: Expanded scheduled charges with AUDIT_MONTH
        actual_detail: Normalized AR transactions with AUDIT_MONTH (API codes already filtered)
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
    
    Note: Timed/external charges (API codes) are filtered out before reconciliation,
    so they never reach this classification step.
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


# ==================== FRAMEWORK: Detailed Row-Level Reconciliation ====================

def reconcile_detail(
    scheduled_df: pd.DataFrame,
    ar_df: pd.DataFrame,
    recon_config: ReconciliationConfig
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    STEP 3: Match Actual to Expected using PRIMARY and SECONDARY matching hierarchy.
    
    FRAMEWORK IMPLEMENTATION:
    - PRIMARY MATCHING: SCHEDULED_CHARGE_ID_LINK (direct foreign key)
    - SECONDARY MATCHING: Fuzzy match on LEASE_INTERVAL_ID + AR_CODE_ID + date proximity + amount
    - Event-driven charges (payments, fees, adjustments) expected to remain unmatched
    
    Args:
        scheduled_df: Active scheduled charges (already filtered by _scheduled_row_filter)
        ar_df: Posted AR transactions (already filtered to posted & not deleted)
        recon_config: Reconciliation configuration with tolerances
    
    Returns:
        Tuple of:
        - DataFrame with detailed match results (one row per scheduled charge or AR trans)
        - Dict with match statistics (primary_matched, secondary_matched, unmatched_ar, etc.)
    """
    logger.info(f"Starting detailed reconciliation: {len(scheduled_df)} scheduled, {len(ar_df)} AR transactions")
    
    # Initialize match tracking columns
    scheduled_df = scheduled_df.copy()
    ar_df = ar_df.copy()
    
    scheduled_df['MATCHED'] = False
    scheduled_df['MATCH_TYPE'] = None
    
    ar_df['MATCHED'] = False
    ar_df['MATCH_TYPE'] = None
    ar_df['MATCHED_SCHEDULED_ID'] = None
    
    # Track matched AR IDs separately in a dictionary (avoid storing lists in DataFrame)
    scheduled_to_ar_map = {sched_id: [] for sched_id in scheduled_df[CanonicalField.SCHEDULED_CHARGES_ID.value]}
    
    # STEP 3A: PRIMARY MATCHING - SCHEDULED_CHARGE_ID_LINK
    primary_matched_ar, primary_matched_scheduled = _match_primary(ar_df, scheduled_df, recon_config, scheduled_to_ar_map)
    
    # STEP 3B: SECONDARY MATCHING - Fuzzy match on remaining unmatched
    secondary_matched_ar, secondary_matched_scheduled = _match_secondary(
        ar_df[~ar_df['MATCHED']],
        scheduled_df[~scheduled_df['MATCHED']],
        recon_config,
        scheduled_to_ar_map
    )
    
    # STEP 3C: TERTIARY MATCHING - Match by lease/AR code/amount even if dates don't align (DATE_MISMATCH)
    tertiary_matched_ar, tertiary_matched_scheduled = _match_tertiary_date_mismatch(
        ar_df[~ar_df['MATCHED']],
        scheduled_df[~scheduled_df['MATCHED']],
        recon_config,
        scheduled_to_ar_map
    )
    
    # Combine results
    ar_df.update(primary_matched_ar)
    ar_df.update(secondary_matched_ar)
    ar_df.update(tertiary_matched_ar)
    scheduled_df.update(primary_matched_scheduled)
    scheduled_df.update(secondary_matched_scheduled)
    scheduled_df.update(tertiary_matched_scheduled)
    
    # STEP 4: IDENTIFY VARIANCES - Classify unmatched and mismatched records
    variance_df = _identify_variances(scheduled_df, ar_df, recon_config, scheduled_to_ar_map)
    
    # Calculate statistics
    stats = {
        'total_scheduled': len(scheduled_df),
        'total_ar': len(ar_df),
        'primary_matched_ar': len(primary_matched_ar[primary_matched_ar['MATCHED']]),
        'secondary_matched_ar': len(secondary_matched_ar[secondary_matched_ar['MATCHED']]),
        'tertiary_matched_ar': len(tertiary_matched_ar[tertiary_matched_ar['MATCHED']]),
        'unmatched_ar': len(ar_df[~ar_df['MATCHED']]),
        'unmatched_scheduled': len(scheduled_df[~scheduled_df['MATCHED']]),
        'variances': len(variance_df)
    }
    
    logger.info(f"Reconciliation complete: {stats}")
    
    return variance_df, stats


def _match_primary(
    ar_df: pd.DataFrame,
    scheduled_df: pd.DataFrame,
    recon_config: ReconciliationConfig,
    scheduled_to_ar_map: dict
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    PRIMARY MATCHING: Match AR transactions to scheduled charges via SCHEDULED_CHARGE_ID_LINK.
    
    This is the most reliable matching method - a direct foreign key relationship.
    One scheduled charge can generate multiple AR transactions.
    
    Returns:
        Tuple of updated AR and scheduled DataFrames with MATCHED flags set
    """
    ar_result = ar_df.copy()
    scheduled_result = scheduled_df.copy()
    
    # Check if link field exists
    if CanonicalField.SCHEDULED_CHARGE_ID_LINK.value not in ar_df.columns:
        logger.warning("SCHEDULED_CHARGE_ID_LINK not found in AR data - skipping primary matching")
        return ar_result, scheduled_result
    
    # Filter AR with valid links
    linked_ar = ar_df[ar_df[CanonicalField.SCHEDULED_CHARGE_ID_LINK.value].notna()].copy()
    
    if len(linked_ar) == 0:
        logger.info("No AR transactions with SCHEDULED_CHARGE_ID_LINK - skipping primary matching")
        return ar_result, scheduled_result
    
    # Match to scheduled charges
    scheduled_ids = scheduled_df[CanonicalField.SCHEDULED_CHARGES_ID.value].tolist()
    matched_ar_mask = linked_ar[CanonicalField.SCHEDULED_CHARGE_ID_LINK.value].isin(scheduled_ids)
    
    # Update AR matches
    matched_ar_ids = linked_ar[matched_ar_mask].index
    ar_result.loc[matched_ar_ids, 'MATCHED'] = True
    ar_result.loc[matched_ar_ids, 'MATCH_TYPE'] = 'PRIMARY'
    ar_result.loc[matched_ar_ids, 'MATCHED_SCHEDULED_ID'] = linked_ar.loc[matched_ar_ids, CanonicalField.SCHEDULED_CHARGE_ID_LINK.value]
    
    # Update scheduled matches (aggregate AR trans by scheduled ID)
    for sched_id in scheduled_ids:
        matched_ar_for_sched = linked_ar[
            (linked_ar[CanonicalField.SCHEDULED_CHARGE_ID_LINK.value] == sched_id) &
            matched_ar_mask
        ]
        
        if len(matched_ar_for_sched) > 0:
            sched_mask = scheduled_result[CanonicalField.SCHEDULED_CHARGES_ID.value] == sched_id
            scheduled_result.loc[sched_mask, 'MATCHED'] = True
            scheduled_result.loc[sched_mask, 'MATCH_TYPE'] = 'PRIMARY'
            # Store AR IDs in dictionary instead of DataFrame
            scheduled_to_ar_map[sched_id] = matched_ar_for_sched[CanonicalField.AR_TRANSACTION_ID.value].tolist()
    
    logger.info(f"Primary matching: {matched_ar_mask.sum()} AR transactions matched to scheduled charges")
    
    return ar_result, scheduled_result


def _match_secondary(
    ar_df: pd.DataFrame,
    scheduled_df: pd.DataFrame,
    recon_config: ReconciliationConfig,
    scheduled_to_ar_map: dict
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    SECONDARY MATCHING: Fuzzy match on LEASE_INTERVAL_ID + AR_CODE_ID + date + amount.
    
    Used for AR transactions without SCHEDULED_CHARGE_ID_LINK (mostly pre-2025 data).
    
    Match criteria:
    - Same LEASE_INTERVAL_ID and AR_CODE_ID
    - POST_DATE within scheduled charge period (PERIOD_START to PERIOD_END)
    - Amount matches within tolerance
    
    Returns:
        Tuple of updated AR and scheduled DataFrames with MATCHED flags set
    """
    ar_result = ar_df.copy()
    scheduled_result = scheduled_df.copy()
    
    if len(ar_df) == 0 or len(scheduled_df) == 0:
        return ar_result, scheduled_result
    
    # Required fields for secondary matching
    required_ar_fields = [
        CanonicalField.LEASE_INTERVAL_ID.value,
        CanonicalField.AR_CODE_ID.value,
        CanonicalField.POST_DATE.value,
        CanonicalField.ACTUAL_AMOUNT.value
    ]
    
    required_sched_fields = [
        CanonicalField.LEASE_INTERVAL_ID.value,
        CanonicalField.AR_CODE_ID.value,
        CanonicalField.PERIOD_START.value,
        CanonicalField.PERIOD_END.value,
        CanonicalField.EXPECTED_AMOUNT.value
    ]
    
    # Check if required fields exist
    if not all(f in ar_df.columns for f in required_ar_fields):
        logger.warning("Missing required fields for secondary matching in AR data")
        return ar_result, scheduled_result
    
    if not all(f in scheduled_df.columns for f in required_sched_fields):
        logger.warning("Missing required fields for secondary matching in scheduled data")
        return ar_result, scheduled_result
    
    matched_count = 0
    
    # Try to match each unmatched AR transaction
    for ar_idx, ar_row in ar_df.iterrows():
        # Find candidate scheduled charges (same lease interval and AR code)
        candidates = scheduled_df[
            (scheduled_df[CanonicalField.LEASE_INTERVAL_ID.value] == ar_row[CanonicalField.LEASE_INTERVAL_ID.value]) &
            (scheduled_df[CanonicalField.AR_CODE_ID.value] == ar_row[CanonicalField.AR_CODE_ID.value])
        ]
        
        if len(candidates) == 0:
            continue
        
        # Check date range: POST_DATE within PERIOD_START to PERIOD_END
        post_date = ar_row[CanonicalField.POST_DATE.value]
        
        # Filter candidates by date range
        date_match_candidates = candidates[
            (candidates[CanonicalField.PERIOD_START.value] <= post_date) &
            ((candidates[CanonicalField.PERIOD_END.value].isna()) | 
             (candidates[CanonicalField.PERIOD_END.value] >= post_date))
        ]
        
        if len(date_match_candidates) == 0:
            continue
        
        # Check amount match (within tolerance)
        ar_amount = ar_row[CanonicalField.ACTUAL_AMOUNT.value]
        amount_match_candidates = date_match_candidates[
            abs(date_match_candidates[CanonicalField.EXPECTED_AMOUNT.value] - ar_amount) <= recon_config.amount_tolerance
        ]
        
        if len(amount_match_candidates) > 0:
            # Take first match (could refine to pick "best" match)
            matched_sched = amount_match_candidates.iloc[0]
            matched_sched_id = matched_sched[CanonicalField.SCHEDULED_CHARGES_ID.value]
            
            # Update AR match
            ar_result.loc[ar_idx, 'MATCHED'] = True
            ar_result.loc[ar_idx, 'MATCH_TYPE'] = 'SECONDARY'
            ar_result.loc[ar_idx, 'MATCHED_SCHEDULED_ID'] = matched_sched_id
            
            # Update scheduled match
            sched_mask = scheduled_result[CanonicalField.SCHEDULED_CHARGES_ID.value] == matched_sched_id
            scheduled_result.loc[sched_mask, 'MATCHED'] = True
            scheduled_result.loc[sched_mask, 'MATCH_TYPE'] = 'SECONDARY'
            
            # Append AR ID to dictionary map
            if matched_sched_id not in scheduled_to_ar_map:
                scheduled_to_ar_map[matched_sched_id] = []
            scheduled_to_ar_map[matched_sched_id].append(ar_row[CanonicalField.AR_TRANSACTION_ID.value])
            
            matched_count += 1
    
    logger.info(f"Secondary matching: {matched_count} AR transactions matched to scheduled charges")
    
    return ar_result, scheduled_result


def _match_tertiary_date_mismatch(
    ar_df: pd.DataFrame,
    scheduled_df: pd.DataFrame,
    recon_config: ReconciliationConfig,
    scheduled_to_ar_map: dict
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    TERTIARY MATCHING: Match by LEASE_INTERVAL_ID + AR_CODE_ID + amount IGNORING date alignment.
    
    This catches DATE_MISMATCH scenarios where a scheduled charge was billed on the wrong date.
    Without this, they appear as separate "scheduled not billed" + "billed not scheduled" issues.
    
    Match criteria:
    - Same LEASE_INTERVAL_ID and AR_CODE_ID
    - Amount matches within tolerance (or if no amount match, just match by lease/AR code)
    - NO date requirement (that's what makes it a date mismatch)
    
    Matched pairs are flagged as DATE_MISMATCH variance type.
    
    Returns:
        Tuple of updated AR and scheduled DataFrames with MATCHED flags set
    """
    ar_result = ar_df.copy()
    scheduled_result = scheduled_df.copy()
    
    if len(ar_df) == 0 or len(scheduled_df) == 0:
        return ar_result, scheduled_result
    
    # Required fields
    required_ar_fields = [
        CanonicalField.LEASE_INTERVAL_ID.value,
        CanonicalField.AR_CODE_ID.value,
        CanonicalField.ACTUAL_AMOUNT.value
    ]
    
    required_sched_fields = [
        CanonicalField.LEASE_INTERVAL_ID.value,
        CanonicalField.AR_CODE_ID.value,
        CanonicalField.EXPECTED_AMOUNT.value
    ]
    
    # Check if required fields exist
    if not all(f in ar_df.columns for f in required_ar_fields):
        logger.warning("Missing required fields for tertiary matching in AR data")
        return ar_result, scheduled_result
    
    if not all(f in scheduled_df.columns for f in required_sched_fields):
        logger.warning("Missing required fields for tertiary matching in scheduled data")
        return ar_result, scheduled_result
    
    matched_count = 0
    
    print(f"\n[TERTIARY MATCHING] Starting - {len(ar_df)} unmatched AR, {len(scheduled_df)} unmatched scheduled")
    
    # Group by lease interval and AR code to find date mismatches
    for (lease_id, ar_code), ar_group in ar_df.groupby([CanonicalField.LEASE_INTERVAL_ID.value, CanonicalField.AR_CODE_ID.value]):
        # Find matching scheduled charges for this lease + AR code (that aren't already matched)
        sched_candidates = scheduled_result[
            (scheduled_result[CanonicalField.LEASE_INTERVAL_ID.value] == lease_id) &
            (scheduled_result[CanonicalField.AR_CODE_ID.value] == ar_code) &
            (~scheduled_result.get('MATCHED', False))  # Only consider unmatched scheduled charges
        ]
        
        if len(sched_candidates) == 0:
            continue
        
        print(f"[TERTIARY] Lease {lease_id}, AR code {ar_code}: {len(ar_group)} AR trans, {len(sched_candidates)} unmatched scheduled charges")
        
        # Match AR transactions to scheduled charges
        for ar_idx, ar_row in ar_group.iterrows():
            ar_amount = ar_row[CanonicalField.ACTUAL_AMOUNT.value]
            ar_id = ar_row.get(CanonicalField.AR_TRANSACTION_ID.value)
            post_date = ar_row.get(CanonicalField.POST_DATE.value)
            
            print(f"[TERTIARY]   AR trans {ar_id}: post_date={post_date}, amount=${ar_amount}")
            
            # Re-filter to get only currently unmatched scheduled charges (in case previous iteration matched one)
            available_candidates = sched_candidates[~sched_candidates.get('MATCHED', False)]
            
            if len(available_candidates) == 0:
                print(f"[TERTIARY]   No more available scheduled charges to match")
                break
            
            # Filter candidates where date is OUTSIDE the scheduled period (confirms date mismatch)
            if post_date and CanonicalField.PERIOD_START.value in available_candidates.columns:
                date_mismatch_candidates = available_candidates[
                    (available_candidates[CanonicalField.PERIOD_START.value] > post_date) |
                    ((available_candidates.get(CanonicalField.PERIOD_END.value, pd.Series()).notna()) & 
                     (available_candidates.get(CanonicalField.PERIOD_END.value, pd.Series()) < post_date))
                ]
                print(f"[TERTIARY]   Date mismatch candidates: {len(date_mismatch_candidates)}")
                for _, cand in date_mismatch_candidates.iterrows():
                    print(f"[TERTIARY]     Sched ID {cand[CanonicalField.SCHEDULED_CHARGES_ID.value]}: start={cand.get(CanonicalField.PERIOD_START.value)}, end={cand.get(CanonicalField.PERIOD_END.value)}, amount=${cand[CanonicalField.EXPECTED_AMOUNT.value]}")
            else:
                # No date field or can't check, treat all as potential mismatches
                date_mismatch_candidates = available_candidates
                print(f"[TERTIARY]   No post_date, treating all {len(available_candidates)} as candidates")
            
            if len(date_mismatch_candidates) == 0:
                # No candidates with date mismatches found
                print(f"[TERTIARY]   No date mismatch candidates found (dates might align)")
                continue
            
            # Try to match by amount first
            amount_match_candidates = date_mismatch_candidates[
                abs(date_mismatch_candidates[CanonicalField.EXPECTED_AMOUNT.value] - ar_amount) <= recon_config.amount_tolerance
            ]
            
            # Select best match
            if len(amount_match_candidates) > 0:
                matched_sched = amount_match_candidates.iloc[0]
            elif len(date_mismatch_candidates) > 0:
                # No amount match but same lease+AR code with date mismatch
                logger.info(f"TERTIARY: Matching AR trans (lease {lease_id}, AR code {ar_code}) by lease+AR code only (amount differs + date mismatch)")
                matched_sched = date_mismatch_candidates.iloc[0]
            else:
                continue
            
            matched_sched_id = matched_sched[CanonicalField.SCHEDULED_CHARGES_ID.value]
            
            print(f"[TERTIARY]   ✓ MATCHED: AR trans {ar_id} → Sched ID {matched_sched_id}")
            
            # Update AR match - flag as TERTIARY (date mismatch)
            ar_result.loc[ar_idx, 'MATCHED'] = True
            ar_result.loc[ar_idx, 'MATCH_TYPE'] = 'TERTIARY_DATE_MISMATCH'
            ar_result.loc[ar_idx, 'MATCHED_SCHEDULED_ID'] = matched_sched_id
            
            # Update scheduled match in the result DataFrame
            sched_mask = scheduled_result[CanonicalField.SCHEDULED_CHARGES_ID.value] == matched_sched_id
            scheduled_result.loc[sched_mask, 'MATCHED'] = True
            scheduled_result.loc[sched_mask, 'MATCH_TYPE'] = 'TERTIARY_DATE_MISMATCH'
            
            # Also update in local candidates view for next iteration
            sched_candidates.loc[sched_candidates[CanonicalField.SCHEDULED_CHARGES_ID.value] == matched_sched_id, 'MATCHED'] = True
            
            # Append AR ID to dictionary map
            if matched_sched_id not in scheduled_to_ar_map:
                scheduled_to_ar_map[matched_sched_id] = []
            scheduled_to_ar_map[matched_sched_id].append(ar_row[CanonicalField.AR_TRANSACTION_ID.value])
            
            matched_count += 1
    
    logger.info(f"Tertiary matching: {matched_count} AR transactions matched to scheduled charges (date mismatches)")
    
    return ar_result, scheduled_result


def _identify_variances(
    scheduled_df: pd.DataFrame,
    ar_df: pd.DataFrame,
    recon_config: ReconciliationConfig,
    scheduled_to_ar_map: dict
) -> pd.DataFrame:
    """
    STEP 4: IDENTIFY VARIANCES - Classify and detail all variance types.
    
    Variance Types:
    - DATE_MISMATCH: Matched by lease/AR code/amount but POST_DATE outside expected period
    - MISSING_BILLINGS: Scheduled charge exists, no matching AR transaction
    - EXTRA_BILLINGS: AR transaction exists, no matching scheduled charge (distinguish event-driven)
    - AMOUNT_MISMATCH: Both exist, amounts differ beyond tolerance
    - TIMING_ISSUES: POST_DATE outside expected window (legacy, now handled by DATE_MISMATCH)
    - FREQUENCY_ISSUES: More/fewer billings than expected
    
    Returns:
        DataFrame with one row per variance, including details and severity
    """
    variances = []
    
    # DATE_MISMATCH: Matched via tertiary matching (same lease/AR code/amount but wrong date)
    date_mismatch_scheduled = scheduled_df[scheduled_df['MATCH_TYPE'] == 'TERTIARY_DATE_MISMATCH'].copy()
    for _, sched_row in date_mismatch_scheduled.iterrows():
        sched_id = sched_row[CanonicalField.SCHEDULED_CHARGES_ID.value]
        # Get the matched AR transactions from dictionary
        matched_ar_ids = scheduled_to_ar_map.get(sched_id, [])
        
        if matched_ar_ids:
            for ar_id in matched_ar_ids:
                ar_row = ar_df[ar_df[CanonicalField.AR_TRANSACTION_ID.value] == ar_id].iloc[0]
                
                # Check if this AR transaction is deleted or reversed
                is_deleted = ar_row.get(CanonicalField.IS_DELETED.value, 0) == 1
                is_reversal = ar_row.get(CanonicalField.IS_REVERSAL.value, 0) == 1
                
                # If deleted/reversed, flag as REVERSED_BILLING instead of DATE_MISMATCH
                if is_deleted or is_reversal:
                    variance_type = 'REVERSED_BILLING'
                    severity = 'INFO'
                    description = f"{'Deleted' if is_deleted else 'Reversed'} transaction: {sched_row.get(CanonicalField.AR_CODE_NAME.value)} - Originally billed but subsequently reversed/deleted"
                else:
                    # Determine if date is before, after, or just wrong
                    post_date = ar_row.get(CanonicalField.POST_DATE.value)
                    period_start = sched_row[CanonicalField.PERIOD_START.value]
                    period_end = sched_row.get(CanonicalField.PERIOD_END.value)
                    
                    # Helper function to safely format dates
                    def safe_date_format(date_val):
                        if pd.isna(date_val):
                            return "N/A"
                        try:
                            return pd.to_datetime(date_val).strftime('%Y-%m-%d')
                        except:
                            return str(date_val)
                    
                    if pd.notna(post_date) and pd.notna(period_start):
                        post_str = safe_date_format(post_date)
                        start_str = safe_date_format(period_start)
                        end_str = safe_date_format(period_end)
                        
                        if post_date < period_start:
                            timing_desc = f"billed EARLY ({post_str} before {start_str})"
                        elif pd.notna(period_end) and post_date > period_end:
                            timing_desc = f"billed LATE ({post_str} after {end_str})"
                        else:
                            timing_desc = f"date mismatch ({post_str} vs expected {start_str})"
                    else:
                        timing_desc = "date information incomplete"
                    
                    variance_type = 'DATE_MISMATCH'
                    severity = 'MEDIUM'
                    description = f"Date mismatch: {sched_row.get(CanonicalField.AR_CODE_NAME.value)} - {timing_desc}"
                
                variances.append({
                    'VARIANCE_TYPE': variance_type,
                    'SEVERITY': severity,
                    'SCHEDULED_CHARGE_ID': sched_row[CanonicalField.SCHEDULED_CHARGES_ID.value],
                    'AR_TRANSACTION_ID': ar_id,
                    'LEASE_INTERVAL_ID': sched_row[CanonicalField.LEASE_INTERVAL_ID.value],
                    'AR_CODE_ID': sched_row[CanonicalField.AR_CODE_ID.value],
                    'AR_CODE_NAME': sched_row.get(CanonicalField.AR_CODE_NAME.value),
                    'EXPECTED_AMOUNT': sched_row[CanonicalField.EXPECTED_AMOUNT.value],
                    'ACTUAL_AMOUNT': ar_row[CanonicalField.ACTUAL_AMOUNT.value],
                    'VARIANCE': 0.0 if is_deleted or is_reversal else ar_row[CanonicalField.ACTUAL_AMOUNT.value] - sched_row[CanonicalField.EXPECTED_AMOUNT.value],
                    'POST_DATE': ar_row.get(CanonicalField.POST_DATE.value),
                    'PERIOD_START': sched_row[CanonicalField.PERIOD_START.value],
                    'PERIOD_END': sched_row.get(CanonicalField.PERIOD_END.value),
                    'IS_DELETED': is_deleted,
                    'IS_REVERSAL': is_reversal,
                    'DESCRIPTION': description
                })
    
    # Import API codes for timed/external charge detection
    from .mappings import API_POSTED_AR_CODES
    
    # MISSING_BILLINGS: Unmatched scheduled charges
    missing_billings = scheduled_df[~scheduled_df['MATCHED']].copy()
    for _, row in missing_billings.iterrows():
        ar_code_id = row[CanonicalField.AR_CODE_ID.value]
        
        # Check if this is a timed/external charge (shouldn't be in scheduled)
        if ar_code_id in API_POSTED_AR_CODES:
            variances.append({
                'VARIANCE_TYPE': 'TIMED_OR_EXTERNAL_CHARGE',
                'SEVERITY': 'MEDIUM',
                'SCHEDULED_CHARGE_ID': row[CanonicalField.SCHEDULED_CHARGES_ID.value],
                'LEASE_INTERVAL_ID': row[CanonicalField.LEASE_INTERVAL_ID.value],
                'AR_CODE_ID': ar_code_id,
                'AR_CODE_NAME': row.get(CanonicalField.AR_CODE_NAME.value),
                'EXPECTED_AMOUNT': row[CanonicalField.EXPECTED_AMOUNT.value],
                'ACTUAL_AMOUNT': 0.0,
                'VARIANCE': -row[CanonicalField.EXPECTED_AMOUNT.value],
                'PERIOD_START': row[CanonicalField.PERIOD_START.value],
                'PERIOD_END': row[CanonicalField.PERIOD_END.value],
                'DESCRIPTION': f"Timed/External charge should not be scheduled: {row.get(CanonicalField.AR_CODE_NAME.value)} - ${row[CanonicalField.EXPECTED_AMOUNT.value]:.2f}"
            })
        else:
            variances.append({
                'VARIANCE_TYPE': 'MISSING_BILLINGS',
                'SEVERITY': 'HIGH',
                'SCHEDULED_CHARGE_ID': row[CanonicalField.SCHEDULED_CHARGES_ID.value],
                'LEASE_INTERVAL_ID': row[CanonicalField.LEASE_INTERVAL_ID.value],
                'AR_CODE_ID': ar_code_id,
                'AR_CODE_NAME': row.get(CanonicalField.AR_CODE_NAME.value),
                'EXPECTED_AMOUNT': row[CanonicalField.EXPECTED_AMOUNT.value],
                'ACTUAL_AMOUNT': 0.0,
                'VARIANCE': -row[CanonicalField.EXPECTED_AMOUNT.value],
                'PERIOD_START': row[CanonicalField.PERIOD_START.value],
                'PERIOD_END': row[CanonicalField.PERIOD_END.value],
                'DESCRIPTION': f"Scheduled charge not billed: {row.get(CanonicalField.AR_CODE_NAME.value)} - ${row[CanonicalField.EXPECTED_AMOUNT.value]:.2f}"
            })
    
    # EXTRA_BILLINGS: Unmatched AR transactions (distinguish timed/external, event-driven, and unexpected)
    # Event-driven AR codes (known list from analysis): PYMT, ADJST, LATEFEE, etc.
    event_driven_codes = ['PYMT', 'ADJST', 'LATEFEE', 'DEPOSIT', 'REFUND', 'WAIVER', 
                          'PENALTY', 'CREDIT', 'WRITEOFF', 'NSF', 'REVERSAL', 'TRANSFER',
                          'REIMBURSE', 'UTILITY', 'DAMAGE']  # Extend as needed
    
    extra_billings = ar_df[~ar_df['MATCHED']].copy()
    for _, row in extra_billings.iterrows():
        ar_code = row.get(CanonicalField.AR_CODE_NAME.value, '')
        ar_code_id = row[CanonicalField.AR_CODE_ID.value]
        
        # Skip timed/external charges (API codes) - these are EXPECTED to be billed without schedule
        # They appear in AR but not in scheduled charges by design (not a variance)
        if ar_code_id in API_POSTED_AR_CODES:
            continue  # Don't flag as any variance - this is normal behavior
        
        # Check if event-driven
        is_event_driven = any(code in str(ar_code).upper() for code in event_driven_codes)
        
        variances.append({
            'VARIANCE_TYPE': 'EXTRA_BILLINGS' if not is_event_driven else 'EVENT_DRIVEN',
            'SEVERITY': 'MEDIUM' if not is_event_driven else 'INFO',
            'SCHEDULED_CHARGE_ID': None,
            'LEASE_INTERVAL_ID': row[CanonicalField.LEASE_INTERVAL_ID.value],
            'AR_CODE_ID': ar_code_id,
            'AR_CODE_NAME': ar_code,
            'EXPECTED_AMOUNT': 0.0,
            'ACTUAL_AMOUNT': row[CanonicalField.ACTUAL_AMOUNT.value],
            'VARIANCE': row[CanonicalField.ACTUAL_AMOUNT.value],
            'POST_DATE': row.get(CanonicalField.POST_DATE.value),
            'AR_TRANSACTION_ID': row.get(CanonicalField.AR_TRANSACTION_ID.value),
            'DESCRIPTION': f"{'Event-driven' if is_event_driven else 'Unexpected'} AR transaction: {ar_code} - ${row[CanonicalField.ACTUAL_AMOUNT.value]:.2f}"
        })
    
    # AMOUNT_MISMATCH: Matched but amounts differ (check matched records)
    # TODO: Implement amount variance checking for matched pairs
    # This requires aggregating AR transactions by scheduled charge and comparing totals
    
    logger.info(f"Identified {len(variances)} variances: {len(missing_billings)} missing, {len(extra_billings)} extra")
    
    return pd.DataFrame(variances) if variances else pd.DataFrame()
