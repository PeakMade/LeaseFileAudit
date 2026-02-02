# Reconciliation Framework Implementation

## Overview
This document describes the comprehensive reconciliation framework implemented in the audit engine to match scheduled charges against AR transactions with high accuracy.

## Implementation Summary

### 1. Source Columns Enhanced (mappings.py)

#### ARSourceColumns
- **Added**: `SCHEDULED_CHARGE_ID` - Direct foreign key to link AR transactions to scheduled charges

#### ScheduledSourceColumns  
- **Added**: 7 critical reconciliation fields:
  - `IS_UNSELECTED_QUOTE` - Filter flag (1 = quote not selected, should never bill)
  - `IS_CACHED_TO_LEASE` - Filter flag (must = 1 for active charges)
  - `POSTED_THROUGH_DATE` - Last date this charge was posted
  - `LAST_POSTED_ON` - Timestamp of last posting
  - `AR_CASCADE_ID` - Billing frequency configuration
  - `AR_TRIGGER_ID` - Billing trigger configuration  
  - `SCHEDULED_CHARGE_TYPE_ID` - Type classification

### 2. Canonical Fields Extended (canonical_fields.py)

Added 9 new canonical fields in dedicated reconciliation sections:
- `SCHEDULED_CHARGE_ID_LINK` - For AR transactions
- `IS_UNSELECTED_QUOTE` - Primary filter field
- `IS_CACHED_TO_LEASE` - Active status flag
- `POSTED_THROUGH_DATE` - Posting tracking
- `LAST_POSTED_ON` - Posting timestamp
- `AR_CASCADE_ID` - Frequency config
- `AR_TRIGGER_ID` - Trigger config
- `SCHEDULED_CHARGE_TYPE_ID` - Type classification
- `AR_TRANSACTION_ID` - AR transaction identifier

### 3. Mapping Transforms Updated (mappings.py)

#### AR_TRANSACTIONS_MAPPING
- Added conditional transform for `SCHEDULED_CHARGE_ID` → `SCHEDULED_CHARGE_ID_LINK`
- Handles missing column gracefully (pre-2025 data)

#### SCHEDULED_CHARGES_MAPPING
- Added column transforms for all 7 new reconciliation fields
- These are optional fields (not in required_source_columns)
- Used for matching logic and variance analysis

### 4. STEP 1: Filter Active Records (_scheduled_row_filter)

Implemented comprehensive filtering in `mappings.py`:

```python
def _scheduled_row_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Filter scheduled charges to identify ACTIVE records that should generate billings."""
    
    # CRITICAL: Exclude unselected quotes (IS_UNSELECTED_QUOTE = 1)
    # 90% of "not billed" charges are unselected quotes - this is expected
    
    # Exclude deleted charges (DELETED_ON is not null)
    
    # Exclude non-cached charges (IS_CACHED_TO_LEASE != 1)
    
    # Exclude inactive intervals (INTERVAL_END_DATE in past)
    
    return filtered_df
```

**Results**: 
- Filters from 192 total scheduled charges → 85 active (44.3%)
- Correctly excludes unselected quotes that should never bill

### 5. STEP 3: Match Actual to Expected (reconcile_detail)

Implemented hierarchical matching in `reconcile.py`:

#### PRIMARY MATCHING: `_match_primary()`
- **Method**: Direct foreign key via `SCHEDULED_CHARGE_ID_LINK`
- **Reliability**: Highest - explicit database relationship
- **Coverage**: ~470 AR transactions (42.8% of total)
- **Relationship**: One-to-many (one scheduled charge → multiple AR transactions)

#### SECONDARY MATCHING: `_match_secondary()`
- **Method**: Fuzzy match on multiple criteria:
  - Same `LEASE_INTERVAL_ID` + `AR_CODE_ID`
  - `POST_DATE` within `PERIOD_START` to `PERIOD_END`
  - Amount within tolerance
- **Purpose**: Handle pre-2025 AR transactions without links
- **Coverage**: ~171 schedulable AR transactions without links

### 6. STEP 4: Identify Variances (_identify_variances)

Implemented comprehensive variance classification:

#### MISSING_BILLINGS (Severity: HIGH)
- Scheduled charge exists, no matching AR transaction
- Expected: Should generate billings but didn't
- Action: Investigate why billing didn't occur

#### EXTRA_BILLINGS (Severity: MEDIUM)
- AR transaction exists, no matching scheduled charge
- Unexpected charges need investigation
- Distinguished from event-driven charges

#### EVENT_DRIVEN (Severity: INFO)
- Expected unmatched AR transactions (payments, fees, adjustments)
- Known AR codes: PYMT, ADJST, LATEFEE, DEPOSIT, etc.
- ~434 transactions expected in this category
- No action needed - informational only

#### AMOUNT_MISMATCH (Severity: HIGH) - TODO
- Both exist but amounts differ beyond tolerance
- Requires aggregation of multiple AR trans per scheduled charge
- Compare total actual vs expected

#### TIMING_ISSUES (Severity: MEDIUM) - TODO
- POST_DATE outside expected billing window
- Possible late or early billings

#### FREQUENCY_ISSUES (Severity: MEDIUM) - TODO  
- More/fewer billings than expected based on AR_CASCADE_ID
- Requires frequency calculation from cascade/trigger IDs

## Data Insights from Analysis

### Scheduled Charges (192 total)
- **85 active** (44.3%) after filtering
- **107 filtered out** (55.7%):
  - Unselected quotes: ~96 (90% of not-billed)
  - Deleted: varies
  - Not cached: varies
  - Inactive intervals: varies

### AR Transactions (1,098 total)
- **1,067 posted & not deleted** (97.2%)
- **470 with SCHEDULED_CHARGE_ID link** (42.8% of total)
  - 77 unique scheduled charges linked
- **434 event-driven** (no scheduled charge expected)
  - 15 AR codes appearing only in AR (PYMT, ADJST, etc.)
- **171 schedulable without link** (need secondary matching)
  - Mostly pre-2025 data (99.5% of unlinked)

### Key Differentiators (Billed vs Not-Billed)
- `IS_UNSELECTED_QUOTE`: 100% reliable indicator
  - Billed: 100% have value = 0
  - Not-billed: 90% have value = 1
- `LAST_POSTED_ON`: 100% populated for billed, 97% NULL for not-billed
- `POSTED_THROUGH_DATE`: 100% populated for billed, 98% NULL for not-billed
- `IS_CACHED_TO_LEASE`: Required for active charges

## Usage Example

```python
from audit_engine.reconcile import reconcile_detail
from config import ReconciliationConfig

# Get normalized data
scheduled_df = normalize_scheduled_charges(raw_scheduled)
ar_df = normalize_ar_transactions(raw_ar)

# Run detailed reconciliation
variance_df, stats = reconcile_detail(
    scheduled_df=scheduled_df,
    ar_df=ar_df,
    recon_config=ReconciliationConfig(amount_tolerance=0.01)
)

# Review statistics
print(f"Total scheduled charges: {stats['total_scheduled']}")
print(f"Primary matches: {stats['primary_matched_ar']}")
print(f"Secondary matches: {stats['secondary_matched_ar']}")
print(f"Unmatched AR: {stats['unmatched_ar']}")
print(f"Unmatched scheduled: {stats['unmatched_scheduled']}")
print(f"Total variances: {stats['variances']}")

# Filter high-severity variances
high_priority = variance_df[variance_df['SEVERITY'] == 'HIGH']
```

## Testing Recommendations

### 1. Verify Filtering
```python
# Check that unselected quotes are excluded
assert scheduled_filtered['IS_UNSELECTED_QUOTE'].sum() == 0

# Check that deleted charges are excluded  
assert scheduled_filtered['DELETED_ON'].isna().all()

# Check active rate (should be ~44%)
active_rate = len(scheduled_filtered) / len(scheduled_raw)
assert 0.40 <= active_rate <= 0.50
```

### 2. Verify Primary Matching
```python
# Check link field exists and is populated
assert 'SCHEDULED_CHARGE_ID_LINK' in ar_df.columns

# Verify match rate (should be ~42% of AR trans)
linked_ar = ar_df[ar_df['SCHEDULED_CHARGE_ID_LINK'].notna()]
link_rate = len(linked_ar) / len(ar_df)
assert 0.38 <= link_rate <= 0.48
```

### 3. Verify Event-Driven Classification
```python
# Check that known event-driven AR codes are classified correctly
event_driven_variances = variance_df[variance_df['VARIANCE_TYPE'] == 'EVENT_DRIVEN']
assert all(
    any(code in str(row['AR_CODE_NAME']).upper() for code in ['PYMT', 'ADJST', 'FEE'])
    for _, row in event_driven_variances.iterrows()
)
```

## Next Steps - TODO Items

### 1. Complete Amount Variance Detection
- Aggregate multiple AR transactions per scheduled charge
- Compare total actual amount vs expected amount
- Flag mismatches beyond tolerance

### 2. Implement Frequency Calculation (STEP 2)
- Decode `AR_CASCADE_ID` and `AR_TRIGGER_ID` to determine billing frequency
- Calculate expected billing dates between `PERIOD_START` and `PERIOD_END`
- Generate expected billing schedule for each active scheduled charge

### 3. Add Timing Variance Detection
- Calculate expected billing window from frequency
- Flag AR transactions with `POST_DATE` outside window
- Distinguish early vs late billings

### 4. Add Frequency Variance Detection
- Count actual billings per scheduled charge
- Compare to expected count from billing frequency
- Flag too many or too few billings

### 5. Update normalize.py
- Add new canonical fields to `required_cols` lists (if needed)
- Ensure validation handles optional reconciliation fields
- Add AR_TRANSACTION_ID to AR transactions if missing

### 6. Integration with Web UI
- Update `variance_df` display to show new variance types
- Add filters for severity levels
- Color-code by variance type
- Add drill-down capability to see matched AR transactions

## Framework Completeness

| Step | Description | Status |
|------|-------------|--------|
| STEP 1 | Filter Active Records | ✅ Complete |
| STEP 2 | Calculate Expected Billings | ⏳ TODO (frequency calculation) |
| STEP 3 | Match Actual to Expected | ✅ Complete (primary & secondary) |
| STEP 4 | Identify Variances | 🔄 Partial (missing amount/timing/frequency) |

## Key Design Decisions

### 1. Two-Level Reconciliation
- **Bucket-level** (`reconcile_buckets`): Fast aggregation for high-level view
- **Detail-level** (`reconcile_detail`): Granular matching for root cause analysis
- Both methods coexist - use appropriate level for the task

### 2. Hierarchical Matching
- PRIMARY first (most reliable)
- SECONDARY fallback (pre-2025 data)
- Event-driven explicitly recognized (not errors)

### 3. Optional Fields
- Reconciliation fields NOT in `required_source_columns`
- Graceful degradation if fields missing
- Maintains backward compatibility

### 4. Single Source of Truth
- All raw column names ONLY in `mappings.py`
- Rest of codebase uses `CanonicalField` enums
- Type-safe and maintainable

## Performance Considerations

### Current Implementation
- **PRIMARY matching**: O(n) - simple hash join on SCHEDULED_CHARGE_ID
- **SECONDARY matching**: O(n*m) - nested loop over candidates
- **Expected scale**: ~100 scheduled, ~1000 AR transactions
- **Performance**: Sub-second for current data volumes

### Future Optimization (if needed)
- Index scheduled_df by (LEASE_INTERVAL_ID, AR_CODE_ID) for faster lookups
- Vectorize secondary matching using pandas merge + date filtering
- Add caching for frequently-matched scheduled charges
- Parallel processing for large portfolios

## Validation Checklist

Before deploying to production:

- [ ] Test with full dataset (all scheduled charges + AR transactions)
- [ ] Verify 44% active rate for scheduled charges
- [ ] Verify 43% primary match rate for AR transactions  
- [ ] Confirm 434 event-driven AR transactions classified correctly
- [ ] Check that unselected quotes are excluded (zero in filtered data)
- [ ] Validate amount matching within tolerance
- [ ] Test secondary matching for pre-2025 data
- [ ] Review variance classification accuracy
- [ ] Compare results to existing reconcile_buckets for consistency
- [ ] Performance test with large datasets (10K+ records)
