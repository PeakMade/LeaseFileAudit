# Future Lease Audit - Phase 1 Implementation Guide

## Overview

The Future Lease Audit feature validates future-status leases by comparing signed lease contract amounts against scheduled recurring charge totals. This is distinct from the standard reconciliation workflow, which compares scheduled charges against ledger-posted AR transactions.

## Key Differences from Standard Reconciliation

| Aspect | Standard Reconciliation | Future Lease Audit |
|--------|------------------------|-------------------|
| **Source of Truth** | Posted AR transactions | Signed lease contract |
| **Comparison** | Scheduled vs. Posted AR | Scheduled vs. Contract Amount |
| **Lease Scope** | Current/active leases | Future-status leases only |
| **Ledger Dependency** | Requires posted transactions | No ledger dependency |
| **Exception Type** | MATCHED/SCHEDULED_NOT_BILLED/BILLED_NOT_SCHEDULED | Pass/Needs Review/True Discrepancy |
| **Future Months** | Flags SCHEDULED_NOT_BILLED for unbilled future months | Suppresses future month flags (expected behavior) |

## Configuration

### File: `future_lease_audit_config.json`

Located in the project root, this JSON file controls all aspects of the future lease audit:

```json
{
  "enabled": true,
  "audit_mode": "future_lease_contract_validation",
  "description": "Phase 1: Compare lease contract amounts against scheduled charge rollup totals",
  
  "lease_selection": {
    "require_lease_start_after_today": true,
    "exclude_test_leases": true,
    "exclude_model_leases": true,
    "exclude_corporate_leases": true
  },
  
  "charge_rollup_rules": {
    "included_usage_categories": ["Base", "Amenity", "Parking", "Pet", "Add Ons"],
    "excluded_usage_categories": ["Lease Violation", "Maintenance", "Special"],
    "recurring_only": true,
    "active_charges_only": true
  },
  
  "variance_thresholds": {
    "tolerance_amount": 0.01
  }
}
```

### Configuration Options

#### `enabled` (boolean)
- `true`: Future lease audit runs automatically during standard audit execution
- `false`: Feature is completely disabled

#### `lease_selection`
Controls which leases are included in the future lease audit:
- `require_lease_start_after_today`: Only audit leases starting after today's date
- `exclude_test_leases`: Exclude test/demo leases
- `exclude_model_leases`: Exclude model unit leases
- `exclude_corporate_leases`: Exclude corporate housing leases

#### `charge_rollup_rules`
Defines which AR codes contribute to the scheduled charge rollup total:
- `included_usage_categories`: Array of usage categories to include (from ar_code_name_usage_map.json)
- `excluded_usage_categories`: Array of usage categories to explicitly exclude
- `recurring_only`: Only include recurring charges (future enhancement)
- `active_charges_only`: Only include active scheduled charges (future enhancement)

#### `variance_thresholds`
- `tolerance_amount`: Dollar amount tolerance for variance matching (default: $0.01)
- Variances within tolerance are classified as "Pass"

## Workflow

### 1. Lease Identification
The system identifies future leases using:
- Lease start date > today (if `LEASE_START_DATE` available)
- Charge start date > today (fallback using `PERIOD_START`)

### 2. Charge Rollup Calculation
For each future lease:
1. Load AR code usage mapping from `ar_code_name_usage_map.json`
2. Filter scheduled charges to included usage categories
3. Sum filtered charges to get `SCHEDULED_CHARGE_ROLLUP_TOTAL`
4. Track:
   - Included charge codes (participated in rollup)
   - Excluded charge codes (filtered out by category)
   - Unmapped charge codes (no usage category defined)

### 3. Lease Contract Amount Lookup
For each future lease:
- Query SharePoint `LeaseTerms` list (or equivalent storage)
- Extract and sum:
  - BASE_RENT
  - amenity_rent
  - parking_rent
  - pet_rent
  - Other required recurring charges
- Result: `LEASE_CONTRACT_AMOUNT`

### 4. Variance Calculation
```
variance = SCHEDULED_CHARGE_ROLLUP_TOTAL - LEASE_CONTRACT_AMOUNT

Direction:
- variance > tolerance → "overcharge"
- variance < -tolerance → "undercharge"
- |variance| ≤ tolerance → "matched"
```

### 5. Status Classification

#### Pass
- Scheduled charge rollup matches lease contract within tolerance
- No action required

#### Needs Review
Triggered by:
- Missing lease contract amount (extraction incomplete/failed)
- Unmapped AR codes present in scheduled charges
- Unable to determine variance direction

**Required Action**: Complete data before classification is possible

#### True Discrepancy
- Variance exceeds tolerance threshold
- All required data is present
- Overcharge: Scheduled > Contract (revenue leakage to customer)
- Undercharge: Scheduled < Contract (potential lost revenue)

**Required Action**: Correct scheduled charges to match lease contract

#### Expected Exception (Future Enhancement)
- Variance exceeds tolerance BUT matches a configured business rule
- Example: Prorated first month, concessions, etc.

## Output Fields

Each audited future lease includes:

| Field | Description |
|-------|-------------|
| `PROPERTY_ID` | Property identifier |
| `PROPERTY_NAME` | Property name |
| `LEASE_INTERVAL_ID` | Lease interval identifier |
| `LEASE_CONTRACT_AMOUNT` | Total from signed lease document |
| `SCHEDULED_CHARGE_ROLLUP_TOTAL` | Sum of included scheduled charges |
| `VARIANCE` | Difference (scheduled - contract) |
| `VARIANCE_DIRECTION` | matched / overcharge / undercharge / unknown |
| `INCLUDED_CHARGE_CODES` | AR codes included in rollup |
| `EXCLUDED_CHARGE_CODES` | AR codes excluded by category |
| `UNMAPPED_CHARGE_CODES` | AR codes without usage mapping |
| `FUTURE_LEASE_AUDIT_STATUS` | Pass / Needs Review / True Discrepancy |
| `EXCEPTION_REASON` | Explanation for status |
| `RECOMMENDED_ACTION` | Suggested corrective action |

## KPIs

The future lease audit generates separate KPIs displayed in the portfolio dashboard:

- **Total Future Leases**: Count of leases audited
- **Pass Count**: Leases with variance within tolerance
- **Needs Review Count**: Leases with incomplete data
- **True Discrepancy Count**: Leases requiring correction
- **Total Potential Undercharge**: Sum of negative variances (lost revenue)
- **Total Potential Overcharge**: Sum of positive variances (at-risk revenue)
- **Match Rate**: (Pass + Expected Exception) / Total * 100

## Integration Points

### Audit Pipeline
Future lease audit runs as **Phase 8** in the standard audit execution:
1. Phase 1-7: Standard reconciliation (scheduled vs. AR transactions)
2. **Phase 8: Future Lease Audit** (scheduled vs. contract)
3. Results stored separately, not mixed with standard reconciliation

### UI Integration
- Portfolio dashboard shows future lease KPIs when available
- Future lease results stored in separate DataFrame
- Conditional display (only shown when future leases present)

### Storage Integration
Future lease audit results can be persisted to:
- SharePoint list: `FutureLeaseAuditResults` (future enhancement)
- Local JSON: `{run_id}/future_lease_audit_results.json`
- In-memory only (current Phase 1 implementation)

## Usage

### Enable/Disable Feature
Edit `future_lease_audit_config.json`:
```json
{
  "enabled": true   // Set to false to disable
}
```

### Customize Charge Rollup Rules
To change which AR codes are included:
```json
{
  "charge_rollup_rules": {
    "included_usage_categories": [
      "Base",
      "Amenity",
      "Parking",
      "Pet",
      "Add Ons",
      "Utilities"  // Added utilities
    ],
    "excluded_usage_categories": [
      "Lease Violation",
      "Maintenance",
      "Special",
      "Late Fees"  // Added late fees
    ]
  }
}
```

### Adjust Tolerance
```json
{
  "variance_thresholds": {
    "tolerance_amount": 5.00  // Allow $5 variance before flagging
  }
}
```

## Testing

Run the test suite:
```powershell
pytest tests/test_future_lease_audit.py -v
```

Test scenarios covered:
1. Perfect match (1500 = 1400 + 100)
2. Undercharge (-100)
3. Overcharge (+100)
4. Missing contract amount
5. Unmapped charge code
6. Excluded charge categories
7. Integration test (full workflow)

## Troubleshooting

### Issue: No future leases found
**Symptoms**: KPI section not displayed, log shows "No future leases found"

**Solutions**:
1. Check `require_lease_start_after_today` setting
2. Verify scheduled charges have `LEASE_START_DATE` or `PERIOD_START` > today
3. Review lease status filters

### Issue: All leases show "Needs Review"
**Symptoms**: 100% Needs Review status

**Solutions**:
1. Lease contract extraction not implemented yet (expected in Phase 1)
2. `get_lease_contract_amounts()` returning None (placeholder)
3. Implement LeaseTerms lookup in Phase 2

### Issue: Unexpected variance classifications
**Symptoms**: Charges you expect to match are flagged as discrepancies

**Solutions**:
1. Review `charge_rollup_rules` configuration
2. Check AR code usage categories in `ar_code_name_usage_map.json`
3. Verify tolerance threshold setting
4. Examine included/excluded charge code lists in output

### Issue: Feature not running
**Symptoms**: No Phase 8 logs, no KPIs displayed

**Solutions**:
1. Verify `enabled: true` in config
2. Check for import errors in logs
3. Ensure `audit_engine/future_lease_audit.py` exists
4. Review startup logs for configuration load errors

## Future Enhancements

### Phase 2 Roadmap
1. **Lease Term Extraction Integration**
   - Connect to SharePoint `LeaseTerms` list
   - Automatically extract contract amounts from lease PDFs
   - Handle multi-occupant lease splitting

2. **Expected Exception Rules**
   - Configurable business rules for known variances
   - Proration logic for partial months
   - Concession patterns
   - Move-in specials

3. **Persistent Storage**
   - Save results to SharePoint `FutureLeaseAuditResults` list
   - Enable historical trend analysis
   - Support re-auditing same leases

4. **Enhanced Reporting**
   - Drill-down to individual lease details
   - Export to Excel
   - Email notifications for discrepancies
   - Scheduled automatic audits

5. **Resident Status Filtering**
   - Filter by `RESIDENT_STATUS` field
   - Support "applicant", "approved", "future" statuses
   - Exclude denied/withdrawn applications

## API Reference

### `execute_future_lease_audit()`
Main entry point for future lease audit workflow.

**Parameters:**
- `scheduled_df` (pd.DataFrame): Normalized scheduled charges
- `run_id` (str): Audit run identifier
- `config` (dict): Future lease audit configuration
- `storage_service`: Storage service instance

**Returns:**
```python
{
    "future_lease_results": pd.DataFrame,  # Audit results per lease
    "kpis": dict,                          # Summary KPIs
    "metadata": dict                       # Run metadata
}
```

### `calculate_scheduled_charge_rollup()`
Calculate rollup totals for each lease.

**Parameters:**
- `scheduled_df` (pd.DataFrame): Scheduled charges for future leases
- `rollup_map` (dict): AR code inclusion rules
- `config` (dict): Configuration

**Returns:**
- pd.DataFrame with one row per lease containing rollup totals

## Support

For questions or issues:
1. Review this documentation
2. Check test scenarios in `tests/test_future_lease_audit.py`
3. Examine logs with `[FUTURE LEASE AUDIT]` prefix
4. Review `future_lease_audit_config.json` settings

## Version History

### v1.0.0 (Phase 1 - Initial Release)
- Future lease identification by lease start date
- Configurable charge rollup by usage category
- Variance calculation with tolerance threshold
- Status classification (Pass/Needs Review/True Discrepancy)
- KPI calculation and dashboard integration
- Comprehensive test coverage
- **Limitation**: Lease contract amounts not yet extracted (returns None)
