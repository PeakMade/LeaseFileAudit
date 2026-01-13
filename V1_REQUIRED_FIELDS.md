# V1 Required Canonical Fields Specification

## Data Flow and Required Fields

### 1. Scheduled Charges - Raw Input
**Before expansion to monthly buckets:**
- `PROPERTY_ID`
- `LEASE_INTERVAL_ID`
- `AR_CODE_ID`
- `CHARGE_AMOUNT` (source field)
- `DATE_CHARGE_START` (source field)
- `DATE_CHARGE_END` (source field)
- `SCHEDULED_CHARGES_ID`

**Mapping:** Raw source → Canonical using `SCHEDULED_CHARGES_MAPPING`
- `CHARGE_AMOUNT` → `EXPECTED_AMOUNT`
- `DATE_CHARGE_START` → `PERIOD_START`
- `DATE_CHARGE_END` → `PERIOD_END`

---

### 2. Scheduled Charges - After Expansion
**After month expansion (expected_detail):**

**Bucket Key Fields:**
- `PROPERTY_ID`
- `LEASE_INTERVAL_ID`
- `AR_CODE_ID`
- `AUDIT_MONTH` (derived during expansion)

**Amount Field:**
- `EXPECTED_AMOUNT`

**Provenance:**
- `SCHEDULED_CHARGES_ID`

**Total:** 6 canonical fields per expanded row

**Implementation:** `expand_scheduled_to_months()` explodes each scheduled charge row into one row per month in the date range.

---

### 3. AR Transactions - Raw Input
**Before normalization:**
- `PROPERTY_ID`
- `LEASE_INTERVAL_ID`
- `AR_CODE_ID`
- `POST_MONTH_DATE` (source field)
- `TRANSACTION_AMOUNT` (source field)
- `IS_POSTED` (filter field)
- `IS_DELETED` (filter field)
- `IS_REVERSAL` (include field)
- `ID` (source field)

**Mapping:** Raw source → Canonical using `AR_TRANSACTIONS_MAPPING`
- `TRANSACTION_AMOUNT` → `ACTUAL_AMOUNT`
- `POST_MONTH_DATE` → `POST_DATE` + derived `AUDIT_MONTH`
- `ID` → `AR_TRANSACTION_ID`

---

### 4. AR Transactions - After Normalization
**After normalization and filtering (actual_detail):**

**Bucket Key Fields:**
- `PROPERTY_ID`
- `LEASE_INTERVAL_ID`
- `AR_CODE_ID`
- `AUDIT_MONTH` (derived from POST_MONTH_DATE)

**Amount Field:**
- `ACTUAL_AMOUNT`

**Provenance:**
- `AR_TRANSACTION_ID`

**Filters Applied:**
- `IS_POSTED = 1`
- `IS_DELETED = 0`
- Reversals included (IS_REVERSAL may be 1)

**Total:** 6 canonical fields per normalized row

**Implementation:** `normalize_ar_transactions()` filters and transforms raw AR data.

---

## Reconciliation Grain (Bucket Key)

**The audit grain is defined as:**
```python
BUCKET_KEY_FIELDS = (
    PROPERTY_ID,
    LEASE_INTERVAL_ID,
    AR_CODE_ID,
    AUDIT_MONTH
)
```

Both `expected_detail` and `actual_detail` must have these 4 fields to enable bucket-level reconciliation.

---

## Implementation Status

✅ **canonical_fields.py**
- `REQUIRED_EXPECTED_DETAIL_FIELDS` = 6 fields (bucket keys + EXPECTED_AMOUNT + SCHEDULED_CHARGES_ID)
- `REQUIRED_ACTUAL_DETAIL_FIELDS` = 6 fields (bucket keys + ACTUAL_AMOUNT + AR_TRANSACTION_ID)
- `BUCKET_KEY_FIELDS` = 4 fields

✅ **mappings.py**
- `AR_TRANSACTIONS_MAPPING` - Raw AR → Canonical
  - Row filter: `IS_POSTED=1 AND IS_DELETED=0`
  - Derived field: `AUDIT_MONTH` from `POST_MONTH_DATE`
- `SCHEDULED_CHARGES_MAPPING` - Raw Scheduled → Canonical
  - No row filter
  - Derived fields: `PERIOD_START`, `PERIOD_END` from date columns

✅ **normalize.py**
- `normalize_ar_transactions()` - Uses AR mapping
- `normalize_scheduled_charges()` - Uses Scheduled mapping

✅ **expand.py**
- `expand_scheduled_to_months()` - Explodes scheduled charges into monthly rows
- Adds `AUDIT_MONTH` to each expanded row

✅ **reconcile.py**
- `reconcile_buckets()` - Aggregates by `BUCKET_KEY_FIELDS`
- Produces `EXPECTED_TOTAL`, `ACTUAL_TOTAL`, `VARIANCE`, `STATUS`, `MATCH_RULE`

---

## Field Count Summary

| Dataset | Stage | Required Canonical Fields | Count |
|---------|-------|---------------------------|-------|
| Scheduled | Raw | Source columns (not canonical yet) | 7 |
| Scheduled | Normalized | PROPERTY_ID, LEASE_INTERVAL_ID, AR_CODE_ID, EXPECTED_AMOUNT, PERIOD_START, PERIOD_END, SCHEDULED_CHARGES_ID | 7 |
| Scheduled | Expanded | Bucket keys + EXPECTED_AMOUNT + SCHEDULED_CHARGES_ID | 6 |
| AR | Raw | Source columns (not canonical yet) | 9 |
| AR | Normalized | Bucket keys + ACTUAL_AMOUNT + AR_TRANSACTION_ID | 6 |
| Bucket Results | Reconciled | Bucket keys + EXPECTED_TOTAL, ACTUAL_TOTAL, VARIANCE, STATUS, MATCH_RULE | 9 |

---

## Validation

The schema validation ensures these minimal fields are present:

```python
from audit_engine.schemas import validate_columns
from audit_engine.canonical_fields import (
    REQUIRED_EXPECTED_DETAIL_FIELDS,
    REQUIRED_ACTUAL_DETAIL_FIELDS
)

# Validate expected detail after expansion
validate_columns(expected_detail, REQUIRED_EXPECTED_DETAIL_FIELDS, "expected_detail")

# Validate actual detail after normalization
validate_columns(actual_detail, REQUIRED_ACTUAL_DETAIL_FIELDS, "actual_detail")
```

Any DataFrame missing these required canonical fields will raise a `ValueError` with a clear list of missing fields.
