# Canonical Fields and Schema Layer - Implementation Summary

## Overview
Created a comprehensive canonical field dictionary and schema validation system for the Lease Audit Engine. This architecture ensures the system can scale beyond AR + Scheduled to include Lease/Resident/Unit/Pet data without rewriting core logic.

## Files Created

### 1. `audit_engine/canonical_fields.py` (318 lines)
**Purpose**: Single source of truth for all field names used throughout the system.

**Key Components**:
- `CanonicalField` Enum - Type-safe field definitions with docstrings
- Field groups for common operations:
  - `BUCKET_KEY_FIELDS` - Reconciliation grain
  - `REQUIRED_EXPECTED_DETAIL_FIELDS` - Expected detail validation
  - `REQUIRED_ACTUAL_DETAIL_FIELDS` - Actual detail validation  
  - `REQUIRED_BUCKET_RESULTS_FIELDS` - Bucket results validation
  - `REQUIRED_FINDING_FIELDS` - Findings validation
  - `IDENTIFIER_FIELDS`, `AMOUNT_FIELDS`, `DATE_FIELDS` - Type groups
- Helper functions:
  - `get_field_names()` - Convert enum set to string tuple for pandas
  - `validate_field_group()` - Ensure required fields present

**Field Categories**:
- Common identifiers (PROPERTY_ID, LEASE_INTERVAL_ID, etc.)
- Charge coding (AR_CODE_ID, AR_CODE_NAME, etc.)
- Time dimensions (AUDIT_MONTH, PERIOD_START, POST_DATE, etc.)
- Amounts (EXPECTED_AMOUNT, ACTUAL_AMOUNT, VARIANCE, etc.)
- Source provenance (SOURCE_SYSTEM, SCHEDULED_CHARGES_ID, etc.)
- Status/metadata (STATUS, MATCH_RULE, SEVERITY, etc.)
- Future extensions (LEASE_START_DATE, RESIDENT_NAME, UNIT_TYPE, etc.)

### 2. `audit_engine/schemas.py` (285 lines)
**Purpose**: Validation utilities and canonical dataset container.

**Key Components**:
- `validate_columns()` - Verify DataFrame has required canonical fields
- `enforce_dtypes()` - Apply correct data types (datetime, float, Int64)
- `get_default_dtype_map()` - Default type mappings for canonical fields
- `CanonicalDataSet` dataclass - Type-safe container for all audit datasets
  - `expected_detail`, `actual_detail`, `bucket_results`, `findings`
  - `extras` dict for future sources (lease_terms, residents, etc.)
  - `validate()` method - Validate all datasets at once
  - `add_extra()` / `get_extra()` - Manage additional sources
  - `summary()` - Get record counts across all datasets

### 3. `audit_engine/mappings.py` (Updated, 369 lines)
**Purpose**: Source-to-canonical mappings. ONLY place raw source column names appear.

**Key Components**:
- **Raw Source Column Classes** (raw names isolated here):
  - `ARSourceColumns` - Raw AR transaction column names
  - `ScheduledSourceColumns` - Raw scheduled charges column names
  
- **Mapping Configuration Classes**:
  - `ColumnTransform` - Single column transformation definition
  - `SourceMapping` - Complete source configuration (columns, filters, derived fields)
  
- **V1 Mappings**:
  - `AR_TRANSACTIONS_MAPPING` - AR source → canonical
  - `SCHEDULED_CHARGES_MAPPING` - Scheduled source → canonical
  
- **Utilities**:
  - `apply_source_mapping()` - Transform raw DataFrame to canonical
  
- **Legacy Compatibility**:
  - Old `ARColumns`, `BucketColumns`, etc. classes maintained for backward compatibility
  - Will be deprecated once all modules migrate to `CanonicalField` enum

### 4. `audit_engine/CANONICAL_FIELDS_EXAMPLE.py` (Documentation)
Comprehensive examples showing:
- Basic usage of CanonicalField enum
- How normalize.py uses mappings
- Schema validation patterns
- CanonicalDataSet usage
- Adding new data sources (complete example)
- Field groups usage
- Type safety benefits
- Migration guide from old string literals

## Architecture Benefits

### 1. **Strong Typing**
- IDE autocomplete for all field names
- Compile-time typo prevention
- Easy refactoring (IDE finds all usages)

### 2. **Single Source of Truth**
- Raw source columns ONLY in mappings.py
- Canonical fields used everywhere else
- Clear separation of concerns

### 3. **Extensibility**
- Add new data source: create mapping, no core logic changes
- New canonical fields: add to enum, extend mappings
- Future-proof for Lease/Resident/Unit/Pet sources

### 4. **Validation**
- Schema validation at runtime
- Type enforcement (dates, amounts, IDs)
- Clear error messages for missing fields

### 5. **Discoverability**
- New developers explore CanonicalField enum
- Docstrings on every field
- Field groups show common patterns
- Example file demonstrates usage

## Usage Patterns

### Transform Raw Data to Canonical
```python
from audit_engine.mappings import apply_source_mapping, AR_TRANSACTIONS_MAPPING

df_canonical = apply_source_mapping(df_raw, AR_TRANSACTIONS_MAPPING)
# Result has only CanonicalField columns
```

### Access Canonical Fields
```python
from audit_engine.canonical_fields import CanonicalField

amount = df[CanonicalField.ACTUAL_AMOUNT.value]
property_id = df[CanonicalField.PROPERTY_ID.value]
```

### Validate Schema
```python
from audit_engine.schemas import validate_columns, enforce_dtypes
from audit_engine.canonical_fields import REQUIRED_EXPECTED_DETAIL_FIELDS

validate_columns(df, REQUIRED_EXPECTED_DETAIL_FIELDS, "expected_detail")
df = enforce_dtypes(df)
```

### Use Dataset Container
```python
from audit_engine.schemas import CanonicalDataSet

dataset = CanonicalDataSet(
    expected_detail=expected_df,
    actual_detail=actual_df,
    bucket_results=bucket_df
)
dataset.validate()
dataset.add_extra("lease_terms", lease_terms_df)
```

## Adding New Data Sources

1. **Define raw columns** in mappings.py:
   ```python
   class LeaseTermsSourceColumns:
       LEASE_ID = "LeaseID"
       PROPERTY_NUM = "PropertyNumber"
       ...
   ```

2. **Create mapping**:
   ```python
   LEASE_TERMS_MAPPING = SourceMapping(
       name="lease_terms",
       required_source_columns=[...],
       column_transforms=[...],
       derived_fields={...}
   )
   ```

3. **Normalize and add to dataset**:
   ```python
   lease_terms_canonical = apply_source_mapping(raw_df, LEASE_TERMS_MAPPING)
   dataset.add_extra("lease_terms", lease_terms_canonical)
   ```

4. **Create rules using new source**:
   ```python
   class LeaseStatusRule(Rule):
       def evaluate(self, context: RuleContext):
           lease_terms = context.get_source("lease_terms")
           # Use CanonicalField enum to access columns
   ```

## Migration Path

**Current State**: Old code uses string literals and ARColumns/BucketColumns classes.

**Backward Compatible**: Old classes still work, mapped to CanonicalField values.

**Future**: Gradually migrate modules to use CanonicalField enum directly.

**Priority Migration Order**:
1. normalize.py - Use apply_source_mapping()
2. expand.py - Use CanonicalField enum
3. reconcile.py - Use CanonicalField enum
4. rules.py - Use CanonicalField enum
5. findings.py - Use CanonicalField enum
6. metrics.py - Use CanonicalField enum
7. web/views.py - Use CanonicalField enum for display

## Testing Recommendations

1. **Unit tests** for each mapping (test transformations, filters, derived fields)
2. **Schema validation tests** (ensure required fields present, correct dtypes)
3. **End-to-end tests** (raw data → canonical → reconciliation → findings)
4. **Backward compatibility tests** (old code still works with new system)

## Summary

This canonical field layer provides:
✅ Type-safe field references
✅ Single source of truth
✅ Easy extensibility for new sources
✅ Schema validation
✅ Backward compatibility
✅ Clear documentation
✅ IDE support

The system is now ready to scale beyond AR + Scheduled to Lease/Resident/Unit/Pet data with minimal changes to core audit logic.
