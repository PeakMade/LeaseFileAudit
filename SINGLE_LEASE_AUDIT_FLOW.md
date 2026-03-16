# Single Lease Audit - Complete Technical Flow

## Overview
When a user enters a Lease ID in the "Run Single Lease Audit from API" form and clicks submit, the system executes a complex pipeline to fetch, normalize, reconcile, and save audit results for that single lease.

---

## Phase 1: HTTP Request & Route Handler
**File**: `web/views.py` → `upload_api_lease()` route

### Step 1.1: Form Data Extraction
```python
# Extract user inputs from HTML form
lease_id = int(request.form.get('api_lease_id'))          # Required: 15293094
property_id = int(request.form.get('api_lease_property_id'))  # Optional: 1150907
transaction_from_date = request.form.get('api_lease_from_date')  # Optional: MM/DD/YYYY
```

**What's Happening**:
- User submits form from [templates/upload.html](templates/upload.html)
- Route validates lease_id is present
- Property ID is optional (will be discovered from API if not provided)
- Date filter is optional (defaults to all transactions)

### Step 1.2: Run ID Generation
```python
storage = get_storage_service()
run_id = storage.generate_run_id()  # e.g., "run_20260312_143022"
```

**What's Happening**:
- Creates unique run identifier with timestamp
- Used to track this specific audit execution
- Format: `run_YYYYMMDD_HHMMSS`

---

## Phase 2: API Data Fetch
**File**: `audit_engine/api_ingest.py` → `fetch_single_lease_api_sources()`

### Step 2.1: Property ID Discovery (if not provided)
```python
if property_id is None:
    # Make discovery call with just leaseIds to find property
    discovery_params = {
        "leaseIds": "15293094",
        "includeAddOns": "0",
        "includeCharge": "0",  # Minimal data for discovery
    }
    discovery_payload = _post_method(
        endpoint_url=LEASE_API_URL,
        method_name="getLeaseDetails",
        params=discovery_params
    )
    # Extract property_id from response
    property_id = discovery_payload['response']['lease'][0]['propertyId']
    # Logs: "[SINGLE LEASE API] Discovered property_id=1150907 for lease_id=15293094"
```

**What's Happening**:
- If user didn't provide property ID, system discovers it
- Makes lightweight API call with minimal data (no charges)
- Extracts property_id from lease details response
- This ensures we always have both IDs for the main calls

### Step 2.2: Fetch Lease Details (Scheduled Charges)
```python
# Call getLeaseDetails with BOTH propertyId and leaseIds
lease_details_params = {
    "propertyId": 1150907,      # Required by Entrata API
    "leaseIds": "15293094",     # Filters to specific lease
    "includeAddOns": "0",
    "includeCharge": "1",        # Include scheduled charges
    "leaseStatusTypeIds": "3,4"  # Active + Notice leases
}

lease_details_payload = _post_method(
    endpoint_url="https://apis.entrata.com/ext/orgs/peakmade/v1/leases",
    api_key=LEASE_API_KEY,
    method_name="getLeaseDetails",
    version="r2",
    params=lease_details_params
)
```

**What's Happening**:
- Sends API request to Entrata for lease details
- **CRITICAL**: Sends BOTH propertyId AND leaseIds (Entrata requires both)
- Returns lease metadata + scheduled charges setup
- Response includes:
  - Lease interval info (start/end dates, status)
  - Tenant/guarantor info
  - Scheduled charges (base rent, fees, recurring items)
  - Lease addons/charges configuration

**API Response Structure**:
```json
{
  "response": {
    "lease": [{
      "propertyId": 1150907,
      "propertyName": "Bibby Hall",
      "leaseId": 15293094,
      "leaseIntervalId": 123456,
      "firstName": "John",
      "lastName": "Doe",
      "leaseStartDate": "08/15/2024",
      "leaseEndDate": "05/01/2025",
      "charges": [{
        "scheduledChargeId": "SC789",
        "arCodeId": "154771",
        "chargeStartDate": "08/15/2024",
        "chargeEndDate": "05/01/2025",
        "chargeAmount": "850.00",
        "monthlyAmount": "850.00",
        "frequency": "MONTHLY"
      }, ...]
    }]
  }
}
```

### Step 2.3: Build Scheduled Charges DataFrame
```python
scheduled_df, lease_ids = _build_scheduled_df(property_id, property_name, lease_details_payload)
```

**What's Happening**:
- Parses lease_details_payload JSON response
- Extracts each scheduled charge into a row
- **Raw column names** (before mapping):
  - `PROPERTY_ID`, `PROPERTY_NAME`
  - `LEASE_ID`, `LEASE_INTERVAL_ID`
  - `RESIDENT_NAME_FIRST`, `RESIDENT_NAME_LAST`, `RESIDENT_NAME_FULL`
  - `SCHEDULED_CHARGE_ID` (from Entrata)
  - `AR_CODE_ID`, `AR_CODE_NAME`
  - `CHARGE_START_DATE`, `CHARGE_END_DATE`
  - `CHARGE_AMOUNT`, `CHARGE_FREQUENCY`
  - Many more fields...
- Returns DataFrame with all scheduled charges for this lease

**Example Row**:
```
PROPERTY_ID: 1150907
LEASE_ID: 15293094
SCHEDULED_CHARGE_ID: SC789
AR_CODE_ID: 154771
CHARGE_AMOUNT: 850.00
CHARGE_FREQUENCY: MONTHLY
```

### Step 2.4: Defensive Filtering (Scheduled)
```python
if 'LEASE_ID' in scheduled_df.columns:
    before = len(scheduled_df)
    scheduled_df = scheduled_df[scheduled_df['LEASE_ID'] == '15293094'].copy()
    after = len(scheduled_df)
    if before != after:
        # Logs: "[SINGLE LEASE API] Filtered scheduled charges: 98 → 12 rows"
```

**What's Happening**:
- Safety check: ensures ONLY requested lease data remains
- If API returned extra leases (despite leaseIds filter), removes them
- Only logs if filtering actually occurred (before ≠ after)

### Step 2.5: Fetch AR Transactions
```python
# Call getLeaseArTransactions with BOTH propertyId and leaseIds
ar_params = {
    "propertyId": 1150907,
    "leaseIds": "15293094",
    "leaseStatusTypeIds": "3,4",
    "showFullLedger": "1",
    "transactionFromDate": "01/01/2024",  # If provided by user
    "transactionToDate": "12/31/2024"     # If provided by user
}

ar_payload = _post_method(
    endpoint_url="https://apis.entrata.com/ext/orgs/peakmade/v1/artransactions",
    method_name="getLeaseArTransactions",
    version="r1",
    params=ar_params
)
```

**What's Happening**:
- Fetches all posted AR transactions for this lease
- **CRITICAL**: Sends BOTH propertyId AND leaseIds
- Includes all transaction types (charges, payments, credits, adjustments)
- Date filter applied if user specified

**API Response Structure**:
```json
{
  "response": {
    "transactions": [{
      "leaseId": 15293094,
      "arCodeId": "154771",
      "postDate": "09/01/2024",
      "transactionAmount": "850.00",
      "transactionType": "Charge",
      "notes": "September Rent"
    }, ...]
  }
}
```

### Step 2.6: Build AR Transactions DataFrame
```python
ar_df = _build_ar_df(property_id, property_name, ar_payload)
```

**What's Happening**:
- Parses ar_payload JSON response
- Extracts each transaction into a row
- **Raw column names** (before mapping):
  - `PROPERTY_ID`, `PROPERTY_NAME`
  - `LEASE_ID`, `LEASE_INTERVAL_ID`
  - `RESIDENT_NAME_FIRST`, `RESIDENT_NAME_LAST`
  - `AR_CODE_ID`, `AR_CODE_NAME`
  - `POST_DATE`, `POST_MONTH_DATE`
  - `AMOUNT`
  - `TRANSACTION_TYPE`, `NOTES`
  - `SCHEDULED_CHARGE_ID_LINK` (links to scheduled charge)

**Example Row**:
```
PROPERTY_ID: 1150907
LEASE_ID: 15293094
AR_CODE_ID: 154771
POST_DATE: 2024-09-01
AMOUNT: 850.00
SCHEDULED_CHARGE_ID_LINK: SC789
```

### Step 2.7: Defensive Filtering (AR Transactions)
```python
if 'LEASE_ID' in ar_df.columns:
    before = len(ar_df)
    ar_df = ar_df[ar_df['LEASE_ID'] == '15293094'].copy()
    after = len(ar_df)
    if before != after:
        # Logs: "[SINGLE LEASE API] Filtered AR transactions: 450 → 45 rows"
```

**What's Happening**:
- Safety check for AR transactions
- Ensures only requested lease data remains
- Only logs if filtering occurred

### Step 2.8: Return API Sources
```python
return {
    'property_name': "Bibby Hall",
    'property_id': 1150907,
    'scheduled_raw': scheduled_df,  # RAW format (12 rows)
    'ar_raw': ar_df,                # RAW format (45 rows)
    'lease_count': 1
}
```

**What's Happening**:
- Returns dictionary with all fetched data
- Data is still in **RAW format** (Entrata column names)
- Ready for normalization pipeline

---

## Phase 3: Audit Execution Pipeline
**File**: `web/views.py` → `execute_audit_run()`

### Step 3.1: Pass Preloaded Sources
```python
results = execute_audit_run(
    file_path=None,  # No Excel file (using API data)
    run_id="run_20260312_143022",
    audit_year=None,  # Single lease audits don't filter by year
    audit_month=None,
    scoped_property_ids=None,
    preloaded_sources={
        'AR Transactions': ar_raw,      # From API
        'Scheduled Charges': scheduled_raw  # From API
    }
)
```

**What's Happening**:
- Instead of loading Excel file, passes API data directly
- `preloaded_sources` bypasses file I/O
- Data still in RAW format at this point

### Step 3.2: Source Mapping (RAW → CANONICAL)
**File**: `audit_engine/mappings.py` → `apply_source_mapping()`

```python
# Convert RAW column names to CANONICAL field names
ar_canonical = apply_source_mapping(ar_raw, AR_TRANSACTIONS_MAPPING)
scheduled_canonical = apply_source_mapping(scheduled_raw, SCHEDULED_CHARGES_MAPPING)
```

**What's Happening**:
- **CRITICAL TRANSFORMATION**: Converts Entrata-specific names to internal standard
- Uses mapping rules defined in `audit_engine/mappings.py`
- **Only** file that knows about raw source column names

**Example Mapping**:
```python
# RAW → CANONICAL transformations:
"PROPERTY_ID" → CanonicalField.PROPERTY_ID
"LEASE_ID" → CanonicalField.LEASE_INTERVAL_ID
"AR_CODE_ID" → CanonicalField.AR_CODE_ID
"POST_DATE" → CanonicalField.AUDIT_MONTH (normalized to first of month)
"AMOUNT" → CanonicalField.AMOUNT
"SCHEDULED_CHARGE_ID_LINK" → CanonicalField.SCHEDULED_CHARGE_ID_LINK
```

**Before (RAW)**:
```
PROPERTY_ID | LEASE_ID | AR_CODE_ID | POST_DATE  | AMOUNT
1150907     | 15293094 | 154771     | 2024-09-01 | 850.00
```

**After (CANONICAL)**:
```
PROPERTY_ID | LEASE_INTERVAL_ID | AR_CODE_ID | AUDIT_MONTH | AMOUNT
1150907     | 15293094          | 154771     | 2024-09-01  | 850.00
```

### Step 3.3: Normalization
**File**: `audit_engine/normalize.py`

```python
# Validate and normalize AR transactions
actual_detail = normalize_ar_transactions(ar_canonical)

# Validate and normalize scheduled charges
scheduled_normalized = normalize_scheduled_charges(scheduled_canonical)
```

**What's Happening**:

**For AR Transactions**:
- Validates required fields exist
- Converts data types (dates to datetime, amounts to float)
- Normalizes AUDIT_MONTH to first day of month
- Filters out invalid/deleted rows
- Excludes API-posted AR codes (155030, 155037, etc.)
- Logs: `[AR FILTER DEBUG] Total AR transactions: 45`

**For Scheduled Charges**:
- Validates required fields
- Converts data types
- Filters out deleted/cancelled charges
- Validates date ranges
- Logs: `[SCHEDULED FILTER DEBUG] Total scheduled charges: 12`

**Example Normalized Row (AR)**:
```
PROPERTY_ID: 1150907 (int)
LEASE_INTERVAL_ID: 15293094 (int)
AR_CODE_ID: "154771" (str)
AUDIT_MONTH: datetime(2024, 9, 1)
AMOUNT: 850.00 (float)
SCHEDULED_CHARGE_ID_LINK: "SC789" (str)
```

### Step 3.4: Expansion (Scheduled → Monthly Buckets)
**File**: `audit_engine/expand.py` → `expand_scheduled_to_months()`

```python
expected_detail = expand_scheduled_to_months(scheduled_normalized)
```

**What's Happening**:
- Takes ONE scheduled charge row and expands it into MULTIPLE monthly rows
- **Example**: 
  - Input: Base rent $850/month from Aug 2024 to May 2025 (1 row)
  - Output: 10 separate rows (Aug, Sep, Oct, Nov, Dec, Jan, Feb, Mar, Apr, May)

**Before Expansion (scheduled_normalized)**:
```
1 row:
LEASE_INTERVAL_ID: 15293094
AR_CODE_ID: 154771
CHARGE_START_DATE: 2024-08-15
CHARGE_END_DATE: 2025-05-01
MONTHLY_AMOUNT: 850.00
```

**After Expansion (expected_detail)**:
```
10 rows (one per month):
Row 1: AUDIT_MONTH: 2024-08-01, EXPECTED_AMOUNT: 850.00
Row 2: AUDIT_MONTH: 2024-09-01, EXPECTED_AMOUNT: 850.00
Row 3: AUDIT_MONTH: 2024-10-01, EXPECTED_AMOUNT: 850.00
...
Row 10: AUDIT_MONTH: 2025-05-01, EXPECTED_AMOUNT: 850.00
```

**Why**: Reconciliation compares expected vs actual **by month**, so we need monthly buckets

### Step 3.5: Audit Period Filtering
```python
# Default: Current Academic Year (Aug through current month)
expected_detail = filter_to_current_academic_year(expected_detail)
actual_detail = filter_to_current_academic_year(actual_detail)
```

**What's Happening**:
- Filters data to audit window
- Default: August of current academic year through current month
- Example: If today is March 2026, includes Aug 2025 - Mar 2026
- Logs before/after row counts

**Before Filter**:
```
Expected: 150 rows (all months from lease start to end)
Actual: 200 rows (all transactions ever posted)
```

**After Filter (Academic Year)**:
```
Expected: 80 rows (Aug 2025 - Mar 2026 only)
Actual: 95 rows (Aug 2025 - Mar 2026 only)
```

### Step 3.6: Property-Scoped Reconciliation
```python
# Group data by property
expected_by_property = {1150907: expected_detail}
actual_by_property = {1150907: actual_detail}

# Reconcile THIS property only
variance_detail, recon_stats = reconcile_detail(
    scheduled_normalized,  # For this property
    actual_detail,         # For this property
    config.reconciliation
)

bucket_results, findings = reconcile_buckets(
    expected_detail,  # For this property
    actual_detail,    # For this property  
    config.reconciliation
)
```

**What's Happening**:
- System reconciles property-by-property (even though we only have one)
- Keeps property isolation for consistency with multi-property audits

---

## Phase 4: Reconciliation Engine
**File**: `audit_engine/reconcile.py`

### Step 4.1: Bucket Creation
```python
# Create unique bucket key for each combination
BUCKET_KEY = (PROPERTY_ID, LEASE_INTERVAL_ID, AR_CODE_ID, AUDIT_MONTH)

# Example buckets:
Bucket 1: (1150907, 15293094, "154771", 2024-09-01)  # September base rent
Bucket 2: (1150907, 15293094, "154771", 2024-10-01)  # October base rent
Bucket 3: (1150907, 15293094, "155001", 2024-09-01)  # September pet fee
```

**What's Happening**:
- Groups transactions into "buckets"
- Each bucket = one property + one lease + one AR code + one month
- Buckets allow O(n) matching performance via hash tables

### Step 4.2: Tier 1 PRIMARY Matching
```python
# Group expected by bucket
expected_grouped = expected_detail.groupby(BUCKET_KEY).agg({
    'EXPECTED_AMOUNT': 'sum'  # Total expected for this bucket
})

# Group actual by bucket
actual_grouped = actual_detail.groupby(BUCKET_KEY).agg({
    'ACTUAL_AMOUNT': 'sum'  # Total posted for this bucket
})

# Join on bucket key (hash join - very fast!)
matched = expected_grouped.merge(actual_grouped, on=BUCKET_KEY, how='outer')
```

**What's Happening**:
- Pandas performs **hash join** (O(n) performance)
- Each bucket compared: expected total vs actual total
- Creates bucket_results rows

**Example Bucket Results**:
```
Bucket: (1150907, 15293094, "154771", 2024-09-01)
Expected Total: $850.00
Actual Total: $850.00
Variance: $0.00
Status: "MATCHED" ✅

Bucket: (1150907, 15293094, "154771", 2024-10-01)
Expected Total: $850.00
Actual Total: $0.00
Variance: -$850.00
Status: "SCHEDULED_NOT_BILLED" ❌ (undercharge)

Bucket: (1150907, 15293094, "155001", 2024-09-01)
Expected Total: $0.00
Actual Total: $25.00
Variance: +$25.00
Status: "BILLED_NOT_SCHEDULED" ❌ (overcharge)
```

### Step 4.3: Tier 2 SECONDARY Matching
```python
# Match transactions with same amount but different bucket keys
# Helps identify charges posted to wrong AR code or wrong lease
```

**What's Happening**:
- Identifies transactions that "wandered" to wrong bucket
- Example: Pet fee posted as misc fee (different AR code)
- Still flagged as variance, but provides explanation

### Step 4.4: Tier 3 TERTIARY Matching
```python
# Match same property/lease/AR code but different month
# Helps identify timing issues vs true missing charges
```

**What's Happening**:
- Identifies charges posted in wrong month
- Example: September rent posted in October
- Provides context for date mismatches

### Step 4.5: Findings Generation
```python
# Create findings for rule violations
findings = []

# Example finding:
{
    'finding_id': 'F001',
    'category': 'UNDERCHARGE',
    'severity': 'HIGH',
    'title': 'Scheduled charge not posted',
    'description': 'Base Rent scheduled for October 2024 was not posted',
    'expected_value': '$850.00',
    'actual_value': '$0.00',
    'variance': -850.00,
    'impact_amount': 850.00
}
```

**What's Happening**:
- Analyzes bucket results for specific issues
- Creates human-readable findings
- Categorizes by severity (HIGH/MEDIUM/LOW)
- Provides actionable descriptions

---

## Phase 5: Results Aggregation
**File**: `web/views.py` → `execute_audit_run()`

### Step 5.1: Aggregate Property Results
```python
# Combine results from all properties (in our case, just one)
final_bucket_results = pd.concat(bucket_parts)
final_findings = pd.concat(finding_parts)
final_expected_detail = pd.concat(expected_parts)
final_actual_detail = pd.concat(actual_parts)
final_variance_detail = pd.concat(variance_parts)
```

**What's Happening**:
- System designed for multiple properties
- For single lease audit, just one property in the concat
- Creates portfolio-level aggregates

### Step 5.2: Return Results Dictionary
```python
return {
    'bucket_results': final_bucket_results,    # Reconciliation summary
    'findings': final_findings,                # Audit findings
    'expected_detail': final_expected_detail,  # Expanded scheduled charges
    'actual_detail': final_actual_detail,      # Posted AR transactions
    'variance_detail': final_variance_detail,  # Row-level variances
    'property_name_map': {1150907: "Bibby Hall"}
}
```

**Final Data Volumes (Example)**:
```
bucket_results: 120 rows (10 months × 12 AR codes per month)
findings: 15 rows (variances found)
expected_detail: 120 rows (expanded scheduled charges)
actual_detail: 95 rows (posted transactions)
variance_detail: 25 rows (detailed discrepancies)
```

---

## Phase 6: Metadata Creation
**File**: `web/views.py` → `upload_api_lease()`

### Step 6.1: Build Run Metadata
```python
metadata = {
    'run_id': 'run_20260312_143022',
    'timestamp': '2026-03-12T14:30:22.123456',
    'config_version': 'v1',
    'file_name': 'api_lease_15293094.json',
    'run_scope': {
        'type': 'lease',  # CRITICAL: Marks this as single lease audit
        'source': 'api_lease',
        'lease_id': 15293094,
        'property_id': 1150907,
        'property_name': 'Bibby Hall'
    },
    'property_name_map': {
        1150907: 'Bibby Hall'
    },
    'audit_period': 'Current Academic Year (Aug 2025 - Mar 2026)',
    'row_counts': {
        'bucket_results': 120,
        'findings': 15,
        'expected_detail': 120,
        'actual_detail': 95
    }
}
```

**What's Happening**:
- Creates comprehensive metadata about this audit run
- `run_scope.type='lease'` marks this as single lease audit
- Includes property/lease identifiers
- Records data volumes for validation

---

## Phase 7: Save to SharePoint
**File**: `storage/service.py`

### Step 7.1: Save Run Metadata (JSON)
```python
# Save to SharePoint document library
storage.save_run(
    run_id='run_20260312_143022',
    bucket_results=bucket_results,
    findings=findings,
    expected_detail=expected_detail,
    actual_detail=actual_detail,
    variance_detail=variance_detail,
    metadata=metadata
)
```

**What's Happening**:
- Creates folder: `LeaseFileAudit Runs/run_20260312_143022/`
- Saves `run_meta.json` with metadata

**File: run_meta.json**:
```json
{
  "run_id": "run_20260312_143022",
  "timestamp": "2026-03-12T14:30:22.123456",
  "run_scope": {
    "type": "lease",
    "lease_id": 15293094,
    "property_id": 1150907
  },
  "row_counts": {...}
}
```

### Step 7.2: Save CSV Files (Document Library)
```python
# Save CSVs to SharePoint folder
bucket_results.to_csv('bucket_results.csv')
findings.to_csv('findings.csv')
expected_detail.to_csv('expected_detail.csv')
actual_detail.to_csv('actual_detail.csv')
variance_detail.to_csv('variance_detail.csv')
```

**What's Happening**:
- Converts DataFrames to CSV format
- Uploads to SharePoint document library
- **Purpose**: Fallback compatibility for CSV-based views

**Files Created**:
```
LeaseFileAudit Runs/
  run_20260312_143022/
    run_meta.json
    bucket_results.csv        ← Reconciliation summary
    findings.csv              ← Audit findings
    expected_detail.csv       ← Expanded scheduled
    actual_detail.csv         ← Posted transactions
    variance_detail.csv       ← Row-level variances
```

### Step 7.3: Save to AuditRuns SharePoint List
```python
# Write bucket_results to list (synchronous by default)
for index, row in bucket_results.iterrows():
    list_item = {
        'Title': f'bucket_result:{index}',
        'CompositeKey': f'{property_id}:{lease_id}:{ar_code_id}:{audit_month}',
        'RunId': 'run_20260312_143022',
        'ResultType': 'bucket_result',
        'PropertyId': 1150907,
        'LeaseIntervalId': 15293094,
        'ArCodeId': '154771',
        'AuditMonth': '2024-09-01',
        'Status': 'MATCHED',
        'Variance': 0.00,
        'ExpectedTotal': 850.00,
        'ActualTotal': 850.00,
        'MatchRule': 'PRIMARY',
        'CreatedAt': datetime.now()
    }
    
    # Post to SharePoint AuditRuns list
    graph_api.post_list_item(list_item)
```

**What's Happening**:
- Persists bucket_results to SharePoint list
- **Each row becomes a list item**
- Uses Microsoft Graph $batch API (10 items per batch)
- **Synchronous** by default (waits for completion)
- Retry logic: exponential backoff for 429/503/504 errors

**AuditRuns List Rows Created**:
```
Title                   | RunId              | ResultType     | PropertyId | LeaseIntervalId | ...
bucket_result:0         | run_20260312_...   | bucket_result  | 1150907    | 15293094        | ...
bucket_result:1         | run_20260312_...   | bucket_result  | 1150907    | 15293094        | ...
finding:0               | run_20260312_...   | finding        | 1150907    | 15293094        | ...
```

**Why List Storage**:
- Faster queries (indexed columns)
- No CSV parsing overhead
- Supports filtering by RunId, PropertyId, LeaseIntervalId
- Primary data source for UI (CSV is fallback)

### Step 7.4: Save to RunDisplaySnapshots List
```python
# Create property-level snapshot
property_snapshot = {
    'Title': f'property:{property_id}',
    'SnapshotKey': f'{run_id}:property:{property_id}',
    'RunId': 'run_20260312_143022',
    'ScopeType': 'property',
    'PropertyId': 1150907,
    'ExceptionCountStatic': 15,  # Unresolved exceptions
    'UnderchargeStatic': 850.00,
    'OverchargeStatic': 25.00,
    'MatchRateStatic': 87.5,
    'TotalBucketsStatic': 120,
    'MatchedBucketsStatic': 105,
    'CreatedAt': datetime.now()
}

# Post to RunDisplaySnapshots list
graph_api.post_list_item(property_snapshot)

# Also create lease-level snapshot
lease_snapshot = {
    'ScopeType': 'lease',
    'PropertyId': 1150907,
    'LeaseIntervalId': 15293094,
    # ... same metrics scoped to lease
}
```

**What's Happening**:
- Creates precomputed summary snapshots
- **Property-level** snapshot for property view
- **Lease-level** snapshot for lease view
- Snapshots are **static** (don't recalculate on resolution changes)
- **Performance**: UI loads snapshots instead of recalculating from details

**RunDisplaySnapshots List Rows**:
```
SnapshotKey                      | ScopeType | PropertyId | LeaseIntervalId | ExceptionCountStatic | ...
run_20260312_...:property:1150907| property  | 1150907    | null            | 15                   | ...
run_20260312_...:lease:15293094  | lease     | 1150907    | 15293094        | 15                   | ...
```

### Step 7.5: Activity Logging
```python
# Log successful audit to Innovation Use Log
log_user_activity(
    user_info=current_user,
    activity_type='Successful Audit',
    details={
        'run_id': 'run_20260312_143022',
        'lease_id': 15293094,
        'property_id': 1150907,
        'source': 'api_lease',
        'total_buckets': 120,
        'exceptions_found': 15
    }
)
```

**What's Happening**:
- Records user activity to SharePoint
- Tracks successful audits
- Includes run metadata
- Used for reporting/analytics

---

## Phase 8: HTTP Response & Redirect
**File**: `web/views.py` → `upload_api_lease()`

### Step 8.1: Flash Success Message
```python
flash(
    f'Single lease audit completed for Lease {lease_id}. '
    f'Found {total_buckets} buckets with {exception_count} exceptions.',
    'success'
)
```

**What's Happening**:
- Creates success notification for user
- Shows summary statistics
- Displayed as green banner in UI

### Step 8.2: Redirect to Property View
```python
return redirect(url_for(
    'main.property_view',
    run_id='run_20260312_143022',
    property_id=1150907
))
```

**What's Happening**:
- Redirects browser to property view page
- URL: `/property/run_20260312_143022/1150907`
- Shows audit results for this property
- User can drill down to lease details from there

---

## Phase 9: UI Display (Property View)
**File**: `web/views.py` → `property_view()` + `templates/property.html`

### Step 9.1: Load Property Data
```python
# Load bucket results from SharePoint AuditRuns list
bucket_results = storage.load_audit_results_from_list(
    run_id='run_20260312_143022',
    result_type='bucket_result',
    filter_property_id=1150907
)

# Load snapshot for fast KPI display
snapshot = storage.load_property_snapshot(
    run_id='run_20260312_143022',
    property_id=1150907
)
```

**What's Happening**:
- Queries AuditRuns list with filters
- Loads precomputed snapshot
- Much faster than recalculating from CSVs

### Step 9.2: Display Property Summary
```html
<!-- Property Header -->
<h1>Bibby Hall - Audit Results</h1>
<div class="kpi-cards">
  <div class="card">
    <h3>Total Leases</h3>
    <p>1</p>  <!-- Single lease audit -->
  </div>
  <div class="card">
    <h3>Open Exceptions</h3>
    <p>15</p>
  </div>
  <div class="card">
    <h3>Undercharge</h3>
    <p class="text-danger">$850.00</p>
  </div>
  <div class="card">
    <h3>Match Rate</h3>
    <p>87.5%</p>
  </div>
</div>
```

**What's Happening**:
- Shows property-level KPIs
- For single lease audit, shows just that lease
- Red highlights for undercharges

### Step 9.3: Display Lease Table
```html
<table class="lease-table">
  <tr>
    <th>Lease</th>
    <th>Resident</th>
    <th>Exceptions</th>
    <th>Variance</th>
    <th>Actions</th>
  </tr>
  <tr>
    <td>15293094</td>
    <td>John Doe</td>
    <td>15</td>
    <td class="text-danger">-$825.00</td>
    <td>
      <a href="/lease/run_20260312_143022/1150907/15293094">
        View Details →
      </a>
    </td>
  </tr>
</table>
```

**What's Happening**:
- Shows one row for the audited lease
- Clickable link to drill into lease details
- Shows exception count and net variance

---

## Complete Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ USER INPUT (HTML Form)                                          │
│ - Lease ID: 15293094                                            │
│ - Property ID: 1150907 (optional)                               │
│ - From Date: 01/01/2024 (optional)                              │
└────────────────────────┬────────────────────────────────────────┘
                         │ HTTP POST
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│ ROUTE HANDLER (web/views.py::upload_api_lease)                 │
│ 1. Extract form data                                            │
│ 2. Generate run_id: run_20260312_143022                         │
└────────────────────────┬────────────────────────────────────────┘
                         │ Call API fetch
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│ API FETCH (audit_engine/api_ingest.py)                         │
│ Step 1: Discover property_id (if not provided)                 │
│   POST getLeaseDetails(leaseIds=15293094)                       │
│   Response: property_id=1150907                                 │
│                                                                  │
│ Step 2: Fetch scheduled charges                                │
│   POST getLeaseDetails(propertyId=1150907, leaseIds=15293094)  │
│   Response: 12 scheduled charges (RAW)                          │
│                                                                  │
│ Step 3: Fetch AR transactions                                  │
│   POST getLeaseArTransactions(propertyId=1150907, leaseIds=...) │
│   Response: 45 transactions (RAW)                               │
│                                                                  │
│ Step 4: Defensive filtering                                    │
│   Filter scheduled_df to lease_id=15293094                      │
│   Filter ar_df to lease_id=15293094                             │
│                                                                  │
│ Return: {scheduled_raw, ar_raw, property_name, property_id}    │
└────────────────────────┬────────────────────────────────────────┘
                         │ Pass to audit pipeline
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│ AUDIT PIPELINE (web/views.py::execute_audit_run)               │
│                                                                  │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ STEP 1: SOURCE MAPPING (RAW → CANONICAL)                    │ │
│ │ Input: RAW DataFrames (Entrata column names)                │ │
│ │ Process: apply_source_mapping()                             │ │
│ │ Output: CANONICAL DataFrames (internal field names)         │ │
│ │   AR: 45 rows                                               │ │
│ │   Scheduled: 12 rows                                        │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                         │                                        │
│                         ▼                                        │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ STEP 2: NORMALIZATION                                       │ │
│ │ Process: normalize_ar_transactions()                        │ │
│ │          normalize_scheduled_charges()                      │ │
│ │ Actions:                                                    │ │
│ │   - Validate required fields                               │ │
│ │   - Convert data types                                     │ │
│ │   - Filter out deleted/invalid rows                        │ │
│ │   - Exclude API-posted codes                               │ │
│ │ Output: Validated DataFrames                               │ │
│ │   actual_detail: 45 rows                                   │ │
│ │   scheduled_normalized: 12 rows                            │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                         │                                        │
│                         ▼                                        │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ STEP 3: EXPANSION (Scheduled → Monthly Buckets)            │ │
│ │ Process: expand_scheduled_to_months()                      │ │
│ │ Example:                                                   │ │
│ │   Input: 1 row (Aug 2024 - May 2025, $850/mo)             │ │
│ │   Output: 10 rows (one per month)                          │ │
│ │ Total: 12 scheduled → 120 monthly expected rows            │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                         │                                        │
│                         ▼                                        │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ STEP 4: AUDIT PERIOD FILTERING                             │ │
│ │ Process: filter_to_current_academic_year()                 │ │
│ │ Filter: Aug 2025 - Mar 2026 (current academic year)        │ │
│ │ Output:                                                    │ │
│ │   expected_detail: 80 rows (8 months × ~10 charges)        │ │
│ │   actual_detail: 60 rows (filtered transactions)           │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                         │                                        │
│                         ▼                                        │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ STEP 5: RECONCILIATION                                     │ │
│ │ Process: reconcile_buckets()                               │ │
│ │                                                            │ │
│ │ A. Create bucket keys:                                     │ │
│ │    (property, lease, ar_code, month)                       │ │
│ │                                                            │ │
│ │ B. Tier 1 PRIMARY matching:                                │ │
│ │    - Group expected by bucket, sum amounts                 │ │
│ │    - Group actual by bucket, sum amounts                   │ │
│ │    - Hash join on bucket key (O(n))                        │ │
│ │    - Compare expected vs actual                            │ │
│ │    - Assign status: MATCHED / SCHEDULED_NOT_BILLED /       │ │
│ │                     BILLED_NOT_SCHEDULED                   │ │
│ │                                                            │ │
│ │ C. Tier 2 SECONDARY matching:                              │ │
│ │    - Match by amount (different buckets)                   │ │
│ │                                                            │ │
│ │ D. Tier 3 TERTIARY matching:                               │ │
│ │    - Match by date mismatch                                │ │
│ │                                                            │ │
│ │ E. Generate findings:                                      │ │
│ │    - Analyze variances                                     │ │
│ │    - Create human-readable findings                        │ │
│ │    - Categorize by severity                                │ │
│ │                                                            │ │
│ │ Output:                                                    │ │
│ │   bucket_results: 120 rows (all buckets)                   │ │
│ │   findings: 15 rows (variance findings)                    │ │
│ │   variance_detail: 25 rows (detailed discrepancies)        │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                         │                                        │
│                         ▼                                        │
│ Return: {bucket_results, findings, expected_detail,            │
│          actual_detail, variance_detail, property_name_map}    │
└────────────────────────┬────────────────────────────────────────┘
                         │ Save results
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│ SAVE TO SHAREPOINT (storage/service.py::save_run)              │
│                                                                  │
│ Step 1: Create metadata                                        │
│   {run_id, timestamp, run_scope: {type: 'lease', ...}}         │
│                                                                  │
│ Step 2: Save to Document Library                               │
│   Folder: LeaseFileAudit Runs/run_20260312_143022/             │
│   Files:                                                        │
│     - run_meta.json                                             │
│     - bucket_results.csv                                        │
│     - findings.csv                                              │
│     - expected_detail.csv                                       │
│     - actual_detail.csv                                         │
│     - variance_detail.csv                                       │
│                                                                  │
│ Step 3: Save to AuditRuns List (synchronous)                   │
│   For each bucket_results row:                                 │
│     - Create list item                                          │
│     - Post via Graph API ($batch, 10 items/batch)              │
│     - Retry on 429/503/504 errors                               │
│   Total: 120 bucket_result rows + 15 finding rows              │
│                                                                  │
│ Step 4: Save snapshot to RunDisplaySnapshots List              │
│   - Property-level snapshot (1 row)                            │
│   - Lease-level snapshot (1 row)                               │
│   - Precomputed KPIs for fast UI loading                       │
│                                                                  │
│ Step 5: Log activity to Innovation Use Log                     │
│   - Activity type: "Successful Audit"                          │
│   - Details: run_id, lease_id, property_id, counts             │
└────────────────────────┬────────────────────────────────────────┘
                         │ Redirect user
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│ HTTP RESPONSE                                                   │
│ - Flash message: "Audit completed, found 15 exceptions"         │
│ - Redirect: /property/run_20260312_143022/1150907              │
└────────────────────────┬────────────────────────────────────────┘
                         │ Browser follows redirect
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│ PROPERTY VIEW (web/views.py::property_view)                    │
│                                                                  │
│ Step 1: Load data from SharePoint                              │
│   - Query AuditRuns list: filter RunId + PropertyId            │
│   - Load property snapshot from RunDisplaySnapshots            │
│                                                                  │
│ Step 2: Render UI (templates/property.html)                    │
│   - Property header with KPIs                                  │
│   - Lease table (1 row for lease 15293094)                     │
│   - Link to drill into lease details                           │
│                                                                  │
│ Display:                                                        │
│   Property: Bibby Hall                                          │
│   Total Leases: 1                                               │
│   Open Exceptions: 15                                           │
│   Undercharge: $850.00                                          │
│   Match Rate: 87.5%                                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Performance Characteristics

### API Fetch Phase
- **getLeaseDetails**: ~2-3 seconds
- **getLeaseArTransactions**: ~2-3 seconds
- **Total API time**: ~5-6 seconds

### Reconciliation Phase
- **Source mapping**: <0.1 seconds (120 rows)
- **Normalization**: <0.1 seconds
- **Expansion**: <0.5 seconds (12 → 120 rows)
- **Reconciliation**: <0.2 seconds (O(n) hash join)
- **Total processing**: <1 second

### SharePoint Save Phase
- **CSV writes**: ~1-2 seconds
- **AuditRuns list write** (synchronous):
  - 135 rows (120 buckets + 15 findings)
  - 14 batches (10 items/batch)
  - With retries: ~20-30 seconds
- **Snapshot writes**: ~0.5 seconds
- **Total save time**: ~25-35 seconds

### Total End-to-End
- **Typical**: ~35-45 seconds
- **With retries/throttling**: up to 60 seconds

---

## Data Volumes (Example Single Lease)

```
RAW API DATA:
  Scheduled charges: 12 rows
  AR transactions: 45 rows

AFTER EXPANSION:
  Expected detail: 120 rows (12 charges × 10 months avg)
  Actual detail: 45 rows (unchanged)

AFTER FILTERING (Academic Year):
  Expected detail: 80 rows (filtered to 8 months)
  Actual detail: 38 rows (filtered to 8 months)

RECONCILIATION OUTPUTS:
  Bucket results: 120 rows (all possible buckets)
  Findings: 15 rows (variances found)
  Variance detail: 25 rows (detailed discrepancies)

SHAREPOINT STORAGE:
  AuditRuns list: 135 rows (120 buckets + 15 findings)
  RunDisplaySnapshots: 2 rows (property + lease snapshots)
  Document library: 6 files (JSONs + CSVs)
```

---

## Key Design Decisions

### 1. Why Send Both propertyId AND leaseIds to Entrata API?
- **Entrata API requirement**: Requires both parameters for proper filtering
- **Without both**: API returns all leases for the property
- **With both**: API returns only the requested lease

### 2. Why Defensive Filtering After API?
- **Safety net**: In case API returns extra data despite filters
- **Data integrity**: Ensures audit processes ONLY requested lease
- **Performance**: Minimal overhead, only filters if needed

### 3. Why Synchronous SharePoint Writes?
- **Data completeness**: All rows visible immediately after redirect
- **User experience**: No partial data display
- **Trade-off**: Slower uploads (~30 sec) but guaranteed complete data

### 4. Why Three Storage Locations?
- **Document Library (CSVs)**: Fallback compatibility, full data export
- **AuditRuns List**: Fast filtered queries, indexed columns
- **RunDisplaySnapshots**: Precomputed KPIs for instant UI load

### 5. Why Property-Scoped Reconciliation?
- **Scalability**: Can handle multi-property audits efficiently
- **Isolation**: Property results don't interfere with each other
- **Consistency**: Same code path for single and multi-property audits

---

## Error Handling & Edge Cases

### API Fetch Errors
```python
try:
    api_sources = fetch_single_lease_api_sources(...)
except ValueError as e:
    # No lease found with that ID
    flash(f'API error: {e}', 'danger')
    return redirect(url_for('main.index'))
except Exception as e:
    # Network error, timeout, auth failure
    flash(f'API connection failed: {e}', 'danger')
    return redirect(url_for('main.index'))
```

### Empty Data Handling
```python
if ar_raw.empty and scheduled_raw.empty:
    flash('API returned no data for that lease ID.', 'warning')
    return redirect(url_for('main.index'))
```

### SharePoint Throttling
```python
# Automatic retry with exponential backoff
for attempt in range(3):
    try:
        result = graph_api.post_batch(items)
        break
    except ThrottlingError as e:
        if attempt < 2:
            wait_time = [0.5, 1.0, 2.0][attempt]
            time.sleep(wait_time)
        else:
            # Fall back to individual posts
            for item in items:
                graph_api.post_single_item(item)
```

### Missing Property Names
```python
# Try multiple sources for property name
property_name = (
    api_sources.get('property_name') or
    property_name_map.get(property_id) or
    f"Property {property_id}"
)
```

---

## Logging Checkpoints

Current logging in the system:

```
[API LEASE UPLOAD] Lease 15293094, Property 1150907 (Bibby Hall)
[SINGLE LEASE API] Discovered property_id=1150907 for lease_id=15293094
[SINGLE LEASE API] Filtered scheduled charges: 98 → 12 rows (lease_id=15293094)
[SINGLE LEASE API] Filtered AR transactions: 450 → 45 rows (lease_id=15293094)
[EXECUTE_AUDIT_RUN] Loaded raw sources:
  AR Transactions: (45, 28)
  Scheduled Charges: (12, 35)
[EXECUTE_AUDIT_RUN] Applying source mappings...
[EXECUTE_AUDIT_RUN] Normalizing canonical data...
[AR FILTER DEBUG] Total AR transactions: 45
[SCHEDULED FILTER DEBUG] Total scheduled charges: 12
[EXECUTE_AUDIT_RUN] Upload-time property name map size: 1
[AUDIT PERIOD FILTER] Applying default Current Academic Year filter
[AUDIT PERIOD FILTER] Before filter - Expected: 120, Actual: 45
[AUDIT PERIOD FILTER] After filter - Expected: 80, Actual: 38
[API CODE FILTER] ========== FILTERING API CODES ==========
[API CODE FILTER] Total AR transactions before filter: 38
[API CODE FILTER] Remaining AR transactions: 38
[PROPERTY EXECUTION] Running reconciliation per property (1 properties)
[PROPERTY EXECUTION] PROPERTY_ID=1150907: expected=80, actual=38, scheduled=12
[RECONCILIATION STATS] Property 1150907: 120 buckets, 105 matched, 15 exceptions
[STORAGE] Saving run to SharePoint: run_20260312_143022
[STORAGE] Batch 1/14 posted successfully (10 items)
[STORAGE] Batch 2/14 posted successfully (10 items)
...
[STORAGE] ✅ AuditRuns write finished: 135 rows persisted
[STORAGE] Saved property snapshot for property_id=1150907
[STORAGE] Saved lease snapshot for lease_id=15293094
```

---

## Suggested Additional Debug Statements

If you want more visibility into the flow, here are strategic places to add debug logging:

### 1. API Response Details
```python
# In fetch_single_lease_api_sources(), after API call
print(f"[API DEBUG] getLeaseDetails response size: {len(json.dumps(lease_details_payload))} bytes")
print(f"[API DEBUG] Found {len(lease_nodes)} lease nodes")
print(f"[API DEBUG] Lease interval status: {lease_nodes[0].get('leaseIntervalStatus')}")
```

### 2. DataFrame Column Tracking
```python
# After source mapping
print(f"[MAPPING DEBUG] AR columns after mapping: {list(ar_canonical.columns)}")
print(f"[MAPPING DEBUG] Scheduled columns after mapping: {list(scheduled_canonical.columns)}")
```

### 3. Expansion Details
```python
# In expand_scheduled_to_months()
print(f"[EXPAND DEBUG] Charge {row['AR_CODE_ID']}: {start_date} to {end_date} = {month_count} months")
```

### 4. Bucket Details
```python
# In reconcile_buckets()
for bucket_key, bucket_data in bucket_results.iterrows():
    if bucket_data['STATUS'] != 'MATCHED':
        print(f"[BUCKET DEBUG] Exception: {bucket_key} | Expected: ${bucket_data['EXPECTED_TOTAL']} | Actual: ${bucket_data['ACTUAL_TOTAL']} | Variance: ${bucket_data['VARIANCE']}")
```

### 5. SharePoint Write Progress
```python
# In batch write loop
print(f"[BATCH DEBUG] Writing batch {batch_num}/{total_batches} ({len(batch_items)} items)")
print(f"[BATCH DEBUG] First item in batch: {batch_items[0]['CompositeKey']}")
```

Would you like me to:
1. **Add these debug statements** to the code for better visibility?
2. **Create a debug mode** (environment variable) that enables verbose logging?
3. **Add performance timers** at each phase to track bottlenecks?
4. **Create a visual flow diagram** with actual API request/response examples?

Let me know what would be most helpful!
